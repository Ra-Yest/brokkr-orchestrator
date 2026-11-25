"""Runner lock management, crash recovery, control-plane handling, pauses, and hibernation."""

from __future__ import annotations

import contextlib
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from orchestrator.config import PipelineConfig
from orchestrator.control import ControlPlane
from orchestrator.events import EventBus
from orchestrator.fs_utils import atomic_write_json
from orchestrator.git_ops import GitOps
from orchestrator.models import (
    AgentRole,
    ControlAction,
    ControlCommand,
    EventType,
    RunStatus,
    Task,
    TaskStatus,
    utc_now_iso,
)
from orchestrator.notifications import BaseNotifier
from orchestrator.process_utils import is_process_alive
from orchestrator.runtime.errors import (
    OrchestratorFatalError,
    RunnerLockError,
    _AbortedByControlError,
)
from orchestrator.stores.base import BaseTaskStore


class TransitionTaskFn(Protocol):
    """Callable signature for transitioning a task status with optional persisted fields."""

    def __call__(self, task_id: str, status: TaskStatus, **fields: Any) -> Task: ...


class RunnerLifecycleManager:
    """Manages runner PID locks, preflight/crash recovery, control inbox, pauses, and backoff."""

    def __init__(
        self,
        *,
        repo_root: Path,
        lock_path: Path,
        config: PipelineConfig,
        store: BaseTaskStore,
        git_ops: GitOps,
        event_bus: EventBus,
        control_plane: ControlPlane,
        notifier: BaseNotifier,
        sleep_fn: Callable[[float], Any],
        transition_task: TransitionTaskFn,
        is_process_alive_fn: Callable[[int], bool] = is_process_alive,
    ) -> None:
        self.repo_root = repo_root
        self.lock_path = lock_path
        self.config = config
        self.store = store
        self.git_ops = git_ops
        self.event_bus = event_bus
        self.control_plane = control_plane
        self.notifier = notifier
        self._sleep = sleep_fn
        self._transition_task = transition_task
        self._is_process_alive = is_process_alive_fn

        self._lock_acquired = False
        self._abort_requested = False
        self._resumable_needs_review_base_sha: dict[str, str] = {}

    @property
    def abort_requested(self) -> bool:
        """Return True if an operator abort command has been received."""
        return self._abort_requested

    def pop_resumable_needs_review_base_sha(self, task_id: str) -> str | None:
        """Pop and return the preserved base SHA for a checkpointed NEEDS_REVIEW task, if any."""
        return self._resumable_needs_review_base_sha.pop(task_id, None)

    # -------------------------------------------------------------------------
    # PID Lockfile Management
    # -------------------------------------------------------------------------

    def acquire_runner_lock(self) -> dict[str, Any] | None:
        """Acquire `.pipeline/runner.lock`. Raises RunnerLockError if an active PID holds it."""
        stale_info: dict[str, Any] | None = None
        if self.lock_path.exists():
            old_pid: int | None = None
            old_run_id: str | None = None
            old_started_at: str | None = None
            try:
                raw = json.loads(self.lock_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    raw_pid = raw.get("pid")
                    old_pid = int(raw_pid) if isinstance(raw_pid, int) else None
                    old_run_id = str(raw.get("run_id")) if raw.get("run_id") else None
                    old_started_at = str(raw.get("started_at")) if raw.get("started_at") else None
            except Exception:
                old_pid = None

            if old_pid is not None and self._is_process_alive(old_pid):
                raise RunnerLockError(
                    f"Another orchestrator instance is already running "
                    f"(pid={old_pid}, run_id={old_run_id}, lock={self.lock_path})."
                )
            stale_info = {
                "stale_pid": old_pid,
                "stale_run_id": old_run_id,
                "stale_started_at": old_started_at,
            }

        atomic_write_json(
            self.lock_path,
            {
                "pid": os.getpid(),
                "run_id": self.event_bus.run_id,
                "started_at": utc_now_iso(),
            },
        )
        self._lock_acquired = True
        return stale_info

    def release_runner_lock(self) -> None:
        if not self._lock_acquired:
            return
        self._lock_acquired = False
        with contextlib.suppress(OSError):
            if self.lock_path.exists():
                self.lock_path.unlink()

    # -------------------------------------------------------------------------
    # Crash Recovery, Preflight & Abort Cleanup
    # -------------------------------------------------------------------------

    def cleanup_aborted_run(self) -> None:
        """Roll back uncommitted work and return any unfinished active task to TODO on abort."""
        active_task_id = self.event_bus.state.active_task_id
        base_sha = self.event_bus.state.active_task_base_sha

        active_task: Task | None = None
        if active_task_id and "," not in active_task_id:
            with contextlib.suppress(KeyError):
                active_task = self.store.get_task(active_task_id)

        unfinished_statuses = (
            TaskStatus.IN_PROGRESS,
            TaskStatus.GATES_FAILED,
            TaskStatus.NEEDS_REVIEW,
        )
        unfinished_tasks: list[Task] = []
        if active_task is not None and active_task.status in unfinished_statuses:
            unfinished_tasks.append(active_task)
        for task in self.store.list_tasks():
            if task.status in unfinished_statuses and all(
                t.id != task.id for t in unfinished_tasks
            ):
                unfinished_tasks.append(task)

        needs_rollback = (
            bool(unfinished_tasks)
            or (active_task is not None and active_task.status == TaskStatus.ARCH_PENDING)
            or (self.event_bus.state.active_phase == "refactorer")
            or (base_sha is not None and self.git_ops.has_uncommitted_changes())
        )

        rollback_sha: str | None = None
        if needs_rollback:
            if base_sha and self.git_ops.commit_exists(base_sha):
                rollback_sha = base_sha
            else:
                rollback_sha = self.git_ops.get_head_sha()
            if (
                self.git_ops.has_uncommitted_changes()
                or self.git_ops.get_head_sha() != rollback_sha
            ):
                self.git_ops.hard_rollback(rollback_sha)
                self.event_bus.emit(
                    EventType.TASK_ROLLED_BACK,
                    task_id=active_task.id if active_task is not None else None,
                    payload={"reason": "control_abort", "base_sha": rollback_sha},
                )

        for task in unfinished_tasks:
            sha_note = f" (rolled back to {rollback_sha[:10]})" if rollback_sha else ""
            self.store.add_comment(
                task.id,
                author="control_plane",
                kind="control",
                body=(
                    f"Returned task from '{task.status.value}' to 'todo' "
                    f"after control abort{sha_note}."
                ),
            )
            self._transition_task(
                task.id,
                TaskStatus.TODO,
                wip_commit_sha=None,
                changed_files=[],
                touched_yellow_zone=False,
                pending_progress_note=None,
                pre_review_approved=False,
            )

    def _is_resumable_needs_review(self, task: Task) -> bool:
        """Return True if a NEEDS_REVIEW task matches a prior paused_infra/active checkpoint."""
        prev = self.event_bus.previous_state
        if prev is None:
            return False
        paused_in_review = (
            prev.status == RunStatus.PAUSED
            and prev.active_phase == "paused_infra"
            and (
                prev.active_role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER)
                or prev.active_task_id == task.id
            )
        )
        return paused_in_review or prev.active_task_id == task.id

    def recover_or_preflight(self, *, stale_lock_recovered: bool) -> None:
        if not self.git_ops.is_git_repo():
            raise OrchestratorFatalError(f"Target path is not a git repository: {self.repo_root}")

        prev = self.event_bus.previous_state
        prev_was_running = prev is not None and prev.status == RunStatus.RUNNING
        all_tasks = self.store.list_tasks()
        dirty_tree = self.git_ops.has_uncommitted_changes()

        needs_review_tasks = [t for t in all_tasks if t.status == TaskStatus.NEEDS_REVIEW]
        in_progress_tasks = [
            t for t in all_tasks if t.status in (TaskStatus.IN_PROGRESS, TaskStatus.GATES_FAILED)
        ]
        arch_pending_tasks = [t for t in all_tasks if t.status == TaskStatus.ARCH_PENDING]

        # Handle NEEDS_REVIEW checkpoint preservation vs clean-tree safe reset
        resumable_needs_review: list[Task] = []
        clean_needs_review: list[Task] = []
        non_resumable_needs_review: list[Task] = []
        for task in needs_review_tasks:
            if not dirty_tree:
                clean_needs_review.append(task)
            elif self._is_resumable_needs_review(task):
                resumable_needs_review.append(task)
            else:
                non_resumable_needs_review.append(task)

        # If a NEEDS_REVIEW task has a clean working tree (reverted manually/externally), reset safely to TODO
        if clean_needs_review:
            head_sha = self.git_ops.get_head_sha()
            for task in clean_needs_review:
                self.store.add_comment(
                    task.id,
                    author="orchestrator",
                    kind="system",
                    body=(
                        "Reset task from 'needs_review' to 'todo' because working tree is clean "
                        "(external or manual rollback detected)."
                    ),
                )
                self._transition_task(
                    task.id,
                    TaskStatus.TODO,
                    changed_files=[],
                    touched_yellow_zone=False,
                    pending_progress_note=None,
                    pre_review_approved=False,
                )
            self.event_bus.emit(
                EventType.CRASH_RECOVERY_COMPLETED,
                payload={
                    "rollback_sha": head_sha,
                    "rolled_back_dirty_tree": False,
                    "recovered_task_ids": [t.id for t in clean_needs_review],
                    "reason": "clean_working_tree_for_needs_review",
                    "arch_pending_task_ids": [t.id for t in arch_pending_tasks],
                },
            )

        # Preserve dirty working tree when resuming a checkpointed NEEDS_REVIEW task
        if resumable_needs_review and not in_progress_tasks and not non_resumable_needs_review:
            candidate_sha = prev.active_task_base_sha if prev is not None else None
            base_sha = (
                candidate_sha
                if (candidate_sha and self.git_ops.commit_exists(candidate_sha))
                else self.git_ops.get_head_sha()
            )
            for task in resumable_needs_review:
                self._resumable_needs_review_base_sha[task.id] = base_sha
            return

        interrupted_tasks = [*in_progress_tasks, *non_resumable_needs_review]
        had_crash = (
            prev_was_running
            or len(interrupted_tasks) > 0
            or (stale_lock_recovered and len(arch_pending_tasks) > 0)
        )

        if had_crash:
            candidate_sha = prev.active_task_base_sha if prev is not None else None
            if candidate_sha and self.git_ops.commit_exists(candidate_sha):
                rollback_sha = candidate_sha
            else:
                rollback_sha = self.git_ops.get_head_sha()

            rolled_back = False
            if self.git_ops.has_uncommitted_changes() or self.git_ops.get_head_sha() != rollback_sha:
                self.git_ops.hard_rollback(rollback_sha)
                rolled_back = True

            recovered_ids: list[str] = []
            for task in interrupted_tasks:
                self.store.add_comment(
                    task.id,
                    author="orchestrator",
                    kind="system",
                    body=(
                        f"Recovered task from '{task.status.value}' to 'todo' "
                        f"after previous crashed run (base_sha={rollback_sha[:10]})."
                    ),
                )
                self._transition_task(
                    task.id,
                    TaskStatus.TODO,
                    pre_review_approved=False,
                )
                recovered_ids.append(task.id)

            self.event_bus.emit(
                EventType.CRASH_RECOVERY_COMPLETED,
                payload={
                    "rollback_sha": rollback_sha,
                    "rolled_back_dirty_tree": rolled_back,
                    "recovered_task_ids": recovered_ids,
                    "arch_pending_task_ids": [t.id for t in arch_pending_tasks],
                },
            )
            return

        # No crashed run detected: NEVER wipe dirty manual user changes!
        if self.config.workflow.strict_clean_start and self.git_ops.has_uncommitted_changes():
            raise OrchestratorFatalError(
                "Target repository has uncommitted changes and no prior crashed run was detected. "
                "Refusing to reset manual changes; commit or stash them first "
                "(or set workflow.strict_clean_start: false)."
            )

    # -------------------------------------------------------------------------
    # Control Plane, Pause, Hibernation & Backoff Handling
    # -------------------------------------------------------------------------

    def process_control_commands(self) -> list[ControlCommand]:
        processed_set = set(self.event_bus.state.processed_control_ids)
        pending = self.control_plane.poll_pending(processed_set)
        applied: list[ControlCommand] = []

        for cmd, source_path in pending:
            result_msg = self._apply_control_command(cmd)
            completed = self.control_plane.mark_processed(cmd, source_path, result=result_msg)
            processed_ids = [*self.event_bus.state.processed_control_ids, cmd.id][-500:]
            self.event_bus.update_state(
                processed_control_ids=processed_ids,
                last_control_command_id=cmd.id,
            )
            self.event_bus.emit(
                EventType.CONTROL_COMMAND_APPLIED,
                task_id=cmd.task_id,
                payload={
                    "command_id": cmd.id,
                    "action": cmd.action.value,
                    "reason": cmd.reason,
                    "result": result_msg,
                },
            )
            applied.append(completed)

        return applied

    def _apply_control_command(self, cmd: ControlCommand) -> str:
        if cmd.action == ControlAction.PAUSE_AFTER_CURRENT:
            self.event_bus.update_state(pause_after_current=True)
            return "scheduled pause after current step"

        if cmd.action == ControlAction.RESUME:
            self.event_bus.update_state(
                paused=False,
                pause_after_current=False,
                pause_reason=None,
                resume_at=None,
                status=RunStatus.RUNNING,
            )
            self.event_bus.emit(EventType.PAUSE_RESUMED, payload={"command_id": cmd.id})
            return "resumed"

        if cmd.action == ControlAction.ABORT:
            self._abort_requested = True
            return "abort requested"

        if cmd.action == ControlAction.RETRY_TASK:
            if not cmd.task_id:
                return "ignored: missing task_id"
            try:
                unblocked = self.store.retry_task(cmd.task_id)
                return f"retried #{cmd.task_id}; unblocked={unblocked}"
            except KeyError:
                return f"ignored: unknown task_id #{cmd.task_id}"

        if cmd.action == ControlAction.SKIP_TASK:
            if not cmd.task_id:
                return "ignored: missing task_id"
            try:
                reason = cmd.reason or "Skipped via control command"
                self.store.update_status(
                    cmd.task_id,
                    TaskStatus.FAILED,
                    resolution="skipped",
                    failure_reason=reason,
                )
                self.store.add_comment(
                    cmd.task_id,
                    author="control_plane",
                    kind="control",
                    body=reason,
                )
                blocked = self.store.cascade_block_dependents(cmd.task_id)
                return f"skipped #{cmd.task_id}; blocked={blocked}"
            except KeyError:
                return f"ignored: unknown task_id #{cmd.task_id}"

        return "unsupported action"

    def check_control_after_step(self) -> None:
        """Process control commands immediately after a provider step, honoring abort & pause_after_current."""
        self.process_control_commands()
        if self._abort_requested:
            raise _AbortedByControlError()
        if self.event_bus.state.pause_after_current or self.event_bus.state.paused:
            self.wait_while_paused(reason="operator_pause")
            if self._abort_requested:
                raise _AbortedByControlError()

    def wait_while_paused(self, *, reason: str) -> None:
        self.event_bus.update_state(
            status=RunStatus.PAUSED,
            paused=True,
            pause_reason=reason,
            pause_after_current=False,
        )
        self.event_bus.emit(EventType.PAUSE_ENTERED, payload={"reason": reason})
        self.notifier.notify(
            "pause",
            f"Pipeline Paused ({self.config.project.name})",
            f"Run {self.event_bus.run_id} paused ({reason}). Send `orchestrator control resume` to continue.",
        )
        while self.event_bus.state.paused and not self._abort_requested:
            self._sleep(self.config.workflow.control_poll_seconds)
            self.process_control_commands()
        if not self._abort_requested:
            self.event_bus.update_state(status=RunStatus.RUNNING)

    def hibernate_until(
        self,
        *,
        resume_dt: datetime,
        providers: list[str],
        task_id: str | None,
        role: AgentRole,
    ) -> None:
        """Enter hibernating state when all providers for a role are rate-limited / quota-capped."""
        now = datetime.now(UTC)
        wait_seconds = max(1, int((resume_dt - now).total_seconds()))
        resume_iso = resume_dt.isoformat().replace("+00:00", "Z")
        local_resume = resume_dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
        providers_label = ", ".join(providers)

        self.event_bus.update_state(
            status=RunStatus.HIBERNATING,
            active_task_id=task_id,
            active_phase="hibernating",
            active_role=role,
            paused=True,
            pause_reason=f"rate_limit:{providers_label}",
            resume_at=resume_iso,
        )
        self.event_bus.emit(
            EventType.RUNNER_HIBERNATING,
            task_id=task_id,
            role=role,
            provider=providers[0] if providers else None,
            payload={
                "providers": providers,
                "cooldown_seconds": wait_seconds,
                "resume_at": resume_iso,
            },
        )
        task_suffix = f" on #{task_id}" if task_id else ""
        self.notifier.notify(
            "hibernation",
            f"Rate Limit Pause / Hibernating ({self.config.project.name})",
            f"All provider(s) [{providers_label}] rate-limited/quota-capped{task_suffix}. "
            f"Hibernating for {wait_seconds}s; wake-up at {resume_iso} ({local_resume}).",
        )

        wake_reason = "cooldown_complete"
        poll_step = min(
            self.config.workflow.control_poll_seconds,
            self.config.workflow.rate_limit_poll_seconds,
        )
        while not self._abort_requested:
            self.process_control_commands()
            if self._abort_requested:
                break
            if not self.event_bus.state.paused:
                # Operator explicitly issued `resume` -> clear cooldowns so runner wakes immediately
                for pname in providers:
                    self.event_bus.quota_tracker.clear_cooldown(pname)
                wake_reason = "operator_resume"
                break
            remaining = (resume_dt - datetime.now(UTC)).total_seconds()
            if remaining <= 0:
                break
            self._sleep(min(poll_step, remaining))

        if not self._abort_requested:
            self.event_bus.update_state(
                status=RunStatus.RUNNING,
                paused=False,
                pause_reason=None,
                resume_at=None,
            )
            self.event_bus.emit(
                EventType.RUNNER_WOKE_UP,
                task_id=task_id,
                role=role,
                provider=providers[0] if providers else None,
                payload={"reason": wake_reason, "providers": providers},
            )

    def wait_backoff(
        self,
        *,
        delay_seconds: float,
        provider_name: str,
        task_id: str | None,
        role: AgentRole,
        phase_prefix: str = "transient_backoff",
    ) -> None:
        """Sleep for backoff in short ticks, polling control_inbox for immediate abort/pause."""
        self.event_bus.update_state(
            active_task_id=task_id,
            active_phase=f"{phase_prefix}:{provider_name}",
            active_role=role,
            active_provider=provider_name,
        )
        poll = self.config.workflow.control_poll_seconds
        slept_total = 0.0
        while slept_total < delay_seconds and not self._abort_requested:
            self.process_control_commands()
            if self._abort_requested:
                raise _AbortedByControlError()
            if self.event_bus.state.pause_after_current or self.event_bus.state.paused:
                self.wait_while_paused(reason="operator_pause")
                if self._abort_requested:
                    raise _AbortedByControlError()
            step = min(poll, delay_seconds - slept_total)
            if step <= 0:
                break
            self._sleep(step)
            slept_total += step
        self.process_control_commands()
        if self._abort_requested:
            raise _AbortedByControlError()
