"""Main headless AFK orchestrator runner with routing, batch review, escalation, splitting, and guards."""

from __future__ import annotations

import contextlib
import json
import os
import random
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TextIO

from orchestrator.config import PipelineConfig
from orchestrator.control import ControlPlane, InboxManager
from orchestrator.diff_guard import (
    DiffGuardReport,
    GuardViolation,
    _split_diff_sections,
    evaluate_diff,
    matches_any_glob,
)
from orchestrator.events import EventBus, make_console_event_printer
from orchestrator.fs_utils import (
    atomic_write_json,
    atomic_write_text,
    diff_directory_snapshots,
    normalize_newlines,
    sha256_file,
    snapshot_directory_hashes,
)
from orchestrator.gates import GateRunner
from orchestrator.git_ops import GitChange, GitOperationError, GitOps
from orchestrator.models import (
    READ_ONLY_ROLES,
    WORKER_ROLES,
    WRITING_ROLES,
    AgentRole,
    ControlAction,
    ControlCommand,
    EventType,
    ParsedWorkerOutput,
    ProxyHealthStatus,
    ReviewVerdictType,
    RunStatus,
    Task,
    TaskComplexity,
    TaskStatus,
    WorkerStatus,
    utc_now_iso,
)
from orchestrator.notifications import BaseNotifier, build_notifier
from orchestrator.output_parser import (
    parse_batch_review_verdicts,
    parse_planner_subtasks,
    parse_single_review_verdict,
    parse_worker_output,
)
from orchestrator.process_utils import (
    cleanup_ports,
    is_meaningful_output,
    is_process_alive,
    run_noninteractive_process,
)
from orchestrator.prompt_builder import BuiltPrompt, PromptBuilder, generate_repo_map
from orchestrator.providers.base import (
    BaseAgentProvider,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
)
from orchestrator.proxy_monitor import BaseProxyMonitor, build_proxy_monitor
from orchestrator.reporter import write_report
from orchestrator.stores.base import BaseTaskStore

_PERMANENT_ERROR_RE = re.compile(
    r"(?:\b401\b|\b403\b|unauthorized|forbidden|invalid[\s_-]*api[\s_-]*key|"
    r"authentication[\s_-]*failed|auth[\s_-]*failed|unknown\s+option|"
    r"unrecognized\s+arguments?|invalid\s+option|command\s+not\s+found|"
    r"FileNotFoundError|is\s+not\s+recognized\s+as\s+an\s+internal\s+or\s+external\s+command)",
    re.IGNORECASE,
)

_TRANSIENT_ERROR_RE = re.compile(
    r"(?:\b502\b|\b503\b|\b504\b|bad\s+gateway|service\s+unavailable|"
    r"gateway\s+timeout|connection\s+reset|econnreset|etimedout|"
    r"broken\s+pipe|\beof\b|temporary\s+failure|network|connection\s+refused|"
    r"econnrefused|socket\s+hang\s+up|tls\s+handshake|remote\s+disconnected|"
    r"proxy|fetch\s+failed|unable\s+to\s+connect|could\s+not\s+connect|"
    r"dns\s+resolution|getaddrinfo)",
    re.IGNORECASE,
)

# Legitimate operator input / runtime log channels inside `.pipeline/` that may change while a provider runs.
PIPELINE_INTEGRITY_EXCLUDES: tuple[str, ...] = (
    "control_inbox/**",
    "INBOX.md",
    "control.json",
    "console.log",
)


def classify_provider_failure(
    response: ProviderResponse,
) -> Literal["none", "timeout", "permanent", "transient"]:
    """Classify a failed provider response as permanent (fail-fast), transient (retry), or timeout."""
    if response.exit_code == 0 and not response.timed_out and not response.idle_timed_out:
        return "none"
    if response.timed_out or response.idle_timed_out:
        return "timeout"
    combined = f"{response.raw_stderr}\n{response.raw_stdout}"
    if _PERMANENT_ERROR_RE.search(combined):
        return "permanent"
    if _TRANSIENT_ERROR_RE.search(combined):
        return "transient"
    # Empty or abnormal crash (e.g. killed by signal or empty stdout) with no permanent config markers
    if not combined.strip() or not response.parsed_text.strip() or response.exit_code < 0:
        return "transient"
    return "permanent"


def is_transport_failure_before_response(
    response: ProviderResponse,
    *,
    max_duration_seconds: int,
    git_tree_changed: bool,
    proxy_route_switching: bool = False,
) -> bool:
    """Return True if the call failed before producing work and qualifies for a free transport retry."""
    if (
        response.exit_code == 0
        or response.timed_out
        or response.idle_timed_out
        or response.rate_limited
    ):
        return False
    if classify_provider_failure(response) == "permanent":
        return False
    if response.duration_seconds >= max_duration_seconds:
        return False
    if git_tree_changed:
        return False
    if proxy_route_switching:
        return True
    combined = f"{response.raw_stderr}\n{response.raw_stdout}"
    if _TRANSIENT_ERROR_RE.search(combined):
        return True
    return (not response.received_meaningful_output) and (
        not is_meaningful_output(response.raw_stdout)
    )


class OrchestratorFatalError(RuntimeError):
    """Unrecoverable internal/environment error requiring immediate fail-fast exit."""


class RunnerLockError(OrchestratorFatalError):
    """Raised when another active orchestrator process holds `.pipeline/runner.lock`."""


class _AbortedByControlError(Exception):
    """Internal flow-control signal raised when an operator sends `abort`."""


class _TaskDeferredOnCooldownError(Exception):
    """Internal signal to return a clean worker task to TODO when another task can run sooner."""

    def __init__(
        self,
        *,
        task_id: str,
        role: AgentRole,
        providers: list[str],
        wait_seconds: int,
    ) -> None:
        super().__init__(f"Task #{task_id} deferred on cooldown ({wait_seconds}s)")
        self.task_id = task_id
        self.role = role
        self.providers = providers
        self.wait_seconds = wait_seconds


class _PausedByInfraError(Exception):
    """Raised when a review/architect/planner/proxy monitor fails so work is preserved."""

    def __init__(
        self,
        reason: str,
        *,
        pause_reason: str | None = None,
        task_id: str | None = None,
        role: AgentRole | None = None,
        provider: str | None = None,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.pause_reason = pause_reason or reason
        self.task_id = task_id
        self.role = role
        self.provider = provider


class OrchestratorRunner:
    """Headless event-driven AFK pipeline runner."""

    def __init__(
        self,
        *,
        repo_root: Path,
        config: PipelineConfig,
        store: BaseTaskStore,
        registry: ProviderRegistry,
        event_bus: EventBus | None = None,
        git_ops: GitOps | None = None,
        gate_runner: GateRunner | None = None,
        notifier: BaseNotifier | None = None,
        proxy_monitor: BaseProxyMonitor | None = None,
        sleep_fn: Any = None,
        jitter_fn: Callable[[float], float] | None = None,
        quiet: bool = False,
        console_stream: TextIO | None = None,
    ) -> None:
        self.repo_root = repo_root.resolve()
        self.config = config
        self.pipeline_dir = config.pipeline_path(self.repo_root)
        self.pipeline_dir.mkdir(parents=True, exist_ok=True)

        self.store = store
        self.registry = registry
        self.quiet = quiet
        console_callback = None if quiet else make_console_event_printer(console_stream)
        if event_bus is not None:
            self.event_bus = event_bus
        else:
            self.event_bus = EventBus(
                self.pipeline_dir,
                config,
                defer_save=True,
                on_event=console_callback,
            )
        self.git_ops = git_ops or GitOps(self.repo_root, pipeline_dir=config.project.pipeline_dir)
        self.gate_runner = gate_runner or GateRunner(self.repo_root, config)
        self.prompt_builder = PromptBuilder(self.repo_root, config)
        self.control_plane = ControlPlane(self.pipeline_dir)
        self.inbox_manager = InboxManager(self.pipeline_dir)
        self.notifier = notifier or build_notifier(config.notifications)
        self.proxy_monitor: BaseProxyMonitor | None = (
            proxy_monitor
            if proxy_monitor is not None
            else build_proxy_monitor(config.proxy_monitor)
        )
        self._sleep = sleep_fn or time.sleep
        self._jitter: Callable[[float], float] = jitter_fn or (
            lambda d: max(0.0, d * random.uniform(0.85, 1.15)) if d > 0 else 0.0
        )

        self.repo_map_path = self.pipeline_dir / "REPO_MAP.md"
        self.progress_path = self.pipeline_dir / "PROGRESS.md"
        self.report_path = self.pipeline_dir / "REPORT.md"
        self.lock_path = self.pipeline_dir / "runner.lock"
        self.runs_dir = self.pipeline_dir / "runs" / self.event_bus.run_id

        self._lock_acquired = False
        self._abort_requested = False
        self._batch_diffs: dict[str, str] = {}
        self._batch_diff_stats: dict[str, str] = {}
        self._attempt_counter: dict[str, int] = {}
        self._last_proxy_key: tuple[str, bool, bool, str | None] | None = None
        self._resumable_needs_review_base_sha: dict[str, str] = {}

    def _acquire_runner_lock(self) -> dict[str, Any] | None:
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

            if old_pid is not None and is_process_alive(old_pid):
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

    def _release_runner_lock(self) -> None:
        if not self._lock_acquired:
            return
        self._lock_acquired = False
        with contextlib.suppress(OSError):
            if self.lock_path.exists():
                self.lock_path.unlink()

    def run(self, *, max_tasks: int | None = None) -> RunStatus:
        """Execute the AFK pipeline loop until all eligible tasks finish, pause, abort, or fail-fast."""
        # Acquire PID lock BEFORE touching state.json or git so a second launch cannot corrupt a live run.
        stale_lock = self._acquire_runner_lock()

        try:
            if stale_lock is not None:
                self.event_bus.emit(
                    EventType.STALE_LOCK_RECOVERED,
                    payload=stale_lock,
                )

            self._recover_or_preflight(stale_lock_recovered=stale_lock is not None)
            self._ensure_progress_file()
            self._sync_batch_queue_from_store()

            self.event_bus.update_state(
                status=RunStatus.RUNNING,
                active_task_id=None,
                active_task_base_sha=None,
                active_phase="startup",
                active_role=None,
                active_provider=None,
                paused=False,
                pause_reason=None,
                resume_at=None,
            )
            self.event_bus.emit(
                EventType.RUN_STARTED,
                payload={"project": self.config.project.name, "max_tasks": max_tasks},
            )
            todo_count, done_count, total_active = self._task_progress_counts()
            max_suffix = f", max_tasks={max_tasks}" if max_tasks is not None else ""
            self.notifier.notify(
                "run_started",
                f"Pipeline Started ({self.config.project.name})",
                f"Run {self.event_bus.run_id}: {todo_count} todo, {done_count}/{total_active} done{max_suffix}.",
            )

            tasks_processed = 0
            last_stage: str | None = None

            while True:
                self._process_control_commands()
                if self._abort_requested:
                    raise _AbortedByControlError()

                if self.event_bus.state.pause_after_current or self.event_bus.state.paused:
                    self._wait_while_paused(reason="operator_pause")
                    if self._abort_requested:
                        raise _AbortedByControlError()

                if max_tasks is not None and tasks_processed >= max_tasks:
                    self._flush_batch_review()
                    break

                # Resume any task left in ARCH_PENDING after a prior crash before picking new work
                arch_pending = [
                    t for t in self.store.list_tasks() if t.status == TaskStatus.ARCH_PENDING
                ]
                if arch_pending:
                    self._resume_arch_pending_task(arch_pending[0])
                    continue

                # Resume any task checkpointed in NEEDS_REVIEW (e.g. after paused_infra on pre_reviewer/reviewer)
                needs_review = [
                    t for t in self.store.list_tasks() if t.status == TaskStatus.NEEDS_REVIEW
                ]
                if needs_review:
                    if self._resume_needs_review_task(needs_review[0]):
                        tasks_processed += 1
                    continue

                next_task, woke_from_hibernation = self._select_next_runnable_task()
                if woke_from_hibernation:
                    continue

                if next_task is None:
                    if self._get_batch_pending_tasks():
                        self._flush_batch_review()
                        continue
                    # Stage / run complete: check periodic/end-of-stage refactoring
                    if (
                        self.config.workflow.refactor_on_stage_complete
                        and self.event_bus.state.accepted_since_last_refactor > 0
                    ):
                        self._run_refactorer(reason="stage_or_queue_complete")
                    break

                # Stage boundary detection -> flush batch & optional end-of-stage refactor
                if last_stage is not None and next_task.stage != last_stage:
                    if self._get_batch_pending_tasks():
                        self._flush_batch_review()
                        continue
                    if (
                        self.config.workflow.refactor_on_stage_complete
                        and self.event_bus.state.accepted_since_last_refactor > 0
                    ):
                        self._run_refactorer(reason=f"stage_complete:{last_stage}")

                # Ensure batch independence BEFORE starting the next task
                pending_batch = self._get_batch_pending_tasks()
                if pending_batch and self._must_flush_batch_before(next_task, pending_batch):
                    self._flush_batch_review()
                    continue

                last_stage = next_task.stage
                executed = self._execute_task(next_task)
                if not executed:
                    continue
                tasks_processed += 1

                # Flush batch if batch size threshold reached
                if len(self._get_batch_pending_tasks()) >= self.config.workflow.batch_review_size:
                    self._flush_batch_review()

                # Trigger scheduled refactor every N accepted tasks
                refactor_n = self.config.workflow.refactor_every_n_tasks
                if (
                    refactor_n > 0
                    and self.event_bus.state.accepted_since_last_refactor >= refactor_n
                ):
                    if self._get_batch_pending_tasks():
                        self._flush_batch_review()
                    if self.event_bus.state.accepted_since_last_refactor >= refactor_n:
                        self._run_refactorer(
                            reason=f"every_{refactor_n}_accepted_tasks",
                        )

            self.event_bus.update_state(
                status=RunStatus.COMPLETED,
                active_task_id=None,
                active_task_base_sha=None,
                active_phase="completed",
                active_role=None,
                active_provider=None,
                paused=False,
                pause_reason=None,
                resume_at=None,
            )
            self.event_bus.emit(EventType.RUN_FINISHED, payload={"tasks_processed": tasks_processed})
            self._generate_report()
            self.notifier.notify(
                "run_finished",
                f"Pipeline Completed ({self.config.project.name})",
                f"Run {self.event_bus.run_id} finished ({self._task_summary_line()}).\n"
                f"Report: {self.report_path}",
            )
            return RunStatus.COMPLETED

        except _PausedByInfraError as exc:
            self.event_bus.update_state(
                status=RunStatus.PAUSED,
                active_phase="paused_infra",
                paused=True,
                pause_reason=exc.pause_reason,
                resume_at=None,
            )
            self.event_bus.emit(
                EventType.PAUSE_ENTERED,
                task_id=exc.task_id,
                role=exc.role,
                provider=exc.provider,
                payload={"reason": exc.pause_reason, "details": exc.reason},
            )
            self._generate_report()
            self.notifier.notify(
                "pause",
                f"Pipeline Paused on Provider Error ({self.config.project.name})",
                f"Run {self.event_bus.run_id} paused without reverting approved/pending code: {exc.reason}",
            )
            return RunStatus.PAUSED

        except _AbortedByControlError:
            self._cleanup_aborted_run()
            self.event_bus.update_state(
                status=RunStatus.ABORTED,
                active_task_id=None,
                active_task_base_sha=None,
                active_phase="aborted",
                active_role=None,
                active_provider=None,
                paused=False,
                pause_reason=None,
                resume_at=None,
            )
            self.event_bus.emit(EventType.RUN_ABORTED, payload={"reason": "control_abort"})
            self._generate_report()
            self.notifier.notify(
                "run_finished",
                f"Pipeline Aborted ({self.config.project.name})",
                f"Run {self.event_bus.run_id} was aborted via control plane ({self._task_summary_line()}).\n"
                f"Report: {self.report_path}",
            )
            return RunStatus.ABORTED

        except KeyboardInterrupt:
            with contextlib.suppress(Exception):
                self.event_bus.update_state(
                    status=RunStatus.ABORTED,
                    active_phase="aborted_by_user",
                    active_role=None,
                    active_provider=None,
                    paused=False,
                    pause_reason="keyboard_interrupt",
                    resume_at=None,
                )
            with contextlib.suppress(Exception):
                self.event_bus.emit(
                    EventType.RUN_ABORTED,
                    task_id=self.event_bus.state.active_task_id,
                    payload={"reason": "keyboard_interrupt"},
                )
            with contextlib.suppress(Exception):
                self.notifier.notify(
                    "run_finished",
                    f"Pipeline Interrupted ({self.config.project.name})",
                    f"Run {self.event_bus.run_id} was stopped manually via Ctrl+C "
                    f"({self._task_summary_line()}).",
                )
            return RunStatus.ABORTED

        except Exception as exc:
            # Fail-fast: NEVER attempt orchestrator self-repair
            self.event_bus.emit(
                EventType.ORCHESTRATOR_FATAL_ERROR,
                task_id=self.event_bus.state.active_task_id,
                role=self.event_bus.state.active_role,
                provider=self.event_bus.state.active_provider,
                payload={"error_type": type(exc).__name__, "message": str(exc)},
            )
            self.event_bus.update_state(
                status=RunStatus.FAILED,
                active_phase="fatal_error",
            )
            with contextlib.suppress(Exception):
                self._generate_report()
            self.notifier.notify(
                "fatal_error",
                f"Orchestrator Fatal Error ({self.config.project.name})",
                f"{type(exc).__name__}: {exc}",
            )
            raise
        finally:
            self._release_runner_lock()

    def _task_progress_counts(self) -> tuple[int, int, int]:
        """Return (todo_count, completed_count, active_total_count) excluding split parent placeholders."""
        tasks = self.store.list_tasks()
        active_tasks = [t for t in tasks if t.resolution != "split"]
        todo_count = sum(1 for t in active_tasks if t.status == TaskStatus.TODO)
        done_count = sum(1 for t in active_tasks if t.status == TaskStatus.DONE)
        return todo_count, done_count, len(active_tasks)

    def _task_summary_line(self) -> str:
        tasks = [t for t in self.store.list_tasks() if t.resolution != "split"]
        done = sum(1 for t in tasks if t.status == TaskStatus.DONE)
        failed = sum(1 for t in tasks if t.status == TaskStatus.FAILED)
        blocked = sum(1 for t in tasks if t.status == TaskStatus.BLOCKED)
        remaining = len(tasks) - done - failed - blocked
        return f"done={done}, failed={failed}, blocked={blocked}, remaining={remaining}"

    def _cleanup_aborted_run(self) -> None:
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

    def _recover_or_preflight(self, *, stale_lock_recovered: bool) -> None:
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

    def _ensure_progress_file(self) -> None:
        if not self.progress_path.exists():
            atomic_write_text(
                self.progress_path,
                "# Pipeline Progress Handoff (PROGRESS.md)\n\n"
                "Completed milestones, module summaries, and architectural notes.\n",
            )

    def _read_progress_text(self) -> str:
        if not self.progress_path.exists():
            return ""
        return normalize_newlines(self.progress_path.read_text(encoding="utf-8", errors="replace"))

    def _append_progress_entry(self, heading: str, body: str) -> None:
        self._ensure_progress_file()
        existing = self._read_progress_text().rstrip()
        entry = f"\n\n## {heading} ({utc_now_iso()})\n\n{normalize_newlines(body).strip()}\n"
        atomic_write_text(self.progress_path, existing + entry)

    def _refresh_repo_map(self) -> str:
        content = generate_repo_map(
            self.repo_root,
            self.config.project.repo_map_ignore,
            pipeline_dir=self.config.project.pipeline_dir,
        )
        atomic_write_text(self.repo_map_path, content)
        return content

    def _generate_report(self) -> Path:
        path = write_report(
            self.report_path,
            project_name=self.config.project.name,
            state=self.event_bus.state,
            tasks=self.store.list_tasks(),
        )
        self.event_bus.emit(EventType.REPORT_GENERATED, payload={"path": str(path)})
        return path

    def _next_attempt_dir(self, scope_id: str) -> tuple[int, Path]:
        count = self._attempt_counter.get(scope_id, 0) + 1
        self._attempt_counter[scope_id] = count
        attempt_dir = self.runs_dir / scope_id / f"attempt_{count}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        return count, attempt_dir

    def _sync_batch_queue_from_store(self) -> None:
        pending_ids = [t.id for t in self.store.list_tasks() if t.status == TaskStatus.BATCH_PENDING]
        self.event_bus.update_state(batch_queue=pending_ids)

    def _get_batch_pending_tasks(self) -> list[Task]:
        return [t for t in self.store.list_tasks() if t.status == TaskStatus.BATCH_PENDING]

    def _must_flush_batch_before(self, next_task: Task, pending_batch: list[Task]) -> bool:
        if next_task.complexity != TaskComplexity.EASY:
            return True
        batch_ids = {t.id for t in pending_batch}
        if self.store.depends_on_any(next_task.id, batch_ids):
            return True
        # Check potential file overlap between next_task.allowed_paths and batch changed_files
        batch_files: set[str] = set()
        for b_task in pending_batch:
            batch_files.update(b_task.changed_files)
        if not next_task.allowed_paths:
            return True
        return any(matches_any_glob(bf, next_task.allowed_paths) for bf in batch_files)

    # -------------------------------------------------------------------------
    # Control Plane & Pause Handling
    # -------------------------------------------------------------------------

    def _process_control_commands(self) -> list[ControlCommand]:
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

    def _check_control_after_step(self) -> None:
        """Process control commands immediately after a provider step, honoring abort & pause_after_current."""
        self._process_control_commands()
        if self._abort_requested:
            raise _AbortedByControlError()
        if self.event_bus.state.pause_after_current or self.event_bus.state.paused:
            self._wait_while_paused(reason="operator_pause")
            if self._abort_requested:
                raise _AbortedByControlError()

    def _wait_while_paused(self, *, reason: str) -> None:
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
            self._process_control_commands()
        if not self._abort_requested:
            self.event_bus.update_state(status=RunStatus.RUNNING)

    def _hibernate_until(
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
            self._process_control_commands()
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

    def _wait_backoff(
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
            self._process_control_commands()
            if self._abort_requested:
                raise _AbortedByControlError()
            if self.event_bus.state.pause_after_current or self.event_bus.state.paused:
                self._wait_while_paused(reason="operator_pause")
                if self._abort_requested:
                    raise _AbortedByControlError()
            step = min(poll, delay_seconds - slept_total)
            if step <= 0:
                break
            self._sleep(step)
            slept_total += step
        self._process_control_commands()
        if self._abort_requested:
            raise _AbortedByControlError()

    # -------------------------------------------------------------------------
    # Provider Fallback, Account Switching & Proxy Monitor Helpers
    # -------------------------------------------------------------------------

    def _candidate_providers_for_role(self, role: AgentRole) -> list[str]:
        """Return ordered candidate profile names [primary, *compatible_fallbacks] for a role."""
        primary = self.registry.profile_name_for_role(role)
        raw_fallbacks: list[str] = [
            *self.config.fallback_providers.get(role, []),
            *self.config.providers[primary].fallback_providers,
        ]
        ordered: list[str] = [primary]
        for fb_name in raw_fallbacks:
            if fb_name in ordered:
                continue
            if fb_name not in self.config.providers:
                continue
            fb_profile = self.config.providers[fb_name]
            if role in WRITING_ROLES and not fb_profile.can_edit_files:
                continue
            ordered.append(fb_name)
        return ordered

    def _select_available_provider_for_role(
        self,
        role: AgentRole,
        *,
        exclude: set[str] | None = None,
    ) -> tuple[str, int, list[str]]:
        """Return (chosen_provider_name, wait_seconds, all_candidates)."""
        candidates = self._candidate_providers_for_role(role)
        skip = exclude or set()
        for cand in candidates:
            if cand in skip:
                continue
            if self.event_bus.quota_tracker.seconds_until_available(cand) == 0:
                return cand, 0, candidates

        best_name = candidates[0]
        best_wait = self.event_bus.quota_tracker.seconds_until_available(best_name)
        for cand in candidates[1:]:
            wait = self.event_bus.quota_tracker.seconds_until_available(cand)
            if wait < best_wait:
                best_name = cand
                best_wait = wait
        return best_name, best_wait, candidates

    def _list_available_todo_tasks(self) -> list[Task]:
        """Return all unlocked TODO tasks in deterministic queue order."""
        available: list[Task] = []
        seen: set[str] = set()
        while True:
            candidate = self.store.get_next_available_task(exclude_ids=seen)
            if candidate is None or candidate.id in seen:
                break
            available.append(candidate)
            seen.add(candidate.id)
        return available

    def _should_defer_task_on_cooldown(self, current_task_id: str, current_min_wait: int) -> bool:
        """Return True if another unlocked TODO task can run now or wakes sooner than current_min_wait."""
        for candidate in self._list_available_todo_tasks():
            if candidate.id == current_task_id:
                continue
            cand_role = self._worker_role_for(candidate.complexity)
            _, cand_wait, _ = self._select_available_provider_for_role(cand_role)
            if cand_wait == 0 or cand_wait < current_min_wait:
                return True
        return False

    def _select_next_runnable_task(self) -> tuple[Task | None, bool]:
        """Pick the first unlocked TODO task with an available provider, or hibernate until earliest cooldown.

        Returns (selected_task_or_none, woke_from_hibernation).
        """
        available = self._list_available_todo_tasks()
        if not available:
            return None, False

        skipped: list[tuple[Task, AgentRole, list[str], int]] = []
        for candidate in available:
            worker_role = self._worker_role_for(candidate.complexity)
            _, wait_seconds, candidates = self._select_available_provider_for_role(worker_role)
            if wait_seconds == 0:
                for skipped_task, skipped_role, skipped_candidates, skipped_wait in skipped:
                    self.event_bus.emit(
                        EventType.TASK_DEFERRED_COOLDOWN,
                        task_id=skipped_task.id,
                        role=skipped_role,
                        provider=skipped_candidates[0] if skipped_candidates else None,
                        payload={
                            "complexity": skipped_task.complexity.value,
                            "providers": skipped_candidates,
                            "wait_seconds": skipped_wait,
                            "selected_task_id": candidate.id,
                            "reason": "all_role_providers_in_cooldown",
                        },
                    )
                return candidate, False
            skipped.append((candidate, worker_role, candidates, wait_seconds))

        # No unlocked TODO task can run right now -> hibernate until the earliest cooldown_until
        earliest_task, earliest_role, _, min_wait = min(skipped, key=lambda item: item[3])
        all_blocked_providers: list[str] = []
        for _, _, cands, _ in skipped:
            for pname in cands:
                if pname not in all_blocked_providers:
                    all_blocked_providers.append(pname)

        resume_dt = datetime.now(UTC) + timedelta(seconds=max(1, min_wait))
        self._hibernate_until(
            resume_dt=resume_dt,
            providers=all_blocked_providers,
            task_id=earliest_task.id,
            role=earliest_role,
        )
        if self._abort_requested:
            raise _AbortedByControlError()
        return None, True

    def _poll_proxy_monitor_for_provider(
        self,
        provider: BaseAgentProvider,
        *,
        task_id: str | None,
        role: AgentRole,
    ) -> ProxyHealthStatus | None:
        """Read external proxy monitor status if enabled & applicable, persisting state and alerts."""
        if self.proxy_monitor is None:
            return None
        if not self.proxy_monitor.applies_to(provider.name, provider.profile):
            return None

        status = self.proxy_monitor.read_status()
        self.event_bus.update_state(proxy_health_status=status)

        key = (
            status.state,
            status.healthy,
            status.requires_manual_action,
            status.last_message,
        )
        prev_key = self._last_proxy_key
        if key != prev_key:
            self._last_proxy_key = key
            payload = {
                "state": status.state,
                "healthy": status.healthy,
                "proxy_reachable": status.proxy_reachable,
                "active_route": status.active_route,
                "route_switching": status.route_switching,
                "requires_manual_action": status.requires_manual_action,
                "message": status.fatal_reason or status.last_message or "",
                "blockers": status.blockers,
            }
            if not status.healthy or status.requires_manual_action:
                self.event_bus.emit(
                    EventType.PROXY_ERROR,
                    task_id=task_id,
                    role=role,
                    provider=provider.name,
                    payload=payload,
                )
                self.notifier.notify(
                    "proxy",
                    f"Proxy Monitor Error ({self.config.project.name})",
                    f"State: {status.state}\n"
                    f"Message: {status.fatal_reason or status.last_message or 'Proxy unhealthy'}\n"
                    f"Route: {status.active_route or '-'}",
                )
            else:
                self.event_bus.emit(
                    EventType.PROXY_STATUS_CHANGED,
                    task_id=task_id,
                    role=role,
                    provider=provider.name,
                    payload=payload,
                )
                if prev_key is not None and not prev_key[1] and status.healthy:
                    self.notifier.notify(
                        "proxy",
                        f"Proxy Monitor Recovered ({self.config.project.name})",
                        f"Proxy monitor recovered (state={status.state}, route={status.active_route or '-'}).",
                    )
        return status

    def _ensure_provider_account(
        self,
        provider: BaseAgentProvider,
        *,
        task_id: str | None,
        role: AgentRole,
    ) -> None:
        """Run profile.switch_command when account_group/account_profile differs from active_accounts."""
        group = provider.profile.account_group
        target_profile = provider.profile.account_profile
        switch_cmd = provider.profile.switch_command
        if not group or not target_profile:
            return

        current_profile = self.event_bus.state.active_accounts.get(group)
        if current_profile == target_profile:
            return

        try:
            res = run_noninteractive_process(
                switch_cmd,
                cwd=self.repo_root,
                timeout_seconds=30,
            )
            exit_code = res.exit_code
            timed_out = res.timed_out
            stderr_text = res.stderr.strip() or res.stdout.strip()
        except OSError as exc:
            exit_code = -1
            timed_out = False
            stderr_text = str(exc)

        if exit_code == 0 and not timed_out:
            updated_accounts = {**self.event_bus.state.active_accounts, group: target_profile}
            self.event_bus.update_state(active_accounts=updated_accounts)
            self.event_bus.emit(
                EventType.PROVIDER_ACCOUNT_SWITCHED,
                task_id=task_id,
                role=role,
                provider=provider.name,
                payload={
                    "account_group": group,
                    "from_profile": current_profile,
                    "to_profile": target_profile,
                    "command": list(switch_cmd),
                },
            )
            return

        self.event_bus.emit(
            EventType.PROVIDER_ACCOUNT_SWITCH_FAILED,
            task_id=task_id,
            role=role,
            provider=provider.name,
            payload={
                "account_group": group,
                "from_profile": current_profile,
                "to_profile": target_profile,
                "command": list(switch_cmd),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "stderr": stderr_text[:400],
            },
        )
        self.notifier.notify(
            "account_switch_failed",
            f"Account Switch Failed ({self.config.project.name})",
            f"Failed switching account group '{group}' to '{target_profile}' "
            f"for provider '{provider.name}' (exit={exit_code}, timed_out={timed_out}): "
            f"{stderr_text[:300]}",
        )
        raise OrchestratorFatalError(
            f"Account switch failed for group '{group}' -> '{target_profile}' "
            f"(provider '{provider.name}', exit={exit_code}, timed_out={timed_out}): {stderr_text[:300]}"
        )

    def _make_first_output_handler(
        self,
        *,
        task_id: str | None,
        role: AgentRole,
        provider_name: str,
        transport_retries: int,
        pipeline_snapshot: dict[str, str],
    ) -> tuple[list[bool], Callable[[str], None]]:
        fired = [False]

        def _on_first_output(chunk: str) -> None:
            if fired[0]:
                return
            fired[0] = True
            self.event_bus.update_state(
                active_task_id=task_id,
                active_phase=f"streaming:{role.value}",
                active_role=role,
                active_provider=provider_name,
            )
            self.event_bus.emit(
                EventType.PROVIDER_RESPONSE_STARTED,
                task_id=task_id,
                role=role,
                provider=provider_name,
                payload={"first_chunk_chars": len(chunk)},
            )
            target_label = task_id or role.value
            if transport_retries > 0:
                self.event_bus.emit(
                    EventType.PROVIDER_RECOVERED_AFTER_RETRY,
                    task_id=task_id,
                    role=role,
                    provider=provider_name,
                    payload={"transport_retries": transport_retries},
                )
                self.notifier.notify(
                    "provider_recovered",
                    f"Provider Recovered ({self.config.project.name})",
                    f"[{provider_name}] восстановился после {transport_retries} "
                    f"ретраев, начал отвечать по {target_label}",
                )
            else:
                self.notifier.notify(
                    "provider_started",
                    f"Provider Started ({self.config.project.name})",
                    f"[{provider_name}] начал отвечать по {target_label} ({role.value})",
                )
            # Refresh expected hashes for orchestrator-written state.json / events.jsonl
            # so Integrity Guard does not flag our own streaming telemetry updates as agent tampering.
            for own_path in (self.event_bus.state_path, self.event_bus.events_path):
                if own_path.is_file():
                    rel = own_path.relative_to(self.pipeline_dir).as_posix()
                    with contextlib.suppress(OSError):
                        pipeline_snapshot[rel] = sha256_file(own_path)

        return fired, _on_first_output

    def _snapshot_git_tree_state(self, base_sha: str) -> tuple[str, list[str], str]:
        """Capture lightweight git state (HEAD, untracked files, working diff) to detect modifications."""
        head_sha = self.git_ops.get_head_sha()
        untracked = self.git_ops.list_untracked_files()
        diff_text = self.git_ops.get_diff(base_sha)
        return head_sha, untracked, diff_text

    # -------------------------------------------------------------------------
    # Guarded Provider Invocation (base_sha, untracked add -N, Integrity Guard)
    # -------------------------------------------------------------------------

    def _invoke_provider_guarded(
        self,
        *,
        role: AgentRole,
        built_prompt: BuiltPrompt,
        task_id: str | None,
        attempt_dir: Path,
        base_sha: str,
        task_complexity: TaskComplexity | None = None,
        allowed_paths: list[str] | None = None,
        preserve_existing_diff_for_readonly: bool = False,
    ) -> tuple[ProviderResponse, DiffGuardReport]:
        primary_name = self.registry.profile_name_for_role(role)
        chosen_name, pre_wait, candidates = self._select_available_provider_for_role(role)

        if pre_wait > 0:
            resume_dt = datetime.now(UTC) + timedelta(seconds=pre_wait)
            self._hibernate_until(
                resume_dt=resume_dt,
                providers=candidates,
                task_id=task_id,
                role=role,
            )
            if self._abort_requested:
                raise _AbortedByControlError()
            chosen_name, _, candidates = self._select_available_provider_for_role(role)
        elif chosen_name != primary_name:
            self.event_bus.emit(
                EventType.PROVIDER_FALLBACK_USED,
                task_id=task_id,
                role=role,
                provider=chosen_name,
                payload={
                    "from_provider": primary_name,
                    "to_provider": chosen_name,
                    "reason": "quota_or_cooldown",
                },
            )

        provider = self.registry.get(chosen_name)
        attempt_dir.mkdir(parents=True, exist_ok=True)

        # For read-only roles running on a dirty working tree (worker edits awaiting review),
        # snapshot the exact git diff/status beforehand so we can detect ANY read-only mutation.
        pre_head_sha = self.git_ops.get_head_sha()
        pre_changes: list[GitChange] = []
        pre_diff = ""
        if role in READ_ONLY_ROLES and preserve_existing_diff_for_readonly:
            self.git_ops.mark_untracked_intent_to_add()
            pre_changes = self.git_ops.get_name_status(base_sha)
            pre_diff = self.git_ops.get_diff(base_sha)

        expected_head = (
            pre_head_sha
            if (role in READ_ONLY_ROLES and preserve_existing_diff_for_readonly)
            else base_sha
        )
        retry_delays = list(self.config.workflow.transient_retry_delays_seconds)
        transport_delays = list(self.config.workflow.transport_retry_delays_seconds)
        max_transport_retries = self.config.workflow.max_transport_retries
        transport_retries = 0
        transient_attempt = 0
        readonly_timeout_retries = 0

        while True:
            # Proxy monitor preflight before invoking governed providers
            pre_proxy = self._poll_proxy_monitor_for_provider(
                provider,
                task_id=task_id,
                role=role,
            )
            if pre_proxy is not None and pre_proxy.requires_manual_action:
                raise _PausedByInfraError(
                    reason=(
                        pre_proxy.fatal_reason
                        or pre_proxy.last_message
                        or f"Proxy monitor requires manual action ({pre_proxy.state})"
                    ),
                    pause_reason="proxy_error",
                    task_id=task_id,
                    role=role,
                    provider=provider.name,
                )

            # Switch account profile if configured and differs from active_accounts
            self._ensure_provider_account(provider, task_id=task_id, role=role)

            self.event_bus.update_state(
                active_task_id=task_id,
                active_phase=f"provider:{role.value}",
                active_role=role,
                active_provider=provider.name,
            )
            self.event_bus.emit(
                EventType.PROVIDER_INVOKED,
                task_id=task_id,
                role=role,
                provider=provider.name,
                payload={
                    "static_prefix_sha256": built_prompt.static_prefix_sha256,
                    "prompt_chars": len(built_prompt.full_text),
                    "attempt_dir": str(attempt_dir),
                    "transport_retry": transport_retries,
                },
            )

            # Snapshot git working tree before invocation to detect zero-edit transport failures
            git_before_sig = self._snapshot_git_tree_state(base_sha)

            # Integrity Guard snapshot of protected `.pipeline/` files BEFORE agent runs
            pipeline_snapshot_before = snapshot_directory_hashes(
                self.pipeline_dir,
                exclude=PIPELINE_INTEGRITY_EXCLUDES,
            )

            first_output_fired, _on_first_output = self._make_first_output_handler(
                task_id=task_id,
                role=role,
                provider_name=provider.name,
                transport_retries=transport_retries,
                pipeline_snapshot=pipeline_snapshot_before,
            )

            dynamic_body = "\n\n===\n\n".join(
                part
                for part in (built_prompt.semi_static_layer, built_prompt.dynamic_tail)
                if part
            )
            idle_check_cpu = (
                provider.profile.idle_check_process_cpu
                if provider.profile.idle_check_process_cpu is not None
                else self.config.workflow.idle_check_process_cpu
            )
            request = ProviderRequest(
                run_id=self.event_bus.run_id,
                task_id=task_id,
                role=role,
                prompt=built_prompt.full_text,
                static_prefix=built_prompt.static_prefix,
                dynamic_body=dynamic_body,
                static_prefix_sha256=built_prompt.static_prefix_sha256,
                workdir=self.repo_root,
                timeout_seconds=provider.profile.timeout_seconds,
                idle_timeout_seconds=provider.profile.idle_timeout_seconds,
                idle_check_process_cpu=idle_check_cpu,
                attempt_dir=attempt_dir,
                on_first_output=_on_first_output,
            )

            # FileNotFoundError is a permanent configuration error -> fail-fast without retry
            response = provider.invoke(request)

            # Emit provider_response_started / recovered for non-streaming providers (API/mock)
            if (
                not first_output_fired[0]
                and response.exit_code == 0
                and not response.timed_out
                and (response.received_meaningful_output or is_meaningful_output(response.raw_stdout))
            ):
                _on_first_output(response.raw_stdout[:120])

            # Integrity Guard check AFTER agent runs (agents must NEVER touch protected `.pipeline/` files)
            pipeline_snapshot_after = snapshot_directory_hashes(
                self.pipeline_dir,
                exclude=PIPELINE_INTEGRITY_EXCLUDES,
            )
            tamper_changes = diff_directory_snapshots(
                pipeline_snapshot_before,
                pipeline_snapshot_after,
            )
            git_after_sig = self._snapshot_git_tree_state(base_sha)
            git_tree_changed = git_after_sig != git_before_sig

            if response.rate_limited:
                cooldown = response.retry_after_seconds or provider.profile.rate_limit_cooldown_seconds
                self.event_bus.quota_tracker.record_rate_limit(
                    provider.name,
                    cooldown_seconds=cooldown,
                )
                self.event_bus.save_state()
                self.event_bus.emit(
                    EventType.PROVIDER_RATE_LIMITED,
                    task_id=task_id,
                    role=role,
                    provider=provider.name,
                    payload={"cooldown_seconds": cooldown, "stderr": response.raw_stderr[:400]},
                )
                self._check_control_after_step()

                # Try fallback provider first before hibernating
                next_name, next_wait, all_candidates = self._select_available_provider_for_role(
                    role,
                    exclude={provider.name},
                )
                if next_wait == 0 and next_name != provider.name:
                    prev_name = provider.name
                    provider = self.registry.get(next_name)
                    self.event_bus.emit(
                        EventType.PROVIDER_FALLBACK_USED,
                        task_id=task_id,
                        role=role,
                        provider=provider.name,
                        payload={
                            "from_provider": prev_name,
                            "to_provider": provider.name,
                            "reason": "rate_limit",
                        },
                    )
                    continue

                # All candidates are in cooldown/quota-capped -> defer to another runnable task if possible
                # (only defer mid-task for meaningful cooldowns >= 10s; tiny 1-9s waits complete in place)
                _, min_wait, all_candidates = self._select_available_provider_for_role(role)
                if (
                    role in WORKER_ROLES
                    and task_id is not None
                    and min_wait >= 10
                    and not git_tree_changed
                    and not self.git_ops.has_uncommitted_changes()
                    and self.git_ops.get_head_sha() == base_sha
                    and self._should_defer_task_on_cooldown(task_id, min_wait)
                ):
                    raise _TaskDeferredOnCooldownError(
                        task_id=task_id,
                        role=role,
                        providers=all_candidates,
                        wait_seconds=min_wait,
                    )

                resume_dt = datetime.now(UTC) + timedelta(seconds=max(1, min_wait))
                self._hibernate_until(
                    resume_dt=resume_dt,
                    providers=all_candidates,
                    task_id=task_id,
                    role=role,
                )
                if self._abort_requested:
                    raise _AbortedByControlError()
                avail_name, _, _ = self._select_available_provider_for_role(role)
                provider = self.registry.get(avail_name)
                continue

            # Poll proxy monitor after non-zero / timed-out call to classify fatal error vs route switch
            post_proxy: ProxyHealthStatus | None = None
            if response.exit_code != 0 or response.timed_out or response.idle_timed_out:
                post_proxy = self._poll_proxy_monitor_for_provider(
                    provider,
                    task_id=task_id,
                    role=role,
                )
                if post_proxy is not None and post_proxy.requires_manual_action:
                    raise _PausedByInfraError(
                        reason=(
                            post_proxy.fatal_reason
                            or post_proxy.last_message
                            or f"Proxy monitor error ({post_proxy.state})"
                        ),
                        pause_reason="proxy_error",
                        task_id=task_id,
                        role=role,
                        provider=provider.name,
                    )

            # Free transport retry classification (before response, no git changes, fast failure)
            route_changed = bool(
                pre_proxy is not None
                and post_proxy is not None
                and pre_proxy.active_route
                and post_proxy.active_route
                and pre_proxy.active_route != post_proxy.active_route
            )
            proxy_switching = bool(
                (pre_proxy and pre_proxy.route_switching)
                or (post_proxy and post_proxy.route_switching)
                or route_changed
            )
            transport_fail = (not tamper_changes) and is_transport_failure_before_response(
                response,
                max_duration_seconds=self.config.workflow.transport_failure_max_duration_seconds,
                git_tree_changed=git_tree_changed,
                proxy_route_switching=proxy_switching,
            )

            if transport_fail and max_transport_retries > 0:
                # Honor abort / pause_after_current BEFORE launching a transport retry
                self._check_control_after_step()
                if transport_retries < max_transport_retries:
                    base_delay = (
                        float(
                            transport_delays[min(transport_retries, len(transport_delays) - 1)]
                        )
                        if transport_delays
                        else 0.0
                    )
                    delay = round(self._jitter(base_delay), 2)
                    transport_retries += 1
                    reason_snippet = (response.raw_stderr or response.raw_stdout).strip()[:200] or (
                        f"exit_code={response.exit_code}"
                    )
                    self.event_bus.emit(
                        EventType.PROVIDER_TRANSPORT_RETRY,
                        task_id=task_id,
                        role=role,
                        provider=provider.name,
                        payload={
                            "attempt": transport_retries,
                            "max_attempts": max_transport_retries,
                            "reason": reason_snippet,
                            "delay_seconds": delay,
                            "exit_code": response.exit_code,
                            "proxy_route_switching": proxy_switching,
                        },
                    )
                    self._wait_backoff(
                        delay_seconds=delay,
                        provider_name=provider.name,
                        task_id=task_id,
                        role=role,
                        phase_prefix="transport_backoff",
                    )
                    continue

                # Exhausted free transport retries without ever getting a model response -> pause
                # without burning QuotaTracker, task.attempts, or review_rounds.
                raise _PausedByInfraError(
                    reason=(
                        f"{role.value} provider '{provider.name}' failed before response after "
                        f"{max_transport_retries} transport retries (exit_code={response.exit_code})"
                    ),
                    task_id=task_id,
                    role=role,
                    provider=provider.name,
                )

            # Record quota usage ONLY for non-transport calls (actual responses / work done)
            self.event_bus.quota_tracker.record_request(provider.name)
            self.event_bus.save_state()

            failure_kind = classify_provider_failure(response)

            if failure_kind == "timeout":
                if (
                    role in READ_ONLY_ROLES
                    and not tamper_changes
                    and readonly_timeout_retries < self.config.workflow.retry_on_timeout_readonly
                ):
                    self._check_control_after_step()
                    if self.git_ops.restore_soft_to_base(expected_head):
                        self.event_bus.emit(
                            EventType.AGENT_COMMIT_SOFT_RESET,
                            task_id=task_id,
                            role=role,
                            provider=provider.name,
                            payload={"restored_to_sha": expected_head},
                        )
                    delay = (
                        retry_delays[min(readonly_timeout_retries, len(retry_delays) - 1)]
                        if retry_delays
                        else 0
                    )
                    readonly_timeout_retries += 1
                    self.event_bus.emit(
                        EventType.PROVIDER_TRANSIENT_RETRY,
                        task_id=task_id,
                        role=role,
                        provider=provider.name,
                        payload={
                            "attempt": readonly_timeout_retries,
                            "max_attempts": self.config.workflow.retry_on_timeout_readonly,
                            "reason": "idle_timeout" if response.idle_timed_out else "readonly_timeout",
                            "delay_seconds": delay,
                        },
                    )
                    self._wait_backoff(
                        delay_seconds=delay,
                        provider_name=provider.name,
                        task_id=task_id,
                        role=role,
                    )
                    continue
                # Writing roles on timeout / idle timeout NEVER blindly loop; return upward
                break

            if failure_kind == "permanent":
                # Fail-fast without retries for 401/403/invalid CLI option/command not found
                break

            if (
                failure_kind == "transient"
                and not tamper_changes
                and transient_attempt < len(retry_delays)
            ):
                # Guard against 19-minute blind retry hangs on writing roles:
                # if a writing role ran longer than transport_failure_max_duration_seconds OR already
                # modified repository files, do NOT blindly retry here — hand control back upward.
                if role in WRITING_ROLES and (
                    response.duration_seconds
                    >= self.config.workflow.transport_failure_max_duration_seconds
                    or git_tree_changed
                ):
                    break

                self._check_control_after_step()
                if self.git_ops.restore_soft_to_base(expected_head):
                    self.event_bus.emit(
                        EventType.AGENT_COMMIT_SOFT_RESET,
                        task_id=task_id,
                        role=role,
                        provider=provider.name,
                        payload={"restored_to_sha": expected_head},
                    )
                delay = retry_delays[transient_attempt]
                transient_attempt += 1
                reason_snippet = (response.raw_stderr or response.raw_stdout).strip()[:200] or (
                    f"exit_code={response.exit_code}"
                )
                self.event_bus.emit(
                    EventType.PROVIDER_TRANSIENT_RETRY,
                    task_id=task_id,
                    role=role,
                    provider=provider.name,
                    payload={
                        "attempt": transient_attempt,
                        "max_attempts": len(retry_delays),
                        "reason": reason_snippet,
                        "delay_seconds": delay,
                        "exit_code": response.exit_code,
                    },
                )
                self._wait_backoff(
                    delay_seconds=delay,
                    provider_name=provider.name,
                    task_id=task_id,
                    role=role,
                )
                continue

            break

        # Guard #2: Detect unauthorized agent commits and soft-reset back to base_sha
        if self.git_ops.restore_soft_to_base(expected_head):
            self.event_bus.emit(
                EventType.AGENT_COMMIT_SOFT_RESET,
                task_id=task_id,
                role=role,
                provider=provider.name,
                payload={"restored_to_sha": expected_head},
            )

        # Guard #3: Mark all newly created untracked files with `git add -N`
        self.git_ops.mark_untracked_intent_to_add()
        post_changes = self.git_ops.get_name_status(base_sha)
        post_diff = self.git_ops.get_diff(base_sha)

        # Guard #4: Role-aware Diff Guard
        if role in READ_ONLY_ROLES:
            ro_ignores = self.config.diff_guard.readonly_ignore_paths
            pre_change_set = set(pre_changes) if preserve_existing_diff_for_readonly else set()
            pre_sections = (
                dict(_split_diff_sections(pre_diff)) if preserve_existing_diff_for_readonly else {}
            )
            post_sections = dict(_split_diff_sections(post_diff))

            delta_change_items = [c for c in post_changes if c not in pre_change_set]
            delta_diff_paths = [
                p for p, sec in post_sections.items() if pre_sections.get(p) != sec
            ]
            removed_diff_paths = [p for p in pre_sections if p not in post_sections]

            ignored_delta_paths: list[str] = []
            for change in delta_change_items:
                if matches_any_glob(change.path, ro_ignores):
                    ignored_delta_paths.append(change.path)
            for p in delta_diff_paths:
                if matches_any_glob(p, ro_ignores) and p not in ignored_delta_paths:
                    ignored_delta_paths.append(p)

            if ignored_delta_paths:
                # Revert/delete only read-only service artifacts (e.g. go.work.sum) without touching worker edits
                self.git_ops.discard_paths(base_sha, ignored_delta_paths)

            delta_path_set = {p for p in (*delta_diff_paths, *removed_diff_paths) if not matches_any_glob(p, ro_ignores)}
            non_ignored_changes: list[GitChange] = [
                c for c in delta_change_items if not matches_any_glob(c.path, ro_ignores)
            ]
            seen_non_ignored = {c.path for c in non_ignored_changes}
            for c in post_changes:
                if c.path in delta_path_set and c.path not in seen_non_ignored:
                    non_ignored_changes.append(c)
                    seen_non_ignored.add(c.path)

            non_ignored_diff = "".join(
                post_sections[p]
                for p in delta_diff_paths
                if not matches_any_glob(p, ro_ignores)
            )
            if not non_ignored_diff and removed_diff_paths:
                non_ignored_removed = [
                    p for p in removed_diff_paths if not matches_any_glob(p, ro_ignores)
                ]
                if non_ignored_removed and not non_ignored_changes:
                    non_ignored_changes = [
                        GitChange(status="M", path=p) for p in non_ignored_removed
                    ]

            guard_report = evaluate_diff(
                role=role,
                changes=non_ignored_changes,
                raw_diff=non_ignored_diff,
                config=self.config,
            )
        else:
            guard_report = evaluate_diff(
                role=role,
                changes=post_changes,
                raw_diff=post_diff,
                config=self.config,
                task_complexity=task_complexity,
                allowed_paths=allowed_paths,
            )

        if tamper_changes:
            for item in tamper_changes:
                guard_report.red_violations.append(
                    GuardViolation(
                        code="RED_ZONE_PIPELINE_TAMPERED",
                        path=f"{self.config.project.pipeline_dir}/{item}",
                        line=None,
                        message=f"Agent modified forbidden orchestrator directory: {item}",
                    )
                )
            guard_report.passed = False

        # Persist artifacts strictly by the orchestrator (never by the agent)
        self._write_attempt_artifacts(
            attempt_dir=attempt_dir,
            role=role,
            response=response,
            guard_report=guard_report,
        )

        if response.exit_code == 0 and not response.timed_out and not response.idle_timed_out:
            self.event_bus.emit(
                EventType.PROVIDER_SUCCEEDED,
                task_id=task_id,
                role=role,
                provider=provider.name,
                payload={
                    "duration_seconds": round(response.duration_seconds, 2),
                    "changed_files": guard_report.changed_files,
                },
            )
        else:
            self.event_bus.emit(
                EventType.PROVIDER_FAILED,
                task_id=task_id,
                role=role,
                provider=provider.name,
                payload={
                    "exit_code": response.exit_code,
                    "timed_out": response.timed_out,
                    "idle_timed_out": response.idle_timed_out,
                    "stderr_tail": response.raw_stderr[-400:],
                },
            )

        return response, guard_report

    def _write_attempt_artifacts(
        self,
        *,
        attempt_dir: Path,
        role: AgentRole,
        response: ProviderResponse,
        guard_report: DiffGuardReport,
    ) -> None:
        raw_combined = response.raw_stdout
        if response.raw_stderr:
            raw_combined = (
                f"{raw_combined}\n\n=== STDERR ===\n{response.raw_stderr}"
                if raw_combined
                else f"=== STDERR ===\n{response.raw_stderr}"
            )
        atomic_write_text(attempt_dir / "raw_output.txt", raw_combined)
        atomic_write_text(attempt_dir / "diff.patch", guard_report.filtered_diff)

        parsed_payload: dict[str, Any] = {
            "role": role.value,
            "provider": response.provider_name,
            "exit_code": response.exit_code,
            "timed_out": response.timed_out,
            "idle_timed_out": response.idle_timed_out,
            "duration_seconds": response.duration_seconds,
            "parsed_text": response.parsed_text,
            "guard_passed": guard_report.passed,
            "changed_files": guard_report.changed_files,
            "touched_yellow_zone": guard_report.touched_yellow_zone,
            "violations": [
                {"code": v.code, "path": v.path, "line": v.line, "message": v.message}
                for v in guard_report.all_violations
            ],
            "soft_limit_warnings": [
                {"code": v.code, "path": v.path, "line": v.line, "message": v.message}
                for v in guard_report.soft_limit_warnings
            ],
        }
        if role not in READ_ONLY_ROLES:
            worker_parsed = parse_worker_output(response.parsed_text)
            parsed_payload["worker_status"] = worker_parsed.status.value
            parsed_payload["progress_update"] = worker_parsed.progress_update
            parsed_payload["escalation_summary"] = worker_parsed.escalation_summary
            atomic_write_text(attempt_dir / "last_report.md", worker_parsed.report + "\n")
        else:
            atomic_write_text(attempt_dir / "last_report.md", response.parsed_text + "\n")

        atomic_write_json(attempt_dir / "parsed_output.json", parsed_payload)

    # -------------------------------------------------------------------------
    # Task Execution Lifecycle
    # -------------------------------------------------------------------------

    def _worker_role_for(self, complexity: TaskComplexity) -> AgentRole:
        if complexity == TaskComplexity.EASY:
            return AgentRole.WORKER_EASY
        if complexity == TaskComplexity.HARD:
            return AgentRole.WORKER_HARD
        return AgentRole.WORKER_MEDIUM

    def _transition_task(self, task_id: str, status: TaskStatus, **fields: Any) -> Task:
        prev = self.store.get_task(task_id).status
        updated = self.store.update_status(task_id, status, **fields)
        self.event_bus.emit(
            EventType.TASK_STATUS_CHANGED,
            task_id=task_id,
            payload={"from": prev.value, "to": status.value},
        )
        return updated

    def _execute_task(self, task: Task) -> bool:
        # Clean up lingering dev-server/test ports before starting the task
        if self.config.workflow.cleanup_ports:
            cleanup_ports(self.config.workflow.cleanup_ports)

        repo_map_text = self._refresh_repo_map()
        inbox_text = self.inbox_manager.consume(task_id=task.id)
        if inbox_text:
            self.event_bus.emit(
                EventType.INBOX_CONSUMED,
                task_id=task.id,
                payload={"chars": len(inbox_text)},
            )

        task_base_sha = self.git_ops.get_head_sha()
        worker_role = self._worker_role_for(task.complexity)
        worker_profile = self.registry.for_role(worker_role).profile

        self.event_bus.update_state(
            active_task_id=task.id,
            active_task_base_sha=task_base_sha,
            active_phase="task_claimed",
        )
        self.event_bus.emit(
            EventType.TASK_CLAIMED,
            task_id=task.id,
            role=worker_role,
            provider=self.registry.profile_name_for_role(worker_role),
            payload={"complexity": task.complexity.value, "base_sha": task_base_sha},
        )
        task = self._transition_task(task.id, TaskStatus.IN_PROGRESS)

        # Carry over previous rejection comments (e.g., if returned from batch review)
        prior_rejections = [c.body for c in task.comments if c.kind == "rejection"]
        review_feedback: str | None = prior_rejections[-1] if prior_rejections else None
        prior_arch = [c.body for c in task.comments if c.kind == "architect"]
        architect_notes: str | None = prior_arch[-1] if prior_arch else None
        gate_error_log: str | None = None
        guard_error_text: str | None = None

        max_rounds = self.config.workflow.max_review_rounds
        max_gate_retries = self.config.workflow.max_gate_retries_per_round
        max_attempts = (
            task.max_attempts
            if task.max_attempts is not None
            else self.config.workflow.max_attempts
        )
        last_diff_guard_sig: tuple[tuple[str, str | None], ...] | None = None

        while task.review_rounds < max_rounds:
            self._check_control_after_step()
            if task.attempts >= max_attempts:
                self._fail_task_and_rollback(
                    task,
                    task_base_sha,
                    reason=f"Reached max_attempts ({max_attempts}) without completing task.",
                )
                return True

            gates_passed_this_round = False
            last_guard_report: DiffGuardReport | None = None
            last_worker_parsed: ParsedWorkerOutput | None = None

            for _gate_try in range(max_gate_retries):
                if task.attempts >= max_attempts:
                    self._fail_task_and_rollback(
                        task,
                        task_base_sha,
                        reason=f"Reached max_attempts ({max_attempts}) without completing task.",
                    )
                    return True

                _, attempt_dir = self._next_attempt_dir(task.id)

                progress_text = self._read_progress_text()
                repo_map_text = self._refresh_repo_map()
                extra_inst = (
                    f"Architect resolution guidance:\n{architect_notes}" if architect_notes else None
                )
                built_prompt = self.prompt_builder.build(
                    role=worker_role,
                    profile=worker_profile,
                    repo_map_text=repo_map_text,
                    progress_text=progress_text,
                    task=task,
                    inbox_text=inbox_text,
                    gate_error_log=gate_error_log,
                    guard_error_text=guard_error_text,
                    review_feedback=review_feedback,
                    extra_instructions=extra_inst,
                )

                try:
                    response, guard_report = self._invoke_provider_guarded(
                        role=worker_role,
                        built_prompt=built_prompt,
                        task_id=task.id,
                        attempt_dir=attempt_dir,
                        base_sha=task_base_sha,
                        task_complexity=task.complexity,
                        allowed_paths=task.allowed_paths,
                    )
                except _TaskDeferredOnCooldownError:
                    self._transition_task(task.id, TaskStatus.TODO)
                    self.event_bus.update_state(
                        active_task_id=None,
                        active_task_base_sha=None,
                        active_phase="deferred_cooldown",
                        active_role=None,
                        active_provider=None,
                    )
                    return False
                task = self.store.update_task_fields(task.id, attempts=task.attempts + 1)

                # Honor `control abort` AND `pause_after_current` immediately after a provider step
                # (before running gates, next retries, or reviews)
                self._check_control_after_step()

                last_guard_report = guard_report
                parsed = parse_worker_output(response.parsed_text)
                last_worker_parsed = parsed

                # Reset one-shot error messages once passed to worker
                gate_error_log = None
                guard_error_text = None

                # 1. Immediate rollback on Red Zone violation (existing test M/D, protected files, .pipeline tamper)
                if guard_report.red_violations:
                    last_diff_guard_sig = None
                    self.event_bus.emit(
                        EventType.RED_ZONE_VIOLATION,
                        task_id=task.id,
                        role=worker_role,
                        provider=response.provider_name,
                        payload={
                            "violations": [
                                {"code": v.code, "path": v.path, "message": v.message}
                                for v in guard_report.red_violations
                            ]
                        },
                    )
                    self.git_ops.hard_rollback(task_base_sha)
                    self.event_bus.emit(
                        EventType.TASK_ROLLED_BACK,
                        task_id=task.id,
                        payload={"reason": "red_zone_violation", "base_sha": task_base_sha},
                    )
                    if task.attempts >= max_attempts:
                        self._fail_task_and_rollback(
                            task,
                            task_base_sha,
                            reason=(
                                f"Reached max_attempts ({max_attempts}) after Red Zone violation: "
                                f"{guard_report.summary_text()[:240]}"
                            ),
                        )
                        return True
                    guard_error_text = (
                        "Your previous attempt was ROLLED BACK because of a Red Zone violation:\n"
                        + guard_report.summary_text()
                    )
                    task = self._transition_task(task.id, TaskStatus.GATES_FAILED)
                    task = self._transition_task(task.id, TaskStatus.IN_PROGRESS)
                    continue

                # 2. Architectural escalation protocol (STATUS: NEEDS_ARCH_CHANGE)
                if parsed.status == WorkerStatus.NEEDS_ARCH_CHANGE:
                    last_diff_guard_sig = None
                    self.git_ops.hard_rollback(task_base_sha)
                    new_base, guidance, resolved = self._handle_escalation(
                        task=task,
                        worker_output=parsed,
                        base_sha=task_base_sha,
                    )
                    if not resolved:
                        self._fail_task_and_rollback(
                            task,
                            task_base_sha,
                            reason=f"Architect could not resolve escalation: {guidance}",
                        )
                        return True
                    task_base_sha = new_base
                    self.event_bus.update_state(active_task_base_sha=task_base_sha)
                    architect_notes = guidance
                    task = self._transition_task(task.id, TaskStatus.IN_PROGRESS)
                    continue

                # 3. Provider execution error / timeout / idle timeout
                if response.timed_out or response.idle_timed_out or response.exit_code != 0:
                    last_diff_guard_sig = None
                    if task.attempts >= max_attempts:
                        self._fail_task_and_rollback(
                            task,
                            task_base_sha,
                            reason=(
                                f"Reached max_attempts ({max_attempts}) after provider failure "
                                f"(exit_code={response.exit_code}, timed_out={response.timed_out}, "
                                f"idle_timed_out={response.idle_timed_out})."
                            ),
                        )
                        return True
                    guard_error_text = (
                        f"Provider execution failed (exit_code={response.exit_code}, "
                        f"timed_out={response.timed_out}, idle_timed_out={response.idle_timed_out}). "
                        f"Fix and complete the task.\n"
                        f"{response.raw_stderr[-500:]}"
                    )
                    task = self._transition_task(task.id, TaskStatus.GATES_FAILED)
                    task = self._transition_task(task.id, TaskStatus.IN_PROGRESS)
                    continue

                # 4. Placeholder Guard & Diff Guard / Scope checks (0 tokens)
                if not guard_report.passed:
                    if guard_report.placeholder_violations:
                        self.event_bus.emit(
                            EventType.PLACEHOLDER_GUARD_FAILED,
                            task_id=task.id,
                            role=worker_role,
                            payload={
                                "count": len(guard_report.placeholder_violations),
                                "details": [v.message for v in guard_report.placeholder_violations[:10]],
                            },
                        )
                    diff_violations = [
                        *guard_report.scope_violations,
                        *guard_report.limit_violations,
                    ]
                    if diff_violations:
                        self.event_bus.emit(
                            EventType.DIFF_GUARD_FAILED,
                            task_id=task.id,
                            role=worker_role,
                            payload={
                                "details": [v.message for v in diff_violations]
                            },
                        )
                        diff_sig = tuple(sorted((v.code, v.path) for v in diff_violations))
                        if diff_sig == last_diff_guard_sig:
                            self._fail_task_and_rollback(
                                task,
                                task_base_sha,
                                reason=(
                                    "Repeated identical diff_guard_failed (2 consecutive attempts): "
                                    f"{guard_report.summary_text()[:240]}"
                                ),
                            )
                            return True
                        last_diff_guard_sig = diff_sig
                    else:
                        last_diff_guard_sig = None

                    if task.attempts >= max_attempts:
                        self._fail_task_and_rollback(
                            task,
                            task_base_sha,
                            reason=(
                                f"Reached max_attempts ({max_attempts}) after guard failure: "
                                f"{guard_report.summary_text()[:240]}"
                            ),
                        )
                        return True
                    guard_error_text = guard_report.summary_text()
                    task = self._transition_task(task.id, TaskStatus.GATES_FAILED)
                    task = self._transition_task(task.id, TaskStatus.IN_PROGRESS)
                    continue

                last_diff_guard_sig = None

                # 5. Free local gates (lint, typecheck, build, tests, Playwright, verify_cmd)
                self.event_bus.update_state(active_phase="gates", active_task_id=task.id)
                self.event_bus.emit(EventType.GATE_STARTED, task_id=task.id)
                gates_res = self.gate_runner.run_all(
                    attempt_dir=attempt_dir,
                    verify_cmd=task.verify_cmd,
                    fail_fast=True,
                )
                atomic_write_text(attempt_dir / "gates.log", gates_res.combined_trimmed_log + "\n")

                if not gates_res.passed:
                    failed_names = [r.name for r in gates_res.results if not r.passed and not r.optional]
                    self.event_bus.emit(
                        EventType.GATE_FAILED,
                        task_id=task.id,
                        payload={
                            "failed_gates": failed_names,
                            "screenshots": gates_res.screenshots,
                            "log_path": str(attempt_dir / "gates.log"),
                        },
                    )
                    if task.attempts >= max_attempts:
                        self._fail_task_and_rollback(
                            task,
                            task_base_sha,
                            reason=(
                                f"Reached max_attempts ({max_attempts}) failing gate(s): "
                                f"{', '.join(failed_names)}"
                            ),
                        )
                        return True
                    gate_error_log = gates_res.combined_trimmed_log
                    task = self._transition_task(task.id, TaskStatus.GATES_FAILED)
                    task = self._transition_task(task.id, TaskStatus.IN_PROGRESS)
                    continue

                self.event_bus.emit(
                    EventType.GATE_PASSED,
                    task_id=task.id,
                    payload={
                        "gates": [r.name for r in gates_res.results],
                        "screenshots": gates_res.screenshots,
                    },
                )
                gates_passed_this_round = True
                break

            # If gates/guards never passed in this round -> count as a failed round
            if not gates_passed_this_round or last_guard_report is None or last_worker_parsed is None:
                new_rounds = task.review_rounds + 1
                task = self.store.update_task_fields(task.id, review_rounds=new_rounds)
                if (
                    task.complexity == TaskComplexity.HARD
                    and new_rounds == self.config.workflow.hard_split_after_rounds
                    and self._try_split_hard_task(task, task_base_sha)
                ):
                    return True
                if new_rounds >= max_rounds:
                    self._fail_task_and_rollback(
                        task,
                        task_base_sha,
                        reason=f"Exceeded {max_rounds} rounds failing guards/gates.",
                    )
                    return True
                continue

            if last_guard_report.touched_yellow_zone:
                self.event_bus.emit(
                    EventType.YELLOW_ZONE_TRIGGERED,
                    task_id=task.id,
                    payload={"yellow_files": last_guard_report.yellow_files},
                )

            soft_warning_note: str | None = None
            if last_guard_report.has_soft_warnings:
                soft_warning_note = last_guard_report.soft_warning_text()
                self.event_bus.emit(
                    EventType.DIFF_GUARD_SOFT_WARNING,
                    task_id=task.id,
                    role=worker_role,
                    payload={
                        "details": [w.message for w in last_guard_report.soft_limit_warnings],
                        "codes": [w.code for w in last_guard_report.soft_limit_warnings],
                        "prod_files_count": last_guard_report.prod_files_count,
                        "prod_changed_lines": last_guard_report.prod_changed_lines,
                        "test_files_count": last_guard_report.test_files_count,
                        "test_changed_lines": last_guard_report.test_changed_lines,
                    },
                )

            # Route A: Easy task without Yellow Zone, without soft warnings & without file overlap -> BATCH_PENDING
            existing_batch_files = {
                fp for bt in self._get_batch_pending_tasks() for fp in bt.changed_files
            }
            overlaps_batch = not existing_batch_files.isdisjoint(set(last_guard_report.changed_files))

            if (
                task.complexity == TaskComplexity.EASY
                and not last_guard_report.touched_yellow_zone
                and not last_guard_report.has_soft_warnings
                and not overlaps_batch
                and self.config.workflow.batch_review_size > 1
            ):
                batch_raw_diff = self.git_ops.get_diff(task_base_sha)
                batch_diff_stat = self.git_ops.get_diff_stat(task_base_sha)
                wip_sha: str | None = None
                if last_guard_report.changed_files and self.config.workflow.auto_commit:
                    wip_sha = self.git_ops.commit_all(f"wip(task-{task.id}): {task.title}")
                    self.event_bus.emit(
                        EventType.GIT_COMMITTED,
                        task_id=task.id,
                        payload={"commit_sha": wip_sha, "wip": True},
                    )
                # IMPORTANT: Do NOT update PROGRESS.md yet — only after Master APPROVE!
                pending_progress = last_worker_parsed.progress_update or (
                    f"Completed `#{task.id}` ({task.title}). "
                    f"Files: {', '.join(last_guard_report.changed_files) or 'none'}."
                )
                self._batch_diffs[task.id] = batch_raw_diff or last_guard_report.filtered_diff
                self._batch_diff_stats[task.id] = batch_diff_stat
                self._transition_task(
                    task.id,
                    TaskStatus.BATCH_PENDING,
                    wip_commit_sha=wip_sha,
                    changed_files=last_guard_report.changed_files,
                    touched_yellow_zone=False,
                    pending_progress_note=pending_progress,
                    last_report=last_worker_parsed.report,
                )
                self._sync_batch_queue_from_store()
                return True

            # Route B: Medium, Hard, Yellow-Zone-triggered, or Soft-Warning Easy -> Individual review
            pending_progress_note = last_worker_parsed.progress_update or (
                f"Completed `#{task.id}` ({task.title}). "
                f"Files: {', '.join(last_guard_report.changed_files) or 'none'}."
            )
            task = self._transition_task(
                task.id,
                TaskStatus.NEEDS_REVIEW,
                changed_files=last_guard_report.changed_files,
                touched_yellow_zone=last_guard_report.touched_yellow_zone,
                pending_progress_note=pending_progress_note,
                last_report=last_worker_parsed.report,
                pre_review_approved=False,
            )

            outcome, task, rejected_feedback = self._run_needs_review_pipeline(
                task=task,
                task_base_sha=task_base_sha,
                guard_report=last_guard_report,
                last_report=last_worker_parsed.report,
                progress_note=pending_progress_note,
                repo_map_text=repo_map_text,
                soft_warning_note=soft_warning_note,
                on_reject_status=TaskStatus.IN_PROGRESS,
            )
            if outcome != "rejected":
                return True
            review_feedback = rejected_feedback
        return True

    def _run_needs_review_pipeline(
        self,
        *,
        task: Task,
        task_base_sha: str,
        guard_report: DiffGuardReport,
        last_report: str,
        progress_note: str,
        repo_map_text: str,
        soft_warning_note: str | None,
        on_reject_status: TaskStatus,
    ) -> tuple[Literal["done", "failed_or_split", "rejected"], Task, str | None]:
        """Run pre_reviewer (unless already approved) and Master reviewer for a NEEDS_REVIEW task."""
        max_rounds = self.config.workflow.max_review_rounds

        # B1. Evil Pre-Reviewer for medium/hard tasks (skipped when already approved before a pause)
        if (
            task.complexity in (TaskComplexity.MEDIUM, TaskComplexity.HARD)
            and not task.pre_review_approved
        ):
            pre_ok, pre_feedback = self._run_single_review(
                role=AgentRole.PRE_REVIEWER,
                task=task,
                filtered_diff=guard_report.filtered_diff,
                last_report=last_report,
                base_sha=task_base_sha,
                repo_map_text=repo_map_text,
                soft_warning_note=soft_warning_note,
            )
            self._check_control_after_step()
            if not pre_ok:
                self.store.add_comment(
                    task.id,
                    author="pre_reviewer",
                    kind="rejection",
                    body=pre_feedback,
                )
                new_rounds = task.review_rounds + 1
                task = self.store.update_task_fields(
                    task.id,
                    review_rounds=new_rounds,
                    pre_review_approved=False,
                )
                if (
                    task.complexity == TaskComplexity.HARD
                    and new_rounds == self.config.workflow.hard_split_after_rounds
                    and self._try_split_hard_task(task, task_base_sha)
                ):
                    return "failed_or_split", task, None
                if new_rounds >= max_rounds:
                    self._fail_task_and_rollback(
                        task,
                        task_base_sha,
                        reason=f"Rejected by pre_reviewer after {max_rounds} rounds: {pre_feedback[:240]}",
                    )
                    return "failed_or_split", task, None
                task = self._transition_task(
                    task.id,
                    on_reject_status,
                    pre_review_approved=False,
                )
                return "rejected", task, pre_feedback
            task = self.store.update_task_fields(task.id, pre_review_approved=True)

        # B2. Master Reviewer
        master_ok, master_feedback = self._run_single_review(
            role=AgentRole.REVIEWER,
            task=task,
            filtered_diff=guard_report.filtered_diff,
            last_report=last_report,
            base_sha=task_base_sha,
            repo_map_text=repo_map_text,
            soft_warning_note=soft_warning_note,
        )
        self._check_control_after_step()

        if master_ok:
            final_sha: str | None = None
            if guard_report.changed_files and self.config.workflow.auto_commit:
                prefix = self.config.workflow.commit_prefix
                final_sha = self.git_ops.commit_all(f"{prefix}(task-{task.id}): {task.title}")
                self.event_bus.emit(
                    EventType.GIT_COMMITTED,
                    task_id=task.id,
                    payload={"commit_sha": final_sha, "wip": False},
                )
            self._append_progress_entry(f"Task #{task.id}: {task.title}", progress_note)
            task = self._transition_task(
                task.id,
                TaskStatus.DONE,
                resolution="completed",
                final_commit_sha=final_sha,
                changed_files=guard_report.changed_files,
                touched_yellow_zone=guard_report.touched_yellow_zone,
                pending_progress_note=None,
                last_report=last_report,
                pre_review_approved=False,
            )
            self.event_bus.update_state(
                accepted_since_last_refactor=self.event_bus.state.accepted_since_last_refactor + 1
            )
            _, done_count, total_active = self._task_progress_counts()
            sha_part = f" | Commit: {final_sha[:10]}" if final_sha else ""
            self.notifier.notify(
                "task_done",
                f"Task Done #{task.id} ({self.config.project.name})",
                f"{task.title}\nProgress: {done_count}/{total_active}{sha_part}",
            )
            return "done", task, None

        # Rejected by Master
        self.store.add_comment(
            task.id,
            author="reviewer",
            kind="rejection",
            body=master_feedback,
        )
        new_rounds = task.review_rounds + 1
        task = self.store.update_task_fields(
            task.id,
            review_rounds=new_rounds,
            pre_review_approved=False,
        )
        if (
            task.complexity == TaskComplexity.HARD
            and new_rounds == self.config.workflow.hard_split_after_rounds
            and self._try_split_hard_task(task, task_base_sha)
        ):
            return "failed_or_split", task, None
        if new_rounds >= max_rounds:
            self._fail_task_and_rollback(
                task,
                task_base_sha,
                reason=f"Rejected by reviewer after {max_rounds} rounds: {master_feedback[:240]}",
            )
            return "failed_or_split", task, None
        task = self._transition_task(
            task.id,
            on_reject_status,
            pre_review_approved=False,
        )
        return "rejected", task, master_feedback

    def _resume_needs_review_task(self, task: Task) -> bool:
        """Resume a checkpointed NEEDS_REVIEW task directly at pre_reviewer or reviewer."""
        if not self.git_ops.has_uncommitted_changes():
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
                task_id=task.id,
                payload={
                    "rollback_sha": self.git_ops.get_head_sha(),
                    "rolled_back_dirty_tree": False,
                    "recovered_task_ids": [task.id],
                    "reason": "clean_working_tree_for_needs_review",
                },
            )
            return False

        task_base_sha = (
            self._resumable_needs_review_base_sha.pop(task.id, None)
            or self.git_ops.get_head_sha()
        )
        self.event_bus.update_state(
            active_task_id=task.id,
            active_task_base_sha=task_base_sha,
            active_phase="needs_review_resume",
        )
        self.git_ops.mark_untracked_intent_to_add()
        changes = self.git_ops.get_name_status(task_base_sha)
        raw_diff = self.git_ops.get_diff(task_base_sha)
        worker_role = self._worker_role_for(task.complexity)
        guard_report = evaluate_diff(
            role=worker_role,
            changes=changes,
            raw_diff=raw_diff,
            config=self.config,
            task_complexity=task.complexity,
            allowed_paths=task.allowed_paths,
        )
        soft_warning_note = (
            guard_report.soft_warning_text() if guard_report.has_soft_warnings else None
        )
        progress_note = task.pending_progress_note or (
            f"Completed `#{task.id}` ({task.title}). "
            f"Files: {', '.join(guard_report.changed_files) or 'none'}."
        )
        repo_map_text = self._refresh_repo_map()
        outcome, _, _ = self._run_needs_review_pipeline(
            task=task,
            task_base_sha=task_base_sha,
            guard_report=guard_report,
            last_report=task.last_report or "",
            progress_note=progress_note,
            repo_map_text=repo_map_text,
            soft_warning_note=soft_warning_note,
            on_reject_status=TaskStatus.TODO,
        )
        return outcome != "rejected"

    def _run_single_review(
        self,
        *,
        role: AgentRole,
        task: Task,
        filtered_diff: str,
        last_report: str,
        base_sha: str,
        repo_map_text: str,
        soft_warning_note: str | None = None,
    ) -> tuple[bool, str]:
        _, attempt_dir = self._next_attempt_dir(f"{task.id}_{role.value}")
        profile = self.registry.for_role(role).profile
        diff_stat = self.git_ops.get_diff_stat(base_sha)
        raw_diff_for_review = self.git_ops.get_diff(base_sha)
        review_diff_input = raw_diff_for_review if raw_diff_for_review.strip() else filtered_diff
        built_prompt = self.prompt_builder.build(
            role=role,
            profile=profile,
            repo_map_text=repo_map_text,
            progress_text=self._read_progress_text(),
            task=task,
            filtered_diff=review_diff_input,
            diff_stat=diff_stat,
            last_report=last_report,
            extra_instructions=soft_warning_note,
        )
        response, guard_report = self._invoke_provider_guarded(
            role=role,
            built_prompt=built_prompt,
            task_id=task.id,
            attempt_dir=attempt_dir,
            base_sha=base_sha,
            preserve_existing_diff_for_readonly=True,
        )
        if not guard_report.passed:
            self.event_bus.emit(
                EventType.RED_ZONE_VIOLATION,
                task_id=task.id,
                role=role,
                provider=response.provider_name,
                payload={"reason": "read_only_role_modified_files", "details": guard_report.summary_text()},
            )
            self.git_ops.hard_rollback(base_sha)
            raise OrchestratorFatalError(
                f"Read-only role '{role.value}' modified repository files during review of #{task.id}: "
                f"{guard_report.summary_text()}"
            )

        verdict = parse_single_review_verdict(response.parsed_text, task.id)
        event_type = (
            EventType.PRE_REVIEW_COMPLETED
            if role == AgentRole.PRE_REVIEWER
            else EventType.REVIEW_COMPLETED
        )
        self.event_bus.emit(
            event_type,
            task_id=task.id,
            role=role,
            provider=response.provider_name,
            payload={"verdict": verdict.verdict.value},
        )
        if response.exit_code != 0 or response.timed_out:
            raise _PausedByInfraError(
                reason=(
                    f"{role.value} provider '{response.provider_name}' failed "
                    f"(exit_code={response.exit_code}, timed_out={response.timed_out})"
                ),
                task_id=task.id,
                role=role,
                provider=response.provider_name,
            )
        return verdict.verdict == ReviewVerdictType.APPROVE, verdict.feedback

    # -------------------------------------------------------------------------
    # Batch Review (Easy Independent Tasks)
    # -------------------------------------------------------------------------

    def _flush_batch_review(self) -> None:
        batch_tasks = self._get_batch_pending_tasks()
        if not batch_tasks:
            return

        task_ids = [t.id for t in batch_tasks]
        self.event_bus.emit(
            EventType.BATCH_REVIEW_STARTED,
            role=AgentRole.REVIEWER,
            provider=self.registry.profile_name_for_role(AgentRole.REVIEWER),
            payload={"task_ids": task_ids},
        )

        batch_items: list[tuple[Task, str, str]] = []
        batch_diff_stats: dict[str, str] = {}
        for task in batch_tasks:
            diff_text = self._batch_diffs.get(task.id, "")
            diff_stat = self._batch_diff_stats.get(task.id, "")
            if not diff_text and task.wip_commit_sha:
                # Reconstruct diff from commit if resumed across runs
                res = self.git_ops._run(["show", "--format=", task.wip_commit_sha], check=False)
                if res.exit_code == 0:
                    diff_text = res.stdout
            if not diff_stat and task.wip_commit_sha:
                diff_stat = self.git_ops.get_commit_diff_stat(task.wip_commit_sha)
            batch_diff_stats[task.id] = diff_stat
            batch_items.append((task, diff_text, task.last_report or ""))

        _, attempt_dir = self._next_attempt_dir("batch_review")
        base_sha = self.git_ops.get_head_sha()
        reviewer_profile = self.registry.for_role(AgentRole.REVIEWER).profile
        built_prompt = self.prompt_builder.build(
            role=AgentRole.REVIEWER,
            profile=reviewer_profile,
            repo_map_text=self._refresh_repo_map(),
            progress_text=self._read_progress_text(),
            batch_items=batch_items,
            batch_diff_stats=batch_diff_stats,
        )

        response, guard_report = self._invoke_provider_guarded(
            role=AgentRole.REVIEWER,
            built_prompt=built_prompt,
            task_id=",".join(task_ids),
            attempt_dir=attempt_dir,
            base_sha=base_sha,
        )
        if not guard_report.passed:
            self.event_bus.emit(
                EventType.RED_ZONE_VIOLATION,
                role=AgentRole.REVIEWER,
                provider=response.provider_name,
                payload={"reason": "batch_reviewer_modified_files", "details": guard_report.summary_text()},
            )
            self.git_ops.hard_rollback(base_sha)
            raise OrchestratorFatalError(
                f"Read-only batch reviewer modified repository files: {guard_report.summary_text()}"
            )

        # Do NOT falsely revert valid batch tasks when the reviewer provider fails due to infra/config error
        if response.exit_code != 0 or response.timed_out:
            self.event_bus.emit(
                EventType.BATCH_REVIEW_DEFERRED,
                task_id=",".join(task_ids),
                role=AgentRole.REVIEWER,
                provider=response.provider_name,
                payload={
                    "task_ids": task_ids,
                    "exit_code": response.exit_code,
                    "timed_out": response.timed_out,
                    "stderr_tail": response.raw_stderr[-300:],
                },
            )
            raise _PausedByInfraError(
                reason=(
                    f"batch_reviewer provider '{response.provider_name}' failed "
                    f"(exit_code={response.exit_code}, timed_out={response.timed_out}); "
                    f"tasks kept in batch_pending"
                ),
                task_id=",".join(task_ids),
                role=AgentRole.REVIEWER,
                provider=response.provider_name,
            )

        verdicts = parse_batch_review_verdicts(response.parsed_text, task_ids)
        approved_tasks: list[Task] = []
        requeued_ids: list[str] = []
        batch_failed: list[tuple[Task, str, list[str]]] = []

        for task in batch_tasks:
            verdict = verdicts[task.id]
            self.event_bus.emit(
                EventType.BATCH_VERDICT_RECEIVED,
                task_id=task.id,
                role=AgentRole.REVIEWER,
                provider=response.provider_name,
                payload={"verdict": verdict.verdict.value},
            )
            self._batch_diffs.pop(task.id, None)
            self._batch_diff_stats.pop(task.id, None)

            if verdict.verdict == ReviewVerdictType.APPROVE:
                # Update PROGRESS.md ONLY after APPROVE (never at wip-commit time)
                note = task.pending_progress_note or (
                    f"Completed `#{task.id}` ({task.title}). "
                    f"Files: {', '.join(task.changed_files) or 'none'}."
                )
                self._append_progress_entry(f"Task #{task.id}: {task.title}", note)
                self._transition_task(
                    task.id,
                    TaskStatus.DONE,
                    resolution="completed",
                    final_commit_sha=task.wip_commit_sha,
                    pending_progress_note=None,
                )
                self.event_bus.update_state(
                    accepted_since_last_refactor=self.event_bus.state.accepted_since_last_refactor + 1
                )
                approved_tasks.append(task)
            else:
                # Selectively revert ONLY the rejected task's commit without touching approved tasks
                if task.wip_commit_sha:
                    try:
                        revert_sha = self.git_ops.revert_commit(task.wip_commit_sha)
                        self.event_bus.emit(
                            EventType.GIT_REVERTED,
                            task_id=task.id,
                            payload={
                                "reverted_commit": task.wip_commit_sha,
                                "revert_commit": revert_sha,
                            },
                        )
                    except GitOperationError as exc:
                        raise OrchestratorFatalError(
                            f"Cannot cleanly revert rejected batch task #{task.id}: {exc}"
                        ) from exc

                self.store.add_comment(
                    task.id,
                    author="reviewer",
                    kind="rejection",
                    body=verdict.feedback or "Rejected during batch review.",
                )
                new_rounds = task.review_rounds + 1
                if new_rounds >= self.config.workflow.max_review_rounds:
                    fail_reason = (
                        f"Rejected in batch review after {new_rounds} rounds: "
                        f"{verdict.feedback[:200]}"
                    )
                    self._transition_task(
                        task.id,
                        TaskStatus.FAILED,
                        review_rounds=new_rounds,
                        wip_commit_sha=None,
                        pending_progress_note=None,
                        failure_reason=fail_reason,
                    )
                    blocked_ids = self.store.cascade_block_dependents(task.id)
                    batch_failed.append((task, fail_reason, blocked_ids))
                else:
                    self._transition_task(
                        task.id,
                        TaskStatus.TODO,
                        review_rounds=new_rounds,
                        wip_commit_sha=None,
                        pending_progress_note=None,
                    )
                    requeued_ids.append(task.id)

        self._sync_batch_queue_from_store()

        if approved_tasks:
            _, done_count, total_active = self._task_progress_counts()
            approved_str = ", ".join(f"#{t.id} ({t.title})" for t in approved_tasks)
            lines = [f"Approved ({len(approved_tasks)}): {approved_str}"]
            if requeued_ids:
                lines.append("Requeued: " + ", ".join(f"#{tid}" for tid in requeued_ids))
            if batch_failed:
                lines.append("Failed: " + ", ".join(f"#{t.id}" for t, _, _ in batch_failed))
            lines.append(f"Progress: {done_count}/{total_active}")
            self.notifier.notify(
                "task_done",
                f"Batch Review Done ({self.config.project.name})",
                "\n".join(lines),
            )

        for failed_task, fail_reason, blocked_ids in batch_failed:
            blocked_line = (
                f"\nBlocked dependents: {', '.join(f'#{bid}' for bid in blocked_ids)}"
                if blocked_ids
                else ""
            )
            self.notifier.notify(
                "task_failed",
                f"Task Failed #{failed_task.id} ({self.config.project.name})",
                f"{failed_task.title}\nReason: {fail_reason[:300]}{blocked_line}",
            )

    # -------------------------------------------------------------------------
    # Architectural Escalation (STATUS: NEEDS_ARCH_CHANGE)
    # -------------------------------------------------------------------------

    def _resume_arch_pending_task(self, task: Task) -> None:
        """Resume an ARCH_PENDING task left over from a prior crashed run."""
        base_sha = self.git_ops.get_head_sha()
        self.event_bus.update_state(
            active_task_id=task.id,
            active_task_base_sha=base_sha,
            active_phase="arch_pending_resume",
        )
        escalation_comments = [c.body for c in task.comments if c.kind == "escalation"]
        details = escalation_comments[-1] if escalation_comments else task.description
        _, guidance, resolved = self._invoke_architect_for_escalation(
            task=task,
            details=details,
            base_sha=base_sha,
        )
        if not resolved:
            self._fail_task_and_rollback(
                task,
                base_sha,
                reason=f"Architect could not resolve escalation: {guidance}",
            )
            return
        self._transition_task(task.id, TaskStatus.TODO)

    def _handle_escalation(
        self,
        *,
        task: Task,
        worker_output: ParsedWorkerOutput,
        base_sha: str,
    ) -> tuple[str, str, bool]:
        details = worker_output.escalation_details or worker_output.report
        self.store.add_comment(
            task.id,
            author="worker",
            kind="escalation",
            body=details,
        )
        self._transition_task(task.id, TaskStatus.ARCH_PENDING)
        self.event_bus.emit(
            EventType.ESCALATION_TRIGGERED,
            task_id=task.id,
            role=AgentRole.ARCHITECT,
            provider=self.registry.profile_name_for_role(AgentRole.ARCHITECT),
            payload={"summary": worker_output.escalation_summary or details[:200]},
        )
        self.notifier.notify(
            "escalation",
            f"Escalation to Architect #{task.id} ({self.config.project.name})",
            f"{task.title}\nSummary: {(worker_output.escalation_summary or details)[:300]}",
        )
        return self._invoke_architect_for_escalation(
            task=task,
            details=details,
            base_sha=base_sha,
        )

    def _invoke_architect_for_escalation(
        self,
        *,
        task: Task,
        details: str,
        base_sha: str,
    ) -> tuple[str, str, bool]:
        self.event_bus.update_state(
            active_task_id=task.id,
            active_task_base_sha=base_sha,
        )
        _, attempt_dir = self._next_attempt_dir(f"{task.id}_architect")
        arch_profile = self.registry.for_role(AgentRole.ARCHITECT).profile
        built_prompt = self.prompt_builder.build(
            role=AgentRole.ARCHITECT,
            profile=arch_profile,
            repo_map_text=self._refresh_repo_map(),
            progress_text=self._read_progress_text(),
            task=task,
            escalation_context=details,
        )

        response, guard_report = self._invoke_provider_guarded(
            role=AgentRole.ARCHITECT,
            built_prompt=built_prompt,
            task_id=task.id,
            attempt_dir=attempt_dir,
            base_sha=base_sha,
        )

        if (response.exit_code != 0 or response.timed_out) and guard_report.passed:
            self.git_ops.hard_rollback(base_sha)
            raise _PausedByInfraError(
                reason=(
                    f"architect provider '{response.provider_name}' failed "
                    f"(exit_code={response.exit_code}, timed_out={response.timed_out}); "
                    f"task #{task.id} remains arch_pending"
                ),
                task_id=task.id,
                role=AgentRole.ARCHITECT,
                provider=response.provider_name,
            )

        if not guard_report.passed:
            self.git_ops.hard_rollback(base_sha)
            return base_sha, guard_report.summary_text() or "Architect violated Diff Guard", False

        new_base_sha = base_sha
        if guard_report.changed_files:
            gates_res = self.gate_runner.run_all(attempt_dir=attempt_dir, fail_fast=True)
            atomic_write_text(attempt_dir / "gates.log", gates_res.combined_trimmed_log + "\n")
            if not gates_res.passed:
                self.git_ops.hard_rollback(base_sha)
                return base_sha, f"Architect changes failed gates:\n{gates_res.combined_trimmed_log}", False
            if self.config.workflow.auto_commit:
                new_base_sha = self.git_ops.commit_all(f"arch(task-{task.id}): resolve escalation")
                self.event_bus.emit(
                    EventType.GIT_COMMITTED,
                    task_id=task.id,
                    role=AgentRole.ARCHITECT,
                    payload={"commit_sha": new_base_sha, "kind": "architect"},
                )

        arch_parsed = parse_worker_output(response.parsed_text)
        guidance = arch_parsed.report or response.parsed_text
        if arch_parsed.progress_update:
            self._append_progress_entry(
                f"Architect Resolution for #{task.id}",
                arch_parsed.progress_update,
            )
        elif guard_report.changed_files:
            self._append_progress_entry(
                f"Architect Resolution for #{task.id}",
                f"Updated shared files: {', '.join(guard_report.changed_files)}.\n\n{guidance[:500]}",
            )

        self.store.add_comment(
            task.id,
            author="architect",
            kind="architect",
            body=guidance,
        )
        self.event_bus.emit(
            EventType.ARCHITECT_RESOLVED,
            task_id=task.id,
            role=AgentRole.ARCHITECT,
            provider=response.provider_name,
            payload={"changed_files": guard_report.changed_files, "new_base_sha": new_base_sha},
        )
        return new_base_sha, guidance, True

    # -------------------------------------------------------------------------
    # Hard Task Auto-Splitting (after 2 failed rounds)
    # -------------------------------------------------------------------------

    def _try_split_hard_task(self, task: Task, base_sha: str) -> bool:
        self.git_ops.hard_rollback(base_sha)
        _, attempt_dir = self._next_attempt_dir(f"{task.id}_planner_split")
        planner_profile = self.registry.for_role(AgentRole.PLANNER).profile

        failure_history = "\n\n".join(
            f"- [{c.author}/{c.kind}] {c.body}" for c in task.comments[-5:]
        )
        extra_inst = (
            f"Hard task #{task.id} failed {task.review_rounds} full rounds.\n"
            "Decompose it into 2 to 3 smaller atomic subtasks (easy or medium complexity) "
            "and output valid JSON inside BEGIN_SUBTASKS_JSON / END_SUBTASKS_JSON.\n"
            + (f"\nRecent failure history:\n{failure_history}" if failure_history else "")
        )

        built_prompt = self.prompt_builder.build(
            role=AgentRole.PLANNER,
            profile=planner_profile,
            repo_map_text=self._refresh_repo_map(),
            progress_text=self._read_progress_text(),
            task=task,
            extra_instructions=extra_inst,
        )
        response, guard_report = self._invoke_provider_guarded(
            role=AgentRole.PLANNER,
            built_prompt=built_prompt,
            task_id=task.id,
            attempt_dir=attempt_dir,
            base_sha=base_sha,
        )
        if not guard_report.passed:
            self.git_ops.hard_rollback(base_sha)
            raise OrchestratorFatalError(
                f"Read-only planner modified files while splitting #{task.id}: {guard_report.summary_text()}"
            )
        if response.exit_code != 0 or response.timed_out:
            raise _PausedByInfraError(
                reason=(
                    f"planner provider '{response.provider_name}' failed "
                    f"(exit_code={response.exit_code}, timed_out={response.timed_out})"
                ),
                task_id=task.id,
                role=AgentRole.PLANNER,
                provider=response.provider_name,
            )

        try:
            planned = parse_planner_subtasks(response.parsed_text, task)
        except (ValueError, json.JSONDecodeError) as exc:
            self.store.add_comment(
                task.id,
                author="orchestrator",
                kind="system",
                body=f"Planner split output could not be parsed: {exc}",
            )
            return False

        subtasks = [
            Task(
                id=spec.id,
                title=spec.title,
                description=spec.description,
                complexity=spec.complexity,
                depends_on=list(spec.depends_on),
                allowed_paths=list(spec.allowed_paths or task.allowed_paths),
                context_files=list(spec.context_files or task.context_files),
                verify_cmd=spec.verify_cmd,
                acceptance_criteria=list(spec.acceptance_criteria),
                stage=task.stage,
            )
            for spec in planned.subtasks
        ]
        created = self.store.add_subtasks(task.id, subtasks)
        self.event_bus.emit(
            EventType.TASK_SPLIT,
            task_id=task.id,
            role=AgentRole.PLANNER,
            provider=response.provider_name,
            payload={"subtask_ids": [st.id for st in created], "rationale": planned.rationale},
        )
        self.notifier.notify(
            "escalation",
            f"Hard Task Split #{task.id} ({self.config.project.name})",
            f"{task.title}\nSubtasks: {', '.join(f'#{st.id}' for st in created)}\n"
            f"Rationale: {planned.rationale[:240]}",
        )
        return True

    # -------------------------------------------------------------------------
    # Scheduled Refactoring (every N accepted tasks / end of stage)
    # -------------------------------------------------------------------------

    def _run_refactorer(self, *, reason: str) -> None:
        base_sha = self.git_ops.get_head_sha()
        self.event_bus.update_state(
            active_task_id=None,
            active_task_base_sha=base_sha,
            active_phase="refactorer",
        )
        _, attempt_dir = self._next_attempt_dir("refactorer")
        self.event_bus.emit(
            EventType.REFACTOR_STARTED,
            role=AgentRole.REFACTORER,
            provider=self.registry.profile_name_for_role(AgentRole.REFACTORER),
            payload={
                "reason": reason,
                "accepted_since_last_refactor": self.event_bus.state.accepted_since_last_refactor,
            },
        )

        ref_profile = self.registry.for_role(AgentRole.REFACTORER).profile
        built_prompt = self.prompt_builder.build(
            role=AgentRole.REFACTORER,
            profile=ref_profile,
            repo_map_text=self._refresh_repo_map(),
            progress_text=self._read_progress_text(),
            extra_instructions=(
                f"Scheduled refactoring trigger: {reason}. "
                "Focus strictly on modules referenced in PROGRESS.md. "
                "Do not modify existing tests or Red Zone files."
            ),
        )
        response, guard_report = self._invoke_provider_guarded(
            role=AgentRole.REFACTORER,
            built_prompt=built_prompt,
            task_id=None,
            attempt_dir=attempt_dir,
            base_sha=base_sha,
        )

        if response.exit_code != 0 or response.timed_out or not guard_report.passed:
            self.git_ops.hard_rollback(base_sha)
            self.event_bus.emit(
                EventType.REFACTOR_REVERTED,
                role=AgentRole.REFACTORER,
                provider=response.provider_name,
                payload={"reason": guard_report.summary_text() or response.raw_stderr[:300]},
            )
            self.event_bus.update_state(accepted_since_last_refactor=0)
            return

        if not guard_report.changed_files:
            self.event_bus.emit(
                EventType.REFACTOR_FINISHED,
                role=AgentRole.REFACTORER,
                provider=response.provider_name,
                payload={"changed_files": [], "no_changes": True},
            )
            self.event_bus.update_state(accepted_since_last_refactor=0)
            return

        gates_res = self.gate_runner.run_all(attempt_dir=attempt_dir, fail_fast=False)
        atomic_write_text(attempt_dir / "gates.log", gates_res.combined_trimmed_log + "\n")
        if not gates_res.passed:
            self.git_ops.hard_rollback(base_sha)
            self.event_bus.emit(
                EventType.REFACTOR_REVERTED,
                role=AgentRole.REFACTORER,
                provider=response.provider_name,
                payload={"reason": "gates_failed", "log_path": str(attempt_dir / "gates.log")},
            )
            self.event_bus.update_state(accepted_since_last_refactor=0)
            return

        commit_sha: str | None = None
        if self.config.workflow.auto_commit:
            commit_sha = self.git_ops.commit_all(f"refactor(pipeline): scheduled cleanup ({reason})")
            self.event_bus.emit(
                EventType.GIT_COMMITTED,
                role=AgentRole.REFACTORER,
                payload={"commit_sha": commit_sha, "kind": "refactor"},
            )
        parsed = parse_worker_output(response.parsed_text)
        self._append_progress_entry(
            f"Scheduled Refactoring ({reason})",
            parsed.progress_update
            or f"Refactored files: {', '.join(guard_report.changed_files)}.",
        )
        self.event_bus.emit(
            EventType.REFACTOR_FINISHED,
            role=AgentRole.REFACTORER,
            provider=response.provider_name,
            payload={"changed_files": guard_report.changed_files, "commit_sha": commit_sha},
        )
        self.event_bus.update_state(accepted_since_last_refactor=0)

    # -------------------------------------------------------------------------
    # Rollback & Failure Helper
    # -------------------------------------------------------------------------

    def _fail_task_and_rollback(self, task: Task, base_sha: str, *, reason: str) -> None:
        self.git_ops.hard_rollback(base_sha)
        self.event_bus.emit(
            EventType.TASK_ROLLED_BACK,
            task_id=task.id,
            payload={"reason": reason, "base_sha": base_sha},
        )
        self.store.add_comment(
            task.id,
            author="orchestrator",
            kind="system",
            body=reason,
        )
        self._transition_task(
            task.id,
            TaskStatus.FAILED,
            failure_reason=reason,
            wip_commit_sha=None,
            pending_progress_note=None,
        )
        blocked_ids = self.store.cascade_block_dependents(task.id)
        for blocked_id in blocked_ids:
            self.event_bus.emit(
                EventType.TASK_STATUS_CHANGED,
                task_id=blocked_id,
                payload={"from": TaskStatus.TODO.value, "to": TaskStatus.BLOCKED.value, "blocked_by": task.id},
            )
        blocked_line = (
            f"\nBlocked dependents: {', '.join(f'#{bid}' for bid in blocked_ids)}"
            if blocked_ids
            else ""
        )
        self.notifier.notify(
            "task_failed",
            f"Task Failed #{task.id} ({self.config.project.name})",
            f"{task.title}\nReason: {reason[:300]}{blocked_line}",
        )
