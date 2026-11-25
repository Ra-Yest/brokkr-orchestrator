"""Main headless AFK orchestrator runner with routing, batch review, escalation, splitting, and guards."""

from __future__ import annotations

import contextlib
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TextIO

from orchestrator.config import PipelineConfig
from orchestrator.control import ControlPlane, InboxManager
from orchestrator.diff_guard import DiffGuardReport
from orchestrator.events import EventBus, make_console_event_printer
from orchestrator.fs_utils import atomic_write_text, normalize_newlines
from orchestrator.gates import GateRunner, format_gate_execution_summary
from orchestrator.git_ops import GitOps
from orchestrator.models import (
    AgentRole,
    EventType,
    ParsedWorkerOutput,
    RunStatus,
    Task,
    TaskComplexity,
    TaskStatus,
    WorkerStatus,
    utc_now_iso,
)
from orchestrator.notifications import BaseNotifier, build_notifier
from orchestrator.output_parser import parse_worker_output
from orchestrator.process_utils import cleanup_ports, is_process_alive
from orchestrator.prompt_builder import PromptBuilder, generate_repo_map
from orchestrator.providers.base import ProviderRegistry
from orchestrator.proxy_monitor import BaseProxyMonitor, build_proxy_monitor
from orchestrator.reporter import write_report
from orchestrator.runtime.errors import (
    OrchestratorFatalError,
    RunnerLockError,
    _AbortedByControlError,
    _PausedByInfraError,
    _TaskDeferredOnCooldownError,
)
from orchestrator.runtime.escalation_manager import EscalationManager
from orchestrator.runtime.lifecycle import RunnerLifecycleManager
from orchestrator.runtime.provider_executor import (
    PIPELINE_INTEGRITY_EXCLUDES,
    ProviderExecutor,
    classify_provider_failure,
    is_transport_failure_before_response,
)
from orchestrator.runtime.review_pipeline import ReviewPipeline
from orchestrator.stores.base import BaseTaskStore

__all__ = [
    "PIPELINE_INTEGRITY_EXCLUDES",
    "OrchestratorFatalError",
    "OrchestratorRunner",
    "RunnerLockError",
    "classify_provider_failure",
    "is_process_alive",
    "is_transport_failure_before_response",
]


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
        self._sleep: Callable[[float], Any] = sleep_fn or time.sleep
        self._jitter: Callable[[float], float] = jitter_fn or (
            lambda d: max(0.0, d * random.uniform(0.85, 1.15)) if d > 0 else 0.0
        )

        self.repo_map_path = self.pipeline_dir / "REPO_MAP.md"
        self.progress_path = self.pipeline_dir / "PROGRESS.md"
        self.report_path = self.pipeline_dir / "REPORT.md"
        self.lock_path = self.pipeline_dir / "runner.lock"
        self.runs_dir = self.pipeline_dir / "runs" / self.event_bus.run_id

        self._last_gate_summaries: dict[str, str] = {}
        self._attempt_counter: dict[str, int] = {}

        self._lifecycle = RunnerLifecycleManager(
            repo_root=self.repo_root,
            lock_path=self.lock_path,
            config=self.config,
            store=self.store,
            git_ops=self.git_ops,
            event_bus=self.event_bus,
            control_plane=self.control_plane,
            notifier=self.notifier,
            sleep_fn=self._sleep,
            transition_task=self._transition_task,
            is_process_alive_fn=lambda pid: is_process_alive(pid),
        )
        self._executor = ProviderExecutor(
            repo_root=self.repo_root,
            pipeline_dir=self.pipeline_dir,
            config=self.config,
            registry=self.registry,
            event_bus=self.event_bus,
            git_ops=self.git_ops,
            notifier=self.notifier,
            proxy_monitor=self.proxy_monitor,
            lifecycle=self._lifecycle,
            jitter_fn=self._jitter,
            list_available_todo_tasks=self._list_available_todo_tasks,
            worker_role_for=self._worker_role_for,
        )
        self._escalation_manager = EscalationManager(
            config=self.config,
            store=self.store,
            registry=self.registry,
            event_bus=self.event_bus,
            git_ops=self.git_ops,
            gate_runner=self.gate_runner,
            prompt_builder=self.prompt_builder,
            notifier=self.notifier,
            executor=self._executor,
            transition_task=self._transition_task,
            refresh_repo_map=self._refresh_repo_map,
            read_progress_text=self._read_progress_text,
            append_progress_entry=self._append_progress_entry,
            next_attempt_dir=self._next_attempt_dir,
        )
        self._review_pipeline = ReviewPipeline(
            repo_root=self.repo_root,
            pipeline_dir=self.pipeline_dir,
            config=self.config,
            store=self.store,
            registry=self.registry,
            event_bus=self.event_bus,
            git_ops=self.git_ops,
            prompt_builder=self.prompt_builder,
            notifier=self.notifier,
            lifecycle=self._lifecycle,
            executor=self._executor,
            escalation_manager=self._escalation_manager,
            transition_task=self._transition_task,
            refresh_repo_map=self._refresh_repo_map,
            read_progress_text=self._read_progress_text,
            append_progress_entry=self._append_progress_entry,
            next_attempt_dir=self._next_attempt_dir,
            task_progress_counts=self._task_progress_counts,
            get_downstream_tasks=self._get_downstream_tasks,
            worker_role_for=self._worker_role_for,
            get_gate_summary=self._last_gate_summaries.get,
        )

    def run(self, *, max_tasks: int | None = None) -> RunStatus:
        """Execute the AFK pipeline loop until all eligible tasks finish, pause, abort, or fail-fast."""
        # Acquire PID lock BEFORE touching state.json or git so a second launch cannot corrupt a live run.
        stale_lock = self._lifecycle.acquire_runner_lock()

        try:
            if stale_lock is not None:
                self.event_bus.emit(
                    EventType.STALE_LOCK_RECOVERED,
                    payload=stale_lock,
                )

            self._lifecycle.recover_or_preflight(stale_lock_recovered=stale_lock is not None)
            self._ensure_progress_file()
            self._review_pipeline.sync_batch_queue_from_store()

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
                self._lifecycle.process_control_commands()
                if self._lifecycle.abort_requested:
                    raise _AbortedByControlError()

                if self.event_bus.state.pause_after_current or self.event_bus.state.paused:
                    self._lifecycle.wait_while_paused(reason="operator_pause")
                    if self._lifecycle.abort_requested:
                        raise _AbortedByControlError()

                if max_tasks is not None and tasks_processed >= max_tasks:
                    self._review_pipeline.flush_batch_review()
                    break

                # Resume any task left in ARCH_PENDING after a prior crash before picking new work
                arch_pending = [
                    t for t in self.store.list_tasks() if t.status == TaskStatus.ARCH_PENDING
                ]
                if arch_pending:
                    self._escalation_manager.resume_arch_pending_task(arch_pending[0])
                    continue

                # Resume any task checkpointed in NEEDS_REVIEW (e.g. after paused_infra on pre_reviewer/reviewer)
                needs_review = [
                    t for t in self.store.list_tasks() if t.status == TaskStatus.NEEDS_REVIEW
                ]
                if needs_review:
                    if self._review_pipeline.resume_needs_review_task(needs_review[0]):
                        tasks_processed += 1
                    continue

                next_task, woke_from_hibernation = self._select_next_runnable_task()
                if woke_from_hibernation:
                    continue

                if next_task is None:
                    if self._review_pipeline.get_batch_pending_tasks():
                        self._review_pipeline.flush_batch_review()
                        continue
                    # Stage / run complete: check periodic/end-of-stage refactoring
                    if (
                        self.config.workflow.refactor_on_stage_complete
                        and self.event_bus.state.accepted_since_last_refactor > 0
                    ):
                        self._escalation_manager.run_refactorer(reason="stage_or_queue_complete")
                    break

                # Stage boundary detection -> flush batch & optional end-of-stage refactor
                if last_stage is not None and next_task.stage != last_stage:
                    if self._review_pipeline.get_batch_pending_tasks():
                        self._review_pipeline.flush_batch_review()
                        continue
                    if (
                        self.config.workflow.refactor_on_stage_complete
                        and self.event_bus.state.accepted_since_last_refactor > 0
                    ):
                        self._escalation_manager.run_refactorer(
                            reason=f"stage_complete:{last_stage}"
                        )

                # Ensure batch independence BEFORE starting the next task
                pending_batch = self._review_pipeline.get_batch_pending_tasks()
                if pending_batch and self._review_pipeline.must_flush_batch_before(
                    next_task, pending_batch
                ):
                    self._review_pipeline.flush_batch_review()
                    continue

                last_stage = next_task.stage
                executed = self._execute_task(next_task)
                if not executed:
                    continue
                tasks_processed += 1

                # Flush batch if batch size threshold reached
                if (
                    len(self._review_pipeline.get_batch_pending_tasks())
                    >= self.config.workflow.batch_review_size
                ):
                    self._review_pipeline.flush_batch_review()

                # Trigger scheduled refactor every N accepted tasks
                refactor_n = self.config.workflow.refactor_every_n_tasks
                if (
                    refactor_n > 0
                    and self.event_bus.state.accepted_since_last_refactor >= refactor_n
                ):
                    if self._review_pipeline.get_batch_pending_tasks():
                        self._review_pipeline.flush_batch_review()
                    if self.event_bus.state.accepted_since_last_refactor >= refactor_n:
                        self._escalation_manager.run_refactorer(
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
            is_seam_pause = exc.pause_reason == "seam_audit_critical"
            self.event_bus.update_state(
                status=RunStatus.PAUSED,
                active_phase="paused_seam_audit" if is_seam_pause else "paused_infra",
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
            notify_title = (
                f"Pipeline Paused: Critical Seam Audit ({self.config.project.name})"
                if is_seam_pause
                else f"Pipeline Paused on Provider Error ({self.config.project.name})"
            )
            notify_body = (
                f"Run {self.event_bus.run_id} paused after critical seam audit finding: {exc.reason}"
                if is_seam_pause
                else f"Run {self.event_bus.run_id} paused without reverting approved/pending code: {exc.reason}"
            )
            self.notifier.notify("pause", notify_title, notify_body)
            return RunStatus.PAUSED

        except _AbortedByControlError:
            self._lifecycle.cleanup_aborted_run()
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
            self._lifecycle.release_runner_lock()

    # -------------------------------------------------------------------------
    # Progress, Artifacts, Reports & Task Transition Helpers
    # -------------------------------------------------------------------------

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

    def _worker_role_for(self, complexity: TaskComplexity) -> AgentRole:
        if complexity == TaskComplexity.EASY:
            return AgentRole.WORKER_EASY
        if complexity == TaskComplexity.HARD:
            return AgentRole.WORKER_HARD
        return AgentRole.WORKER_MEDIUM

    def _get_downstream_tasks(self, task_id: str) -> list[Task]:
        """Return direct downstream tasks that depend on task_id."""
        return [t for t in self.store.list_tasks() if task_id in t.depends_on]

    def _transition_task(self, task_id: str, status: TaskStatus, **fields: Any) -> Task:
        prev = self.store.get_task(task_id).status
        updated = self.store.update_status(task_id, status, **fields)
        self.event_bus.emit(
            EventType.TASK_STATUS_CHANGED,
            task_id=task_id,
            payload={"from": prev.value, "to": status.value},
        )
        return updated

    # -------------------------------------------------------------------------
    # Task Selection & Execution Lifecycle
    # -------------------------------------------------------------------------

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
            _, wait_seconds, candidates = self._executor.select_available_provider_for_role(
                worker_role
            )
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
        self._lifecycle.hibernate_until(
            resume_dt=resume_dt,
            providers=all_blocked_providers,
            task_id=earliest_task.id,
            role=earliest_role,
        )
        if self._lifecycle.abort_requested:
            raise _AbortedByControlError()
        return None, True

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
            self._lifecycle.check_control_after_step()
            if task.attempts >= max_attempts:
                self._escalation_manager.fail_task_and_rollback(
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
                    self._escalation_manager.fail_task_and_rollback(
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
                    downstream_tasks=self._get_downstream_tasks(task.id),
                    contracts=self.store.list_contracts(),
                    inbox_text=inbox_text,
                    gate_error_log=gate_error_log,
                    guard_error_text=guard_error_text,
                    review_feedback=review_feedback,
                    extra_instructions=extra_inst,
                )

                try:
                    response, guard_report = self._executor.invoke_provider_guarded(
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
                self._lifecycle.check_control_after_step()

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
                        self._escalation_manager.fail_task_and_rollback(
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
                    new_base, guidance, resolved = self._escalation_manager.handle_escalation(
                        task=task,
                        worker_output=parsed,
                        base_sha=task_base_sha,
                    )
                    if not resolved:
                        self._escalation_manager.fail_task_and_rollback(
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
                        self._escalation_manager.fail_task_and_rollback(
                            task,
                            task_base_sha,
                            reason=(
                                f"Reached max_attempts ({max_attempts}) after provider failure "
                                f"(exit_code={response.exit_code}, timed_out={response.timed_out}, "
                                f"idle_timed_out={response.idle_timed_out})."
                            ),
                        )
                        return True
                    stall_note = (
                        f" Reason: {response.metadata['stall_reason']}."
                        if response.metadata.get("stall_reason")
                        else ""
                    )
                    guard_error_text = (
                        f"Provider execution failed (exit_code={response.exit_code}, "
                        f"timed_out={response.timed_out}, idle_timed_out={response.idle_timed_out})."
                        f"{stall_note} Fix and complete the task.\n"
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
                            payload={"details": [v.message for v in diff_violations]},
                        )
                        diff_sig = tuple(sorted((v.code, v.path) for v in diff_violations))
                        if diff_sig == last_diff_guard_sig:
                            self._escalation_manager.fail_task_and_rollback(
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
                        self._escalation_manager.fail_task_and_rollback(
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
                        self._escalation_manager.fail_task_and_rollback(
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
                self._last_gate_summaries[task.id] = format_gate_execution_summary(
                    gates_res,
                    configured_gates=self.config.gates,
                    verify_cmd=task.verify_cmd,
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
                    and self._escalation_manager.try_split_hard_task(task, task_base_sha)
                ):
                    return True
                if new_rounds >= max_rounds:
                    self._escalation_manager.fail_task_and_rollback(
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

            # Route A (BATCH_PENDING) vs. Route B (NEEDS_REVIEW -> pre_reviewer / reviewer)
            outcome, task, rejected_feedback = self._review_pipeline.route_task_to_review(
                task=task,
                task_base_sha=task_base_sha,
                guard_report=last_guard_report,
                progress_update=last_worker_parsed.progress_update,
                last_report=last_worker_parsed.report,
                repo_map_text=repo_map_text,
                soft_warning_note=soft_warning_note,
                gate_summary=self._last_gate_summaries.get(task.id),
            )
            if outcome != "rejected":
                return True
            review_feedback = rejected_feedback
        return True
