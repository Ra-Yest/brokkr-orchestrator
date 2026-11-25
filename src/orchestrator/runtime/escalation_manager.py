"""Architectural escalation, hard-task auto-splitting, scheduled refactoring, and rollback."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

from orchestrator.config import PipelineConfig
from orchestrator.events import EventBus
from orchestrator.fs_utils import atomic_write_text
from orchestrator.gates import GateRunner
from orchestrator.git_ops import GitOps
from orchestrator.models import (
    AgentRole,
    EventType,
    ParsedWorkerOutput,
    Task,
    TaskStatus,
)
from orchestrator.notifications import BaseNotifier
from orchestrator.output_parser import parse_planner_subtasks, parse_worker_output
from orchestrator.prompt_builder import PromptBuilder
from orchestrator.providers.base import ProviderRegistry
from orchestrator.runtime.errors import OrchestratorFatalError, _PausedByInfraError
from orchestrator.runtime.lifecycle import TransitionTaskFn
from orchestrator.runtime.provider_executor import ProviderExecutor
from orchestrator.stores.base import BaseTaskStore


class EscalationManager:
    """Coordinates architect escalations, planner hard-task splits, refactorer runs, and task rollbacks."""

    def __init__(
        self,
        *,
        config: PipelineConfig,
        store: BaseTaskStore,
        registry: ProviderRegistry,
        event_bus: EventBus,
        git_ops: GitOps,
        gate_runner: GateRunner,
        prompt_builder: PromptBuilder,
        notifier: BaseNotifier,
        executor: ProviderExecutor,
        transition_task: TransitionTaskFn,
        refresh_repo_map: Callable[[], str],
        read_progress_text: Callable[[], str],
        append_progress_entry: Callable[[str, str], None],
        next_attempt_dir: Callable[[str], tuple[int, Path]],
    ) -> None:
        self.config = config
        self.store = store
        self.registry = registry
        self.event_bus = event_bus
        self.git_ops = git_ops
        self.gate_runner = gate_runner
        self.prompt_builder = prompt_builder
        self.notifier = notifier
        self._executor = executor
        self._transition_task = transition_task
        self._refresh_repo_map = refresh_repo_map
        self._read_progress_text = read_progress_text
        self._append_progress_entry = append_progress_entry
        self._next_attempt_dir = next_attempt_dir

    # -------------------------------------------------------------------------
    # Architectural Escalation (STATUS: NEEDS_ARCH_CHANGE)
    # -------------------------------------------------------------------------

    def resume_arch_pending_task(self, task: Task) -> None:
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
            self.fail_task_and_rollback(
                task,
                base_sha,
                reason=f"Architect could not resolve escalation: {guidance}",
            )
            return
        self._transition_task(task.id, TaskStatus.TODO)

    def handle_escalation(
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

        response, guard_report = self._executor.invoke_provider_guarded(
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

    def try_split_hard_task(self, task: Task, base_sha: str) -> bool:
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
        response, guard_report = self._executor.invoke_provider_guarded(
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

    def run_refactorer(self, *, reason: str) -> None:
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
        response, guard_report = self._executor.invoke_provider_guarded(
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

    def fail_task_and_rollback(self, task: Task, base_sha: str, *, reason: str) -> None:
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
