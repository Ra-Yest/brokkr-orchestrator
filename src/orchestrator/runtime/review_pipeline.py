"""Single review, batch review queue, Evidence-Bound Review, and Pairwise Seam Audit pipeline."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from orchestrator.config import PipelineConfig
from orchestrator.diff_guard import DiffGuardReport, evaluate_diff, matches_any_glob
from orchestrator.events import EventBus
from orchestrator.fs_utils import atomic_write_json, normalize_newlines
from orchestrator.git_ops import GitOperationError, GitOps
from orchestrator.models import (
    AgentRole,
    ContractCheckResult,
    EventType,
    ParsedReviewVerdict,
    ReviewVerdictType,
    Task,
    TaskComplexity,
    TaskStatus,
    utc_now_iso,
)
from orchestrator.notifications import BaseNotifier
from orchestrator.output_parser import (
    parse_batch_review_verdicts,
    parse_seam_audit_output,
    parse_single_review_verdict,
)
from orchestrator.prompt_builder import PromptBuilder
from orchestrator.providers.base import ProviderRegistry
from orchestrator.runtime.errors import OrchestratorFatalError, _PausedByInfraError
from orchestrator.runtime.escalation_manager import EscalationManager
from orchestrator.runtime.lifecycle import RunnerLifecycleManager, TransitionTaskFn
from orchestrator.runtime.provider_executor import ProviderExecutor
from orchestrator.stores.base import BaseTaskStore


@dataclass(frozen=True)
class _CriticalSeamFinding:
    consumer_id: str
    seam_key: str
    summary: str
    report_path: Path
    provider_name: str


class ReviewPipeline:
    """Runs pre-review, master review, batch review, Evidence-Bound Review, and Pairwise Seam Audits."""

    def __init__(
        self,
        *,
        repo_root: Path,
        pipeline_dir: Path,
        config: PipelineConfig,
        store: BaseTaskStore,
        registry: ProviderRegistry,
        event_bus: EventBus,
        git_ops: GitOps,
        prompt_builder: PromptBuilder,
        notifier: BaseNotifier,
        lifecycle: RunnerLifecycleManager,
        executor: ProviderExecutor,
        escalation_manager: EscalationManager,
        transition_task: TransitionTaskFn,
        refresh_repo_map: Callable[[], str],
        read_progress_text: Callable[[], str],
        append_progress_entry: Callable[[str, str], None],
        next_attempt_dir: Callable[[str], tuple[int, Path]],
        task_progress_counts: Callable[[], tuple[int, int, int]],
        get_downstream_tasks: Callable[[str], list[Task]],
        worker_role_for: Callable[[TaskComplexity], AgentRole],
        get_gate_summary: Callable[[str], str | None],
    ) -> None:
        self.repo_root = repo_root
        self.pipeline_dir = pipeline_dir
        self.config = config
        self.store = store
        self.registry = registry
        self.event_bus = event_bus
        self.git_ops = git_ops
        self.prompt_builder = prompt_builder
        self.notifier = notifier
        self._lifecycle = lifecycle
        self._executor = executor
        self._escalation_manager = escalation_manager
        self._transition_task = transition_task
        self._refresh_repo_map = refresh_repo_map
        self._read_progress_text = read_progress_text
        self._append_progress_entry = append_progress_entry
        self._next_attempt_dir = next_attempt_dir
        self._task_progress_counts = task_progress_counts
        self._get_downstream_tasks = get_downstream_tasks
        self._worker_role_for = worker_role_for
        self._get_gate_summary = get_gate_summary

        self._batch_diffs: dict[str, str] = {}
        self._batch_diff_stats: dict[str, str] = {}

    # -------------------------------------------------------------------------
    # Batch Queue Helpers
    # -------------------------------------------------------------------------

    def sync_batch_queue_from_store(self) -> None:
        pending_ids = [t.id for t in self.store.list_tasks() if t.status == TaskStatus.BATCH_PENDING]
        self.event_bus.update_state(batch_queue=pending_ids)

    def get_batch_pending_tasks(self) -> list[Task]:
        return [t for t in self.store.list_tasks() if t.status == TaskStatus.BATCH_PENDING]

    def must_flush_batch_before(self, next_task: Task, pending_batch: list[Task]) -> bool:
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
    # Evidence-Bound Review
    # -------------------------------------------------------------------------

    def required_clause_ids_for_task(self, task: Task) -> list[str]:
        """Collect deterministic deduplicated clause IDs from contracts in task.provides/consumes."""
        if not task.provides and not task.consumes:
            return []
        contracts = self.store.list_contracts()
        if not contracts:
            return []
        required: list[str] = []
        seen: set[str] = set()
        for cid in (*task.provides, *task.consumes):
            spec = contracts.get(cid)
            if spec is None:
                continue
            for clause in spec.clauses:
                if clause.id not in seen:
                    seen.add(clause.id)
                    required.append(clause.id)
        return required

    def _check_evidence_bound_review(
        self,
        task: Task,
        verdict: ParsedReviewVerdict,
    ) -> tuple[bool, list[str], str]:
        """Verify that an APPROVE verdict supplies VERIFIED + non-empty evidence for every required clause."""
        required_ids = self.required_clause_ids_for_task(task)
        if not required_ids:
            return True, [], ""

        checks_by_id: dict[str, ContractCheckResult] = {
            chk.clause_id: chk for chk in verdict.contract_checks
        }
        failed_ids: list[str] = []
        issue_lines: list[str] = []
        for clause_id in required_ids:
            entry = checks_by_id.get(clause_id)
            if entry is None:
                failed_ids.append(clause_id)
                issue_lines.append(
                    f"- `{clause_id}`: missing from BEGIN_CONTRACT_CHECKS_JSON"
                )
            elif entry.status != "VERIFIED":
                failed_ids.append(clause_id)
                detail = f" ({entry.evidence.strip()})" if entry.evidence.strip() else ""
                issue_lines.append(
                    f"- `{clause_id}`: status={entry.status}{detail}"
                )
            elif not entry.evidence.strip():
                failed_ids.append(clause_id)
                issue_lines.append(
                    f"- `{clause_id}`: status=VERIFIED but missing non-empty evidence citation"
                )

        if not failed_ids:
            return True, [], ""

        header = (
            "Evidence-Bound Review failed (`evidence_bound_review_failed`): "
            "reviewer APPROVE was overridden to REJECT because the following active contract "
            "clause(s) were missing, unverified, or lacked evidence:\n"
            + "\n".join(issue_lines)
        )
        original = verdict.feedback.strip()
        feedback = f"{header}\n\nOriginal reviewer notes:\n{original}" if original else header
        return False, failed_ids, feedback

    # -------------------------------------------------------------------------
    # Post-Gates Routing (Batch Pending vs. Individual Review)
    # -------------------------------------------------------------------------

    def route_task_to_review(
        self,
        *,
        task: Task,
        task_base_sha: str,
        guard_report: DiffGuardReport,
        progress_update: str | None,
        last_report: str,
        repo_map_text: str,
        soft_warning_note: str | None,
        gate_summary: str | None,
    ) -> tuple[Literal["batched", "done", "failed_or_split", "rejected"], Task, str | None]:
        """Route a gate-passing worker task to BATCH_PENDING (Route A) or NEEDS_REVIEW (Route B)."""
        existing_batch_files = {
            fp for bt in self.get_batch_pending_tasks() for fp in bt.changed_files
        }
        overlaps_batch = not existing_batch_files.isdisjoint(set(guard_report.changed_files))

        has_contract_clauses = bool(self.required_clause_ids_for_task(task))
        if (
            task.complexity == TaskComplexity.EASY
            and not guard_report.touched_yellow_zone
            and not guard_report.has_soft_warnings
            and not overlaps_batch
            and not has_contract_clauses
            and self.config.workflow.batch_review_size > 1
        ):
            batch_raw_diff = self.git_ops.get_diff(task_base_sha)
            batch_diff_stat = self.git_ops.get_diff_stat(task_base_sha)
            wip_sha: str | None = None
            if guard_report.changed_files and self.config.workflow.auto_commit:
                wip_sha = self.git_ops.commit_all(f"wip(task-{task.id}): {task.title}")
                self.event_bus.emit(
                    EventType.GIT_COMMITTED,
                    task_id=task.id,
                    payload={"commit_sha": wip_sha, "wip": True},
                )
            # IMPORTANT: Do NOT update PROGRESS.md yet — only after Master APPROVE!
            pending_progress = progress_update or (
                f"Completed `#{task.id}` ({task.title}). "
                f"Files: {', '.join(guard_report.changed_files) or 'none'}."
            )
            self._batch_diffs[task.id] = batch_raw_diff or guard_report.filtered_diff
            self._batch_diff_stats[task.id] = batch_diff_stat
            task = self._transition_task(
                task.id,
                TaskStatus.BATCH_PENDING,
                wip_commit_sha=wip_sha,
                changed_files=guard_report.changed_files,
                touched_yellow_zone=False,
                pending_progress_note=pending_progress,
                last_report=last_report,
            )
            self.sync_batch_queue_from_store()
            return "batched", task, None

        # Route B: Medium, Hard, Yellow-Zone-triggered, or Soft-Warning Easy -> Individual review
        pending_progress_note = progress_update or (
            f"Completed `#{task.id}` ({task.title}). "
            f"Files: {', '.join(guard_report.changed_files) or 'none'}."
        )
        task = self._transition_task(
            task.id,
            TaskStatus.NEEDS_REVIEW,
            changed_files=guard_report.changed_files,
            touched_yellow_zone=guard_report.touched_yellow_zone,
            pending_progress_note=pending_progress_note,
            last_report=last_report,
            pre_review_approved=False,
        )

        outcome, task, rejected_feedback = self.run_needs_review_pipeline(
            task=task,
            task_base_sha=task_base_sha,
            guard_report=guard_report,
            last_report=last_report,
            progress_note=pending_progress_note,
            repo_map_text=repo_map_text,
            soft_warning_note=soft_warning_note,
            gate_summary=gate_summary,
            on_reject_status=TaskStatus.IN_PROGRESS,
        )
        return outcome, task, rejected_feedback

    # -------------------------------------------------------------------------
    # Single & Needs-Review Pipeline
    # -------------------------------------------------------------------------

    def run_needs_review_pipeline(
        self,
        *,
        task: Task,
        task_base_sha: str,
        guard_report: DiffGuardReport,
        last_report: str,
        progress_note: str,
        repo_map_text: str,
        soft_warning_note: str | None,
        gate_summary: str | None = None,
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
                gate_summary=gate_summary,
            )
            self._lifecycle.check_control_after_step()
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
                    and self._escalation_manager.try_split_hard_task(task, task_base_sha)
                ):
                    return "failed_or_split", task, None
                if new_rounds >= max_rounds:
                    self._escalation_manager.fail_task_and_rollback(
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
            gate_summary=gate_summary,
        )
        self._lifecycle.check_control_after_step()

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
            self._run_seam_audits_for_consumer(task)
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
            and self._escalation_manager.try_split_hard_task(task, task_base_sha)
        ):
            return "failed_or_split", task, None
        if new_rounds >= max_rounds:
            self._escalation_manager.fail_task_and_rollback(
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

    def resume_needs_review_task(self, task: Task) -> bool:
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
            self._lifecycle.pop_resumable_needs_review_base_sha(task.id)
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
        outcome, _, _ = self.run_needs_review_pipeline(
            task=task,
            task_base_sha=task_base_sha,
            guard_report=guard_report,
            last_report=task.last_report or "",
            progress_note=progress_note,
            repo_map_text=repo_map_text,
            soft_warning_note=soft_warning_note,
            gate_summary=self._get_gate_summary(task.id),
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
        gate_summary: str | None = None,
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
            downstream_tasks=self._get_downstream_tasks(task.id),
            contracts=self.store.list_contracts(),
            filtered_diff=review_diff_input,
            diff_stat=diff_stat,
            last_report=last_report,
            gate_summary=gate_summary,
            extra_instructions=soft_warning_note,
        )
        response, guard_report = self._executor.invoke_provider_guarded(
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
        if response.exit_code != 0 or response.timed_out:
            self.event_bus.emit(
                event_type,
                task_id=task.id,
                role=role,
                provider=response.provider_name,
                payload={"verdict": verdict.verdict.value},
            )
            raise _PausedByInfraError(
                reason=(
                    f"{role.value} provider '{response.provider_name}' failed "
                    f"(exit_code={response.exit_code}, timed_out={response.timed_out})"
                ),
                task_id=task.id,
                role=role,
                provider=response.provider_name,
            )

        effective_verdict = verdict.verdict
        effective_feedback = verdict.feedback
        event_payload: dict[str, Any] = {"verdict": effective_verdict.value}

        if effective_verdict == ReviewVerdictType.APPROVE:
            ebr_ok, failed_clauses, ebr_feedback = self._check_evidence_bound_review(task, verdict)
            if not ebr_ok:
                effective_verdict = ReviewVerdictType.REJECT
                effective_feedback = ebr_feedback
                event_payload = {
                    "verdict": effective_verdict.value,
                    "raw_verdict": ReviewVerdictType.APPROVE.value,
                    "reason": "evidence_bound_review_failed",
                    "failed_clauses": failed_clauses,
                }

        self.event_bus.emit(
            event_type,
            task_id=task.id,
            role=role,
            provider=response.provider_name,
            payload=event_payload,
        )
        return effective_verdict == ReviewVerdictType.APPROVE, effective_feedback

    # -------------------------------------------------------------------------
    # Batch Review (Easy Independent Tasks)
    # -------------------------------------------------------------------------

    def flush_batch_review(self) -> None:
        batch_tasks = self.get_batch_pending_tasks()
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

        response, guard_report = self._executor.invoke_provider_guarded(
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

        self.sync_batch_queue_from_store()

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

        critical_seams: list[_CriticalSeamFinding] = []
        for approved_task in approved_tasks:
            refreshed = self.store.get_task(approved_task.id)
            critical_seams.extend(
                self._run_seam_audits_for_consumer(refreshed, pause_on_critical=False)
            )
        if critical_seams and self.config.workflow.seam_audit_pause_on_critical:
            self._raise_critical_seam_pause(critical_seams)

    # -------------------------------------------------------------------------
    # Pairwise Seam Audit at Contract Closure Points (v0.3.2)
    # -------------------------------------------------------------------------

    @staticmethod
    def _sanitize_seam_token(token: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", token.strip())
        return cleaned or "unknown"

    def _collect_seam_fallback_files(
        self,
        *,
        surface: str,
        changed_files: list[str],
    ) -> dict[str, str]:
        candidates: list[str] = []
        if surface.strip():
            raw_path = surface.strip().split(":", 1)[0].strip()
            if raw_path and " " not in raw_path:
                candidates.append(raw_path.replace("\\", "/"))
        for fp in changed_files:
            norm = fp.replace("\\", "/")
            if norm not in candidates:
                candidates.append(norm)

        contents: dict[str, str] = {}
        for rel in candidates:
            full = (self.repo_root / rel).resolve()
            try:
                full.relative_to(self.repo_root)
            except ValueError:
                continue
            if full.is_file():
                contents[rel] = normalize_newlines(full.read_text(encoding="utf-8", errors="replace"))
        return contents

    def _raise_critical_seam_pause(
        self,
        critical_seams: list[_CriticalSeamFinding],
    ) -> None:
        """Raise a pause error summarizing all critical seam audit findings."""
        if len(critical_seams) == 1:
            finding = critical_seams[0]
            reason = (
                f"Critical seam audit finding for {finding.seam_key}: "
                f"{finding.summary} (report: {finding.report_path})"
            )
        else:
            details = "; ".join(
                f"{finding.seam_key}: {finding.summary} (report: {finding.report_path})"
                for finding in critical_seams
            )
            reason = f"Critical seam audit findings ({len(critical_seams)}): {details}"

        raise _PausedByInfraError(
            reason=reason,
            pause_reason="seam_audit_critical",
            task_id=",".join(dict.fromkeys(f.consumer_id for f in critical_seams)),
            role=AgentRole.SEAM_AUDITOR,
            provider=",".join(dict.fromkeys(f.provider_name for f in critical_seams)),
        )

    def _run_seam_audits_for_consumer(
        self,
        consumer: Task,
        *,
        pause_on_critical: bool = True,
    ) -> list[_CriticalSeamFinding]:
        """Run read-only pairwise seam audits when a committed consumer closes a contract."""
        if not self.config.workflow.seam_audit_enabled:
            return []
        if not consumer.consumes:
            return []
        contracts = self.store.list_contracts()
        if not contracts:
            return []

        done_tasks = [
            t
            for t in self.store.list_tasks()
            if t.status == TaskStatus.DONE and t.resolution != "split"
        ]

        critical_seams: list[_CriticalSeamFinding] = []

        for contract_id in consumer.consumes:
            spec = contracts.get(contract_id)
            if spec is None:
                continue
            producers = [
                t
                for t in done_tasks
                if t.id != consumer.id and contract_id in t.provides
            ]
            for producer in producers:
                seam_key = f"{contract_id}:{producer.id}:{consumer.id}"
                if seam_key in self.event_bus.state.audited_seams:
                    continue

                producer_diff = (
                    self.git_ops.get_commit_diff(producer.final_commit_sha)
                    if producer.final_commit_sha
                    else ""
                )
                consumer_diff = (
                    self.git_ops.get_commit_diff(consumer.final_commit_sha)
                    if consumer.final_commit_sha
                    else ""
                )
                producer_files = (
                    None
                    if producer_diff.strip()
                    else self._collect_seam_fallback_files(
                        surface=spec.surface,
                        changed_files=producer.changed_files,
                    )
                )
                consumer_files = (
                    None
                    if consumer_diff.strip()
                    else self._collect_seam_fallback_files(
                        surface=spec.surface,
                        changed_files=consumer.changed_files,
                    )
                )

                safe_cid = self._sanitize_seam_token(contract_id)
                safe_prod = self._sanitize_seam_token(producer.id)
                safe_cons = self._sanitize_seam_token(consumer.id)
                _, attempt_dir = self._next_attempt_dir(
                    f"{consumer.id}_seam_audit_{safe_cid}_{safe_prod}"
                )
                auditor_profile = self.registry.for_role(AgentRole.SEAM_AUDITOR).profile
                built_prompt = self.prompt_builder.build_seam_audit_prompt(
                    profile=auditor_profile,
                    repo_map_text=self._refresh_repo_map(),
                    progress_text=self._read_progress_text(),
                    contract_id=contract_id,
                    contract=spec,
                    producer=producer,
                    consumer=consumer,
                    producer_diff=producer_diff,
                    consumer_diff=consumer_diff,
                    producer_files=producer_files,
                    consumer_files=consumer_files,
                )

                base_sha = self.git_ops.get_head_sha()
                response, guard_report = self._executor.invoke_provider_guarded(
                    role=AgentRole.SEAM_AUDITOR,
                    built_prompt=built_prompt,
                    task_id=consumer.id,
                    attempt_dir=attempt_dir,
                    base_sha=base_sha,
                )
                if not guard_report.passed:
                    self.event_bus.emit(
                        EventType.RED_ZONE_VIOLATION,
                        task_id=consumer.id,
                        role=AgentRole.SEAM_AUDITOR,
                        provider=response.provider_name,
                        payload={
                            "reason": "read_only_role_modified_files",
                            "seam_key": seam_key,
                            "details": guard_report.summary_text(),
                        },
                    )
                    self.git_ops.hard_rollback(base_sha)
                    raise OrchestratorFatalError(
                        f"Read-only role '{AgentRole.SEAM_AUDITOR.value}' modified repository files "
                        f"during seam audit of {seam_key}: {guard_report.summary_text()}"
                    )

                if response.exit_code != 0 or response.timed_out:
                    raise _PausedByInfraError(
                        reason=(
                            f"seam_auditor provider '{response.provider_name}' failed on {seam_key} "
                            f"(exit_code={response.exit_code}, timed_out={response.timed_out})"
                        ),
                        task_id=consumer.id,
                        role=AgentRole.SEAM_AUDITOR,
                        provider=response.provider_name,
                    )

                audit_output = parse_seam_audit_output(response.parsed_text)
                report_path = (
                    self.pipeline_dir
                    / "seam_audits"
                    / f"{safe_cid}__{safe_prod}__{safe_cons}.json"
                )
                atomic_write_json(
                    report_path,
                    {
                        "seam_key": seam_key,
                        "contract_id": contract_id,
                        "producer_id": producer.id,
                        "consumer_id": consumer.id,
                        "provider": response.provider_name,
                        "timestamp": utc_now_iso(),
                        **audit_output.model_dump(mode="json"),
                    },
                )

                updated_seams = [*self.event_bus.state.audited_seams, seam_key]
                self.event_bus.update_state(audited_seams=updated_seams)
                self.event_bus.emit(
                    EventType.SEAM_AUDIT_COMPLETED,
                    task_id=consumer.id,
                    role=AgentRole.SEAM_AUDITOR,
                    provider=response.provider_name,
                    payload={
                        "seam_key": seam_key,
                        "contract_id": contract_id,
                        "producer_id": producer.id,
                        "consumer_id": consumer.id,
                        "verdict": audit_output.verdict,
                        "summary": audit_output.summary,
                        "findings_count": len(audit_output.findings),
                        "report_path": str(report_path),
                    },
                )

                if audit_output.verdict == "CRITICAL":
                    critical_seams.append(
                        _CriticalSeamFinding(
                            consumer_id=consumer.id,
                            seam_key=seam_key,
                            summary=audit_output.summary,
                            report_path=report_path,
                            provider_name=response.provider_name,
                        )
                    )

        if (
            critical_seams
            and pause_on_critical
            and self.config.workflow.seam_audit_pause_on_critical
        ):
            self._raise_critical_seam_pause(critical_seams)

        return critical_seams
