"""Structured event bus (events.jsonl), atomic state snapshot (state.json), and rolling quota tracker."""

from __future__ import annotations

import contextlib
import json
import sys
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TextIO

from orchestrator.config import PipelineConfig
from orchestrator.fs_utils import append_jsonl, atomic_write_json
from orchestrator.models import (
    AgentRole,
    EventType,
    PipelineEvent,
    PipelineState,
    ProviderQuotaState,
    RunStatus,
    utc_now_iso,
)


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


class QuotaTracker:
    """Tracks rolling 5-hour (configurable) request windows and cooldowns per provider profile."""

    def __init__(self, quotas: dict[str, ProviderQuotaState]) -> None:
        self.quotas = quotas

    @classmethod
    def from_config(
        cls,
        config: PipelineConfig,
        existing: dict[str, ProviderQuotaState] | None = None,
    ) -> QuotaTracker:
        quotas: dict[str, ProviderQuotaState] = {}
        prev = existing or {}
        for name, profile in config.providers.items():
            if name in prev:
                state = prev[name].model_copy(
                    update={
                        "window_seconds": profile.quota_window_seconds,
                        "max_requests": profile.quota_max_requests,
                    }
                )
            else:
                state = ProviderQuotaState(
                    window_seconds=profile.quota_window_seconds,
                    max_requests=profile.quota_max_requests,
                )
            quotas[name] = state
        tracker = cls(quotas)
        tracker.prune_all()
        return tracker

    def prune_all(self, now: datetime | None = None) -> None:
        current = now or datetime.now(UTC)
        for name, q in self.quotas.items():
            cutoff = current - timedelta(seconds=q.window_seconds)
            kept = [ts for ts in q.request_timestamps if _parse_iso(ts) >= cutoff]
            cooldown = q.cooldown_until
            if cooldown is not None and _parse_iso(cooldown) <= current:
                cooldown = None
            self.quotas[name] = q.model_copy(
                update={
                    "request_timestamps": kept,
                    "requests_in_window": len(kept),
                    "cooldown_until": cooldown,
                }
            )

    def record_request(self, provider_name: str, now: datetime | None = None) -> ProviderQuotaState:
        current = now or datetime.now(UTC)
        self.prune_all(current)
        q = self.quotas.get(provider_name) or ProviderQuotaState()
        timestamps = [*q.request_timestamps, current.isoformat().replace("+00:00", "Z")]
        updated = q.model_copy(
            update={
                "request_timestamps": timestamps,
                "requests_in_window": len(timestamps),
            }
        )
        self.quotas[provider_name] = updated
        return updated

    def record_rate_limit(
        self,
        provider_name: str,
        cooldown_seconds: int,
        now: datetime | None = None,
    ) -> datetime:
        current = now or datetime.now(UTC)
        self.prune_all(current)
        resume_dt = current + timedelta(seconds=cooldown_seconds)
        resume_iso = resume_dt.isoformat().replace("+00:00", "Z")
        now_iso = current.isoformat().replace("+00:00", "Z")
        q = self.quotas.get(provider_name) or ProviderQuotaState()
        self.quotas[provider_name] = q.model_copy(
            update={
                "cooldown_until": resume_iso,
                "last_rate_limited_at": now_iso,
            }
        )
        return resume_dt

    def seconds_until_available(self, provider_name: str, now: datetime | None = None) -> int:
        current = now or datetime.now(UTC)
        self.prune_all(current)
        q = self.quotas.get(provider_name)
        if q is None:
            return 0
        wait_seconds = 0
        if q.cooldown_until is not None:
            delta = int((_parse_iso(q.cooldown_until) - current).total_seconds())
            wait_seconds = max(wait_seconds, delta)
        if q.max_requests is not None and q.requests_in_window >= q.max_requests and q.request_timestamps:
            oldest = _parse_iso(q.request_timestamps[0])
            window_release = oldest + timedelta(seconds=q.window_seconds)
            delta = int((window_release - current).total_seconds())
            wait_seconds = max(wait_seconds, delta)
        return max(0, wait_seconds)


def _format_local_hms(iso_timestamp: str) -> str:
    try:
        return _parse_iso(iso_timestamp).astimezone().strftime("%H:%M:%S")
    except Exception:
        return datetime.now().strftime("%H:%M:%S")


def format_console_event(event: PipelineEvent) -> str | None:
    """Format key pipeline events into compact `[HH:MM:SS]` one-liners for live stdout."""
    ts = _format_local_hms(event.timestamp)
    et = event.event_type
    p = event.payload
    task_tag = f"#{event.task_id} " if event.task_id else ""
    role_val = event.role.value if event.role is not None else "-"
    prov_val = event.provider or "-"

    if et == EventType.RUN_STARTED:
        max_part = f" max_tasks={p.get('max_tasks')}" if p.get("max_tasks") is not None else ""
        return f"[{ts}] run_started project={p.get('project')} run_id={event.run_id}{max_part}"

    if et == EventType.TASK_CLAIMED:
        return (
            f"[{ts}] task_claimed #{event.task_id} ({p.get('complexity', '?')}) "
            f"role={role_val} provider={prov_val}"
        )

    if et == EventType.PROVIDER_INVOKED:
        return (
            f"[{ts}] provider_invoked {task_tag}role={role_val} "
            f"provider={prov_val} ({p.get('prompt_chars', 0)} chars)"
        )

    if et == EventType.PROVIDER_SUCCEEDED:
        changed = p.get("changed_files")
        file_count = len(changed) if isinstance(changed, list) else 0
        return (
            f"[{ts}] provider_succeeded {task_tag}role={role_val} "
            f"provider={prov_val} duration={p.get('duration_seconds', 0)}s files={file_count}"
        )

    if et == EventType.PROVIDER_FAILED:
        return (
            f"[{ts}] provider_failed {task_tag}role={role_val} "
            f"provider={prov_val} exit={p.get('exit_code')} timed_out={p.get('timed_out')}"
        )

    if et == EventType.PROVIDER_RATE_LIMITED:
        return (
            f"[{ts}] provider_rate_limited {task_tag}provider={prov_val} "
            f"cooldown={p.get('cooldown_seconds')}s"
        )

    if et == EventType.PROVIDER_TRANSIENT_RETRY:
        return (
            f"[{ts}] provider_transient_retry {task_tag}provider={prov_val} "
            f"attempt={p.get('attempt')}/{p.get('max_attempts')} delay={p.get('delay_seconds')}s"
        )

    if et == EventType.GATE_PASSED:
        gates = p.get("gates")
        gate_names = ",".join(str(g) for g in gates) if isinstance(gates, list) else ""
        return f"[{ts}] gate_passed {task_tag}gates={gate_names or 'none'}"

    if et == EventType.GATE_FAILED:
        failed = p.get("failed_gates")
        names = ",".join(str(g) for g in failed) if isinstance(failed, list) else ""
        return f"[{ts}] gate_failed {task_tag}failed={names or 'unknown'}"

    if et == EventType.DIFF_GUARD_FAILED:
        details = p.get("details")
        count = len(details) if isinstance(details, list) else 0
        first = str(details[0])[:120] if isinstance(details, list) and details else ""
        suffix = f" ({first})" if first else ""
        return f"[{ts}] diff_guard_failed {task_tag}violations={count}{suffix}"

    if et == EventType.PLACEHOLDER_GUARD_FAILED:
        return f"[{ts}] placeholder_guard_failed {task_tag}count={p.get('count', 0)}"

    if et == EventType.RED_ZONE_VIOLATION:
        violations = p.get("violations")
        if isinstance(violations, list) and violations:
            codes = ",".join(
                str(v.get("code", "RED")) for v in violations[:3] if isinstance(v, dict)
            )
            return f"[{ts}] red_zone_violation {task_tag}role={role_val} codes={codes}"
        reason = str(p.get("reason", "violation"))
        return f"[{ts}] red_zone_violation {task_tag}role={role_val} reason={reason}"

    if et == EventType.YELLOW_ZONE_TRIGGERED:
        yfiles = p.get("yellow_files")
        joined = ",".join(str(f) for f in yfiles[:5]) if isinstance(yfiles, list) else ""
        return f"[{ts}] yellow_zone_triggered {task_tag}files={joined}"

    if et == EventType.PRE_REVIEW_COMPLETED:
        return f"[{ts}] pre_review_completed {task_tag}verdict={p.get('verdict')}"

    if et == EventType.REVIEW_COMPLETED:
        return f"[{ts}] review_completed {task_tag}verdict={p.get('verdict')}"

    if et == EventType.BATCH_REVIEW_STARTED:
        tids = p.get("task_ids")
        joined = ",".join(str(t) for t in tids) if isinstance(tids, list) else ""
        return f"[{ts}] batch_review_started tasks={joined}"

    if et == EventType.BATCH_VERDICT_RECEIVED:
        return f"[{ts}] batch_verdict_received {task_tag}verdict={p.get('verdict')}"

    if et == EventType.BATCH_REVIEW_DEFERRED:
        tids = p.get("task_ids")
        joined = ",".join(str(t) for t in tids) if isinstance(tids, list) else ""
        return f"[{ts}] batch_review_deferred tasks={joined}"

    if et == EventType.ESCALATION_TRIGGERED:
        summary = str(p.get("summary", ""))[:120].replace("\n", " ")
        return f"[{ts}] escalation_triggered {task_tag}-> architect ({summary})"

    if et == EventType.ARCHITECT_RESOLVED:
        changed = p.get("changed_files")
        file_count = len(changed) if isinstance(changed, list) else 0
        return f"[{ts}] architect_resolved {task_tag}files={file_count}"

    if et == EventType.TASK_SPLIT:
        subs = p.get("subtask_ids")
        joined = ",".join(str(s) for s in subs) if isinstance(subs, list) else ""
        return f"[{ts}] task_split {task_tag}-> {joined}"

    if et == EventType.REFACTOR_STARTED:
        return f"[{ts}] refactor_started reason={p.get('reason')}"

    if et == EventType.REFACTOR_FINISHED:
        changed = p.get("changed_files")
        file_count = len(changed) if isinstance(changed, list) else 0
        return f"[{ts}] refactor_finished files={file_count}"

    if et == EventType.REFACTOR_REVERTED:
        return f"[{ts}] refactor_reverted"

    if et == EventType.GIT_COMMITTED:
        sha = str(p.get("commit_sha", ""))[:10]
        wip = " (wip)" if p.get("wip") else ""
        return f"[{ts}] git_committed {task_tag}sha={sha}{wip}"

    if et == EventType.GIT_REVERTED:
        sha = str(p.get("revert_commit", ""))[:10]
        return f"[{ts}] git_reverted {task_tag}sha={sha}"

    if et == EventType.TASK_STATUS_CHANGED:
        to_status = str(p.get("to", ""))
        if to_status in ("done", "failed", "blocked"):
            return f"[{ts}] task_{to_status} {task_tag}({p.get('from')} -> {to_status})"
        return None

    if et == EventType.CONTROL_COMMAND_APPLIED:
        return f"[{ts}] control_command_applied action={p.get('action')} ({p.get('result')})"

    if et == EventType.PAUSE_ENTERED:
        resume_part = f" resume_at={p.get('resume_at')}" if p.get("resume_at") else ""
        return f"[{ts}] pause_entered {task_tag}reason={p.get('reason')}{resume_part}"

    if et == EventType.PAUSE_RESUMED:
        reason = p.get("reason") or "operator_resume"
        return f"[{ts}] pause_resumed {task_tag}reason={reason}"

    if et == EventType.RUN_FINISHED:
        return f"[{ts}] run_finished tasks_processed={p.get('tasks_processed', 0)}"

    if et == EventType.RUN_ABORTED:
        return f"[{ts}] run_aborted reason={p.get('reason', 'control_abort')}"

    if et == EventType.ORCHESTRATOR_FATAL_ERROR:
        msg = str(p.get("message", ""))[:200].replace("\n", " ")
        return f"[{ts}] orchestrator_fatal_error {p.get('error_type')}: {msg}"

    return None


def make_console_event_printer(
    stream: TextIO | None = None,
) -> Callable[[PipelineEvent], None]:
    """Build a fail-safe EventBus callback that prints compact live progress lines."""

    def _printer(event: PipelineEvent) -> None:
        line = format_console_event(event)
        if line is None:
            return
        target = sys.stdout if stream is None else stream
        with contextlib.suppress(OSError):
            print(line, file=target, flush=True)

    return _printer


class EventBus:
    """Headless event stream (`events.jsonl`) and atomic state manager (`state.json`)."""

    def __init__(
        self,
        pipeline_dir: Path,
        config: PipelineConfig,
        *,
        run_id: str | None = None,
        defer_save: bool = False,
        on_event: Callable[[PipelineEvent], None] | None = None,
    ) -> None:
        self.pipeline_dir = pipeline_dir
        self.pipeline_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.pipeline_dir / "events.jsonl"
        self.state_path = self.pipeline_dir / "state.json"
        self.run_id = run_id or self._generate_run_id()
        self._on_event = on_event

        previous_state = self._load_existing_state()
        self.previous_state: PipelineState | None = previous_state
        self.quota_tracker = QuotaTracker.from_config(
            config,
            existing=previous_state.provider_quotas if previous_state else None,
        )
        self.state = PipelineState(
            run_id=self.run_id,
            status=RunStatus.IDLE,
            accepted_since_last_refactor=(
                previous_state.accepted_since_last_refactor if previous_state else 0
            ),
            batch_queue=list(previous_state.batch_queue) if previous_state else [],
            processed_control_ids=(
                list(previous_state.processed_control_ids[-500:]) if previous_state else []
            ),
            last_control_command_id=(
                previous_state.last_control_command_id if previous_state else None
            ),
            provider_quotas=dict(self.quota_tracker.quotas),
        )
        if not defer_save:
            self.save_state()

    def set_on_event(self, callback: Callable[[PipelineEvent], None] | None) -> None:
        self._on_event = callback

    @staticmethod
    def _generate_run_id() -> str:
        stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        return f"run_{stamp}_{uuid.uuid4().hex[:6]}"

    def _load_existing_state(self) -> PipelineState | None:
        if not self.state_path.exists():
            return None
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            return PipelineState.model_validate(raw)
        except Exception:
            return None

    def save_state(self) -> None:
        self.quota_tracker.prune_all()
        self.state = self.state.model_copy(
            update={
                "provider_quotas": dict(self.quota_tracker.quotas),
                "updated_at": utc_now_iso(),
            }
        )
        atomic_write_json(self.state_path, self.state.model_dump(mode="json"))

    def update_state(self, **fields: Any) -> PipelineState:
        self.state = self.state.model_copy(update=fields)
        self.save_state()
        return self.state

    def emit(
        self,
        event_type: EventType,
        *,
        task_id: str | None = None,
        role: AgentRole | None = None,
        provider: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> PipelineEvent:
        event = PipelineEvent(
            event_id=f"evt_{uuid.uuid4().hex[:12]}",
            run_id=self.run_id,
            task_id=task_id,
            role=role,
            provider=provider,
            event_type=event_type,
            payload=payload or {},
        )
        append_jsonl(self.events_path, event.model_dump(mode="json"))
        if self._on_event is not None:
            self._on_event(event)
        return event
