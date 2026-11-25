"""Pipeline telemetry aggregator over `.pipeline/events.jsonl`."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from orchestrator.models import EventType

WASTE_REASONS: tuple[str, ...] = (
    "timeout",
    "idle/stall",
    "capacity/rate_limit",
    "transport",
    "other",
)


class ProviderStats(BaseModel):
    """Per-provider call counts and execution duration breakdown."""

    provider: str
    calls: int = 0
    succeeded: int = 0
    failed: int = 0
    rate_limited: int = 0
    total_seconds: float = 0.0
    avg_seconds: float = 0.0


class PreReviewEffectiveness(BaseModel):
    """Effectiveness metrics comparing pre_reviewer approvals against final reviewer rejections."""

    pre_review_approvals: int = 0
    pre_review_rejections: int = 0
    reviewer_rejections_after_pre_approve: int = 0
    pre_review_escape_rate: float = 0.0


class TaskStats(BaseModel):
    """Per-task call volume, duration, and failure breakdown."""

    task_id: str
    calls: int = 0
    total_seconds: float = 0.0
    diff_guard_failed: int = 0
    gates_failed: int = 0
    timeouts: int = 0
    failures: int = 0


class SeamAuditStats(BaseModel):
    """Summary of pairwise Seam Audit verdicts across contract closure points."""

    total: int = 0
    passed: int = 0
    warnings: int = 0
    critical: int = 0


class PipelineStats(BaseModel):
    """Aggregated telemetry computed from `.pipeline/events.jsonl`."""

    events_total: int = 0
    malformed_lines: int = 0
    succeeded_seconds: float = 0.0
    failed_seconds: float = 0.0
    rate_limited_seconds: float = 0.0
    wasted_pct: float = 0.0
    wasted_seconds_by_reason: dict[str, float] = Field(
        default_factory=lambda: {reason: 0.0 for reason in WASTE_REASONS}
    )
    providers: dict[str, ProviderStats] = Field(default_factory=dict)
    pre_review: PreReviewEffectiveness = Field(default_factory=PreReviewEffectiveness)
    tasks: dict[str, TaskStats] = Field(default_factory=dict)
    seam_audits: SeamAuditStats = Field(default_factory=SeamAuditStats)


def _parse_iso(ts: object) -> datetime | None:
    if not isinstance(ts, str) or not ts.strip():
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _as_non_negative_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return max(0.0, float(value))
    return None


def _extract_duration_seconds(
    event: dict[str, Any],
    payload: dict[str, Any],
    last_invoked_by_key: dict[tuple[str | None, str | None, str | None], datetime],
    last_invoked_by_provider: dict[str, datetime],
) -> float:
    explicit = _as_non_negative_float(payload.get("duration_seconds"))
    if explicit is not None:
        return explicit

    event_ts = _parse_iso(event.get("timestamp"))
    if event_ts is None:
        return 0.0

    task_id = event.get("task_id") if isinstance(event.get("task_id"), str) else None
    role = event.get("role") if isinstance(event.get("role"), str) else None
    provider = event.get("provider") if isinstance(event.get("provider"), str) else None

    started_ts = last_invoked_by_key.get((task_id, role, provider))
    if started_ts is None and provider is not None:
        started_ts = last_invoked_by_provider.get(provider)
    if started_ts is None:
        return 0.0

    return max(0.0, (event_ts - started_ts).total_seconds())


def _classify_failure_reason(payload: dict[str, Any]) -> str:
    if payload.get("idle_timed_out") or payload.get("stall_detected"):
        return "idle/stall"
    if payload.get("timed_out"):
        return "timeout"
    reason_raw = str(payload.get("reason", "")).lower()
    stderr_raw = str(payload.get("stderr_tail", "")).lower()
    combined = f"{reason_raw} {stderr_raw}"
    if any(
        tok in combined
        for tok in ("503", "429", "rate_limit", "rate limit", "capacity", "overloaded", "quota")
    ):
        return "capacity/rate_limit"
    if any(
        tok in combined
        for tok in (
            "transport",
            "502",
            "bad gateway",
            "econnreset",
            "etimedout",
            "socket hang up",
            "connection reset",
        )
    ):
        return "transport"
    return "other"


def compute_pipeline_stats(events_path: Path) -> PipelineStats:
    """Read `.pipeline/events.jsonl` resiliently and compute aggregated PipelineStats."""
    if not events_path.is_file():
        return PipelineStats()

    events_total = 0
    malformed_lines = 0
    succeeded_seconds = 0.0
    failed_seconds = 0.0
    rate_limited_seconds = 0.0
    wasted_by_reason: dict[str, float] = {reason: 0.0 for reason in WASTE_REASONS}

    providers: dict[str, ProviderStats] = {}
    tasks: dict[str, TaskStats] = {}
    pre_review = PreReviewEffectiveness()
    seam_audits = SeamAuditStats()

    last_invoked_by_key: dict[tuple[str | None, str | None, str | None], datetime] = {}
    last_invoked_by_provider: dict[str, datetime] = {}
    task_pre_approved: dict[str, bool] = {}

    def _get_provider(name: str) -> ProviderStats:
        if name not in providers:
            providers[name] = ProviderStats(provider=name)
        return providers[name]

    def _get_task(tid: str) -> TaskStats:
        if tid not in tasks:
            tasks[tid] = TaskStats(task_id=tid)
        return tasks[tid]

    try:
        raw_text = events_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return PipelineStats()

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
        except json.JSONDecodeError:
            malformed_lines += 1
            continue
        if not isinstance(record, dict):
            malformed_lines += 1
            continue

        events_total += 1
        event_type = str(record.get("event_type", ""))
        payload = record.get("payload")
        if not isinstance(payload, dict):
            payload = {}

        task_id = record.get("task_id") if isinstance(record.get("task_id"), str) else None
        role = record.get("role") if isinstance(record.get("role"), str) else None
        provider_name = (
            record.get("provider") if isinstance(record.get("provider"), str) else None
        )

        if event_type == EventType.PROVIDER_INVOKED.value:
            ts = _parse_iso(record.get("timestamp"))
            if ts is not None:
                last_invoked_by_key[(task_id, role, provider_name)] = ts
                if provider_name is not None:
                    last_invoked_by_provider[provider_name] = ts
            continue

        if event_type == EventType.PROVIDER_SUCCEEDED.value:
            dur = _extract_duration_seconds(
                record, payload, last_invoked_by_key, last_invoked_by_provider
            )
            succeeded_seconds += dur
            if provider_name:
                pstat = _get_provider(provider_name)
                pstat.calls += 1
                pstat.succeeded += 1
                pstat.total_seconds += dur
            if task_id:
                tstat = _get_task(task_id)
                tstat.calls += 1
                tstat.total_seconds += dur
            continue

        if event_type == EventType.PROVIDER_FAILED.value:
            dur = _extract_duration_seconds(
                record, payload, last_invoked_by_key, last_invoked_by_provider
            )
            failed_seconds += dur
            reason = _classify_failure_reason(payload)
            wasted_by_reason[reason] = wasted_by_reason.get(reason, 0.0) + dur
            if provider_name:
                pstat = _get_provider(provider_name)
                pstat.calls += 1
                pstat.failed += 1
                pstat.total_seconds += dur
            if task_id:
                tstat = _get_task(task_id)
                tstat.calls += 1
                tstat.total_seconds += dur
                tstat.failures += 1
                if reason in ("timeout", "idle/stall"):
                    tstat.timeouts += 1
            continue

        if event_type == EventType.PROVIDER_RATE_LIMITED.value:
            dur = _extract_duration_seconds(
                record, payload, last_invoked_by_key, last_invoked_by_provider
            )
            rate_limited_seconds += dur
            wasted_by_reason["capacity/rate_limit"] += dur
            if provider_name:
                pstat = _get_provider(provider_name)
                pstat.calls += 1
                pstat.rate_limited += 1
                pstat.total_seconds += dur
            if task_id:
                tstat = _get_task(task_id)
                tstat.calls += 1
                tstat.total_seconds += dur
            continue

        if event_type in (
            EventType.PROVIDER_TRANSPORT_RETRY.value,
            EventType.PROVIDER_TRANSIENT_RETRY.value,
        ):
            call_dur = _extract_duration_seconds(
                record, payload, last_invoked_by_key, last_invoked_by_provider
            )
            delay_dur = _as_non_negative_float(payload.get("delay_seconds")) or 0.0
            total_retry_dur = call_dur + delay_dur
            failed_seconds += total_retry_dur
            wasted_by_reason["transport"] += total_retry_dur
            if provider_name:
                pstat = _get_provider(provider_name)
                pstat.calls += 1
                pstat.failed += 1
                pstat.total_seconds += total_retry_dur
            if task_id:
                tstat = _get_task(task_id)
                tstat.calls += 1
                tstat.total_seconds += total_retry_dur
            continue

        if event_type == EventType.DIFF_GUARD_FAILED.value:
            if task_id:
                tstat = _get_task(task_id)
                tstat.diff_guard_failed += 1
                tstat.failures += 1
            continue

        if event_type == EventType.GATE_FAILED.value:
            if task_id:
                tstat = _get_task(task_id)
                tstat.gates_failed += 1
                tstat.failures += 1
            continue

        if event_type == EventType.PRE_REVIEW_COMPLETED.value:
            verdict = str(payload.get("verdict", "")).strip().upper()
            if verdict == "APPROVE":
                pre_review.pre_review_approvals += 1
                if task_id:
                    task_pre_approved[task_id] = True
            elif verdict == "REJECT":
                pre_review.pre_review_rejections += 1
                if task_id:
                    task_pre_approved[task_id] = False
            continue

        if event_type == EventType.REVIEW_COMPLETED.value:
            verdict = str(payload.get("verdict", "")).strip().upper()
            if task_id and task_pre_approved.pop(task_id, False) and verdict == "REJECT":
                pre_review.reviewer_rejections_after_pre_approve += 1
            continue

        if event_type == EventType.SEAM_AUDIT_COMPLETED.value:
            seam_audits.total += 1
            verdict = str(payload.get("verdict", "")).strip().upper()
            if verdict == "PASS":
                seam_audits.passed += 1
            elif verdict == "WARNING":
                seam_audits.warnings += 1
            elif verdict == "CRITICAL":
                seam_audits.critical += 1
            continue

    total_time = succeeded_seconds + failed_seconds + rate_limited_seconds
    wasted_time = failed_seconds + rate_limited_seconds
    wasted_pct = round((wasted_time / total_time) * 100.0, 2) if total_time > 0 else 0.0

    for pstat in providers.values():
        pstat.total_seconds = round(pstat.total_seconds, 2)
        pstat.avg_seconds = round(pstat.total_seconds / pstat.calls, 2) if pstat.calls > 0 else 0.0

    for tstat in tasks.values():
        tstat.total_seconds = round(tstat.total_seconds, 2)

    if pre_review.pre_review_approvals > 0:
        pre_review.pre_review_escape_rate = round(
            pre_review.reviewer_rejections_after_pre_approve / pre_review.pre_review_approvals,
            4,
        )

    return PipelineStats(
        events_total=events_total,
        malformed_lines=malformed_lines,
        succeeded_seconds=round(succeeded_seconds, 2),
        failed_seconds=round(failed_seconds, 2),
        rate_limited_seconds=round(rate_limited_seconds, 2),
        wasted_pct=wasted_pct,
        wasted_seconds_by_reason={k: round(v, 2) for k, v in wasted_by_reason.items()},
        providers=dict(sorted(providers.items())),
        pre_review=pre_review,
        tasks=dict(sorted(tasks.items())),
        seam_audits=seam_audits,
    )


def format_stats_console(stats: PipelineStats) -> str:
    """Format PipelineStats into compact, readable console tables."""
    lines: list[str] = [
        "=== Pipeline Telemetry ===",
        f"Events: {stats.events_total} (malformed skipped: {stats.malformed_lines})",
        (
            f"Provider Time: succeeded={stats.succeeded_seconds:.2f}s | "
            f"failed={stats.failed_seconds:.2f}s | "
            f"rate_limited={stats.rate_limited_seconds:.2f}s | "
            f"wasted={stats.wasted_pct:.2f}%"
        ),
    ]

    waste_parts = [
        f"{reason}={secs:.2f}s"
        for reason, secs in stats.wasted_seconds_by_reason.items()
        if secs > 0
    ]
    if waste_parts:
        lines.append("Wasted by reason: " + ", ".join(waste_parts))

    pr = stats.pre_review
    escape_pct = pr.pre_review_escape_rate * 100.0
    lines.append(
        f"Pre-Review: approvals={pr.pre_review_approvals}, rejections={pr.pre_review_rejections}, "
        f"escapes={pr.reviewer_rejections_after_pre_approve}, escape_rate={escape_pct:.1f}%"
    )

    sa = stats.seam_audits
    lines.append(
        f"Seam Audits: total={sa.total}, passed={sa.passed}, "
        f"warnings={sa.warnings}, critical={sa.critical}"
    )

    if stats.providers:
        lines.extend(
            [
                "",
                "Providers:",
                f"  {'Provider':<26} {'Calls':>5} {'OK':>5} {'Fail':>5} {'RL':>4} {'Total(s)':>9} {'Avg(s)':>8}",
                f"  {'-' * 26} {'-' * 5} {'-' * 5} {'-' * 5} {'-' * 4} {'-' * 9} {'-' * 8}",
            ]
        )
        for p in stats.providers.values():
            lines.append(
                f"  {p.provider:<26} {p.calls:>5} {p.succeeded:>5} {p.failed:>5} "
                f"{p.rate_limited:>4} {p.total_seconds:>9.2f} {p.avg_seconds:>8.2f}"
            )

    if stats.tasks:
        lines.extend(
            [
                "",
                "Tasks:",
                f"  {'Task':<20} {'Calls':>5} {'Time(s)':>9} {'DiffFail':>8} {'GateFail':>8} {'Timeouts':>8}",
                f"  {'-' * 20} {'-' * 5} {'-' * 9} {'-' * 8} {'-' * 8} {'-' * 8}",
            ]
        )
        for t in stats.tasks.values():
            lines.append(
                f"  {t.task_id:<20} {t.calls:>5} {t.total_seconds:>9.2f} "
                f"{t.diff_guard_failed:>8} {t.gates_failed:>8} {t.timeouts:>8}"
            )

    return "\n".join(lines)


def render_telemetry_markdown(stats: PipelineStats) -> str:
    """Render a compact `## Pipeline Telemetry` Markdown section for REPORT.md."""
    pr = stats.pre_review
    sa = stats.seam_audits
    escape_pct = pr.pre_review_escape_rate * 100.0

    lines: list[str] = [
        "## Pipeline Telemetry",
        "",
        (
            f"- **Provider Time:** succeeded `{stats.succeeded_seconds:.2f}s`, "
            f"failed `{stats.failed_seconds:.2f}s`, "
            f"rate-limited `{stats.rate_limited_seconds:.2f}s` "
            f"(**wasted:** `{stats.wasted_pct:.2f}%`)"
        ),
    ]

    non_zero_waste = [
        f"`{reason}`: {secs:.2f}s"
        for reason, secs in stats.wasted_seconds_by_reason.items()
        if secs > 0
    ]
    if non_zero_waste:
        lines.append("- **Wasted Time Breakdown:** " + ", ".join(non_zero_waste))

    lines.append(
        f"- **Pre-Review Effectiveness:** `{pr.pre_review_approvals}` approvals, "
        f"`{pr.pre_review_rejections}` rejections, "
        f"`{pr.reviewer_rejections_after_pre_approve}` escapes "
        f"(**escape rate:** `{escape_pct:.1f}%`)"
    )
    lines.append(
        f"- **Seam Audits:** `{sa.total}` total "
        f"(`{sa.passed}` PASS, `{sa.warnings}` WARNING, `{sa.critical}` CRITICAL)"
    )

    if stats.providers:
        lines.extend(
            [
                "",
                "| Provider | Calls | Succeeded | Failed | Rate Limited | Total (s) | Avg (s) |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for p in stats.providers.values():
            lines.append(
                f"| `{p.provider}` | {p.calls} | {p.succeeded} | {p.failed} | "
                f"{p.rate_limited} | {p.total_seconds:.2f} | {p.avg_seconds:.2f} |"
            )

    return "\n".join(lines)
