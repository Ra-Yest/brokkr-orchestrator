"""Structured event bus (events.jsonl), atomic state snapshot (state.json), and rolling quota tracker."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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


class EventBus:
    """Headless event stream (`events.jsonl`) and atomic state manager (`state.json`)."""

    def __init__(
        self,
        pipeline_dir: Path,
        config: PipelineConfig,
        *,
        run_id: str | None = None,
        defer_save: bool = False,
    ) -> None:
        self.pipeline_dir = pipeline_dir
        self.pipeline_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.pipeline_dir / "events.jsonl"
        self.state_path = self.pipeline_dir / "state.json"
        self.run_id = run_id or self._generate_run_id()

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
        return event
