"""Guarded provider execution, error classification, quota/fallback routing, proxy & account checks."""

from __future__ import annotations

import contextlib
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from orchestrator.config import PipelineConfig
from orchestrator.diff_guard import (
    DiffGuardReport,
    GuardViolation,
    _split_diff_sections,
    evaluate_diff,
    matches_any_glob,
)
from orchestrator.events import EventBus
from orchestrator.fs_utils import (
    atomic_write_json,
    atomic_write_text,
    diff_directory_snapshots,
    sha256_file,
    snapshot_directory_hashes,
)
from orchestrator.git_ops import GitChange, GitOps
from orchestrator.models import (
    READ_ONLY_ROLES,
    WORKER_ROLES,
    WRITING_ROLES,
    AgentRole,
    EventType,
    ProxyHealthStatus,
    Task,
    TaskComplexity,
)
from orchestrator.notifications import BaseNotifier
from orchestrator.output_parser import parse_worker_output
from orchestrator.process_utils import is_meaningful_output, run_noninteractive_process
from orchestrator.prompt_builder import BuiltPrompt
from orchestrator.providers.base import (
    BaseAgentProvider,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
)
from orchestrator.proxy_monitor import BaseProxyMonitor
from orchestrator.runtime.errors import (
    OrchestratorFatalError,
    _AbortedByControlError,
    _PausedByInfraError,
    _TaskDeferredOnCooldownError,
)
from orchestrator.runtime.lifecycle import RunnerLifecycleManager

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


def _matches_any_pattern(text: str, patterns: list[str]) -> bool:
    """Return True if any case-insensitive regex in patterns matches text."""
    return any(re.search(pat, text, re.IGNORECASE) for pat in patterns)


def is_transport_failure_before_response(
    response: ProviderResponse,
    *,
    max_duration_seconds: int,
    git_tree_changed: bool,
    proxy_route_switching: bool = False,
    transient_error_patterns: list[str] | None = None,
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
    if transient_error_patterns and _matches_any_pattern(combined, transient_error_patterns):
        return True
    if _TRANSIENT_ERROR_RE.search(combined):
        return True
    return (not response.received_meaningful_output) and (
        not is_meaningful_output(response.raw_stdout)
    )


@dataclass
class _ProviderCallOutcome:
    """Result of a single guarded provider invocation attempt before retry/guard routing."""

    response: ProviderResponse
    pre_proxy: ProxyHealthStatus | None
    tamper_changes: list[str]
    git_tree_changed: bool


class ProviderExecutor:
    """Selects, guards, retries, and records artifacts for every AI provider invocation."""

    def __init__(
        self,
        *,
        repo_root: Path,
        pipeline_dir: Path,
        config: PipelineConfig,
        registry: ProviderRegistry,
        event_bus: EventBus,
        git_ops: GitOps,
        notifier: BaseNotifier,
        proxy_monitor: BaseProxyMonitor | None,
        lifecycle: RunnerLifecycleManager,
        jitter_fn: Callable[[float], float],
        list_available_todo_tasks: Callable[[], list[Task]],
        worker_role_for: Callable[[TaskComplexity], AgentRole],
    ) -> None:
        self.repo_root = repo_root
        self.pipeline_dir = pipeline_dir
        self.config = config
        self.registry = registry
        self.event_bus = event_bus
        self.git_ops = git_ops
        self.notifier = notifier
        self.proxy_monitor = proxy_monitor
        self._lifecycle = lifecycle
        self._jitter = jitter_fn
        self._list_available_todo_tasks = list_available_todo_tasks
        self._worker_role_for = worker_role_for

        self._last_proxy_key: tuple[str, bool, bool, str | None] | None = None

    # -------------------------------------------------------------------------
    # Provider Fallback, Quota & Cooldown Selection
    # -------------------------------------------------------------------------

    def candidate_providers_for_role(self, role: AgentRole) -> list[str]:
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

    def select_available_provider_for_role(
        self,
        role: AgentRole,
        *,
        exclude: set[str] | None = None,
    ) -> tuple[str, int, list[str]]:
        """Return (chosen_provider_name, wait_seconds, all_candidates)."""
        candidates = self.candidate_providers_for_role(role)
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

    def should_defer_task_on_cooldown(self, current_task_id: str, current_min_wait: int) -> bool:
        """Return True if another unlocked TODO task can run now or wakes sooner than current_min_wait."""
        for candidate in self._list_available_todo_tasks():
            if candidate.id == current_task_id:
                continue
            cand_role = self._worker_role_for(candidate.complexity)
            _, cand_wait, _ = self.select_available_provider_for_role(cand_role)
            if cand_wait == 0 or cand_wait < current_min_wait:
                return True
        return False

    # -------------------------------------------------------------------------
    # Proxy Monitor & Account Switching
    # -------------------------------------------------------------------------

    def poll_proxy_monitor_for_provider(
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

    def ensure_provider_account(
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

    # -------------------------------------------------------------------------
    # Streaming, Git Tree Snapshots & Artifact Persistence
    # -------------------------------------------------------------------------

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
    # Guarded Provider Invocation (Decomposed Helpers + Main Entry Point)
    # -------------------------------------------------------------------------

    def _select_initial_provider(
        self,
        *,
        role: AgentRole,
        task_id: str | None,
        attempt_dir: Path,
    ) -> BaseAgentProvider:
        """Select initial available provider for role, hibernating if all candidates are in cooldown."""
        primary_name = self.registry.profile_name_for_role(role)
        chosen_name, pre_wait, candidates = self.select_available_provider_for_role(role)

        if pre_wait > 0:
            resume_dt = datetime.now(UTC) + timedelta(seconds=pre_wait)
            self._lifecycle.hibernate_until(
                resume_dt=resume_dt,
                providers=candidates,
                task_id=task_id,
                role=role,
            )
            if self._lifecycle.abort_requested:
                raise _AbortedByControlError()
            chosen_name, _, candidates = self.select_available_provider_for_role(role)
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
        return provider

    def _execute_single_provider_call(
        self,
        *,
        provider: BaseAgentProvider,
        role: AgentRole,
        built_prompt: BuiltPrompt,
        task_id: str | None,
        attempt_dir: Path,
        base_sha: str,
        transport_retries: int,
    ) -> _ProviderCallOutcome:
        """Run preflight proxy/account checks, invoke provider, and capture integrity/git snapshots."""
        # Proxy monitor preflight before invoking governed providers
        pre_proxy = self.poll_proxy_monitor_for_provider(
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
        self.ensure_provider_account(provider, task_id=task_id, role=role)

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
        stall_patterns = (
            provider.profile.stall_stdout_patterns
            if provider.profile.stall_stdout_patterns is not None
            else self.config.workflow.stall_stdout_patterns
        )
        stall_grace = (
            provider.profile.stall_grace_seconds
            if provider.profile.stall_grace_seconds is not None
            else self.config.workflow.stall_grace_seconds
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
            stall_stdout_patterns=stall_patterns,
            stall_grace_seconds=float(stall_grace),
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

        return _ProviderCallOutcome(
            response=response,
            pre_proxy=pre_proxy,
            tamper_changes=tamper_changes,
            git_tree_changed=git_tree_changed,
        )

    def _handle_rate_limited_response(
        self,
        *,
        provider: BaseAgentProvider,
        response: ProviderResponse,
        role: AgentRole,
        task_id: str | None,
        base_sha: str,
        git_tree_changed: bool,
    ) -> BaseAgentProvider:
        """Record 429 cooldown, switch to fallback, defer clean worker task, or hibernate."""
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
            payload={
                "cooldown_seconds": cooldown,
                "duration_seconds": round(response.duration_seconds, 2),
                "stderr": response.raw_stderr[:400],
            },
        )
        self._lifecycle.check_control_after_step()

        # Try fallback provider first before hibernating
        next_name, next_wait, all_candidates = self.select_available_provider_for_role(
            role,
            exclude={provider.name},
        )
        if next_wait == 0 and next_name != provider.name:
            prev_name = provider.name
            next_provider = self.registry.get(next_name)
            self.event_bus.emit(
                EventType.PROVIDER_FALLBACK_USED,
                task_id=task_id,
                role=role,
                provider=next_provider.name,
                payload={
                    "from_provider": prev_name,
                    "to_provider": next_provider.name,
                    "reason": "rate_limit",
                },
            )
            return next_provider

        # All candidates are in cooldown/quota-capped -> defer to another runnable task if possible
        # (only defer mid-task for meaningful cooldowns >= 10s; tiny 1-9s waits complete in place)
        _, min_wait, all_candidates = self.select_available_provider_for_role(role)
        if (
            role in WORKER_ROLES
            and task_id is not None
            and min_wait >= 10
            and not git_tree_changed
            and not self.git_ops.has_uncommitted_changes()
            and self.git_ops.get_head_sha() == base_sha
            and self.should_defer_task_on_cooldown(task_id, min_wait)
        ):
            raise _TaskDeferredOnCooldownError(
                task_id=task_id,
                role=role,
                providers=all_candidates,
                wait_seconds=min_wait,
            )

        resume_dt = datetime.now(UTC) + timedelta(seconds=max(1, min_wait))
        self._lifecycle.hibernate_until(
            resume_dt=resume_dt,
            providers=all_candidates,
            task_id=task_id,
            role=role,
        )
        if self._lifecycle.abort_requested:
            raise _AbortedByControlError()
        avail_name, _, _ = self.select_available_provider_for_role(role)
        return self.registry.get(avail_name)

    def _try_handle_capacity_error(
        self,
        *,
        provider: BaseAgentProvider,
        outcome: _ProviderCallOutcome,
        role: AgentRole,
        task_id: str | None,
        exhausted_providers: set[str],
    ) -> BaseAgentProvider | None:
        """Handle 503/UNAVAILABLE/capacity errors with short cooldown and immediate fallback."""
        response = outcome.response
        combined_output = f"{response.raw_stderr}\n{response.raw_stdout}"
        is_capacity_error = (
            response.exit_code != 0
            and not response.timed_out
            and not response.idle_timed_out
            and not outcome.tamper_changes
            and not _PERMANENT_ERROR_RE.search(combined_output)
            and _matches_any_pattern(
                combined_output,
                self.config.workflow.capacity_error_patterns,
            )
        )
        if not is_capacity_error:
            return None

        cooldown = self.config.workflow.capacity_cooldown_seconds
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
            payload={
                "cooldown_seconds": cooldown,
                "duration_seconds": round(response.duration_seconds, 2),
                "reason": "capacity_error",
                "stderr": response.raw_stderr[:400],
            },
        )
        self._lifecycle.check_control_after_step()

        next_name, next_wait, _ = self.select_available_provider_for_role(
            role,
            exclude={provider.name, *exhausted_providers},
        )
        if (
            next_wait == 0
            and next_name != provider.name
            and next_name not in exhausted_providers
        ):
            prev_name = provider.name
            next_provider = self.registry.get(next_name)
            self.event_bus.emit(
                EventType.PROVIDER_FALLBACK_USED,
                task_id=task_id,
                role=role,
                provider=next_provider.name,
                payload={
                    "from_provider": prev_name,
                    "to_provider": next_provider.name,
                    "reason": "capacity_error",
                },
            )
            return next_provider
        return None

    def _try_handle_transport_failure(
        self,
        *,
        provider: BaseAgentProvider,
        outcome: _ProviderCallOutcome,
        role: AgentRole,
        task_id: str | None,
        transport_retries: int,
        max_transport_retries: int,
        transport_delays: list[int],
        exhausted_providers: set[str],
    ) -> tuple[bool, BaseAgentProvider, int]:
        """Poll post-call proxy status and handle free pre-response transport retries/fallbacks."""
        response = outcome.response
        post_proxy: ProxyHealthStatus | None = None
        if response.exit_code != 0 or response.timed_out or response.idle_timed_out:
            post_proxy = self.poll_proxy_monitor_for_provider(
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
        pre_proxy = outcome.pre_proxy
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
        transport_fail = (not outcome.tamper_changes) and is_transport_failure_before_response(
            response,
            max_duration_seconds=self.config.workflow.transport_failure_max_duration_seconds,
            git_tree_changed=outcome.git_tree_changed,
            proxy_route_switching=proxy_switching,
            transient_error_patterns=self.config.workflow.transient_error_patterns,
        )

        if not (transport_fail and max_transport_retries > 0):
            return False, provider, transport_retries

        # Honor abort / pause_after_current BEFORE launching a transport retry
        self._lifecycle.check_control_after_step()
        if transport_retries < max_transport_retries:
            base_delay = (
                float(transport_delays[min(transport_retries, len(transport_delays) - 1)])
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
            self._lifecycle.wait_backoff(
                delay_seconds=delay,
                provider_name=provider.name,
                task_id=task_id,
                role=role,
                phase_prefix="transport_backoff",
            )
            return True, provider, transport_retries

        # Exhausted transport retries on this provider -> switch to available fallback if any
        exhausted_providers.add(provider.name)
        next_name, next_wait, _ = self.select_available_provider_for_role(
            role,
            exclude=exhausted_providers,
        )
        if next_wait == 0 and next_name not in exhausted_providers:
            prev_name = provider.name
            next_provider = self.registry.get(next_name)
            self.event_bus.emit(
                EventType.PROVIDER_FALLBACK_USED,
                task_id=task_id,
                role=role,
                provider=next_provider.name,
                payload={
                    "from_provider": prev_name,
                    "to_provider": next_provider.name,
                    "reason": "transport_retry_exhausted",
                },
            )
            return True, next_provider, 0

        # Exhausted free transport retries across all available providers -> pause
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

    def _should_retry_after_quota(
        self,
        *,
        provider: BaseAgentProvider,
        outcome: _ProviderCallOutcome,
        role: AgentRole,
        task_id: str | None,
        expected_head: str,
        retry_delays: list[int],
        readonly_timeout_retries: int,
        transient_attempt: int,
    ) -> tuple[bool, int, int]:
        """Record quota usage and decide whether to retry a read-only timeout or transient failure."""
        response = outcome.response
        # Record quota usage ONLY for non-transport calls (actual responses / work done)
        self.event_bus.quota_tracker.record_request(provider.name)
        self.event_bus.save_state()

        failure_kind = classify_provider_failure(response)

        if failure_kind == "timeout":
            if (
                role in READ_ONLY_ROLES
                and not outcome.tamper_changes
                and readonly_timeout_retries < self.config.workflow.retry_on_timeout_readonly
            ):
                self._lifecycle.check_control_after_step()
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
                self._lifecycle.wait_backoff(
                    delay_seconds=delay,
                    provider_name=provider.name,
                    task_id=task_id,
                    role=role,
                )
                return True, readonly_timeout_retries, transient_attempt
            # Writing roles on timeout / idle timeout NEVER blindly loop; return upward
            return False, readonly_timeout_retries, transient_attempt

        if failure_kind == "permanent":
            # Fail-fast without retries for 401/403/invalid CLI option/command not found
            return False, readonly_timeout_retries, transient_attempt

        if (
            failure_kind == "transient"
            and not outcome.tamper_changes
            and transient_attempt < len(retry_delays)
        ):
            # Guard against 19-minute blind retry hangs on writing roles:
            # if a writing role ran longer than transport_failure_max_duration_seconds OR already
            # modified repository files, do NOT blindly retry here — hand control back upward.
            if role in WRITING_ROLES and (
                response.duration_seconds
                >= self.config.workflow.transport_failure_max_duration_seconds
                or outcome.git_tree_changed
            ):
                return False, readonly_timeout_retries, transient_attempt

            self._lifecycle.check_control_after_step()
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
            self._lifecycle.wait_backoff(
                delay_seconds=delay,
                provider_name=provider.name,
                task_id=task_id,
                role=role,
            )
            return True, readonly_timeout_retries, transient_attempt

        return False, readonly_timeout_retries, transient_attempt

    def _finalize_provider_call(
        self,
        *,
        provider: BaseAgentProvider,
        outcome: _ProviderCallOutcome,
        role: AgentRole,
        task_id: str | None,
        attempt_dir: Path,
        base_sha: str,
        expected_head: str,
        pre_changes: list[GitChange],
        pre_diff: str,
        task_complexity: TaskComplexity | None,
        allowed_paths: list[str] | None,
        preserve_existing_diff_for_readonly: bool,
    ) -> tuple[ProviderResponse, DiffGuardReport]:
        """Run post-call soft-reset, git add -N, Diff Guard, Integrity Guard, artifacts, and final event."""
        response = outcome.response

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

            delta_path_set = {
                p for p in (*delta_diff_paths, *removed_diff_paths) if not matches_any_glob(p, ro_ignores)
            }
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

        if outcome.tamper_changes:
            for item in outcome.tamper_changes:
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
            fail_payload: dict[str, Any] = {
                "exit_code": response.exit_code,
                "timed_out": response.timed_out,
                "idle_timed_out": response.idle_timed_out,
                "duration_seconds": round(response.duration_seconds, 2),
                "stderr_tail": response.raw_stderr[-400:],
            }
            if response.metadata.get("stall_detected"):
                fail_payload["stall_detected"] = True
                fail_payload["stall_reason"] = response.metadata.get("stall_reason")
            self.event_bus.emit(
                EventType.PROVIDER_FAILED,
                task_id=task_id,
                role=role,
                provider=provider.name,
                payload=fail_payload,
            )

        return response, guard_report

    def invoke_provider_guarded(
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
        """Invoke a provider with proxy/account preflight, retry/fallback routing, Integrity Guard & Diff Guard."""
        provider = self._select_initial_provider(
            role=role,
            task_id=task_id,
            attempt_dir=attempt_dir,
        )

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
        exhausted_providers: set[str] = set()

        while True:
            outcome = self._execute_single_provider_call(
                provider=provider,
                role=role,
                built_prompt=built_prompt,
                task_id=task_id,
                attempt_dir=attempt_dir,
                base_sha=base_sha,
                transport_retries=transport_retries,
            )

            if outcome.response.rate_limited:
                provider = self._handle_rate_limited_response(
                    provider=provider,
                    response=outcome.response,
                    role=role,
                    task_id=task_id,
                    base_sha=base_sha,
                    git_tree_changed=outcome.git_tree_changed,
                )
                continue

            capacity_fallback = self._try_handle_capacity_error(
                provider=provider,
                outcome=outcome,
                role=role,
                task_id=task_id,
                exhausted_providers=exhausted_providers,
            )
            if capacity_fallback is not None:
                provider = capacity_fallback
                transport_retries = 0
                continue

            handled_transport, provider, transport_retries = self._try_handle_transport_failure(
                provider=provider,
                outcome=outcome,
                role=role,
                task_id=task_id,
                transport_retries=transport_retries,
                max_transport_retries=max_transport_retries,
                transport_delays=transport_delays,
                exhausted_providers=exhausted_providers,
            )
            if handled_transport:
                continue

            should_retry, readonly_timeout_retries, transient_attempt = (
                self._should_retry_after_quota(
                    provider=provider,
                    outcome=outcome,
                    role=role,
                    task_id=task_id,
                    expected_head=expected_head,
                    retry_delays=retry_delays,
                    readonly_timeout_retries=readonly_timeout_retries,
                    transient_attempt=transient_attempt,
                )
            )
            if should_retry:
                continue

            return self._finalize_provider_call(
                provider=provider,
                outcome=outcome,
                role=role,
                task_id=task_id,
                attempt_dir=attempt_dir,
                base_sha=base_sha,
                expected_head=expected_head,
                pre_changes=pre_changes,
                pre_diff=pre_diff,
                task_complexity=task_complexity,
                allowed_paths=allowed_paths,
                preserve_existing_diff_for_readonly=preserve_existing_diff_for_readonly,
            )
