"""Neutral, declarative external proxy/gateway health monitor (`BaseProxyMonitor` + `GenericProxyMonitor`)."""

from __future__ import annotations

import contextlib
import json
import os
import re
import socket
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestrator.config import (
    LogProbeConfig,
    ProviderProfileConfig,
    ProxyMonitorConfig,
    StatusJsonProbeConfig,
    expand_env_tokens,
)
from orchestrator.models import ProxyHealthStatus, utc_now_iso

_CREDENTIAL_URI_RE = re.compile(r"([A-Za-z0-9_.-]+):([^@\s/]+)@")
_BEARER_TOKEN_RE = re.compile(r"(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE)
_SECRET_KV_RE = re.compile(
    r"((?:api[_-]?key|token|secret|password|passwd|auth)\s*[:=]\s*)([^\s,;\"']+)",
    re.IGNORECASE,
)


_MOJIBAKE_CP1251_RE = re.compile(
    r"(?:\u0420[\u0400-\u04ff\u0080-\u00bf\u2010-\u203f\u2116\u00a0-\u00ff]|"
    r"\u0421[\u0400-\u04ff\u0080-\u00bf\u2010-\u203f\u2116\u00a0-\u00ff]){2,}"
)


def _decode_bytes_utf8_or_cp1251(data: bytes) -> str:
    """Decode bytes strictly as UTF-8 (handling optional BOM), falling back to cp1251 on UnicodeDecodeError."""
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("cp1251", errors="replace")


def normalize_proxy_text(text: str) -> str:
    """Repair classic UTF-8-decoded-as-cp1251 mojibake while leaving valid UTF-8 untouched."""
    if not text or not _MOJIBAKE_CP1251_RE.search(text):
        return text
    try:
        repaired = text.encode("cp1251").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired


def sanitize_proxy_text(text: str) -> str:
    """Normalize encoding and redact potential credentials/tokens before persisting messages."""
    normalized = normalize_proxy_text(text)
    redacted = _CREDENTIAL_URI_RE.sub(r"\1:***@", normalized)
    redacted = _BEARER_TOKEN_RE.sub(r"\1***", redacted)
    redacted = _SECRET_KV_RE.sub(r"\1***", redacted)
    return redacted


def expand_env_path(raw_path: str, host_env: Mapping[str, str] | None = None) -> Path:
    """Expand `${ENV_VAR}` tokens and `~` in a configured filesystem path."""
    expanded = expand_env_tokens(raw_path, host_env=host_env, strict=True)
    return Path(os.path.expanduser(expanded))


def _extract_dot_field(obj: Any, dot_path: str) -> Any:
    """Resolve a dot-separated field path (e.g. `last_ok.route`) on nested dicts/lists."""
    current = obj
    for part in dot_path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
        elif isinstance(current, list):
            try:
                idx = int(part)
                current = current[idx]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def _parse_timestamp_to_iso_and_epoch(value: Any) -> tuple[str | None, float | None]:
    if isinstance(value, (int, float)) and value > 0:
        try:
            iso = datetime.fromtimestamp(float(value), tz=UTC).isoformat().replace("+00:00", "Z")
            return iso, float(value)
        except (OSError, OverflowError, ValueError):
            return None, None
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC).isoformat().replace("+00:00", "Z"), dt.timestamp()
        except ValueError:
            return text, None
    return None, None


class BaseProxyMonitor(ABC):
    """Abstract interface for monitoring external proxy/network gateway health."""

    def __init__(self, config: ProxyMonitorConfig) -> None:
        self.config = config

    def applies_to(self, provider_name: str, profile: ProviderProfileConfig) -> bool:
        """Return True if this monitor governs calls to the given provider profile."""
        if not self.config.enabled:
            return False
        return provider_name in self.config.apply_to_providers

    @abstractmethod
    def check_tcp_proxy(self) -> tuple[bool | None, str]:
        """Verify optional local proxy TCP port reachability."""

    @abstractmethod
    def read_status(self) -> ProxyHealthStatus:
        """Read normalized health snapshot from all configured probes."""

    def preflight(self) -> tuple[bool, str, ProxyHealthStatus]:
        """Run preflight healthcheck (TCP, JSON status, and log probes) for `doctor` and pre-invoke."""
        status = self.read_status()
        if status.requires_manual_action:
            detail = (
                status.fatal_reason
                or status.last_message
                or f"Proxy monitor requires manual action ({status.state})"
            )
            return False, detail, status
        if status.proxy_reachable is False:
            return (
                False,
                f"Proxy endpoint {self.config.proxy_host}:{self.config.proxy_port} unreachable",
                status,
            )
        parts: list[str] = [f"state={status.state}"]
        if self.config.proxy_host and self.config.proxy_port:
            parts.append(f"tcp={self.config.proxy_host}:{self.config.proxy_port}")
        if status.active_route:
            parts.append(f"route='{status.active_route}'")
        return True, f"Proxy monitor healthy ({', '.join(parts)})", status


class GenericProxyMonitor(BaseProxyMonitor):
    """Fully declarative proxy monitor driven purely by `ProxyMonitorConfig` probes."""

    def __init__(
        self,
        config: ProxyMonitorConfig,
        *,
        host_env: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(config)
        self._host_env = host_env

    def check_tcp_proxy(self) -> tuple[bool | None, str]:
        host = self.config.proxy_host
        port = self.config.proxy_port
        if not host or port is None:
            return None, "TCP probe not configured"
        try:
            with socket.create_connection((host, port), timeout=self.config.tcp_timeout_seconds):
                return True, f"TCP {host}:{port} reachable"
        except OSError as exc:
            return False, f"TCP {host}:{port} unreachable ({exc})"

    def _read_status_json(self, probe: StatusJsonProbeConfig) -> dict[str, Any] | None:
        try:
            target_path = expand_env_path(probe.path, host_env=self._host_env)
        except Exception:
            return None
        if not target_path.is_file():
            return None
        try:
            decoded = _decode_bytes_utf8_or_cp1251(target_path.read_bytes())
            raw = json.loads(decoded)
            return raw if isinstance(raw, dict) else None
        except Exception:
            return None

    def _read_log_tail(self, probe: LogProbeConfig) -> list[str]:
        try:
            log_path = expand_env_path(probe.path, host_env=self._host_env)
        except Exception:
            return []
        if not log_path.is_file():
            return []
        try:
            with log_path.open("rb") as fh:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                read_bytes = min(size, max(32768, probe.tail_lines * 512))
                offset = max(0, size - read_bytes)
                fh.seek(offset)
                raw_chunk = fh.read()
            if offset > 0:
                # Skip a potentially partial leading line so UTF-8 multibyte boundaries stay intact
                nl_idx = raw_chunk.find(b"\n")
                if nl_idx != -1:
                    raw_chunk = raw_chunk[nl_idx + 1 :]
            chunk = normalize_proxy_text(_decode_bytes_utf8_or_cp1251(raw_chunk))
            lines = [ln.strip() for ln in chunk.splitlines() if ln.strip()]
            return lines[-probe.tail_lines :]
        except OSError:
            return []

    def read_status(self) -> ProxyHealthStatus:
        proxy_ok, proxy_detail = self.check_tcp_proxy()

        active_route: str | None = None
        last_ok_at: str | None = None
        last_ok_epoch: float | None = None
        blockers: list[str] = []
        route_switching = False
        requires_manual_action = False
        fatal_reason: str | None = None
        last_message: str | None = None
        excerpt_lines: list[str] = []

        # 1. Declarative JSON status probe
        json_probe = self.config.status_json
        json_data: dict[str, Any] | None = None
        if json_probe is not None:
            json_data = self._read_status_json(json_probe)
            if json_data is not None:
                if json_probe.route_field:
                    raw_route = _extract_dot_field(json_data, json_probe.route_field)
                    if isinstance(raw_route, str) and raw_route.strip():
                        active_route = sanitize_proxy_text(raw_route.strip())

                if json_probe.last_ok_at_field:
                    raw_ok = _extract_dot_field(json_data, json_probe.last_ok_at_field)
                    last_ok_at, last_ok_epoch = _parse_timestamp_to_iso_and_epoch(raw_ok)

                if json_probe.disabled_field:
                    raw_disabled = _extract_dot_field(json_data, json_probe.disabled_field)
                    if raw_disabled is True:
                        requires_manual_action = True
                        fatal_reason = (
                            f"Proxy status indicates disabled ({json_probe.disabled_field}=true)"
                        )

                if json_probe.blockers_field:
                    raw_blockers = _extract_dot_field(json_data, json_probe.blockers_field)
                    if isinstance(raw_blockers, list):
                        for item in raw_blockers:
                            if isinstance(item, str) and item.strip():
                                blockers.append(sanitize_proxy_text(item.strip()))
                            elif isinstance(item, dict):
                                msg = str(item.get("message") or item.get("reason") or item)
                                if msg.strip():
                                    blockers.append(sanitize_proxy_text(msg.strip()))
                    elif isinstance(raw_blockers, str) and raw_blockers.strip():
                        blockers.append(sanitize_proxy_text(raw_blockers.strip()))
                    if blockers:
                        requires_manual_action = True
                        fatal_reason = f"Proxy active blocker(s): {'; '.join(blockers[:3])}"

                if json_probe.error_field and not requires_manual_action:
                    raw_err = _extract_dot_field(json_data, json_probe.error_field)
                    if isinstance(raw_err, str) and raw_err.strip():
                        requires_manual_action = True
                        fatal_reason = sanitize_proxy_text(raw_err.strip())

                if json_probe.transient_error_field:
                    raw_tr = _extract_dot_field(json_data, json_probe.transient_error_field)
                    if isinstance(raw_tr, dict) and raw_tr:
                        _, err_epoch = _parse_timestamp_to_iso_and_epoch(raw_tr.get("at"))
                        is_recent = (
                            err_epoch is None
                            or last_ok_epoch is None
                            or err_epoch >= last_ok_epoch
                        )
                        if is_recent:
                            route_switching = True
                            msg_val = (
                                raw_tr.get("acted")
                                or raw_tr.get("message")
                                or raw_tr.get("reason")
                                or json.dumps(raw_tr, ensure_ascii=False)
                            )
                            if msg_val:
                                last_message = sanitize_proxy_text(str(msg_val).strip())
                    elif isinstance(raw_tr, str) and raw_tr.strip():
                        route_switching = True
                        last_message = sanitize_proxy_text(raw_tr.strip())
                    elif raw_tr is True:
                        route_switching = True

        # 2. Declarative log file probes
        any_log_lines = False
        for probe in self.config.log_probes:
            lines = self._read_log_tail(probe)
            if not lines:
                continue
            any_log_lines = True
            last_message = sanitize_proxy_text(lines[-1])
            excerpt_lines.extend(sanitize_proxy_text(ln) for ln in lines[-5:])

            ok_rxs = [re.compile(pat, re.IGNORECASE) for pat in probe.ok_patterns]
            tr_rxs = [re.compile(pat, re.IGNORECASE) for pat in probe.transient_patterns]
            fatal_rxs = [re.compile(pat, re.IGNORECASE) for pat in probe.fatal_patterns]

            last_ok_idx = -1
            if ok_rxs:
                for idx, line in enumerate(lines):
                    if any(rx.search(line) for rx in ok_rxs):
                        last_ok_idx = idx

            recent_lines = (
                lines[last_ok_idx + 1 :]
                if last_ok_idx >= 0
                else lines[-min(10, len(lines)) :]
            )
            if last_ok_idx >= 0 and not recent_lines:
                # Newest line confirms healthy reply -> clear prior transient route-switching marker
                route_switching = False
            else:
                for line in reversed(recent_lines):
                    if fatal_rxs and any(rx.search(line) for rx in fatal_rxs):
                        requires_manual_action = True
                        if fatal_reason is None:
                            fatal_reason = sanitize_proxy_text(line)
                        break
                    if tr_rxs and any(rx.search(line) for rx in tr_rxs):
                        route_switching = True

        # 3. Synthesize normalized health status
        if proxy_ok is False:
            requires_manual_action = True
            if fatal_reason is None:
                fatal_reason = f"Proxy unavailable: {proxy_detail}"
            if last_message is None:
                last_message = fatal_reason

        if proxy_ok is False:
            state = "service_down"
            healthy = False
        elif requires_manual_action:
            state = "fatal_error"
            healthy = False
        elif route_switching:
            state = "route_switching"
            healthy = True
        elif json_data is not None or any_log_lines or proxy_ok is True:
            state = "ok"
            healthy = True
        else:
            state = "ok_no_data"
            healthy = True

        if not excerpt_lines and json_data is not None:
            with contextlib.suppress(Exception):
                excerpt_lines = [
                    sanitize_proxy_text(
                        json.dumps(
                            {
                                "route": active_route,
                                "last_ok_at": last_ok_at,
                                "blockers": blockers,
                            },
                            ensure_ascii=False,
                        )
                    )
                ]
        raw_excerpt = "\n".join(excerpt_lines[-8:])[:800]

        return ProxyHealthStatus(
            healthy=healthy,
            state=state,
            proxy_reachable=proxy_ok,
            active_route=active_route,
            last_message=last_message,
            last_ok_at=last_ok_at,
            route_switching=route_switching,
            requires_manual_action=requires_manual_action,
            fatal_reason=fatal_reason,
            blockers=blockers,
            raw_excerpt=raw_excerpt,
            checked_at=utc_now_iso(),
        )


def build_proxy_monitor(config: ProxyMonitorConfig) -> BaseProxyMonitor | None:
    """Instantiate the declarative proxy monitor, or None when disabled."""
    if not config.enabled:
        return None
    return GenericProxyMonitor(config)
