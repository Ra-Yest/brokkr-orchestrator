"""OpenAI-compatible HTTP API agent provider for read-only roles (planner, pre_reviewer, reviewer)."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from email.utils import parsedate_to_datetime
from typing import Any

from orchestrator.config import ProviderProfileConfig, resolve_env_map
from orchestrator.providers.base import BaseAgentProvider, ProviderRequest, ProviderResponse


def _parse_retry_after_header(value: str | None, default_seconds: int) -> int:
    if not value:
        return default_seconds
    stripped = value.strip()
    if stripped.isdigit():
        return max(1, int(stripped))
    try:
        dt = parsedate_to_datetime(stripped)
        delta = int(dt.timestamp() - time.time())
        return max(1, delta)
    except Exception:
        return default_seconds


class ApiAgentProvider(BaseAgentProvider):
    """OpenAI-compatible Chat Completions provider (strictly read-only: can_edit_files=False)."""

    def __init__(self, name: str, profile: ProviderProfileConfig) -> None:
        super().__init__(name=name, profile=profile)

    def _build_endpoint(self) -> str:
        base = (self.profile.base_url or "").rstrip("/")
        if base.endswith("/chat/completions"):
            return base
        return f"{base}/chat/completions"

    def _build_messages(self, request: ProviderRequest) -> list[dict[str, str]]:
        if request.static_prefix.strip() and request.dynamic_body.strip():
            return [
                {"role": "system", "content": request.static_prefix},
                {"role": "user", "content": request.dynamic_body},
            ]
        return [{"role": "user", "content": request.prompt}]

    def invoke(self, request: ProviderRequest) -> ProviderResponse:
        start = time.monotonic()
        endpoint = self._build_endpoint()
        headers: dict[str, str] = {"Content-Type": "application/json"}

        resolved_headers, _ = resolve_env_map(self.profile.headers, strict=True)
        headers.update(resolved_headers)

        if self.profile.api_key_env:
            api_key = os.environ.get(self.profile.api_key_env, "")
            if not api_key:
                return ProviderResponse(
                    provider_name=self.name,
                    exit_code=1,
                    raw_stderr=(
                        f"Missing API key environment variable '{self.profile.api_key_env}'"
                    ),
                    duration_seconds=time.monotonic() - start,
                )
            headers.setdefault("Authorization", f"Bearer {api_key}")

        body: dict[str, Any] = {
            "model": self.profile.model,
            "messages": self._build_messages(request),
        }
        if self.profile.max_tokens is not None:
            body["max_tokens"] = self.profile.max_tokens
        if self.profile.temperature is not None:
            body["temperature"] = self.profile.temperature

        payload_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
        http_req = urllib.request.Request(
            endpoint,
            data=payload_bytes,
            headers=headers,
            method="POST",
        )
        timeout_sec = min(request.timeout_seconds, self.profile.timeout_seconds)

        try:
            with urllib.request.urlopen(http_req, timeout=timeout_sec) as resp:
                raw_body = resp.read().decode("utf-8", errors="replace")
                status_code = int(getattr(resp, "status", 200))
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            duration = time.monotonic() - start
            if exc.code == 429:
                retry_hdr = exc.headers.get("Retry-After") if exc.headers else None
                retry_sec = _parse_retry_after_header(
                    retry_hdr,
                    self.profile.rate_limit_cooldown_seconds,
                )
                return ProviderResponse(
                    provider_name=self.name,
                    exit_code=429,
                    raw_stderr=err_body,
                    duration_seconds=duration,
                    rate_limited=True,
                    retry_after_seconds=retry_sec,
                    metadata={"kind": "api", "http_status": 429},
                )
            return ProviderResponse(
                provider_name=self.name,
                exit_code=exc.code or 1,
                raw_stderr=err_body,
                duration_seconds=duration,
                metadata={"kind": "api", "http_status": exc.code},
            )
        except TimeoutError as exc:
            return ProviderResponse(
                provider_name=self.name,
                exit_code=-1,
                raw_stderr=f"API request timed out: {exc}",
                duration_seconds=time.monotonic() - start,
                timed_out=True,
                metadata={"kind": "api"},
            )
        except urllib.error.URLError as exc:
            return ProviderResponse(
                provider_name=self.name,
                exit_code=1,
                raw_stderr=f"API network error: {exc}",
                duration_seconds=time.monotonic() - start,
                metadata={"kind": "api"},
            )

        duration = time.monotonic() - start
        parsed_text = raw_body
        usage: dict[str, Any] = {}
        try:
            parsed_json = json.loads(raw_body)
            choices = parsed_json.get("choices", [])
            if choices and isinstance(choices[0], dict):
                message = choices[0].get("message", {})
                content = message.get("content", "")
                if isinstance(content, str):
                    parsed_text = content
            if isinstance(parsed_json.get("usage"), dict):
                usage = parsed_json["usage"]
        except json.JSONDecodeError:
            pass

        return ProviderResponse(
            provider_name=self.name,
            exit_code=0 if 200 <= status_code < 300 else status_code,
            raw_stdout=raw_body,
            parsed_text=parsed_text,
            duration_seconds=duration,
            metadata={"kind": "api", "http_status": status_code, "usage": usage},
        )

    def healthcheck(self) -> tuple[bool, str]:
        if not self.profile.base_url:
            return False, "Missing base_url"
        if not self.profile.model:
            return False, "Missing model"
        if self.profile.api_key_env and not os.environ.get(self.profile.api_key_env):
            return (
                False,
                f"API key env var '{self.profile.api_key_env}' is not set "
                f"(endpoint: {self.profile.base_url})",
            )
        return True, f"API endpoint configured: {self._build_endpoint()} (model={self.profile.model})"
