"""Universal CLI agent adapter supporting Codex CLI, Antigravity, Claude Code, Gemini CLI, Aider, or custom scripts."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

from orchestrator.config import ProviderProfileConfig, resolve_env_map
from orchestrator.process_utils import resolve_executable, run_noninteractive_process
from orchestrator.providers.base import BaseAgentProvider, ProviderRequest, ProviderResponse

_RETRY_AFTER_RE = re.compile(
    r"(?:retry[\s_-]*after|try again in|wait)\s*[:=]?\s*(\d+)\s*(s|sec|seconds|m|min|minutes)?",
    re.IGNORECASE,
)


def _extract_json_path(obj: Any, dot_path: str) -> str:
    current = obj
    for part in dot_path.split("."):
        if isinstance(current, dict):
            if part not in current:
                raise KeyError(f"Key '{part}' not found in JSON object")
            current = current[part]
        elif isinstance(current, list):
            idx = int(part)
            current = current[idx]
        else:
            raise KeyError(f"Cannot traverse '{part}' on {type(current).__name__}")
    if isinstance(current, str):
        return current
    return json.dumps(current, ensure_ascii=False)


class CliAgentProvider(BaseAgentProvider):
    """Runs any external CLI agent with configurable prompt delivery, output parsing, and 429 detection."""

    def __init__(self, name: str, profile: ProviderProfileConfig) -> None:
        super().__init__(name=name, profile=profile)
        self._rate_limit_regexes = [
            re.compile(pattern, re.IGNORECASE) for pattern in profile.rate_limit_patterns
        ]

    def _detect_rate_limit(self, combined_output: str) -> tuple[bool, int | None]:
        matched = any(rx.search(combined_output) for rx in self._rate_limit_regexes)
        if not matched:
            return False, None
        retry_match = _RETRY_AFTER_RE.search(combined_output)
        if retry_match:
            amount = int(retry_match.group(1))
            unit = (retry_match.group(2) or "s").lower()
            seconds = amount * 60 if unit.startswith("m") else amount
            return True, max(1, seconds)
        return True, self.profile.rate_limit_cooldown_seconds

    def _parse_output(self, raw_text: str) -> str:
        parser = self.profile.output_parser
        if parser == "plain":
            return raw_text
        json_path = self.profile.output_json_path
        if not json_path:
            return raw_text

        if parser == "json_field":
            try:
                data = json.loads(raw_text)
                return _extract_json_path(data, json_path)
            except (json.JSONDecodeError, KeyError, IndexError, ValueError):
                return raw_text

        if parser == "jsonl_last":
            lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
            for line in reversed(lines):
                try:
                    data = json.loads(line)
                    return _extract_json_path(data, json_path)
                except (json.JSONDecodeError, KeyError, IndexError, ValueError):
                    continue
            return raw_text

        return raw_text

    def _render_args(
        self,
        *,
        request: ProviderRequest,
        prompt_file: Path | None,
        output_file: Path | None,
    ) -> list[str]:
        placeholders = {
            "prompt": request.prompt,
            "prompt_file": str(prompt_file) if prompt_file is not None else "",
            "output_file": str(output_file) if output_file is not None else "",
            "workdir": str(request.workdir),
            "role": request.role.value,
            "task_id": request.task_id or "",
            "model": self.profile.model or "",
        }

        rendered: list[str] = []
        used_prompt_placeholder = False
        used_prompt_file_placeholder = False

        for token in self.profile.args_template:
            if "{prompt}" in token:
                used_prompt_placeholder = True
            if "{prompt_file}" in token:
                used_prompt_file_placeholder = True
            rendered_token = token
            for key, value in placeholders.items():
                rendered_token = rendered_token.replace(f"{{{key}}}", value)
            rendered.append(rendered_token)

        if self.profile.prompt_mode == "arg" and not used_prompt_placeholder:
            rendered.append(request.prompt)
        elif (
            self.profile.prompt_mode == "tempfile"
            and prompt_file is not None
            and not used_prompt_file_placeholder
        ):
            rendered.append(str(prompt_file))

        return rendered

    def _validate_resolved_executable(self, resolved_exe: str) -> None:
        if self.profile.prompt_mode == "arg" and resolved_exe.lower().endswith((".cmd", ".bat")):
            raise ValueError(
                f"Provider '{self.name}': prompt_mode='arg' is unsafe for Windows "
                f".cmd/.bat wrapper '{resolved_exe}' (8191-character command-line limit "
                f"and cmd.exe metacharacter parsing). Use prompt_mode='stdin' or 'tempfile'."
            )

    def invoke(self, request: ProviderRequest) -> ProviderResponse:
        resolved_env, _ = resolve_env_map(self.profile.env, strict=True)
        resolved_env.update(request.extra_env)

        lookup_env = {**os.environ, **resolved_env}
        resolved_base_cmd = resolve_executable(self.profile.command, env=lookup_env)
        self._validate_resolved_executable(resolved_base_cmd[0])

        timeout_sec = min(request.timeout_seconds, self.profile.timeout_seconds)

        # Store temporary prompt/output files strictly outside the target repo and .pipeline/
        with tempfile.TemporaryDirectory(prefix="orch_cli_") as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            prompt_file: Path | None = None
            output_file: Path | None = None

            if self.profile.prompt_mode == "tempfile":
                prompt_file = tmp_dir / "prompt.md"
                prompt_file.write_text(request.prompt, encoding="utf-8", newline="\n")

            if any("{output_file}" in tok for tok in self.profile.args_template):
                output_file = tmp_dir / "output.txt"

            args = self._render_args(
                request=request,
                prompt_file=prompt_file,
                output_file=output_file,
            )
            full_command = [*resolved_base_cmd, *args]
            stdin_data = request.prompt if self.profile.prompt_mode == "stdin" else None

            proc_res = run_noninteractive_process(
                full_command,
                cwd=request.workdir,
                env=resolved_env,
                stdin_data=stdin_data,
                timeout_seconds=timeout_sec,
            )

            file_output = ""
            if output_file is not None and output_file.exists():
                file_output = output_file.read_text(encoding="utf-8", errors="replace")

        effective_raw = file_output if file_output.strip() else proc_res.stdout
        combined_for_rl = f"{proc_res.stdout}\n{proc_res.stderr}"
        rate_limited, retry_after = self._detect_rate_limit(combined_for_rl)
        parsed_text = self._parse_output(effective_raw)

        return ProviderResponse(
            provider_name=self.name,
            exit_code=proc_res.exit_code,
            raw_stdout=proc_res.stdout,
            raw_stderr=proc_res.stderr,
            parsed_text=parsed_text,
            duration_seconds=proc_res.duration_seconds,
            timed_out=proc_res.timed_out,
            rate_limited=rate_limited,
            retry_after_seconds=retry_after,
            metadata={
                "kind": "cli",
                "command": self.profile.command[0],
                "prompt_mode": self.profile.prompt_mode,
                "used_output_file": bool(file_output.strip()),
            },
        )

    def healthcheck(self) -> tuple[bool, str]:
        if not self.profile.command:
            return False, "No command configured"
        if any("{model}" in tok for tok in self.profile.args_template) and not (
            self.profile.model and self.profile.model.strip()
        ):
            return (
                False,
                "args_template references '{model}', but profile 'model' is missing or empty",
            )
        exe = self.profile.command[0]
        resolved = shutil.which(exe, path=os.environ.get("PATH"))
        if resolved is None:
            return False, f"Executable '{exe}' not found in PATH"
        resolved_cmd = resolve_executable(self.profile.command)
        self._validate_resolved_executable(resolved_cmd[0])
        _, missing_env = resolve_env_map(self.profile.env, strict=False)
        if missing_env:
            return (
                False,
                f"Executable '{resolved_cmd[0]}' found, but missing required env var(s): "
                f"{', '.join(missing_env)}",
            )
        return True, f"Executable resolved: {resolved_cmd[0]}"
