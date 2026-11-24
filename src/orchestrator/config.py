"""Configuration schemas and loader for pipeline.yaml (Pydantic v2)."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from orchestrator.models import WRITING_ROLES, AgentRole, TaskComplexity

_ENV_TOKEN_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?:(:?-)([^}]*))?\}")
_FULL_ENV_TOKEN_RE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)(?:(:?-)([^}]*))?\}$")


class MissingEnvVarError(RuntimeError):
    """Raised when a required environment variable reference is missing."""


class EnvEntryConfig(BaseModel):
    """Explicit environment variable mapping entry with optionality."""

    value: str | None = None
    from_env: str | None = None
    default: str | None = None
    optional: bool = False

    @model_validator(mode="after")
    def _validate_source(self) -> EnvEntryConfig:
        if self.value is None and self.from_env is None:
            raise ValueError("EnvEntryConfig requires either 'value' or 'from_env'")
        return self


EnvSpecValue = str | EnvEntryConfig


def resolve_env_map(
    env_spec: Mapping[str, EnvSpecValue],
    host_env: Mapping[str, str] | None = None,
    *,
    strict: bool = True,
) -> tuple[dict[str, str], list[str]]:
    """Resolve provider/gate environment variables supporting optional `${VAR:-}` syntax.

    Returns (resolved_vars, missing_required_var_names).
    When an entry is `${VAR:-}` (empty default) and VAR is unset/empty, the key is omitted
    so CLI tools like `codex exec` under OAuth subscription do not receive an empty API key.
    """
    source_env = os.environ if host_env is None else host_env
    resolved: dict[str, str] = {}
    missing: list[str] = []

    for key, raw_val in env_spec.items():
        if isinstance(raw_val, EnvEntryConfig):
            if raw_val.from_env is not None:
                val = source_env.get(raw_val.from_env)
                if val:
                    resolved[key] = val
                elif raw_val.default is not None:
                    if raw_val.default != "":
                        resolved[key] = raw_val.default
                elif raw_val.optional:
                    continue
                else:
                    missing.append(raw_val.from_env)
            elif raw_val.value is not None:
                val_res, val_missing, omit = _resolve_env_string(
                    raw_val.value,
                    source_env,
                    force_optional=raw_val.optional,
                )
                missing.extend(val_missing)
                if not omit and val_res is not None:
                    resolved[key] = val_res
            continue

        val_res, val_missing, omit = _resolve_env_string(raw_val, source_env)
        missing.extend(val_missing)
        if not omit and val_res is not None:
            resolved[key] = val_res

    if strict and missing:
        unique_missing = sorted(set(missing))
        raise MissingEnvVarError(
            f"Missing required environment variable(s): {', '.join(unique_missing)}"
        )

    return resolved, sorted(set(missing))


def expand_env_tokens(
    raw_value: str,
    host_env: Mapping[str, str] | None = None,
    *,
    strict: bool = True,
) -> str:
    """Expand `${VAR}` / `${VAR:-default}` tokens in a string (e.g., external file paths)."""
    source_env = os.environ if host_env is None else host_env
    rendered, missing, _ = _resolve_env_string(raw_value, source_env, force_optional=not strict)
    if strict and missing:
        unique_missing = sorted(set(missing))
        raise MissingEnvVarError(
            f"Missing required environment variable(s): {', '.join(unique_missing)}"
        )
    return rendered or ""


def _resolve_env_string(
    raw_value: str,
    source_env: Mapping[str, str],
    *,
    force_optional: bool = False,
) -> tuple[str | None, list[str], bool]:
    """Resolve `${VAR}`, `${VAR:-default}`, and `${VAR-default}` in a string.

    Returns (resolved_string_or_none, missing_vars, should_omit_key).
    """
    full_match = _FULL_ENV_TOKEN_RE.match(raw_value)
    if full_match:
        var_name, op, default_val = full_match.groups()
        val = source_env.get(var_name)
        if op == ":-":
            if val is not None and val != "":
                return val, [], False
            if default_val:
                return default_val, [], False
            return None, [], True
        if op == "-":
            if val is not None:
                return val, [], False
            if default_val:
                return default_val, [], False
            return None, [], True
        # No default operator
        if val is not None and val != "":
            return val, [], False
        if force_optional:
            return None, [], True
        return None, [var_name], True

    missing: list[str] = []

    def _replace(match: re.Match[str]) -> str:
        var_name, op, default_val = match.groups()
        val = source_env.get(var_name)
        if op == ":-":
            return val if (val is not None and val != "") else (default_val or "")
        if op == "-":
            return val if val is not None else (default_val or "")
        if val is not None:
            return val
        if force_optional:
            return ""
        missing.append(var_name)
        return ""

    rendered = _ENV_TOKEN_RE.sub(_replace, raw_value)
    return rendered, missing, False


ProviderKind = Literal["cli", "api"]
PromptMode = Literal["stdin", "arg", "tempfile"]
OutputParserKind = Literal["plain", "json_field", "jsonl_last"]

DEFAULT_RATE_LIMIT_PATTERNS: list[str] = [
    r"\b429\b",
    r"rate[\s_-]*limit",
    r"quota[\s_-]*exceeded",
    r"too many requests",
    r"usage limit reached",
]

DEFAULT_CAPACITY_ERROR_PATTERNS: list[str] = [
    "503",
    "UNAVAILABLE",
    "No capacity available",
    "overloaded",
    "Service Unavailable",
]

DEFAULT_TRANSIENT_ERROR_PATTERNS: list[str] = [
    "502",
    "Bad Gateway",
    "ECONNRESET",
    "ETIMEDOUT",
    "socket hang up",
]

DEFAULT_STALL_STDOUT_PATTERNS: list[str] = [
    r"waiting for \d+ background task",
    r"root agent idle; waiting for",
]

DEFAULT_PAIRED_PATHS: dict[str, list[str]] = {
    "**/pubspec.yaml": ["**/pubspec.lock"],
    "**/go.mod": ["**/go.sum"],
    "**/package.json": [
        "**/package-lock.json",
        "**/pnpm-lock.yaml",
        "**/yarn.lock",
    ],
    "**/Cargo.toml": ["**/Cargo.lock"],
}


class ProviderProfileConfig(BaseModel):
    """Profile definition for a CLI or API agent provider."""

    kind: ProviderKind = "cli"
    can_edit_files: bool | None = None
    # CLI settings
    command: list[str] = Field(default_factory=list)
    prompt_mode: PromptMode = "stdin"
    args_template: list[str] = Field(default_factory=list)
    output_parser: OutputParserKind = "plain"
    output_json_path: str | None = None
    # API settings (OpenAI-compatible)
    base_url: str | None = None
    api_key_env: str | None = None
    headers: dict[str, EnvSpecValue] = Field(default_factory=dict)
    max_tokens: int | None = None
    temperature: float | None = None
    # Common settings
    model: str | None = None
    env: dict[str, EnvSpecValue] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=900, ge=1)
    idle_timeout_seconds: int | None = Field(default=None, ge=1)
    idle_check_process_cpu: bool | None = None
    stall_stdout_patterns: list[str] | None = None
    stall_grace_seconds: int | None = Field(default=None, ge=1)
    anti_laziness: bool = False
    rate_limit_patterns: list[str] = Field(
        default_factory=lambda: list(DEFAULT_RATE_LIMIT_PATTERNS)
    )
    rate_limit_cooldown_seconds: int = Field(default=900, ge=1)
    quota_window_seconds: int = Field(default=18000, ge=60)
    quota_max_requests: int | None = Field(default=None, ge=1)
    fallback_providers: list[str] = Field(default_factory=list)
    # Sequential account switching (e.g., aisw use antigravity pro)
    account_group: str | None = None
    account_profile: str | None = None
    switch_command: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_profile(self) -> ProviderProfileConfig:
        if self.can_edit_files is None:
            self.can_edit_files = self.kind == "cli"

        if self.kind == "cli":
            if not self.command:
                raise ValueError("CLI provider profile requires non-empty 'command'")
            if any("{model}" in tok for tok in self.args_template) and not (
                self.model and self.model.strip()
            ):
                raise ValueError(
                    "CLI provider profile uses '{model}' in args_template, "
                    "but 'model' is missing or empty"
                )
            if self.output_parser in ("json_field", "jsonl_last") and not self.output_json_path:
                raise ValueError(
                    f"output_parser='{self.output_parser}' requires 'output_json_path'"
                )
        elif self.kind == "api":
            if self.can_edit_files:
                raise ValueError(
                    "API provider profiles cannot edit local repository files "
                    "(can_edit_files must be false)"
                )
            if not self.base_url:
                raise ValueError("API provider profile requires 'base_url'")
            if not self.model:
                raise ValueError("API provider profile requires 'model'")

        if (self.account_group is None) != (self.account_profile is None):
            raise ValueError(
                "Provider profile must set both 'account_group' and 'account_profile' together"
            )
        if self.account_group is not None and not self.switch_command:
            raise ValueError(
                "Provider profile with 'account_group'/'account_profile' requires non-empty 'switch_command'"
            )

        for pattern in self.rate_limit_patterns:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"Invalid rate_limit_pattern '{pattern}': {exc}") from exc

        for pattern in self.stall_stdout_patterns or []:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"Invalid stall_stdout_pattern '{pattern}': {exc}") from exc

        return self


class RoleMappingConfig(BaseModel):
    """Maps every pipeline role to a configured provider profile."""

    planner: str
    worker_easy: str
    worker_medium: str
    worker_hard: str
    pre_reviewer: str
    reviewer: str
    architect: str
    refactorer: str
    role_prompt_files: dict[str, str] = Field(default_factory=dict)

    def profile_for_role(self, role: AgentRole) -> str:
        value: str = getattr(self, role.value)
        return value

    def as_dict(self) -> dict[AgentRole, str]:
        return {role: self.profile_for_role(role) for role in AgentRole}


class ZonesConfig(BaseModel):
    """Three-zone access policy for repository paths."""

    red_protected: list[str] = Field(
        default_factory=lambda: [
            ".pipeline/**",
            "pipeline.yaml",
            ".env*",
            "**/.env*",
            ".agents/**",
            "AGENTS.md",
            "docs/specs/**",
        ]
    )
    test_patterns: list[str] = Field(
        default_factory=lambda: [
            "tests/**",
            "**/tests/**",
            "**/test_*.py",
            "**/*_test.py",
            "**/*.test.*",
            "**/*.spec.*",
        ]
    )
    yellow_shared: list[str] = Field(
        default_factory=lambda: [
            "pyproject.toml",
            "package.json",
            "tsconfig.json",
            "src/shared/**",
            "src/db/**",
            "src/router*",
            "src/routes/**",
        ]
    )
    enforce_allowed_paths_on_yellow: bool = False


class DiffLimitConfig(BaseModel):
    """Soft and hard Diff Guard thresholds with full backward compatibility for `max_files`/`max_lines`."""

    soft_max_files: int | None = Field(default=None, ge=1)
    soft_max_lines: int | None = Field(default=None, ge=1)
    hard_max_files: int | None = Field(default=None, ge=1)
    hard_max_lines: int | None = Field(default=None, ge=1)
    # Legacy aliases: mapped into hard_max_files / hard_max_lines when set
    max_files: int | None = Field(default=None, ge=1)
    max_lines: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _normalize_limits(self) -> DiffLimitConfig:
        if self.hard_max_files is None and self.max_files is not None:
            self.hard_max_files = self.max_files
        if self.hard_max_lines is None and self.max_lines is not None:
            self.hard_max_lines = self.max_lines
        if self.hard_max_files is None:
            raise ValueError("DiffLimitConfig requires 'hard_max_files' (or legacy 'max_files')")
        if self.hard_max_lines is None:
            raise ValueError("DiffLimitConfig requires 'hard_max_lines' (or legacy 'max_lines')")
        if self.max_files is None:
            self.max_files = self.hard_max_files
        if self.max_lines is None:
            self.max_lines = self.hard_max_lines
        if self.soft_max_files is not None and self.soft_max_files > self.hard_max_files:
            raise ValueError("soft_max_files cannot exceed hard_max_files")
        if self.soft_max_lines is not None and self.soft_max_lines > self.hard_max_lines:
            raise ValueError("soft_max_lines cannot exceed hard_max_lines")
        return self


DEFAULT_DIFF_EXCLUDES: list[str] = [
    "**/package-lock.json",
    "**/pnpm-lock.yaml",
    "**/yarn.lock",
    "**/poetry.lock",
    "**/uv.lock",
    "**/*.svg",
    "**/*.png",
    "**/*.jpg",
    "**/*.jpeg",
    "**/*.webp",
    "**/*.ico",
    "**/*.woff*",
    "dist/**",
    "app/build/**",
    "android/build/**",
    "web/build/**",
    "**/build/outputs/**",
    "**/*.generated.*",
    "**/*.min.js",
    "**/*.min.css",
]

DEFAULT_READONLY_IGNORE_PATHS: list[str] = [
    "go.work.sum",
    "**/go.work.sum",
]

DEFAULT_REVIEW_DIFF_EXCLUDE_PATTERNS: list[str] = [
    "*.lock",
    "**/pubspec.lock",
    "**/go.sum",
    "**/go.work.sum",
    "**/*_gen.go",
    "**/*_gen.dart",
    "**/*.g.dart",
    "**/*.freezed.dart",
]


class DiffGuardConfig(BaseModel):
    exclude_tests_from_limits: bool = True
    hard_max_test_lines: int = Field(default=3000, ge=1)
    limits: dict[TaskComplexity, DiffLimitConfig] = Field(
        default_factory=lambda: {
            TaskComplexity.EASY: DiffLimitConfig(
                soft_max_files=5,
                soft_max_lines=200,
                hard_max_files=8,
                hard_max_lines=400,
            ),
            TaskComplexity.MEDIUM: DiffLimitConfig(
                soft_max_files=12,
                soft_max_lines=600,
                hard_max_files=20,
                hard_max_lines=1400,
            ),
            TaskComplexity.HARD: DiffLimitConfig(
                soft_max_files=25,
                soft_max_lines=1500,
                hard_max_files=40,
                hard_max_lines=2500,
            ),
        }
    )
    architect_limit: DiffLimitConfig = Field(
        default_factory=lambda: DiffLimitConfig(
            soft_max_files=15,
            soft_max_lines=800,
            hard_max_files=25,
            hard_max_lines=1500,
        )
    )
    refactorer_limit: DiffLimitConfig = Field(
        default_factory=lambda: DiffLimitConfig(
            soft_max_files=30,
            soft_max_lines=1800,
            hard_max_files=45,
            hard_max_lines=3000,
        )
    )
    exclude_from_review: list[str] = Field(default_factory=lambda: list(DEFAULT_DIFF_EXCLUDES))
    readonly_ignore_paths: list[str] = Field(
        default_factory=lambda: list(DEFAULT_READONLY_IGNORE_PATHS)
    )
    paired_paths: dict[str, list[str]] = Field(
        default_factory=lambda: {k: list(v) for k, v in DEFAULT_PAIRED_PATHS.items()}
    )


class PlaceholderGuardConfig(BaseModel):
    enabled: bool = True
    extra_patterns: list[str] = Field(default_factory=list)
    allow_in_paths: list[str] = Field(default_factory=list)


class GateConfig(BaseModel):
    name: str = Field(min_length=1)
    command: list[str] = Field(min_length=1)
    timeout_seconds: int = Field(default=300, ge=1)
    kind: Literal["command", "playwright"] = "command"
    optional: bool = False
    env: dict[str, EnvSpecValue] = Field(default_factory=dict)
    cwd: str | None = None


class WorkflowConfig(BaseModel):
    max_attempts: int = Field(default=6, ge=1)
    max_review_rounds: int = Field(default=3, ge=1)
    max_gate_retries_per_round: int = Field(default=3, ge=1)
    hard_split_after_rounds: int = Field(default=2, ge=1)
    batch_review_size: int = Field(default=3, ge=1)
    refactor_every_n_tasks: int = Field(default=6, ge=0)
    refactor_on_stage_complete: bool = True
    cleanup_ports: list[int] = Field(default_factory=list)
    max_transport_retries: int = Field(default=6, ge=0)
    transport_retry_delays_seconds: list[int] = Field(default_factory=lambda: [3, 5, 10, 20, 30])
    transport_failure_max_duration_seconds: int = Field(default=120, ge=1)
    transient_retry_delays_seconds: list[int] = Field(default_factory=lambda: [10, 30, 90])
    retry_on_timeout_readonly: int = Field(default=1, ge=0)
    rate_limit_poll_seconds: float = Field(default=2.0, gt=0)
    control_poll_seconds: float = Field(default=1.0, gt=0)
    auto_commit: bool = True
    commit_prefix: str = "feat"
    strict_clean_start: bool = True
    review_diff_exclude_patterns: list[str] = Field(
        default_factory=lambda: list(DEFAULT_REVIEW_DIFF_EXCLUDE_PATTERNS)
    )
    review_diff_max_file_lines: int = Field(default=400, ge=1)
    review_diff_max_chars: int = Field(default=80000, ge=1)
    idle_check_process_cpu: bool = True
    capacity_error_patterns: list[str] = Field(
        default_factory=lambda: list(DEFAULT_CAPACITY_ERROR_PATTERNS)
    )
    capacity_cooldown_seconds: int = Field(default=90, ge=1)
    transient_error_patterns: list[str] = Field(
        default_factory=lambda: list(DEFAULT_TRANSIENT_ERROR_PATTERNS)
    )
    stall_stdout_patterns: list[str] = Field(
        default_factory=lambda: list(DEFAULT_STALL_STDOUT_PATTERNS)
    )
    stall_grace_seconds: int = Field(default=30, ge=1)
    context_file_max_chars: int = Field(default=12000, ge=1)
    context_files_total_max_chars: int = Field(default=40000, ge=1)

    @model_validator(mode="after")
    def _validate_workflow(self) -> WorkflowConfig:
        if self.hard_split_after_rounds > self.max_review_rounds:
            raise ValueError("hard_split_after_rounds cannot exceed max_review_rounds")
        if any(delay < 0 for delay in self.transient_retry_delays_seconds):
            raise ValueError("transient_retry_delays_seconds entries must be >= 0")
        if any(delay < 0 for delay in self.transport_retry_delays_seconds):
            raise ValueError("transport_retry_delays_seconds entries must be >= 0")
        for group_name, patterns in (
            ("capacity_error_patterns", self.capacity_error_patterns),
            ("transient_error_patterns", self.transient_error_patterns),
            ("stall_stdout_patterns", self.stall_stdout_patterns),
        ):
            for pat in patterns:
                try:
                    re.compile(pat)
                except re.error as exc:
                    raise ValueError(f"Invalid regex in {group_name} '{pat}': {exc}") from exc
        return self


NotificationEvent = Literal[
    "run_started",
    "task_done",
    "task_failed",
    "escalation",
    "pause",
    "run_finished",
    "fatal_error",
    "provider_started",
    "provider_recovered",
    "hibernation",
    "proxy",
    "account_switch_failed",
]

DEFAULT_TELEGRAM_EVENTS: list[NotificationEvent] = [
    "run_started",
    "task_done",
    "task_failed",
    "escalation",
    "pause",
    "run_finished",
    "fatal_error",
]


class TelegramNotificationConfig(BaseModel):
    enabled: bool = False
    bot_token_env: str = "TELEGRAM_BOT_TOKEN"
    chat_id_env: str = "TELEGRAM_CHAT_ID"
    timeout_seconds: int = Field(default=10, ge=1)
    notify_on_provider_started: bool = False
    events: list[NotificationEvent] = Field(
        default_factory=lambda: list(DEFAULT_TELEGRAM_EVENTS)
    )


class NotificationsConfig(BaseModel):
    telegram: TelegramNotificationConfig = Field(default_factory=TelegramNotificationConfig)


class StatusJsonProbeConfig(BaseModel):
    """Declarative JSON state file probe for an external proxy/gateway."""

    path: str = Field(min_length=1)
    route_field: str | None = None
    last_ok_at_field: str | None = None
    disabled_field: str | None = None
    blockers_field: str | None = None
    transient_error_field: str | None = None
    error_field: str | None = None


class LogProbeConfig(BaseModel):
    """Declarative tail-and-regex probe for an external proxy/gateway log file."""

    path: str = Field(min_length=1)
    tail_lines: int = Field(default=80, ge=1)
    ok_patterns: list[str] = Field(default_factory=list)
    transient_patterns: list[str] = Field(default_factory=list)
    fatal_patterns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_patterns(self) -> LogProbeConfig:
        for group_name, patterns in (
            ("ok_patterns", self.ok_patterns),
            ("transient_patterns", self.transient_patterns),
            ("fatal_patterns", self.fatal_patterns),
        ):
            for pat in patterns:
                try:
                    re.compile(pat)
                except re.error as exc:
                    raise ValueError(f"Invalid regex in {group_name} '{pat}': {exc}") from exc
        return self


class ProxyMonitorConfig(BaseModel):
    """Optional vendor-neutral declarative external proxy/gateway monitor (disabled by default)."""

    enabled: bool = False
    config_file: str | None = None
    apply_to_providers: list[str] = Field(default_factory=list)
    proxy_host: str | None = None
    proxy_port: int | None = Field(default=None, ge=1, le=65535)
    tcp_timeout_seconds: float = Field(default=2.0, gt=0)
    status_json: StatusJsonProbeConfig | None = None
    log_probes: list[LogProbeConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_tcp_endpoint(self) -> ProxyMonitorConfig:
        if (self.proxy_host is None) != (self.proxy_port is None):
            raise ValueError(
                "proxy_monitor must set both 'proxy_host' and 'proxy_port' together (or omit both)"
            )
        return self


class ProjectConfig(BaseModel):
    name: str = "target-project"
    pipeline_dir: str = ".pipeline"
    agents_file: str = "AGENTS.md"
    agent_roles_dir: str = ".agents"
    anti_laziness_file: str = ".agents/anti_laziness_openai.md"
    tasks_file: str = "tasks.json"
    repo_map_ignore: list[str] = Field(
        default_factory=lambda: [
            ".git",
            ".pipeline",
            ".venv",
            "venv",
            "node_modules",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            "dist",
            "app/build",
            "android/build",
            "web/build",
            "**/build/outputs",
            ".next",
        ]
    )


class PipelineConfig(BaseModel):
    """Root validated configuration loaded from pipeline.yaml."""

    schema_version: int = 1
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    providers: dict[str, ProviderProfileConfig] = Field(min_length=1)
    roles: RoleMappingConfig
    fallback_providers: dict[AgentRole, list[str]] = Field(default_factory=dict)
    zones: ZonesConfig = Field(default_factory=ZonesConfig)
    diff_guard: DiffGuardConfig = Field(default_factory=DiffGuardConfig)
    placeholder_guard: PlaceholderGuardConfig = Field(default_factory=PlaceholderGuardConfig)
    gates: list[GateConfig] = Field(default_factory=list)
    workflow: WorkflowConfig = Field(default_factory=WorkflowConfig)
    notifications: NotificationsConfig = Field(default_factory=NotificationsConfig)
    proxy_monitor: ProxyMonitorConfig = Field(default_factory=ProxyMonitorConfig)

    @model_validator(mode="after")
    def _validate_roles_and_capabilities(self) -> PipelineConfig:
        for role, profile_name in self.roles.as_dict().items():
            if profile_name not in self.providers:
                raise ValueError(
                    f"Role '{role.value}' references unknown provider profile '{profile_name}'"
                )
            profile = self.providers[profile_name]
            if role in WRITING_ROLES and not profile.can_edit_files:
                raise ValueError(
                    f"Writing role '{role.value}' cannot use provider profile '{profile_name}' "
                    f"because can_edit_files is False"
                )
        for profile_name, profile in self.providers.items():
            for fb_name in profile.fallback_providers:
                if fb_name not in self.providers:
                    raise ValueError(
                        f"Provider '{profile_name}' references unknown fallback provider '{fb_name}'"
                    )
        for role, fb_list in self.fallback_providers.items():
            for fb_name in fb_list:
                if fb_name not in self.providers:
                    raise ValueError(
                        f"fallback_providers for role '{role.value}' references unknown provider '{fb_name}'"
                    )
                if role in WRITING_ROLES and not self.providers[fb_name].can_edit_files:
                    raise ValueError(
                        f"Writing role '{role.value}' cannot use fallback provider '{fb_name}' "
                        f"because can_edit_files is False"
                    )
        for p_name in self.proxy_monitor.apply_to_providers:
            if p_name not in self.providers:
                raise ValueError(
                    f"proxy_monitor.apply_to_providers references unknown provider '{p_name}'"
                )
        return self

    def pipeline_path(self, repo_root: Path) -> Path:
        return repo_root / self.project.pipeline_dir

    def tasks_path(self, repo_root: Path) -> Path:
        candidate = Path(self.project.tasks_file)
        if candidate.is_absolute():
            return candidate
        return self.pipeline_path(repo_root) / candidate


def _merge_external_proxy_monitor_config(
    raw: dict[str, object],
    *,
    base_dir: Path,
) -> dict[str, object]:
    """Merge optional `proxy_monitor.config_file` YAML with explicit fields in `pipeline.yaml`."""
    pm_raw = raw.get("proxy_monitor")
    if not isinstance(pm_raw, dict):
        return raw
    cfg_file = pm_raw.get("config_file")
    if not isinstance(cfg_file, str) or not cfg_file.strip():
        return raw

    enabled_flag = pm_raw.get("enabled")
    is_explicitly_disabled = enabled_flag is False

    expanded_str = expand_env_tokens(cfg_file.strip(), strict=not is_explicitly_disabled)
    if not expanded_str:
        return raw
    candidate = Path(os.path.expanduser(expanded_str))
    ext_path = candidate if candidate.is_absolute() else (base_dir / candidate)

    if not ext_path.is_file():
        if is_explicitly_disabled:
            return raw
        raise FileNotFoundError(f"proxy_monitor.config_file not found: {ext_path}")

    ext_loaded = yaml.safe_load(ext_path.read_text(encoding="utf-8"))
    if ext_loaded is None:
        ext_dict: dict[str, object] = {}
    elif isinstance(ext_loaded, dict):
        inner = ext_loaded.get("proxy_monitor")
        ext_dict = dict(inner) if isinstance(inner, dict) else dict(ext_loaded)
    else:
        raise ValueError(f"proxy_monitor config_file must be a YAML mapping: {ext_path}")

    # Explicit fields in pipeline.yaml override external file values (deep-merge status_json)
    merged: dict[str, object] = {**ext_dict}
    for key, val in pm_raw.items():
        if (
            key == "status_json"
            and isinstance(val, dict)
            and isinstance(merged.get("status_json"), dict)
        ):
            merged["status_json"] = {**merged["status_json"], **val}  # type: ignore[dict-item]
        else:
            merged[key] = val

    updated_raw = dict(raw)
    updated_raw["proxy_monitor"] = merged
    return updated_raw


def load_pipeline_config(path: Path) -> PipelineConfig:
    """Load and validate pipeline.yaml into a PipelineConfig instance."""
    if not path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Pipeline config must be a YAML mapping: {path}")
    merged_raw = _merge_external_proxy_monitor_config(raw, base_dir=path.resolve().parent)
    return PipelineConfig.model_validate(merged_raw)
