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
    anti_laziness: bool = False
    rate_limit_patterns: list[str] = Field(
        default_factory=lambda: list(DEFAULT_RATE_LIMIT_PATTERNS)
    )
    rate_limit_cooldown_seconds: int = Field(default=900, ge=1)
    quota_window_seconds: int = Field(default=18000, ge=60)
    quota_max_requests: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _validate_profile(self) -> ProviderProfileConfig:
        if self.can_edit_files is None:
            self.can_edit_files = self.kind == "cli"

        if self.kind == "cli":
            if not self.command:
                raise ValueError("CLI provider profile requires non-empty 'command'")
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

        for pattern in self.rate_limit_patterns:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"Invalid rate_limit_pattern '{pattern}': {exc}") from exc

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


class DiffLimitConfig(BaseModel):
    max_files: int = Field(ge=1)
    max_lines: int = Field(ge=1)


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
    "build/**",
    "**/*.generated.*",
    "**/*.min.js",
    "**/*.min.css",
]


class DiffGuardConfig(BaseModel):
    limits: dict[TaskComplexity, DiffLimitConfig] = Field(
        default_factory=lambda: {
            TaskComplexity.EASY: DiffLimitConfig(max_files=5, max_lines=200),
            TaskComplexity.MEDIUM: DiffLimitConfig(max_files=12, max_lines=600),
            TaskComplexity.HARD: DiffLimitConfig(max_files=25, max_lines=1500),
        }
    )
    architect_limit: DiffLimitConfig = Field(
        default_factory=lambda: DiffLimitConfig(max_files=15, max_lines=800)
    )
    refactorer_limit: DiffLimitConfig = Field(
        default_factory=lambda: DiffLimitConfig(max_files=30, max_lines=1800)
    )
    exclude_from_review: list[str] = Field(default_factory=lambda: list(DEFAULT_DIFF_EXCLUDES))


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
    max_review_rounds: int = Field(default=3, ge=1)
    max_gate_retries_per_round: int = Field(default=3, ge=1)
    hard_split_after_rounds: int = Field(default=2, ge=1)
    batch_review_size: int = Field(default=3, ge=1)
    refactor_every_n_tasks: int = Field(default=6, ge=0)
    refactor_on_stage_complete: bool = True
    cleanup_ports: list[int] = Field(default_factory=list)
    transient_retry_delays_seconds: list[int] = Field(default_factory=lambda: [10, 30, 90])
    retry_on_timeout_readonly: int = Field(default=1, ge=0)
    rate_limit_poll_seconds: float = Field(default=2.0, gt=0)
    control_poll_seconds: float = Field(default=1.0, gt=0)
    auto_commit: bool = True
    commit_prefix: str = "feat"
    strict_clean_start: bool = True

    @model_validator(mode="after")
    def _validate_workflow(self) -> WorkflowConfig:
        if self.hard_split_after_rounds > self.max_review_rounds:
            raise ValueError("hard_split_after_rounds cannot exceed max_review_rounds")
        if any(delay < 0 for delay in self.transient_retry_delays_seconds):
            raise ValueError("transient_retry_delays_seconds entries must be >= 0")
        return self


class TelegramNotificationConfig(BaseModel):
    enabled: bool = False
    bot_token_env: str = "TELEGRAM_BOT_TOKEN"
    chat_id_env: str = "TELEGRAM_CHAT_ID"
    timeout_seconds: int = Field(default=10, ge=1)


class NotificationsConfig(BaseModel):
    telegram: TelegramNotificationConfig = Field(default_factory=TelegramNotificationConfig)


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
            "build",
            ".next",
        ]
    )


class PipelineConfig(BaseModel):
    """Root validated configuration loaded from pipeline.yaml."""

    schema_version: int = 1
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    providers: dict[str, ProviderProfileConfig] = Field(min_length=1)
    roles: RoleMappingConfig
    zones: ZonesConfig = Field(default_factory=ZonesConfig)
    diff_guard: DiffGuardConfig = Field(default_factory=DiffGuardConfig)
    placeholder_guard: PlaceholderGuardConfig = Field(default_factory=PlaceholderGuardConfig)
    gates: list[GateConfig] = Field(default_factory=list)
    workflow: WorkflowConfig = Field(default_factory=WorkflowConfig)
    notifications: NotificationsConfig = Field(default_factory=NotificationsConfig)

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
        return self

    def pipeline_path(self, repo_root: Path) -> Path:
        return repo_root / self.project.pipeline_dir

    def tasks_path(self, repo_root: Path) -> Path:
        candidate = Path(self.project.tasks_file)
        if candidate.is_absolute():
            return candidate
        return self.pipeline_path(repo_root) / candidate


def load_pipeline_config(path: Path) -> PipelineConfig:
    """Load and validate pipeline.yaml into a PipelineConfig instance."""
    if not path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Pipeline config must be a YAML mapping: {path}")
    return PipelineConfig.model_validate(raw)
