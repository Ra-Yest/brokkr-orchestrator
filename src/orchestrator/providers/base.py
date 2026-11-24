"""Abstract BaseAgentProvider contract and ProviderRegistry."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from orchestrator.config import PipelineConfig, ProviderProfileConfig
from orchestrator.models import AgentRole


class ProviderRequest(BaseModel):
    """Normalized invocation request passed to any BaseAgentProvider implementation."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_id: str
    task_id: str | None = None
    role: AgentRole
    prompt: str
    static_prefix: str = ""
    dynamic_body: str = ""
    static_prefix_sha256: str = ""
    workdir: Path
    timeout_seconds: int = Field(default=900, ge=1)
    idle_timeout_seconds: int | None = Field(default=None, ge=1)
    idle_check_process_cpu: bool = True
    attempt_dir: Path
    extra_env: dict[str, str] = Field(default_factory=dict)
    on_first_output: Callable[[str], None] | None = Field(default=None, exclude=True)


class ProviderResponse(BaseModel):
    """Normalized result returned by any BaseAgentProvider implementation."""

    provider_name: str
    exit_code: int
    raw_stdout: str = ""
    raw_stderr: str = ""
    parsed_text: str = ""
    duration_seconds: float = 0.0
    timed_out: bool = False
    idle_timed_out: bool = False
    received_meaningful_output: bool = False
    rate_limited: bool = False
    retry_after_seconds: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BaseAgentProvider(ABC):
    """Abstract interface decoupling the orchestrator core from any specific LLM/CLI tool."""

    def __init__(self, name: str, profile: ProviderProfileConfig) -> None:
        self.name = name
        self.profile = profile

    @property
    def can_edit_files(self) -> bool:
        return bool(self.profile.can_edit_files)

    @abstractmethod
    def invoke(self, request: ProviderRequest) -> ProviderResponse:
        """Execute the provider non-interactively and return a structured ProviderResponse."""

    @abstractmethod
    def healthcheck(self) -> tuple[bool, str]:
        """Verify local binary/credentials availability for preflight doctor checks."""


class ProviderRegistry:
    """Registry mapping profile names and pipeline roles to concrete BaseAgentProvider instances."""

    def __init__(
        self,
        providers: dict[str, BaseAgentProvider],
        role_map: dict[AgentRole, str],
    ) -> None:
        self._providers: dict[str, BaseAgentProvider] = dict(providers)
        self._role_map: dict[AgentRole, str] = dict(role_map)

    def register(self, name: str, provider: BaseAgentProvider) -> None:
        self._providers[name] = provider

    def get(self, name: str) -> BaseAgentProvider:
        if name not in self._providers:
            raise KeyError(f"Unknown provider profile: '{name}'")
        return self._providers[name]

    def profile_name_for_role(self, role: AgentRole) -> str:
        if role not in self._role_map:
            raise KeyError(f"No provider profile mapped for role '{role.value}'")
        return self._role_map[role]

    def for_role(self, role: AgentRole) -> BaseAgentProvider:
        return self.get(self.profile_name_for_role(role))

    def all_providers(self) -> dict[str, BaseAgentProvider]:
        return dict(self._providers)

    @classmethod
    def from_config(cls, config: PipelineConfig) -> ProviderRegistry:
        from orchestrator.providers.api_provider import ApiAgentProvider
        from orchestrator.providers.cli_provider import CliAgentProvider

        providers: dict[str, BaseAgentProvider] = {}
        for name, profile in config.providers.items():
            if profile.kind == "cli":
                providers[name] = CliAgentProvider(name=name, profile=profile)
            elif profile.kind == "api":
                providers[name] = ApiAgentProvider(name=name, profile=profile)
            else:
                raise ValueError(f"Unsupported provider kind: {profile.kind}")
        return cls(providers=providers, role_map=config.roles.as_dict())
