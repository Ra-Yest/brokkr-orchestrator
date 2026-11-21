"""Pluggable agent provider interfaces and built-in CLI/API implementations."""

from orchestrator.providers.api_provider import ApiAgentProvider
from orchestrator.providers.base import (
    BaseAgentProvider,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
)
from orchestrator.providers.cli_provider import CliAgentProvider

__all__ = [
    "ApiAgentProvider",
    "BaseAgentProvider",
    "CliAgentProvider",
    "ProviderRegistry",
    "ProviderRequest",
    "ProviderResponse",
]
