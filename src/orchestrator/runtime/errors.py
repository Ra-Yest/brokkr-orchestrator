"""Internal and fatal exception types for the orchestrator runtime."""

from __future__ import annotations

from orchestrator.models import AgentRole


class OrchestratorFatalError(RuntimeError):
    """Unrecoverable internal/environment error requiring immediate fail-fast exit."""


class RunnerLockError(OrchestratorFatalError):
    """Raised when another active orchestrator process holds `.pipeline/runner.lock`."""


class _AbortedByControlError(Exception):
    """Internal flow-control signal raised when an operator sends `abort`."""


class _TaskDeferredOnCooldownError(Exception):
    """Internal signal to return a clean worker task to TODO when another task can run sooner."""

    def __init__(
        self,
        *,
        task_id: str,
        role: AgentRole,
        providers: list[str],
        wait_seconds: int,
    ) -> None:
        super().__init__(f"Task #{task_id} deferred on cooldown ({wait_seconds}s)")
        self.task_id = task_id
        self.role = role
        self.providers = providers
        self.wait_seconds = wait_seconds


class _PausedByInfraError(Exception):
    """Raised when a review/architect/planner/proxy monitor fails so work is preserved."""

    def __init__(
        self,
        reason: str,
        *,
        pause_reason: str | None = None,
        task_id: str | None = None,
        role: AgentRole | None = None,
        provider: str | None = None,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.pause_reason = pause_reason or reason
        self.task_id = task_id
        self.role = role
        self.provider = provider
