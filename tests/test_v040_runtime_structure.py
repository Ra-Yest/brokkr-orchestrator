"""Smoke tests for AFK Orchestrator v0.4.0 modular runtime decomposition and backward compatibility."""

from __future__ import annotations

from pathlib import Path

import orchestrator
import orchestrator.runner as runner_mod
import orchestrator.runtime as runtime_mod
from orchestrator.runtime import (
    PIPELINE_INTEGRITY_EXCLUDES,
    EscalationManager,
    OrchestratorFatalError,
    ProviderExecutor,
    ReviewPipeline,
    RunnerLifecycleManager,
    RunnerLockError,
    classify_provider_failure,
    is_transport_failure_before_response,
)
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import make_mock_registry, make_test_config, ok_response, write_tasks_file


def test_v040_version_and_runtime_exports() -> None:
    assert orchestrator.__version__ == "0.4.0"

    assert runtime_mod.RunnerLifecycleManager is RunnerLifecycleManager
    assert runtime_mod.ProviderExecutor is ProviderExecutor
    assert runtime_mod.ReviewPipeline is ReviewPipeline
    assert runtime_mod.EscalationManager is EscalationManager


def test_runner_backward_compatible_reexports() -> None:
    assert runner_mod.OrchestratorFatalError is OrchestratorFatalError
    assert runner_mod.RunnerLockError is RunnerLockError
    assert runner_mod.PIPELINE_INTEGRITY_EXCLUDES is PIPELINE_INTEGRITY_EXCLUDES
    assert runner_mod.classify_provider_failure is classify_provider_failure
    assert runner_mod.is_transport_failure_before_response is is_transport_failure_before_response
    assert callable(runner_mod.is_process_alive)


def test_orchestrator_runner_wires_runtime_collaborators(temp_git_repo: Path) -> None:
    cfg = make_test_config()
    tasks_path = write_tasks_file(temp_git_repo, [])
    store = LocalJsonTaskStore(tasks_path)
    registry, _ = make_mock_registry(cfg, lambda req: ok_response("m", "STATUS: DONE\n"))

    runner = runner_mod.OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
    )

    assert isinstance(runner._lifecycle, RunnerLifecycleManager)
    assert isinstance(runner._executor, ProviderExecutor)
    assert isinstance(runner._review_pipeline, ReviewPipeline)
    assert isinstance(runner._escalation_manager, EscalationManager)
