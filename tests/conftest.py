"""Shared pytest fixtures: temporary git repositories, mock providers, and config factories."""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from orchestrator.config import (
    DiffGuardConfig,
    DiffLimitConfig,
    GateConfig,
    PipelineConfig,
    ProjectConfig,
    ProviderProfileConfig,
    RoleMappingConfig,
    WorkflowConfig,
    ZonesConfig,
)
from orchestrator.fs_utils import atomic_write_json
from orchestrator.models import Task, TaskComplexity, TasksFile
from orchestrator.providers.base import (
    BaseAgentProvider,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
)

HandlerFn = Callable[[ProviderRequest], ProviderResponse]


class ScriptedMockProvider(BaseAgentProvider):
    """Deterministic mock provider driven by a user-supplied Python callback."""

    def __init__(
        self,
        name: str,
        profile: ProviderProfileConfig,
        handler: HandlerFn,
    ) -> None:
        super().__init__(name=name, profile=profile)
        self.handler = handler
        self.calls: list[ProviderRequest] = []

    def invoke(self, request: ProviderRequest) -> ProviderResponse:
        self.calls.append(request)
        return self.handler(request)

    def healthcheck(self) -> tuple[bool, str]:
        return True, f"mock:{self.name}"


def git(cwd: Path, *args: str) -> str:
    res = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=True,
    )
    return res.stdout.strip()


@pytest.fixture()
def temp_git_repo(tmp_path: Path) -> Path:
    """Create a real initialized git repository with templates, starter source, and existing test."""
    repo = tmp_path / "target_repo"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.email", "ci@example.com")
    git(repo, "config", "user.name", "CI Bot")

    # Copy templates from project root
    templates_src = Path(__file__).resolve().parent.parent / "templates"
    shutil.copy2(templates_src / "AGENTS.md", repo / "AGENTS.md")
    shutil.copytree(templates_src / ".agents", repo / ".agents")

    (repo / ".gitignore").write_text(".pipeline/\n__pycache__/\n*.pyc\n", encoding="utf-8")
    (repo / "src" / "shared").mkdir(parents=True)
    (repo / "src" / "features" / "alpha").mkdir(parents=True)
    (repo / "src" / "features" / "beta").mkdir(parents=True)
    (repo / "src" / "features" / "gamma").mkdir(parents=True)
    (repo / "tests").mkdir(parents=True)

    (repo / "src" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "features" / "alpha" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "features" / "beta" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "features" / "gamma" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "shared" / "schema.py").write_text(
        'SCHEMA_VERSION = 1\n',
        encoding="utf-8",
    )
    (repo / "src" / "features" / "alpha" / "mod.py").write_text(
        "def alpha() -> int:\n    return 1\n",
        encoding="utf-8",
    )
    (repo / "tests" / "test_existing.py").write_text(
        "def test_baseline() -> None:\n    assert True\n",
        encoding="utf-8",
    )

    git(repo, "add", "-A")
    git(repo, "commit", "-m", "chore: initial commit")
    return repo


def make_test_config(
    *,
    gates: list[GateConfig] | None = None,
    batch_review_size: int = 3,
    max_review_rounds: int = 3,
    max_gate_retries: int = 3,
    hard_split_after_rounds: int = 2,
    refactor_every_n_tasks: int = 0,
    refactor_on_stage_complete: bool = False,
    transient_retry_delays_seconds: list[int] | None = None,
    retry_on_timeout_readonly: int = 1,
) -> PipelineConfig:
    cli_edit_profile = ProviderProfileConfig(
        kind="cli",
        can_edit_files=True,
        command=["echo"],
        prompt_mode="stdin",
        timeout_seconds=60,
        anti_laziness=True,
        rate_limit_cooldown_seconds=1,
    )
    cli_main_profile = ProviderProfileConfig(
        kind="cli",
        can_edit_files=True,
        command=["echo"],
        prompt_mode="stdin",
        timeout_seconds=60,
        anti_laziness=False,
        rate_limit_cooldown_seconds=1,
    )
    api_ro_profile = ProviderProfileConfig(
        kind="api",
        can_edit_files=False,
        base_url="https://api.example.com/v1",
        model="mock-reviewer",
        timeout_seconds=60,
        anti_laziness=False,
        rate_limit_cooldown_seconds=1,
    )
    return PipelineConfig(
        project=ProjectConfig(name="test-repo"),
        providers={
            "provider_fast": cli_edit_profile,
            "provider_main": cli_main_profile,
            "provider_master": cli_main_profile,
            "provider_reviewer_api": api_ro_profile,
        },
        roles=RoleMappingConfig(
            planner="provider_master",
            worker_easy="provider_fast",
            worker_medium="provider_main",
            worker_hard="provider_master",
            pre_reviewer="provider_reviewer_api",
            reviewer="provider_reviewer_api",
            architect="provider_master",
            refactorer="provider_master",
        ),
        zones=ZonesConfig(),
        diff_guard=DiffGuardConfig(
            limits={
                TaskComplexity.EASY: DiffLimitConfig(max_files=5, max_lines=200),
                TaskComplexity.MEDIUM: DiffLimitConfig(max_files=12, max_lines=600),
                TaskComplexity.HARD: DiffLimitConfig(max_files=25, max_lines=1500),
            }
        ),
        gates=gates or [],
        workflow=WorkflowConfig(
            max_review_rounds=max_review_rounds,
            max_gate_retries_per_round=max_gate_retries,
            hard_split_after_rounds=hard_split_after_rounds,
            batch_review_size=batch_review_size,
            refactor_every_n_tasks=refactor_every_n_tasks,
            refactor_on_stage_complete=refactor_on_stage_complete,
            transient_retry_delays_seconds=(
                [10, 30, 90]
                if transient_retry_delays_seconds is None
                else transient_retry_delays_seconds
            ),
            retry_on_timeout_readonly=retry_on_timeout_readonly,
            rate_limit_poll_seconds=0.01,
            control_poll_seconds=0.01,
        ),
    )


def write_tasks_file(repo: Path, tasks: list[Task]) -> Path:
    tasks_path = repo / ".pipeline" / "tasks.json"
    atomic_write_json(tasks_path, TasksFile(tasks=tasks).model_dump(mode="json"))
    return tasks_path


def make_mock_registry(
    config: PipelineConfig,
    handler: HandlerFn,
) -> tuple[ProviderRegistry, dict[str, ScriptedMockProvider]]:
    mocks: dict[str, ScriptedMockProvider] = {}
    for name, profile in config.providers.items():
        mocks[name] = ScriptedMockProvider(name=name, profile=profile, handler=handler)
    registry = ProviderRegistry(providers=dict(mocks), role_map=config.roles.as_dict())
    return registry, mocks


def ok_response(provider_name: str, text: str) -> ProviderResponse:
    return ProviderResponse(
        provider_name=provider_name,
        exit_code=0,
        raw_stdout=text,
        parsed_text=text,
        duration_seconds=0.01,
    )
