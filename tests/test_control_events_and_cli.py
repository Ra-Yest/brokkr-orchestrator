"""Tests for race-free Control Plane, legacy control.json, INBOX.md, rate-limit pause, refactorer, and CLI."""

from __future__ import annotations

import json
from pathlib import Path

from orchestrator.cli import main as cli_main
from orchestrator.control import ControlPlane, InboxManager, enqueue_control_command
from orchestrator.models import (
    AgentRole,
    ControlAction,
    EventType,
    RunStatus,
    Task,
    TaskComplexity,
    TaskStatus,
)
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.runner import OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import make_mock_registry, make_test_config, ok_response, write_tasks_file


def test_control_plane_inbox_skip_retry_abort_and_legacy_control_json(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="C-1",
            title="First task",
            description="Alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="C-2",
            title="Dependent task",
            description="Beta depends on C-1",
            complexity=TaskComplexity.EASY,
            depends_on=["C-1"],
            allowed_paths=["src/features/beta/**"],
        ),
        Task(
            id="C-3",
            title="Independent task",
            description="Gamma",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/gamma/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1)
    store = LocalJsonTaskStore(tasks_path)
    pipeline_dir = temp_git_repo / ".pipeline"

    # Drop a race-free skip_task command into control_inbox/ AND a legacy control.json
    cmd_skip, inbox_file = enqueue_control_command(
        pipeline_dir,
        action=ControlAction.SKIP_TASK,
        task_id="C-1",
        reason="Defer C-1",
    )
    assert inbox_file.exists()

    # Also drop INBOX.md operator instruction
    (pipeline_dir / "INBOX.md").write_text("Use strict docstrings on C-3.\n", encoding="utf-8")

    seen_prompts: list[str] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            seen_prompts.append(req.prompt)
            (temp_git_repo / "src/features/gamma/done.py").write_text(
                '"""Gamma module."""\nVAL = 3\n',
                encoding="utf-8",
            )
            return ok_response("provider_fast", "BEGIN_REPORT\nDone C-3\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED

    # C-1 skipped -> C-2 automatically blocked; C-3 ran with INBOX.md injected!
    assert store.get_task("C-1").status == TaskStatus.FAILED
    assert store.get_task("C-1").resolution == "skipped"
    assert store.get_task("C-2").status == TaskStatus.BLOCKED
    assert store.get_task("C-3").status == TaskStatus.DONE
    assert any("Use strict docstrings on C-3." in p for p in seen_prompts)
    assert InboxManager(pipeline_dir).peek() is None

    # Control inbox file moved to control_processed/
    assert not inbox_file.exists()
    processed_file = pipeline_dir / "control_processed" / f"{cmd_skip.id}.json"
    assert processed_file.exists()

    # Now enqueue retry_task for C-1 via legacy control.json -> C-1 becomes TODO and C-2 unblocks!
    (pipeline_dir / "control.json").write_text(
        json.dumps({"id": "legacy_retry_1", "action": "retry_task", "task_id": "C-1"}),
        encoding="utf-8",
    )
    # Also enqueue abort right after via control_inbox so runner stops after applying retry
    enqueue_control_command(pipeline_dir, action=ControlAction.ABORT)

    runner2 = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner2.run() == RunStatus.ABORTED
    assert store.get_task("C-1").status == TaskStatus.TODO
    assert store.get_task("C-2").status == TaskStatus.TODO

    # Verify REPORT.md was generated
    report_text = (pipeline_dir / "REPORT.md").read_text(encoding="utf-8")
    assert "Manual Verification Checklist" in report_text
    assert "C-3" in report_text


def test_rate_limit_pause_and_scheduled_refactorer(temp_git_repo: Path) -> None:
    tasks = [
        Task(
            id="R-1",
            title="Rate-limited then refactored",
            description="Implement alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(
        batch_review_size=1,
        refactor_every_n_tasks=1,
        refactor_on_stage_complete=False,
    )
    store = LocalJsonTaskStore(tasks_path)
    worker_calls = {"count": 0}
    refactor_calls = {"count": 0}
    slept: list[float] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            worker_calls["count"] += 1
            if worker_calls["count"] == 1:
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=429,
                    raw_stderr="429 rate limit exceeded",
                    rate_limited=True,
                    retry_after_seconds=1,
                )
            (temp_git_repo / "src/features/alpha/mod.py").write_text(
                "def alpha() -> int:\n    return 10\n",
                encoding="utf-8",
            )
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        if req.role == AgentRole.REFACTORER:
            refactor_calls["count"] += 1
            (temp_git_repo / "src/features/alpha/mod.py").write_text(
                '"""Alpha module."""\n\ndef alpha() -> int:\n    return 10\n',
                encoding="utf-8",
            )
            return ok_response(
                "provider_master",
                "BEGIN_REPORT\nAdded docstring\nEND_REPORT\n"
                "BEGIN_PROGRESS\nRefactored alpha\nEND_PROGRESS\nSTATUS: DONE\n",
            )

        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=lambda sec: slept.append(sec),
    )
    assert runner.run() == RunStatus.COMPLETED
    assert worker_calls["count"] == 2
    assert refactor_calls["count"] == 1

    state_data = json.loads((temp_git_repo / ".pipeline/state.json").read_text(encoding="utf-8"))
    assert state_data["provider_quotas"]["provider_fast"]["last_rate_limited_at"] is not None
    assert state_data["accepted_since_last_refactor"] == 0

    events = [
        json.loads(ln)["event_type"]
        for ln in (temp_git_repo / ".pipeline/events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert EventType.PROVIDER_RATE_LIMITED.value in events
    assert EventType.REFACTOR_FINISHED.value in events


def test_cli_init_control_and_status_on_fresh_repo(tmp_path: Path) -> None:
    fresh_repo = tmp_path / "fresh"
    fresh_repo.mkdir()
    # Initialize git repo
    import subprocess

    subprocess.run(["git", "init"], cwd=str(fresh_repo), check=True, capture_output=True)

    # `init` copies templates, creates .pipeline/, updates .gitignore, runs doctor (providers may fail healthcheck if not installed)
    cli_main(["init", "--repo", str(fresh_repo), "--stack", "python"])

    assert (fresh_repo / "AGENTS.md").exists()
    assert (fresh_repo / ".agents/worker.md").exists()
    assert (fresh_repo / ".agents/anti_laziness_openai.md").exists()
    assert (fresh_repo / ".pipeline/pipeline.yaml").exists()
    assert (fresh_repo / ".pipeline/tasks.json").exists()
    assert ".pipeline/" in (fresh_repo / ".gitignore").read_text(encoding="utf-8")

    # `control` writes race-free file into control_inbox/
    rc = cli_main(["control", "pause", "--repo", str(fresh_repo), "--reason", "Check UI"])
    assert rc == 0
    cp = ControlPlane(fresh_repo / ".pipeline")
    pending = cp.poll_pending(set())
    assert len(pending) == 1
    assert pending[0][0].action == ControlAction.PAUSE_AFTER_CURRENT

    # `status` runs cleanly
    assert cli_main(["status", "--repo", str(fresh_repo)]) == 0
