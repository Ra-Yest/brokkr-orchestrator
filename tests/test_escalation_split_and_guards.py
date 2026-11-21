"""Integration tests for NEEDS_ARCH_CHANGE escalation, hard-task splitting, unauthorized commits, untracked files, and Integrity Guard."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from orchestrator.models import AgentRole, EventType, RunStatus, Task, TaskComplexity, TaskStatus
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.runner import OrchestratorFatalError, OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import git, make_mock_registry, make_test_config, ok_response, write_tasks_file


def test_escalation_needs_arch_change_invokes_architect_and_resumes_worker(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="M-1",
            title="Use new shared constant in alpha",
            description="Requires FEATURE_FLAG in src/shared/schema.py.",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config()
    store = LocalJsonTaskStore(tasks_path)
    worker_calls = {"count": 0}
    architect_calls = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            worker_calls["count"] += 1
            if worker_calls["count"] == 1:
                return ok_response(
                    "provider_main",
                    "BEGIN_ESCALATION\n"
                    "- Blocker: FEATURE_FLAG missing from src/shared/schema.py\n"
                    "- Requested change: export FEATURE_FLAG = True\n"
                    "END_ESCALATION\n"
                    "STATUS: NEEDS_ARCH_CHANGE\n",
                )
            assert "FEATURE_FLAG" in req.prompt
            (temp_git_repo / "src/features/alpha/mod.py").write_text(
                "from src.shared.schema import FEATURE_FLAG\n\ndef alpha() -> bool:\n    return FEATURE_FLAG\n",
                encoding="utf-8",
            )
            return ok_response(
                "provider_main",
                "BEGIN_REPORT\nImplemented alpha using FEATURE_FLAG\nEND_REPORT\nSTATUS: DONE\n",
            )

        if req.role == AgentRole.ARCHITECT:
            architect_calls["count"] += 1
            (temp_git_repo / "src/shared/schema.py").write_text(
                "SCHEMA_VERSION = 2\nFEATURE_FLAG = True\n",
                encoding="utf-8",
            )
            return ok_response(
                "provider_master",
                "BEGIN_REPORT\nAdded FEATURE_FLAG to src/shared/schema.py:2\nEND_REPORT\n"
                "BEGIN_PROGRESS\nAdded FEATURE_FLAG to shared schema.\nEND_PROGRESS\n"
                "STATUS: DONE\n",
            )

        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            return ok_response(req.role.value, "VERDICT: APPROVE\nSolid.\n")

        raise AssertionError(f"Unexpected role {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED
    assert architect_calls["count"] == 1
    assert worker_calls["count"] == 2
    assert store.get_task("M-1").status == TaskStatus.DONE

    events = [
        json.loads(ln)
        for ln in (temp_git_repo / ".pipeline/events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    event_types = [e["event_type"] for e in events]
    assert EventType.ESCALATION_TRIGGERED.value in event_types
    assert EventType.ARCHITECT_RESOLVED.value in event_types


def test_hard_task_auto_splits_after_two_failed_rounds_and_rewires_dependents(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="H-1",
            title="Complex multi-part feature",
            description="Too large for one shot.",
            complexity=TaskComplexity.HARD,
            allowed_paths=["src/features/alpha/**", "src/features/beta/**"],
        ),
        Task(
            id="D-1",
            title="Downstream feature",
            description="Depends on H-1.",
            complexity=TaskComplexity.EASY,
            depends_on=["H-1"],
            allowed_paths=["src/features/gamma/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1, hard_split_after_rounds=2, max_review_rounds=3)
    store = LocalJsonTaskStore(tasks_path)

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_HARD and req.task_id == "H-1":
            (temp_git_repo / "src/features/alpha/mod.py").write_text(
                "def alpha() -> int:\n    return 99\n",
                encoding="utf-8",
            )
            return ok_response("provider_master", "BEGIN_REPORT\nAttempted H-1\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.PRE_REVIEWER and req.task_id == "H-1":
            return ok_response("provider_reviewer_api", "VERDICT: REJECT\nStill missing half the spec.\n")

        if req.role == AgentRole.PLANNER:
            payload = {
                "rationale": "Split into alpha and beta slices",
                "subtasks": [
                    {
                        "id": "H-1.1",
                        "title": "Alpha slice",
                        "description": "Implement alpha",
                        "complexity": "easy",
                        "depends_on": [],
                        "allowed_paths": ["src/features/alpha/**"],
                    },
                    {
                        "id": "H-1.2",
                        "title": "Beta slice",
                        "description": "Implement beta",
                        "complexity": "easy",
                        "depends_on": ["H-1.1"],
                        "allowed_paths": ["src/features/beta/**"],
                    },
                ],
            }
            return ok_response(
                "provider_master",
                f"BEGIN_SUBTASKS_JSON\n{json.dumps(payload)}\nEND_SUBTASKS_JSON\n",
            )

        if req.role == AgentRole.WORKER_EASY:
            target = {
                "H-1.1": "src/features/alpha/slice.py",
                "H-1.2": "src/features/beta/slice.py",
                "D-1": "src/features/gamma/downstream.py",
            }[req.task_id or ""]
            (temp_git_repo / target).write_text("VALUE = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")

        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nGood.\n")

        raise AssertionError(f"Unexpected call {req.role} {req.task_id}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED

    parent = store.get_task("H-1")
    assert parent.status == TaskStatus.DONE
    assert parent.resolution == "split"
    assert parent.subtask_ids == ["H-1.1", "H-1.2"]

    # Downstream D-1 was rewired onto leaf subtask H-1.2 and completed
    d1 = store.get_task("D-1")
    assert d1.depends_on == ["H-1.2"]
    assert d1.status == TaskStatus.DONE
    assert store.get_task("H-1.1").status == TaskStatus.DONE
    assert store.get_task("H-1.2").status == TaskStatus.DONE


def test_unauthorized_agent_commit_and_untracked_placeholder_are_caught(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="G-1",
            title="Guard test task",
            description="Write clean code in alpha.",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1, max_gate_retries=3)
    store = LocalJsonTaskStore(tasks_path)
    calls = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            calls["count"] += 1
            new_file = temp_git_repo / "src/features/alpha/untracked_new.py"
            if calls["count"] == 1:
                # Sneakily create an untracked file with TODO AND self-commit it!
                new_file.write_text("def f():\n    # TODO: cheat\n    return 1\n", encoding="utf-8")
                git(temp_git_repo, "add", "-A")
                git(temp_git_repo, "commit", "-m", "agent rogue commit")
                return ok_response("provider_fast", "STATUS: DONE\n")
            # Second attempt cleans up the placeholder
            new_file.write_text("def f() -> int:\n    return 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nClean impl\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.REVIEWER:
            assert "def f() -> int:" in req.prompt
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nClean.\n")
        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED
    assert calls["count"] == 2

    events = [
        json.loads(ln)["event_type"]
        for ln in (temp_git_repo / ".pipeline/events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert EventType.AGENT_COMMIT_SOFT_RESET.value in events
    assert EventType.PLACEHOLDER_GUARD_FAILED.value in events


def test_integrity_guard_catches_agent_tampering_with_pipeline_dir(temp_git_repo: Path) -> None:
    tasks = [
        Task(
            id="I-1",
            title="Tamper test",
            description="Should fail if agent writes into .pipeline/.",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(
        batch_review_size=1,
        max_review_rounds=1,
        hard_split_after_rounds=1,
        max_gate_retries=1,
    )
    store = LocalJsonTaskStore(tasks_path)

    def handler(req: ProviderRequest) -> ProviderResponse:
        # Agent violates rule and writes a file inside .pipeline/
        (temp_git_repo / ".pipeline/rogue.txt").write_text("tampered", encoding="utf-8")
        return ok_response("provider_fast", "STATUS: DONE\n")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED
    assert store.get_task("I-1").status == TaskStatus.FAILED

    events = [
        json.loads(ln)["event_type"]
        for ln in (temp_git_repo / ".pipeline/events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert EventType.RED_ZONE_VIOLATION.value in events


def test_readonly_reviewer_modifying_files_triggers_fatal_rollback(temp_git_repo: Path) -> None:
    tasks = [
        Task(
            id="RO-1",
            title="Read-only reviewer guard",
            description="Implement helper.",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1)
    store = LocalJsonTaskStore(tasks_path)

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/features/alpha/ok.py").write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            # Rogue read-only reviewer edits a file!
            (temp_git_repo / "src/features/alpha/rogue_edit.py").write_text("HACK = 1\n", encoding="utf-8")
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\n")
        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    with pytest.raises(OrchestratorFatalError, match="Read-only role 'reviewer' modified"):
        runner.run()
    assert not (temp_git_repo / "src/features/alpha/rogue_edit.py").exists()
