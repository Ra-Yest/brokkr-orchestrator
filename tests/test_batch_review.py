"""Integration tests for batch review independence, selective git revert, deferred PROGRESS.md, and yellow-zone escalation."""

from __future__ import annotations

import json
from pathlib import Path

from orchestrator.models import AgentRole, EventType, RunStatus, Task, TaskComplexity, TaskStatus
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.runner import OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import make_mock_registry, make_test_config, ok_response, write_tasks_file


def test_batch_review_partial_approve_reject_revert_and_deferred_progress(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="E-1",
            title="Create alpha helper",
            description="Add helper in alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="E-2",
            title="Create beta helper",
            description="Add helper in beta",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
        Task(
            id="E-3",
            title="Create gamma helper",
            description="Add helper in gamma",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/gamma/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=3, max_review_rounds=3)
    store = LocalJsonTaskStore(tasks_path)

    e2_attempts = {"count": 0}
    batch_review_calls = {"count": 0}
    progress_seen_before_first_batch = {"text": ""}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            if req.task_id == "E-1":
                (temp_git_repo / "src/features/alpha/helper.py").write_text(
                    "def alpha_helper() -> str:\n    return 'ok'\n",
                    encoding="utf-8",
                )
                return ok_response(
                    "provider_fast",
                    "BEGIN_REPORT\nAdded alpha_helper\nEND_REPORT\n"
                    "BEGIN_PROGRESS\nE-1_PROGRESS_MARKER\nEND_PROGRESS\nSTATUS: DONE\n",
                )
            if req.task_id == "E-2":
                e2_attempts["count"] += 1
                val = "bad" if e2_attempts["count"] == 1 else "fixed"
                (temp_git_repo / "src/features/beta/helper.py").write_text(
                    f"def beta_helper() -> str:\n    return '{val}'\n",
                    encoding="utf-8",
                )
                return ok_response(
                    "provider_fast",
                    f"BEGIN_REPORT\nBeta version {val}\nEND_REPORT\n"
                    f"BEGIN_PROGRESS\nE-2_PROGRESS_{val}\nEND_PROGRESS\nSTATUS: DONE\n",
                )
            if req.task_id == "E-3":
                # Verify PROGRESS.md does NOT contain E-1 or E-2 notes before batch APPROVE!
                progress_seen_before_first_batch["text"] = (
                    temp_git_repo / ".pipeline/PROGRESS.md"
                ).read_text(encoding="utf-8")
                (temp_git_repo / "src/features/gamma/helper.py").write_text(
                    "def gamma_helper() -> str:\n    return 'ok'\n",
                    encoding="utf-8",
                )
                return ok_response(
                    "provider_fast",
                    "BEGIN_REPORT\nAdded gamma_helper\nEND_REPORT\n"
                    "BEGIN_PROGRESS\nE-3_PROGRESS_MARKER\nEND_PROGRESS\nSTATUS: DONE\n",
                )

        if req.role == AgentRole.REVIEWER:
            batch_review_calls["count"] += 1
            if batch_review_calls["count"] == 1:
                assert "E-1" in req.prompt and "E-2" in req.prompt and "E-3" in req.prompt
                return ok_response(
                    "provider_reviewer_api",
                    "VERDICT #E-1: APPROVE\nLooks clean.\n\n"
                    "VERDICT #E-2: REJECT\nReturn 'fixed' instead of 'bad'.\n\n"
                    "VERDICT #E-3: APPROVE\nLooks clean.\n",
                )
            # Second batch review contains only retried E-2
            assert "E-2" in req.prompt
            return ok_response("provider_reviewer_api", "VERDICT #E-2: APPROVE\nFixed properly.\n")

        raise AssertionError(f"Unexpected role {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
    )
    status = runner.run()
    assert status == RunStatus.COMPLETED

    # PROGRESS.md must NOT have contained E-1/E-2 notes while they were still batch_pending!
    assert "E-1_PROGRESS_MARKER" not in progress_seen_before_first_batch["text"]
    assert "E-2_PROGRESS_bad" not in progress_seen_before_first_batch["text"]

    # After completion, all 3 approved tasks exist and PROGRESS.md has approved notes only
    final_progress = (temp_git_repo / ".pipeline/PROGRESS.md").read_text(encoding="utf-8")
    assert "E-1_PROGRESS_MARKER" in final_progress
    assert "E-3_PROGRESS_MARKER" in final_progress
    assert "E-2_PROGRESS_fixed" in final_progress
    assert "E-2_PROGRESS_bad" not in final_progress

    assert (temp_git_repo / "src/features/alpha/helper.py").exists()
    assert (temp_git_repo / "src/features/gamma/helper.py").exists()
    assert "fixed" in (temp_git_repo / "src/features/beta/helper.py").read_text(encoding="utf-8")
    assert all(t.status == TaskStatus.DONE for t in store.list_tasks())
    assert batch_review_calls["count"] == 2
    assert e2_attempts["count"] == 2


def test_easy_task_touching_yellow_zone_bypasses_batch_for_strict_master_review(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="E-Y1",
            title="Touch shared schema",
            description="Update shared schema version.",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=4)
    store = LocalJsonTaskStore(tasks_path)
    review_dynamic_bodies: list[str] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/shared/schema.py").write_text("SCHEMA_VERSION = 2\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nUpdated schema\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            review_dynamic_bodies.append(req.dynamic_body)
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nApproved shared edit.\n")
        raise AssertionError(f"Unexpected role {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED

    # Must have gone through individual Master review (NOT batch review prompt)
    assert len(review_dynamic_bodies) == 1
    assert "## BATCH REVIEW ITEMS" not in review_dynamic_bodies[0]
    events = [
        json.loads(ln)["event_type"]
        for ln in (temp_git_repo / ".pipeline/events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert EventType.YELLOW_ZONE_TRIGGERED.value in events
    assert EventType.BATCH_REVIEW_STARTED.value not in events
    t = store.get_task("E-Y1")
    assert t.status == TaskStatus.DONE
    assert t.touched_yellow_zone is True


def test_batch_flushes_before_dependent_or_overlapping_easy_tasks(temp_git_repo: Path) -> None:
    tasks = [
        Task(
            id="B-1",
            title="Alpha part 1",
            description="Create alpha_1",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="B-2",
            title="Alpha overlapping path",
            description="Edit same alpha path -> must NOT share batch with B-1",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="B-3",
            title="Dependent on B-2",
            description="Depends on B-2 -> cannot run until B-2 is approved",
            complexity=TaskComplexity.EASY,
            depends_on=["B-2"],
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=4)
    store = LocalJsonTaskStore(tasks_path)
    batches_seen: list[list[str]] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            target = {
                "B-1": "src/features/alpha/p1.py",
                "B-2": "src/features/alpha/p2.py",
                "B-3": "src/features/beta/p3.py",
            }[req.task_id or ""]
            (temp_git_repo / target).write_text("OK = True\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            ids = [tid for tid in (req.task_id or "").split(",") if tid]
            batches_seen.append(ids)
            verdicts = "\n".join(f"VERDICT #{tid}: APPROVE\nOK" for tid in ids)
            return ok_response("provider_reviewer_api", verdicts)
        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED

    # B-1 and B-2 share allowed_paths, B-3 depends on B-2 -> 3 separate batch flushes!
    assert batches_seen == [["B-1"], ["B-2"], ["B-3"]]
    assert all(t.status == TaskStatus.DONE for t in store.list_tasks())
