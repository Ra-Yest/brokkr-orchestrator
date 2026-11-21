"""Unit and integration tests for transient retries, backoff abort, PID lockfile, and safe crash recovery."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest import mock

import pytest

from orchestrator.control import enqueue_control_command
from orchestrator.fs_utils import atomic_write_json
from orchestrator.models import (
    AgentRole,
    ControlAction,
    EventType,
    PipelineState,
    RunStatus,
    Task,
    TaskComment,
    TaskComplexity,
    TaskStatus,
    utc_now_iso,
)
from orchestrator.process_utils import is_process_alive
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.runner import (
    OrchestratorFatalError,
    OrchestratorRunner,
    RunnerLockError,
    classify_provider_failure,
)
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import git, make_mock_registry, make_test_config, ok_response, write_tasks_file


def _read_events(repo: Path) -> list[dict[str, object]]:
    events_file = repo / ".pipeline/events.jsonl"
    if not events_file.exists():
        return []
    return [json.loads(ln) for ln in events_file.read_text(encoding="utf-8").splitlines() if ln.strip()]


def test_classify_provider_failure_permanent_vs_transient_vs_timeout() -> None:
    assert classify_provider_failure(ProviderResponse(provider_name="p", exit_code=0)) == "none"
    assert (
        classify_provider_failure(ProviderResponse(provider_name="p", exit_code=-1, timed_out=True))
        == "timeout"
    )
    for err in (
        "HTTP 502 Bad Gateway",
        "503 Service Unavailable",
        "Connection reset by peer (ECONNRESET)",
        "unexpected EOF reading response",
        "Broken pipe",
        "network error: ETIMEDOUT",
    ):
        assert (
            classify_provider_failure(
                ProviderResponse(provider_name="p", exit_code=1, raw_stderr=err)
            )
            == "transient"
        )

    for err in (
        "HTTP 401 Unauthorized: invalid api key",
        "403 Forbidden",
        "Authentication failed",
        "error: unknown option '--foo'",
        "unrecognized arguments: --bar",
        "codex: command not found",
        "FileNotFoundError: [Errno 2] No such file",
    ):
        assert (
            classify_provider_failure(
                ProviderResponse(provider_name="p", exit_code=1, raw_stderr=err)
            )
            == "permanent"
        )


def test_transient_retry_on_reviewer_and_batch_reviewer_succeeds_without_reject_or_revert(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="M-1",
            title="Medium task",
            description="Edit alpha",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="E-1",
            title="Batch easy 1",
            description="Add beta file",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
        Task(
            id="E-2",
            title="Batch easy 2",
            description="Add gamma file",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/gamma/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=2, transient_retry_delays_seconds=[10, 30, 90])
    store = LocalJsonTaskStore(tasks_path)

    worker_medium_calls = {"count": 0}
    single_reviewer_calls = {"count": 0}
    batch_reviewer_calls = {"count": 0}
    slept: list[float] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            worker_medium_calls["count"] += 1
            (temp_git_repo / "src/features/alpha/mod.py").write_text(
                "def alpha() -> int:\n    return 2\n",
                encoding="utf-8",
            )
            return ok_response("provider_main", "BEGIN_REPORT\nUpdated alpha\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.PRE_REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        if req.role == AgentRole.WORKER_EASY:
            target = (
                "src/features/beta/b1.py" if req.task_id == "E-1" else "src/features/gamma/g1.py"
            )
            (temp_git_repo / target).write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.REVIEWER:
            if req.task_id == "M-1":
                single_reviewer_calls["count"] += 1
                if single_reviewer_calls["count"] == 1:
                    return ProviderResponse(
                        provider_name="provider_reviewer_api",
                        exit_code=1,
                        raw_stderr="HTTP 502 Bad Gateway",
                    )
                return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nClean.\n")

            batch_reviewer_calls["count"] += 1
            if batch_reviewer_calls["count"] == 1:
                return ProviderResponse(
                    provider_name="provider_reviewer_api",
                    exit_code=1,
                    raw_stderr="Connection reset by peer (ECONNRESET)",
                )
            return ok_response(
                "provider_reviewer_api",
                "VERDICT #E-1: APPROVE\nOK\nVERDICT #E-2: APPROVE\nOK\n",
            )

        raise AssertionError(f"Unexpected call {req.role} {req.task_id}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=lambda s: slept.append(s),
    )
    assert runner.run() == RunStatus.COMPLETED
    assert worker_medium_calls["count"] == 1
    assert single_reviewer_calls["count"] == 2
    assert batch_reviewer_calls["count"] == 2
    assert all(t.status == TaskStatus.DONE for t in store.list_tasks())

    events = _read_events(temp_git_repo)
    event_types = [e["event_type"] for e in events]
    assert event_types.count(EventType.PROVIDER_TRANSIENT_RETRY.value) == 2
    assert EventType.GIT_REVERTED.value not in event_types
    assert sum(slept) == pytest.approx(20.0, abs=0.05)


def test_batch_reviewer_exhausting_retries_keeps_batch_pending_without_git_revert(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="E-1",
            title="Alpha easy",
            description="Add a.py",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="E-2",
            title="Beta easy",
            description="Add b.py",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=2, transient_retry_delays_seconds=[2, 4])
    store = LocalJsonTaskStore(tasks_path)
    reviewer_calls = {"count": 0}
    slept: list[float] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            target = (
                "src/features/alpha/a.py" if req.task_id == "E-1" else "src/features/beta/b.py"
            )
            (temp_git_repo / target).write_text("VAL = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            reviewer_calls["count"] += 1
            return ProviderResponse(
                provider_name="provider_reviewer_api",
                exit_code=1,
                raw_stderr="503 Service Unavailable",
            )
        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=lambda s: slept.append(s),
    )
    status = runner.run()
    assert status == RunStatus.PAUSED
    # 1 initial call + 2 configured transient retries
    assert reviewer_calls["count"] == 3
    assert sum(slept) == pytest.approx(6.0, abs=0.05)

    # Tasks remain BATCH_PENDING and their committed files were NOT reverted!
    assert store.get_task("E-1").status == TaskStatus.BATCH_PENDING
    assert store.get_task("E-2").status == TaskStatus.BATCH_PENDING
    assert (temp_git_repo / "src/features/alpha/a.py").exists()
    assert (temp_git_repo / "src/features/beta/b.py").exists()

    event_types = [e["event_type"] for e in _read_events(temp_git_repo)]
    assert EventType.BATCH_REVIEW_DEFERRED.value in event_types
    assert EventType.GIT_REVERTED.value not in event_types
    assert event_types.count(EventType.PROVIDER_TRANSIENT_RETRY.value) == 2


def test_permanent_errors_fail_fast_without_retries(temp_git_repo: Path) -> None:
    tasks = [
        Task(
            id="P-1",
            title="Auth fail test",
            description="Edit alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(
        batch_review_size=1,
        transient_retry_delays_seconds=[10, 30, 90],
        max_review_rounds=1,
        hard_split_after_rounds=1,
        max_gate_retries=1,
    )
    store = LocalJsonTaskStore(tasks_path)
    reviewer_calls = {"count": 0}
    slept: list[float] = []

    # 1. Permanent 401 Unauthorized on reviewer -> zero retries, zero sleep, immediate PAUSED
    def handler_401(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/features/alpha/ok.py").write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            reviewer_calls["count"] += 1
            return ProviderResponse(
                provider_name="provider_reviewer_api",
                exit_code=1,
                raw_stderr="HTTP 401 Unauthorized: invalid api key",
            )
        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler_401)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=lambda s: slept.append(s),
    )
    assert runner.run() == RunStatus.PAUSED
    assert reviewer_calls["count"] == 1
    assert slept == []
    event_types = [e["event_type"] for e in _read_events(temp_git_repo)]
    assert EventType.PROVIDER_TRANSIENT_RETRY.value not in event_types

    # 2. Permanent invalid CLI flag on worker -> zero transient retries
    git(temp_git_repo, "reset", "--hard", "HEAD")
    git(temp_git_repo, "clean", "-fd", "-e", ".pipeline/")
    store.retry_task("P-1")
    worker_calls = {"count": 0}

    def handler_bad_flag(req: ProviderRequest) -> ProviderResponse:
        worker_calls["count"] += 1
        return ProviderResponse(
            provider_name="provider_fast",
            exit_code=2,
            raw_stderr="error: unrecognized arguments: --bogus-flag",
        )

    reg2, _ = make_mock_registry(cfg, handler_bad_flag)
    runner2 = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg2,
        sleep_fn=lambda s: slept.append(s),
    )
    assert runner2.run() == RunStatus.COMPLETED
    assert worker_calls["count"] == 1
    assert slept == []
    assert store.get_task("P-1").status == TaskStatus.FAILED


def test_abort_command_interrupts_transient_backoff_immediately(temp_git_repo: Path) -> None:
    tasks = [
        Task(
            id="A-1",
            title="Abort during backoff",
            description="Edit alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1, transient_retry_delays_seconds=[90])
    store = LocalJsonTaskStore(tasks_path)
    pipeline_dir = temp_git_repo / ".pipeline"
    reviewer_calls = {"count": 0}
    sleep_ticks: list[float] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/features/alpha/a1.py").write_text("A = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            reviewer_calls["count"] += 1
            return ProviderResponse(
                provider_name="provider_reviewer_api",
                exit_code=1,
                raw_stderr="502 Bad Gateway",
            )
        raise AssertionError(f"Unexpected {req.role}")

    def sleep_and_abort(sec: float) -> None:
        sleep_ticks.append(sec)
        if len(sleep_ticks) == 1:
            enqueue_control_command(pipeline_dir, action=ControlAction.ABORT, reason="Stop now")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=sleep_and_abort,
    )
    assert runner.run() == RunStatus.ABORTED
    # Interrupted after just 1 tick — did NOT wait all 90 seconds or invoke reviewer a second time
    assert len(sleep_ticks) == 1
    assert reviewer_calls["count"] == 1


def test_runner_lock_blocks_live_pid_and_recovers_stale_pid(temp_git_repo: Path) -> None:
    assert is_process_alive(os.getpid()) is True
    assert is_process_alive(-1) is False

    tasks_path = write_tasks_file(temp_git_repo, [])
    cfg = make_test_config()
    store = LocalJsonTaskStore(tasks_path)
    pipeline_dir = temp_git_repo / ".pipeline"
    lock_path = pipeline_dir / "runner.lock"

    # 1. Live PID in runner.lock -> RunnerLockError; existing lock file is preserved untouched
    live_lock = {"pid": os.getpid(), "run_id": "run_live_owner", "started_at": utc_now_iso()}
    atomic_write_json(lock_path, live_lock)

    registry, _ = make_mock_registry(cfg, lambda req: ok_response("m", "STATUS: DONE\n"))
    runner_blocked = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
    )
    with pytest.raises(RunnerLockError, match="already running"):
        runner_blocked.run()
    # Original owner's lock must still exist!
    assert json.loads(lock_path.read_text(encoding="utf-8"))["run_id"] == "run_live_owner"

    # 2. Stale (dead) PID in runner.lock -> recovered, event emitted, lock cleaned up on finish
    stale_lock = {"pid": 99999991, "run_id": "run_dead_prev", "started_at": utc_now_iso()}
    atomic_write_json(lock_path, stale_lock)

    with mock.patch("orchestrator.runner.is_process_alive", return_value=False):
        runner_ok = OrchestratorRunner(
            repo_root=temp_git_repo,
            config=cfg,
            store=store,
            registry=registry,
        )
        assert runner_ok.run() == RunStatus.COMPLETED

    assert not lock_path.exists()
    events = _read_events(temp_git_repo)
    stale_events = [e for e in events if e["event_type"] == EventType.STALE_LOCK_RECOVERED.value]
    assert len(stale_events) == 1
    assert stale_events[0]["payload"] == {
        "stale_pid": 99999991,
        "stale_run_id": "run_dead_prev",
        "stale_started_at": stale_lock["started_at"],
    }


def test_crash_recovery_rolls_back_dirty_in_progress_and_preserves_arch_pending(
    temp_git_repo: Path,
) -> None:
    initial_sha = git(temp_git_repo, "rev-parse", "HEAD")

    tasks = [
        Task(
            id="T-1",
            title="Interrupted worker task",
            description="Update alpha",
            complexity=TaskComplexity.EASY,
            status=TaskStatus.IN_PROGRESS,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="T-2",
            title="Interrupted architect task",
            description="Needs shared schema update",
            complexity=TaskComplexity.EASY,
            status=TaskStatus.ARCH_PENDING,
            allowed_paths=["src/features/beta/**"],
            comments=[
                TaskComment(
                    author="worker",
                    kind="escalation",
                    body="Add ARCH_CONST = 1 to src/shared/schema.py",
                )
            ],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    pipeline_dir = temp_git_repo / ".pipeline"

    # Simulate crashed prior run state.json + dirty uncommitted edits + untracked junk
    crashed_state = PipelineState(
        run_id="run_crashed_1",
        status=RunStatus.RUNNING,
        active_task_id="T-1",
        active_task_base_sha=initial_sha,
        active_phase="provider:worker_easy",
    )
    atomic_write_json(pipeline_dir / "state.json", crashed_state.model_dump(mode="json"))
    (temp_git_repo / "src/features/alpha/mod.py").write_text("BROKEN HALF EDIT\n", encoding="utf-8")
    (temp_git_repo / "src/features/alpha/crash_junk.py").write_text("JUNK = 1\n", encoding="utf-8")

    cfg = make_test_config(batch_review_size=1)
    store = LocalJsonTaskStore(tasks_path)
    seen_roles: list[tuple[AgentRole, str | None]] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        seen_roles.append((req.role, req.task_id))
        # Crash junk must already have been rolled back before any provider runs!
        assert not (temp_git_repo / "src/features/alpha/crash_junk.py").exists()
        if req.role == AgentRole.ARCHITECT and req.task_id == "T-2":
            (temp_git_repo / "src/shared/schema.py").write_text(
                "SCHEMA_VERSION = 2\nARCH_CONST = 1\n",
                encoding="utf-8",
            )
            return ok_response(
                "provider_master",
                "BEGIN_REPORT\nAdded ARCH_CONST\nEND_REPORT\nSTATUS: DONE\n",
            )
        if req.role == AgentRole.WORKER_EASY:
            target = (
                "src/features/alpha/mod.py" if req.task_id == "T-1" else "src/features/beta/b.py"
            )
            (temp_git_repo / target).write_text("CLEAN = True\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nClean\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role} {req.task_id}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)
    assert runner.run() == RunStatus.COMPLETED

    assert store.get_task("T-1").status == TaskStatus.DONE
    assert store.get_task("T-2").status == TaskStatus.DONE
    # Architect resumed T-2 cleanly right after crash recovery
    assert (AgentRole.ARCHITECT, "T-2") in seen_roles

    events = _read_events(temp_git_repo)
    recovery_events = [
        e for e in events if e["event_type"] == EventType.CRASH_RECOVERY_COMPLETED.value
    ]
    assert len(recovery_events) == 1
    payload = recovery_events[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["rollback_sha"] == initial_sha
    assert payload["rolled_back_dirty_tree"] is True
    assert payload["recovered_task_ids"] == ["T-1"]
    assert payload["arch_pending_task_ids"] == ["T-2"]


def test_dirty_working_tree_without_prior_crash_refuses_to_reset_manual_edits(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="T-1",
            title="Normal task",
            description="Edit alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    pipeline_dir = temp_git_repo / ".pipeline"

    # Prior run finished cleanly (no crash)
    clean_state = PipelineState(run_id="run_clean_prev", status=RunStatus.COMPLETED)
    atomic_write_json(pipeline_dir / "state.json", clean_state.model_dump(mode="json"))

    # User made manual local edits
    manual_edit_file = temp_git_repo / "src/features/alpha/mod.py"
    manual_new_file = temp_git_repo / "src/features/alpha/manual_work.py"
    manual_edit_file.write_text("# user manual WIP edit\n", encoding="utf-8")
    manual_new_file.write_text("# user new file\n", encoding="utf-8")

    cfg = make_test_config()
    store = LocalJsonTaskStore(tasks_path)
    registry, _ = make_mock_registry(cfg, lambda req: ok_response("m", "STATUS: DONE\n"))
    runner = OrchestratorRunner(repo_root=temp_git_repo, config=cfg, store=store, registry=registry)

    with pytest.raises(OrchestratorFatalError, match="Refusing to reset manual changes"):
        runner.run()

    # Manual user work MUST remain completely intact and runner.lock released!
    assert manual_edit_file.read_text(encoding="utf-8") == "# user manual WIP edit\n"
    assert manual_new_file.read_text(encoding="utf-8") == "# user new file\n"
    assert not (pipeline_dir / "runner.lock").exists()


def test_timeout_retry_policy_readonly_retries_once_and_writing_soft_resets_without_retry(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="TO-1",
            title="Timeout policy check",
            description="Edit alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(
        batch_review_size=1,
        transient_retry_delays_seconds=[5, 10],
        retry_on_timeout_readonly=1,
    )
    store = LocalJsonTaskStore(tasks_path)

    worker_calls = {"count": 0}
    reviewer_calls = {"count": 0}
    slept: list[float] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            worker_calls["count"] += 1
            if worker_calls["count"] == 1:
                # Worker makes a rogue commit right before timing out
                (temp_git_repo / "src/features/alpha/mod.py").write_text(
                    "def alpha() -> int:\n    return 7\n",
                    encoding="utf-8",
                )
                git(temp_git_repo, "add", "-A")
                git(temp_git_repo, "commit", "-m", "rogue commit before timeout")
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=-1,
                    timed_out=True,
                )
            return ok_response("provider_fast", "BEGIN_REPORT\nFinished\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.REVIEWER:
            reviewer_calls["count"] += 1
            if reviewer_calls["count"] == 1:
                return ProviderResponse(
                    provider_name="provider_reviewer_api",
                    exit_code=-1,
                    timed_out=True,
                )
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=lambda s: slept.append(s),
    )
    assert runner.run() == RunStatus.COMPLETED
    # Worker timed out on call 1 -> soft-reset rogue commit -> standard retry loop called worker call 2
    assert worker_calls["count"] == 2
    # Reviewer timed out on call 1 -> 1 readonly timeout retry -> approved on call 2
    assert reviewer_calls["count"] == 2
    assert sum(slept) == pytest.approx(5.0, abs=0.05)

    events = _read_events(temp_git_repo)
    retry_events = [
        e for e in events if e["event_type"] == EventType.PROVIDER_TRANSIENT_RETRY.value
    ]
    # Only the read-only reviewer timed out and retried inside _invoke_provider_guarded (not the writing worker!)
    assert len(retry_events) == 1
    assert retry_events[0]["role"] == AgentRole.REVIEWER.value
    assert any(e["event_type"] == EventType.AGENT_COMMIT_SOFT_RESET.value for e in events)
