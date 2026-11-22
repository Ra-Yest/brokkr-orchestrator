"""Comprehensive unit and integration tests for AFK Orchestrator v0.2 features."""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import yaml
from pydantic import ValidationError

from orchestrator.cli import run_doctor_checks
from orchestrator.config import (
    DiffGuardConfig,
    DiffLimitConfig,
    GateConfig,
    LogProbeConfig,
    NotificationsConfig,
    PipelineConfig,
    ProviderProfileConfig,
    ProxyMonitorConfig,
    RoleMappingConfig,
    StatusJsonProbeConfig,
    TelegramNotificationConfig,
    load_pipeline_config,
)
from orchestrator.control import enqueue_control_command
from orchestrator.diff_guard import evaluate_diff, scan_placeholders
from orchestrator.git_ops import GitChange
from orchestrator.models import (
    AgentRole,
    ControlAction,
    EventType,
    RunStatus,
    Task,
    TaskComplexity,
    TaskStatus,
)
from orchestrator.process_utils import is_meaningful_output, run_noninteractive_process
from orchestrator.providers.base import ProviderRegistry, ProviderRequest, ProviderResponse
from orchestrator.providers.cli_provider import CliAgentProvider
from orchestrator.proxy_monitor import GenericProxyMonitor
from orchestrator.runner import OrchestratorFatalError, OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import (
    ScriptedMockProvider,
    git,
    make_mock_registry,
    make_test_config,
    ok_response,
    write_tasks_file,
)


def _make_diff(path: str, added_lines: list[str], removed_lines: list[str] | None = None) -> str:
    body = [f"-{ln}" for ln in (removed_lines or [])] + [f"+{ln}" for ln in added_lines]
    return (
        f"diff --git a/{path} b/{path}\n"
        f"--- a/{path}\n"
        f"+++ b/{path}\n"
        f"@@ -1,{len(removed_lines or [])} +1,{len(added_lines)} @@\n"
        + "\n".join(body)
        + "\n"
    )


def _read_events(repo: Path) -> list[dict[str, Any]]:
    events_path = repo / ".pipeline" / "events.jsonl"
    if not events_path.exists():
        return []
    return [
        json.loads(ln)
        for ln in events_path.read_text(encoding="utf-8").splitlines()
        if ln.strip()
    ]


class _FakeHttpResponse:
    def __init__(self, status: int = 200) -> None:
        self.status = status

    def __enter__(self) -> _FakeHttpResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _LocalTcpServer:
    """Ephemeral localhost TCP server for mocking local proxy port reachability."""

    def __init__(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(5)
        self._sock.settimeout(0.2)
        self.port: int = int(self._sock.getsockname()[1])
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
                conn.close()
            except TimeoutError:
                continue
            except OSError:
                break

    def __enter__(self) -> _LocalTcpServer:
        self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        self._stop.set()
        self._sock.close()
        self._thread.join(timeout=2.0)


# ===========================================================================
# 1. Diff Guard: test exclusion, hard_max_test_lines, soft/hard, compat, stub
# ===========================================================================


def test_diff_guard_excludes_tests_from_prod_limits_and_enforces_hard_max_test_lines() -> None:
    cfg = make_test_config().model_copy(
        update={
            "diff_guard": DiffGuardConfig(
                exclude_tests_from_limits=True,
                hard_max_test_lines=100,
                limits={
                    TaskComplexity.EASY: DiffLimitConfig(
                        soft_max_files=2,
                        soft_max_lines=20,
                        hard_max_files=3,
                        hard_max_lines=40,
                    ),
                    TaskComplexity.MEDIUM: DiffLimitConfig(max_files=12, max_lines=600),
                    TaskComplexity.HARD: DiffLimitConfig(max_files=25, max_lines=1500),
                },
            )
        }
    )

    # 1 prod file (10 lines) + 4 new test files (80 lines total) -> passes EASY prod limits (hard=3 files, 40 lines)!
    changes = [
        GitChange(status="A", path="src/features/alpha/core.py"),
        GitChange(status="A", path="tests/features/alpha/test_a.py"),
        GitChange(status="A", path="tests/features/alpha/test_b.py"),
        GitChange(status="A", path="tests/features/alpha/test_c.py"),
        GitChange(status="A", path="tests/features/alpha/test_d.py"),
    ]
    raw_diff = (
        _make_diff("src/features/alpha/core.py", [f"val_{i} = {i}" for i in range(10)])
        + _make_diff("tests/features/alpha/test_a.py", [f"assert {i} == {i}" for i in range(20)])
        + _make_diff("tests/features/alpha/test_b.py", [f"assert {i} == {i}" for i in range(20)])
        + _make_diff("tests/features/alpha/test_c.py", [f"assert {i} == {i}" for i in range(20)])
        + _make_diff("tests/features/alpha/test_d.py", [f"assert {i} == {i}" for i in range(20)])
    )
    rep = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=changes,
        raw_diff=raw_diff,
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**", "tests/features/alpha/**"],
    )
    assert rep.passed is True
    assert rep.has_soft_warnings is False
    assert rep.prod_files_count == 1
    assert rep.prod_changed_lines == 10
    assert rep.test_files_count == 4
    assert rep.test_changed_lines == 80

    # PlaceholderGuard and Red Zone M/D still apply to test files even when excluded from prod limits
    bad_test_diff = _make_diff("tests/features/alpha/test_todo.py", ["# TODO: implement assertion"])
    rep_todo = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="A", path="tests/features/alpha/test_todo.py")],
        raw_diff=bad_test_diff,
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["tests/features/alpha/**"],
    )
    assert rep_todo.passed is False
    assert any(v.code == "PLACEHOLDER_TODO_MARKER" for v in rep_todo.placeholder_violations)

    # Exceeding hard_max_test_lines (100) triggers hard violation DIFF_GUARD_MAX_TEST_LINES
    huge_test_diff = _make_diff(
        "tests/features/alpha/test_huge.py",
        [f"def test_{i}(): assert {i} >= 0" for i in range(120)],
    )
    rep_huge = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="A", path="tests/features/alpha/test_huge.py")],
        raw_diff=huge_test_diff,
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["tests/features/alpha/**"],
    )
    assert rep_huge.passed is False
    assert any(v.code == "DIFF_GUARD_MAX_TEST_LINES" for v in rep_huge.limit_violations)


def test_diff_guard_soft_warning_disables_batch_and_injects_reviewer_note_vs_hard_rollback(
    temp_git_repo: Path,
) -> None:
    cfg = make_test_config(batch_review_size=3, max_review_rounds=1, hard_split_after_rounds=1, max_gate_retries=1).model_copy(
        update={
            "diff_guard": DiffGuardConfig(
                exclude_tests_from_limits=True,
                limits={
                    TaskComplexity.EASY: DiffLimitConfig(
                        soft_max_files=1,
                        soft_max_lines=5,
                        hard_max_files=3,
                        hard_max_lines=20,
                    ),
                    TaskComplexity.MEDIUM: DiffLimitConfig(max_files=12, max_lines=600),
                    TaskComplexity.HARD: DiffLimitConfig(max_files=25, max_lines=1500),
                },
            )
        }
    )

    tasks = [
        Task(
            id="SOFT-1",
            title="Soft limit easy task",
            description="Write 10 lines (> soft 5, <= hard 20)",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="HARD-1",
            title="Hard limit easy task",
            description="Write 30 lines (> hard 20)",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    store = LocalJsonTaskStore(tasks_path)
    reviewer_prompts: list[str] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY and req.task_id == "SOFT-1":
            lines = "\n".join(f"X_{i} = {i}" for i in range(10)) + "\n"
            (temp_git_repo / "src/features/alpha/soft.py").write_text(lines, encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nSoft done\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.WORKER_EASY and req.task_id == "HARD-1":
            lines = "\n".join(f"Y_{i} = {i}" for i in range(30)) + "\n"
            (temp_git_repo / "src/features/beta/hard.py").write_text(lines, encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nHard done\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            reviewer_prompts.append(req.prompt)
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nReviewed carefully.\n")
        raise AssertionError(f"Unexpected {req.role} {req.task_id}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED

    # SOFT-1 was NOT rolled back, was NOT put in batch_pending, went straight to individual reviewer with warning!
    assert store.get_task("SOFT-1").status == TaskStatus.DONE
    assert len(reviewer_prompts) == 1
    assert "DIFF_GUARD_SOFT_WARNING" in reviewer_prompts[0]
    assert "prod_lines=10" in reviewer_prompts[0]

    # HARD-1 hit hard limit, emitted diff_guard_failed, rolled back, and failed
    assert store.get_task("HARD-1").status == TaskStatus.FAILED
    assert not (temp_git_repo / "src/features/beta/hard.py").exists()

    events = _read_events(temp_git_repo)
    assert any(
        e["event_type"] == EventType.DIFF_GUARD_SOFT_WARNING.value and e["task_id"] == "SOFT-1"
        for e in events
    )
    assert any(
        e["event_type"] == EventType.DIFF_GUARD_FAILED.value and e["task_id"] == "HARD-1"
        for e in events
    )
    assert not any(e["event_type"] == EventType.BATCH_REVIEW_STARTED.value for e in events)


def test_legacy_diff_guard_max_files_max_lines_yaml_backward_compat(tmp_path: Path) -> None:
    legacy_yaml = {
        "schema_version": 1,
        "providers": {
            "w": {"kind": "cli", "command": ["echo"]},
            "r": {"kind": "api", "base_url": "https://api.example.com/v1", "model": "m"},
        },
        "roles": {
            "planner": "r",
            "worker_easy": "w",
            "worker_medium": "w",
            "worker_hard": "w",
            "pre_reviewer": "r",
            "reviewer": "r",
            "architect": "w",
            "refactorer": "w",
        },
        "diff_guard": {
            "limits": {
                "easy": {"max_files": 5, "max_lines": 200},
                "medium": {"max_files": 12, "max_lines": 600},
                "hard": {"max_files": 25, "max_lines": 1500},
            }
        },
    }
    cfg_path = tmp_path / "pipeline.yaml"
    cfg_path.write_text(yaml.safe_dump(legacy_yaml), encoding="utf-8")
    cfg = load_pipeline_config(cfg_path)
    easy = cfg.diff_guard.limits[TaskComplexity.EASY]
    assert easy.hard_max_files == 5
    assert easy.hard_max_lines == 200
    assert easy.soft_max_files is None
    assert easy.soft_max_lines is None

    # soft > hard must fail validation
    with pytest.raises(ValidationError, match="soft_max_lines cannot exceed hard_max_lines"):
        DiffLimitConfig(soft_max_files=2, soft_max_lines=500, hard_max_files=5, hard_max_lines=200)


def test_placeholder_guard_stub_trailing_spaces_caught() -> None:
    for bad in ("# stub ", "// stub   ", "# empty stub  ", "// stub\t "):
        v = scan_placeholders(_make_diff("src/a.py", [bad]))
        assert any(item.code == "PLACEHOLDER_EMPTY_STUB_COMMENT" for item in v), f"Missed {bad!r}"


# ===========================================================================
# 2. Control Plane step-level pause, abort rollback, and doctor relative gate
# ===========================================================================


def test_pause_after_current_stops_immediately_after_step_before_next_retry(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="PAUSE-1",
            title="Pause mid-task",
            description="Fails first attempt, operator pauses before retry",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1, max_gate_retries=3, max_transport_retries=0)
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"

    worker_calls = {"count": 0}
    paused_seen = {"flag": False}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            worker_calls["count"] += 1
            if worker_calls["count"] == 1:
                # Worker fails with placeholder (would trigger retry #2), while operator pauses
                (temp_git_repo / "src/features/alpha/wip.py").write_text(
                    "# TODO: finish\n", encoding="utf-8"
                )
                enqueue_control_command(
                    pdir,
                    action=ControlAction.PAUSE_AFTER_CURRENT,
                    reason="Stop before retry",
                )
                return ok_response("provider_fast", "STATUS: DONE\n")
            raise AssertionError("Second provider call must NOT start while paused before abort!")
        raise AssertionError(f"Unexpected {req.role}")

    def sleep_inspect_then_abort(_sec: float) -> None:
        # Verify runner is PAUSED after just 1 provider call (never started retry #2)
        state_data = json.loads((pdir / "state.json").read_text(encoding="utf-8"))
        assert state_data["status"] == RunStatus.PAUSED.value
        assert state_data["paused"] is True
        assert worker_calls["count"] == 1
        paused_seen["flag"] = True
        enqueue_control_command(pdir, action=ControlAction.ABORT, reason="Stop cleanly")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=sleep_inspect_then_abort,
        quiet=True,
    )
    assert runner.run() == RunStatus.ABORTED
    assert paused_seen["flag"] is True
    assert worker_calls["count"] == 1
    # Abort cleaned up dirty tree and returned task to TODO while preserving .pipeline/
    assert store.get_task("PAUSE-1").status == TaskStatus.TODO
    assert not (temp_git_repo / "src/features/alpha/wip.py").exists()
    assert (pdir / "state.json").exists()
    assert git(temp_git_repo, "status", "--porcelain") == ""


def test_doctor_resolves_relative_gate_binary_inside_repo(temp_git_repo: Path) -> None:
    rel_script = temp_git_repo / "scripts" / "check_tool.cmd"
    rel_script.parent.mkdir(parents=True, exist_ok=True)
    rel_script.write_text("@echo off\r\necho ok\r\n", encoding="utf-8")
    root_script = temp_git_repo / "local_gate.bat"
    root_script.write_text("@echo off\r\necho ok\r\n", encoding="utf-8")

    cfg = make_test_config(
        gates=[
            GateConfig(name="rel_nested", command=["scripts/check_tool.cmd"]),
            GateConfig(name="rel_root", command=["local_gate.bat"]),
        ]
    )
    cfg_path = temp_git_repo / ".pipeline" / "pipeline.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(yaml.safe_dump(cfg.model_dump(mode="json")), encoding="utf-8")
    write_tasks_file(temp_git_repo, [])

    checks = dict((name, (ok, detail)) for name, ok, detail in run_doctor_checks(temp_git_repo, cfg_path))
    assert checks["gate:rel_nested"][0] is True
    assert checks["gate:rel_root"][0] is True


# ===========================================================================
# 3. Free transport_retries vs attempts/QuotaTracker & long-call guard
# ===========================================================================


def test_transport_retries_do_not_consume_attempts_or_quota_and_long_worker_skips_blind_retry(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="TR-1",
            title="Transport retry recovery",
            description="Fails twice with connection error before succeeding",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="TR-2",
            title="Long writing failure",
            description="Runs >120s then fails; must NOT blindly transport/transient retry",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(
        batch_review_size=1,
        max_transport_retries=6,
        transport_retry_delays_seconds=[1, 2, 3],
        transport_failure_max_duration_seconds=120,
        max_gate_retries=2,
        max_review_rounds=1,
        hard_split_after_rounds=1,
    )
    store = LocalJsonTaskStore(tasks_path)
    tr1_calls = {"count": 0}
    tr2_calls = {"count": 0}
    slept: list[float] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY and req.task_id == "TR-1":
            tr1_calls["count"] += 1
            if tr1_calls["count"] <= 2:
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=1,
                    raw_stderr="connection reset by peer (ECONNRESET)",
                    duration_seconds=2.5,
                )
            (temp_git_repo / "src/features/alpha/tr1.py").write_text("OK = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.WORKER_EASY and req.task_id == "TR-2":
            tr2_calls["count"] += 1
            if tr2_calls["count"] == 1:
                # Ran 180s (>= 120s threshold) and crashed with ECONNRESET -> must NOT transport or transient retry!
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=1,
                    raw_stderr="ECONNRESET after long run",
                    duration_seconds=180.0,
                )
            (temp_git_repo / "src/features/beta/tr2.py").write_text("OK = 2\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone 2\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        raise AssertionError(f"Unexpected {req.role} {req.task_id}")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=lambda s: slept.append(s),
        jitter_fn=lambda d: d,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED

    # TR-1: 2 free transport retries + 1 success => task.attempts == 1 (NOT 3!), review_rounds == 0
    t1 = store.get_task("TR-1")
    assert t1.status == TaskStatus.DONE
    assert t1.attempts == 1
    assert t1.review_rounds == 0

    # TR-2: long failure went straight up to normal attempt loop (no transport_retry, no transient_retry)
    t2 = store.get_task("TR-2")
    assert t2.status == TaskStatus.DONE
    assert t2.attempts == 2
    assert tr2_calls["count"] == 2

    # QuotaTracker for provider_fast recorded ONLY 3 real calls (1 for TR-1 + 2 for TR-2), NOT 5!
    state_data = json.loads((temp_git_repo / ".pipeline/state.json").read_text(encoding="utf-8"))
    assert state_data["provider_quotas"]["provider_fast"]["requests_in_window"] == 3

    events = _read_events(temp_git_repo)
    tr_events = [e for e in events if e["event_type"] == EventType.PROVIDER_TRANSPORT_RETRY.value]
    assert len(tr_events) == 2
    assert all(e["task_id"] == "TR-1" for e in tr_events)
    assert not any(e["event_type"] == EventType.PROVIDER_TRANSIENT_RETRY.value for e in events)
    assert any(
        e["event_type"] == EventType.PROVIDER_RECOVERED_AFTER_RETRY.value and e["task_id"] == "TR-1"
        for e in events
    )


# ===========================================================================
# 4. Streaming stdout, recovery Telegram notification, and idle_timeout_seconds
# ===========================================================================


def test_streaming_stdout_emits_started_recovered_telegram_and_idle_timeout_kills_hung_process(
    temp_git_repo: Path,
) -> None:
    assert is_meaningful_output("\x1b[32m   \r\n\x1b[0m") is False
    assert is_meaningful_output("\x1b[32mHello\x1b[0m") is True

    # A. Real subprocess streaming: leading ANSI noise ignored, first meaningful chunk fires callback
    stream_script = temp_git_repo / "stream_agent.py"
    stream_script.write_text(
        "import sys, time\n"
        "sys.stdout.write('\\x1b[33m   \\n')\n"
        "sys.stdout.flush()\n"
        "time.sleep(0.05)\n"
        "sys.stdout.write('BEGIN_REPORT\\nПривет мир\\nEND_REPORT\\nSTATUS: DONE\\n')\n"  # noqa: RUF001
        "sys.stdout.flush()\n",
        encoding="utf-8",
    )
    first_chunks: list[str] = []
    res_stream = run_noninteractive_process(
        [sys.executable, str(stream_script)],
        cwd=temp_git_repo,
        timeout_seconds=15,
        on_first_output=lambda chunk: first_chunks.append(chunk),
    )
    assert res_stream.exit_code == 0
    assert res_stream.received_meaningful_output is True
    assert len(first_chunks) == 1
    assert "BEGIN_REPORT" in first_chunks[0]
    assert "Привет мир" in res_stream.stdout

    # B. Idle timeout kills a hung process that produces no output and edits no files
    hang_script = temp_git_repo / "idle_hang.py"
    hang_script.write_text("import time\ntime.sleep(30)\n", encoding="utf-8")
    idle_provider = CliAgentProvider(
        "p_idle",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(hang_script)],
            prompt_mode="stdin",
            timeout_seconds=30,
            idle_timeout_seconds=1,
        ),
    )
    res_idle = idle_provider.invoke(
        ProviderRequest(
            run_id="r1",
            task_id="T-IDLE",
            role=AgentRole.WORKER_EASY,
            prompt="hi",
            workdir=temp_git_repo,
            timeout_seconds=30,
            attempt_dir=temp_git_repo,
        )
    )
    assert res_idle.idle_timed_out is True
    assert res_idle.timed_out is True
    assert res_idle.duration_seconds < 10.0

    # C. Process with no stdout that actively touches workspace files stays alive past idle_timeout
    touch_script = temp_git_repo / "silent_editor.py"
    touch_script.write_text(
        "import pathlib, time\n"
        "p = pathlib.Path('src/features/alpha/touched.py')\n"
        "for i in range(4):\n"
        "    p.write_text(f'N = {i}\\n', encoding='utf-8')\n"
        "    time.sleep(0.35)\n"
        "print('STATUS: DONE')\n",
        encoding="utf-8",
    )
    res_alive = run_noninteractive_process(
        [sys.executable, str(touch_script)],
        cwd=temp_git_repo,
        timeout_seconds=15,
        idle_timeout_seconds=0.8,
        workspace_watch_dir=temp_git_repo,
    )
    assert res_alive.exit_code == 0
    assert res_alive.idle_timed_out is False
    # Clean up helper scripts before runner test
    for helper in (stream_script, hang_script, touch_script, temp_git_repo / "src/features/alpha/touched.py"):
        if helper.exists():
            helper.unlink()

    # D. Runner integration: recovery after transport retry emits event & mandatory Telegram message
    tg_cfg = TelegramNotificationConfig(
        enabled=True,
        bot_token_env="TG_V02_TOKEN",
        chat_id_env="TG_V02_CHAT",
        notify_on_provider_started=False,
        events=["run_started"],  # Note: provider_recovered still fires unconditionally when TG enabled!
    )
    cfg = make_test_config(
        batch_review_size=1,
        max_transport_retries=4,
        transport_retry_delays_seconds=[1],
    ).model_copy(update={"notifications": NotificationsConfig(telegram=tg_cfg)})
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="T-04b",
                title="Stream recovery task",
                description="Write alpha file",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    sent_tg: list[str] = []
    seen_streaming_phase = {"value": None}
    calls = {"worker": 0}

    def fake_urlopen(req: Any, timeout: float = 10) -> _FakeHttpResponse:
        body = json.loads(req.data.decode("utf-8"))
        sent_tg.append(str(body["text"]))
        return _FakeHttpResponse(200)

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            calls["worker"] += 1
            if calls["worker"] <= 2:
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=1,
                    raw_stderr="proxy ECONNREFUSED",
                    duration_seconds=0.5,
                )
            if req.on_first_output is not None:
                req.on_first_output("Working on T-04b...")
                st = json.loads((temp_git_repo / ".pipeline/state.json").read_text(encoding="utf-8"))
                seen_streaming_phase["value"] = st["active_phase"]
            (temp_git_repo / "src/features/alpha/rec.py").write_text("OK = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    with (
        mock.patch.dict(os.environ, {"TG_V02_TOKEN": "tok", "TG_V02_CHAT": "42"}),
        mock.patch("orchestrator.notifications.urllib.request.urlopen", side_effect=fake_urlopen),
    ):
        registry, _ = make_mock_registry(cfg, handler)
        runner = OrchestratorRunner(
            repo_root=temp_git_repo,
            config=cfg,
            store=store,
            registry=registry,
            sleep_fn=lambda _s: None,
            jitter_fn=lambda d: d,
            quiet=True,
        )
        assert runner.run() == RunStatus.COMPLETED

    assert seen_streaming_phase["value"] == "streaming:worker_easy"
    events = _read_events(temp_git_repo)
    assert any(e["event_type"] == EventType.PROVIDER_RESPONSE_STARTED.value for e in events)
    assert any(
        e["event_type"] == EventType.PROVIDER_RECOVERED_AFTER_RETRY.value
        and e["payload"].get("transport_retries") == 2
        for e in events
    )
    assert any(
        "[provider_fast] восстановился после 2 ретраев, начал отвечать по T-04b" in msg
        for msg in sent_tg
    )
    # Normal reviewer start did NOT send Telegram because notify_on_provider_started=False
    assert not any("[Provider Started" in msg for msg in sent_tg)


# ===========================================================================
# 5. Fallback providers on 429 & Hibernation with wake-up / abort
# ===========================================================================


def test_fallback_provider_used_on_429_and_hibernation_wakes_or_aborts(
    temp_git_repo: Path,
) -> None:
    cfg = make_test_config(batch_review_size=1).model_copy(
        update={
            "fallback_providers": {
                AgentRole.WORKER_EASY: ["provider_main"],
            }
        }
    )
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="FB-1",
                title="Fallback on 429",
                description="Primary 429 -> fallback succeeds immediately",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    called_providers: list[str] = []
    slept: list[float] = []

    def handler_fb(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            # First call hits provider_fast -> 429; second call must hit provider_main!
            if not called_providers:
                called_providers.append("provider_fast")
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=429,
                    raw_stderr="429 Too Many Requests",
                    rate_limited=True,
                    retry_after_seconds=600,
                )
            called_providers.append("provider_main")
            (temp_git_repo / "src/features/alpha/fb.py").write_text("FB = 1\n", encoding="utf-8")
            return ok_response("provider_main", "BEGIN_REPORT\nFallback ok\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    # Wire distinct handlers per provider so we verify exact provider name invoked
    mocks: dict[str, ScriptedMockProvider] = {}
    for pname, prof in cfg.providers.items():
        def _make(p: str) -> Any:
            def _h(req: ProviderRequest) -> ProviderResponse:
                if req.role == AgentRole.WORKER_EASY:
                    called_providers.append(p)
                    if p == "provider_fast":
                        return ProviderResponse(
                            provider_name=p,
                            exit_code=429,
                            raw_stderr="429 Too Many Requests",
                            rate_limited=True,
                            retry_after_seconds=600,
                        )
                    (temp_git_repo / "src/features/alpha/fb.py").write_text("FB = 1\n", encoding="utf-8")
                    return ok_response(p, "BEGIN_REPORT\nFallback ok\nEND_REPORT\nSTATUS: DONE\n")
                return ok_response(p, "VERDICT: APPROVE\nOK\n")
            return _h
        mocks[pname] = ScriptedMockProvider(name=pname, profile=prof, handler=_make(pname))

    reg = ProviderRegistry(providers=dict(mocks), role_map=cfg.roles.as_dict())
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        sleep_fn=lambda s: slept.append(s),
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert called_providers == ["provider_fast", "provider_main"]
    assert slept == []  # Zero hibernation sleep because fallback was available!
    events = _read_events(temp_git_repo)
    assert any(
        e["event_type"] == EventType.PROVIDER_FALLBACK_USED.value
        and e["payload"].get("from_provider") == "provider_fast"
        and e["payload"].get("to_provider") == "provider_main"
        for e in events
    )

    # Now test hibernation when ALL providers are rate-limited -> interrupted immediately by `abort`
    tasks_path2 = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="HIB-1",
                title="All rate limited -> hibernate then abort",
                description="All providers 429",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/beta/**"],
            )
        ],
    )
    store2 = LocalJsonTaskStore(tasks_path2)
    pdir = temp_git_repo / ".pipeline"
    saw_hibernating = {"ok": False}

    def all_429(req: ProviderRequest) -> ProviderResponse:
        return ProviderResponse(
            provider_name=req.role.value,
            exit_code=429,
            raw_stderr="429 Too Many Requests",
            rate_limited=True,
            retry_after_seconds=300,
        )

    reg2, _ = make_mock_registry(cfg, all_429)

    def sleep_check_hibernating_then_abort(_sec: float) -> None:
        st = json.loads((pdir / "state.json").read_text(encoding="utf-8"))
        assert st["status"] == RunStatus.HIBERNATING.value
        assert st["resume_at"] is not None
        saw_hibernating["ok"] = True
        enqueue_control_command(pdir, action=ControlAction.ABORT, reason="Wake & abort")

    runner2 = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store2,
        registry=reg2,
        sleep_fn=sleep_check_hibernating_then_abort,
        quiet=True,
    )
    assert runner2.run() == RunStatus.ABORTED
    assert saw_hibernating["ok"] is True
    events2 = _read_events(temp_git_repo)
    assert any(e["event_type"] == EventType.RUNNER_HIBERNATING.value for e in events2)


def test_hibernation_wakes_up_and_completes_when_cooldown_elapses(temp_git_repo: Path) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="WAKE-1",
                title="Hibernate and wake up",
                description="429 then resume",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"
    calls = {"w": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            calls["w"] += 1
            if calls["w"] == 1:
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=429,
                    raw_stderr="429 rate limit",
                    rate_limited=True,
                    retry_after_seconds=60,
                )
            (temp_git_repo / "src/features/alpha/wake.py").write_text("W = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nAwake\nEND_REPORT\nSTATUS: DONE\n")
        return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

    def sleep_send_resume(_sec: float) -> None:
        enqueue_control_command(pdir, action=ControlAction.RESUME, reason="Manual wake")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        sleep_fn=sleep_send_resume,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert store.get_task("WAKE-1").status == TaskStatus.DONE
    events = _read_events(temp_git_repo)
    assert any(e["event_type"] == EventType.RUNNER_HIBERNATING.value for e in events)
    assert any(e["event_type"] == EventType.RUNNER_WOKE_UP.value for e in events)


# ===========================================================================
# 6. Declarative GenericProxyMonitor: external config_file, route switch vs fatal
# ===========================================================================


def test_proxy_monitor_external_config_file_env_expansion_and_override(tmp_path: Path) -> None:
    state_dir = tmp_path / "proxy_state"
    state_dir.mkdir()
    status_file = state_dir / "status.json"
    log_file = state_dir / "gateway.log"

    now_ts = int(datetime.now(UTC).timestamp())
    status_file.write_text(
        json.dumps(
            {
                "active": {"route": "eu-west-gateway"},
                "last_ok": {"at": now_ts},
                "disabled": False,
                "blockers": [],
            }
        ),
        encoding="utf-8",
    )
    log_file.write_text(
        "12:00:00 gateway upstream ok token=secret_abc123\n",
        encoding="utf-8",
    )

    ext_cfg = {
        "proxy_host": "127.0.0.1",
        "proxy_port": 18080,
        "status_json": {
            "path": "${TEST_PROXY_STATE_DIR}/status.json",
            "route_field": "active.route",
            "last_ok_at_field": "last_ok.at",
            "disabled_field": "disabled",
            "blockers_field": "blockers",
            "transient_error_field": "last_error",
        },
        "log_probes": [
            {
                "path": "${TEST_PROXY_STATE_DIR}/gateway.log",
                "tail_lines": 50,
                "ok_patterns": ["upstream ok"],
                "transient_patterns": ["upstream switched"],
                "fatal_patterns": ["auth token expired"],
            }
        ],
    }
    ext_path = tmp_path / "proxy_monitor.local.yaml"
    ext_path.write_text(yaml.safe_dump(ext_cfg), encoding="utf-8")

    with _LocalTcpServer() as srv:
        pipeline_yaml = {
            "schema_version": 1,
            "providers": {
                "w": {"kind": "cli", "command": ["echo"]},
                "r": {"kind": "api", "base_url": "https://api.example.com/v1", "model": "m"},
            },
            "roles": {
                "planner": "r",
                "worker_easy": "w",
                "worker_medium": "w",
                "worker_hard": "w",
                "pre_reviewer": "r",
                "reviewer": "r",
                "architect": "w",
                "refactorer": "w",
            },
            "proxy_monitor": {
                "enabled": True,
                "config_file": "proxy_monitor.local.yaml",
                # Explicit override in pipeline.yaml must take precedence over external config_file
                "proxy_port": srv.port,
                "apply_to_providers": ["w"],
            },
        }
        cfg_path = tmp_path / "pipeline.yaml"
        cfg_path.write_text(yaml.safe_dump(pipeline_yaml), encoding="utf-8")

        with mock.patch.dict(os.environ, {"TEST_PROXY_STATE_DIR": str(state_dir)}):
            cfg = load_pipeline_config(cfg_path)
            assert cfg.proxy_monitor.enabled is True
            assert cfg.proxy_monitor.proxy_host == "127.0.0.1"
            assert cfg.proxy_monitor.proxy_port == srv.port
            assert cfg.proxy_monitor.apply_to_providers == ["w"]
            assert cfg.proxy_monitor.status_json is not None
            assert len(cfg.proxy_monitor.log_probes) == 1

            monitor = GenericProxyMonitor(cfg.proxy_monitor)
            assert monitor.applies_to("w", cfg.providers["w"]) is True
            assert monitor.applies_to("r", cfg.providers["r"]) is False

            ok, _detail, status = monitor.preflight()
            assert ok is True
            assert status.healthy is True
            assert status.active_route == "eu-west-gateway"
            assert status.last_ok_at is not None
            # Credentials/tokens must be redacted from last_message & raw_excerpt
            assert "secret_abc123" not in (status.last_message or "")
            assert "secret_abc123" not in status.raw_excerpt


def test_generic_proxy_monitor_route_switch_retries_and_fatal_error_pauses_immediately(
    temp_git_repo: Path,
) -> None:
    proxy_dir = temp_git_repo.parent / "mock_proxy_state"
    proxy_dir.mkdir(parents=True, exist_ok=True)
    status_path = proxy_dir / "status.json"
    log_path = proxy_dir / "proxy.log"
    now_ts = int(datetime.now(UTC).timestamp())

    with _LocalTcpServer() as srv:
        # 1. Healthy status.json + proxy.log -> preflight OK
        status_path.write_text(
            json.dumps(
                {
                    "route": "primary-route",
                    "disabled": False,
                    "last_ok": {"at": now_ts, "route": "primary-route"},
                    "blockers": [],
                }
            ),
            encoding="utf-8",
        )
        log_path.write_text(
            "11:47:34 proxy upstream ok via primary-route\n",
            encoding="utf-8",
        )
        pm_cfg = ProxyMonitorConfig(
            enabled=True,
            apply_to_providers=["provider_fast"],
            proxy_host="127.0.0.1",
            proxy_port=srv.port,
            status_json=StatusJsonProbeConfig(
                path=str(status_path),
                route_field="route",
                last_ok_at_field="last_ok.at",
                disabled_field="disabled",
                blockers_field="blockers",
                transient_error_field="last_error",
            ),
            log_probes=[
                LogProbeConfig(
                    path=str(log_path),
                    tail_lines=50,
                    ok_patterns=["upstream ok"],
                    transient_patterns=["upstream switched", "connection dropped during reply"],
                    fatal_patterns=["auth token expired", "blocked by firewall"],
                )
            ],
        )
        monitor = GenericProxyMonitor(pm_cfg)
        ok, _detail, st = monitor.preflight()
        assert ok is True
        assert st.healthy is True
        assert st.active_route == "primary-route"
        assert st.last_ok_at is not None

        # 2. Route switching during failure -> free transport_retry, then recovers!
        cfg = make_test_config(
            batch_review_size=1,
            max_transport_retries=4,
            transport_retry_delays_seconds=[1],
        ).model_copy(update={"proxy_monitor": pm_cfg})
        tasks_path = write_tasks_file(
            temp_git_repo,
            [
                Task(
                    id="PM-1",
                    title="Proxy route switch",
                    description="Recover after upstream switch",
                    complexity=TaskComplexity.EASY,
                    allowed_paths=["src/features/alpha/**"],
                )
            ],
        )
        store = LocalJsonTaskStore(tasks_path)
        calls = {"w": 0}

        def handler_switch(req: ProviderRequest) -> ProviderResponse:
            if req.role == AgentRole.WORKER_EASY:
                calls["w"] += 1
                if calls["w"] == 1:
                    log_path.write_text(
                        "11:48:00 proxy upstream switched to backup-route\n",
                        encoding="utf-8",
                    )
                    return ProviderResponse(
                        provider_name="provider_fast",
                        exit_code=1,
                        raw_stderr="unexpected error",
                        raw_stdout="partial",
                        duration_seconds=3.0,
                    )
                log_path.write_text(
                    "11:48:05 proxy upstream ok via backup-route\n",
                    encoding="utf-8",
                )
                (temp_git_repo / "src/features/alpha/pm.py").write_text("P = 1\n", encoding="utf-8")
                return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        reg, _ = make_mock_registry(cfg, handler_switch)
        runner = OrchestratorRunner(
            repo_root=temp_git_repo,
            config=cfg,
            store=store,
            registry=reg,
            sleep_fn=lambda _s: None,
            jitter_fn=lambda d: d,
            quiet=True,
        )
        assert runner.run() == RunStatus.COMPLETED
        assert calls["w"] == 2
        assert store.get_task("PM-1").attempts == 1
        events = _read_events(temp_git_repo)
        tr_ev = [e for e in events if e["event_type"] == EventType.PROVIDER_TRANSPORT_RETRY.value]
        assert len(tr_ev) == 1
        assert tr_ev[0]["payload"]["proxy_route_switching"] is True
        state_json = json.loads((temp_git_repo / ".pipeline/state.json").read_text(encoding="utf-8"))
        assert state_json["proxy_health_status"]["healthy"] is True

        # 3. Fatal blocker in status.json -> zero retries, immediate PAUSED with pause_reason="proxy_error"
        status_path.write_text(
            json.dumps(
                {
                    "route": "primary-route",
                    "disabled": False,
                    "blockers": ["auth token expired — manual re-authentication required"],
                }
            ),
            encoding="utf-8",
        )
        tasks_path2 = write_tasks_file(
            temp_git_repo,
            [
                Task(
                    id="PM-FATAL",
                    title="Proxy fatal auth error",
                    description="Should pause immediately without calling provider",
                    complexity=TaskComplexity.EASY,
                    allowed_paths=["src/features/beta/**"],
                )
            ],
        )
        store2 = LocalJsonTaskStore(tasks_path2)
        fatal_calls = {"count": 0}

        def handler_never_called(req: ProviderRequest) -> ProviderResponse:
            fatal_calls["count"] += 1
            return ok_response("provider_fast", "STATUS: DONE\n")

        reg2, _ = make_mock_registry(cfg, handler_never_called)
        runner2 = OrchestratorRunner(
            repo_root=temp_git_repo,
            config=cfg,
            store=store2,
            registry=reg2,
            quiet=True,
        )
        assert runner2.run() == RunStatus.PAUSED
        assert fatal_calls["count"] == 0
        state2 = json.loads((temp_git_repo / ".pipeline/state.json").read_text(encoding="utf-8"))
        assert state2["pause_reason"] == "proxy_error"
        assert state2["proxy_health_status"]["requires_manual_action"] is True
        events2 = _read_events(temp_git_repo)
        assert any(e["event_type"] == EventType.PROXY_ERROR.value for e in events2)


# ===========================================================================
# 7. Sequential account switching (account_group, account_profile, switch_command)
# ===========================================================================


def test_account_switching_runs_on_profile_change_skips_same_and_fails_fast_on_error(
    temp_git_repo: Path,
) -> None:
    switch_log = temp_git_repo.parent / "switch_calls.txt"
    switch_script = temp_git_repo.parent / "mock_aisw.py"
    switch_script.write_text(
        "import pathlib, sys\n"
        "log = pathlib.Path(sys.argv[1])\n"
        "profile = sys.argv[2]\n"
        "if profile == 'broken':\n"
        "    sys.stderr.write('aisw: profile broken not found\\n')\n"
        "    sys.exit(2)\n"
        "with log.open('a', encoding='utf-8') as f:\n"
        "    f.write(profile + '\\n')\n",
        encoding="utf-8",
    )

    cheap_prof = ProviderProfileConfig(
        kind="cli",
        can_edit_files=True,
        command=["echo"],
        account_group="agy",
        account_profile="cheap",
        switch_command=[sys.executable, str(switch_script), str(switch_log), "cheap"],
    )
    pro_prof = ProviderProfileConfig(
        kind="cli",
        can_edit_files=True,
        command=["echo"],
        account_group="agy",
        account_profile="pro",
        switch_command=[sys.executable, str(switch_script), str(switch_log), "pro"],
    )
    broken_prof = ProviderProfileConfig(
        kind="cli",
        can_edit_files=True,
        command=["echo"],
        account_group="agy",
        account_profile="broken",
        switch_command=[sys.executable, str(switch_script), str(switch_log), "broken"],
    )
    api_ro = ProviderProfileConfig(
        kind="api",
        can_edit_files=False,
        base_url="https://api.example.com/v1",
        model="rev",
    )

    cfg = PipelineConfig(
        providers={
            "agy_cheap": cheap_prof,
            "agy_pro": pro_prof,
            "agy_broken": broken_prof,
            "rev": api_ro,
        },
        roles=RoleMappingConfig(
            planner="rev",
            worker_easy="agy_cheap",
            worker_medium="agy_pro",
            worker_hard="agy_pro",
            pre_reviewer="rev",
            reviewer="rev",
            architect="agy_pro",
            refactorer="agy_pro",
        ),
        workflow={"batch_review_size": 1, "refactor_every_n_tasks": 0, "refactor_on_stage_complete": False},  # type: ignore[arg-type]
    )

    tasks = [
        Task(
            id="ACC-1",
            title="Easy 1 (cheap)",
            description="Edit alpha 1",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="ACC-2",
            title="Easy 2 (same cheap -> skip switch)",
            description="Edit beta 1",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
        Task(
            id="ACC-3",
            title="Medium 1 (switch to pro)",
            description="Edit gamma 1",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/gamma/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    store = LocalJsonTaskStore(tasks_path)

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY and req.task_id == "ACC-1":
            (temp_git_repo / "src/features/alpha/a1.py").write_text("A = 1\n", encoding="utf-8")
            return ok_response("agy_cheap", "STATUS: DONE\n")
        if req.role == AgentRole.WORKER_EASY and req.task_id == "ACC-2":
            (temp_git_repo / "src/features/beta/b1.py").write_text("B = 1\n", encoding="utf-8")
            return ok_response("agy_cheap", "STATUS: DONE\n")
        if req.role == AgentRole.WORKER_MEDIUM and req.task_id == "ACC-3":
            (temp_git_repo / "src/features/gamma/g1.py").write_text("G = 1\n", encoding="utf-8")
            return ok_response("agy_pro", "STATUS: DONE\n")
        return ok_response("rev", "VERDICT: APPROVE\nOK\n")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED

    # switch_command executed ONLY twice: None -> cheap (for ACC-1), skipped for ACC-2, cheap -> pro (for ACC-3)
    recorded = switch_log.read_text(encoding="utf-8").splitlines()
    assert recorded == ["cheap", "pro"]
    st = json.loads((temp_git_repo / ".pipeline/state.json").read_text(encoding="utf-8"))
    assert st["active_accounts"] == {"agy": "pro"}
    events = _read_events(temp_git_repo)
    sw_events = [e for e in events if e["event_type"] == EventType.PROVIDER_ACCOUNT_SWITCHED.value]
    assert len(sw_events) == 2

    # Fail-fast when switch_command fails: provider MUST NOT be invoked under wrong account!
    cfg_fail = cfg.model_copy(
        update={
            "roles": cfg.roles.model_copy(update={"worker_easy": "agy_broken"}),
        }
    )
    tasks_path2 = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="ACC-FAIL",
                title="Broken switch",
                description="Must fail fast",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store2 = LocalJsonTaskStore(tasks_path2)
    invoked = {"count": 0}

    def never_run(req: ProviderRequest) -> ProviderResponse:
        invoked["count"] += 1
        return ok_response("agy_broken", "STATUS: DONE\n")

    reg2, _ = make_mock_registry(cfg_fail, never_run)
    runner2 = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg_fail,
        store=store2,
        registry=reg2,
        quiet=True,
    )
    with pytest.raises(OrchestratorFatalError, match="Account switch failed"):
        runner2.run()
    assert invoked["count"] == 0
    events2 = _read_events(temp_git_repo)
    assert any(e["event_type"] == EventType.PROVIDER_ACCOUNT_SWITCH_FAILED.value for e in events2)

    # Doctor checks switch_command binary existence
    cfg_doc = cfg.model_copy(
        update={
            "providers": {
                **cfg.providers,
                "agy_cheap": cheap_prof.model_copy(
                    update={"switch_command": ["nonexistent_aisw_binary_xyz", "use", "agy", "cheap"]}
                ),
            }
        }
    )
    cfg_path = temp_git_repo / ".pipeline" / "pipeline.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg_doc.model_dump(mode="json")), encoding="utf-8")
    checks = dict((name, (ok, detail)) for name, ok, detail in run_doctor_checks(temp_git_repo, cfg_path))
    assert checks["account_switch:agy_cheap"][0] is False
    assert checks["account_switch:agy_pro"][0] is True
