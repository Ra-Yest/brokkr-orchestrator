"""Regression tests for AFK Orchestrator v0.2.1 bug fixes."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from orchestrator.config import (
    LogProbeConfig,
    NotificationsConfig,
    ProxyMonitorConfig,
    StatusJsonProbeConfig,
    TelegramNotificationConfig,
)
from orchestrator.control import enqueue_control_command
from orchestrator.diff_guard import evaluate_diff
from orchestrator.git_ops import GitChange
from orchestrator.models import (
    AgentRole,
    ControlAction,
    EventType,
    ProxyHealthStatus,
    RunStatus,
    Task,
    TaskComplexity,
    TaskStatus,
)
from orchestrator.process_utils import is_process_alive, run_noninteractive_process
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.proxy_monitor import (
    BaseProxyMonitor,
    GenericProxyMonitor,
    normalize_proxy_text,
)
from orchestrator.runner import OrchestratorFatalError, OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import (
    git,
    make_mock_registry,
    make_test_config,
    ok_response,
    write_tasks_file,
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


# ===========================================================================
# 1. NEEDS_REVIEW checkpoint resume after paused_infra & clean-tree fallback
# ===========================================================================


def test_needs_review_checkpoint_resumes_directly_at_reviewer_without_rerunning_worker_or_prereviewer(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="CHK-1",
            title="Checkpoint review task",
            description="Edit alpha module",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1, max_transport_retries=0, transient_retry_delays_seconds=[])
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"

    calls: list[AgentRole] = []
    reviewer_calls = {"count": 0}

    def handler_run1(req: ProviderRequest) -> ProviderResponse:
        calls.append(req.role)
        if req.role == AgentRole.WORKER_MEDIUM:
            (temp_git_repo / "src/features/alpha/mod.py").write_text(
                "def alpha() -> int:\n    return 42\n",
                encoding="utf-8",
            )
            return ok_response(
                "provider_main",
                "BEGIN_REPORT\nImplemented alpha=42\nEND_REPORT\n"
                "BEGIN_PROGRESS_UPDATE\nUpdated alpha() to return 42.\nEND_PROGRESS_UPDATE\n"
                "STATUS: DONE\n",
            )
        if req.role == AgentRole.PRE_REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nLooks clean.\n")
        if req.role == AgentRole.REVIEWER:
            reviewer_calls["count"] += 1
            return ProviderResponse(
                provider_name="provider_reviewer_api",
                exit_code=1,
                raw_stderr="HTTP 401 Unauthorized: token expired",
            )
        raise AssertionError(f"Unexpected role {req.role}")

    reg1, _ = make_mock_registry(cfg, handler_run1)
    runner1 = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg1,
        quiet=True,
    )
    assert runner1.run() == RunStatus.PAUSED
    assert calls == [AgentRole.WORKER_MEDIUM, AgentRole.PRE_REVIEWER, AgentRole.REVIEWER]

    t_paused = store.get_task("CHK-1")
    assert t_paused.status == TaskStatus.NEEDS_REVIEW
    assert t_paused.pre_review_approved is True
    assert "return 42" in (temp_git_repo / "src/features/alpha/mod.py").read_text(encoding="utf-8")

    # Operator sends `control resume` and restarts `orchestrator run`
    enqueue_control_command(pdir, action=ControlAction.RESUME, reason="Fixed reviewer token")
    calls_run2: list[AgentRole] = []

    def handler_run2(req: ProviderRequest) -> ProviderResponse:
        calls_run2.append(req.role)
        if req.role == AgentRole.REVIEWER:
            assert "return 42" in req.prompt
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nApproved on resume.\n")
        raise AssertionError(f"Must NOT re-run {req.role} after NEEDS_REVIEW checkpoint!")

    store2 = LocalJsonTaskStore(tasks_path)
    reg2, _ = make_mock_registry(cfg, handler_run2)
    runner2 = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store2,
        registry=reg2,
        quiet=True,
    )
    assert runner2.run() == RunStatus.COMPLETED
    # Neither WORKER_MEDIUM nor PRE_REVIEWER was invoked again; only REVIEWER ran and committed!
    assert calls_run2 == [AgentRole.REVIEWER]
    t_done = store2.get_task("CHK-1")
    assert t_done.status == TaskStatus.DONE
    assert t_done.final_commit_sha is not None
    assert git(temp_git_repo, "status", "--porcelain") == ""
    assert "return 42" in (temp_git_repo / "src/features/alpha/mod.py").read_text(encoding="utf-8")


def test_needs_review_with_manually_cleaned_working_tree_resets_safely_to_todo(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="CHK-CLEAN",
            title="Clean tree fallback",
            description="Edit alpha",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1, max_transport_retries=0, transient_retry_delays_seconds=[])
    store = LocalJsonTaskStore(tasks_path)

    def handler_pause(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            (temp_git_repo / "src/features/alpha/mod.py").write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_main", "STATUS: DONE\n")
        if req.role == AgentRole.PRE_REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        return ProviderResponse(
            provider_name="provider_reviewer_api",
            exit_code=1,
            raw_stderr="401 Unauthorized",
        )

    reg1, _ = make_mock_registry(cfg, handler_pause)
    assert OrchestratorRunner(
        repo_root=temp_git_repo, config=cfg, store=store, registry=reg1, quiet=True
    ).run() == RunStatus.PAUSED
    assert store.get_task("CHK-CLEAN").status == TaskStatus.NEEDS_REVIEW

    # User manually reverts working tree outside the orchestrator
    git(temp_git_repo, "reset", "--hard", "HEAD")
    git(temp_git_repo, "clean", "-fd", "-e", ".pipeline/")

    seen_run2: list[AgentRole] = []

    def handler_rerun(req: ProviderRequest) -> ProviderResponse:
        seen_run2.append(req.role)
        if req.role == AgentRole.WORKER_MEDIUM:
            (temp_git_repo / "src/features/alpha/mod.py").write_text("X = 2\n", encoding="utf-8")
            return ok_response("provider_main", "STATUS: DONE\n")
        return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

    store2 = LocalJsonTaskStore(tasks_path)
    reg2, _ = make_mock_registry(cfg, handler_rerun)
    assert OrchestratorRunner(
        repo_root=temp_git_repo, config=cfg, store=store2, registry=reg2, quiet=True
    ).run() == RunStatus.COMPLETED

    assert seen_run2 == [AgentRole.WORKER_MEDIUM, AgentRole.PRE_REVIEWER, AgentRole.REVIEWER]
    assert store2.get_task("CHK-CLEAN").status == TaskStatus.DONE
    events = _read_events(temp_git_repo)
    assert any(
        e["event_type"] == EventType.CRASH_RECOVERY_COMPLETED.value
        and e["payload"].get("reason") == "clean_working_tree_for_needs_review"
        for e in events
    )


# ===========================================================================
# 2. Strict max_attempts ceiling on timed_out / idle_timed_out / diff_guard_failed
# ===========================================================================


def test_repeated_timeouts_strictly_stop_at_max_attempts_and_block_dependents(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="MAX-TO",
            title="Always times out",
            description="Worker times out every attempt",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
            max_attempts=3,
        ),
        Task(
            id="MAX-DEP",
            title="Dependent task",
            description="Depends on MAX-TO",
            complexity=TaskComplexity.EASY,
            depends_on=["MAX-TO"],
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    # Deliberately set max_review_rounds=3, max_gate_retries=3 (product=9) to prove max_attempts=3 caps it!
    cfg = make_test_config(
        batch_review_size=1,
        max_review_rounds=3,
        max_gate_retries=3,
        max_transport_retries=0,
        transient_retry_delays_seconds=[],
    )
    store = LocalJsonTaskStore(tasks_path)
    worker_calls = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            worker_calls["count"] += 1
            return ProviderResponse(
                provider_name="provider_fast",
                exit_code=-1,
                timed_out=True,
                idle_timed_out=(worker_calls["count"] % 2 == 0),
                raw_stderr="idle timeout",
            )
        raise AssertionError(f"Unexpected {req.role}")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo, config=cfg, store=store, registry=reg, quiet=True
    )
    assert runner.run() == RunStatus.COMPLETED
    assert worker_calls["count"] == 3
    t_fail = store.get_task("MAX-TO")
    assert t_fail.status == TaskStatus.FAILED
    assert t_fail.attempts == 3
    assert "max_attempts (3)" in (t_fail.failure_reason or "")
    assert store.get_task("MAX-DEP").status == TaskStatus.BLOCKED


def test_identical_diff_guard_failed_twice_in_a_row_stops_without_wasting_attempts(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="DG-DUP",
            title="Repeated out-of-scope lockfile",
            description="Touches go.sum outside allowed_paths every time",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
            max_attempts=6,
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(
        batch_review_size=1,
        max_review_rounds=3,
        max_gate_retries=3,
        max_transport_retries=0,
    )
    store = LocalJsonTaskStore(tasks_path)
    worker_calls = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            worker_calls["count"] += 1
            (temp_git_repo / "go.sum").write_text(f"mod v1.0.{worker_calls['count']}\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")
        raise AssertionError(f"Unexpected {req.role}")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo, config=cfg, store=store, registry=reg, quiet=True
    )
    assert runner.run() == RunStatus.COMPLETED
    # Must stop after 2 identical consecutive diff_guard_failed violations, NOT burn all 6 attempts!
    assert worker_calls["count"] == 2
    t = store.get_task("DG-DUP")
    assert t.status == TaskStatus.FAILED
    assert t.attempts == 2
    assert "Repeated identical diff_guard_failed" in (t.failure_reason or "")
    assert not (temp_git_repo / "go.sum").exists()


# ===========================================================================
# 3. Clean Ctrl+C (KeyboardInterrupt) without hanging or orphan processes
# ===========================================================================


def test_keyboard_interrupt_kills_process_tree_quickly_without_hanging_on_join(
    tmp_path: Path,
) -> None:
    pid_file = tmp_path / "child_pid.txt"
    sleeper = tmp_path / "sleeper.py"
    sleeper.write_text(
        "import os, pathlib, sys, time\n"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid()), encoding='utf-8')\n"
        "sys.stdout.write('started\\n')\n"
        "sys.stdout.flush()\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )

    real_sleep = time.sleep
    sleep_calls = {"n": 0}

    def interrupt_after_start(sec: float) -> None:
        sleep_calls["n"] += 1
        real_sleep(sec)
        if pid_file.exists() and sleep_calls["n"] >= 2:
            raise KeyboardInterrupt()

    t0 = time.monotonic()
    with (
        mock.patch("orchestrator.process_utils.time.sleep", side_effect=interrupt_after_start),
        pytest.raises(KeyboardInterrupt),
    ):
        run_noninteractive_process(
            [sys.executable, str(sleeper)],
            cwd=tmp_path,
            timeout_seconds=60,
        )
    elapsed = time.monotonic() - t0
    assert elapsed < 3.0, f"KeyboardInterrupt took too long ({elapsed:.2f}s) — hung on join!"
    assert pid_file.exists()
    child_pid = int(pid_file.read_text(encoding="utf-8").strip())
    # Give OS a tiny moment to reap the terminated process
    time.sleep(0.1)
    assert is_process_alive(child_pid) is False


def test_runner_keyboard_interrupt_releases_lock_updates_state_and_notifies_telegram(
    temp_git_repo: Path,
) -> None:
    tg_cfg = TelegramNotificationConfig(
        enabled=True,
        bot_token_env="TG_KI_TOKEN",
        chat_id_env="TG_KI_CHAT",
        events=["run_finished"],
    )
    cfg = make_test_config(batch_review_size=1).model_copy(
        update={"notifications": NotificationsConfig(telegram=tg_cfg)}
    )
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="KI-1",
                title="Interrupt mid-run",
                description="Raises KeyboardInterrupt",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    sent_tg: list[str] = []

    def fake_urlopen(req: Any, timeout: float = 10) -> _FakeHttpResponse:
        body = json.loads(req.data.decode("utf-8"))
        sent_tg.append(str(body["text"]))
        return _FakeHttpResponse(200)

    def handler(req: ProviderRequest) -> ProviderResponse:
        raise KeyboardInterrupt()

    with (
        mock.patch.dict(os.environ, {"TG_KI_TOKEN": "tok", "TG_KI_CHAT": "42"}),
        mock.patch("orchestrator.notifications.urllib.request.urlopen", side_effect=fake_urlopen),
    ):
        reg, _ = make_mock_registry(cfg, handler)
        runner = OrchestratorRunner(
            repo_root=temp_git_repo, config=cfg, store=store, registry=reg, quiet=True
        )
        status = runner.run()

    assert status == RunStatus.ABORTED
    assert not (temp_git_repo / ".pipeline" / "runner.lock").exists()
    st = json.loads((temp_git_repo / ".pipeline" / "state.json").read_text(encoding="utf-8"))
    assert st["status"] == RunStatus.ABORTED.value
    assert st["pause_reason"] == "keyboard_interrupt"
    assert any("Ctrl+C" in msg for msg in sent_tg)
    events = _read_events(temp_git_repo)
    assert any(
        e["event_type"] == EventType.RUN_ABORTED.value
        and e["payload"].get("reason") == "keyboard_interrupt"
        for e in events
    )


# ===========================================================================
# 4. Read-only ignore list (go.work.sum allowed & cleaned; real edits fail)
# ===========================================================================


def test_readonly_ignore_paths_allows_go_work_sum_and_rejects_real_code_edits(
    temp_git_repo: Path,
) -> None:
    cfg = make_test_config()
    assert "go.work.sum" in cfg.diff_guard.readonly_ignore_paths

    # Unit check: go.work.sum & nested go.work.sum pass for read-only roles
    go_sum_diff = (
        "diff --git a/go.work.sum b/go.work.sum\n"
        "--- /dev/null\n+++ b/go.work.sum\n@@ -0,0 +1 @@\n+example.com/pkg v1.0.0 h1:abc=\n"
    )
    rep_ok = evaluate_diff(
        role=AgentRole.PRE_REVIEWER,
        changes=[GitChange(status="A", path="go.work.sum")],
        raw_diff=go_sum_diff,
        config=cfg,
    )
    assert rep_ok.passed is True

    # Unit check: real .go or .py edit still fails
    go_code_diff = (
        "diff --git a/cmd/main.go b/cmd/main.go\n"
        "--- /dev/null\n+++ b/cmd/main.go\n@@ -0,0 +1 @@\n+package main\n"
    )
    rep_bad = evaluate_diff(
        role=AgentRole.REVIEWER,
        changes=[GitChange(status="A", path="cmd/main.go")],
        raw_diff=go_code_diff,
        config=cfg,
    )
    assert rep_bad.passed is False
    assert any(v.code == "READ_ONLY_ROLE_MODIFIED_FILES" for v in rep_bad.red_violations)

    # Integration: pre_reviewer side-effect creates go.work.sum while worker edited alpha/mod.py -> passes & cleans up!
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="RO-1",
                title="LSP go.work.sum during review",
                description="Edit alpha",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)

    def handler_ok(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            (temp_git_repo / "src/features/alpha/mod.py").write_text(
                "def alpha() -> int:\n    return 99\n",
                encoding="utf-8",
            )
            return ok_response("provider_main", "STATUS: DONE\n")
        if req.role == AgentRole.PRE_REVIEWER:
            # Background LSP (e.g. gopls mcp) writes go.work.sum during read-only review
            (temp_git_repo / "go.work.sum").write_text("example.com/a v1.0.0\n", encoding="utf-8")
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    reg, _ = make_mock_registry(cfg, handler_ok)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo, config=cfg, store=store, registry=reg, quiet=True
    )
    assert runner.run() == RunStatus.COMPLETED
    assert store.get_task("RO-1").status == TaskStatus.DONE
    assert not (temp_git_repo / "go.work.sum").exists()
    assert "return 99" in (temp_git_repo / "src/features/alpha/mod.py").read_text(encoding="utf-8")

    # Integration: reviewer modifies a real .py file -> OrchestratorFatalError!
    tasks_path2 = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="RO-2",
                title="Reviewer modifies real code",
                description="Edit beta",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/beta/**"],
            )
        ],
    )
    store2 = LocalJsonTaskStore(tasks_path2)
    cfg_single = make_test_config(batch_review_size=1)

    def handler_bad(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/features/beta/b.py").write_text("B = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            (temp_git_repo / "src/features/beta/rogue.py").write_text("ROGUE = 1\n", encoding="utf-8")
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    reg2, _ = make_mock_registry(cfg_single, handler_bad)
    runner2 = OrchestratorRunner(
        repo_root=temp_git_repo, config=cfg_single, store=store2, registry=reg2, quiet=True
    )
    with pytest.raises(OrchestratorFatalError, match="Read-only role 'reviewer' modified repository files"):
        runner2.run()


# ===========================================================================
# 5. proxy_monitor UTF-8/mojibake normalization & pre_proxy / route-change retry
# ===========================================================================


def test_proxy_monitor_reads_cyrillic_utf8_cp1251_and_repairs_mojibake(tmp_path: Path) -> None:
    expected_route = "через ваш VPN"
    mojibake_route = expected_route.encode("utf-8").decode("cp1251")
    assert mojibake_route != expected_route
    assert normalize_proxy_text(mojibake_route) == expected_route
    assert normalize_proxy_text(expected_route) == expected_route

    status_path = tmp_path / "status.json"
    log_path = tmp_path / "proxy.log"
    now_ts = int(datetime.now(UTC).timestamp())
    pm_cfg = ProxyMonitorConfig(
        enabled=True,
        apply_to_providers=["provider_fast"],
        status_json=StatusJsonProbeConfig(
            path=str(status_path),
            route_field="route",
            last_ok_at_field="last_ok.at",
        ),
        log_probes=[LogProbeConfig(path=str(log_path), tail_lines=20, ok_patterns=["ok"])],
    )
    monitor = GenericProxyMonitor(pm_cfg)

    # Case A: Clean UTF-8 JSON & log
    status_path.write_text(
        json.dumps({"route": expected_route, "last_ok": {"at": now_ts}}, ensure_ascii=False),
        encoding="utf-8",
    )
    log_path.write_text(f"12:00:00 ok {expected_route}\n", encoding="utf-8")
    st_utf8 = monitor.read_status()
    assert st_utf8.active_route == expected_route
    assert expected_route in (st_utf8.last_message or "")

    # Case B: cp1251-encoded bytes on disk
    status_path.write_bytes(
        json.dumps({"route": expected_route, "last_ok": {"at": now_ts}}, ensure_ascii=False).encode("cp1251")
    )
    log_path.write_bytes(f"12:00:01 ok {expected_route}\n".encode("cp1251"))
    st_cp1251 = monitor.read_status()
    assert st_cp1251.active_route == expected_route
    assert expected_route in (st_cp1251.last_message or "")

    # Case C: UTF-8 JSON containing pre-mangled mojibake string
    status_path.write_text(
        json.dumps({"route": mojibake_route, "last_ok": {"at": now_ts}}, ensure_ascii=False),
        encoding="utf-8",
    )
    st_repaired = monitor.read_status()
    assert st_repaired.active_route == expected_route


class _SequenceProxyMonitor(BaseProxyMonitor):
    def __init__(self, config: ProxyMonitorConfig, snapshots: list[ProxyHealthStatus]) -> None:
        super().__init__(config)
        self._snapshots = list(snapshots)
        self._idx = 0

    def check_tcp_proxy(self) -> tuple[bool | None, str]:
        return True, "ok"

    def read_status(self) -> ProxyHealthStatus:
        snap = self._snapshots[min(self._idx, len(self._snapshots) - 1)]
        self._idx += 1
        return snap


def test_proxy_pre_switching_and_route_change_trigger_free_transport_retry(
    temp_git_repo: Path,
) -> None:
    pm_cfg = ProxyMonitorConfig(enabled=True, apply_to_providers=["provider_fast"])
    cfg = make_test_config(
        batch_review_size=1,
        max_transport_retries=4,
        transport_retry_delays_seconds=[1],
    ).model_copy(update={"proxy_monitor": pm_cfg})

    tasks = [
        Task(
            id="PM-PRE",
            title="Pre-proxy switching recovered by post-proxy",
            description="Edit alpha",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="PM-CHG",
            title="Active route changed between pre and post proxy",
            description="Edit beta",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    store = LocalJsonTaskStore(tasks_path)

    snapshots = [
        # PM-PRE attempt 1: pre_proxy is route_switching=True, post_proxy is already ok/False
        ProxyHealthStatus(healthy=True, state="route_switching", active_route="route-1", route_switching=True),
        ProxyHealthStatus(healthy=True, state="ok", active_route="route-1", route_switching=False),
        # PM-PRE retry 1: healthy
        ProxyHealthStatus(healthy=True, state="ok", active_route="route-1", route_switching=False),
        # PM-CHG attempt 1: neither snapshot has route_switching=True, but active_route changed!
        ProxyHealthStatus(healthy=True, state="ok", active_route="route-A", route_switching=False),
        ProxyHealthStatus(healthy=True, state="ok", active_route="route-B", route_switching=False),
        # PM-CHG retry 1: healthy
        ProxyHealthStatus(healthy=True, state="ok", active_route="route-B", route_switching=False),
    ]
    monitor = _SequenceProxyMonitor(pm_cfg, snapshots)
    calls: dict[str, int] = {"PM-PRE": 0, "PM-CHG": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY and req.task_id in calls:
            calls[req.task_id] += 1
            if calls[req.task_id] == 1:
                # Non-zero exit=3 with non-empty stdout (so ONLY proxy_switching makes it a free transport retry!)
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=3,
                    raw_stdout="partial output",
                    raw_stderr="exit 3",
                    duration_seconds=2.0,
                    received_meaningful_output=True,
                )
            target = (
                "src/features/alpha/a.py" if req.task_id == "PM-PRE" else "src/features/beta/b.py"
            )
            (temp_git_repo / target).write_text("OK = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")
        return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        proxy_monitor=monitor,
        sleep_fn=lambda _s: None,
        jitter_fn=lambda d: d,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert store.get_task("PM-PRE").status == TaskStatus.DONE
    assert store.get_task("PM-PRE").attempts == 1
    assert store.get_task("PM-CHG").status == TaskStatus.DONE
    assert store.get_task("PM-CHG").attempts == 1

    events = _read_events(temp_git_repo)
    tr_events = [e for e in events if e["event_type"] == EventType.PROVIDER_TRANSPORT_RETRY.value]
    assert len(tr_events) == 2
    assert all(e["payload"]["proxy_route_switching"] is True for e in tr_events)
    assert {e["task_id"] for e in tr_events} == {"PM-PRE", "PM-CHG"}
