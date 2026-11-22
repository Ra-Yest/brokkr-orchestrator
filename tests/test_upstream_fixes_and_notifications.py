"""Regression tests for Integrity Guard exclusions, abort rollback/TODO reset, upstream fixes U1-U7, live console output, and Telegram notifications."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import urllib.error
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from pydantic import ValidationError

from orchestrator import __version__
from orchestrator.cli import (
    _configure_stdio_buffering,
    build_parser,
    run_doctor_checks,
)
from orchestrator.cli import main as cli_main
from orchestrator.config import (
    DEFAULT_TELEGRAM_EVENTS,
    GateConfig,
    NotificationsConfig,
    PipelineConfig,
    ProjectConfig,
    ProviderProfileConfig,
    TelegramNotificationConfig,
    ZonesConfig,
    load_pipeline_config,
)
from orchestrator.control import enqueue_control_command
from orchestrator.diff_guard import evaluate_diff, filter_diff_for_review, scan_placeholders
from orchestrator.fs_utils import snapshot_directory_hashes
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
from orchestrator.notifications import TelegramNotifier
from orchestrator.process_utils import run_noninteractive_process
from orchestrator.prompt_builder import generate_repo_map
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.providers.cli_provider import CliAgentProvider
from orchestrator.runner import (
    PIPELINE_INTEGRITY_EXCLUDES,
    OrchestratorFatalError,
    OrchestratorRunner,
)
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import git, make_mock_registry, make_test_config, ok_response, write_tasks_file


def _simple_diff(path: str, added_lines: list[str]) -> str:
    body = "\n".join(f"+{ln}" for ln in added_lines)
    return (
        f"diff --git a/{path} b/{path}\n"
        f"--- a/{path}\n"
        f"+++ b/{path}\n"
        f"@@ -0,0 +1,{len(added_lines)} @@\n"
        f"{body}\n"
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


# ---------------------------------------------------------------------------
# C1: Integrity Guard excludes legitimate operator channels & console.log
# ---------------------------------------------------------------------------


def test_snapshot_directory_hashes_excludes_operator_channels_and_protects_core_files(
    tmp_path: Path,
) -> None:
    pdir = tmp_path / ".pipeline"
    (pdir / "control_inbox").mkdir(parents=True)
    (pdir / "runs" / "r1").mkdir(parents=True)

    (pdir / "pipeline.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (pdir / "tasks.json").write_text("{}", encoding="utf-8")
    (pdir / "state.json").write_text("{}", encoding="utf-8")
    (pdir / "events.jsonl").write_text("", encoding="utf-8")
    (pdir / "runs" / "r1" / "diff.patch").write_text("", encoding="utf-8")
    (pdir / "control_inbox" / "cmd.json").write_text('{"action":"pause"}', encoding="utf-8")
    (pdir / "INBOX.md").write_text("operator note\n", encoding="utf-8")
    (pdir / "control.json").write_text('{"pause_after_current": true}', encoding="utf-8")
    (pdir / "console.log").write_text("[12:00:00] started\n", encoding="utf-8")

    snap = snapshot_directory_hashes(pdir, exclude=PIPELINE_INTEGRITY_EXCLUDES)

    assert "pipeline.yaml" in snap
    assert "tasks.json" in snap
    assert "state.json" in snap
    assert "events.jsonl" in snap
    assert "runs/r1/diff.patch" in snap

    assert "control_inbox/cmd.json" not in snap
    assert "INBOX.md" not in snap
    assert "control.json" not in snap
    assert "console.log" not in snap


def test_integrity_guard_allows_control_inbox_inbox_md_control_json_and_console_log_during_provider(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="C1-1",
            title="Task during operator input",
            description="Write alpha helper",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="C1-2",
            title="Second task after resume",
            description="Write beta helper",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1)
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"
    pause_ticks = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY and req.task_id == "C1-1":
            # Simulate operator issuing `control pause`, editing INBOX.md / control.json, and Tee-Object writing console.log
            enqueue_control_command(pdir, action=ControlAction.PAUSE_AFTER_CURRENT, reason="Check status")
            (pdir / "INBOX.md").write_text("Follow up on C1-2 cleanly.\n", encoding="utf-8")
            (pdir / "control.json").write_text('{"commands": []}\n', encoding="utf-8")
            with (pdir / "console.log").open("a", encoding="utf-8") as logf:
                logf.write("[12:00:01] provider running...\n")
            (temp_git_repo / "src/features/alpha/ok.py").write_text("VAL = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone C1-1\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.WORKER_EASY and req.task_id == "C1-2":
            (temp_git_repo / "src/features/beta/ok.py").write_text("VAL = 2\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone C1-2\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        raise AssertionError(f"Unexpected {req.role} {req.task_id}")

    def sleep_and_resume(_sec: float) -> None:
        pause_ticks["count"] += 1
        if pause_ticks["count"] == 1:
            enqueue_control_command(pdir, action=ControlAction.RESUME, reason="Resume now")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=sleep_and_resume,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert store.get_task("C1-1").status == TaskStatus.DONE
    assert store.get_task("C1-2").status == TaskStatus.DONE
    assert pause_ticks["count"] >= 1

    events = _read_events(temp_git_repo)
    assert not any(e["event_type"] == EventType.RED_ZONE_VIOLATION.value for e in events)
    assert any(e["event_type"] == EventType.PAUSE_ENTERED.value for e in events)
    assert any(e["event_type"] == EventType.PAUSE_RESUMED.value for e in events)


def test_integrity_guard_still_blocks_tampering_with_protected_pipeline_files(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="C1-PROT",
            title="Protected pipeline tamper",
            description="Must catch edits to state.json / tasks.json / runs/",
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
    pdir = temp_git_repo / ".pipeline"
    (pdir / "pipeline.yaml").write_text("schema_version: 1\n", encoding="utf-8")

    def handler(req: ProviderRequest) -> ProviderResponse:
        # Agent tampers with protected pipeline.yaml
        (pdir / "pipeline.yaml").write_text("schema_version: 999\n", encoding="utf-8")
        return ok_response("provider_fast", "STATUS: DONE\n")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert store.get_task("C1-PROT").status == TaskStatus.FAILED
    events = _read_events(temp_git_repo)
    red_events = [e for e in events if e["event_type"] == EventType.RED_ZONE_VIOLATION.value]
    assert red_events
    assert any(
        v["code"] == "RED_ZONE_PIPELINE_TAMPERED"
        for e in red_events
        for v in e["payload"].get("violations", [])
    )


# ---------------------------------------------------------------------------
# C2: `control abort` rolls back dirty work and returns active task to TODO
# ---------------------------------------------------------------------------


def test_control_abort_during_worker_rolls_back_and_resets_task_to_todo_for_next_run(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="C2-1",
            title="Abortable worker task",
            description="Create alpha_wip.py",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1)
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"
    reviewer_calls = {"count": 0}
    worker_calls = {"count": 0}

    def handler_abort(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            worker_calls["count"] += 1
            (temp_git_repo / "src/features/alpha/alpha_wip.py").write_text(
                "WIP = True\n", encoding="utf-8"
            )
            # Operator sends `control abort` while the worker is executing
            enqueue_control_command(pdir, action=ControlAction.ABORT, reason="User stop")
            return ok_response("provider_fast", "BEGIN_REPORT\nPartial\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            reviewer_calls["count"] += 1
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    registry, _ = make_mock_registry(cfg, handler_abort)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
    )
    assert runner.run() == RunStatus.ABORTED

    # Reviewer was never called, dirty file was rolled back, task returned cleanly to TODO
    assert worker_calls["count"] == 1
    assert reviewer_calls["count"] == 0
    assert not (temp_git_repo / "src/features/alpha/alpha_wip.py").exists()
    assert git(temp_git_repo, "status", "--porcelain") == ""

    task_after = store.get_task("C2-1")
    assert task_after.status == TaskStatus.TODO
    assert store.get_next_available_task() is not None
    assert store.get_next_available_task().id == "C2-1"  # type: ignore[union-attr]

    events = _read_events(temp_git_repo)
    assert not any(e["event_type"] == EventType.RED_ZONE_VIOLATION.value for e in events)
    assert any(e["event_type"] == EventType.TASK_ROLLED_BACK.value for e in events)

    # Next run picks up C2-1 immediately without needing crash recovery or manual retry
    def handler_finish(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/features/alpha/alpha_wip.py").write_text(
                "DONE = True\n", encoding="utf-8"
            )
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    registry2, _ = make_mock_registry(cfg, handler_finish)
    runner2 = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry2,
        quiet=True,
    )
    assert runner2.run() == RunStatus.COMPLETED
    assert store.get_task("C2-1").status == TaskStatus.DONE


def test_control_abort_during_batch_review_preserves_batch_pending_commits(
    temp_git_repo: Path,
) -> None:
    tasks = [
        Task(
            id="CB-1",
            title="Batch task 1",
            description="Create a.py",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="CB-2",
            title="Batch task 2",
            description="Create b.py",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=2, transient_retry_delays_seconds=[60])
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"
    ticks = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            target = "src/features/alpha/a.py" if req.task_id == "CB-1" else "src/features/beta/b.py"
            (temp_git_repo / target).write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            return ProviderResponse(
                provider_name="provider_reviewer_api",
                exit_code=1,
                raw_stderr="503 Service Unavailable",
            )
        raise AssertionError(f"Unexpected {req.role}")

    def sleep_and_abort(_sec: float) -> None:
        ticks["count"] += 1
        if ticks["count"] == 1:
            enqueue_control_command(pdir, action=ControlAction.ABORT, reason="Abort batch wait")

    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        sleep_fn=sleep_and_abort,
        quiet=True,
    )
    assert runner.run() == RunStatus.ABORTED
    assert store.get_task("CB-1").status == TaskStatus.BATCH_PENDING
    assert store.get_task("CB-2").status == TaskStatus.BATCH_PENDING
    assert (temp_git_repo / "src/features/alpha/a.py").exists()
    assert (temp_git_repo / "src/features/beta/b.py").exists()


# ---------------------------------------------------------------------------
# U1: `agy` flags, `--provider-preset agy-only`, and `{model}` validation
# ---------------------------------------------------------------------------


def test_cli_profile_rejects_model_placeholder_without_model() -> None:
    with pytest.raises(ValidationError, match="missing or empty"):
        ProviderProfileConfig(
            kind="cli",
            command=["agy"],
            prompt_mode="stdin",
            args_template=["--model", "{model}", "--mode", "auto"],
            model=None,
        )

    with pytest.raises(ValidationError, match="missing or empty"):
        ProviderProfileConfig(
            kind="cli",
            command=["agy"],
            prompt_mode="stdin",
            args_template=["--model", "{model}"],
            model="   ",
        )

    # Also verify defense-in-depth in CliAgentProvider.healthcheck()
    valid = ProviderProfileConfig(
        kind="cli",
        command=["agy"],
        prompt_mode="stdin",
        args_template=["--model", "{model}"],
        model="gemini-4-hydrogen-high",
    )
    mutated = valid.model_copy(update={"model": ""})
    ok, msg = CliAgentProvider("agy_bad", mutated).healthcheck()
    assert ok is False
    assert "{model}" in msg


def test_example_yaml_and_init_agy_only_preset_have_valid_agy_flags(tmp_path: Path) -> None:
    example_cfg = load_pipeline_config(Path(__file__).resolve().parent.parent / "pipeline.example.yaml")
    main_prof = example_cfg.providers["provider_main"]
    assert main_prof.command == ["agy"]
    assert main_prof.prompt_mode == "stdin"
    assert "--mode" not in main_prof.args_template and "auto" not in main_prof.args_template
    assert "--dangerously-skip-permissions" in main_prof.args_template
    assert "--disable-slash-commands" in main_prof.args_template
    assert "--input-format" in main_prof.args_template
    assert "--output-format" in main_prof.args_template
    assert main_prof.model == "gemini-4-hydrogen-high"

    pre_prof = example_cfg.providers["provider_pre_reviewer_agy"]
    assert pre_prof.can_edit_files is False
    assert pre_prof.command == ["agy"]
    assert pre_prof.prompt_mode == "stdin"
    assert "plan" in pre_prof.args_template
    assert "--disable-slash-commands" not in pre_prof.args_template

    # Init with `--provider-preset agy-only`
    repo = tmp_path / "agy_repo"
    repo.mkdir()
    subprocess.run(["git", "init"], cwd=str(repo), check=True, capture_output=True)
    cli_main(["init", "--repo", str(repo), "--stack", "generic", "--provider-preset", "agy-only"])

    cfg = load_pipeline_config(repo / ".pipeline" / "pipeline.yaml")
    assert cfg.providers[cfg.roles.worker_easy].model == "gemini-4-hydrogen-medium"
    assert cfg.providers[cfg.roles.worker_medium].model == "gemini-4-hydrogen-high"
    assert cfg.providers[cfg.roles.worker_hard].model == "gemini-4-neon-high"
    assert cfg.providers[cfg.roles.pre_reviewer].model == "gemini-4-neon-medium"
    assert cfg.providers[cfg.roles.planner].model == "gemini-4-neon-xhigh"
    assert cfg.providers[cfg.roles.reviewer].model == "gemini-4-neon-xhigh"
    assert cfg.providers[cfg.roles.architect].model == "gemini-4-neon-xhigh"
    assert cfg.providers[cfg.roles.refactorer].model == "gemini-4-neon-xhigh"

    # Read-only roles use `--mode plan` WITHOUT `--disable-slash-commands` and can_edit_files=False
    for ro_role in (cfg.roles.planner, cfg.roles.pre_reviewer, cfg.roles.reviewer):
        prof = cfg.providers[ro_role]
        assert prof.can_edit_files is False
        assert prof.command == ["agy"]
        assert prof.prompt_mode == "stdin"
        assert "plan" in prof.args_template
        assert "--disable-slash-commands" not in prof.args_template

    # Writing roles omit `--mode auto` (avoids stderr warnings on some accounts) and use `--dangerously-skip-permissions`
    for w_role in (
        cfg.roles.worker_easy,
        cfg.roles.worker_medium,
        cfg.roles.worker_hard,
        cfg.roles.architect,
        cfg.roles.refactorer,
    ):
        prof = cfg.providers[w_role]
        assert prof.can_edit_files is True
        assert "--mode" not in prof.args_template
        assert "auto" not in prof.args_template
        assert "--dangerously-skip-permissions" in prof.args_template


# ---------------------------------------------------------------------------
# U2: Root `build/` is no longer ignored by default; artifact build paths are
# ---------------------------------------------------------------------------


def test_root_build_dir_is_included_in_repo_map_and_diff_guard_while_artifact_builds_excluded(
    temp_git_repo: Path,
) -> None:
    (temp_git_repo / "build" / "scripts").mkdir(parents=True)
    (temp_git_repo / "build" / "scripts" / "sign.py").write_text("print('sign')\n", encoding="utf-8")
    (temp_git_repo / "app" / "build").mkdir(parents=True)
    (temp_git_repo / "app" / "build" / "bundle.js").write_text("var x=1;\n", encoding="utf-8")
    (temp_git_repo / "android" / "app" / "build" / "outputs").mkdir(parents=True)
    (temp_git_repo / "android" / "app" / "build" / "outputs" / "app.apk").write_text(
        "apk", encoding="utf-8"
    )

    repo_map = generate_repo_map(temp_git_repo, ProjectConfig().repo_map_ignore)
    assert "build/scripts/sign.py" in repo_map
    assert "app/build/bundle.js" not in repo_map
    assert "android/app/build/outputs/app.apk" not in repo_map

    cfg = make_test_config()
    raw_diff = (
        _simple_diff("build/scripts/sign.py", ["def sign():", "    # TODO: replace", "    return 1"])
        + _simple_diff("app/build/bundle.js", ["// TODO: minified artifact"])
        + _simple_diff("android/app/build/outputs/out.txt", ["// TODO: build output"])
    )
    filtered = filter_diff_for_review(raw_diff, cfg.diff_guard.exclude_from_review)
    assert "def sign():" in filtered
    assert "minified artifact" not in filtered
    assert "build output" not in filtered

    rep = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[
            GitChange(status="A", path="build/scripts/sign.py"),
            GitChange(status="A", path="app/build/bundle.js"),
        ],
        raw_diff=raw_diff,
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["build/**", "app/**"],
    )
    # Placeholder inside root `build/scripts/sign.py` IS caught!
    assert any(
        v.code == "PLACEHOLDER_TODO_MARKER" and v.path == "build/scripts/sign.py"
        for v in rep.placeholder_violations
    )
    assert all(v.path != "app/build/bundle.js" for v in rep.placeholder_violations)


# ---------------------------------------------------------------------------
# U3: DiffGuard.max_files ignores `exclude_from_review` files
# ---------------------------------------------------------------------------


def test_diff_guard_max_files_ignores_excluded_files_while_preserving_changed_files() -> None:
    cfg = make_test_config()
    # Easy limit is max_files=5: 3 source files + 8 excluded files = 11 total, 3 countable -> PASS
    changes = [
        GitChange(status="A", path="src/features/alpha/a1.py"),
        GitChange(status="A", path="src/features/alpha/a2.py"),
        GitChange(status="A", path="src/features/alpha/a3.py"),
        GitChange(status="M", path="package-lock.json"),
        GitChange(status="A", path="assets/s1.png"),
        GitChange(status="A", path="assets/s2.png"),
        GitChange(status="A", path="assets/s3.svg"),
        GitChange(status="A", path="src/features/alpha/model.generated.ts"),
        GitChange(status="A", path="src/features/alpha/api.generated.ts"),
        GitChange(status="A", path="dist/out1.js"),
        GitChange(status="A", path="app/build/out2.js"),
    ]
    rep = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=changes,
        raw_diff=_simple_diff("src/features/alpha/a1.py", ["X = 1"]),
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**", "package-lock.json", "assets/**", "dist/**", "app/**"],
    )
    assert rep.passed is True
    assert rep.limit_violations == []
    assert len(rep.changed_files) == 11

    # 6 countable source files + 2 excluded files -> FAIL on DIFF_GUARD_MAX_FILES
    too_many = [
        *[GitChange(status="A", path=f"src/features/alpha/f{i}.py") for i in range(6)],
        GitChange(status="A", path="assets/icon.png"),
        GitChange(status="M", path="package-lock.json"),
    ]
    rep_fail = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=too_many,
        raw_diff=_simple_diff("src/features/alpha/f0.py", ["X = 1"]),
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**", "assets/**", "package-lock.json"],
    )
    assert rep_fail.passed is False
    assert any(
        v.code == "DIFF_GUARD_MAX_FILES" and "6 countable files" in v.message
        for v in rep_fail.limit_violations
    )


# ---------------------------------------------------------------------------
# U4: PlaceholderGuard `stub` domain comments vs real empty stub comments
# ---------------------------------------------------------------------------


def test_placeholder_guard_allows_domain_stub_comments_and_catches_real_stub_placeholders() -> None:
    legit_diff = _simple_diff(
        "src/features/alpha/vpn.go",
        [
            "// stub entry returned by provider when offline",
            "# stub subscription fixture for integration tests",
            "// create gRPC stub client",
            "func connect() error { return nil }",
        ],
    )
    assert scan_placeholders(legit_diff) == []

    bad_lines = [
        "// stub",
        "# stub   ",
        "// empty stub",
        "# stub implementation",
        "// stub only",
        "# stub method",
        "// stub function",
        "// placeholder implementation",
        "# implement me",
    ]
    for line in bad_lines:
        violations = scan_placeholders(_simple_diff("src/features/alpha/bad.py", [line]))
        assert any(
            v.code == "PLACEHOLDER_EMPTY_STUB_COMMENT" for v in violations
        ), f"Expected EMPTY_STUB_COMMENT for: {line!r}"


# ---------------------------------------------------------------------------
# U5: `zones.enforce_allowed_paths_on_yellow` both False (default) and True
# ---------------------------------------------------------------------------


def test_yellow_shared_enforce_allowed_paths_flag_false_vs_true() -> None:
    cfg_default = make_test_config()
    assert cfg_default.zones.enforce_allowed_paths_on_yellow is False

    # Default False: worker may touch yellow_shared even if not in allowed_paths (routes to solo Master review)
    rep_default = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="M", path="src/shared/schema.py")],
        raw_diff=_simple_diff("src/shared/schema.py", ["SCHEMA_VERSION = 2"]),
        config=cfg_default,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**"],
    )
    assert rep_default.passed is True
    assert rep_default.touched_yellow_zone is True

    # Strict True: worker must explicitly have yellow file in allowed_paths
    cfg_strict = cfg_default.model_copy(
        update={"zones": ZonesConfig(enforce_allowed_paths_on_yellow=True)}
    )
    rep_blocked = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="M", path="src/shared/schema.py")],
        raw_diff=_simple_diff("src/shared/schema.py", ["SCHEMA_VERSION = 2"]),
        config=cfg_strict,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**"],
    )
    assert rep_blocked.passed is False
    assert rep_blocked.touched_yellow_zone is True
    assert any(v.code == "OUT_OF_ALLOWED_PATHS" for v in rep_blocked.scope_violations)

    # Strict True with yellow path explicitly listed in allowed_paths -> allowed + solo Master review
    rep_explicit = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="M", path="src/shared/schema.py")],
        raw_diff=_simple_diff("src/shared/schema.py", ["SCHEMA_VERSION = 2"]),
        config=cfg_strict,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**", "src/shared/**"],
    )
    assert rep_explicit.passed is True
    assert rep_explicit.touched_yellow_zone is True

    # Strict True still permits ARCHITECT / REFACTORER to touch yellow_shared freely
    for arch_role in (AgentRole.ARCHITECT, AgentRole.REFACTORER):
        rep_arch = evaluate_diff(
            role=arch_role,
            changes=[GitChange(status="M", path="src/shared/schema.py")],
            raw_diff=_simple_diff("src/shared/schema.py", ["SCHEMA_VERSION = 3"]),
            config=cfg_strict,
            allowed_paths=["src/features/alpha/**"],
        )
        assert rep_arch.passed is True
        assert rep_arch.touched_yellow_zone is True


# ---------------------------------------------------------------------------
# U6: `--version` in CLI & `gates` + Telegram env checks in `doctor`
# ---------------------------------------------------------------------------


def test_cli_version_and_doctor_checks_for_gates_and_telegram(
    temp_git_repo: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli_main(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert f"orchestrator {__version__}" in captured.out

    cfg = make_test_config(
        gates=[
            GateConfig(name="py_ok", command=[sys.executable, "-V"], optional=False),
            GateConfig(name="missing_req", command=["definitely_nonexistent_tool_xyz_123"], optional=False),
            GateConfig(name="missing_opt", command=["definitely_nonexistent_opt_xyz_456"], optional=True),
        ]
    ).model_copy(
        update={
            "notifications": NotificationsConfig(
                telegram=TelegramNotificationConfig(
                    enabled=True,
                    bot_token_env="TEST_MISSING_TG_TOKEN_XYZ",
                    chat_id_env="TEST_MISSING_TG_CHAT_XYZ",
                )
            )
        }
    )
    cfg_path = temp_git_repo / ".pipeline" / "pipeline.yaml"
    import yaml

    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(yaml.safe_dump(cfg.model_dump(mode="json")), encoding="utf-8")
    write_tasks_file(temp_git_repo, [])

    checks = dict((name, (ok, detail)) for name, ok, detail in run_doctor_checks(temp_git_repo, cfg_path))
    assert checks["gate:py_ok"][0] is True
    assert checks["gate:missing_req"][0] is False
    assert "not found in PATH" in checks["gate:missing_req"][1]
    assert checks["gate:missing_opt"][0] is True
    assert "WARN (optional)" in checks["gate:missing_opt"][1]
    assert checks["notifications:telegram"][0] is True
    assert "WARN:" in checks["notifications:telegram"][1]


# ---------------------------------------------------------------------------
# U7: PYTHONDONTWRITEBYTECODE=1 in child processes
# ---------------------------------------------------------------------------


def test_run_noninteractive_process_sets_pythondontwritebytecode(tmp_path: Path) -> None:
    script = tmp_path / "check_env.py"
    script.write_text(
        "import os\nprint(os.environ.get('PYTHONDONTWRITEBYTECODE', ''))\n",
        encoding="utf-8",
    )
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PYTHONDONTWRITEBYTECODE", None)
        res = run_noninteractive_process([sys.executable, str(script)], cwd=tmp_path, timeout_seconds=15)
    assert res.exit_code == 0
    assert res.stdout.strip() == "1"


# ---------------------------------------------------------------------------
# Live console progress output + `--quiet` + stdio buffering
# ---------------------------------------------------------------------------


def test_live_console_output_enabled_by_default_and_suppressed_with_quiet(
    temp_git_repo: Path,
) -> None:
    parser = build_parser()
    assert parser.parse_args(["run"]).quiet is False
    assert parser.parse_args(["run", "--quiet"]).quiet is True
    assert parser.parse_args(["run", "-q"]).quiet is True

    # Verify `_configure_stdio_buffering` calls reconfigure(line_buffering=True, write_through=True)
    fake_out = mock.MagicMock()
    fake_err = mock.MagicMock()
    with mock.patch("orchestrator.cli.sys.stdout", fake_out), mock.patch(
        "orchestrator.cli.sys.stderr", fake_err
    ):
        _configure_stdio_buffering()
    fake_out.reconfigure.assert_called_once_with(line_buffering=True, write_through=True)
    fake_err.reconfigure.assert_called_once_with(line_buffering=True, write_through=True)

    tasks = [
        Task(
            id="LOG-1",
            title="Console progress test",
            description="Write alpha module",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/alpha/**"],
        )
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = make_test_config(batch_review_size=1)
    store = LocalJsonTaskStore(tasks_path)

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/features/alpha/log_test.py").write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.REVIEWER:
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected {req.role}")

    stream = io.StringIO()
    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=False,
        console_stream=stream,
    )
    assert runner.run() == RunStatus.COMPLETED
    out = stream.getvalue()
    for marker in (
        "run_started",
        "task_claimed #LOG-1",
        "provider_invoked #LOG-1",
        "provider_succeeded #LOG-1",
        "gate_passed #LOG-1",
        "review_completed #LOG-1 verdict=APPROVE",
        "git_committed #LOG-1",
        "run_finished",
    ):
        assert marker in out, f"Missing '{marker}' in live output:\n{out}"
    # Must NOT dump full AGENTS.md / prompts
    assert "REPOSITORY INSTRUCTIONS" not in out

    # Quiet mode suppresses all live lines
    store.retry_task("LOG-1")
    quiet_stream = io.StringIO()
    runner_quiet = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
        console_stream=quiet_stream,
    )
    assert runner_quiet.run() == RunStatus.COMPLETED
    assert quiet_stream.getvalue() == ""


# ---------------------------------------------------------------------------
# Telegram notifications: all 7 events, batch summary, filtering & fail-safety
# ---------------------------------------------------------------------------


def test_telegram_notifier_event_filtering_and_failsafe_never_leaks_token(
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = TelegramNotificationConfig(
        enabled=True,
        bot_token_env="TG_TEST_TOKEN",
        chat_id_env="TG_TEST_CHAT",
        events=["run_started", "fatal_error"],
    )
    notifier = TelegramNotifier(cfg)
    assert notifier.should_notify("run_started") is True
    assert notifier.should_notify("task_done") is False

    sent_payloads: list[dict[str, Any]] = []

    def fake_urlopen(req: Any, timeout: float = 10) -> _FakeHttpResponse:
        sent_payloads.append(json.loads(req.data.decode("utf-8")))
        return _FakeHttpResponse(200)

    secret_token = "123456:SUPER_SECRET_BOT_TOKEN"
    with (
        mock.patch.dict(os.environ, {"TG_TEST_TOKEN": secret_token, "TG_TEST_CHAT": "999"}),
        mock.patch("orchestrator.notifications.urllib.request.urlopen", side_effect=fake_urlopen),
    ):
        assert notifier.notify("task_done", "Ignored", "Should not send") is False
        assert sent_payloads == []
        assert notifier.notify("run_started", "Started", "Hello AFK") is True
        assert len(sent_payloads) == 1
        assert sent_payloads[0]["chat_id"] == "999"
        assert sent_payloads[0]["text"] == "[Started]\nHello AFK"

    # Network failure / timeout / HTTP error must NEVER raise and must NOT leak token to stderr
    with (
        mock.patch.dict(os.environ, {"TG_TEST_TOKEN": secret_token, "TG_TEST_CHAT": "999"}),
        mock.patch(
            "orchestrator.notifications.urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection timed out"),
        ),
    ):
        assert notifier.notify("fatal_error", "Boom", "Details") is False
    err_text = capsys.readouterr().err
    assert "Telegram notification failed" in err_text
    assert secret_token not in err_text


def test_runner_sends_all_seven_telegram_events_with_mocked_http_and_survives_network_errors(
    temp_git_repo: Path,
) -> None:
    tg_cfg = TelegramNotificationConfig(
        enabled=True,
        bot_token_env="TG_TEST_TOKEN",
        chat_id_env="TG_TEST_CHAT",
        events=list(DEFAULT_TELEGRAM_EVENTS),
    )

    def _with_tg(base: PipelineConfig) -> PipelineConfig:
        return base.model_copy(update={"notifications": NotificationsConfig(telegram=tg_cfg)})

    sent_messages: list[str] = []

    def fake_urlopen(req: Any, timeout: float = 10) -> _FakeHttpResponse:
        body = json.loads(req.data.decode("utf-8"))
        sent_messages.append(str(body["text"]))
        return _FakeHttpResponse(200)

    # Scenario A: run_started + pause (rate_limit) + escalation + task_done (solo & consolidated batch) + task_failed + run_finished
    tasks = [
        Task(
            id="TG-M1",
            title="Escalated medium task",
            description="Needs architect then completes",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="TG-B1",
            title="Batch easy 1",
            description="Beta file",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
        Task(
            id="TG-B2",
            title="Batch easy 2",
            description="Gamma file",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/gamma/**"],
        ),
        Task(
            id="TG-F1",
            title="Failing task",
            description="Always rejected",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="TG-DEP",
            title="Blocked dependent",
            description="Depends on TG-F1",
            complexity=TaskComplexity.EASY,
            depends_on=["TG-F1"],
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    cfg = _with_tg(
        make_test_config(
            batch_review_size=2,
            max_review_rounds=1,
            hard_split_after_rounds=1,
            max_gate_retries=2,
        )
    )
    store = LocalJsonTaskStore(tasks_path)
    m1_calls = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM and req.task_id == "TG-M1":
            m1_calls["count"] += 1
            if m1_calls["count"] == 1:
                return ProviderResponse(
                    provider_name="provider_main",
                    exit_code=429,
                    raw_stderr="429 rate limit",
                    rate_limited=True,
                    retry_after_seconds=1,
                )
            if m1_calls["count"] == 2:
                return ok_response(
                    "provider_main",
                    "BEGIN_ESCALATION\nNeed shared constant\nEND_ESCALATION\nSTATUS: NEEDS_ARCH_CHANGE\n",
                )
            (temp_git_repo / "src/features/alpha/m1.py").write_text("OK = 1\n", encoding="utf-8")
            return ok_response("provider_main", "BEGIN_REPORT\nDone M1\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.ARCHITECT:
            (temp_git_repo / "src/shared/schema.py").write_text(
                "SCHEMA_VERSION = 2\n", encoding="utf-8"
            )
            return ok_response("provider_master", "BEGIN_REPORT\nUpdated schema\nEND_REPORT\nSTATUS: DONE\n")

        if req.role == AgentRole.WORKER_EASY:
            target = "src/features/beta/b1.py" if req.task_id == "TG-B1" else "src/features/gamma/g1.py"
            (temp_git_repo / target).write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "STATUS: DONE\n")

        if req.role == AgentRole.WORKER_MEDIUM and req.task_id == "TG-F1":
            (temp_git_repo / "src/features/alpha/f1.py").write_text("FAIL = 1\n", encoding="utf-8")
            return ok_response("provider_main", "STATUS: DONE\n")

        if req.role == AgentRole.PRE_REVIEWER:
            if req.task_id == "TG-F1":
                return ok_response("provider_reviewer_api", "VERDICT: REJECT\nBad design.\n")
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        if req.role == AgentRole.REVIEWER:
            if req.task_id and "," in req.task_id:
                return ok_response(
                    "provider_reviewer_api",
                    "VERDICT #TG-B1: APPROVE\nOK\nVERDICT #TG-B2: APPROVE\nOK\n",
                )
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

        raise AssertionError(f"Unexpected {req.role} {req.task_id}")

    with (
        mock.patch.dict(os.environ, {"TG_TEST_TOKEN": "tok", "TG_TEST_CHAT": "123"}),
        mock.patch("orchestrator.notifications.urllib.request.urlopen", side_effect=fake_urlopen),
    ):
        registry, _ = make_mock_registry(cfg, handler)
        runner = OrchestratorRunner(
            repo_root=temp_git_repo,
            config=cfg,
            store=store,
            registry=registry,
            sleep_fn=lambda _s: None,
            quiet=True,
        )
        assert runner.run() == RunStatus.COMPLETED

    assert any("[Pipeline Started" in m for m in sent_messages)
    assert any("[Rate Limit Pause" in m for m in sent_messages)
    assert any("[Escalation to Architect #TG-M1" in m for m in sent_messages)
    assert any("[Task Done #TG-M1" in m for m in sent_messages)
    # Consolidated batch summary (1 notification for both TG-B1 & TG-B2, not 2 separate pushes)
    batch_msgs = [m for m in sent_messages if "[Batch Review Done" in m]
    assert len(batch_msgs) == 1
    assert "#TG-B1" in batch_msgs[0] and "#TG-B2" in batch_msgs[0]
    assert any("[Task Failed #TG-F1" in m and "#TG-DEP" in m for m in sent_messages)
    assert any("[Pipeline Completed" in m for m in sent_messages)

    # Scenario B: fatal_error notification + verify Telegram timeout does NOT break a clean run
    sent_messages.clear()
    (temp_git_repo / "src/features/alpha/dirty_manual.py").write_text("WIP\n", encoding="utf-8")
    with (
        mock.patch.dict(os.environ, {"TG_TEST_TOKEN": "tok", "TG_TEST_CHAT": "123"}),
        mock.patch("orchestrator.notifications.urllib.request.urlopen", side_effect=fake_urlopen),
    ):
        runner_fatal = OrchestratorRunner(
            repo_root=temp_git_repo,
            config=cfg,
            store=store,
            registry=registry,
            quiet=True,
        )
        with pytest.raises(OrchestratorFatalError):
            runner_fatal.run()
    assert any("[Orchestrator Fatal Error" in m for m in sent_messages)

    # Clean up dirty file and verify pipeline STILL completes when Telegram raises TimeoutError
    (temp_git_repo / "src/features/alpha/dirty_manual.py").unlink()
    with (
        mock.patch.dict(os.environ, {"TG_TEST_TOKEN": "tok", "TG_TEST_CHAT": "123"}),
        mock.patch(
            "orchestrator.notifications.urllib.request.urlopen",
            side_effect=TimeoutError("telegram unreachable"),
        ),
    ):
        runner_resilient = OrchestratorRunner(
            repo_root=temp_git_repo,
            config=cfg,
            store=store,
            registry=registry,
            quiet=True,
        )
        assert runner_resilient.run() == RunStatus.COMPLETED
