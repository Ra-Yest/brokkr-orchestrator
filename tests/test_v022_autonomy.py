"""Tests for AFK Orchestrator v0.2.2 autonomy features:
1. Queue cooldown bypass & earliest-cooldown hibernation.
2. Review prompt diff trimming (--stat, pattern exclusions, per-file line cap, total char cap).
3. Smart idle_timeout with process-tree CPU activity tracking.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestrator import __version__
from orchestrator.config import (
    DEFAULT_REVIEW_DIFF_EXCLUDE_PATTERNS,
    ProviderProfileConfig,
    WorkflowConfig,
    load_pipeline_config,
)
from orchestrator.control import enqueue_control_command
from orchestrator.events import EventBus, format_console_event
from orchestrator.models import (
    AgentRole,
    ControlAction,
    EventType,
    PipelineEvent,
    RunStatus,
    Task,
    TaskComplexity,
    TaskStatus,
)
from orchestrator.process_utils import run_noninteractive_process
from orchestrator.prompt_builder import PromptBuilder, prepare_review_diff
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.providers.cli_provider import CliAgentProvider
from orchestrator.runner import OrchestratorRunner
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


def _make_diff(path: str, added_lines: list[str]) -> str:
    body = [f"+{ln}" for ln in added_lines]
    return (
        f"diff --git a/{path} b/{path}\n"
        f"--- /dev/null\n"
        f"+++ b/{path}\n"
        f"@@ -0,0 +1,{len(added_lines)} @@\n"
        + "\n".join(body)
        + "\n"
    )


# ===========================================================================
# 1. Queue Cooldown Bypass & Earliest-Cooldown Hibernation
# ===========================================================================


def test_queue_skips_cooled_down_medium_task_and_runs_easy_task_without_hibernation(
    temp_git_repo: Path,
) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks = [
        Task(
            id="MED-1",
            title="Medium task on cooled-down provider_main",
            description="Requires provider_main",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="EASY-1",
            title="Easy task on free provider_fast",
            description="Can run immediately on provider_fast",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"

    # Pre-seed provider_main in cooldown (600s), while provider_fast is completely free
    bus = EventBus(pdir, cfg)
    bus.quota_tracker.record_rate_limit("provider_main", cooldown_seconds=600)
    bus.save_state()

    claimed_order: list[str] = []
    hibernated = {"count": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY and req.task_id == "EASY-1":
            claimed_order.append("EASY-1")
            (temp_git_repo / "src/features/beta/easy.py").write_text("EASY = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nEasy done\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.WORKER_MEDIUM and req.task_id == "MED-1":
            claimed_order.append("MED-1")
            (temp_git_repo / "src/features/alpha/med.py").write_text("MED = 1\n", encoding="utf-8")
            return ok_response("provider_main", "BEGIN_REPORT\nMed done\nEND_REPORT\nSTATUS: DONE\n")
        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")
        raise AssertionError(f"Unexpected call: {req.role} {req.task_id}")

    def sleep_on_hibernation(_sec: float) -> None:
        # Should ONLY be called after EASY-1 finishes and only MED-1 remains!
        assert store.get_task("EASY-1").status == TaskStatus.DONE
        hibernated["count"] += 1
        enqueue_control_command(pdir, action=ControlAction.RESUME, reason="Wake for MED-1")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        sleep_fn=sleep_on_hibernation,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED

    # EASY-1 ran FIRST without hibernating; MED-1 ran only after wake-up
    assert claimed_order == ["EASY-1", "MED-1"]
    assert store.get_task("EASY-1").status == TaskStatus.DONE
    assert store.get_task("MED-1").status == TaskStatus.DONE
    assert hibernated["count"] >= 1

    events = _read_events(temp_git_repo)
    deferred_events = [
        e for e in events if e["event_type"] == EventType.TASK_DEFERRED_COOLDOWN.value
    ]
    assert len(deferred_events) == 1
    assert deferred_events[0]["task_id"] == "MED-1"
    assert deferred_events[0]["payload"]["selected_task_id"] == "EASY-1"
    assert "provider_main" in deferred_events[0]["payload"]["providers"]

    # Console formatter produces a clear human-readable explanation
    formatted = format_console_event(PipelineEvent.model_validate(deferred_events[0]))
    assert formatted is not None
    assert "task_deferred_cooldown" in formatted
    assert "#MED-1" in formatted
    assert "#EASY-1" in formatted


def test_queue_live_429_on_medium_defers_to_easy_and_both_cooldown_hibernates_until_earliest(
    temp_git_repo: Path,
) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks = [
        Task(
            id="MED-LIVE",
            title="Medium hits 429 live",
            description="Hits 429 on first attempt",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="EASY-LIVE",
            title="Easy runs while medium is in cooldown",
            description="Free provider_fast",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path = write_tasks_file(temp_git_repo, tasks)
    store = LocalJsonTaskStore(tasks_path)
    pdir = temp_git_repo / ".pipeline"

    med_calls = {"n": 0}
    completed_order: list[str] = []

    def handler_live(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM and req.task_id == "MED-LIVE":
            med_calls["n"] += 1
            if med_calls["n"] == 1:
                return ProviderResponse(
                    provider_name="provider_main",
                    exit_code=429,
                    raw_stderr="429 Too Many Requests",
                    rate_limited=True,
                    retry_after_seconds=600,
                )
            (temp_git_repo / "src/features/alpha/ml.py").write_text("M = 1\n", encoding="utf-8")
            completed_order.append("MED-LIVE")
            return ok_response("provider_main", "STATUS: DONE\n")
        if req.role == AgentRole.WORKER_EASY and req.task_id == "EASY-LIVE":
            (temp_git_repo / "src/features/beta/el.py").write_text("E = 1\n", encoding="utf-8")
            completed_order.append("EASY-LIVE")
            return ok_response("provider_fast", "STATUS: DONE\n")
        return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

    def sleep_resume(_sec: float) -> None:
        assert store.get_task("EASY-LIVE").status == TaskStatus.DONE
        enqueue_control_command(pdir, action=ControlAction.RESUME, reason="Wake MED-LIVE")

    reg, _ = make_mock_registry(cfg, handler_live)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        sleep_fn=sleep_resume,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert completed_order == ["EASY-LIVE", "MED-LIVE"]

    # Part B: Both medium (600s) and easy (120s) in cooldown -> hibernates until earliest (~120s)
    tasks2 = [
        Task(
            id="BOTH-MED",
            title="Medium 600s cooldown",
            description="Long cooldown",
            complexity=TaskComplexity.MEDIUM,
            allowed_paths=["src/features/alpha/**"],
        ),
        Task(
            id="BOTH-EASY",
            title="Easy 120s cooldown",
            description="Shorter cooldown",
            complexity=TaskComplexity.EASY,
            allowed_paths=["src/features/beta/**"],
        ),
    ]
    tasks_path2 = write_tasks_file(temp_git_repo, tasks2)
    store2 = LocalJsonTaskStore(tasks_path2)

    bus2 = EventBus(pdir, cfg)
    now = datetime.now(UTC)
    bus2.quota_tracker.record_rate_limit("provider_main", cooldown_seconds=600, now=now)
    bus2.quota_tracker.record_rate_limit("provider_fast", cooldown_seconds=120, now=now)
    bus2.save_state()

    captured_hibernation: dict[str, Any] = {}

    def sleep_inspect_and_abort(_sec: float) -> None:
        st = json.loads((pdir / "state.json").read_text(encoding="utf-8"))
        assert st["status"] == RunStatus.HIBERNATING.value
        captured_hibernation["resume_at"] = st["resume_at"]
        captured_hibernation["active_task_id"] = st["active_task_id"]
        enqueue_control_command(pdir, action=ControlAction.ABORT, reason="Inspect done")

    reg2, _ = make_mock_registry(cfg, lambda _req: ok_response("provider_fast", "STATUS: DONE\n"))
    runner2 = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store2,
        registry=reg2,
        sleep_fn=sleep_inspect_and_abort,
        quiet=True,
    )
    assert runner2.run() == RunStatus.ABORTED

    assert captured_hibernation["active_task_id"] == "BOTH-EASY"
    resume_dt = datetime.fromisoformat(captured_hibernation["resume_at"].replace("Z", "+00:00"))
    delta_from_now = (resume_dt - now).total_seconds()
    # Must target the ~120s earliest cooldown, definitely NOT the 600s cooldown!
    assert 100 <= delta_from_now <= 140


# ===========================================================================
# 2. Review Diff Trimming (--stat, excludes, per-file line cap, total chars)
# ===========================================================================


def test_review_diff_trimming_excludes_lock_and_gen_truncates_long_files_and_caps_chars(
    temp_git_repo: Path,
) -> None:
    lock_diff = _make_diff("app/pubspec.lock", [f"  pkg_{i}: 1.0.0" for i in range(50)])
    gen_diff = _make_diff("lib/models/user_gen.dart", [f"// generated line {i}" for i in range(50)])
    long_code_diff = _make_diff("src/features/alpha/long_mod.py", [f"VAL_{i} = {i}" for i in range(60)])
    small_code_diff = _make_diff("src/features/alpha/small.py", ["X = 1", "Y = 2"])
    raw_diff = lock_diff + gen_diff + long_code_diff + small_code_diff
    diff_stat = (
        " app/pubspec.lock              | 50 ++++++++++++++++++++++++++\n"
        " lib/models/user_gen.dart      | 50 ++++++++++++++++++++++++++\n"
        " src/features/alpha/long_mod.py| 60 ++++++++++++++++++++++++++++++++\n"
        " src/features/alpha/small.py   |  2 ++\n"
        " 4 files changed, 162 insertions(+)\n"
    )

    # A. Unit check: exclusions + per-file line truncation
    prepared = prepare_review_diff(
        raw_diff,
        diff_stat=diff_stat,
        exclude_patterns=list(DEFAULT_REVIEW_DIFF_EXCLUDE_PATTERNS),
        max_file_lines=20,
        max_chars=80000,
    )
    assert "app/pubspec.lock" in prepared.diff_stat
    assert "lib/models/user_gen.dart" in prepared.diff_stat
    assert "pkg_0" not in prepared.body
    assert "generated line 0" not in prepared.body
    assert "... [truncated " in prepared.body
    assert "VAL_59" not in prepared.body
    assert "X = 1" in prepared.body

    notes_map = dict(prepared.excluded_or_truncated)
    assert notes_map["app/pubspec.lock"] == "excluded by pattern"
    assert notes_map["lib/models/user_gen.dart"] == "excluded by pattern"
    assert "truncated" in notes_map["src/features/alpha/long_mod.py"]

    # B. Total character cap strictly enforced
    prepared_capped = prepare_review_diff(
        raw_diff,
        diff_stat=diff_stat,
        exclude_patterns=list(DEFAULT_REVIEW_DIFF_EXCLUDE_PATTERNS),
        max_file_lines=400,
        max_chars=300,
    )
    assert len(prepared_capped.body) <= 300
    assert prepared_capped.truncated_total is True

    # C. PromptBuilder injects --stat and exclusion/truncation list ONLY for review roles
    cfg = make_test_config().model_copy(
        update={
            "workflow": WorkflowConfig(
                review_diff_max_file_lines=20,
                review_diff_max_chars=5000,
            )
        }
    )
    builder = PromptBuilder(temp_git_repo, cfg)
    rev_prompt = builder.build(
        role=AgentRole.REVIEWER,
        profile=cfg.providers["provider_reviewer_api"],
        repo_map_text="# REPO_MAP\n",
        progress_text="",
        filtered_diff=raw_diff,
        diff_stat=diff_stat,
    )
    assert "## GIT DIFF STAT" in rev_prompt.full_text
    assert "## REVIEW DIFF EXCLUSIONS & TRUNCATIONS" in rev_prompt.full_text
    assert "`app/pubspec.lock`: excluded by pattern" in rev_prompt.full_text
    assert "`lib/models/user_gen.dart`: excluded by pattern" in rev_prompt.full_text
    assert "pkg_0" not in rev_prompt.full_text
    assert "generated line 0" not in rev_prompt.full_text
    assert "... [truncated " in rev_prompt.full_text

    # --stat appears BEFORE the filtered diff body
    assert rev_prompt.full_text.index("## GIT DIFF STAT") < rev_prompt.full_text.index(
        "## FILTERED GIT DIFF"
    )


def test_review_diff_trimming_in_runner_does_not_affect_git_commit_or_diff_guard(
    temp_git_repo: Path,
) -> None:
    base_cfg = make_test_config(batch_review_size=1)
    cfg = base_cfg.model_copy(
        update={
            "workflow": base_cfg.workflow.model_copy(
                update={
                    "review_diff_max_file_lines": 15,
                    "review_diff_max_chars": 4000,
                }
            )
        }
    )
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="REV-TRIM",
                title="Creates lock, generated dart, and normal python file",
                description="Verify review prompt trimming without affecting commit",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**", "pubspec.lock"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    review_prompts: dict[AgentRole, str] = {}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            (temp_git_repo / "pubspec.lock").write_text(
                "\n".join(f"lock_entry_{i}: 1.0" for i in range(40)) + "\n",
                encoding="utf-8",
            )
            (temp_git_repo / "src/features/alpha/model_gen.dart").write_text(
                "\n".join(f"// gen_dart_{i}" for i in range(40)) + "\n",
                encoding="utf-8",
            )
            (temp_git_repo / "src/features/alpha/service.py").write_text(
                "\n".join(f"LINE_{i} = {i}" for i in range(30)) + "\n",
                encoding="utf-8",
            )
            return ok_response("provider_main", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            review_prompts[req.role] = req.prompt
            return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nLooks good.\n")
        raise AssertionError(f"Unexpected {req.role}")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert store.get_task("REV-TRIM").status == TaskStatus.DONE

    # Both pre_reviewer and reviewer received trimmed prompts with --stat and exclusion list
    for role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
        prompt = review_prompts[role]
        assert "## GIT DIFF STAT" in prompt
        assert "pubspec.lock" in prompt
        assert "model_gen.dart" in prompt
        assert "excluded by pattern" in prompt
        assert "lock_entry_0" not in prompt
        assert "gen_dart_0" not in prompt
        assert "... [truncated " in prompt

    # All 3 files WERE committed cleanly to git (trimming only affected review prompts!)
    committed_files = git(temp_git_repo, "show", "--name-only", "--format=", "HEAD").splitlines()
    assert "pubspec.lock" in committed_files
    assert "src/features/alpha/model_gen.dart" in committed_files
    assert "src/features/alpha/service.py" in committed_files


# ===========================================================================
# 3. Smart idle_timeout with Process-Tree CPU Activity
# ===========================================================================


def test_smart_idle_timeout_keeps_silent_cpu_busy_process_alive_and_kills_sleeping_process(
    tmp_path: Path,
) -> None:
    # A. Silent CPU-busy process (no stdout, no file edits, burns CPU for ~1.6s > idle_timeout=0.7s)
    cpu_script = tmp_path / "silent_cpu_worker.py"
    cpu_script.write_text(
        "import time\n"
        "deadline = time.monotonic() + 1.6\n"
        "acc = 0\n"
        "while time.monotonic() < deadline:\n"
        "    for i in range(50000):\n"
        "        acc = (acc + i * i) % 1000003\n"
        "print(f'DONE:{acc}')\n",
        encoding="utf-8",
    )
    res_cpu = run_noninteractive_process(
        [sys.executable, str(cpu_script)],
        cwd=tmp_path,
        timeout_seconds=15,
        idle_timeout_seconds=0.7,
        workspace_watch_dir=tmp_path,
        idle_check_process_cpu=True,
    )
    assert res_cpu.exit_code == 0
    assert res_cpu.timed_out is False
    assert res_cpu.idle_timed_out is False
    assert "DONE:" in res_cpu.stdout

    # B. Same silent CPU-busy process with idle_check_process_cpu=False IS killed by idle_timeout
    res_cpu_disabled = run_noninteractive_process(
        [sys.executable, str(cpu_script)],
        cwd=tmp_path,
        timeout_seconds=15,
        idle_timeout_seconds=0.5,
        workspace_watch_dir=tmp_path,
        idle_check_process_cpu=False,
    )
    assert res_cpu_disabled.timed_out is True
    assert res_cpu_disabled.idle_timed_out is True

    # C. Pure sleeping process (no stdout, no file edits, ~0 CPU) IS killed with idle_timed_out=True
    sleep_script = tmp_path / "pure_sleep.py"
    sleep_script.write_text("import time\ntime.sleep(30)\n", encoding="utf-8")
    provider = CliAgentProvider(
        "p_sleep",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(sleep_script)],
            prompt_mode="stdin",
            timeout_seconds=30,
            idle_timeout_seconds=1,
            idle_check_process_cpu=True,
        ),
    )
    res_sleep = provider.invoke(
        ProviderRequest(
            run_id="r_v022",
            task_id="T-SLEEP",
            role=AgentRole.WORKER_EASY,
            prompt="hello",
            workdir=tmp_path,
            timeout_seconds=30,
            attempt_dir=tmp_path,
        )
    )
    assert res_sleep.timed_out is True
    assert res_sleep.idle_timed_out is True
    assert res_sleep.duration_seconds < 10.0


def test_v022_version_and_example_config_backward_compatibility() -> None:
    assert __version__ == "0.2.2"
    example_path = Path(__file__).resolve().parent.parent / "pipeline.example.yaml"
    cfg = load_pipeline_config(example_path)
    assert cfg.workflow.review_diff_max_file_lines == 400
    assert cfg.workflow.review_diff_max_chars == 80000
    assert cfg.workflow.idle_check_process_cpu is True
    assert "**/pubspec.lock" in cfg.workflow.review_diff_exclude_patterns
    assert "**/*_gen.dart" in cfg.workflow.review_diff_exclude_patterns
