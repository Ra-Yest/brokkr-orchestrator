"""Tests for AFK Orchestrator v0.3.0 quality, resilience, and guard enhancements."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from orchestrator import __version__
from orchestrator.config import (
    DEFAULT_CAPACITY_ERROR_PATTERNS,
    DEFAULT_PAIRED_PATHS,
    DEFAULT_STALL_STDOUT_PATTERNS,
    DEFAULT_TRANSIENT_ERROR_PATTERNS,
    GateConfig,
    ProviderProfileConfig,
    WorkflowConfig,
    load_pipeline_config,
)
from orchestrator.diff_guard import evaluate_diff
from orchestrator.gates import GateResult, GatesRunResult, format_gate_execution_summary
from orchestrator.git_ops import GitChange
from orchestrator.models import (
    AgentRole,
    EventType,
    ReviewVerdictType,
    RunStatus,
    Task,
    TaskComplexity,
    TaskStatus,
)
from orchestrator.output_parser import parse_single_review_verdict
from orchestrator.process_utils import run_noninteractive_process
from orchestrator.prompt_builder import PromptBuilder, prepare_review_diff
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.providers.cli_provider import CliAgentProvider
from orchestrator.runner import OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import (
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
# 1. Capacity (503) cooldown + auto-fallback & Transient (502) transport_retry
# ===========================================================================


def test_503_capacity_error_triggers_short_cooldown_and_fallback_without_burning_attempts(
    temp_git_repo: Path,
) -> None:
    base_cfg = make_test_config(batch_review_size=1, max_transport_retries=3)
    cfg = base_cfg.model_copy(
        update={
            "fallback_providers": {AgentRole.WORKER_MEDIUM: ["provider_master"]},
            "workflow": base_cfg.workflow.model_copy(
                update={"capacity_cooldown_seconds": 75}
            ),
        }
    )
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="CAP-1",
                title="Capacity fallback test",
                description="Primary hits 503 No capacity available, fallback completes task",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    invoked_providers: list[str] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            # First call goes to provider_main, second to fallback provider_master
            if len(invoked_providers) == 0:
                invoked_providers.append("provider_main")
                return ProviderResponse(
                    provider_name="provider_main",
                    exit_code=1,
                    raw_stderr="HTTP 503 UNAVAILABLE: No capacity available for model",
                    duration_seconds=1.2,
                )
            invoked_providers.append("provider_master")
            (temp_git_repo / "src/features/alpha/cap.py").write_text("CAP = 1\n", encoding="utf-8")
            return ok_response(
                "provider_master",
                "BEGIN_REPORT\nDone via fallback\nEND_REPORT\nSTATUS: DONE\n",
            )
        return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nLooks good.\n")

    reg, _ = make_mock_registry(cfg, handler)
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
    assert invoked_providers == ["provider_main", "provider_master"]

    task_done = store.get_task("CAP-1")
    assert task_done.status == TaskStatus.DONE
    # Fallback switch did NOT burn an extra task attempt!
    assert task_done.attempts == 1

    # Primary provider was placed on short cooldown (75s)
    assert runner.event_bus.quota_tracker.seconds_until_available("provider_main") > 0

    events = _read_events(temp_git_repo)
    assert any(
        e["event_type"] == EventType.PROVIDER_RATE_LIMITED.value
        and e["provider"] == "provider_main"
        and e["payload"].get("reason") == "capacity_error"
        and e["payload"].get("cooldown_seconds") == 75
        for e in events
    )
    assert any(
        e["event_type"] == EventType.PROVIDER_FALLBACK_USED.value
        and e["payload"].get("from_provider") == "provider_main"
        and e["payload"].get("to_provider") == "provider_master"
        and e["payload"].get("reason") == "capacity_error"
        for e in events
    )
    # Must NOT waste transport retries on 503 capacity error when fallback is available
    assert not any(e["event_type"] == EventType.PROVIDER_TRANSPORT_RETRY.value for e in events)


def test_502_bad_gateway_triggers_transport_retry_and_falls_back_when_exhausted(
    temp_git_repo: Path,
) -> None:
    base_cfg = make_test_config(
        batch_review_size=1,
        max_transport_retries=2,
        transport_retry_delays_seconds=[1, 2],
    )
    cfg = base_cfg.model_copy(
        update={
            "fallback_providers": {AgentRole.WORKER_EASY: ["provider_master"]},
        }
    )
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="TR-502",
                title="502 Bad Gateway retry then fallback",
                description="Primary returns 502 repeatedly, then fallback succeeds",
                complexity=TaskComplexity.EASY,
                allowed_paths=["src/features/alpha/**"],
            )
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    primary_calls = {"n": 0}
    fallback_calls = {"n": 0}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_EASY:
            # 1 initial + 2 transport retries on provider_fast, then fallback to provider_master
            if primary_calls["n"] < 3:
                primary_calls["n"] += 1
                return ProviderResponse(
                    provider_name="provider_fast",
                    exit_code=1,
                    raw_stderr="Error: 502 Bad Gateway",
                    duration_seconds=0.8,
                )
            fallback_calls["n"] += 1
            (temp_git_repo / "src/features/alpha/gw.py").write_text("GW = 1\n", encoding="utf-8")
            return ok_response(
                "provider_master",
                "BEGIN_REPORT\nRecovered via fallback\nEND_REPORT\nSTATUS: DONE\n",
            )
        return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

    reg, _ = make_mock_registry(cfg, handler)
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
    assert primary_calls["n"] == 3
    assert fallback_calls["n"] == 1

    done_task = store.get_task("TR-502")
    assert done_task.status == TaskStatus.DONE
    assert done_task.attempts == 1

    events = _read_events(temp_git_repo)
    tr_events = [e for e in events if e["event_type"] == EventType.PROVIDER_TRANSPORT_RETRY.value]
    assert len(tr_events) == 2
    assert any(
        e["event_type"] == EventType.PROVIDER_FALLBACK_USED.value
        and e["payload"].get("from_provider") == "provider_fast"
        and e["payload"].get("to_provider") == "provider_master"
        and e["payload"].get("reason") == "transport_retry_exhausted"
        for e in events
    )


# ===========================================================================
# 2. Stalled-agent stdout/stderr detector
# ===========================================================================


def test_stalled_agent_pattern_kills_process_after_grace_and_allows_active_workspace_edits(
    tmp_path: Path,
) -> None:
    # A. Process prints stall message and hangs without touching files -> killed after grace
    stall_script = tmp_path / "stalled_agent.py"
    stall_script.write_text(
        "import sys, time\n"
        "sys.stdout.write('root agent idle; waiting for 1 background task(s) to finish\\n')\n"
        "sys.stdout.flush()\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    res_stalled = run_noninteractive_process(
        [sys.executable, str(stall_script)],
        cwd=tmp_path,
        timeout_seconds=30,
        idle_timeout_seconds=None,
        workspace_watch_dir=tmp_path,
        stall_stdout_patterns=list(DEFAULT_STALL_STDOUT_PATTERNS),
        stall_grace_seconds=0.5,
    )
    assert res_stalled.timed_out is True
    assert res_stalled.idle_timed_out is True
    assert res_stalled.stall_detected is True
    assert res_stalled.stall_reason is not None
    assert "Stalled agent detected" in res_stalled.stall_reason
    assert res_stalled.duration_seconds < 10.0

    # B. Via CliAgentProvider, stall metadata is propagated to ProviderResponse
    provider = CliAgentProvider(
        "p_stall",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(stall_script)],
            prompt_mode="stdin",
            timeout_seconds=30,
            stall_stdout_patterns=list(DEFAULT_STALL_STDOUT_PATTERNS),
            stall_grace_seconds=1,
        ),
    )
    prov_res = provider.invoke(
        ProviderRequest(
            run_id="r_v030",
            task_id="T-STALL",
            role=AgentRole.WORKER_EASY,
            prompt="hello",
            workdir=tmp_path,
            timeout_seconds=30,
            attempt_dir=tmp_path,
        )
    )
    assert prov_res.timed_out is True
    assert prov_res.idle_timed_out is True
    assert prov_res.metadata.get("stall_detected") is True
    assert "Stalled agent detected" in str(prov_res.metadata.get("stall_reason", ""))

    # C. Process prints stall message BUT edits a workspace file within grace window and exits -> NOT killed
    recover_script = tmp_path / "recover_agent.py"
    marker_file = tmp_path / "touched.txt"
    recover_script.write_text(
        "import pathlib, sys, time\n"
        "sys.stdout.write('waiting for 1 background task to finish\\n')\n"
        "sys.stdout.flush()\n"
        "time.sleep(0.25)\n"
        f"pathlib.Path({str(marker_file)!r}).write_text('updated', encoding='utf-8')\n"
        "time.sleep(0.45)\n"
        "sys.stdout.write('STATUS: DONE\\n')\n"
        "sys.stdout.flush()\n",
        encoding="utf-8",
    )
    res_ok = run_noninteractive_process(
        [sys.executable, str(recover_script)],
        cwd=tmp_path,
        timeout_seconds=15,
        idle_timeout_seconds=None,
        workspace_watch_dir=tmp_path,
        stall_stdout_patterns=list(DEFAULT_STALL_STDOUT_PATTERNS),
        stall_grace_seconds=0.5,
    )
    assert res_ok.exit_code == 0
    assert res_ok.timed_out is False
    assert res_ok.idle_timed_out is False
    assert res_ok.stall_detected is False
    assert "STATUS: DONE" in res_ok.stdout


# ===========================================================================
# 3. Configurable paired_paths for lock files in diff_guard
# ===========================================================================


def test_diff_guard_paired_paths_allows_same_dir_lock_and_blocks_foreign_or_red_protected() -> None:
    cfg = make_test_config()
    assert cfg.diff_guard.paired_paths == DEFAULT_PAIRED_PATHS

    # A. app/pubspec.yaml in allowed_paths -> app/pubspec.lock automatically allowed!
    rep_ok = evaluate_diff(
        role=AgentRole.WORKER_MEDIUM,
        changes=[
            GitChange(status="M", path="app/pubspec.yaml"),
            GitChange(status="M", path="app/pubspec.lock"),
        ],
        raw_diff=(
            _make_diff("app/pubspec.yaml", ["  http: ^1.2.0"])
            + _make_diff("app/pubspec.lock", ["  http: 1.2.0"])
        ),
        config=cfg,
        task_complexity=TaskComplexity.MEDIUM,
        allowed_paths=["app/pubspec.yaml"],
    )
    assert rep_ok.passed is True
    assert rep_ok.scope_violations == []

    # B. Lock file in a DIFFERENT directory (other/pubspec.lock) is still blocked!
    rep_other_dir = evaluate_diff(
        role=AgentRole.WORKER_MEDIUM,
        changes=[GitChange(status="M", path="other/pubspec.lock")],
        raw_diff=_make_diff("other/pubspec.lock", ["  http: 1.2.0"]),
        config=cfg,
        task_complexity=TaskComplexity.MEDIUM,
        allowed_paths=["app/pubspec.yaml"],
    )
    assert rep_other_dir.passed is False
    assert any(v.code == "OUT_OF_ALLOWED_PATHS" for v in rep_other_dir.scope_violations)

    # C. Unrelated file in the same directory is still blocked!
    rep_unrelated = evaluate_diff(
        role=AgentRole.WORKER_MEDIUM,
        changes=[GitChange(status="M", path="app/secret.dart")],
        raw_diff=_make_diff("app/secret.dart", ["final x = 1;"]),
        config=cfg,
        task_complexity=TaskComplexity.MEDIUM,
        allowed_paths=["app/pubspec.yaml"],
    )
    assert rep_unrelated.passed is False
    assert any(v.code == "OUT_OF_ALLOWED_PATHS" for v in rep_unrelated.scope_violations)

    # D. Red-protected lock file MUST STILL be blocked even if paired!
    cfg_red_lock = cfg.model_copy(
        update={
            "zones": cfg.zones.model_copy(
                update={"red_protected": [*cfg.zones.red_protected, "app/pubspec.lock"]}
            )
        }
    )
    rep_red = evaluate_diff(
        role=AgentRole.WORKER_MEDIUM,
        changes=[GitChange(status="M", path="app/pubspec.lock")],
        raw_diff=_make_diff("app/pubspec.lock", ["  http: 1.2.0"]),
        config=cfg_red_lock,
        task_complexity=TaskComplexity.MEDIUM,
        allowed_paths=["app/pubspec.yaml"],
    )
    assert rep_red.passed is False
    assert any(v.code == "RED_ZONE_PROTECTED_FILE" for v in rep_red.red_violations)


# ===========================================================================
# 4. PromptBuilder: diff priority, DOWNSTREAM CONSUMERS, GATE SUMMARY, context budget
# ===========================================================================


def test_prepare_review_diff_prioritizes_yellow_and_production_before_tests_and_fixtures() -> None:
    # Alphabetically, `a_fixtures/huge.yaml` and `b_tests/test_x.py` would come BEFORE
    # `src/features/alpha/core.py` and `src/shared/schema.py`!
    fixture_diff = _make_diff("a_fixtures/huge.yaml", [f"fixture_item_{i}: value" for i in range(40)])
    docs_diff = _make_diff("docs/guide.md", [f"Doc line {i}" for i in range(30)])
    test_diff = _make_diff("tests/test_core.py", [f"def test_case_{i}(): assert True" for i in range(30)])
    prod_diff = _make_diff("src/features/alpha/core.py", ["def compute(x: int) -> int:", "    return x * 2"])
    yellow_diff = _make_diff("src/shared/schema.py", ["CONTRACT_VERSION = 2"])

    raw_diff = fixture_diff + docs_diff + test_diff + prod_diff + yellow_diff

    # Tight budget that fits P1 (yellow) + P2 (prod) but must truncate P3 (tests) & P4 (fixtures/docs)
    prepared = prepare_review_diff(
        raw_diff,
        max_file_lines=400,
        max_chars=520,
        yellow_patterns=["src/shared/**"],
        test_patterns=["tests/**"],
    )
    assert prepared.truncated_total is True
    assert "CONTRACT_VERSION = 2" in prepared.body
    assert "def compute(x: int) -> int:" in prepared.body
    assert "fixture_item_0" not in prepared.body
    assert "Doc line 0" not in prepared.body
    # P1 (yellow) appears before P2 (prod)
    assert prepared.body.index("src/shared/schema.py") < prepared.body.index(
        "src/features/alpha/core.py"
    )


def test_prompt_builder_includes_downstream_consumers_gate_summary_and_truncates_context_files(
    temp_git_repo: Path,
) -> None:
    # Create two large context files exceeding per-file and total budgets
    (temp_git_repo / "src/features/alpha/ctx_a.py").write_text("A" * 500 + "\n", encoding="utf-8")
    (temp_git_repo / "src/features/alpha/ctx_b.py").write_text("B" * 500 + "\n", encoding="utf-8")

    base_cfg = make_test_config()
    cfg = base_cfg.model_copy(
        update={
            "workflow": base_cfg.workflow.model_copy(
                update={
                    "context_file_max_chars": 200,
                    "context_files_total_max_chars": 300,
                }
            )
        }
    )
    builder = PromptBuilder(temp_git_repo, cfg)
    task = Task(
        id="T-10",
        title="Core storage API",
        description="Implement storage layer",
        complexity=TaskComplexity.MEDIUM,
        allowed_paths=["src/features/alpha/**"],
        context_files=[
            "src/features/alpha/ctx_a.py",
            "src/features/alpha/ctx_b.py",
        ],
    )
    downstream = [
        Task(
            id="T-11",
            title="REST handler consuming storage API",
            description="Exposes GET /items",
            complexity=TaskComplexity.EASY,
            depends_on=["T-10"],
            allowed_paths=["src/features/beta/**"],
            acceptance_criteria=[
                "Calls Storage.get_item(id) and returns 404 on KeyError",
                "Preserves stable string IDs",
            ],
        )
    ]
    gates_res = GatesRunResult(
        passed=True,
        results=[
            GateResult(
                name="unit_tests",
                kind="command",
                command=["pytest", "-q"],
                exit_code=0,
                passed=True,
                optional=False,
                duration_seconds=1.4,
                timed_out=False,
                raw_output="5 passed",
                trimmed_output="",
            )
        ],
        combined_trimmed_log="",
        screenshots=[],
    )
    gate_summary = format_gate_execution_summary(
        gates_res,
        configured_gates=[
            GateConfig(name="unit_tests", command=["pytest", "-q"]),
            GateConfig(name="optional_e2e", command=["pytest", "-m", "e2e"], optional=True),
        ],
    )

    built = builder.build(
        role=AgentRole.PRE_REVIEWER,
        profile=cfg.providers["provider_reviewer_api"],
        repo_map_text="# REPO_MAP\n",
        progress_text="",
        task=task,
        downstream_tasks=downstream,
        gate_summary=gate_summary,
        last_report="Implemented storage.",
    )
    assert "## DOWNSTREAM CONSUMERS (DO NOT BREAK CONTRACT)" in built.full_text
    assert "#T-11: REST handler consuming storage API" in built.full_text
    assert "src/features/beta/**" in built.full_text
    assert "Calls Storage.get_item(id)" in built.full_text

    assert "## GATE EXECUTION SUMMARY" in built.full_text
    assert "`unit_tests` (`pytest -q`): **PASS** (exit=0, 1.4s)" in built.full_text
    assert "`optional_e2e` (`pytest -m e2e`): **SKIP** (not executed)" in built.full_text

    # Per-file (200) and total (300) context_files budgets enforced with explicit marker
    assert "[truncated 301 chars; read full file if needed]" in built.full_text
    assert "[truncated 401 chars; read full file if needed]" in built.full_text
    assert "A" * 201 not in built.full_text
    assert "B" * 101 not in built.full_text


def test_runner_injects_downstream_consumers_and_gate_summary_into_worker_and_review_prompts(
    temp_git_repo: Path,
) -> None:
    gate_cmd = [sys.executable, "-c", "print('ok')"]
    cfg = make_test_config(
        batch_review_size=1,
        gates=[GateConfig(name="smoke", command=gate_cmd)],
    )
    tasks_path = write_tasks_file(
        temp_git_repo,
        [
            Task(
                id="UP-1",
                title="Upstream API",
                description="Produce upstream helper",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
            ),
            Task(
                id="DOWN-1",
                title="Downstream consumer",
                description="Consumes upstream helper",
                complexity=TaskComplexity.EASY,
                depends_on=["UP-1"],
                allowed_paths=["src/features/beta/**"],
                acceptance_criteria=["Imports helper() from alpha"],
            ),
        ],
    )
    store = LocalJsonTaskStore(tasks_path)
    seen_prompts: dict[tuple[str, AgentRole], str] = {}

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.task_id is not None:
            seen_prompts[(req.task_id, req.role)] = req.prompt
        if req.role == AgentRole.WORKER_MEDIUM:
            (temp_git_repo / "src/features/alpha/up.py").write_text(
                "def helper() -> int:\n    return 1\n", encoding="utf-8"
            )
            return ok_response("provider_main", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.WORKER_EASY:
            (temp_git_repo / "src/features/beta/down.py").write_text("X = 1\n", encoding="utf-8")
            return ok_response("provider_fast", "BEGIN_REPORT\nDone\nEND_REPORT\nSTATUS: DONE\n")
        return ok_response("provider_reviewer_api", "VERDICT: APPROVE\nOK\n")

    reg, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=reg,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED

    # UP-1 worker, pre_reviewer, and reviewer all see DOWN-1 in DOWNSTREAM CONSUMERS
    for role in (AgentRole.WORKER_MEDIUM, AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
        prompt = seen_prompts[("UP-1", role)]
        assert "## DOWNSTREAM CONSUMERS (DO NOT BREAK CONTRACT)" in prompt
        assert "#DOWN-1: Downstream consumer" in prompt

    # Both pre_reviewer and reviewer see real GATE EXECUTION SUMMARY
    for role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
        prompt = seen_prompts[("UP-1", role)]
        assert "## GATE EXECUTION SUMMARY" in prompt
        assert "`smoke`" in prompt
        assert "**PASS**" in prompt


# ===========================================================================
# 5. Role templates, version 0.3.0, and backward-compatible example config
# ===========================================================================


def test_role_templates_and_v030_config_defaults() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    templates_dir = repo_root / "templates" / ".agents"

    worker_md = (templates_dir / "worker.md").read_text(encoding="utf-8")
    assert "background" in worker_md.lower()
    assert "idempotency" in worker_md.lower()
    assert "negative test" in worker_md.lower()
    assert "DOWNSTREAM CONSUMERS" in worker_md

    pre_rev_md = (templates_dir / "pre_reviewer.md").read_text(encoding="utf-8")
    assert "4-Point Checklist" in pre_rev_md
    assert "DOWNSTREAM CONSUMERS" in pre_rev_md
    assert "GATE EXECUTION SUMMARY" in pre_rev_md
    assert "PASS|FAIL" in pre_rev_md

    reviewer_md = (templates_dir / "reviewer.md").read_text(encoding="utf-8")
    assert "Cross-Layer Contract & Failure Mode" in reviewer_md
    assert "DOWNSTREAM CONSUMERS" in reviewer_md
    assert "BEGIN_FOLLOWUPS" in reviewer_md
    assert "END_FOLLOWUPS" in reviewer_md

    # Verify reviewer output with optional BEGIN_FOLLOWUPS/END_FOLLOWUPS still parses cleanly
    sample_output = (
        "BEGIN_FEEDBACK\n- Clean implementation.\nEND_FEEDBACK\n\n"
        "BEGIN_FOLLOWUPS\n- Consider adding metrics in a follow-up.\nEND_FOLLOWUPS\n\n"
        "VERDICT: APPROVE\n"
    )
    parsed = parse_single_review_verdict(sample_output, "T-1")
    assert parsed.verdict == ReviewVerdictType.APPROVE

    # Version & example config defaults
    assert tuple(int(part) for part in __version__.split(".")[:3]) >= (0, 3, 0)
    cfg = load_pipeline_config(repo_root / "pipeline.example.yaml")
    assert cfg.workflow.capacity_error_patterns == DEFAULT_CAPACITY_ERROR_PATTERNS
    assert cfg.workflow.transient_error_patterns == DEFAULT_TRANSIENT_ERROR_PATTERNS
    assert cfg.workflow.stall_stdout_patterns == DEFAULT_STALL_STDOUT_PATTERNS
    assert cfg.workflow.stall_grace_seconds == 30
    assert cfg.workflow.context_file_max_chars == 12000
    assert cfg.workflow.context_files_total_max_chars == 40000
    assert cfg.diff_guard.paired_paths == DEFAULT_PAIRED_PATHS
    assert cfg.providers["provider_main"].idle_timeout_seconds == 300
    assert WorkflowConfig().capacity_cooldown_seconds == 90
