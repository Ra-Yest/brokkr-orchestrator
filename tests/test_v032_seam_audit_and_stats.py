"""Tests for AFK Orchestrator v0.3.2: Pairwise Seam Audit and Pipeline Telemetry (`orchestrator stats`)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from orchestrator import __version__
from orchestrator.cli import main as cli_main
from orchestrator.config import (
    PipelineConfig,
    ProjectConfig,
    ProviderProfileConfig,
    RoleMappingConfig,
)
from orchestrator.fs_utils import atomic_write_json
from orchestrator.models import (
    AgentRole,
    ContractClause,
    ContractKind,
    ContractSpec,
    EventType,
    PipelineState,
    RunStatus,
    Task,
    TaskComplexity,
    TasksFile,
    TaskStatus,
)
from orchestrator.notifications import BaseNotifier
from orchestrator.output_parser import parse_seam_audit_output
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.reporter import write_report
from orchestrator.runner import OrchestratorFatalError, OrchestratorRunner
from orchestrator.stats import compute_pipeline_stats
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import make_mock_registry, make_test_config, ok_response


class _RecordingNotifier(BaseNotifier):
    def __init__(self) -> None:
        self.events: list[tuple[str, str, str]] = []

    def send(self, title: str, message: str) -> bool:
        return True

    def notify(self, event: str, title: str, message: str) -> bool:
        self.events.append((event, title, message))
        return True


def _approve_with_c1() -> str:
    return (
        "BEGIN_FEEDBACK\nAll clauses verified.\nEND_FEEDBACK\n"
        "BEGIN_CONTRACT_CHECKS_JSON\n"
        '[{"clause_id": "INV-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:1"}]\n'
        "END_CONTRACT_CHECKS_JSON\n"
        "VERDICT: APPROVE\n"
    )


def _c1_contract() -> dict[str, ContractSpec]:
    return {
        "c1": ContractSpec(
            kind=ContractKind.WIRE,
            surface="src/features/alpha/mod.py:produce",
            clauses=[ContractClause(id="INV-1", rule="Returns positive int")],
        )
    }


# ===========================================================================
# 0. Version, config validation & output parser unit checks
# ===========================================================================


def test_version_and_seam_auditor_role_validation() -> None:
    parsed_version = tuple(int(part) for part in __version__.split(".")[:3])
    assert parsed_version >= (0, 3, 2)

    edit_profile = ProviderProfileConfig(kind="cli", can_edit_files=True, command=["echo"])
    ro_profile = ProviderProfileConfig(
        kind="api",
        can_edit_files=False,
        base_url="https://api.example.com/v1",
        model="mock-ro",
    )

    # Omitting seam_auditor defaults to reviewer (even if reviewer can_edit_files=True for backward compat)
    cfg_default = PipelineConfig(
        project=ProjectConfig(name="compat"),
        providers={"edit": edit_profile, "ro": ro_profile},
        roles=RoleMappingConfig(
            planner="edit",
            worker_easy="edit",
            worker_medium="edit",
            worker_hard="edit",
            pre_reviewer="ro",
            reviewer="edit",
            architect="edit",
            refactorer="edit",
        ),
    )
    assert cfg_default.roles.profile_for_role(AgentRole.SEAM_AUDITOR) == "edit"
    assert cfg_default.workflow.seam_audit_enabled is True
    assert cfg_default.workflow.seam_audit_pause_on_critical is True
    assert cfg_default.workflow.seam_audit_max_chars == 40000

    # Explicit seam_auditor with can_edit_files=False succeeds
    cfg_ro = PipelineConfig(
        project=ProjectConfig(name="explicit-ro"),
        providers={"edit": edit_profile, "ro": ro_profile},
        roles=RoleMappingConfig(
            planner="edit",
            worker_easy="edit",
            worker_medium="edit",
            worker_hard="edit",
            pre_reviewer="ro",
            reviewer="edit",
            seam_auditor="ro",
            architect="edit",
            refactorer="edit",
        ),
    )
    assert cfg_ro.roles.profile_for_role(AgentRole.SEAM_AUDITOR) == "ro"

    # Explicit seam_auditor with can_edit_files=True fails validation
    with pytest.raises(ValidationError, match="can_edit_files is True"):
        PipelineConfig(
            project=ProjectConfig(name="bad-seam"),
            providers={"edit": edit_profile, "ro": ro_profile},
            roles=RoleMappingConfig(
                planner="edit",
                worker_easy="edit",
                worker_medium="edit",
                worker_hard="edit",
                pre_reviewer="ro",
                reviewer="ro",
                seam_auditor="edit",
                architect="edit",
                refactorer="edit",
            ),
        )


def test_parse_seam_audit_output_fenced_and_invalid_fallback() -> None:
    fenced = (
        "Audit complete.\n"
        "BEGIN_SEAM_AUDIT_JSON\n"
        "```json\n"
        '{"verdict": "pass", "summary": "Aligned.", "findings": ['
        '{"severity": "info", "clause_id": "INV-1", "description": "OK", "file_refs": ["a.py:1"]}'
        "]}\n"
        "```\n"
        "END_SEAM_AUDIT_JSON\n"
    )
    parsed = parse_seam_audit_output(fenced)
    assert parsed.verdict == "PASS"
    assert parsed.summary == "Aligned."
    assert len(parsed.findings) == 1
    assert parsed.findings[0].severity == "INFO"

    broken = parse_seam_audit_output("BEGIN_SEAM_AUDIT_JSON\n{not valid json\nEND_SEAM_AUDIT_JSON")
    assert broken.verdict == "WARNING"
    assert broken.findings == []

    missing = parse_seam_audit_output("No markers here")
    assert missing.verdict == "WARNING"


# ===========================================================================
# 1. Seam Audit PASS end-to-end
# ===========================================================================


def test_seam_audit_pass_runs_after_consumer_commit_and_records_state(temp_git_repo: Path) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        contracts=_c1_contract(),
        tasks=[
            Task(
                id="A",
                title="Producer task",
                description="Produce c1",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
                provides=["c1"],
            ),
            Task(
                id="B",
                title="Consumer task",
                description="Consume c1",
                complexity=TaskComplexity.MEDIUM,
                depends_on=["A"],
                allowed_paths=["src/features/beta/**"],
                consumes=["c1"],
            ),
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    seam_prompts: list[str] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            if req.task_id == "A":
                (req.workdir / "src" / "features" / "alpha" / "mod.py").write_text(
                    "def produce() -> int:\n    return 42\n",
                    encoding="utf-8",
                )
            elif req.task_id == "B":
                (req.workdir / "src" / "features" / "beta" / "consumer.py").write_text(
                    "from src.features.alpha.mod import produce\n\ndef consume() -> int:\n    return produce()\n",
                    encoding="utf-8",
                )
            return ok_response("provider_main", "BEGIN_REPORT\nDone.\nEND_REPORT\nSTATUS: DONE\n")
        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            return ok_response("provider_reviewer_api", _approve_with_c1())
        if req.role == AgentRole.SEAM_AUDITOR:
            seam_prompts.append(req.prompt)
            return ok_response(
                "provider_reviewer_api",
                (
                    "BEGIN_SEAM_AUDIT_JSON\n"
                    '{"verdict": "PASS", "summary": "Producer A and consumer B align on c1.", "findings": []}\n'
                    "END_SEAM_AUDIT_JSON\n"
                ),
            )
        raise AssertionError(f"Unexpected role: {req.role}")

    store = LocalJsonTaskStore(tasks_path)
    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
    )
    status = runner.run()
    assert status == RunStatus.COMPLETED
    assert len(seam_prompts) == 1
    assert "c1" in seam_prompts[0]
    assert "produce" in seam_prompts[0]
    assert "consume" in seam_prompts[0]

    report_file = temp_git_repo / ".pipeline" / "seam_audits" / "c1__A__B.json"
    assert report_file.is_file()
    saved = json.loads(report_file.read_text(encoding="utf-8"))
    assert saved["verdict"] == "PASS"
    assert saved["seam_key"] == "c1:A:B"

    state = PipelineState.model_validate(
        json.loads((temp_git_repo / ".pipeline" / "state.json").read_text(encoding="utf-8"))
    )
    assert "c1:A:B" in state.audited_seams

    events = [
        json.loads(line)
        for line in (temp_git_repo / ".pipeline" / "events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    seam_events = [e for e in events if e["event_type"] == EventType.SEAM_AUDIT_COMPLETED.value]
    assert len(seam_events) == 1
    assert seam_events[0]["payload"]["verdict"] == "PASS"


# ===========================================================================
# 2. Seam Audit CRITICAL pauses pipeline and blocks task C
# ===========================================================================


def test_seam_audit_critical_pauses_pipeline_and_skips_downstream_task_c(
    temp_git_repo: Path,
) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        contracts=_c1_contract(),
        tasks=[
            Task(
                id="A",
                title="Producer task",
                description="Produce c1",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
                provides=["c1"],
            ),
            Task(
                id="B",
                title="Consumer task",
                description="Consume c1",
                complexity=TaskComplexity.MEDIUM,
                depends_on=["A"],
                allowed_paths=["src/features/beta/**"],
                consumes=["c1"],
            ),
            Task(
                id="C",
                title="Downstream task",
                description="Must not run if seam audit is CRITICAL",
                complexity=TaskComplexity.MEDIUM,
                depends_on=["B"],
                allowed_paths=["src/features/gamma/**"],
            ),
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    worker_task_ids: list[str | None] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            worker_task_ids.append(req.task_id)
            if req.task_id == "A":
                (req.workdir / "src" / "features" / "alpha" / "mod.py").write_text(
                    "def produce() -> int:\n    return 1\n",
                    encoding="utf-8",
                )
            elif req.task_id == "B":
                (req.workdir / "src" / "features" / "beta" / "consumer.py").write_text(
                    "def consume() -> str:\n    return 'wrong_type'\n",
                    encoding="utf-8",
                )
            return ok_response("provider_main", "BEGIN_REPORT\nDone.\nEND_REPORT\nSTATUS: DONE\n")
        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            return ok_response("provider_reviewer_api", _approve_with_c1())
        if req.role == AgentRole.SEAM_AUDITOR:
            return ok_response(
                "provider_reviewer_api",
                (
                    "BEGIN_SEAM_AUDIT_JSON\n"
                    '{"verdict": "CRITICAL", "summary": "Type mismatch on c1", "findings": ['
                    '{"severity": "CRITICAL", "clause_id": "INV-1", "description": "Expected int, got str", '
                    '"file_refs": ["src/features/beta/consumer.py:1"]}'
                    "]}\n"
                    "END_SEAM_AUDIT_JSON\n"
                ),
            )
        raise AssertionError(f"Unexpected role: {req.role}")

    notifier = _RecordingNotifier()
    store = LocalJsonTaskStore(tasks_path)
    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        notifier=notifier,
        quiet=True,
    )
    status = runner.run()
    assert status == RunStatus.PAUSED
    assert "C" not in worker_task_ids
    assert store.get_task("B").status == TaskStatus.DONE
    assert store.get_task("C").status == TaskStatus.TODO

    state = PipelineState.model_validate(
        json.loads((temp_git_repo / ".pipeline" / "state.json").read_text(encoding="utf-8"))
    )
    assert state.paused is True
    assert state.pause_reason == "seam_audit_critical"
    assert "c1:A:B" in state.audited_seams
    assert any(ev[0] == "pause" and "Seam Audit" in ev[1] for ev in notifier.events)


# ===========================================================================
# 3. Seam Audit read-only protection
# ===========================================================================


def test_seam_audit_readonly_violation_raises_fatal_error(temp_git_repo: Path) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        contracts=_c1_contract(),
        tasks=[
            Task(
                id="A",
                title="Producer task",
                description="Produce c1",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
                provides=["c1"],
            ),
            Task(
                id="B",
                title="Consumer task",
                description="Consume c1",
                complexity=TaskComplexity.MEDIUM,
                depends_on=["A"],
                allowed_paths=["src/features/beta/**"],
                consumes=["c1"],
            ),
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            if req.task_id == "A":
                (req.workdir / "src" / "features" / "alpha" / "mod.py").write_text(
                    "def produce() -> int:\n    return 5\n",
                    encoding="utf-8",
                )
            elif req.task_id == "B":
                (req.workdir / "src" / "features" / "beta" / "consumer.py").write_text(
                    "def consume() -> int:\n    return 5\n",
                    encoding="utf-8",
                )
            return ok_response("provider_main", "BEGIN_REPORT\nDone.\nEND_REPORT\nSTATUS: DONE\n")
        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            return ok_response("provider_reviewer_api", _approve_with_c1())
        if req.role == AgentRole.SEAM_AUDITOR:
            # Illegal file modification by read-only seam_auditor!
            (req.workdir / "src" / "features" / "beta" / "consumer.py").write_text(
                "# tampered by seam_auditor\n",
                encoding="utf-8",
            )
            return ok_response(
                "provider_reviewer_api",
                'BEGIN_SEAM_AUDIT_JSON\n{"verdict": "PASS", "summary": "ok", "findings": []}\nEND_SEAM_AUDIT_JSON\n',
            )
        raise AssertionError(f"Unexpected role: {req.role}")

    store = LocalJsonTaskStore(tasks_path)
    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
    )
    with pytest.raises(OrchestratorFatalError, match="seam_auditor"):
        runner.run()


# ===========================================================================
# 4. Backward compatibility: tasks without contracts do not trigger SEAM_AUDITOR
# ===========================================================================


def test_tasks_without_contracts_never_trigger_seam_auditor(temp_git_repo: Path) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        tasks=[
            Task(
                id="T-PLAIN",
                title="Plain task",
                description="No contracts",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
            )
        ]
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    seam_calls = 0

    def handler(req: ProviderRequest) -> ProviderResponse:
        nonlocal seam_calls
        if req.role == AgentRole.SEAM_AUDITOR:
            seam_calls += 1
        if req.role == AgentRole.WORKER_MEDIUM:
            (req.workdir / "src" / "features" / "alpha" / "mod.py").write_text(
                "def alpha() -> int:\n    return 99\n",
                encoding="utf-8",
            )
            return ok_response("provider_main", "BEGIN_REPORT\nDone.\nEND_REPORT\nSTATUS: DONE\n")
        return ok_response("provider_reviewer_api", "BEGIN_FEEDBACK\nLGTM\nEND_FEEDBACK\nVERDICT: APPROVE\n")

    store = LocalJsonTaskStore(tasks_path)
    registry, _ = make_mock_registry(cfg, handler)
    runner = OrchestratorRunner(
        repo_root=temp_git_repo,
        config=cfg,
        store=store,
        registry=registry,
        quiet=True,
    )
    assert runner.run() == RunStatus.COMPLETED
    assert seam_calls == 0


# ===========================================================================
# 5. stats.py, CLI `orchestrator stats`, and REPORT.md telemetry section
# ===========================================================================


def test_stats_cli_and_report_telemetry(
    temp_git_repo: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = temp_git_repo / ".pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    events_path = pipeline_dir / "events.jsonl"

    # Synthetic events.jsonl including a malformed line:
    # - 100s succeeded (60s + 40s)
    # - 20s timeout failure, 15s idle/stall failure, 15s transport retry (10s call + 5s delay), 50s rate limit
    # -> total = 200s, wasted = 100s => wasted_pct = 50.0%
    # - 2 pre_reviewer APPROVE (T-1, T-2) -> 1 reviewer REJECT (T-1), 1 reviewer APPROVE (T-2) => escape rate = 0.5 (50%)
    # - 3 seam audits: 1 PASS, 1 WARNING, 1 CRITICAL
    synthetic_lines = [
        "{malformed json line",
        json.dumps(
            {
                "event_id": "e1",
                "timestamp": "2026-11-25T08:00:00Z",
                "run_id": "r1",
                "task_id": "T-1",
                "role": "worker_medium",
                "provider": "provider_main",
                "event_type": "provider_succeeded",
                "payload": {"duration_seconds": 60.0},
            }
        ),
        json.dumps(
            {
                "event_id": "e2",
                "timestamp": "2026-11-25T08:01:00Z",
                "run_id": "r1",
                "task_id": "T-1",
                "role": "worker_medium",
                "provider": "provider_main",
                "event_type": "provider_failed",
                "payload": {"duration_seconds": 20.0, "timed_out": True, "idle_timed_out": False},
            }
        ),
        json.dumps(
            {
                "event_id": "e3",
                "timestamp": "2026-11-25T08:02:00Z",
                "run_id": "r1",
                "task_id": "T-1",
                "role": "worker_medium",
                "provider": "provider_main",
                "event_type": "provider_failed",
                "payload": {"duration_seconds": 15.0, "timed_out": True, "idle_timed_out": True},
            }
        ),
        json.dumps(
            {
                "event_id": "e4",
                "timestamp": "2026-11-25T08:03:00Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "worker_medium",
                "provider": "provider_main",
                "event_type": "provider_transport_retry",
                "payload": {"duration_seconds": 10.0, "delay_seconds": 5.0},
            }
        ),
        json.dumps(
            {
                "event_id": "e5",
                "timestamp": "2026-11-25T08:04:00Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "reviewer",
                "provider": "provider_reviewer_api",
                "event_type": "provider_rate_limited",
                "payload": {"duration_seconds": 50.0, "cooldown_seconds": 90},
            }
        ),
        json.dumps(
            {
                "event_id": "e6",
                "timestamp": "2026-11-25T08:05:00Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "reviewer",
                "provider": "provider_reviewer_api",
                "event_type": "provider_succeeded",
                "payload": {"duration_seconds": 40.0},
            }
        ),
        json.dumps(
            {
                "event_id": "e7",
                "timestamp": "2026-11-25T08:05:10Z",
                "run_id": "r1",
                "task_id": "T-1",
                "event_type": "diff_guard_failed",
                "payload": {"details": ["too many lines"]},
            }
        ),
        json.dumps(
            {
                "event_id": "e8",
                "timestamp": "2026-11-25T08:05:20Z",
                "run_id": "r1",
                "task_id": "T-1",
                "event_type": "gate_failed",
                "payload": {"failed_gates": ["pytest"]},
            }
        ),
        # Pre-review vs Reviewer escape rate: 2 approvals -> 1 escape = 50%
        json.dumps(
            {
                "event_id": "e9",
                "timestamp": "2026-11-25T08:06:00Z",
                "run_id": "r1",
                "task_id": "T-1",
                "role": "pre_reviewer",
                "event_type": "pre_review_completed",
                "payload": {"verdict": "APPROVE"},
            }
        ),
        json.dumps(
            {
                "event_id": "e10",
                "timestamp": "2026-11-25T08:06:10Z",
                "run_id": "r1",
                "task_id": "T-1",
                "role": "reviewer",
                "event_type": "review_completed",
                "payload": {"verdict": "REJECT"},
            }
        ),
        json.dumps(
            {
                "event_id": "e11",
                "timestamp": "2026-11-25T08:07:00Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "pre_reviewer",
                "event_type": "pre_review_completed",
                "payload": {"verdict": "APPROVE"},
            }
        ),
        json.dumps(
            {
                "event_id": "e12",
                "timestamp": "2026-11-25T08:07:10Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "reviewer",
                "event_type": "review_completed",
                "payload": {"verdict": "APPROVE"},
            }
        ),
        # Seam audit summary
        json.dumps(
            {
                "event_id": "e13",
                "timestamp": "2026-11-25T08:08:00Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "seam_auditor",
                "event_type": "seam_audit_completed",
                "payload": {"verdict": "PASS"},
            }
        ),
        json.dumps(
            {
                "event_id": "e14",
                "timestamp": "2026-11-25T08:08:10Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "seam_auditor",
                "event_type": "seam_audit_completed",
                "payload": {"verdict": "WARNING"},
            }
        ),
        json.dumps(
            {
                "event_id": "e15",
                "timestamp": "2026-11-25T08:08:20Z",
                "run_id": "r1",
                "task_id": "T-2",
                "role": "seam_auditor",
                "event_type": "seam_audit_completed",
                "payload": {"verdict": "CRITICAL"},
            }
        ),
    ]
    events_path.write_text("\n".join(synthetic_lines) + "\n", encoding="utf-8")

    stats = compute_pipeline_stats(events_path)
    assert stats.malformed_lines == 1
    assert stats.succeeded_seconds == 100.0
    assert stats.failed_seconds == 50.0
    assert stats.rate_limited_seconds == 50.0
    assert stats.wasted_pct == 50.0
    assert stats.wasted_seconds_by_reason["timeout"] == 20.0
    assert stats.wasted_seconds_by_reason["idle/stall"] == 15.0
    assert stats.wasted_seconds_by_reason["transport"] == 15.0
    assert stats.wasted_seconds_by_reason["capacity/rate_limit"] == 50.0

    assert stats.pre_review.pre_review_approvals == 2
    assert stats.pre_review.reviewer_rejections_after_pre_approve == 1
    assert stats.pre_review.pre_review_escape_rate == 0.5

    assert stats.tasks["T-1"].diff_guard_failed == 1
    assert stats.tasks["T-1"].gates_failed == 1
    assert stats.tasks["T-1"].timeouts == 2

    assert stats.seam_audits.total == 3
    assert stats.seam_audits.passed == 1
    assert stats.seam_audits.warnings == 1
    assert stats.seam_audits.critical == 1

    # CLI `orchestrator stats --json`
    rc_json = cli_main(["stats", "--repo", str(temp_git_repo), "--json"])
    assert rc_json == 0
    json_out = json.loads(capsys.readouterr().out)
    assert json_out["wasted_pct"] == 50.0
    assert json_out["pre_review"]["pre_review_escape_rate"] == 0.5
    assert json_out["seam_audits"]["critical"] == 1

    # CLI `orchestrator stats` (human-readable table)
    rc_txt = cli_main(["stats", "--repo", str(temp_git_repo)])
    assert rc_txt == 0
    txt_out = capsys.readouterr().out
    assert "Pipeline Telemetry" in txt_out
    assert "50.0%" in txt_out
    assert "provider_main" in txt_out

    # REPORT.md includes `## Pipeline Telemetry`
    report_path = write_report(
        pipeline_dir / "REPORT.md",
        project_name="test-repo",
        state=PipelineState(run_id="r1", status=RunStatus.COMPLETED),
        tasks=[
            Task(
                id="T-1",
                title="Task 1",
                description="Desc",
                status=TaskStatus.DONE,
                resolution="completed",
            )
        ],
    )
    report_text = report_path.read_text(encoding="utf-8")
    assert "## Pipeline Telemetry" in report_text
    assert "50.00%" in report_text
    assert "50.0%" in report_text
    assert "`1` CRITICAL" in report_text
