"""Tests for AFK Orchestrator v0.3.1: typed contracts, plan-check, Seam Capsule, and Evidence-Bound Review."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from orchestrator import __version__
from orchestrator.cli import main as cli_main
from orchestrator.config import GateConfig, load_pipeline_config
from orchestrator.diff_guard import evaluate_diff
from orchestrator.fs_utils import atomic_write_json
from orchestrator.git_ops import GitChange
from orchestrator.models import (
    AgentRole,
    ContractClause,
    ContractKind,
    ContractSpec,
    EventType,
    ReviewVerdictType,
    RunStatus,
    Task,
    TaskComplexity,
    TasksFile,
    TaskStatus,
)
from orchestrator.output_parser import (
    ContractCheckResult,
    ReviewOutput,
    parse_single_review_verdict,
)
from orchestrator.plan_check import PlanCheckSeverity, run_plan_check
from orchestrator.prompt_builder import PromptBuilder
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.runner import OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore
from tests.conftest import make_mock_registry, make_test_config, ok_response


def _write_yaml_config(repo: Path, config_dict: dict[str, object] | None = None) -> Path:
    cfg = make_test_config(
        gates=[
            GateConfig(name="pytest", command=["pytest", "-q"]),
        ]
    )
    payload = config_dict if config_dict is not None else cfg.model_dump(mode="json")
    cfg_path = repo / ".pipeline" / "pipeline.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return cfg_path


# ===========================================================================
# 1. Typed contracts schema & LocalJsonTaskStore backward compatibility
# ===========================================================================


def test_contract_spec_validation_all_five_kinds(temp_git_repo: Path) -> None:
    # entity / value / wire require non-empty clauses
    for kind in (ContractKind.ENTITY, ContractKind.VALUE, ContractKind.WIRE):
        with pytest.raises(ValidationError):
            ContractSpec(kind=kind, surface="src/mod.py:Sym", clauses=[])
        valid = ContractSpec(
            kind=kind,
            surface="src/mod.py:Sym",
            clauses=[ContractClause(id="INV-1", rule="Must stay non-negative")],
        )
        assert valid.kind == kind

    # duplicate clause IDs rejected
    with pytest.raises(ValidationError):
        ContractSpec(
            kind=ContractKind.ENTITY,
            surface="src/mod.py:Order",
            clauses=[
                ContractClause(id="INV-1", rule="Rule A"),
                ContractClause(id="INV-1", rule="Rule B"),
            ],
        )

    # fsm requires machine-readable spec_source (not None, not .md)
    with pytest.raises(ValidationError):
        ContractSpec(kind=ContractKind.FSM, surface="OrderFSM", spec_source=None)
    with pytest.raises(ValidationError):
        ContractSpec(kind=ContractKind.FSM, surface="OrderFSM", spec_source="docs/fsm.md")
    with pytest.raises(ValidationError):
        ContractSpec(kind=ContractKind.FSM, surface="OrderFSM", spec_source="DOCS/FSM.MD")
    fsm_ok = ContractSpec(
        kind=ContractKind.FSM,
        surface="OrderFSM",
        spec_source=".contracts/order_fsm.yaml",
    )
    assert fsm_ok.spec_source == ".contracts/order_fsm.yaml"

    # artifact requires non-empty surface, clauses optional
    with pytest.raises(ValidationError):
        ContractSpec(kind=ContractKind.ARTIFACT, surface="")
    with pytest.raises(ValidationError):
        ContractSpec(kind=ContractKind.ARTIFACT, surface="   ")
    art_ok = ContractSpec(kind=ContractKind.ARTIFACT, surface="dist/schema.json")
    assert art_ok.clauses == []

    # Legacy tasks.json without contracts/provides/consumes loads cleanly
    legacy_path = temp_git_repo / ".pipeline" / "tasks.json"
    atomic_write_json(
        legacy_path,
        {
            "schema_version": 1,
            "tasks": [
                {
                    "id": "T-1",
                    "title": "Legacy task",
                    "description": "No contracts field",
                    "allowed_paths": ["src/features/alpha/**"],
                }
            ],
        },
    )
    store = LocalJsonTaskStore(legacy_path)
    assert store.list_contracts() == {}
    t1 = store.get_task("T-1")
    assert t1.provides == []
    assert t1.consumes == []


def test_local_json_store_preserves_contracts_on_add_subtasks(temp_git_repo: Path) -> None:
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    contract = ContractSpec(
        kind=ContractKind.VALUE,
        surface="src/features/alpha/mod.py:Money",
        clauses=[ContractClause(id="INV-1", rule="Amount is integer minor units")],
    )
    tf = TasksFile(
        contracts={"C-MONEY": contract},
        tasks=[
            Task(
                id="T-HARD",
                title="Complex task",
                description="Split me",
                complexity=TaskComplexity.HARD,
                allowed_paths=["src/features/alpha/**"],
                provides=["C-MONEY"],
            )
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    store = LocalJsonTaskStore(tasks_path)
    assert "C-MONEY" in store.list_contracts()
    store.add_subtasks(
        "T-HARD",
        [
            Task(
                id="T-HARD.1",
                title="Part 1",
                description="First half",
                allowed_paths=["src/features/alpha/**"],
                provides=["C-MONEY"],
            ),
            Task(
                id="T-HARD.2",
                title="Part 2",
                description="Second half",
                depends_on=["T-HARD.1"],
                allowed_paths=["src/features/alpha/**"],
            ),
        ],
    )
    reloaded = LocalJsonTaskStore(tasks_path)
    assert "C-MONEY" in reloaded.list_contracts()
    assert reloaded.list_contracts()["C-MONEY"].clauses[0].id == "INV-1"


# ===========================================================================
# 2. Frozen contract fixtures in Diff Guard
# ===========================================================================


def test_diff_guard_blocks_frozen_contract_fixtures_for_writing_roles() -> None:
    cfg = make_test_config()
    assert cfg.diff_guard.contracts_protected_paths == [".contracts/fixtures/**"]

    fixture_change = [GitChange(status="M", path=".contracts/fixtures/c1.json")]
    raw_diff = (
        "diff --git a/.contracts/fixtures/c1.json b/.contracts/fixtures/c1.json\n"
        "--- a/.contracts/fixtures/c1.json\n"
        "+++ b/.contracts/fixtures/c1.json\n"
        "@@ -1 +1 @@\n"
        '-{"ok": true}\n'
        '+{"ok": false}\n'
    )

    for role in (AgentRole.WORKER_MEDIUM, AgentRole.ARCHITECT, AgentRole.REFACTORER):
        report = evaluate_diff(
            role=role,
            changes=fixture_change,
            raw_diff=raw_diff,
            config=cfg,
            task_complexity=TaskComplexity.MEDIUM,
            allowed_paths=[".contracts/fixtures/**", "src/**"],
        )
        assert not report.passed
        assert len(report.red_violations) == 1
        assert report.red_violations[0].code == "RED_ZONE_PROTECTED_FILE"
        assert ".contracts/fixtures/c1.json" in report.red_violations[0].message


# ===========================================================================
# 3. Deterministic plan-check & CLI
# ===========================================================================


def test_plan_check_deterministic_catches_blockers_and_warnings(
    temp_git_repo: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg_path = _write_yaml_config(temp_git_repo)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"

    contract_wire = ContractSpec(
        kind=ContractKind.WIRE,
        surface="POST /v1/pay",
        clauses=[ContractClause(id="ERR-1", rule="Returns 400 on negative amount")],
    )
    contract_fsm = ContractSpec(
        kind=ContractKind.FSM,
        surface="PayFSM",
        spec_source="specs/missing_fsm.yaml",
    )

    tf = TasksFile(
        contracts={
            "C-PAY": contract_wire,
            "C-FSM": contract_fsm,
        },
        tasks=[
            # T-DONE missing final_commit_sha -> WARNING
            Task(
                id="T-DONE",
                title="Completed without sha",
                description="Already finished",
                status=TaskStatus.DONE,
                resolution="completed",
                final_commit_sha=None,
            ),
            # T-RED overlaps red_protected and contracts_protected_paths -> BLOCKER
            Task(
                id="T-RED",
                title="Bad allowed_paths",
                description="Write unit tests for module",
                allowed_paths=[
                    ".pipeline/tasks.json",
                    ".contracts/fixtures/c1.json",
                    "nonexistent_dir/**",
                    "src/features/alpha/**",
                ],
                context_files=["src/does_not_exist.py"],
                verify_cmd="pytest tests/custom_alpha.py",
                provides=["C-PAY"],
            ),
            # T-CONSUMER consumes C-PAY without T-RED in depends_on -> BLOCKER
            # Also consumes unknown contract -> BLOCKER
            Task(
                id="T-CONSUMER",
                title="Dangling consumer",
                description="Consumes wire contract",
                allowed_paths=["src/features/beta/**"],
                context_files=["src/features/alpha/mod.py"],
                verify_cmd="pytest tests/custom_alpha.py",
                consumes=["C-PAY", "C-UNKNOWN"],
            ),
            # Third disjoint task with identical copy-pasted non-global verify_cmd -> WARNING
            # Also spans > 2 top-level non-test dirs (dir_a, dir_b, dir_c) -> WARNING
            Task(
                id="T-COPY",
                title="Oversized and copy-pasted verify_cmd",
                description="Touches 3 top-level dirs",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["dir_a/**", "dir_b/**", "dir_c/**", "tests/**"],
                verify_cmd="pytest tests/custom_alpha.py",
            ),
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    cfg = load_pipeline_config(cfg_path)
    report = run_plan_check(temp_git_repo, cfg)
    codes_by_sev = {(f.severity, f.code, f.task_id) for f in report.findings}

    assert report.has_blockers
    assert (PlanCheckSeverity.BLOCKER, "ALLOWED_PATH_PROTECTED_OVERLAP", "T-RED") in codes_by_sev
    assert (PlanCheckSeverity.BLOCKER, "MISSING_CONTEXT_FILE", "T-RED") in codes_by_sev
    assert (PlanCheckSeverity.BLOCKER, "UNSATISFIED_CONTRACT_CONSUMER", "T-CONSUMER") in codes_by_sev
    assert (PlanCheckSeverity.BLOCKER, "UNKNOWN_CONTRACT_ID", "T-CONSUMER") in codes_by_sev

    assert (PlanCheckSeverity.WARNING, "NONEXISTENT_ALLOWED_PATH_ROOT", "T-RED") in codes_by_sev
    assert (PlanCheckSeverity.WARNING, "TESTS_NOT_IN_ALLOWED_PATHS", "T-RED") in codes_by_sev
    assert (PlanCheckSeverity.WARNING, "COPYPASTE_VERIFY_CMD", None) in codes_by_sev
    assert (PlanCheckSeverity.WARNING, "OVERSIZED_TASK_SCOPE", "T-COPY") in codes_by_sev
    assert (PlanCheckSeverity.WARNING, "DONE_MISSING_COMMIT_SHA", "T-DONE") in codes_by_sev
    assert (PlanCheckSeverity.WARNING, "MISSING_CONTRACT_SPEC_SOURCE", None) in codes_by_sev

    # CLI exit code == 1 on blockers, --json outputs valid report
    rc = cli_main(["plan-check", "--repo", str(temp_git_repo), "--json"])
    assert rc == 1
    out = capsys.readouterr().out
    parsed_json = json.loads(out)
    assert parsed_json["ok"] is False
    assert parsed_json["summary"]["blockers"] >= 4


def test_plan_check_clean_graph_returns_zero_and_respects_ancestor_context(
    temp_git_repo: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_yaml_config(temp_git_repo)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"

    contract = ContractSpec(
        kind=ContractKind.ENTITY,
        surface="src/features/alpha/order.py:Order",
        clauses=[ContractClause(id="INV-1", rule="Order ID is non-empty UUID")],
    )
    tf = TasksFile(
        contracts={"C-ORDER": contract},
        tasks=[
            Task(
                id="T-1",
                title="Create Order model",
                description="Implement Order entity with unit tests",
                allowed_paths=["src/features/alpha/**", "tests/**"],
                context_files=["src/features/alpha/mod.py"],
                verify_cmd="pytest -q",
                provides=["C-ORDER"],
            ),
            Task(
                id="T-2",
                title="Consume Order model",
                description="Use Order entity in beta",
                depends_on=["T-1"],
                allowed_paths=["src/features/beta/**", "tests/**"],
                # File does not exist yet on disk, but is covered by ancestor T-1 allowed_paths
                context_files=["src/features/alpha/order.py"],
                verify_cmd="pytest -q",
                consumes=["C-ORDER"],
            ),
            Task(
                id="T-3",
                title="Gamma consumer",
                description="Use Order in gamma",
                depends_on=["T-2"],
                allowed_paths=["src/features/gamma/**", "tests/**"],
                verify_cmd="pytest -q",  # matches global gate -> NOT flagged as copy-paste
                consumes=["C-ORDER"],  # transitive ancestor T-1 provides C-ORDER
            ),
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    rc = cli_main(["plan-check", "--repo", str(temp_git_repo)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Plan check passed" in out


def test_plan_check_dag_cycles_duplicates_and_dangling_deps(temp_git_repo: Path) -> None:
    cfg = make_test_config()
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    atomic_write_json(
        tasks_path,
        {
            "schema_version": 1,
            "tasks": [
                {
                    "id": "T-A",
                    "title": "A",
                    "description": "A",
                    "depends_on": ["T-B", "T-MISSING"],
                    "allowed_paths": ["src/features/alpha/**"],
                },
                {
                    "id": "T-B",
                    "title": "B",
                    "description": "B",
                    "depends_on": ["T-A"],
                    "allowed_paths": ["src/features/beta/**"],
                },
                {
                    "id": "T-B",
                    "title": "B duplicate",
                    "description": "Dup",
                    "allowed_paths": ["src/features/gamma/**"],
                },
            ],
        },
    )
    report = run_plan_check(temp_git_repo, cfg)
    codes = {f.code for f in report.findings if f.severity == PlanCheckSeverity.BLOCKER}
    assert "DUPLICATE_TASK_ID" in codes
    assert "DANGLING_DEPENDENCY" in codes
    assert "DEPENDENCY_CYCLE" in codes


# ===========================================================================
# 4. plan-check --llm with ScriptedMockProvider
# ===========================================================================


def test_plan_check_llm_merges_findings_and_handles_invalid_output(temp_git_repo: Path) -> None:
    cfg = make_test_config()
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        tasks=[
            Task(
                id="T-1",
                title="Implement feature",
                description="Do work",
                allowed_paths=["src/features/alpha/**"],
                verify_cmd="",  # triggers 1 deterministic WARNING (EMPTY_VERIFY_CMD)
            )
        ]
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    # Case A: valid fenced BEGIN_PLAN_AUDIT_JSON
    def valid_handler(req: ProviderRequest) -> ProviderResponse:
        assert req.role == AgentRole.PLANNER
        assert "PLAN AUDIT INSTRUCTIONS" in req.prompt
        return ok_response(
            "provider_master",
            (
                "Audit complete.\n"
                "BEGIN_PLAN_AUDIT_JSON\n"
                "```json\n"
                '[{"severity": "BLOCKER", "task_id": "T-1", "code": "SEMANTIC_GAP", '
                '"message": "Missing error-handling acceptance criterion."}]\n'
                "```\n"
                "END_PLAN_AUDIT_JSON\n"
            ),
        )

    registry, mocks = make_mock_registry(cfg, valid_handler)
    report = run_plan_check(temp_git_repo, cfg, use_llm=True, registry=registry)
    assert report.llm_used is True
    assert len(mocks["provider_master"].calls) == 1
    codes = {(f.severity, f.code, f.task_id) for f in report.findings}
    assert (PlanCheckSeverity.WARNING, "EMPTY_VERIFY_CMD", "T-1") in codes
    assert (PlanCheckSeverity.BLOCKER, "SEMANTIC_GAP", "T-1") in codes
    assert report.has_blockers

    # Case B: malformed LLM output -> adds WARNING without crashing
    def invalid_handler(_req: ProviderRequest) -> ProviderResponse:
        return ok_response("provider_master", "Sorry, I forgot the JSON markers.")

    bad_registry, _ = make_mock_registry(cfg, invalid_handler)
    bad_report = run_plan_check(temp_git_repo, cfg, use_llm=True, registry=bad_registry)
    assert not bad_report.has_blockers
    assert any(
        f.severity == PlanCheckSeverity.WARNING and f.code == "LLM_AUDIT_INVALID_OUTPUT"
        for f in bad_report.findings
    )


# ===========================================================================
# 5. Seam Capsule injection in PromptBuilder
# ===========================================================================


def test_prompt_builder_injects_seam_capsule_for_worker_and_reviewers(temp_git_repo: Path) -> None:
    cfg = make_test_config()
    pb = PromptBuilder(temp_git_repo, cfg)
    contracts = {
        "C-ALPHA": ContractSpec(
            kind=ContractKind.WIRE,
            surface="src/features/alpha/mod.py:alpha",
            description="Alpha wire response",
            spec_source=".contracts/alpha.yaml",
            fixtures=[".contracts/fixtures/alpha_ok.json"],
            clauses=[
                ContractClause(id="INV-1", rule="Returns positive integer"),
                ContractClause(id="ERR-1", rule="Raises ValueError on negative input"),
            ],
        ),
        "C-UPSTREAM": ContractSpec(
            kind=ContractKind.ARTIFACT,
            surface="src/shared/schema.py:SCHEMA_VERSION",
        ),
    }
    task = Task(
        id="T-SC",
        title="Alpha task",
        description="Implement alpha",
        allowed_paths=["src/features/alpha/**"],
        provides=["C-ALPHA"],
        consumes=["C-UPSTREAM"],
    )

    for role in (AgentRole.WORKER_MEDIUM, AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
        profile = cfg.providers[cfg.roles.profile_for_role(role)]
        built = pb.build(
            role=role,
            profile=profile,
            repo_map_text="# REPO_MAP\n- src/features/alpha/mod.py",
            progress_text="",
            task=task,
            contracts=contracts,
        )
        assert "## SEAM CAPSULE (ACTIVE CONTRACTS)" in built.dynamic_tail
        assert "### [PROVIDES] C-ALPHA (`wire`)" in built.dynamic_tail
        assert "### [CONSUMES] C-UPSTREAM (`artifact`)" in built.dynamic_tail
        assert "`.contracts/alpha.yaml`" in built.dynamic_tail
        assert "`.contracts/fixtures/alpha_ok.json`" in built.dynamic_tail
        assert "1. [INV-1] Returns positive integer" in built.dynamic_tail
        assert "2. [ERR-1] Raises ValueError on negative input" in built.dynamic_tail

    # Without contracts -> omitted from dynamic tail
    no_contract_task = Task(
        id="T-PLAIN",
        title="Plain task",
        description="No contracts",
        allowed_paths=["src/features/alpha/**"],
    )
    plain_prompt = pb.build(
        role=AgentRole.WORKER_MEDIUM,
        profile=cfg.providers["provider_main"],
        repo_map_text="",
        progress_text="",
        task=no_contract_task,
        contracts=contracts,
    )
    assert "## SEAM CAPSULE (ACTIVE CONTRACTS)" not in plain_prompt.dynamic_tail


# ===========================================================================
# 6. Evidence-Bound Review (output_parser + OrchestratorRunner end-to-end)
# ===========================================================================


def test_output_parser_parses_contract_checks_with_and_without_fences() -> None:
    raw = (
        "BEGIN_FEEDBACK\n- All checks verified.\nEND_FEEDBACK\n\n"
        "BEGIN_CONTRACT_CHECKS_JSON\n"
        "```json\n"
        "[\n"
        '  {"clause_id": "INV-1", "status": "VERIFIED", "evidence": "src/a.py:10"},\n'
        '  {"clause_id": "ERR-1", "status": "untested", "evidence": "missing test"}\n'
        "]\n"
        "```\n"
        "END_CONTRACT_CHECKS_JSON\n\n"
        "VERDICT: APPROVE\n"
    )
    parsed: ReviewOutput = parse_single_review_verdict(raw, "T-1")
    assert parsed.verdict == ReviewVerdictType.APPROVE
    assert len(parsed.contract_checks) == 2
    assert parsed.contract_checks[0] == ContractCheckResult(
        clause_id="INV-1", status="VERIFIED", evidence="src/a.py:10"
    )
    assert parsed.contract_checks[1] == ContractCheckResult(
        clause_id="ERR-1", status="UNTESTED", evidence="missing test"
    )


def test_evidence_bound_review_approves_when_all_clauses_verified(temp_git_repo: Path) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        contracts={
            "C-ALPHA": ContractSpec(
                kind=ContractKind.VALUE,
                surface="src/features/alpha/mod.py:alpha",
                clauses=[
                    ContractClause(id="INV-1", rule="Returns positive int"),
                    ContractClause(id="ERR-1", rule="Rejects negative input"),
                ],
            )
        },
        tasks=[
            Task(
                id="T-EBR-OK",
                title="Implement alpha contract",
                description="Satisfy INV-1 and ERR-1",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
                provides=["C-ALPHA"],
            )
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            assert "## SEAM CAPSULE (ACTIVE CONTRACTS)" in req.prompt
            (req.workdir / "src" / "features" / "alpha" / "mod.py").write_text(
                "def alpha(x: int = 1) -> int:\n    if x < 0:\n        raise ValueError('neg')\n    return x + 1\n",
                encoding="utf-8",
            )
            return ok_response("provider_main", "BEGIN_REPORT\nImplemented.\nEND_REPORT\nSTATUS: DONE\n")
        if req.role in (AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
            assert "## SEAM CAPSULE (ACTIVE CONTRACTS)" in req.prompt
            return ok_response(
                "provider_reviewer_api",
                (
                    "BEGIN_FEEDBACK\nAll clauses verified.\nEND_FEEDBACK\n"
                    "BEGIN_CONTRACT_CHECKS_JSON\n"
                    '[{"clause_id": "INV-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:4"}, '
                    '{"clause_id": "ERR-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:2"}]\n'
                    "END_CONTRACT_CHECKS_JSON\n"
                    "VERDICT: APPROVE\n"
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
    done_task = store.get_task("T-EBR-OK")
    assert done_task.status == TaskStatus.DONE
    assert done_task.final_commit_sha is not None


def test_evidence_bound_review_overrides_incomplete_or_untested_approve_to_reject(
    temp_git_repo: Path,
) -> None:
    cfg = make_test_config(batch_review_size=1, max_review_rounds=3)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        contracts={
            "C-ALPHA": ContractSpec(
                kind=ContractKind.WIRE,
                surface="src/features/alpha/mod.py:alpha",
                clauses=[
                    ContractClause(id="INV-1", rule="Returns positive int"),
                    ContractClause(id="ERR-1", rule="Raises ValueError on negative input"),
                ],
            )
        },
        tasks=[
            Task(
                id="T-EBR-REJECT",
                title="Contract task",
                description="Must pass evidence bound review",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
                provides=["C-ALPHA"],
            )
        ],
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    pre_review_calls = 0
    master_review_calls = 0
    worker_prompts: list[str] = []

    def handler(req: ProviderRequest) -> ProviderResponse:
        nonlocal pre_review_calls, master_review_calls
        if req.role == AgentRole.WORKER_MEDIUM:
            worker_prompts.append(req.prompt)
            (req.workdir / "src" / "features" / "alpha" / "mod.py").write_text(
                f"def alpha(x: int = {len(worker_prompts)}) -> int:\n"
                "    if x < 0:\n"
                "        raise ValueError('neg')\n"
                "    return x + 1\n",
                encoding="utf-8",
            )
            return ok_response("provider_main", "BEGIN_REPORT\nDone.\nEND_REPORT\nSTATUS: DONE\n")
        if req.role == AgentRole.PRE_REVIEWER:
            pre_review_calls += 1
            if pre_review_calls == 1:
                # Missing ERR-1 -> must be programmatically overridden to REJECT
                return ok_response(
                    "provider_reviewer_api",
                    (
                        "BEGIN_FEEDBACK\nLooks fine.\nEND_FEEDBACK\n"
                        "BEGIN_CONTRACT_CHECKS_JSON\n"
                        '[{"clause_id": "INV-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:4"}]\n'
                        "END_CONTRACT_CHECKS_JSON\n"
                        "VERDICT: APPROVE\n"
                    ),
                )
            return ok_response(
                "provider_reviewer_api",
                (
                    "BEGIN_FEEDBACK\nVerified both.\nEND_FEEDBACK\n"
                    "BEGIN_CONTRACT_CHECKS_JSON\n"
                    '[{"clause_id": "INV-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:4"}, '
                    '{"clause_id": "ERR-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:2"}]\n'
                    "END_CONTRACT_CHECKS_JSON\n"
                    "VERDICT: APPROVE\n"
                ),
            )
        if req.role == AgentRole.REVIEWER:
            master_review_calls += 1
            if master_review_calls == 1:
                # ERR-1 marked UNTESTED -> must be programmatically overridden to REJECT
                return ok_response(
                    "provider_reviewer_api",
                    (
                        "BEGIN_FEEDBACK\nApprove anyway.\nEND_FEEDBACK\n"
                        "BEGIN_CONTRACT_CHECKS_JSON\n"
                        '[{"clause_id": "INV-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:4"}, '
                        '{"clause_id": "ERR-1", "status": "UNTESTED", "evidence": "not tested"}]\n'
                        "END_CONTRACT_CHECKS_JSON\n"
                        "VERDICT: APPROVE\n"
                    ),
                )
            return ok_response(
                "provider_reviewer_api",
                (
                    "BEGIN_FEEDBACK\nVerified all clauses.\nEND_FEEDBACK\n"
                    "BEGIN_CONTRACT_CHECKS_JSON\n"
                    '[{"clause_id": "INV-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:4"}, '
                    '{"clause_id": "ERR-1", "status": "VERIFIED", "evidence": "src/features/alpha/mod.py:2"}]\n'
                    "END_CONTRACT_CHECKS_JSON\n"
                    "VERDICT: APPROVE\n"
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
    assert pre_review_calls == 3
    assert master_review_calls == 2
    # Worker round 2 and 3 received explicit evidence_bound_review_failed feedback mentioning ERR-1
    assert "evidence_bound_review_failed" in worker_prompts[1]
    assert "ERR-1" in worker_prompts[1]
    assert "evidence_bound_review_failed" in worker_prompts[2]
    assert "UNTESTED" in worker_prompts[2]

    # Verify events.jsonl logged evidence_bound_review_failed for both pre_reviewer and reviewer
    events = [
        json.loads(line)
        for line in (temp_git_repo / ".pipeline" / "events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    ebr_events = [
        e
        for e in events
        if e["event_type"]
        in (EventType.PRE_REVIEW_COMPLETED.value, EventType.REVIEW_COMPLETED.value)
        and e["payload"].get("reason") == "evidence_bound_review_failed"
    ]
    assert len(ebr_events) == 2
    assert all(e["payload"]["verdict"] == "REJECT" for e in ebr_events)
    assert all("ERR-1" in e["payload"]["failed_clauses"] for e in ebr_events)


def test_task_without_contracts_approves_without_contract_checks_json(temp_git_repo: Path) -> None:
    cfg = make_test_config(batch_review_size=1)
    tasks_path = temp_git_repo / ".pipeline" / "tasks.json"
    tf = TasksFile(
        tasks=[
            Task(
                id="T-PLAIN",
                title="Normal task without contracts",
                description="Update alpha",
                complexity=TaskComplexity.MEDIUM,
                allowed_paths=["src/features/alpha/**"],
            )
        ]
    )
    atomic_write_json(tasks_path, tf.model_dump(mode="json"))

    def handler(req: ProviderRequest) -> ProviderResponse:
        if req.role == AgentRole.WORKER_MEDIUM:
            (req.workdir / "src" / "features" / "alpha" / "mod.py").write_text(
                "def alpha() -> int:\n    return 2\n",
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
    status = runner.run()
    assert status == RunStatus.COMPLETED
    assert store.get_task("T-PLAIN").status == TaskStatus.DONE


# ===========================================================================
# 7. Version 0.3.1, role templates, and example config
# ===========================================================================


def test_v031_version_templates_and_example_config() -> None:
    assert __version__ == "0.3.1"

    repo_root = Path(__file__).resolve().parent.parent
    cfg = load_pipeline_config(repo_root / "pipeline.example.yaml")
    assert cfg.diff_guard.contracts_protected_paths == [".contracts/fixtures/**"]

    templates_dir = repo_root / "templates" / ".agents"
    worker_md = (templates_dir / "worker.md").read_text(encoding="utf-8")
    assert "SEAM CAPSULE" in worker_md
    assert ".contracts/fixtures/**" in worker_md

    pre_rev_md = (templates_dir / "pre_reviewer.md").read_text(encoding="utf-8")
    assert "BEGIN_CONTRACT_CHECKS_JSON" in pre_rev_md
    assert "END_CONTRACT_CHECKS_JSON" in pre_rev_md
    assert "VERIFIED" in pre_rev_md

    rev_md = (templates_dir / "reviewer.md").read_text(encoding="utf-8")
    assert "BEGIN_CONTRACT_CHECKS_JSON" in rev_md
    assert "END_CONTRACT_CHECKS_JSON" in rev_md
    assert "UNTESTED" in rev_md

    planner_md = (templates_dir / "planner.md").read_text(encoding="utf-8")
    assert "BEGIN_PLAN_AUDIT_JSON" in planner_md
    assert "END_PLAN_AUDIT_JSON" in planner_md
