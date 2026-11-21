"""Tests for Red/Yellow/Green zones, M/D vs A test detector, role-aware Diff Guard, Placeholder Guard, and diff filtering."""

from __future__ import annotations

from orchestrator.diff_guard import evaluate_diff, filter_diff_for_review, scan_placeholders
from orchestrator.git_ops import GitChange
from orchestrator.models import AgentRole, TaskComplexity
from tests.conftest import make_test_config


def _make_simple_diff(path: str, added_lines: list[str], removed_lines: list[str] | None = None) -> str:
    body_lines = [f"-{ln}" for ln in (removed_lines or [])] + [f"+{ln}" for ln in added_lines]
    return (
        f"diff --git a/{path} b/{path}\n"
        f"--- a/{path}\n"
        f"+++ b/{path}\n"
        f"@@ -1,{len(removed_lines or [])} +1,{len(added_lines)} @@\n"
        + "\n".join(body_lines)
        + "\n"
    )


def test_red_zone_existing_tests_modified_or_deleted_vs_added() -> None:
    cfg = make_test_config()
    clean_diff = _make_simple_diff("tests/features/alpha/test_new.py", ["def test_ok():", "    assert 1 == 1"])

    # M on existing test -> instant red violation
    rep_m = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="M", path="tests/test_existing.py")],
        raw_diff=_make_simple_diff("tests/test_existing.py", ["assert True"]),
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**", "tests/**"],
    )
    assert not rep_m.passed
    assert any(v.code == "RED_ZONE_TEST_MODIFIED_OR_DELETED" for v in rep_m.red_violations)

    # D on existing test -> instant red violation
    rep_d = evaluate_diff(
        role=AgentRole.WORKER_MEDIUM,
        changes=[GitChange(status="D", path="tests/test_existing.py")],
        raw_diff="",
        config=cfg,
        task_complexity=TaskComplexity.MEDIUM,
        allowed_paths=["tests/**"],
    )
    assert not rep_d.passed
    assert any(v.code == "RED_ZONE_TEST_MODIFIED_OR_DELETED" for v in rep_d.red_violations)

    # R (rename) of existing test -> instant red violation
    rep_r = evaluate_diff(
        role=AgentRole.WORKER_MEDIUM,
        changes=[GitChange(status="R", path="tests/test_moved.py", old_path="tests/test_existing.py")],
        raw_diff="",
        config=cfg,
        task_complexity=TaskComplexity.MEDIUM,
        allowed_paths=["tests/**"],
    )
    assert not rep_r.passed
    assert any(v.code == "RED_ZONE_TEST_MODIFIED_OR_DELETED" for v in rep_r.red_violations)

    # A (added) new test INSIDE allowed_paths -> permitted!
    rep_a_ok = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="A", path="tests/features/alpha/test_new.py")],
        raw_diff=clean_diff,
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**", "tests/features/alpha/**"],
    )
    assert rep_a_ok.passed
    assert rep_a_ok.red_violations == []

    # A (added) new test OUTSIDE allowed_paths -> scope violation
    rep_a_outside = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="A", path="tests/other/test_outside.py")],
        raw_diff=_make_simple_diff("tests/other/test_outside.py", ["def test_x():", "    assert True"]),
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**", "tests/features/alpha/**"],
    )
    assert not rep_a_outside.passed
    assert any(v.code == "OUT_OF_ALLOWED_PATHS" for v in rep_a_outside.scope_violations)


def test_role_aware_diff_guard_readonly_and_architect() -> None:
    cfg = make_test_config()

    # Read-only roles must fail on ANY repository modification
    for ro_role in (AgentRole.PLANNER, AgentRole.PRE_REVIEWER, AgentRole.REVIEWER):
        rep = evaluate_diff(
            role=ro_role,
            changes=[GitChange(status="M", path="src/features/alpha/mod.py")],
            raw_diff=_make_simple_diff("src/features/alpha/mod.py", ["x = 2"]),
            config=cfg,
        )
        assert not rep.passed
        assert any(v.code == "READ_ONLY_ROLE_MODIFIED_FILES" for v in rep.red_violations)

    # Architect and Refactorer may edit yellow_shared and files outside a task's allowed_paths
    for arch_role in (AgentRole.ARCHITECT, AgentRole.REFACTORER):
        ok_rep = evaluate_diff(
            role=arch_role,
            changes=[
                GitChange(status="M", path="src/shared/schema.py"),
                GitChange(status="M", path="src/features/beta/mod.py"),
            ],
            raw_diff=_make_simple_diff("src/shared/schema.py", ["SCHEMA_VERSION = 2"]),
            config=cfg,
            allowed_paths=["src/features/alpha/**"],
        )
        assert ok_rep.passed
        assert ok_rep.touched_yellow_zone is True

        # Still strictly forbidden to modify existing tests or Red Zone files
        bad_test_rep = evaluate_diff(
            role=arch_role,
            changes=[GitChange(status="M", path="tests/test_existing.py")],
            raw_diff=_make_simple_diff("tests/test_existing.py", ["assert 1 == 1"]),
            config=cfg,
        )
        assert not bad_test_rep.passed
        assert any(v.code == "RED_ZONE_TEST_MODIFIED_OR_DELETED" for v in bad_test_rep.red_violations)

        bad_red_rep = evaluate_diff(
            role=arch_role,
            changes=[GitChange(status="M", path=".agents/worker.md")],
            raw_diff=_make_simple_diff(".agents/worker.md", ["hacked"]),
            config=cfg,
        )
        assert not bad_red_rep.passed
        assert any(v.code == "RED_ZONE_PROTECTED_FILE" for v in bad_red_rep.red_violations)


def test_diff_guard_limits_and_yellow_zone_flag() -> None:
    cfg = make_test_config()
    # Exceed easy max_files (5)
    many_changes = [GitChange(status="A", path=f"src/features/alpha/f{i}.py") for i in range(6)]
    rep_files = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=many_changes,
        raw_diff=_make_simple_diff("src/features/alpha/f0.py", ["x = 1"]),
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**"],
    )
    assert not rep_files.passed
    assert any(v.code == "DIFF_GUARD_MAX_FILES" for v in rep_files.limit_violations)

    # Exceed easy max_lines (200)
    big_diff = _make_simple_diff("src/features/alpha/big.py", [f"x_{i} = {i}" for i in range(210)])
    rep_lines = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="M", path="src/features/alpha/big.py")],
        raw_diff=big_diff,
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**"],
    )
    assert not rep_lines.passed
    assert any(v.code == "DIFF_GUARD_MAX_LINES" for v in rep_lines.limit_violations)

    # Yellow zone file sets touched_yellow_zone=True while passing worker guard
    rep_yellow = evaluate_diff(
        role=AgentRole.WORKER_EASY,
        changes=[GitChange(status="M", path="src/shared/schema.py")],
        raw_diff=_make_simple_diff("src/shared/schema.py", ["SCHEMA_VERSION = 2"]),
        config=cfg,
        task_complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**"],
    )
    assert rep_yellow.passed
    assert rep_yellow.touched_yellow_zone is True
    assert rep_yellow.yellow_files == ["src/shared/schema.py"]


def test_placeholder_guard_catches_laziness_only_in_added_lines() -> None:
    diff_with_placeholders = (
        "diff --git a/src/features/alpha/impl.py b/src/features/alpha/impl.py\n"
        "--- a/src/features/alpha/impl.py\n"
        "+++ b/src/features/alpha/impl.py\n"
        "@@ -1,4 +1,11 @@\n"
        "-# TODO: old removed comment should NOT trigger\n"
        "+def compute():\n"
        "+    # TODO: finish later\n"
        "+    raise NotImplementedError()\n"
        "+def empty_stub():\n"
        "+    pass\n"
        "+# ... rest of code\n"
        "+// ... existing code\n"
    )
    violations = scan_placeholders(diff_with_placeholders)
    codes = {v.code for v in violations}
    assert "PLACEHOLDER_TODO_MARKER" in codes
    assert "PLACEHOLDER_NOT_IMPLEMENTED_ERROR" in codes
    assert "PLACEHOLDER_EMPTY_FUNCTION_STUB" in codes
    assert "PLACEHOLDER_COMMENT_EXISTING_CODE" in codes
    # Removed line `# TODO: old removed comment` must not appear
    assert all("old removed comment" not in v.message for v in violations)

    # Clean real implementation produces zero placeholder violations
    clean_diff = _make_simple_diff(
        "src/features/alpha/impl.py",
        [
            "class CustomDomainError(Exception):",
            "    pass",
            "",
            "def add(a: int, b: int) -> int:",
            "    return a + b",
        ],
    )
    assert scan_placeholders(clean_diff) == []


def test_filter_diff_for_review_strips_locks_svgs_dist_and_binaries() -> None:
    cfg = make_test_config()
    raw = (
        _make_simple_diff("src/features/alpha/mod.py", ["def alpha() -> int:", "    return 42"])
        + _make_simple_diff("package-lock.json", ['"version": "1.0.0"'])
        + _make_simple_diff("assets/logo.svg", ["<svg></svg>"])
        + _make_simple_diff("dist/bundle.js", ["console.log(1);"])
        + "diff --git a/img/banner.bin b/img/banner.bin\n"
        + "Binary files a/img/banner.bin and b/img/banner.bin differ\n"
    )
    filtered = filter_diff_for_review(raw, cfg.diff_guard.exclude_from_review)
    assert "return 42" in filtered
    assert '"version": "1.0.0"' not in filtered
    assert "<svg></svg>" not in filtered
    assert "console.log(1);" not in filtered
    assert "[omitted from review diff" in filtered
