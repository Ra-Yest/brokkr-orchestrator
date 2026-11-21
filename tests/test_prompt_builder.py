"""Tests for 3-layer deterministic PromptBuilder, prompt caching prefix stability, and REPO_MAP."""

from __future__ import annotations

from pathlib import Path

from orchestrator.models import AgentRole, Task, TaskComplexity
from orchestrator.prompt_builder import PromptBuilder, generate_repo_map
from tests.conftest import make_test_config


def test_static_prefix_is_byte_identical_across_different_tasks(temp_git_repo: Path) -> None:
    cfg = make_test_config()
    builder = PromptBuilder(temp_git_repo, cfg)
    fast_profile = cfg.providers["provider_fast"]  # anti_laziness=True
    main_profile = cfg.providers["provider_main"]  # anti_laziness=False

    task_a = Task(
        id="T-001",
        title="Task Alpha",
        description="Implement alpha feature.",
        complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/alpha/**"],
        context_files=["src/features/alpha/mod.py"],
    )
    task_b = Task(
        id="T-999",
        title="Completely Different Task",
        description="Implement something else with error logs and inbox.",
        complexity=TaskComplexity.EASY,
        allowed_paths=["src/features/beta/**"],
    )

    p1 = builder.build(
        role=AgentRole.WORKER_EASY,
        profile=fast_profile,
        repo_map_text="# REPO_MAP\n- a.py\n",
        progress_text="Progress A",
        task=task_a,
    )
    p2 = builder.build(
        role=AgentRole.WORKER_EASY,
        profile=fast_profile,
        repo_map_text="# REPO_MAP\n- a.py\n- b.py\n",
        progress_text="Progress B with extra lines",
        task=task_b,
        inbox_text="Urgent operator note!",
        gate_error_log="AssertionError: failed",
        review_feedback="Fix edge case on line 10",
    )

    # Layer 1 static prefix MUST be byte-for-byte identical for prompt caching
    assert p1.static_prefix == p2.static_prefix
    assert p1.static_prefix_sha256 == p2.static_prefix_sha256
    assert p1.full_text.startswith(p1.static_prefix)
    assert p2.full_text.startswith(p2.static_prefix)

    # Anti-laziness module included ONLY when profile.anti_laziness=True
    assert "Anti-Laziness" in p1.static_prefix
    p_no_anti = builder.build(
        role=AgentRole.WORKER_MEDIUM,
        profile=main_profile,
        repo_map_text="# REPO_MAP\n",
        progress_text="",
        task=task_a,
    )
    assert "Anti-Laziness" not in p_no_anti.static_prefix
    assert p_no_anti.static_prefix_sha256 != p1.static_prefix_sha256

    # Verify strict 3-layer ordering: Static -> Semi-Static -> Dynamic
    idx_static = p2.full_text.index("REPOSITORY INSTRUCTIONS (AGENTS.md)")
    idx_semi = p2.full_text.index("REPOSITORY MAP")
    idx_dyn = p2.full_text.index("ACTIVE TASK")
    assert idx_static < idx_semi < idx_dyn


def test_crlf_normalization_preserves_prefix_hash(temp_git_repo: Path) -> None:
    cfg = make_test_config()
    builder = PromptBuilder(temp_git_repo, cfg)
    profile = cfg.providers["provider_fast"]

    lf_prefix = builder.build_static_prefix(AgentRole.WORKER_EASY, profile)
    lf_hash = builder.build(
        role=AgentRole.WORKER_EASY,
        profile=profile,
        repo_map_text="",
        progress_text="",
    ).static_prefix_sha256

    # Rewrite AGENTS.md with Windows CRLF endings
    agents_path = temp_git_repo / "AGENTS.md"
    agents_path.write_bytes(agents_path.read_text(encoding="utf-8").replace("\n", "\r\n").encode("utf-8"))

    crlf_built = builder.build(
        role=AgentRole.WORKER_EASY,
        profile=profile,
        repo_map_text="",
        progress_text="",
    )
    assert crlf_built.static_prefix == lf_prefix
    assert crlf_built.static_prefix_sha256 == lf_hash
    assert "\r" not in crlf_built.full_text


def test_repo_map_is_deterministic_and_excludes_pipeline(temp_git_repo: Path) -> None:
    (temp_git_repo / ".pipeline").mkdir(exist_ok=True)
    (temp_git_repo / ".pipeline" / "secret_state.json").write_text("{}", encoding="utf-8")
    cfg = make_test_config()
    repo_map = generate_repo_map(
        temp_git_repo,
        cfg.project.repo_map_ignore,
        pipeline_dir=cfg.project.pipeline_dir,
    )
    assert "src/features/alpha/mod.py" in repo_map
    assert "tests/test_existing.py" in repo_map
    assert "secret_state.json" not in repo_map
    assert "- .git/" not in repo_map
    assert "- .pipeline/" not in repo_map
