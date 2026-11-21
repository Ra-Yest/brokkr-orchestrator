"""Deterministic 3-layer PromptBuilder for prompt caching and zero-token REPO_MAP generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from orchestrator.config import PipelineConfig, ProviderProfileConfig
from orchestrator.diff_guard import matches_any_glob
from orchestrator.fs_utils import normalize_newlines, sha256_text
from orchestrator.models import AgentRole, Task

DEFAULT_ROLE_PROMPT_FILES: dict[AgentRole, str] = {
    AgentRole.PLANNER: "planner.md",
    AgentRole.WORKER_EASY: "worker.md",
    AgentRole.WORKER_MEDIUM: "worker.md",
    AgentRole.WORKER_HARD: "worker.md",
    AgentRole.PRE_REVIEWER: "pre_reviewer.md",
    AgentRole.REVIEWER: "reviewer.md",
    AgentRole.ARCHITECT: "architect.md",
    AgentRole.REFACTORER: "refactorer.md",
}


@dataclass(frozen=True)
class BuiltPrompt:
    static_prefix: str
    semi_static_layer: str
    dynamic_tail: str
    full_text: str
    static_prefix_sha256: str


def generate_repo_map(
    repo_root: Path,
    ignore_patterns: list[str],
    *,
    pipeline_dir: str = ".pipeline",
    max_files: int = 2000,
) -> str:
    """Build a deterministic, lexicographically sorted repository tree (0 LLM tokens)."""
    root = repo_root.resolve()
    ignore_names = {pat.strip("/\\") for pat in ignore_patterns if "/" not in pat and "*" not in pat}
    ignore_names.add(pipeline_dir.strip("/\\"))
    ignore_names.add(".git")

    rel_paths: list[str] = []

    def _walk(current: Path) -> None:
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name)
        except OSError:
            return
        for entry in entries:
            name = entry.name
            if name in ignore_names:
                continue
            rel = entry.relative_to(root).as_posix()
            if matches_any_glob(rel, ignore_patterns):
                continue
            if entry.is_dir():
                _walk(entry)
            elif entry.is_file():
                rel_paths.append(rel)
                if len(rel_paths) >= max_files:
                    return

    _walk(root)
    rel_paths.sort()

    lines = ["# REPO_MAP", ""]
    if not rel_paths:
        lines.append("(repository has no indexed source files)")
    else:
        for rel in rel_paths:
            lines.append(f"- {rel}")
    return "\n".join(lines) + "\n"


class PromptBuilder:
    """Assembles prompts in strict deterministic order for maximal provider prompt caching.

    Layer 1 (Static Prefix):
        AGENTS.md + .agents/<role>.md + optional anti_laziness module (by provider profile flag).
        Contains ZERO dynamic data (no timestamps, run_ids, task_ids, or attempt paths).
    Layer 2 (Semi-Static Layer):
        REPO_MAP.md + PROGRESS.md.
    Layer 3 (Dynamic Tail):
        Task card(s), context_files, INBOX.md, filtered diff, trimmed error logs, review feedback.
    """

    def __init__(self, repo_root: Path, config: PipelineConfig) -> None:
        self.repo_root = repo_root.resolve()
        self.config = config

    def _read_optional_text(self, path: Path) -> str:
        if not path.exists():
            return ""
        return normalize_newlines(path.read_text(encoding="utf-8")).strip()

    def _role_prompt_path(self, role: AgentRole) -> Path:
        custom = self.config.roles.role_prompt_files.get(role.value)
        if custom:
            candidate = Path(custom)
            return candidate if candidate.is_absolute() else (self.repo_root / candidate)
        filename = DEFAULT_ROLE_PROMPT_FILES[role]
        return self.repo_root / self.config.project.agent_roles_dir / filename

    def build_static_prefix(self, role: AgentRole, profile: ProviderProfileConfig) -> str:
        """Build byte-deterministic Layer 1 prefix for prompt caching."""
        sections: list[str] = []

        agents_md = self._read_optional_text(self.repo_root / self.config.project.agents_file)
        if agents_md:
            sections.append(f"## REPOSITORY INSTRUCTIONS (AGENTS.md)\n\n{agents_md}")

        role_md = self._read_optional_text(self._role_prompt_path(role))
        if role_md:
            sections.append(f"## ROLE INSTRUCTIONS ({role.value})\n\n{role_md}")

        if profile.anti_laziness:
            anti_path = self.repo_root / self.config.project.anti_laziness_file
            anti_md = self._read_optional_text(anti_path)
            if anti_md:
                sections.append(f"## STRICT QUALITY & ANTI-LAZINESS RULES\n\n{anti_md}")

        return normalize_newlines("\n\n---\n\n".join(sections)).strip()

    def build_semi_static_layer(
        self,
        *,
        repo_map_text: str,
        progress_text: str,
    ) -> str:
        """Build Layer 2 (REPO_MAP.md + PROGRESS.md)."""
        sections: list[str] = []
        norm_map = normalize_newlines(repo_map_text).strip()
        if norm_map:
            sections.append(f"## REPOSITORY MAP\n\n{norm_map}")
        norm_progress = normalize_newlines(progress_text).strip()
        if norm_progress:
            sections.append(f"## PIPELINE PROGRESS HANDOFF (PROGRESS.md)\n\n{norm_progress}")
        return normalize_newlines("\n\n---\n\n".join(sections)).strip()

    def _render_context_files(self, context_files: list[str]) -> str:
        if not context_files:
            return ""
        # Deduplicate while keeping deterministic sorted order
        unique_paths = sorted({p.replace("\\", "/") for p in context_files if p.strip()})
        blocks: list[str] = ["## CONTEXT FILES"]
        for rel in unique_paths:
            full = (self.repo_root / rel).resolve()
            try:
                full.relative_to(self.repo_root)
            except ValueError:
                blocks.append(f"### {rel}\n(skipped: outside repository root)")
                continue
            if full.exists() and full.is_file():
                content = normalize_newlines(full.read_text(encoding="utf-8", errors="replace"))
                blocks.append(f"### {rel}\n```\n{content}\n```")
            else:
                blocks.append(f"### {rel}\n(file does not exist yet)")
        return "\n\n".join(blocks)

    def _render_task_card(self, task: Task) -> str:
        lines = [
            f"### Task #{task.id}: {task.title}",
            f"- **Complexity:** {task.complexity.value}",
            f"- **Stage:** {task.stage}",
            f"- **Allowed Paths:** {', '.join(task.allowed_paths) if task.allowed_paths else '(unrestricted green zone)'}",
        ]
        if task.verify_cmd:
            lines.append(f"- **Task Verify Command:** `{task.verify_cmd}`")
        lines.extend(["", "**Description:**", normalize_newlines(task.description).strip()])
        if task.acceptance_criteria:
            lines.append("")
            lines.append("**Acceptance Criteria:**")
            for item in task.acceptance_criteria:
                lines.append(f"- {item}")
        return "\n".join(lines)

    def build(
        self,
        *,
        role: AgentRole,
        profile: ProviderProfileConfig,
        repo_map_text: str,
        progress_text: str,
        task: Task | None = None,
        batch_items: list[tuple[Task, str, str]] | None = None,
        inbox_text: str | None = None,
        filtered_diff: str | None = None,
        gate_error_log: str | None = None,
        guard_error_text: str | None = None,
        review_feedback: str | None = None,
        last_report: str | None = None,
        escalation_context: str | None = None,
        extra_instructions: str | None = None,
    ) -> BuiltPrompt:
        """Assemble all 3 layers deterministically."""
        static_prefix = self.build_static_prefix(role, profile)
        static_hash = sha256_text(static_prefix)
        semi_static = self.build_semi_static_layer(
            repo_map_text=repo_map_text,
            progress_text=progress_text,
        )

        dynamic_sections: list[str] = []

        if task is not None:
            dynamic_sections.append(f"## ACTIVE TASK\n\n{self._render_task_card(task)}")
            ctx_block = self._render_context_files(task.context_files)
            if ctx_block:
                dynamic_sections.append(ctx_block)

        if batch_items:
            batch_blocks: list[str] = [
                "## BATCH REVIEW ITEMS\n"
                "Evaluate EACH task independently and output a separate verdict line per ID "
                "(`VERDICT #<id>: APPROVE` or `VERDICT #<id>: REJECT`)."
            ]
            for b_task, b_diff, b_report in batch_items:
                item_parts = [self._render_task_card(b_task)]
                if b_report.strip():
                    item_parts.append(f"#### Worker Report (#{b_task.id})\n{normalize_newlines(b_report).strip()}")
                item_parts.append(f"#### Filtered Diff (#{b_task.id})\n```diff\n{normalize_newlines(b_diff).strip()}\n```")
                batch_blocks.append("\n\n".join(item_parts))
            dynamic_sections.append("\n\n---\n\n".join(batch_blocks))

        if inbox_text and inbox_text.strip():
            dynamic_sections.append(
                f"## OPERATOR INBOX INSTRUCTIONS (INBOX.md)\n\n{normalize_newlines(inbox_text).strip()}"
            )

        if escalation_context and escalation_context.strip():
            dynamic_sections.append(
                f"## ARCHITECTURAL ESCALATION REQUEST\n\n{normalize_newlines(escalation_context).strip()}"
            )

        if last_report and last_report.strip():
            dynamic_sections.append(
                f"## WORKER REPORT (last_report.md)\n\n{normalize_newlines(last_report).strip()}"
            )

        if filtered_diff and filtered_diff.strip():
            dynamic_sections.append(
                f"## FILTERED GIT DIFF\n\n```diff\n{normalize_newlines(filtered_diff).strip()}\n```"
            )

        if guard_error_text and guard_error_text.strip():
            dynamic_sections.append(
                f"## DIFF / POLICY GUARD VIOLATIONS (MUST FIX)\n\n{normalize_newlines(guard_error_text).strip()}"
            )

        if gate_error_log and gate_error_log.strip():
            dynamic_sections.append(
                f"## FAILED QUALITY GATES LOG (TRIMMED)\n\n```\n{normalize_newlines(gate_error_log).strip()}\n```"
            )

        if review_feedback and review_feedback.strip():
            dynamic_sections.append(
                f"## REVIEWER FEEDBACK TO ADDRESS\n\n{normalize_newlines(review_feedback).strip()}"
            )

        if extra_instructions and extra_instructions.strip():
            dynamic_sections.append(
                f"## ADDITIONAL INSTRUCTIONS\n\n{normalize_newlines(extra_instructions).strip()}"
            )

        dynamic_tail = normalize_newlines("\n\n---\n\n".join(dynamic_sections)).strip()

        all_layers = [layer for layer in (static_prefix, semi_static, dynamic_tail) if layer]
        full_text = normalize_newlines("\n\n===\n\n".join(all_layers)) + "\n"

        return BuiltPrompt(
            static_prefix=static_prefix,
            semi_static_layer=semi_static,
            dynamic_tail=dynamic_tail,
            full_text=full_text,
            static_prefix_sha256=static_hash,
        )
