"""Final run REPORT.md generator with task summary and manual verification checklist."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from orchestrator.fs_utils import atomic_write_text
from orchestrator.models import PipelineState, Task, TaskStatus, utc_now_iso
from orchestrator.stats import PipelineStats, compute_pipeline_stats, render_telemetry_markdown


def render_report_markdown(
    *,
    project_name: str,
    state: PipelineState,
    tasks: list[Task],
    stats: PipelineStats | None = None,
) -> str:
    """Render a concise run report and step-by-step manual QA checklist."""
    counts = Counter(t.status for t in tasks)
    completed = [t for t in tasks if t.status == TaskStatus.DONE and t.resolution != "split"]
    split_tasks = [t for t in tasks if t.resolution == "split"]
    failed = [t for t in tasks if t.status == TaskStatus.FAILED]
    blocked = [t for t in tasks if t.status == TaskStatus.BLOCKED]
    pending = [
        t
        for t in tasks
        if t.status
        not in (TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.BLOCKED)
    ]

    lines: list[str] = [
        f"# AFK Pipeline Report — {project_name}",
        "",
        f"- **Run ID:** `{state.run_id}`",
        f"- **Run Status:** `{state.status.value}`",
        f"- **Generated At:** `{utc_now_iso()}`",
        f"- **Total Tasks:** {len(tasks)} "
        f"(completed: {len(completed)}, split: {len(split_tasks)}, "
        f"failed: {counts[TaskStatus.FAILED]}, blocked: {counts[TaskStatus.BLOCKED]}, "
        f"remaining: {len(pending)})",
        "",
        "## Task Summary",
        "",
        "| ID | Title | Complexity | Status | Resolution | Attempts | Commit | Notes |",
        "|---|---|---|---|---|---:|---|---|",
    ]

    for task in tasks:
        commit = (task.final_commit_sha or task.wip_commit_sha or "-")[:10]
        resolution = task.resolution or "-"
        note = task.failure_reason or (
            f"Split into {', '.join(task.subtask_ids)}" if task.subtask_ids else "-"
        )
        safe_title = task.title.replace("|", "\\|")
        safe_note = note.replace("|", "\\|").replace("\n", " ")[:120]
        lines.append(
            f"| `{task.id}` | {safe_title} | {task.complexity.value} | "
            f"`{task.status.value}` | `{resolution}` | {task.attempts} | "
            f"`{commit}` | {safe_note} |"
        )

    if failed or blocked:
        lines.extend(["", "## Failures & Blocked Tasks", ""])
        for task in [*failed, *blocked]:
            lines.append(
                f"- **`#{task.id}` ({task.status.value}) — {task.title}**: "
                f"{task.failure_reason or 'No failure reason recorded.'}"
            )

    if split_tasks:
        lines.extend(["", "## Decomposed Hard Tasks", ""])
        for task in split_tasks:
            lines.append(
                f"- **`#{task.id}` — {task.title}** -> split into: "
                + ", ".join(f"`#{sid}`" for sid in task.subtask_ids)
            )

    lines.extend(["", "## Manual Verification Checklist", ""])
    if not completed:
        lines.append("- [ ] No new completed tasks to verify in this run.")
    else:
        for task in completed:
            lines.append(f"### `#{task.id}` — {task.title}")
            if task.changed_files:
                files_preview = ", ".join(f"`{p}`" for p in task.changed_files[:10])
                lines.append(f"- Modified files: {files_preview}")
            if task.touched_yellow_zone:
                lines.append("- [ ] **Shared/Yellow Zone touched:** verify cross-module compatibility and config/schema changes.")
            if task.verify_cmd:
                lines.append(f"- [ ] Run verification command: `{task.verify_cmd}`")
            if task.acceptance_criteria:
                for criterion in task.acceptance_criteria:
                    lines.append(f"- [ ] Verify: {criterion}")
            else:
                lines.append(f"- [ ] Manually exercise feature behavior described in `#{task.id}`.")
            if task.last_report:
                summary_line = next(
                    (ln.strip() for ln in task.last_report.splitlines() if ln.strip()),
                    "",
                )
                if summary_line:
                    lines.append(f"- Worker report excerpt: {summary_line[:180]}")
            lines.append("")

    if stats is not None:
        lines.extend(["", render_telemetry_markdown(stats)])

    return "\n".join(lines).rstrip() + "\n"


def write_report(
    report_path: Path,
    *,
    project_name: str,
    state: PipelineState,
    tasks: list[Task],
    events_path: Path | None = None,
    stats: PipelineStats | None = None,
) -> Path:
    effective_stats = stats
    if effective_stats is None:
        candidate_events = events_path if events_path is not None else (report_path.parent / "events.jsonl")
        if candidate_events.is_file():
            effective_stats = compute_pipeline_stats(candidate_events)
    content = render_report_markdown(
        project_name=project_name,
        state=state,
        tasks=tasks,
        stats=effective_stats,
    )
    atomic_write_text(report_path, content)
    return report_path
