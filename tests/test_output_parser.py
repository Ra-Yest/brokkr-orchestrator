"""Unit tests for resilient Markdown/ANSI/code-fence marker parsing in output_parser."""

from __future__ import annotations

from orchestrator.models import ReviewVerdictType, WorkerStatus
from orchestrator.output_parser import (
    parse_batch_review_verdicts,
    parse_single_review_verdict,
    parse_worker_output,
)


def test_markdown_and_ansi_wrappers_for_verdict_and_status() -> None:
    # ANSI + heading + bold on VERDICT
    v1 = parse_single_review_verdict("\x1b[1m### **VERDICT: APPROVE**\x1b[0m\n", "T-1")
    assert v1.verdict == ReviewVerdictType.APPROVE

    # Stacked quote + list + inline code + underscores preserved in NEEDS_ARCH_CHANGE
    w1 = parse_worker_output(
        "Analysis complete.\n"
        "> - `STATUS: NEEDS_ARCH_CHANGE`\n"
        "Missing user_role column in shared schema.\n"
    )
    assert w1.status == WorkerStatus.NEEDS_ARCH_CHANGE
    assert w1.escalation_summary == "Missing user_role column in shared schema."

    # Bold key before colon (`**STATUS:** DONE`)
    w2 = parse_worker_output("All tests green.\n**STATUS:** DONE\n")
    assert w2.status == WorkerStatus.DONE

    # Double-underscore bold preserves single underscores inside value
    w3 = parse_worker_output("__STATUS:__ __NEEDS_ARCH_CHANGE__\nNeed shared type.\n")
    assert w3.status == WorkerStatus.NEEDS_ARCH_CHANGE


def test_last_marker_wins_and_code_fences_ignored_with_whole_response_fallback() -> None:
    # Marker inside fenced code block is ignored; final real marker wins over quoted earlier one
    text_with_fence = (
        "Previously the reviewer wrote:\n"
        "VERDICT: REJECT\n\n"
        "Example output from docs:\n"
        "```text\n"
        "VERDICT: REJECT\n"
        "STATUS: NEEDS_ARCH_CHANGE\n"
        "```\n\n"
        "Final decision after fixing all issues:\n"
        "**VERDICT: APPROVE**\n"
    )
    v = parse_single_review_verdict(text_with_fence, "T-10")
    assert v.verdict == ReviewVerdictType.APPROVE

    worker_with_fence = (
        "Earlier note: STATUS: NEEDS_ARCH_CHANGE\n"
        "```markdown\n"
        "STATUS: NEEDS_ARCH_CHANGE\n"
        "```\n"
        "Final:\n"
        "STATUS: DONE\n"
    )
    w = parse_worker_output(worker_with_fence)
    assert w.status == WorkerStatus.DONE

    # Fallback: entire model response wrapped in ```text ... ```
    wrapped_verdict = "```text\nBEGIN_FEEDBACK\nEverything checks out.\nEND_FEEDBACK\n\nVERDICT: APPROVE\n```\n"
    v_fb = parse_single_review_verdict(wrapped_verdict, "T-11")
    assert v_fb.verdict == ReviewVerdictType.APPROVE

    wrapped_status = "```text\nAll done.\nSTATUS: DONE\n```\n"
    w_fb = parse_worker_output(wrapped_status)
    assert w_fb.status == WorkerStatus.DONE


def test_batch_review_formats_trailing_dot_filtering_and_duplicate_override() -> None:
    batch_output = (
        "```text\n"
        "- **#T-1**: REJECT — example inside code fence must be ignored\n"
        "```\n\n"
        "Review results:\n"
        "- **#T-1**: REJECT — preliminary note\n"
        "- REJECT #T-2. — missing edge case\n"
        "3. `VERDICT T-3: APPROVE`\n"
        "- **#T-999**: APPROVE — extra unknown task should be filtered out\n"
        "- **#t-1**: APPROVE — actually verified, looks great\n"
        "VERDICT: REJECT\n"
        "STATUS: DONE\n"
    )

    verdicts = parse_batch_review_verdicts(
        batch_output,
        expected_ids=["T-1", "T-2", "T-3"],
    )

    assert set(verdicts.keys()) == {"T-1", "T-2", "T-3"}
    assert "T-999" not in verdicts

    # Duplicate T-1 (with lowercase `#t-1`) overwritten by last mention & canonical ID restored
    assert verdicts["T-1"].task_id == "T-1"
    assert verdicts["T-1"].verdict == ReviewVerdictType.APPROVE
    assert "looks great" in verdicts["T-1"].feedback

    # Trailing dot stripped from `#T-2.`
    assert verdicts["T-2"].task_id == "T-2"
    assert verdicts["T-2"].verdict == ReviewVerdictType.REJECT
    assert "missing edge case" in verdicts["T-2"].feedback

    # Numbered list + backticks `VERDICT T-3: APPROVE`
    assert verdicts["T-3"].task_id == "T-3"
    assert verdicts["T-3"].verdict == ReviewVerdictType.APPROVE


def test_batch_review_whole_response_code_fence_fallback_and_underscore_ids() -> None:
    fenced_batch = (
        "```text\n"
        "- #task_alpha_1: APPROVE\n"
        "- REJECT #task_beta_2. - handle empty list\n"
        "```\n"
    )
    verdicts = parse_batch_review_verdicts(
        fenced_batch,
        expected_task_ids={"task_alpha_1", "task_beta_2"},
    )
    assert verdicts["task_alpha_1"].verdict == ReviewVerdictType.APPROVE
    assert verdicts["task_beta_2"].verdict == ReviewVerdictType.REJECT
    assert "handle empty list" in verdicts["task_beta_2"].feedback
