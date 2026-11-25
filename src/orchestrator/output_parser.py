"""Strict structured marker parsers for worker, reviewer, batch reviewer, architect, and planner outputs."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence

from orchestrator.models import (
    ContractCheckResult,
    ParsedPlannerOutput,
    ParsedReviewVerdict,
    ParsedWorkerOutput,
    ReviewOutput,
    ReviewVerdictType,
    SeamAuditOutput,
    SubtaskSpec,
    Task,
    WorkerStatus,
)

__all__ = [
    "ContractCheckResult",
    "ReviewOutput",
    "SeamAuditOutput",
    "parse_batch_review_verdicts",
    "parse_contract_checks",
    "parse_planner_subtasks",
    "parse_seam_audit_output",
    "parse_single_review_verdict",
    "parse_worker_output",
]

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
_MD_LINE_PREFIX_RE = re.compile(r"^\s*(?:#{1,6}\s+|>\s*|[-*+]\s+|\d+[.)]\s+)")
# Strip bold/italic/code inline wrappers (*+, __+, `+) WITHOUT touching single underscores `_`.
_INLINE_MD_RE = re.compile(r"\*+|__+|`+")

_TASK_ID = r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?"

_STATUS_RE = re.compile(
    r"^\s*STATUS\s*:\s*(DONE|NEEDS_ARCH_CHANGE)\b",
    re.MULTILINE | re.IGNORECASE,
)
_SINGLE_VERDICT_RE = re.compile(
    rf"^\s*VERDICT(?:\s+#?{_TASK_ID})?\s*:\s*(APPROVE|REJECT)\b",
    re.MULTILINE | re.IGNORECASE,
)
_SEP = r"[\s.:\u2014\u2013-]"

_BATCH_VERDICT_PATTERNS: list[re.Pattern[str]] = [
    # 1. VERDICT #?T-1: APPROVE|REJECT [reason]
    re.compile(
        rf"^\s*VERDICT\s+#?(?P<id>{_TASK_ID})[.\s]*:\s*(?P<verdict>APPROVE|REJECT)\b{_SEP}*(?P<rest>.*)$",
        re.IGNORECASE,
    ),
    # 2. APPROVE|REJECT #T-1 [: - — reason]
    re.compile(
        rf"^\s*(?P<verdict>APPROVE|REJECT)\s+#(?P<id>{_TASK_ID})(?=$|{_SEP}){_SEP}*(?P<rest>.*)$",
        re.IGNORECASE,
    ),
    # 3. #?T-1: APPROVE|REJECT [reason] (excluding VERDICT: / STATUS: prefixes)
    re.compile(
        rf"^\s*(?!(?:VERDICT|STATUS)\s*:)#?(?P<id>{_TASK_ID})\s*:\s*(?P<verdict>APPROVE|REJECT)\b{_SEP}*(?P<rest>.*)$",
        re.IGNORECASE,
    ),
]


def _normalize_for_markers(text: str, *, strip_code_fences: bool = True) -> str:
    """Strip ANSI codes, optional fenced code blocks, Markdown line prefixes, and inline wrappers.

    Preserves single underscores `_` so identifiers like `NEEDS_ARCH_CHANGE` or `task_1` stay intact.
    """
    cleaned = _ANSI_RE.sub("", text)
    if strip_code_fences:
        cleaned = _CODE_FENCE_RE.sub("", cleaned)

    normalized_lines: list[str] = []
    for raw_line in cleaned.splitlines():
        line = raw_line.strip()
        # Repeatedly strip stacked prefixes such as `> - `, `### `, `1. `
        while True:
            stripped = _MD_LINE_PREFIX_RE.sub("", line, count=1)
            if stripped == line:
                break
            line = stripped.strip()
        line = _INLINE_MD_RE.sub("", line).strip()
        # Handle cases where inline wrapper surrounded a prefix like `**> STATUS: DONE**`
        while True:
            stripped = _MD_LINE_PREFIX_RE.sub("", line, count=1)
            if stripped == line:
                break
            line = stripped.strip()
        normalized_lines.append(line)

    return "\n".join(normalized_lines)


def _find_matches_with_fallback(
    pattern: re.Pattern[str],
    text: str,
) -> tuple[list[re.Match[str]], str]:
    """Search normalized text with code fences stripped first, falling back to keeping fences."""
    norm = _normalize_for_markers(text, strip_code_fences=True)
    matches = list(pattern.finditer(norm))
    if matches:
        return matches, norm
    norm_fallback = _normalize_for_markers(text, strip_code_fences=False)
    return list(pattern.finditer(norm_fallback)), norm_fallback


def _extract_block(text: str, start_marker: str, end_marker: str) -> str | None:
    cleaned = _ANSI_RE.sub("", text)
    pattern = re.compile(
        rf"^\s*{re.escape(start_marker)}\s*$(.*?)^\s*{re.escape(end_marker)}\s*$",
        re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(cleaned)
    if match:
        return match.group(1).strip()
    return None


def parse_worker_output(text: str) -> ParsedWorkerOutput:
    """Parse worker/architect/refactorer structured output into report, progress, and status."""
    status_matches, norm_text = _find_matches_with_fallback(_STATUS_RE, text)
    status = WorkerStatus.UNKNOWN
    if status_matches:
        raw_status = status_matches[-1].group(1).upper()
        status = WorkerStatus(raw_status)

    report = _extract_block(text, "BEGIN_REPORT", "END_REPORT")
    if report is None:
        report = text.strip()

    progress_update = _extract_block(text, "BEGIN_PROGRESS", "END_PROGRESS")
    escalation_block = _extract_block(text, "BEGIN_ESCALATION", "END_ESCALATION")
    escalation_summary: str | None = None
    escalation_details: str | None = None

    if escalation_block:
        escalation_details = escalation_block
        first_non_empty = next(
            (ln.strip() for ln in escalation_block.splitlines() if ln.strip()),
            None,
        )
        escalation_summary = first_non_empty
    elif status == WorkerStatus.NEEDS_ARCH_CHANGE and status_matches:
        last_match = status_matches[-1]
        tail = norm_text[last_match.end() :].strip()
        tail_lines = [ln.strip() for ln in tail.splitlines() if ln.strip()][:5]
        if tail_lines:
            escalation_summary = tail_lines[0]
            escalation_details = "\n".join(tail_lines)

    return ParsedWorkerOutput(
        status=status,
        report=report,
        progress_update=progress_update,
        escalation_summary=escalation_summary,
        escalation_details=escalation_details,
        raw_text=text,
    )


def _unwrap_json_fence(raw_block: str) -> str:
    """Strip optional Markdown code fences (```json ... ```) around an extracted JSON payload."""
    cleaned = _ANSI_RE.sub("", raw_block).strip()
    fence_match = re.search(r"```(?:json)?\s*(.*?)```", cleaned, re.DOTALL | re.IGNORECASE)
    if fence_match:
        return fence_match.group(1).strip()
    return cleaned


def parse_contract_checks(text: str) -> list[ContractCheckResult]:
    """Parse optional BEGIN_CONTRACT_CHECKS_JSON..END_CONTRACT_CHECKS_JSON block from review output."""
    raw_block = _extract_block(text, "BEGIN_CONTRACT_CHECKS_JSON", "END_CONTRACT_CHECKS_JSON")
    if raw_block is None:
        return []

    json_text = _unwrap_json_fence(raw_block)
    if not json_text:
        return []

    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError:
        return []

    if isinstance(payload, list):
        raw_items = payload
    elif isinstance(payload, dict):
        candidate = payload.get("contract_checks") or payload.get("checks")
        if not isinstance(candidate, list):
            return []
        raw_items = candidate
    else:
        return []

    results: list[ContractCheckResult] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        if isinstance(normalized.get("status"), str):
            normalized["status"] = normalized["status"].strip().upper()
        try:
            results.append(ContractCheckResult.model_validate(normalized))
        except Exception:
            continue
    return results


def parse_single_review_verdict(text: str, task_id: str) -> ReviewOutput:
    """Parse VERDICT: APPROVE / REJECT and optional contract checks from review output."""
    matches, _ = _find_matches_with_fallback(_SINGLE_VERDICT_RE, text)
    verdict = ReviewVerdictType.UNKNOWN
    if matches:
        verdict = ReviewVerdictType(matches[-1].group(1).upper())

    feedback = _extract_block(text, "BEGIN_FEEDBACK", "END_FEEDBACK")
    if feedback is None:
        feedback = text.strip()

    contract_checks = parse_contract_checks(text)
    return ParsedReviewVerdict(
        task_id=task_id,
        verdict=verdict,
        feedback=feedback,
        contract_checks=contract_checks,
    )


def _collect_batch_hits(
    normalized_text: str,
) -> tuple[list[str], list[tuple[int, str, ReviewVerdictType, str]]]:
    lines = normalized_text.splitlines()
    hits: list[tuple[int, str, ReviewVerdictType, str]] = []
    for line_idx, line in enumerate(lines):
        if not line:
            continue
        for pattern in _BATCH_VERDICT_PATTERNS:
            match = pattern.match(line)
            if match:
                tid = match.group("id")
                verdict = ReviewVerdictType(match.group("verdict").upper())
                rest = match.group("rest").strip()
                hits.append((line_idx, tid, verdict, rest))
                break
    return lines, hits


def _build_batch_parsed_map(
    lines: list[str],
    hits: list[tuple[int, str, ReviewVerdictType, str]],
    canonical_map: dict[str, str] | None,
) -> dict[str, ParsedReviewVerdict]:
    parsed: dict[str, ParsedReviewVerdict] = {}
    for idx, (line_idx, raw_tid, verdict, inline_note) in enumerate(hits):
        if canonical_map is not None:
            canon_id = canonical_map.get(raw_tid.lower())
            if canon_id is None:
                continue
        else:
            canon_id = raw_tid

        next_line_idx = hits[idx + 1][0] if idx + 1 < len(hits) else len(lines)
        body_lines = [inline_note] if inline_note else []
        body_lines.extend(lines[line_idx + 1 : next_line_idx])
        feedback = "\n".join(body_lines).strip()
        # Later mentions overwrite earlier mentions for the same task ID
        parsed[canon_id] = ParsedReviewVerdict(
            task_id=canon_id,
            verdict=verdict,
            feedback=feedback,
        )
    return parsed


def parse_batch_review_verdicts(
    text: str,
    expected_task_ids: Sequence[str] | set[str] | None = None,
    *,
    expected_ids: Sequence[str] | set[str] | None = None,
) -> dict[str, ParsedReviewVerdict]:
    """Parse per-task verdicts supporting Markdown wrappers, ANSI codes, and 3 verdict formats.

    Supports:
    - `VERDICT #?T-1: APPROVE|REJECT [reason]`
    - `APPROVE|REJECT #T-1 [: - — reason]`
    - `#?T-1: APPROVE|REJECT [reason]`
    """
    effective_expected = expected_ids if expected_ids is not None else expected_task_ids
    canonical_map: dict[str, str] | None = (
        {eid.lower(): eid for eid in effective_expected}
        if effective_expected is not None
        else None
    )

    norm_no_fences = _normalize_for_markers(text, strip_code_fences=True)
    lines, hits = _collect_batch_hits(norm_no_fences)
    parsed = _build_batch_parsed_map(lines, hits, canonical_map)

    if not parsed:
        norm_with_fences = _normalize_for_markers(text, strip_code_fences=False)
        fb_lines, fb_hits = _collect_batch_hits(norm_with_fences)
        parsed = _build_batch_parsed_map(fb_lines, fb_hits, canonical_map)

    if effective_expected is None:
        return parsed

    results: dict[str, ParsedReviewVerdict] = {}
    for expected_id in effective_expected:
        if expected_id in parsed:
            results[expected_id] = parsed[expected_id]
        else:
            results[expected_id] = ParsedReviewVerdict(
                task_id=expected_id,
                verdict=ReviewVerdictType.REJECT,
                feedback=(
                    f"Reviewer omitted explicit verdict for task #{expected_id}; "
                    f"failing safe with REJECT."
                ),
            )
    return results


def parse_planner_subtasks(text: str, parent_task: Task) -> ParsedPlannerOutput:
    """Extract and validate atomic subtasks JSON from planner output."""
    json_block = _extract_block(text, "BEGIN_SUBTASKS_JSON", "END_SUBTASKS_JSON")
    if json_block is None:
        cleaned = _ANSI_RE.sub("", text)
        fence_match = re.search(r"```(?:json)?\s*(.*?)```", cleaned, re.DOTALL | re.IGNORECASE)
        if fence_match:
            json_block = fence_match.group(1).strip()
    if json_block is None:
        raise ValueError(
            "Planner output did not contain a BEGIN_SUBTASKS_JSON..END_SUBTASKS_JSON or JSON block"
        )

    payload = json.loads(json_block)
    if isinstance(payload, list):
        raw_subtasks = payload
        rationale = ""
    elif isinstance(payload, dict) and isinstance(payload.get("subtasks"), list):
        raw_subtasks = payload["subtasks"]
        rationale = str(payload.get("rationale", ""))
    else:
        raise ValueError("Planner JSON must be a list of subtasks or {'subtasks': [...]}")

    subtasks: list[SubtaskSpec] = []
    for idx, item in enumerate(raw_subtasks, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Subtask entry #{idx} must be a JSON object")
        normalized = dict(item)
        normalized.setdefault("id", f"{parent_task.id}.{idx}")
        normalized.setdefault("allowed_paths", list(parent_task.allowed_paths))
        normalized.setdefault("context_files", list(parent_task.context_files))
        subtasks.append(SubtaskSpec.model_validate(normalized))

    if len(subtasks) < 2:
        raise ValueError(f"Planner must produce at least 2 subtasks, got {len(subtasks)}")

    return ParsedPlannerOutput(subtasks=subtasks, rationale=rationale)


def parse_seam_audit_output(text: str) -> SeamAuditOutput:
    """Parse BEGIN_SEAM_AUDIT_JSON..END_SEAM_AUDIT_JSON into SeamAuditOutput with WARNING fallback."""
    raw_block = _extract_block(text, "BEGIN_SEAM_AUDIT_JSON", "END_SEAM_AUDIT_JSON")
    if raw_block is None:
        return SeamAuditOutput(
            verdict="WARNING",
            summary="Seam auditor output did not contain BEGIN_SEAM_AUDIT_JSON..END_SEAM_AUDIT_JSON.",
            findings=[],
        )

    json_text = _unwrap_json_fence(raw_block)
    if not json_text:
        return SeamAuditOutput(
            verdict="WARNING",
            summary="Seam auditor output contained an empty BEGIN_SEAM_AUDIT_JSON block.",
            findings=[],
        )

    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        return SeamAuditOutput(
            verdict="WARNING",
            summary=f"Invalid seam audit JSON ({exc.msg}).",
            findings=[],
        )

    if not isinstance(payload, dict):
        return SeamAuditOutput(
            verdict="WARNING",
            summary="Seam audit JSON payload must be a JSON object.",
            findings=[],
        )

    normalized = dict(payload)
    if isinstance(normalized.get("verdict"), str):
        normalized["verdict"] = normalized["verdict"].strip().upper()
    raw_findings = normalized.get("findings")
    if isinstance(raw_findings, list):
        norm_findings: list[object] = []
        for item in raw_findings:
            if isinstance(item, dict):
                copy_item = dict(item)
                if isinstance(copy_item.get("severity"), str):
                    copy_item["severity"] = copy_item["severity"].strip().upper()
                norm_findings.append(copy_item)
            else:
                norm_findings.append(item)
        normalized["findings"] = norm_findings

    try:
        return SeamAuditOutput.model_validate(normalized)
    except Exception as exc:
        return SeamAuditOutput(
            verdict="WARNING",
            summary=f"Seam audit JSON failed schema validation: {exc}",
            findings=[],
        )
