"""Three-zone access control, role-aware Diff Guard, Placeholder Guard, and diff filtering."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from orchestrator.config import DiffLimitConfig, PipelineConfig
from orchestrator.git_ops import GitChange
from orchestrator.models import (
    ARCHITECT_ROLES,
    READ_ONLY_ROLES,
    AgentRole,
    TaskComplexity,
)


@dataclass(frozen=True)
class GuardViolation:
    code: str
    path: str | None
    line: int | None
    message: str


@dataclass
class DiffGuardReport:
    passed: bool
    role: AgentRole
    changed_files: list[str]
    added_lines: int
    deleted_lines: int
    touched_yellow_zone: bool
    yellow_files: list[str] = field(default_factory=list)
    red_violations: list[GuardViolation] = field(default_factory=list)
    scope_violations: list[GuardViolation] = field(default_factory=list)
    limit_violations: list[GuardViolation] = field(default_factory=list)
    placeholder_violations: list[GuardViolation] = field(default_factory=list)
    soft_limit_warnings: list[GuardViolation] = field(default_factory=list)
    prod_files_count: int = 0
    prod_changed_lines: int = 0
    test_files_count: int = 0
    test_changed_lines: int = 0
    filtered_diff: str = ""

    @property
    def all_violations(self) -> list[GuardViolation]:
        return [
            *self.red_violations,
            *self.scope_violations,
            *self.limit_violations,
            *self.placeholder_violations,
        ]

    @property
    def has_soft_warnings(self) -> bool:
        return bool(self.soft_limit_warnings)

    def summary_text(self) -> str:
        lines: list[str] = []
        for violation in self.all_violations:
            loc = f"{violation.path}:{violation.line}" if violation.path and violation.line else (
                violation.path or "<repo>"
            )
            lines.append(f"[{violation.code}] {loc}: {violation.message}")
        return "\n".join(lines)

    def soft_warning_text(self) -> str:
        if not self.soft_limit_warnings:
            return ""
        lines = [
            "DIFF_GUARD_SOFT_WARNING: Production diff exceeded soft threshold(s) "
            f"(prod_files={self.prod_files_count}, prod_lines={self.prod_changed_lines}, "
            f"test_files={self.test_files_count}, test_lines={self.test_changed_lines}). "
            "Review carefully for scope creep, unintended side effects, or maintainability issues:",
        ]
        for warning in self.soft_limit_warnings:
            lines.append(f"- [{warning.code}] {warning.message}")
        return "\n".join(lines)


def _normalize_rel_path(path: str) -> str:
    norm = path.replace("\\", "/")
    while norm.startswith("./"):
        norm = norm[2:]
    return norm.lstrip("/")


def _glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a POSIX glob pattern with proper `**` support into a compiled regex."""
    pat = _normalize_rel_path(pattern)
    i = 0
    n = len(pat)
    chunks: list[str] = ["^"]
    while i < n:
        ch = pat[i]
        if ch == "*":
            if i + 1 < n and pat[i + 1] == "*":
                if i + 2 < n and pat[i + 2] == "/":
                    # **/ matches zero or more directories
                    chunks.append("(?:.*/)?")
                    i += 3
                else:
                    chunks.append(".*")
                    i += 2
            else:
                chunks.append("[^/]*")
                i += 1
        elif ch == "?":
            chunks.append("[^/]")
            i += 1
        elif ch == "[":
            j = i + 1
            if j < n and pat[j] in ("!", "^"):
                j += 1
            if j < n and pat[j] == "]":
                j += 1
            while j < n and pat[j] != "]":
                j += 1
            if j >= n:
                chunks.append(re.escape(ch))
                i += 1
            else:
                stuff = pat[i + 1 : j].replace("\\", "\\\\")
                if stuff.startswith("!"):
                    stuff = "^" + stuff[1:]
                chunks.append(f"[{stuff}]")
                i = j + 1
        else:
            chunks.append(re.escape(ch))
            i += 1
    chunks.append("$")
    return re.compile("".join(chunks))


def matches_any_glob(rel_path: str, patterns: list[str]) -> bool:
    """Return True if rel_path matches any glob pattern (also treats bare dir prefixes safely)."""
    norm = _normalize_rel_path(rel_path)
    for raw_pattern in patterns:
        pat = _normalize_rel_path(raw_pattern)
        if not pat:
            continue
        if pat.endswith("/"):
            if norm.startswith(pat):
                return True
            continue
        if _glob_to_regex(pat).match(norm) is not None:
            return True
        # Convenience: allow specifying a directory path in allowed_paths without trailing /**
        if "*" not in pat and "?" not in pat and "[" not in pat and norm.startswith(pat + "/"):
            return True
    return False


_DIFF_HEADER_RE = re.compile(r"^diff --git a/(.+?) b/(.+?)$")
_HUNK_HEADER_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def _split_diff_sections(raw_diff: str) -> list[tuple[str, str]]:
    """Split a unified git diff into (b_path, section_text) pairs."""
    if not raw_diff:
        return []
    sections: list[tuple[str, str]] = []
    current_path: str | None = None
    current_lines: list[str] = []

    for line in raw_diff.splitlines(keepends=True):
        match = _DIFF_HEADER_RE.match(line.rstrip("\r\n"))
        if match:
            if current_path is not None:
                sections.append((current_path, "".join(current_lines)))
            current_path = _normalize_rel_path(match.group(2))
            current_lines = [line]
        elif current_path is not None:
            current_lines.append(line)

    if current_path is not None:
        sections.append((current_path, "".join(current_lines)))
    return sections


def filter_diff_for_review(raw_diff: str, exclude_patterns: list[str]) -> str:
    """Strip lock files, images, binaries, build outputs, and generated files from a diff."""
    if not raw_diff.strip():
        return ""
    kept: list[str] = []
    for path, section in _split_diff_sections(raw_diff):
        is_binary = "GIT binary patch" in section or "\nBinary files " in section
        if is_binary or matches_any_glob(path, exclude_patterns):
            reason = "binary" if is_binary else "excluded pattern"
            kept.append(f"# [omitted from review diff ({reason}): {path}]\n")
            continue
        kept.append(section)
    return "".join(kept)


def count_diff_lines(raw_diff: str, exclude_patterns: list[str] | None = None) -> tuple[int, int]:
    """Count (added_lines, deleted_lines) across non-excluded diff sections."""
    added = 0
    deleted = 0
    excludes = exclude_patterns or []
    for path, section in _split_diff_sections(raw_diff):
        if excludes and matches_any_glob(path, excludes):
            continue
        if "GIT binary patch" in section or "\nBinary files " in section:
            continue
        for line in section.splitlines():
            if line.startswith("+++") or line.startswith("---"):
                continue
            if line.startswith("+"):
                added += 1
            elif line.startswith("-"):
                deleted += 1
    return added, deleted


DEFAULT_PLACEHOLDER_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("TODO_MARKER", re.compile(r"\bTODO\b")),
    ("FIXME_MARKER", re.compile(r"\bFIXME\b")),
    ("NOT_IMPLEMENTED_ERROR", re.compile(r"\bNotImplementedError\b")),
    ("PASS_PLACEHOLDER", re.compile(r"^\s*pass\s*#\s*(?:TODO|FIXME|\.\.\.|stub|placeholder)", re.IGNORECASE)),
    ("ELLIPSIS_REST", re.compile(r"\.\.\.\s*#\s*(?:rest of|existing|todo|fixme|more|etc)", re.IGNORECASE)),
    ("COMMENT_EXISTING_CODE", re.compile(r"(?://|#|/\*)\s*\.\.\.\s*(?:existing|rest of|remaining|unchanged|more)\b", re.IGNORECASE)),
    ("JS_NOT_IMPLEMENTED", re.compile(r"throw\s+new\s+Error\(\s*['\"][^'\"]*not\s+implemented", re.IGNORECASE)),
    ("RUST_TODO_MACRO", re.compile(r"\b(?:todo|unimplemented)!\s*\(")),
    (
        "EMPTY_STUB_COMMENT",
        re.compile(
            r"(?://|#)\s*(?:(?:empty\s+)?stub\b\s*$|stub\s+(?:implementation|only|method|function)\b|placeholder\s+implementation\b|implement\s+me\b)",
            re.IGNORECASE,
        ),
    ),
]

_PY_DEF_RE = re.compile(r"^(\s*)(?:async\s+)?def\s+[A-Za-z_][A-Za-z0-9_]*\s*\(")
_PY_BARE_PASS_OR_ELLIPSIS_RE = re.compile(r"^(\s*)(?:pass|\.\.\.)\s*(?:#.*)?$")


def scan_placeholders(
    raw_diff: str,
    *,
    extra_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    allow_in_paths: list[str] | None = None,
) -> list[GuardViolation]:
    """Scan added (`+`) lines of a git diff for laziness/placeholder markers (0 tokens)."""
    compiled_patterns: list[tuple[str, re.Pattern[str]]] = list(DEFAULT_PLACEHOLDER_PATTERNS)
    for idx, pat in enumerate(extra_patterns or []):
        compiled_patterns.append((f"CUSTOM_PLACEHOLDER_{idx}", re.compile(pat)))

    excludes = exclude_patterns or []
    allowed = allow_in_paths or []
    violations: list[GuardViolation] = []

    for path, section in _split_diff_sections(raw_diff):
        if (excludes and matches_any_glob(path, excludes)) or (
            allowed and matches_any_glob(path, allowed)
        ):
            continue
        if "GIT binary patch" in section or "\nBinary files " in section:
            continue

        new_line_no = 0
        pending_def_indent: int | None = None
        is_python_file = path.endswith(".py")

        for raw_line in section.splitlines():
            hunk_match = _HUNK_HEADER_RE.match(raw_line)
            if hunk_match:
                new_line_no = int(hunk_match.group(1)) - 1
                pending_def_indent = None
                continue
            if raw_line.startswith("+++") or raw_line.startswith("---"):
                continue
            if raw_line.startswith("-"):
                continue
            if raw_line.startswith("+"):
                new_line_no += 1
                code_line = raw_line[1:]

                matched_any = False
                for code_name, regex in compiled_patterns:
                    if regex.search(code_line):
                        violations.append(
                            GuardViolation(
                                code=f"PLACEHOLDER_{code_name}",
                                path=path,
                                line=new_line_no,
                                message=f"Laziness/placeholder marker detected in added line: {code_line.strip()[:120]}",
                            )
                        )
                        matched_any = True
                        break

                if is_python_file and not matched_any:
                    def_match = _PY_DEF_RE.match(code_line)
                    if def_match:
                        pending_def_indent = len(def_match.group(1))
                    elif pending_def_indent is not None:
                        stripped = code_line.strip()
                        if not stripped or stripped.startswith(('"""', "'''")):
                            # Docstring or blank line right after def; stay open only on blank
                            if stripped:
                                pending_def_indent = None
                        else:
                            stub_match = _PY_BARE_PASS_OR_ELLIPSIS_RE.match(code_line)
                            if stub_match and len(stub_match.group(1)) > pending_def_indent:
                                violations.append(
                                    GuardViolation(
                                        code="PLACEHOLDER_EMPTY_FUNCTION_STUB",
                                        path=path,
                                        line=new_line_no,
                                        message=f"Empty function stub (`{stripped}`) detected in added Python function",
                                    )
                                )
                            pending_def_indent = None
            else:
                # Context line
                new_line_no += 1
                pending_def_indent = None

    return violations


def _is_path_allowed(
    rel_path: str,
    task_allowed: list[str],
    paired_paths: dict[str, list[str]] | None = None,
) -> bool:
    """Return True if rel_path matches task_allowed or is a paired lock/manifest in the same directory."""
    if matches_any_glob(rel_path, task_allowed):
        return True
    if not paired_paths:
        return False

    norm = _normalize_rel_path(rel_path)
    dir_prefix = norm.rsplit("/", 1)[0] if "/" in norm else ""

    for manifest_glob, companion_globs in paired_paths.items():
        if not matches_any_glob(norm, companion_globs):
            continue
        norm_manifest_glob = _normalize_rel_path(manifest_glob)
        manifest_basename = norm_manifest_glob.rsplit("/", 1)[-1]
        if not manifest_basename or any(ch in manifest_basename for ch in ("*", "?", "[")):
            continue
        sibling_manifest = f"{dir_prefix}/{manifest_basename}" if dir_prefix else manifest_basename
        if matches_any_glob(sibling_manifest, [norm_manifest_glob]) and matches_any_glob(
            sibling_manifest, task_allowed
        ):
            return True
    return False


def _resolve_diff_limit(
    role: AgentRole,
    config: PipelineConfig,
    task_complexity: TaskComplexity | None,
) -> DiffLimitConfig:
    if role == AgentRole.ARCHITECT:
        return config.diff_guard.architect_limit
    if role == AgentRole.REFACTORER:
        return config.diff_guard.refactorer_limit
    complexity = task_complexity or TaskComplexity.MEDIUM
    return config.diff_guard.limits[complexity]


def evaluate_diff(
    *,
    role: AgentRole,
    changes: list[GitChange],
    raw_diff: str,
    config: PipelineConfig,
    task_complexity: TaskComplexity | None = None,
    allowed_paths: list[str] | None = None,
) -> DiffGuardReport:
    """Evaluate role-aware Red/Yellow/Green zones, Diff Guard limits, and Placeholder Guard."""
    changed_files = sorted({_normalize_rel_path(c.path) for c in changes})
    filtered_diff = filter_diff_for_review(raw_diff, config.diff_guard.exclude_from_review)
    added_lines, deleted_lines = count_diff_lines(
        raw_diff,
        exclude_patterns=config.diff_guard.exclude_from_review,
    )

    red_violations: list[GuardViolation] = []
    scope_violations: list[GuardViolation] = []
    limit_violations: list[GuardViolation] = []
    placeholder_violations: list[GuardViolation] = []
    yellow_files: list[str] = []

    # 1. Read-only roles: zero repository modifications permitted (except readonly_ignore_paths).
    if role in READ_ONLY_ROLES:
        ro_ignores = config.diff_guard.readonly_ignore_paths
        effective_changes = [
            change
            for change in changes
            if not matches_any_glob(change.path, ro_ignores)
        ]
        effective_diff_sections = [
            (p, sec)
            for p, sec in _split_diff_sections(raw_diff)
            if not matches_any_glob(p, ro_ignores)
        ]
        effective_raw_diff = "".join(sec for _, sec in effective_diff_sections)
        if effective_changes or effective_raw_diff.strip():
            for change in effective_changes:
                red_violations.append(
                    GuardViolation(
                        code="READ_ONLY_ROLE_MODIFIED_FILES",
                        path=change.path,
                        line=None,
                        message=(
                            f"Read-only role '{role.value}' modified repository file "
                            f"({change.status} {change.path})"
                        ),
                    )
                )
            if not red_violations:
                red_violations.append(
                    GuardViolation(
                        code="READ_ONLY_ROLE_MODIFIED_FILES",
                        path=None,
                        line=None,
                        message=f"Read-only role '{role.value}' produced non-empty git diff",
                    )
                )
        return DiffGuardReport(
            passed=len(red_violations) == 0,
            role=role,
            changed_files=sorted({_normalize_rel_path(c.path) for c in effective_changes}),
            added_lines=added_lines,
            deleted_lines=deleted_lines,
            touched_yellow_zone=False,
            red_violations=red_violations,
            filtered_diff=filtered_diff,
        )

    zones = config.zones
    is_architect_role = role in ARCHITECT_ROLES
    task_allowed = allowed_paths or []
    paired_paths = config.diff_guard.paired_paths

    # 2. Three-zone policy per changed file
    for change in changes:
        paths_to_check = [_normalize_rel_path(change.path)]
        if change.old_path:
            paths_to_check.append(_normalize_rel_path(change.old_path))

        # 2a. Red protected paths (always forbidden for every role)
        if any(matches_any_glob(p, zones.red_protected) for p in paths_to_check):
            red_violations.append(
                GuardViolation(
                    code="RED_ZONE_PROTECTED_FILE",
                    path=change.path,
                    line=None,
                    message=f"Attempted to modify protected Red Zone path ({change.status} {change.path})",
                )
            )
            continue

        # 2b. Existing tests policy: M/D/R = instant violation; A = allowed (within allowed_paths for workers)
        is_test_file = any(matches_any_glob(p, zones.test_patterns) for p in paths_to_check)
        if is_test_file:
            if change.status != "A":
                red_violations.append(
                    GuardViolation(
                        code="RED_ZONE_TEST_MODIFIED_OR_DELETED",
                        path=change.path,
                        line=None,
                        message=(
                            f"Forbidden test file change (status={change.status}): "
                            f"modifying/deleting/renaming existing tests is strictly prohibited"
                        ),
                    )
                )
                continue
            if (
                not is_architect_role
                and task_allowed
                and not _is_path_allowed(change.path, task_allowed, paired_paths)
            ):
                scope_violations.append(
                    GuardViolation(
                        code="OUT_OF_ALLOWED_PATHS",
                        path=change.path,
                        line=None,
                        message=(
                            f"New test file '{change.path}' is outside task allowed_paths: "
                            f"{task_allowed}"
                        ),
                    )
                )
            continue

        # 2c. Yellow shared zone: permitted in solo mode, disables batch review -> strict Master review
        is_yellow = any(matches_any_glob(p, zones.yellow_shared) for p in paths_to_check)
        if is_yellow:
            yellow_files.append(_normalize_rel_path(change.path))
            if (
                zones.enforce_allowed_paths_on_yellow
                and not is_architect_role
                and task_allowed
                and not _is_path_allowed(change.path, task_allowed, paired_paths)
            ):
                scope_violations.append(
                    GuardViolation(
                        code="OUT_OF_ALLOWED_PATHS",
                        path=change.path,
                        line=None,
                        message=(
                            f"Yellow Zone file '{change.path}' is outside task allowed_paths: "
                            f"{task_allowed}"
                        ),
                    )
                )
            continue

        # 2d. Green zone: workers must stay within allowed_paths (if specified); architect/refactorer may span modules
        if (
            not is_architect_role
            and task_allowed
            and not _is_path_allowed(change.path, task_allowed, paired_paths)
        ):
            scope_violations.append(
                GuardViolation(
                    code="OUT_OF_ALLOWED_PATHS",
                    path=change.path,
                    line=None,
                    message=f"File '{change.path}' is outside task allowed_paths: {task_allowed}",
                )
            )

    # 3. Diff Guard file & line limits (excluding exclude_from_review artifacts/generated files;
    #    optionally excluding test_patterns from production limits with a dedicated test-line ceiling)
    limit = _resolve_diff_limit(role, config, task_complexity)
    dg_cfg = config.diff_guard
    hard_max_files = limit.hard_max_files or 1
    hard_max_lines = limit.hard_max_lines or 1

    countable_all_files = [
        f for f in changed_files if not matches_any_glob(f, dg_cfg.exclude_from_review)
    ]
    test_files = [f for f in countable_all_files if matches_any_glob(f, zones.test_patterns)]
    prod_files = [f for f in countable_all_files if not matches_any_glob(f, zones.test_patterns)]

    test_added = 0
    test_deleted = 0
    prod_added = 0
    prod_deleted = 0
    for path, section in _split_diff_sections(raw_diff):
        if matches_any_glob(path, dg_cfg.exclude_from_review):
            continue
        if "GIT binary patch" in section or "\nBinary files " in section:
            continue
        sec_added = 0
        sec_deleted = 0
        for line in section.splitlines():
            if line.startswith("+++") or line.startswith("---"):
                continue
            if line.startswith("+"):
                sec_added += 1
            elif line.startswith("-"):
                sec_deleted += 1
        if matches_any_glob(path, zones.test_patterns):
            test_added += sec_added
            test_deleted += sec_deleted
        else:
            prod_added += sec_added
            prod_deleted += sec_deleted

    test_changed_lines = test_added + test_deleted
    if dg_cfg.exclude_tests_from_limits:
        counted_files = prod_files
        counted_added = prod_added
        counted_deleted = prod_deleted
    else:
        counted_files = countable_all_files
        counted_added = added_lines
        counted_deleted = deleted_lines
    counted_lines = counted_added + counted_deleted

    soft_limit_warnings: list[GuardViolation] = []

    if len(counted_files) > hard_max_files:
        limit_violations.append(
            GuardViolation(
                code="DIFF_GUARD_MAX_FILES",
                path=None,
                line=None,
                message=(
                    f"Changed {len(counted_files)} countable files "
                    f"({len(changed_files)} total, {len(test_files)} test files), "
                    f"exceeding Diff Guard hard limit of {hard_max_files} files for {role.value}"
                ),
            )
        )
    elif limit.soft_max_files is not None and len(counted_files) > limit.soft_max_files:
        soft_limit_warnings.append(
            GuardViolation(
                code="DIFF_GUARD_SOFT_MAX_FILES",
                path=None,
                line=None,
                message=(
                    f"Changed {len(counted_files)} production files "
                    f"(soft={limit.soft_max_files}, hard={hard_max_files}) for {role.value}"
                ),
            )
        )

    if counted_lines > hard_max_lines:
        limit_violations.append(
            GuardViolation(
                code="DIFF_GUARD_MAX_LINES",
                path=None,
                line=None,
                message=(
                    f"Changed {counted_lines} lines (+{counted_added}/-{counted_deleted}), "
                    f"exceeding Diff Guard hard limit of {hard_max_lines} lines for {role.value}"
                ),
            )
        )
    elif limit.soft_max_lines is not None and counted_lines > limit.soft_max_lines:
        soft_limit_warnings.append(
            GuardViolation(
                code="DIFF_GUARD_SOFT_MAX_LINES",
                path=None,
                line=None,
                message=(
                    f"Changed {counted_lines} production lines (+{counted_added}/-{counted_deleted}, "
                    f"soft={limit.soft_max_lines}, hard={hard_max_lines}) for {role.value}"
                ),
            )
        )

    if dg_cfg.exclude_tests_from_limits and test_changed_lines > dg_cfg.hard_max_test_lines:
        limit_violations.append(
            GuardViolation(
                code="DIFF_GUARD_MAX_TEST_LINES",
                path=None,
                line=None,
                message=(
                    f"Changed {test_changed_lines} test lines (+{test_added}/-{test_deleted}), "
                    f"exceeding Diff Guard hard_max_test_lines ceiling of {dg_cfg.hard_max_test_lines}"
                ),
            )
        )

    # 4. Placeholder Guard (0 tokens)
    if config.placeholder_guard.enabled:
        placeholder_violations = scan_placeholders(
            raw_diff,
            extra_patterns=config.placeholder_guard.extra_patterns,
            exclude_patterns=config.diff_guard.exclude_from_review,
            allow_in_paths=config.placeholder_guard.allow_in_paths,
        )

    passed = (
        not red_violations
        and not scope_violations
        and not limit_violations
        and not placeholder_violations
    )
    return DiffGuardReport(
        passed=passed,
        role=role,
        changed_files=changed_files,
        added_lines=added_lines,
        deleted_lines=deleted_lines,
        touched_yellow_zone=len(yellow_files) > 0,
        yellow_files=sorted(set(yellow_files)),
        red_violations=red_violations,
        scope_violations=scope_violations,
        limit_violations=limit_violations,
        placeholder_violations=placeholder_violations,
        soft_limit_warnings=soft_limit_warnings,
        prod_files_count=len(prod_files),
        prod_changed_lines=prod_added + prod_deleted,
        test_files_count=len(test_files),
        test_changed_lines=test_changed_lines,
        filtered_diff=filtered_diff,
    )
