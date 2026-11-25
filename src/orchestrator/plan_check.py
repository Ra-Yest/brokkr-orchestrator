"""Pre-flight plan audit (`orchestrator plan-check`): deterministic checks + optional planner LLM audit."""

from __future__ import annotations

import json
import re
import shlex
from collections import deque
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from orchestrator.config import PipelineConfig
from orchestrator.diff_guard import _normalize_rel_path, matches_any_glob
from orchestrator.models import AgentRole, Task, TaskComplexity, TasksFile, TaskStatus
from orchestrator.output_parser import _extract_block, _unwrap_json_fence
from orchestrator.prompt_builder import PromptBuilder, generate_repo_map
from orchestrator.providers.base import ProviderRegistry, ProviderRequest


class PlanCheckSeverity(StrEnum):
    BLOCKER = "BLOCKER"
    WARNING = "WARNING"
    INFO = "INFO"


class PlanCheckFinding(BaseModel):
    severity: PlanCheckSeverity
    code: str
    task_id: str | None = None
    message: str


class PlanCheckReport(BaseModel):
    findings: list[PlanCheckFinding] = Field(default_factory=list)
    llm_used: bool = False

    @property
    def has_blockers(self) -> bool:
        return any(f.severity == PlanCheckSeverity.BLOCKER for f in self.findings)

    @property
    def blocker_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == PlanCheckSeverity.BLOCKER)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == PlanCheckSeverity.WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == PlanCheckSeverity.INFO)


_ACTIVE_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.TODO, TaskStatus.IN_PROGRESS})
_TEST_MENTION_RE = re.compile(r"\b(?:test\w*|unit|fuzz\w*|widget|golden)\b", re.IGNORECASE)
_BRACKET_GLOB_RE = re.compile(r"\[[^\]]+\]")


def _has_glob_magic(segment: str) -> bool:
    return any(ch in segment for ch in ("*", "?", "["))


def _sample_paths_for_glob(pattern: str) -> list[str]:
    """Generate representative concrete relative paths matching a glob pattern."""
    pat = _normalize_rel_path(pattern)
    if not pat:
        return []
    if pat.endswith("/"):
        pat = pat + "sample"

    def _subst(raw: str, *, deep_dir: str, double_star: str) -> str:
        s = raw.replace("**/", deep_dir)
        s = s.replace("**", double_star)
        s = _BRACKET_GLOB_RE.sub("a", s)
        s = s.replace("*", "sample").replace("?", "a")
        return _normalize_rel_path(s)

    candidates = [
        _subst(pat, deep_dir="", double_star="sample"),
        _subst(pat, deep_dir="dir/", double_star="dir/sample"),
    ]
    seen: list[str] = []
    for cand in candidates:
        if cand and cand not in seen:
            seen.append(cand)
    return seen


def _allowed_intersects_protected(allowed_pat: str, protected_pat: str) -> bool:
    """Return True if an allowed_paths pattern overlaps a protected Red/Contract pattern.

    Avoids false positives where a directory glob like `src/**` would theoretically contain
    wildcard-anywhere bans such as `**/.env*`.
    """
    ap = _normalize_rel_path(allowed_pat)
    rp = _normalize_rel_path(protected_pat)
    if not ap or not rp:
        return False
    if ap == rp:
        return True
    if any(matches_any_glob(sample, [rp]) for sample in _sample_paths_for_glob(ap)):
        return True
    rp_first = rp.split("/", 1)[0]
    return bool(
        rp_first
        and not _has_glob_magic(rp_first)
        and any(matches_any_glob(sample, [ap]) for sample in _sample_paths_for_glob(rp))
    )


def _patterns_overlap(pat_a: str, pat_b: str) -> bool:
    """Return True if two allowed_paths glob patterns can match a common path."""
    a = _normalize_rel_path(pat_a)
    b = _normalize_rel_path(pat_b)
    if not a or not b:
        return False
    if a == b:
        return True
    if any(matches_any_glob(sample, [b]) for sample in _sample_paths_for_glob(a)):
        return True
    return any(matches_any_glob(sample, [a]) for sample in _sample_paths_for_glob(b))


def _tasks_have_disjoint_allowed_paths(task_a: Task, task_b: Task) -> bool:
    if not task_a.allowed_paths or not task_b.allowed_paths:
        return False
    for pa in task_a.allowed_paths:
        for pb in task_b.allowed_paths:
            if _patterns_overlap(pa, pb):
                return False
    return True


def _pattern_matches_test_zone(pattern: str, test_patterns: list[str]) -> bool:
    norm = _normalize_rel_path(pattern)
    if not norm or not test_patterns:
        return False
    if matches_any_glob(norm, test_patterns):
        return True
    if any(matches_any_glob(sample, test_patterns) for sample in _sample_paths_for_glob(norm)):
        return True
    for tp in test_patterns:
        norm_tp = _normalize_rel_path(tp)
        tp_first = norm_tp.split("/", 1)[0]
        if (
            tp_first
            and not _has_glob_magic(tp_first)
            and any(matches_any_glob(sample, [norm]) for sample in _sample_paths_for_glob(norm_tp))
        ):
            return True
    return False


def _concrete_root_dir(pattern: str) -> str | None:
    """Return the first-level directory name if pattern starts with a concrete directory."""
    norm = _normalize_rel_path(pattern)
    if not norm:
        return None
    parts = norm.split("/")
    if len(parts) < 2:
        return None
    first = parts[0]
    if not first or _has_glob_magic(first):
        return None
    return first


def _transitive_ancestors(task_id: str, by_id: dict[str, Task]) -> set[str]:
    """Return all direct and transitive depends_on task IDs (cycle-safe)."""
    task = by_id.get(task_id)
    if task is None:
        return set()
    visited: set[str] = set()
    queue: deque[str] = deque(task.depends_on)
    while queue:
        dep = queue.popleft()
        if dep in visited or dep == task_id:
            continue
        visited.add(dep)
        dep_task = by_id.get(dep)
        if dep_task is not None:
            queue.extend(dep_task.depends_on)
    return visited


def _find_disjoint_group_of_at_least_three(tasks: list[Task]) -> list[Task]:
    """Return a subset of >= 3 pairwise-disjoint tasks if one exists, else empty list."""
    n = len(tasks)
    if n < 3:
        return []
    disjoint = [
        [_tasks_have_disjoint_allowed_paths(tasks[i], tasks[j]) for j in range(n)]
        for i in range(n)
    ]
    for i in range(n):
        for j in range(i + 1, n):
            if not disjoint[i][j]:
                continue
            for k in range(j + 1, n):
                if disjoint[i][k] and disjoint[j][k]:
                    group = [i, j, k]
                    for m in range(k + 1, n):
                        if all(disjoint[m][g] for g in group):
                            group.append(m)
                    return [tasks[idx] for idx in group]
    return []


def _check_dag_and_consistency(tasks_file: TasksFile) -> tuple[dict[str, Task], list[PlanCheckFinding]]:
    findings: list[PlanCheckFinding] = []
    by_id: dict[str, Task] = {}

    for task in tasks_file.tasks:
        if task.id in by_id:
            findings.append(
                PlanCheckFinding(
                    severity=PlanCheckSeverity.BLOCKER,
                    code="DUPLICATE_TASK_ID",
                    task_id=task.id,
                    message=f"Duplicate task ID '{task.id}' found in tasks.json.",
                )
            )
        else:
            by_id[task.id] = task

    for task in tasks_file.tasks:
        for dep_id in task.depends_on:
            if dep_id not in by_id:
                findings.append(
                    PlanCheckFinding(
                        severity=PlanCheckSeverity.BLOCKER,
                        code="DANGLING_DEPENDENCY",
                        task_id=task.id,
                        message=f"Task '{task.id}' depends_on unknown task ID '{dep_id}'.",
                    )
                )

    # Cycle detection (0=unvisited, 1=visiting, 2=visited)
    state: dict[str, int] = {tid: 0 for tid in by_id}
    path_stack: list[str] = []
    reported_cycles: set[tuple[str, ...]] = set()

    def _dfs(tid: str) -> None:
        state[tid] = 1
        path_stack.append(tid)
        for dep in by_id[tid].depends_on:
            if dep not in by_id:
                continue
            if state[dep] == 1:
                cycle_start = path_stack.index(dep)
                cycle_nodes = [*path_stack[cycle_start:], dep]
                canonical = tuple(sorted(set(cycle_nodes)))
                if canonical not in reported_cycles:
                    reported_cycles.add(canonical)
                    findings.append(
                        PlanCheckFinding(
                            severity=PlanCheckSeverity.BLOCKER,
                            code="DEPENDENCY_CYCLE",
                            task_id=tid,
                            message=f"Dependency cycle detected: {' -> '.join(cycle_nodes)}.",
                        )
                    )
            elif state[dep] == 0:
                _dfs(dep)
        path_stack.pop()
        state[tid] = 2

    for tid in by_id:
        if state[tid] == 0:
            _dfs(tid)

    # Done tasks without final_commit_sha (excluding split parents)
    for task in tasks_file.tasks:
        if (
            task.status == TaskStatus.DONE
            and task.resolution != "split"
            and not (task.final_commit_sha and task.final_commit_sha.strip())
        ):
            findings.append(
                PlanCheckFinding(
                    severity=PlanCheckSeverity.WARNING,
                    code="DONE_MISSING_COMMIT_SHA",
                    task_id=task.id,
                    message=(
                        f"Task '{task.id}' is marked 'done' (resolution={task.resolution}) "
                        "but has an empty final_commit_sha."
                    ),
                )
            )

    return by_id, findings


def _check_deterministic(
    repo_root: Path,
    config: PipelineConfig,
    tasks_file: TasksFile,
) -> list[PlanCheckFinding]:
    by_id, findings = _check_dag_and_consistency(tasks_file)
    ancestors_cache: dict[str, set[str]] = {
        tid: _transitive_ancestors(tid, by_id) for tid in by_id
    }

    protected_patterns = [
        *config.zones.red_protected,
        *config.diff_guard.contracts_protected_paths,
    ]
    active_tasks = [t for t in tasks_file.tasks if t.status in _ACTIVE_STATUSES]

    # 2. allowed_paths against repo tree & zones
    for task in active_tasks:
        ancestors = ancestors_cache.get(task.id, set())
        ancestor_root_dirs: set[str] = set()
        for aid in ancestors:
            anc = by_id.get(aid)
            if anc is None:
                continue
            for anc_pat in anc.allowed_paths:
                root_dir = _concrete_root_dir(anc_pat)
                if root_dir:
                    ancestor_root_dirs.add(root_dir)
                else:
                    norm_anc = _normalize_rel_path(anc_pat)
                    if norm_anc and "/" not in norm_anc and not _has_glob_magic(norm_anc):
                        ancestor_root_dirs.add(norm_anc)

        warned_roots: set[str] = set()
        for ap in task.allowed_paths:
            for rp in protected_patterns:
                if _allowed_intersects_protected(ap, rp):
                    findings.append(
                        PlanCheckFinding(
                            severity=PlanCheckSeverity.BLOCKER,
                            code="ALLOWED_PATH_PROTECTED_OVERLAP",
                            task_id=task.id,
                            message=(
                                f"allowed_paths entry '{ap}' overlaps protected path pattern '{rp}'."
                            ),
                        )
                    )
                    break

            root_dir = _concrete_root_dir(ap)
            if (
                root_dir
                and root_dir not in warned_roots
                and not (repo_root / root_dir).exists()
                and root_dir not in ancestor_root_dirs
            ):
                warned_roots.add(root_dir)
                findings.append(
                    PlanCheckFinding(
                        severity=PlanCheckSeverity.WARNING,
                        code="NONEXISTENT_ALLOWED_PATH_ROOT",
                        task_id=task.id,
                        message=(
                            f"Top-level directory '{root_dir}' in allowed_paths ('{ap}') "
                            "does not exist on disk and is not created by any ancestor task."
                        ),
                    )
                )

        # Test coverage check when tests are mentioned in description / acceptance_criteria
        combined_text = "\n".join([task.description, *task.acceptance_criteria])
        if task.allowed_paths and _TEST_MENTION_RE.search(combined_text):
            has_test_path = any(
                _pattern_matches_test_zone(ap, config.zones.test_patterns)
                for ap in task.allowed_paths
            )
            if not has_test_path:
                findings.append(
                    PlanCheckFinding(
                        severity=PlanCheckSeverity.WARNING,
                        code="TESTS_NOT_IN_ALLOWED_PATHS",
                        task_id=task.id,
                        message=(
                            "Task mentions tests in description/acceptance_criteria, "
                            "but no allowed_paths pattern matches zones.test_patterns."
                        ),
                    )
                )

    # 3. context_files check for active tasks
    for task in active_tasks:
        ancestors = ancestors_cache.get(task.id, set())
        ancestor_allowed = [
            p
            for aid in ancestors
            if aid in by_id
            for p in by_id[aid].allowed_paths
        ]
        for cf in task.context_files:
            norm_cf = _normalize_rel_path(cf)
            if not norm_cf:
                continue
            if (repo_root / norm_cf).exists():
                continue
            if ancestor_allowed and matches_any_glob(norm_cf, ancestor_allowed):
                continue
            findings.append(
                PlanCheckFinding(
                    severity=PlanCheckSeverity.BLOCKER,
                    code="MISSING_CONTEXT_FILE",
                    task_id=task.id,
                    message=(
                        f"context_files entry '{cf}' does not exist on disk and is not covered "
                        "by any transitive ancestor task's allowed_paths."
                    ),
                )
            )

    # 4. verify_cmd checks (empty + copy-paste detector)
    global_gate_cmds: set[str] = set()
    for gate in config.gates:
        global_gate_cmds.add(" ".join(gate.command).strip())
        global_gate_cmds.add(shlex.join(gate.command).strip())

    cmd_groups: dict[str, list[Task]] = {}
    for task in active_tasks:
        cmd = (task.verify_cmd or "").strip()
        if not cmd:
            findings.append(
                PlanCheckFinding(
                    severity=PlanCheckSeverity.WARNING,
                    code="EMPTY_VERIFY_CMD",
                    task_id=task.id,
                    message=f"Active task '{task.id}' has an empty verify_cmd.",
                )
            )
        else:
            cmd_groups.setdefault(cmd, []).append(task)

    for cmd, group_tasks in cmd_groups.items():
        if len(group_tasks) < 3 or cmd in global_gate_cmds:
            continue
        disjoint_subset = _find_disjoint_group_of_at_least_three(group_tasks)
        if len(disjoint_subset) >= 3:
            ids_str = ", ".join(t.id for t in disjoint_subset)
            findings.append(
                PlanCheckFinding(
                    severity=PlanCheckSeverity.WARNING,
                    code="COPYPASTE_VERIFY_CMD",
                    task_id=None,
                    message=(
                        f"verify_cmd `{cmd}` is duplicated verbatim across {len(disjoint_subset)} "
                        f"disjoint active tasks ({ids_str}) and does not match any global gate."
                    ),
                )
            )

    # 5. Task size / split candidate (> 2 top-level non-test directories for easy/medium)
    for task in active_tasks:
        if task.complexity not in (TaskComplexity.EASY, TaskComplexity.MEDIUM):
            continue
        top_dirs: set[str] = set()
        for ap in task.allowed_paths:
            if _pattern_matches_test_zone(ap, config.zones.test_patterns):
                continue
            root_dir = _concrete_root_dir(ap)
            if root_dir:
                top_dirs.add(root_dir)
        if len(top_dirs) > 2:
            dirs_str = ", ".join(sorted(top_dirs))
            findings.append(
                PlanCheckFinding(
                    severity=PlanCheckSeverity.WARNING,
                    code="OVERSIZED_TASK_SCOPE",
                    task_id=task.id,
                    message=(
                        f"{task.complexity.value} task '{task.id}' spans {len(top_dirs)} "
                        f"top-level non-test directories ({dirs_str}); consider splitting."
                    ),
                )
            )

    # 6. Contract topology checks
    has_contracts = bool(tasks_file.contracts) or any(
        t.provides or t.consumes for t in tasks_file.tasks
    )
    if has_contracts:
        contracts = tasks_file.contracts
        for task in tasks_file.tasks:
            for cid in task.provides:
                if cid not in contracts:
                    findings.append(
                        PlanCheckFinding(
                            severity=PlanCheckSeverity.BLOCKER,
                            code="UNKNOWN_CONTRACT_ID",
                            task_id=task.id,
                            message=(
                                f"Task '{task.id}' provides unknown contract ID '{cid}' "
                                "(not defined in tasks.json contracts)."
                            ),
                        )
                    )
            ancestors = ancestors_cache.get(task.id, set())
            for cid in task.consumes:
                if cid not in contracts:
                    findings.append(
                        PlanCheckFinding(
                            severity=PlanCheckSeverity.BLOCKER,
                            code="UNKNOWN_CONTRACT_ID",
                            task_id=task.id,
                            message=(
                                f"Task '{task.id}' consumes unknown contract ID '{cid}' "
                                "(not defined in tasks.json contracts)."
                            ),
                        )
                    )
                    continue
                has_producer = any(
                    cid in by_id[aid].provides for aid in ancestors if aid in by_id
                )
                if not has_producer:
                    findings.append(
                        PlanCheckFinding(
                            severity=PlanCheckSeverity.BLOCKER,
                            code="UNSATISFIED_CONTRACT_CONSUMER",
                            task_id=task.id,
                            message=(
                                f"Task '{task.id}' consumes contract '{cid}', "
                                "but no transitive ancestor in depends_on lists it in provides."
                            ),
                        )
                    )

        for cid, spec in contracts.items():
            if not spec.spec_source or not spec.spec_source.strip():
                continue
            norm_spec = _normalize_rel_path(spec.spec_source.strip())
            if (repo_root / norm_spec).exists():
                continue
            producers = [t for t in tasks_file.tasks if cid in t.provides]
            allowed_candidates: list[str] = []
            for prod in producers:
                allowed_candidates.extend(prod.allowed_paths)
                for aid in ancestors_cache.get(prod.id, set()):
                    if aid in by_id:
                        allowed_candidates.extend(by_id[aid].allowed_paths)
            if not (allowed_candidates and matches_any_glob(norm_spec, allowed_candidates)):
                findings.append(
                    PlanCheckFinding(
                        severity=PlanCheckSeverity.WARNING,
                        code="MISSING_CONTRACT_SPEC_SOURCE",
                        task_id=producers[0].id if len(producers) == 1 else None,
                        message=(
                            f"Contract '{cid}' spec_source '{spec.spec_source}' does not exist "
                            "on disk and is not covered by producer/ancestor allowed_paths."
                        ),
                    )
                )

    return findings


def parse_plan_audit_findings(text: str) -> list[PlanCheckFinding]:
    """Parse BEGIN_PLAN_AUDIT_JSON..END_PLAN_AUDIT_JSON into validated PlanCheckFinding items."""
    raw_block = _extract_block(text, "BEGIN_PLAN_AUDIT_JSON", "END_PLAN_AUDIT_JSON")
    if raw_block is None:
        raise ValueError(
            "Planner LLM audit output missing BEGIN_PLAN_AUDIT_JSON..END_PLAN_AUDIT_JSON block"
        )
    json_text = _unwrap_json_fence(raw_block)
    payload = json.loads(json_text)
    if not isinstance(payload, list):
        raise ValueError("BEGIN_PLAN_AUDIT_JSON payload must be a JSON array")

    parsed: list[PlanCheckFinding] = []
    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Plan audit entry #{idx} must be a JSON object")
        normalized: dict[str, Any] = dict(item)
        if isinstance(normalized.get("severity"), str):
            normalized["severity"] = normalized["severity"].strip().upper()
        normalized.setdefault("task_id", None)
        parsed.append(PlanCheckFinding.model_validate(normalized))
    return parsed


def _run_llm_audit(
    repo_root: Path,
    config: PipelineConfig,
    tasks_file: TasksFile,
    *,
    registry: ProviderRegistry | None = None,
) -> list[PlanCheckFinding]:
    try:
        effective_registry = registry or ProviderRegistry.from_config(config)
        provider = effective_registry.for_role(AgentRole.PLANNER)
    except Exception as exc:
        return [
            PlanCheckFinding(
                severity=PlanCheckSeverity.WARNING,
                code="LLM_AUDIT_PROVIDER_ERROR",
                task_id=None,
                message=f"Could not initialize planner provider for --llm audit: {exc}",
            )
        ]

    pb = PromptBuilder(repo_root, config)
    static_prefix = pb.build_static_prefix(AgentRole.PLANNER, provider.profile)
    repo_map_text = generate_repo_map(
        repo_root,
        config.project.repo_map_ignore,
        pipeline_dir=config.project.pipeline_dir,
    )
    active_tasks = [t for t in tasks_file.tasks if t.status in _ACTIVE_STATUSES]

    zones_summary = {
        "zones": config.zones.model_dump(mode="json"),
        "diff_guard": {
            "contracts_protected_paths": config.diff_guard.contracts_protected_paths,
            "exclude_tests_from_limits": config.diff_guard.exclude_tests_from_limits,
            "limits": {k.value: v.model_dump(mode="json") for k, v in config.diff_guard.limits.items()},
        },
    }
    tasks_summary = [
        {
            "id": t.id,
            "title": t.title,
            "complexity": t.complexity.value,
            "status": t.status.value,
            "depends_on": t.depends_on,
            "allowed_paths": t.allowed_paths,
            "context_files": t.context_files,
            "verify_cmd": t.verify_cmd,
            "provides": t.provides,
            "consumes": t.consumes,
            "acceptance_criteria": t.acceptance_criteria,
        }
        for t in active_tasks
    ]
    contracts_summary = {
        cid: spec.model_dump(mode="json") for cid, spec in tasks_file.contracts.items()
    }

    dynamic_body = "\n\n---\n\n".join(
        [
            f"## REPOSITORY MAP\n\n{repo_map_text.strip()}",
            "## ZONES & DIFF GUARD CONFIG\n\n```json\n"
            + json.dumps(zones_summary, indent=2, ensure_ascii=False)
            + "\n```",
            "## ACTIVE TASKS\n\n```json\n"
            + json.dumps(tasks_summary, indent=2, ensure_ascii=False)
            + "\n```",
            "## CONTRACTS\n\n```json\n"
            + json.dumps(contracts_summary, indent=2, ensure_ascii=False)
            + "\n```",
            (
                "## PLAN AUDIT INSTRUCTIONS\n\n"
                "You are running in read-only Plan Audit mode (`orchestrator plan-check --llm`). "
                "Do NOT modify any files. Review the active tasks and contracts for semantic gaps, "
                "missing dependencies, ambiguous acceptance criteria, contract clause omissions, "
                "or unrealistic scope. Output ONLY a valid JSON array wrapped in "
                "`BEGIN_PLAN_AUDIT_JSON` and `END_PLAN_AUDIT_JSON`, where each item has "
                "`{severity: 'BLOCKER'|'WARNING'|'INFO', task_id: string|null, code: string, message: string}`."
            ),
        ]
    )
    full_prompt = f"{static_prefix}\n\n===\n\n{dynamic_body}\n" if static_prefix else f"{dynamic_body}\n"
    attempt_dir = config.pipeline_path(repo_root) / "plan_check"
    attempt_dir.mkdir(parents=True, exist_ok=True)

    request = ProviderRequest(
        run_id="plan_check",
        task_id=None,
        role=AgentRole.PLANNER,
        prompt=full_prompt,
        static_prefix=static_prefix,
        dynamic_body=dynamic_body,
        workdir=repo_root,
        timeout_seconds=provider.profile.timeout_seconds,
        idle_timeout_seconds=provider.profile.idle_timeout_seconds,
        attempt_dir=attempt_dir,
    )
    try:
        response = provider.invoke(request)
    except Exception as exc:
        return [
            PlanCheckFinding(
                severity=PlanCheckSeverity.WARNING,
                code="LLM_AUDIT_FAILED",
                task_id=None,
                message=f"Planner LLM audit invocation failed: {exc}",
            )
        ]

    if response.exit_code != 0 or response.timed_out or response.idle_timed_out:
        return [
            PlanCheckFinding(
                severity=PlanCheckSeverity.WARNING,
                code="LLM_AUDIT_FAILED",
                task_id=None,
                message=(
                    f"Planner LLM audit returned error (exit_code={response.exit_code}, "
                    f"timed_out={response.timed_out}, idle_timed_out={response.idle_timed_out})."
                ),
            )
        ]

    try:
        return parse_plan_audit_findings(response.parsed_text)
    except (ValueError, json.JSONDecodeError, ValidationError) as exc:
        return [
            PlanCheckFinding(
                severity=PlanCheckSeverity.WARNING,
                code="LLM_AUDIT_INVALID_OUTPUT",
                task_id=None,
                message=f"Could not parse planner LLM audit output: {exc}",
            )
        ]


def run_plan_check(
    repo_root: Path,
    config: PipelineConfig,
    *,
    tasks_path: Path | None = None,
    use_llm: bool = False,
    registry: ProviderRegistry | None = None,
) -> PlanCheckReport:
    """Run deterministic pre-flight plan audit checks and optional read-only planner LLM audit."""
    root = repo_root.resolve()
    tpath = tasks_path if tasks_path is not None else config.tasks_path(root)

    if not tpath.exists():
        return PlanCheckReport(
            findings=[
                PlanCheckFinding(
                    severity=PlanCheckSeverity.BLOCKER,
                    code="TASKS_FILE_MISSING",
                    task_id=None,
                    message=f"Tasks file not found: {tpath}",
                )
            ],
            llm_used=False,
        )

    try:
        raw_data = json.loads(tpath.read_text(encoding="utf-8"))
        tasks_file = TasksFile.model_validate(raw_data)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        return PlanCheckReport(
            findings=[
                PlanCheckFinding(
                    severity=PlanCheckSeverity.BLOCKER,
                    code="TASKS_SCHEMA_INVALID",
                    task_id=None,
                    message=f"Failed to load/validate tasks.json: {exc}",
                )
            ],
            llm_used=False,
        )

    findings = _check_deterministic(root, config, tasks_file)
    llm_used = False
    if use_llm:
        llm_used = True
        findings.extend(_run_llm_audit(root, config, tasks_file, registry=registry))

    return PlanCheckReport(findings=findings, llm_used=llm_used)


def format_plan_check_table(report: PlanCheckReport) -> str:
    """Format a PlanCheckReport into a compact console table."""
    lines: list[str] = []
    if not report.findings:
        lines.append("Plan check passed: 0 blockers, 0 warnings, 0 info.")
        return "\n".join(lines)

    sev_w = max(len("SEVERITY"), *(len(f.severity.value) for f in report.findings))
    task_w = max(len("TASK"), *(len(f.task_id or "-") for f in report.findings))
    code_w = max(len("CODE"), *(len(f.code) for f in report.findings))

    header = f"{'SEVERITY':<{sev_w}} | {'TASK':<{task_w}} | {'CODE':<{code_w}} | MESSAGE"
    sep = f"{'-' * sev_w}-+-{'-' * task_w}-+-{'-' * code_w}-+--------"
    lines.append(header)
    lines.append(sep)
    for finding in report.findings:
        tid = finding.task_id or "-"
        lines.append(
            f"{finding.severity.value:<{sev_w}} | {tid:<{task_w}} | {finding.code:<{code_w}} | {finding.message}"
        )

    lines.append(
        f"\nSummary: {report.blocker_count} blocker(s), "
        f"{report.warning_count} warning(s), {report.info_count} info."
    )
    return "\n".join(lines)
