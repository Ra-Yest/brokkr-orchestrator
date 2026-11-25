"""Core Pydantic v2 domain models for tasks, events, state, control, and agent outputs."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format with Z suffix."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class AgentRole(StrEnum):
    PLANNER = "planner"
    WORKER_EASY = "worker_easy"
    WORKER_MEDIUM = "worker_medium"
    WORKER_HARD = "worker_hard"
    PRE_REVIEWER = "pre_reviewer"
    REVIEWER = "reviewer"
    SEAM_AUDITOR = "seam_auditor"
    ARCHITECT = "architect"
    REFACTORER = "refactorer"


WORKER_ROLES: frozenset[AgentRole] = frozenset(
    {AgentRole.WORKER_EASY, AgentRole.WORKER_MEDIUM, AgentRole.WORKER_HARD}
)
ARCHITECT_ROLES: frozenset[AgentRole] = frozenset(
    {AgentRole.ARCHITECT, AgentRole.REFACTORER}
)
WRITING_ROLES: frozenset[AgentRole] = WORKER_ROLES | ARCHITECT_ROLES
READ_ONLY_ROLES: frozenset[AgentRole] = frozenset(
    {
        AgentRole.PLANNER,
        AgentRole.PRE_REVIEWER,
        AgentRole.REVIEWER,
        AgentRole.SEAM_AUDITOR,
    }
)


class TaskStatus(StrEnum):
    TODO = "todo"
    IN_PROGRESS = "in_progress"
    GATES_FAILED = "gates_failed"
    NEEDS_REVIEW = "needs_review"
    BATCH_PENDING = "batch_pending"
    ARCH_PENDING = "arch_pending"
    DONE = "done"
    FAILED = "failed"
    BLOCKED = "blocked"


class TaskComplexity(StrEnum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"


class ContractKind(StrEnum):
    ARTIFACT = "artifact"
    ENTITY = "entity"
    VALUE = "value"
    WIRE = "wire"
    FSM = "fsm"


class ContractClause(BaseModel):
    id: str = Field(min_length=1)
    rule: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_non_blank(self) -> ContractClause:
        if not self.id.strip():
            raise ValueError("ContractClause 'id' must not be blank")
        if not self.rule.strip():
            raise ValueError("ContractClause 'rule' must not be blank")
        return self


class ContractSpec(BaseModel):
    kind: ContractKind
    surface: str = ""
    description: str = ""
    spec_source: str | None = None
    fixtures: list[str] = Field(default_factory=list)
    clauses: list[ContractClause] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_contract(self) -> ContractSpec:
        seen_ids: set[str] = set()
        for clause in self.clauses:
            if clause.id in seen_ids:
                raise ValueError(f"Duplicate clause id '{clause.id}' in contract")
            seen_ids.add(clause.id)

        if self.kind in (ContractKind.ENTITY, ContractKind.VALUE, ContractKind.WIRE):
            if not self.clauses:
                raise ValueError(
                    f"Contract of kind '{self.kind.value}' requires non-empty 'clauses'"
                )
        elif self.kind == ContractKind.FSM:
            if not self.spec_source or not self.spec_source.strip():
                raise ValueError("Contract of kind 'fsm' requires non-empty 'spec_source'")
            if self.spec_source.strip().lower().endswith(".md"):
                raise ValueError(
                    "Contract of kind 'fsm' requires a machine-readable 'spec_source' "
                    "(cannot end with '.md')"
                )
        elif self.kind == ContractKind.ARTIFACT and not self.surface.strip():
            raise ValueError("Contract of kind 'artifact' requires non-empty 'surface'")

        return self


class ContractCheckResult(BaseModel):
    clause_id: str = Field(min_length=1)
    status: Literal["VERIFIED", "VIOLATED", "UNTESTED"]
    evidence: str = ""


class TaskComment(BaseModel):
    timestamp: str = Field(default_factory=utc_now_iso)
    author: str
    kind: Literal["note", "rejection", "escalation", "architect", "system", "control"] = "note"
    body: str


class Task(BaseModel):
    id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str
    complexity: TaskComplexity = TaskComplexity.MEDIUM
    status: TaskStatus = TaskStatus.TODO
    depends_on: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)
    context_files: list[str] = Field(default_factory=list)
    verify_cmd: str | None = None
    acceptance_criteria: list[str] = Field(default_factory=list)
    provides: list[str] = Field(default_factory=list)
    consumes: list[str] = Field(default_factory=list)
    stage: str = "default"
    # Runtime tracking fields
    max_attempts: int | None = Field(default=None, ge=1)
    attempts: int = 0
    review_rounds: int = 0
    pre_review_approved: bool = False
    parent_task_id: str | None = None
    subtask_ids: list[str] = Field(default_factory=list)
    resolution: Literal["completed", "split", "skipped"] | None = None
    wip_commit_sha: str | None = None
    final_commit_sha: str | None = None
    changed_files: list[str] = Field(default_factory=list)
    touched_yellow_zone: bool = False
    pending_progress_note: str | None = None
    last_report: str | None = None
    comments: list[TaskComment] = Field(default_factory=list)
    failure_reason: str | None = None
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)


class TasksFile(BaseModel):
    schema_version: int = 1
    contracts: dict[str, ContractSpec] = Field(default_factory=dict)
    tasks: list[Task] = Field(default_factory=list)


class WorkerStatus(StrEnum):
    DONE = "DONE"
    NEEDS_ARCH_CHANGE = "NEEDS_ARCH_CHANGE"
    UNKNOWN = "UNKNOWN"


class ReviewVerdictType(StrEnum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    UNKNOWN = "UNKNOWN"


class ParsedWorkerOutput(BaseModel):
    status: WorkerStatus = WorkerStatus.UNKNOWN
    report: str = ""
    progress_update: str | None = None
    escalation_summary: str | None = None
    escalation_details: str | None = None
    raw_text: str = ""


class ParsedReviewVerdict(BaseModel):
    task_id: str
    verdict: ReviewVerdictType
    feedback: str = ""
    contract_checks: list[ContractCheckResult] = Field(default_factory=list)


ReviewOutput = ParsedReviewVerdict


class SubtaskSpec(BaseModel):
    id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str
    complexity: TaskComplexity = TaskComplexity.MEDIUM
    depends_on: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)
    context_files: list[str] = Field(default_factory=list)
    verify_cmd: str | None = None
    acceptance_criteria: list[str] = Field(default_factory=list)


class ParsedPlannerOutput(BaseModel):
    subtasks: list[SubtaskSpec] = Field(default_factory=list)
    rationale: str = ""


class SeamAuditFinding(BaseModel):
    severity: Literal["CRITICAL", "WARNING", "INFO"]
    clause_id: str | None = None
    description: str
    file_refs: list[str] = Field(default_factory=list)


class SeamAuditOutput(BaseModel):
    verdict: Literal["PASS", "WARNING", "CRITICAL"]
    summary: str
    findings: list[SeamAuditFinding] = Field(default_factory=list)


class EventType(StrEnum):
    RUN_STARTED = "run_started"
    RUN_FINISHED = "run_finished"
    RUN_ABORTED = "run_aborted"
    ORCHESTRATOR_FATAL_ERROR = "orchestrator_fatal_error"
    TASK_CLAIMED = "task_claimed"
    TASK_DEFERRED_COOLDOWN = "task_deferred_cooldown"
    TASK_STATUS_CHANGED = "task_status_changed"
    PROVIDER_INVOKED = "provider_invoked"
    PROVIDER_RESPONSE_STARTED = "provider_response_started"
    PROVIDER_RECOVERED_AFTER_RETRY = "provider_recovered_after_retry"
    PROVIDER_SUCCEEDED = "provider_succeeded"
    PROVIDER_FAILED = "provider_failed"
    PROVIDER_RATE_LIMITED = "provider_rate_limited"
    PROVIDER_TRANSPORT_RETRY = "provider_transport_retry"
    PROVIDER_TRANSIENT_RETRY = "provider_transient_retry"
    PROVIDER_FALLBACK_USED = "provider_fallback_used"
    PROVIDER_ACCOUNT_SWITCHED = "provider_account_switched"
    PROVIDER_ACCOUNT_SWITCH_FAILED = "provider_account_switch_failed"
    AGENT_COMMIT_SOFT_RESET = "agent_commit_soft_reset"
    STALE_LOCK_RECOVERED = "stale_lock_recovered"
    CRASH_RECOVERY_COMPLETED = "crash_recovery_completed"
    GATE_STARTED = "gate_started"
    GATE_PASSED = "gate_passed"
    GATE_FAILED = "gate_failed"
    PLACEHOLDER_GUARD_FAILED = "placeholder_guard_failed"
    RED_ZONE_VIOLATION = "red_zone_violation"
    YELLOW_ZONE_TRIGGERED = "yellow_zone_triggered"
    DIFF_GUARD_SOFT_WARNING = "diff_guard_soft_warning"
    DIFF_GUARD_FAILED = "diff_guard_failed"
    PRE_REVIEW_COMPLETED = "pre_review_completed"
    REVIEW_COMPLETED = "review_completed"
    SEAM_AUDIT_COMPLETED = "seam_audit_completed"
    BATCH_REVIEW_STARTED = "batch_review_started"
    BATCH_REVIEW_DEFERRED = "batch_review_deferred"
    BATCH_VERDICT_RECEIVED = "batch_verdict_received"
    ESCALATION_TRIGGERED = "escalation_triggered"
    ARCHITECT_RESOLVED = "architect_resolved"
    TASK_SPLIT = "task_split"
    REFACTOR_STARTED = "refactor_started"
    REFACTOR_FINISHED = "refactor_finished"
    REFACTOR_REVERTED = "refactor_reverted"
    GIT_COMMITTED = "git_committed"
    GIT_REVERTED = "git_reverted"
    TASK_ROLLED_BACK = "task_rolled_back"
    CONTROL_COMMAND_APPLIED = "control_command_applied"
    INBOX_CONSUMED = "inbox_consumed"
    PAUSE_ENTERED = "pause_entered"
    PAUSE_RESUMED = "pause_resumed"
    RUNNER_HIBERNATING = "runner_hibernating"
    RUNNER_WOKE_UP = "runner_woke_up"
    PROXY_STATUS_CHANGED = "proxy_status_changed"
    PROXY_ERROR = "proxy_error"
    REPORT_GENERATED = "report_generated"


class PipelineEvent(BaseModel):
    event_id: str
    timestamp: str = Field(default_factory=utc_now_iso)
    run_id: str
    task_id: str | None = None
    role: AgentRole | None = None
    provider: str | None = None
    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)


class ProviderQuotaState(BaseModel):
    window_seconds: int = 18000
    max_requests: int | None = None
    requests_in_window: int = 0
    request_timestamps: list[str] = Field(default_factory=list)
    cooldown_until: str | None = None
    last_rate_limited_at: str | None = None


class ProxyHealthStatus(BaseModel):
    """Normalized health snapshot from the declarative external proxy monitor."""

    healthy: bool = True
    state: str = "unknown"
    proxy_reachable: bool | None = None
    active_route: str | None = None
    last_message: str | None = None
    last_ok_at: str | None = None
    route_switching: bool = False
    requires_manual_action: bool = False
    fatal_reason: str | None = None
    blockers: list[str] = Field(default_factory=list)
    raw_excerpt: str = ""
    checked_at: str = Field(default_factory=utc_now_iso)


class RunStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    HIBERNATING = "hibernating"
    COMPLETED = "completed"
    ABORTED = "aborted"
    FAILED = "failed"


class PipelineState(BaseModel):
    schema_version: int = 1
    run_id: str
    status: RunStatus = RunStatus.IDLE
    active_task_id: str | None = None
    active_task_base_sha: str | None = None
    active_phase: str | None = None
    active_role: AgentRole | None = None
    active_provider: str | None = None
    paused: bool = False
    pause_reason: str | None = None
    resume_at: str | None = None
    pause_after_current: bool = False
    accepted_since_last_refactor: int = 0
    batch_queue: list[str] = Field(default_factory=list)
    processed_control_ids: list[str] = Field(default_factory=list)
    last_control_command_id: str | None = None
    provider_quotas: dict[str, ProviderQuotaState] = Field(default_factory=dict)
    active_accounts: dict[str, str] = Field(default_factory=dict)
    proxy_health_status: ProxyHealthStatus | None = None
    audited_seams: list[str] = Field(default_factory=list)
    updated_at: str = Field(default_factory=utc_now_iso)


class ControlAction(StrEnum):
    PAUSE_AFTER_CURRENT = "pause_after_current"
    RESUME = "resume"
    ABORT = "abort"
    RETRY_TASK = "retry_task"
    SKIP_TASK = "skip_task"


class ControlCommand(BaseModel):
    id: str = Field(min_length=1)
    action: ControlAction
    task_id: str | None = None
    reason: str | None = None
    issued_at: str = Field(default_factory=utc_now_iso)
    processed_at: str | None = None
    result: str | None = None
