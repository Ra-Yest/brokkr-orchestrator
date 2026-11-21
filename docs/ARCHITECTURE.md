# AFK Orchestrator — System Architecture & Integration Contracts

## 1. Architectural Overview

The AFK Orchestrator is a headless, event-driven Python 3.11+ pipeline runner designed around four strict boundaries:

1. **Provider Independence (`BaseAgentProvider` + `ProviderRegistry`):** Models and CLI tools are never hardcoded. Profiles (`provider_fast`, `provider_main`, `provider_strong`, `provider_master`, `provider_reviewer_api`) are mapped to the 8 pipeline roles purely via `pipeline.yaml`. Each provider declares `can_edit_files: bool` (`false` by default for `kind: "api"`), and config validation rejects non-editing providers on writing roles (`worker_*`, `architect`, `refactorer`).
2. **Task Store Independence (`BaseTaskStore`):** Task selection, dependency DAG validation, subtask decomposition, and comment persistence go strictly through `BaseTaskStore`. `LocalJsonTaskStore` is shipped now; swapping to `GitHubIssuesTaskStore` in Team Mode requires zero changes in `runner.py`.
3. **Headless Event-Driven Core:** All runtime telemetry streams to append-only `.pipeline/events.jsonl`, current state is atomically snapshotted to `.pipeline/state.json`, attempt artifacts live under `.pipeline/runs/<run_id>/<task_id>/attempt_<n>/`, and control commands arrive race-free via `.pipeline/control_inbox/<cmd>.json`.
4. **Strict Runner Isolation (No Self-Repair):** The orchestrator code lives outside the target repository. Agents are strictly forbidden from touching `.pipeline/` (enforced by an Integrity Guard SHA-256 snapshot before/after every call) and from committing directly to Git (enforced by `base_sha` soft-reset). On any internal orchestrator error, the runner emits `orchestrator_fatal_error`, sends an optional Telegram alert, generates `REPORT.md`, and exits non-zero without attempting self-repair.

### 1.1 Module Diagram

```mermaid
flowchart TD
    CLI["cli.py (init, doctor, run, status, control, report)"] --> CFG["config.py (PipelineConfig + optional env)"]
    CLI --> RUNNER["runner.py (OrchestratorRunner)"]
    RUNNER --> STORE["stores/base.py -> LocalJsonTaskStore"]
    RUNNER --> REG["providers/base.py -> ProviderRegistry"]
    REG --> CLIPROV["providers/cli_provider.py (CliAgentProvider)"]
    REG --> APIPROV["providers/api_provider.py (ApiAgentProvider)"]
    CLIPROV --> PROC["process_utils.py (Job Objects / killpg / which)"]
    RUNNER --> PB["prompt_builder.py (3-Layer Cache + REPO_MAP)"]
    RUNNER --> GIT["git_ops.py (base_sha, add -N, revert, rollback)"]
    RUNNER --> GUARD["diff_guard.py (Role-aware Zones, Diff & Placeholder Guard)"]
    RUNNER --> GATES["gates.py (Free Gates, Playwright, Log Trimmer)"]
    RUNNER --> PARSER["output_parser.py (STATUS / VERDICT / JSON Subtasks)"]
    RUNNER --> EVENTS["events.py (events.jsonl, state.json, QuotaTracker)"]
    RUNNER --> CTRL["control.py (control_inbox -> control_processed, INBOX.md)"]
    RUNNER --> NOTIFY["notifications.py (TelegramNotifier)"]
    RUNNER --> REPORT["reporter.py (REPORT.md + Manual Checklist)"]
```

---

## 2. Task Status Finite State Machine (FSM)

All tasks strictly transition within the 9 canonical statuses:
`todo`, `in_progress`, `gates_failed`, `needs_review`, `batch_pending`, `arch_pending`, `done`, `failed`, `blocked`.

```mermaid
stateDiagram-v2
    [*] --> todo
    todo --> in_progress : runner claims eligible task
    todo --> blocked : upstream dependency failed
    todo --> failed : control skip_task

    in_progress --> gates_failed : Red Zone / Diff Guard / Placeholder / Gate failure
    in_progress --> arch_pending : worker emits STATUS NEEDS_ARCH_CHANGE
    in_progress --> batch_pending : easy + green zone + wip commit
    in_progress --> needs_review : medium/hard OR yellow zone touched
    in_progress --> done : hard task split by planner (resolution=split)
    in_progress --> failed : max rounds exhausted -> rollback

    gates_failed --> in_progress : retry within round budget
    gates_failed --> failed : max rounds exhausted -> rollback

    arch_pending --> in_progress : architect resolved + PROGRESS updated
    arch_pending --> failed : architect failed / unresolvable

    needs_review --> done : Master APPROVE + final commit + PROGRESS updated
    needs_review --> in_progress : pre_reviewer or Master REJECT (rounds < max)
    needs_review --> done : hard task split after 2 failed rounds (resolution=split)
    needs_review --> failed : max rounds exhausted -> rollback

    batch_pending --> done : batch VERDICT #id APPROVE + PROGRESS updated
    batch_pending --> todo : batch VERDICT #id REJECT + git revert (rounds < max)
    batch_pending --> failed : batch VERDICT #id REJECT + git revert (rounds >= max)

    failed --> todo : control retry_task (resets counters)
    blocked --> todo : upstream retried and no failed/blocked deps remain
```

### Key State Invariants

- **Batch Independence & Selective Rollback:** Every `easy` task entering `batch_pending` must have zero mutual `depends_on` (direct or transitive), disjoint `changed_files`, and `touched_yellow_zone == false`. It is recorded as an isolated `wip(task-<id>)` commit. `PROGRESS.md` is **not** updated until Master outputs `VERDICT #<id>: APPROVE`. Any `REJECT` executes `git revert --no-edit <wip_commit_sha>`, rolling back only that task without impacting approved siblings.
- **Hard Task Auto-Split:** After `workflow.hard_split_after_rounds` (default `2`) failed rounds on a `hard` task, the runner rolls back uncommitted edits and invokes read-only `planner` to emit 2–3 atomic subtasks. Downstream tasks depending on the parent are atomically rewired to the leaf subtasks, and the parent transitions to `done` with `resolution: "split"` and `subtask_ids: [...]` (excluded from the refactor counter).
- **Pre-Provider Safety Sequence:**
  1. Snapshot `base_sha = git rev-parse HEAD` and SHA-256 of `.pipeline/` (Integrity Guard).
  2. Run provider non-interactively (`stdin=DEVNULL` for non-stdin modes, `shutil.which` executable resolution, Windows Job Object / POSIX process group kill on timeout).
  3. Verify `.pipeline/` snapshot is byte-for-byte unchanged (agents never write to `.pipeline/`).
  4. If `HEAD != base_sha`, run `git reset --soft <base_sha>` and emit `agent_commit_soft_reset`.
  5. Run `git add -N` on all untracked files outside `.pipeline/` and `.gitignore` so new files enter `git diff --name-status <base_sha>`, Diff Guard, Placeholder Guard, Red Zone test checks, and `diff.patch`.
  6. Run role-aware `evaluate_diff`:
     - `planner`, `pre_reviewer`, `reviewer`: strictly read-only (any repo change is a fatal Red Zone violation with rollback).
     - `architect`, `refactorer`: allowed outside a single task's `allowed_paths` (including `yellow_shared`), while Red Zone and the `M`/`D`/`R` existing-test ban remain enforced.
     - `worker_*`: full Red/Yellow/Green enforcement, `M`/`D`/`R` test ban (`A` permitted inside `allowed_paths`), complexity diff limits, and Placeholder Guard.

---

## 3. Exact Schemas

### 3.1 `pipeline.yaml` Schema

```yaml
schema_version: 1
project:
  name: string                     # Human-readable project name
  pipeline_dir: ".pipeline"        # Relative path to runtime directory (must be in .gitignore)
  agents_file: "AGENTS.md"
  agent_roles_dir: ".agents"
  anti_laziness_file: ".agents/anti_laziness_openai.md"
  tasks_file: "tasks.json"         # Relative to pipeline_dir unless absolute
  repo_map_ignore: [string]

providers:
  <profile_name>:
    kind: "cli" | "api"
    can_edit_files: bool           # Defaults: true for cli, false for api (must be false for api)
    # CLI fields (required when kind == "cli"):
    command: [string]              # Executable resolved via shutil.which
    prompt_mode: "stdin" | "arg" | "tempfile"
    args_template: [string]        # Placeholders: {prompt},{prompt_file},{output_file},{workdir},{role},{task_id},{model}
    output_parser: "plain" | "json_field" | "jsonl_last"
    output_json_path: string?      # Dot path when output_parser is json_field/jsonl_last
    # API fields (required when kind == "api"):
    base_url: string?              # OpenAI-compatible base URL
    api_key_env: string?           # Host env var holding bearer token
    headers: { string: string | EnvEntryConfig }
    max_tokens: int?
    temperature: float?
    # Common fields:
    model: string?
    env:                           # Supports ${VAR} (required), ${VAR:-default} (optional; omitted if ${VAR:-} unset),
      <KEY>: string | { from_env: string, optional: bool, default: string? }
    timeout_seconds: int           # Default: 900
    anti_laziness: bool            # Default: false; appends anti_laziness_openai.md to Layer 1 static prefix
    rate_limit_patterns: [regex]
    rate_limit_cooldown_seconds: int  # Default: 900
    quota_window_seconds: int      # Default: 18000 (5 hours)
    quota_max_requests: int?       # Optional rolling window cap

roles:
  planner: <profile_name>
  worker_easy: <profile_name>      # Must have can_edit_files: true
  worker_medium: <profile_name>    # Must have can_edit_files: true
  worker_hard: <profile_name>      # Must have can_edit_files: true
  pre_reviewer: <profile_name>
  reviewer: <profile_name>
  architect: <profile_name>        # Must have can_edit_files: true
  refactorer: <profile_name>       # Must have can_edit_files: true
  role_prompt_files: { <role>: string }  # Optional overrides

zones:
  red_protected: [glob]
  test_patterns: [glob]
  yellow_shared: [glob]

diff_guard:
  limits:
    easy:   { max_files: int, max_lines: int }
    medium: { max_files: int, max_lines: int }
    hard:   { max_files: int, max_lines: int }
  architect_limit:  { max_files: int, max_lines: int }
  refactorer_limit: { max_files: int, max_lines: int }
  exclude_from_review: [glob]

placeholder_guard:
  enabled: bool
  extra_patterns: [regex]
  allow_in_paths: [glob]

gates:
  - name: string
    command: [string]
    timeout_seconds: int
    kind: "command" | "playwright"
    optional: bool
    env: { string: string | EnvEntryConfig }
    cwd: string?

workflow:
  max_review_rounds: int           # Default: 3
  max_gate_retries_per_round: int  # Default: 3
  hard_split_after_rounds: int     # Default: 2 (<= max_review_rounds)
  batch_review_size: int           # Default: 3
  refactor_every_n_tasks: int      # Default: 6 (0 disables periodic count trigger)
  refactor_on_stage_complete: bool # Default: true
  cleanup_ports: [int]
  rate_limit_poll_seconds: float
  control_poll_seconds: float
  auto_commit: bool
  commit_prefix: string
  strict_clean_start: bool

notifications:
  telegram:
    enabled: bool
    bot_token_env: string
    chat_id_env: string
    timeout_seconds: int
```

### 3.2 `tasks.json` Schema

```json
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "T-001",
      "title": "Add healthcheck endpoint",
      "description": "Implement GET /health returning {\"status\": \"ok\"}.",
      "complexity": "easy",
      "status": "todo",
      "depends_on": [],
      "allowed_paths": ["src/api/health.py", "tests/api/**"],
      "context_files": ["src/api/app.py"],
      "verify_cmd": "pytest tests/api/test_health.py -q",
      "acceptance_criteria": [
        "GET /health returns HTTP 200 and JSON {\"status\": \"ok\"}"
      ],
      "stage": "stage-1",
      "attempts": 0,
      "review_rounds": 0,
      "parent_task_id": null,
      "subtask_ids": [],
      "resolution": null,
      "wip_commit_sha": null,
      "final_commit_sha": null,
      "changed_files": [],
      "touched_yellow_zone": false,
      "pending_progress_note": null,
      "last_report": null,
      "comments": [],
      "failure_reason": null,
      "created_at": "2026-11-20T20:00:00Z",
      "updated_at": "2026-11-20T20:00:00Z"
    }
  ]
}
```

### 3.3 `events.jsonl` Schema (Append-Only Event Stream)

Each line is a self-contained UTF-8 JSON object:

```json
{
  "event_id": "evt_a1b2c3d4e5f6",
  "timestamp": "2026-11-20T20:01:12.345678Z",
  "run_id": "run_20261120_200000_ab12cd",
  "task_id": "T-001",
  "role": "worker_easy",
  "provider": "provider_fast",
  "event_type": "provider_invoked",
  "payload": {
    "static_prefix_sha256": "e3b0c44298fc...",
    "prompt_chars": 4820,
    "attempt_dir": ".pipeline/runs/run_20261120_200000_ab12cd/T-001/attempt_1"
  }
}
```

Canonical `event_type` values:
`run_started`, `run_finished`, `run_aborted`, `orchestrator_fatal_error`, `task_claimed`, `task_status_changed`, `provider_invoked`, `provider_succeeded`, `provider_failed`, `provider_rate_limited`, `agent_commit_soft_reset`, `gate_started`, `gate_passed`, `gate_failed`, `placeholder_guard_failed`, `red_zone_violation`, `yellow_zone_triggered`, `diff_guard_failed`, `pre_review_completed`, `review_completed`, `batch_review_started`, `batch_verdict_received`, `escalation_triggered`, `architect_resolved`, `task_split`, `refactor_started`, `refactor_finished`, `refactor_reverted`, `git_committed`, `git_reverted`, `task_rolled_back`, `control_command_applied`, `inbox_consumed`, `pause_entered`, `pause_resumed`, `report_generated`.

### 3.4 `state.json` Schema (Atomically Replaced Snapshot)

Written via temp-file + `fsync` + `os.replace()` with a 5-attempt Windows `PermissionError` retry loop:

```json
{
  "schema_version": 1,
  "run_id": "run_20261120_200000_ab12cd",
  "status": "running",
  "active_task_id": "T-001",
  "active_phase": "gates",
  "active_role": "worker_easy",
  "active_provider": "provider_fast",
  "paused": false,
  "pause_reason": null,
  "resume_at": null,
  "pause_after_current": false,
  "accepted_since_last_refactor": 2,
  "batch_queue": ["T-001", "T-002"],
  "processed_control_ids": ["cmd_01"],
  "last_control_command_id": "cmd_01",
  "provider_quotas": {
    "provider_fast": {
      "window_seconds": 18000,
      "max_requests": 120,
      "requests_in_window": 9,
      "request_timestamps": ["2026-11-20T19:45:00Z"],
      "cooldown_until": null,
      "last_rate_limited_at": null
    }
  },
  "updated_at": "2026-11-20T20:01:15Z"
}
```

### 3.5 Control Plane Schema (`.pipeline/control_inbox/<ts>_<id>.json` & Legacy `control.json`)

**Primary Race-Free Contract (Recommended for Web UI & CLI):**
External writers never mutate a shared file. Instead, each command is atomically written as a unique file inside `.pipeline/control_inbox/`:

```json
{
  "id": "cmd_8f42ab19cd",
  "action": "retry_task",
  "task_id": "T-003",
  "reason": "Updated missing API fixture",
  "issued_at": "2026-11-20T20:10:00Z",
  "processed_at": null,
  "result": null
}
```

Supported `action` values: `pause_after_current`, `resume`, `abort`, `retry_task`, `skip_task`.

After executing the command, the runner atomically writes the completed record (with `processed_at` and `result`) to `.pipeline/control_processed/<id>.json`, deletes the file from `control_inbox/`, appends `<id>` to `state.processed_control_ids`, and emits `control_command_applied`.

**Legacy Compatibility:** If an operator drops a single `.pipeline/control.json` file (either a single command object or `{"commands": [...]}`), the runner reads it read-only without rewriting it, deduplicates command IDs via `state.processed_control_ids`, and archives processed copies in `control_processed/`.

---

## 4. Integration Contracts

### 4.1 Future Web UI ("Mission Control": FastAPI + WebSocket/SSE + React)

A FastAPI server can be mounted directly over `.pipeline/` without modifying any orchestrator module:

| Direction | Path / Stream | Contract |
|---|---|---|
| **Read (Live Stream)** | `.pipeline/events.jsonl` | Tail file line-by-line (track byte offset) and broadcast each parsed `PipelineEvent` over WebSocket/SSE. |
| **Read (Current Snapshot)** | `.pipeline/state.json` | Poll or watch on mtime change; always safe to read due to atomic replace + Windows retry. Drives status badge, pause countdown (`resume_at`), and 5-hour quota gauges (`provider_quotas`). |
| **Read (Task Board)** | `.pipeline/tasks.json` | Renders Kanban/DAG columns by `status`, comments, rejection history, and `parent_task_id`/`subtask_ids` tree. |
| **Read (Attempt Artifacts)** | `.pipeline/runs/<run_id>/<task_id>/attempt_<n>/` | Serves `diff.patch`, `gates.log`, `raw_output.txt`, `parsed_output.json`, `last_report.md`, and `screenshots/*.png`. |
| **Read (Report)** | `.pipeline/REPORT.md` | Renders final summary and interactive manual QA checklist. |
| **Write (Commands)** | `.pipeline/control_inbox/<ts>_<cmd_id>.json` | Atomically write temp file + `os.replace()` into `control_inbox/`. Never edit `state.json` or `control_processed/`. Confirm execution by watching `control_command_applied` in `events.jsonl` or `control_processed/<cmd_id>.json`. |
| **Write (Mid-Run Notes)** | `.pipeline/INBOX.md` | Atomically write operator instructions; confirmed when `inbox_consumed` event fires. |

### 4.2 Future Team Mode (GitHub Issues & Multi-Worker Farm)

To scale from solo local execution to a multi-machine team farm:

1. **Swap `BaseTaskStore`:** Implement `GitHubIssuesTaskStore(BaseTaskStore)` backed by GitHub Issues + Projects/Labels:
   - `get_next_available_task()` -> queries open issues with label `status:todo`, verifies `depends_on` issue numbers are closed/`status:done`, and atomically claims via assignee/label lease (`status:in_progress`, `worker:<host>`).
   - `update_status()` -> swaps `status:*` labels and updates issue metadata block.
   - `add_subtasks()` -> creates linked sub-issues, updates downstream issue dependency references, and closes parent with label `resolution:split`.
   - `add_comment()` -> posts structured Markdown comments on the Issue/PR.
2. **Branch-Per-Task Git Strategy:** Wrap `GitOps` to create `agent/task-<id>` branches and open PRs on `APPROVE` instead of committing directly to the local branch.
3. **Centralized Event Sink:** Add a webhook/HTTP forwarding subscriber alongside `events.jsonl` in `EventBus.emit()` so multiple workers stream telemetry into one Mission Control instance.
