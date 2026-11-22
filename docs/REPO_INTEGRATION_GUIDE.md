# Repository Integration Guide (10-Minute Onboarding)

This guide walks you through connecting any **new or existing** Git repository to the AFK Orchestrator so autonomous workers, guards, free quality gates, and Master reviews run safely unattended.

---

## Prerequisites

1. **Python 3.11+** and **Git** installed and available in `PATH`.
2. **Orchestrator installed** in its own virtual environment (strictly outside your target project repo so agents can never modify runner code):
   ```bash
   cd D:\orchestrator
   python -m venv .venv
   # Windows PowerShell:
   .\.venv\Scripts\Activate.ps1
   # Linux/macOS:
   # source .venv/bin/activate
   pip install -e .
   ```
3. At least one supported CLI agent installed and authenticated (e.g., `codex`, `agy`, `claude`, `gemini`, `aider`, or your custom CLI wrapper).

---

## Step 1: Initialize Target Repository (`orchestrator init`)

Run `orchestrator init` pointing at your target Git repository. Pick a starter stack preset (`python`, `typescript`, or `generic`) and an optional `--provider-preset` (`default` mixed providers or `agy-only` for Antigravity CLI with Gemini 4 profiles out of the box):

```bash
# Check installed orchestrator version
orchestrator --version

# Python / FastAPI repository
orchestrator init --repo /path/to/my-backend --stack python

# TypeScript / Next.js repository
orchestrator init --repo /path/to/my-frontend --stack typescript

# Generic / polyglot repository using Antigravity CLI (`agy`) only (Gemini 4 preset)
orchestrator init --repo /path/to/my-repo --stack generic --provider-preset agy-only
```

What `orchestrator init` does automatically:
1. Copies `AGENTS.md` to the target repository root.
2. Copies all role instruction templates (`planner.md`, `worker.md`, `pre_reviewer.md`, `reviewer.md`, `architect.md`, `refactorer.md`, `anti_laziness_openai.md`) into `.agents/`.
3. Scaffolds `.pipeline/`:
   - `.pipeline/pipeline.yaml` (stack-tailored configuration)
   - `.pipeline/tasks.json` (starter task schema)
   - `.pipeline/INBOX.md` (mid-run operator instructions)
   - `.pipeline/PROGRESS.md` (context handoff)
   - `.pipeline/control_inbox/` & `.pipeline/control_processed/`
4. Ensures `.pipeline/` is added to `.gitignore` so runtime state, logs, and artifacts are **never** committed or deleted on `git clean`.
5. Automatically runs `orchestrator doctor` to verify environment readiness.

> **Important:** Commit `AGENTS.md`, `.agents/`, and `.gitignore` to your target repository before running the pipeline so `strict_clean_start` sees a clean working tree and `DiffGuard` protects `.agents/` and `AGENTS.md` as tracked Red Zone files.

```bash
cd /path/to/my-repo
git add AGENTS.md .agents .gitignore
git commit -m "chore: onboard repository to AFK orchestrator"
```

---

## Step 2: Repository Requirements Checklist

For reliable AFK runs, your target repository must satisfy these 5 criteria:

1. **Clean Git Baseline:** All manual changes committed before `orchestrator run`.
2. **`.gitignore` Isolation:** `.pipeline/` MUST be ignored by Git (`git check-ignore .pipeline` exits `0`). Never commit `.pipeline/state.json`, `events.jsonl`, or `runs/`.
3. **Deterministic Non-Interactive Gate Commands:** Every command in `gates:` must exit `0` on pass and non-zero on failure without waiting for keyboard input, interactive prompts, or watch mode (e.g., use `vitest run` / `jest --watchAll=false` / `pytest -q`, never watch mode).
4. **Clear Feature Boundaries:** Organize features into predictable directories (`src/features/<name>/...` or `src/modules/<name>/...`) so `allowed_paths` can tightly scope each task and `easy` tasks can safely run in parallel batches.
5. **Separated Shared Layer:** Keep shared DB schemas, global routers, global types, and dependency manifests in well-known locations so they can be listed under `zones.yellow_shared`.

---

## Step 3: Fill Out `AGENTS.md`

Edit `<target-repo>/AGENTS.md`. Remember that `AGENTS.md` is placed in **Layer 1 (Static Prefix)** of every prompt for provider **Prompt Caching**:
- Keep it **concise** (under ~150 lines) and **static** (no dates, sprint numbers, or active task notes — put dynamic notes in `tasks.json` or `INBOX.md`).
- Document the exact stack, directory layout, exact lint/type/build/test commands, and CLI generators (e.g., `alembic`, `prisma`, `shadcn`) so workers never hand-code boilerplate.

---

## Step 4: Configure Zones, Gates & Providers in `.pipeline/pipeline.yaml`

### Provider Swapping & Capabilities

- Every profile declares `can_edit_files: bool` (defaults to `true` for `kind: "cli"`, `false` for `kind: "api"`).
- Writing roles (`worker_easy`, `worker_medium`, `worker_hard`, `architect`, `refactorer`) **must** use a provider with `can_edit_files: true`.
- Read-only roles (`planner`, `pre_reviewer`, `reviewer`) may use either a CLI provider (e.g., `agy` with `can_edit_files: false` and `--mode plan`, without `--disable-slash-commands`) or an OpenAI-compatible `kind: "api"` provider. Writing `agy` profiles omit `--mode auto` (which triggers stderr warnings on some accounts) and use `--dangerously-skip-permissions --disable-slash-commands`.
- If a CLI profile's `args_template` uses `{model}`, the `model` field is required and validated on load / `doctor`.
- Optional env vars use `${VAR:-}` syntax: if `OPENAI_API_KEY` is unset (e.g. `codex exec` authenticated via ChatGPT Plus OAuth), the orchestrator omits the key cleanly instead of failing.
- Set `anti_laziness: true` on OpenAI / shortcut-prone profiles to automatically append `.agents/anti_laziness_openai.md` to their static prefix.
- Set `idle_timeout_seconds` (e.g., `300`) on CLI profiles to automatically terminate hung processes that produce no `stdout`/`stderr` bytes and make no file modifications in the repository.
- Configure `fallback_providers` (per role or per provider profile) to automatically switch to another compatible provider on `429` or `quota_max_requests`; when all candidates are exhausted, the runner enters `status: "hibernating"` with `resume_at` and wakes up automatically (or responds immediately to `control resume` / `control abort`).
- Configure `account_group`, `account_profile`, and `switch_command` (e.g., `["aisw", "use", "antigravity", "pro"]`) for sequential multi-account switching tracked in `state.json` (`active_accounts`).
- Optionally enable `proxy_monitor:` (inline or via an external gitignored `config_file: "proxy_monitor.local.yaml"`) to declaratively probe a local proxy/gateway TCP endpoint, JSON status file, and log files so transient upstream switches trigger free transport retries while fatal auth/service blockers pause cleanly (`pause_reason: "proxy_error"`).
- Under `diff_guard:`, `exclude_tests_from_limits: true` excludes new test files (`zones.test_patterns`) from production `max_files`/`max_lines` (capped separately by `hard_max_test_lines: 3000`), and `soft_max_*` / `hard_max_*` limits route soft exceedances to individual review with `DIFF_GUARD_SOFT_WARNING` without rolling back.
- Set `zones.enforce_allowed_paths_on_yellow: true` if you want workers to fail with `OUT_OF_ALLOWED_PATHS` (prompting `STATUS: NEEDS_ARCH_CHANGE` -> `architect`) when touching `yellow_shared` files not explicitly listed in `task.allowed_paths` (defaults to `false` for solo Master review).
- Configure Telegram AFK notifications under `notifications.telegram` (by default sends concise event categories without retry spam, plus mandatory recovery alerts after `transport_retries` and hibernation/proxy alerts; set `notify_on_provider_started: true` to also notify on every normal provider start).

### Example A: Python / FastAPI (`zones` & `gates`)

```yaml
zones:
  red_protected:
    - ".pipeline/**"
    - "pipeline.yaml"
    - ".env*"
    - "**/.env*"
    - ".agents/**"
    - "AGENTS.md"
    - "docs/specs/**"
  test_patterns:
    - "tests/**"
    - "**/test_*.py"
    - "**/*_test.py"
  yellow_shared:
    - "pyproject.toml"
    - "alembic/**"
    - "src/app/core/**"
    - "src/app/db/models.py"
    - "src/app/api/router.py"

gates:
  - name: "ruff_lint"
    command: ["ruff", "check", "."]
    timeout_seconds: 120
  - name: "mypy_types"
    command: ["mypy", "src"]
    timeout_seconds: 180
  - name: "pytest_unit"
    command: ["pytest", "-q"]
    timeout_seconds: 300
```

### Example B: TypeScript / Next.js (`zones`, `gates`, and Optional Playwright Gate)

```yaml
zones:
  red_protected:
    - ".pipeline/**"
    - "pipeline.yaml"
    - ".env*"
    - "**/.env*"
    - ".agents/**"
    - "AGENTS.md"
    - "docs/specs/**"
  test_patterns:
    - "tests/**"
    - "e2e/**"
    - "**/*.test.ts"
    - "**/*.test.tsx"
    - "**/*.spec.ts"
  yellow_shared:
    - "package.json"
    - "tsconfig.json"
    - "next.config.*"
    - "prisma/schema.prisma"
    - "src/types/**"
    - "src/lib/db.ts"
    - "src/app/layout.tsx"

gates:
  - name: "eslint"
    command: ["pnpm", "run", "lint"]
    timeout_seconds: 180
  - name: "typecheck"
    command: ["pnpm", "exec", "tsc", "--noEmit"]
    timeout_seconds: 180
  - name: "next_build"
    command: ["pnpm", "run", "build"]
    timeout_seconds: 300
  - name: "unit_tests"
    command: ["pnpm", "test", "--", "--run"]
    timeout_seconds: 300
  - name: "playwright_console_network"
    command: ["pnpm", "exec", "playwright", "test", "--grep", "@smoke"]
    timeout_seconds: 300
    kind: "playwright"
    optional: false
```

> When `kind: "playwright"` runs, the orchestrator injects `ORCHESTRATOR_SCREENSHOTS_DIR=<attempt_dir>/screenshots`. Configure your Playwright test helper to save failure/smoke screenshots there so they are automatically indexed in `events.jsonl` and surfaced in the future Web UI.

---

## Step 5: Author `.pipeline/tasks.json`

Write atomic, well-scoped tasks forming an acyclic DAG (`depends_on`):

```json
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "T-001",
      "title": "Add pagination helper for list endpoints",
      "description": "Implement CursorPage dataclass and encode/decode cursor helpers in src/features/pagination/.",
      "complexity": "easy",
      "depends_on": [],
      "allowed_paths": ["src/features/pagination/**", "tests/features/pagination/**"],
      "context_files": ["src/app/core/config.py"],
      "verify_cmd": "pytest tests/features/pagination -q",
      "acceptance_criteria": [
        "encode_cursor/decode_cursor round-trips offset and sort key",
        "Invalid base64 cursor raises ValueError with clear message",
        "New unit tests cover empty list, single page, and malformed cursor"
      ],
      "stage": "phase-1"
    },
    {
      "id": "T-002",
      "title": "Add GET /items endpoint using pagination helper",
      "description": "Implement list items service and route handler using CursorPage from T-001.",
      "complexity": "medium",
      "depends_on": ["T-001"],
      "allowed_paths": ["src/features/items/**", "tests/features/items/**"],
      "context_files": ["src/features/pagination/cursor.py"],
      "verify_cmd": "pytest tests/features/items -q",
      "acceptance_criteria": [
        "GET /items?limit=20 returns items and next_cursor",
        "limit > 100 returns HTTP 422 validation error"
      ],
      "stage": "phase-1"
    }
  ]
}
```

### Best Practices for `tasks.json`

- **Keep `easy` tasks independent:** Disjoint `allowed_paths` and no mutual `depends_on` allow the orchestrator to batch 3–4 `easy` tasks into a single Master review call.
- **Explicit `context_files`:** List only the 1–5 files the worker actually needs to read — never pass the whole repo.
- **Allow new tests in `allowed_paths`:** Include the feature's test directory (e.g., `tests/features/items/**`) so workers can add (`A`) new unit tests while existing tests remain protected against `M`/`D`/`R`.
- **Per-task `verify_cmd`:** Add a fast targeted test command in addition to global `gates`.

---

## Step 6: Run Preflight Check & Launch

```bash
# 1. Verify git state, .gitignore, schemas, templates, active providers, switch_command, proxy_monitor, gate binaries (including repo-relative paths), and Telegram env
orchestrator doctor --repo /path/to/my-repo

# 2. Dry-run validation
orchestrator run --repo /path/to/my-repo --dry-run

# 3. Start AFK execution (streams compact [HH:MM:SS] live progress lines to stdout by default)
orchestrator run --repo /path/to/my-repo

# 4. Suppress live console progress lines (keep events.jsonl / state.json only)
orchestrator run --repo /path/to/my-repo --quiet
```

On Windows PowerShell, `orchestrator` enables line-buffered stdout/stderr automatically, so piping directly to a log file works in real time without `PYTHONUNBUFFERED=1` (and `.pipeline/console.log` is excluded from Integrity Guard):

```powershell
orchestrator run --repo D:\my-repo | Tee-Object D:\my-repo\.pipeline\console.log
```

---

## Step 7: Live Monitoring & Mid-Run Control

While the pipeline runs in AFK mode (commands in `.pipeline/control_inbox/`, `.pipeline/INBOX.md`, `.pipeline/control.json`, and `.pipeline/console.log` are allowed during provider runs and never trigger Integrity Guard):

```bash
# Check live status, active task/provider, active_accounts, proxy_health_status, batch queue, and 5-hour rolling quotas
orchestrator status --repo /path/to/my-repo

# Gracefully pause after the current provider step finishes (before next retry, gate, or review)
orchestrator control pause --repo /path/to/my-repo --reason "Inspecting diff"

# Resume a paused or hibernating run immediately
orchestrator control resume --repo /path/to/my-repo

# Retry a failed task (automatically unblocks eligible downstream tasks)
orchestrator control retry --repo /path/to/my-repo --task-id T-002

# Skip a task and block dependents
orchestrator control skip --repo /path/to/my-repo --task-id T-005 --reason "Deferred"

# Abort cleanly (rolls back uncommitted work and returns any unfinished active task to `todo`)
orchestrator control abort --repo /path/to/my-repo
```

To inject ad-hoc course corrections before the next task starts, simply write plain Markdown into `<target-repo>/.pipeline/INBOX.md`. The runner will inject it into the next prompt's dynamic tail, archive it to `.pipeline/inbox_archive/`, and reset `INBOX.md`.

When the run finishes, open `<target-repo>/.pipeline/REPORT.md` for the full summary table and step-by-step manual QA checklist.
