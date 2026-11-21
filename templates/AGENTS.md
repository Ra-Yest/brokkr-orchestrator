# Repository Agent Instructions (AGENTS.md)

> This file is part of the static prompt prefix (Layer 1) and is read by every pipeline role.
> Keep it concise, factual, and free of timestamps or ephemeral notes so provider prompt caching stays warm.

## 1. Project Stack & Architecture

- **Language & Runtime:** `<e.g., Python 3.12 / TypeScript 5.x + Node 22>`
- **Framework:** `<e.g., FastAPI + SQLAlchemy 2.x / Next.js 15 App Router>`
- **Package Manager:** `<e.g., uv / pip / pnpm>`
- **Directory Layout:**
  - `src/` — application source code (feature-sliced modules)
  - `src/shared/` — shared types, schemas, utilities (Yellow Zone)
  - `tests/` — automated unit and integration tests

## 2. Deterministic Verification Commands

Run these exact non-interactive commands to verify changes before reporting completion:

```bash
# Lint
<e.g., ruff check .  OR  pnpm run lint>

# Typecheck
<e.g., mypy src  OR  pnpm run typecheck>

# Build (if applicable)
<e.g., pnpm run build>

# Tests
<e.g., pytest -q  OR  pnpm test -- --run>
```

## 3. CLI Generators First (No Manual Boilerplate)

- Prefer official project CLI generators over hand-writing repetitive scaffolding:
  - Database migrations: `<e.g., alembic revision --autogenerate -m "..."  OR  npx prisma migrate dev --create-only>`
  - UI components / codegen: `<e.g., npx shadcn@latest add ...  OR  pnpm run codegen>`
- Never hand-edit generated files (`*.generated.*`, lockfiles, `dist/`, `build/`).

## 4. Repository Access Zones (Enforced Automatically by Diff Guard)

- **RED ZONE (Strictly Forbidden — Instant Rollback):**
  - Pipeline files & runtime state: `.pipeline/**`, `pipeline.yaml`
  - Agent instructions & specs: `AGENTS.md`, `.agents/**`, `docs/specs/**`
  - Secrets & env files: `.env*`
  - **Existing tests:** Modifying (`M`), deleting (`D`), or renaming (`R`) any existing test file is strictly forbidden. Adding (`A`) **new** test files inside your task's `allowed_paths` is allowed and encouraged.
- **YELLOW ZONE (Shared — Triggers Strict Master Review):**
  - Shared manifests, schemas, routers, global types (`pyproject.toml`, `package.json`, `src/shared/**`, `src/db/**`, `src/router*`).
  - Workers must NOT hack around missing shared schemas/types/packages — use `STATUS: NEEDS_ARCH_CHANGE` instead.
- **GREEN ZONE (Allowed):**
  - Paths explicitly listed in the task's `allowed_paths`, within Diff Guard file/line limits.

## 5. Mandatory Operational Rules

1. **Never touch `.pipeline/`:** Do not read, create, or edit state/control/report files inside `.pipeline/`. Return all reports and progress summaries in your structured stdout markers; the orchestrator persists them.
2. **Never run `git commit`, `git push`, `git reset`, or switch branches:** The orchestrator owns all Git operations.
3. **Strict typing & error handling:** Follow project lint/type rules strictly; handle invalid inputs, empty states, and boundary conditions explicitly.
4. **No placeholders or shortcuts:** Never leave `TODO`, `FIXME`, `NotImplementedError`, empty `pass` stubs, or `// ... existing code`.
5. **Minimal, cohesive diffs:** Stay strictly within the task scope and acceptance criteria.
