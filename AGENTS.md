# AGENTS.md — Engineering Instructions for `D:\orchestrator`

> Scope: This file governs agents modifying the **orchestrator repository itself** (`D:\orchestrator`).
> Do **not** confuse this with `templates/AGENTS.md`, which is copied into target projects.

---

## 1. Project Identity & Core Principle

- **What this repo is:** `afk-orchestrator` (`v0.3.2`, entry point `orchestrator.cli:main`), a headless, fault-tolerant local AFK AI development pipeline runner.
- **Stack:** Python `>=3.11`, Pydantic v2 (`>=2.6,<3.0`), PyYAML (`>=6.0`). Dev toolchain: `pytest`, `ruff` (`line-length = 100`, rules `E,F,I,UP,B,SIM,RUF`, ignoring `E501`), `mypy --strict` (`packages = ["orchestrator"]`).
- **Core Principle — Stack-Agnostic Core:** Runner and guard logic MUST remain completely agnostic of target project languages, frameworks, and AI CLI tools. Never hardcode language-specific build logic or model/CLI names in `runner.py` or guards. Any default globs, patterns, or limits in `src/orchestrator/config.py` MUST be fully overridable via `pipeline.yaml`. Keep provider (`BaseAgentProvider`) and task-store (`BaseTaskStore`) abstractions decoupled from `runner.py`.

---

## 2. Repository Map

| Path | Responsibility |
|---|---|
| `src/orchestrator/runner.py` | Core FSM loop, task claiming, batching, escalations, auto-split, retries, hibernation, rollback. |
| `src/orchestrator/config.py` | Pydantic v2 `PipelineConfig` schema, defaults, `${ENV}` resolution, and role/provider validation. |
| `src/orchestrator/models.py` | Domain models (`Task`, `TasksFile`, `PipelineState`, `PipelineEvent`, `ControlCommand`, enums). |
| `src/orchestrator/diff_guard.py` | Role-aware Red/Yellow/Green zone checks, soft/hard diff limits, test ban, paired paths, placeholder guard. |
| `src/orchestrator/prompt_builder.py` | 3-layer cacheable prompts, `REPO_MAP`, context budgets, and `prepare_review_diff()` priority truncation. |
| `src/orchestrator/process_utils.py` | Non-interactive subprocess runner, UTF-8 streaming threads, CPU/mtime idle & stall kill, Windows JobObject. |
| `src/orchestrator/proxy_monitor.py` | Declarative `GenericProxyMonitor` (TCP, JSON status, log regex probes) for pre-flight/error routing. |
| `src/orchestrator/git_ops.py` | Git wrapper (`base_sha`, `add -N`, `reset --soft`, `commit`, selective `revert`, rollback). |
| `src/orchestrator/providers/` | `BaseAgentProvider` + `ProviderRegistry` (`base.py`), `CliAgentProvider` (`cli_provider.py`), `ApiAgentProvider` (`api_provider.py`). |
| `src/orchestrator/stores/` | `BaseTaskStore` (`base.py`) and `LocalJsonTaskStore` (`local_json.py`) for DAG validation and task persistence. |
| `src/orchestrator/cli.py` | CLI commands (`init`, `doctor`, `plan-check`, `run`, `status`, `stats`, `control`, `report`); wires runner, gates, events, control, reporter. |
| `src/orchestrator/plan_check.py` | Pre-flight plan audit (`orchestrator plan-check`): deterministic DAG/paths/contracts checks + optional `--llm` planner audit. |
| `src/orchestrator/stats.py` | Pipeline telemetry aggregator (`orchestrator stats` + `REPORT.md` telemetry section) over `.pipeline/events.jsonl`. |
| `src/orchestrator/` (support) | `gates.py` (commands/Playwright), `events.py` (`events.jsonl`/quotas), `control.py` (`control_inbox/`), `output_parser.py`, `fs_utils.py`, `notifications.py`, `reporter.py`. |
| `templates/.agents/` | Default target-repo role prompts (`worker.md`, `reviewer.md`, `pre_reviewer.md`, `seam_auditor.md`, `planner.md`, `architect.md`, `refactorer.md`, `anti_laziness_openai.md`). |
| `docs/ARCHITECTURE.md` | Authoritative FSM, exact schemas, and integration contracts (`pipeline.example.yaml` is the reference config). |

---

## 3. Dev Environment & Mandatory Checks

Run **all three** commands before finishing any task. Execute them **strictly synchronously** (never as background tasks) and ensure zero errors:

```powershell
D:\orchestrator\.venv\Scripts\ruff.exe check src tests
D:\orchestrator\.venv\Scripts\mypy.exe src
D:\orchestrator\.venv\Scripts\pytest.exe -q
```

- All code in `src/orchestrator/` must pass `mypy --strict` (explicit types, no untyped defs, no bare generics, `from __future__ import annotations`).
- Test infrastructure lives in `tests/conftest.py`: reuse `temp_git_repo` (isolated `tmp_path` git repo), `make_test_config`, `make_mock_registry` / `ScriptedMockProvider`, `write_tasks_file`, and `ok_response`.

---

## 4. Hard Invariants (Never Break)

1. **Schema Backward Compatibility:**
   - Every new field added to `pipeline.yaml` (`config.py`), `tasks.json`, or `state.json` (`models.py`) MUST be optional with a safe default.
   - Existing configs, task files, and state snapshots MUST load cleanly without manual migrations.
2. **Strict Read-Only Role Isolation:**
   - `planner`, `pre_reviewer`, and `reviewer` MUST NOT modify repository files.
   - `diff_guard.py` enforces this strictly; any allowed noise files may ONLY be exempted via explicit `diff_guard.readonly_ignore_paths`.
   - Protected `.pipeline/` files are verified byte-for-byte via Integrity Guard SHA-256 snapshots before/after every provider call; never relax this boundary.
3. **Windows-First Process & File Reliability:**
   - Launch all subprocesses through `process_utils.py` with full process-tree termination (`_WindowsJobObject` + `taskkill /F /T /PID` fallback).
   - Drain `stdout`/`stderr` via non-blocking reader threads so child processes never deadlock on full pipes.
   - Always pass explicit `encoding="utf-8"` (and `PYTHONUTF8=1` for child processes) — never rely on Windows default `cp1251`/locale encoding.
   - Write persistent JSON/state files atomically via `fs_utils.py` (`os.replace` with Windows `PermissionError` retry).
4. **Zero Worker Diff Loss on Infra Failures:**
   - Timeouts, HTTP `429`/`502`/`503`, proxy failures, or operator pauses during review (`pre_reviewer` / `reviewer`) MUST NOT roll back a valid worker diff in `needs_review`.
   - Fast transport retries and capacity fallbacks must not burn `task.attempts` or `review_rounds`.
5. **Prompt & Quota Safety:**
   - Diff filtering/truncation (`prompt_builder.prepare_review_diff`) applies **only** to the text injected into reviewer prompts.
   - It MUST NEVER alter what `diff_guard.py` evaluates, what `gates.py` runs against, or what `git_ops.py` commits.

---

## 5. Change Checklist

- [ ] **Config changed (`src/orchestrator/config.py`)?** Update `pipeline.example.yaml` and `docs/ARCHITECTURE.md` to match.
- [ ] **Feature or bug fix added?** Add isolated tests in `tests/` using `ScriptedMockProvider` / `make_mock_registry` and `temp_git_repo` (`tmp_path` git repo). Never make real network calls or invoke real `agy` / external AI CLIs in tests.
- [ ] **Templates untouched?** Do NOT modify `templates/AGENTS.md` or `templates/.agents/*.md` unless explicitly requested by the user.
- [ ] **All checks green?** Synchronously run `ruff check src tests`, `mypy src`, and `pytest -q` and confirm all pass.
