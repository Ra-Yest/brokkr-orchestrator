"""CLI entry point for the AFK Orchestrator (`init`, `doctor`, `run`, `status`, `control`, `report`)."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import sys
from collections import Counter
from pathlib import Path

from orchestrator import __version__
from orchestrator.config import PipelineConfig, load_pipeline_config, resolve_env_map
from orchestrator.control import INBOX_TEMPLATE, enqueue_control_command
from orchestrator.events import EventBus
from orchestrator.fs_utils import atomic_write_json, atomic_write_text
from orchestrator.git_ops import GitOps
from orchestrator.models import ControlAction, PipelineState, RunStatus, Task, TasksFile
from orchestrator.prompt_builder import DEFAULT_ROLE_PROMPT_FILES
from orchestrator.providers.base import ProviderRegistry
from orchestrator.reporter import write_report
from orchestrator.runner import OrchestratorRunner
from orchestrator.stores.local_json import LocalJsonTaskStore


def find_templates_root() -> Path:
    """Locate the `templates/` directory shipped alongside the orchestrator package."""
    # Source checkout layout: <repo>/src/orchestrator/cli.py -> <repo>/templates
    pkg_dir = Path(__file__).resolve().parent
    candidates = [
        pkg_dir.parent.parent / "templates",
        pkg_dir / "templates",
        Path.cwd() / "templates",
    ]
    for candidate in candidates:
        if candidate.is_dir() and (candidate / "AGENTS.md").exists():
            return candidate
    raise FileNotFoundError(
        "Could not locate orchestrator templates directory (expected templates/AGENTS.md)"
    )


def find_example_config() -> Path | None:
    pkg_dir = Path(__file__).resolve().parent
    candidates = [
        pkg_dir.parent.parent / "pipeline.example.yaml",
        Path.cwd() / "pipeline.example.yaml",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _default_config_path(repo_root: Path, explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit)
        return path if path.is_absolute() else (repo_root / path)
    in_pipeline = repo_root / ".pipeline" / "pipeline.yaml"
    if in_pipeline.exists():
        return in_pipeline
    in_root = repo_root / "pipeline.yaml"
    if in_root.exists():
        return in_root
    return in_pipeline


def _providers_and_roles_yaml(provider_preset: str) -> str:
    if provider_preset == "agy-only":
        return """providers:
  agy_worker_easy:
    kind: "cli"
    can_edit_files: true
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "auto"
      - "--dangerously-skip-permissions"
      - "--disable-slash-commands"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-hydrogen-medium"
    timeout_seconds: 900
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  agy_worker_medium:
    kind: "cli"
    can_edit_files: true
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "auto"
      - "--dangerously-skip-permissions"
      - "--disable-slash-commands"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-hydrogen-high"
    timeout_seconds: 900
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  agy_worker_hard:
    kind: "cli"
    can_edit_files: true
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "auto"
      - "--dangerously-skip-permissions"
      - "--disable-slash-commands"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-neon-high"
    timeout_seconds: 900
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  agy_pre_reviewer:
    kind: "cli"
    can_edit_files: false
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "plan"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-neon-medium"
    timeout_seconds: 600
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  agy_master_readonly:
    kind: "cli"
    can_edit_files: false
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "plan"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-neon-xhigh"
    timeout_seconds: 900
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  agy_master_write:
    kind: "cli"
    can_edit_files: true
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "auto"
      - "--dangerously-skip-permissions"
      - "--disable-slash-commands"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-neon-xhigh"
    timeout_seconds: 900
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

roles:
  planner: "agy_master_readonly"
  worker_easy: "agy_worker_easy"
  worker_medium: "agy_worker_medium"
  worker_hard: "agy_worker_hard"
  pre_reviewer: "agy_pre_reviewer"
  reviewer: "agy_master_readonly"
  architect: "agy_master_write"
  refactorer: "agy_master_write" """.rstrip()

    return """providers:
  provider_fast:
    kind: "cli"
    can_edit_files: true
    command: ["codex", "exec"]
    prompt_mode: "stdin"
    args_template: ["--full-auto", "--model", "{model}", "-"]
    model: "gpt-5-codex-mini"
    env:
      OPENAI_API_KEY: "${OPENAI_API_KEY:-}"
    timeout_seconds: 900
    anti_laziness: true
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  provider_main:
    kind: "cli"
    can_edit_files: true
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "auto"
      - "--dangerously-skip-permissions"
      - "--disable-slash-commands"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-hydrogen-high"
    timeout_seconds: 900
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  provider_pre_reviewer_agy:
    kind: "cli"
    can_edit_files: false
    command: ["agy"]
    prompt_mode: "stdin"
    args_template:
      - "--model"
      - "{model}"
      - "--mode"
      - "plan"
      - "--input-format"
      - "text"
      - "--output-format"
      - "text"
    model: "gemini-4-neon-medium"
    timeout_seconds: 600
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  provider_master:
    kind: "cli"
    can_edit_files: true
    command: ["claude", "-p"]
    prompt_mode: "stdin"
    args_template: ["--output-format", "text"]
    timeout_seconds: 900
    anti_laziness: false
    rate_limit_cooldown_seconds: 900
    quota_window_seconds: 18000

  provider_reviewer_api:
    kind: "api"
    can_edit_files: false
    base_url: "https://api.openai.com/v1"
    api_key_env: "REVIEWER_API_KEY"
    model: "gpt-5"
    timeout_seconds: 300
    anti_laziness: false

roles:
  planner: "provider_master"
  worker_easy: "provider_fast"
  worker_medium: "provider_main"
  worker_hard: "provider_master"
  pre_reviewer: "provider_pre_reviewer_agy"
  reviewer: "provider_master"
  architect: "provider_master"
  refactorer: "provider_master" """.rstrip()


def _stack_preset_yaml(
    project_name: str,
    stack: str,
    provider_preset: str = "default",
) -> str:
    example_path = find_example_config()
    if stack == "generic" and provider_preset == "default" and example_path is not None:
        raw = example_path.read_text(encoding="utf-8")
        return raw.replace('name: "example-app"', f'name: "{project_name}"')

    if stack == "python":
        repo_map_extra = '    - "build"\n'
        gates_block = """gates:
  - name: "lint"
    command: ["ruff", "check", "."]
    timeout_seconds: 120
  - name: "typecheck"
    command: ["mypy", "src"]
    timeout_seconds: 180
  - name: "tests"
    command: ["pytest", "-q"]
    timeout_seconds: 300"""
        yellow_block = """  yellow_shared:
    - "pyproject.toml"
    - "alembic/**"
    - "src/shared/**"
    - "src/db/**"
    - "src/api/router.py" """.rstrip()
    elif stack == "typescript":
        repo_map_extra = '    - "build"\n'
        gates_block = """gates:
  - name: "lint"
    command: ["npm", "run", "lint"]
    timeout_seconds: 180
  - name: "typecheck"
    command: ["npm", "run", "typecheck"]
    timeout_seconds: 180
  - name: "build"
    command: ["npm", "run", "build"]
    timeout_seconds: 300
  - name: "tests"
    command: ["npm", "test", "--", "--run"]
    timeout_seconds: 300"""
        yellow_block = """  yellow_shared:
    - "package.json"
    - "tsconfig.json"
    - "next.config.*"
    - "src/shared/**"
    - "src/types/**"
    - "prisma/schema.prisma" """.rstrip()
    else:
        repo_map_extra = ""
        gates_block = """gates:
  - name: "tests"
    command: ["pytest", "-q"]
    timeout_seconds: 300"""
        yellow_block = """  yellow_shared:
    - "pyproject.toml"
    - "package.json"
    - "src/shared/**" """.rstrip()

    providers_roles = _providers_and_roles_yaml(provider_preset)

    return f"""schema_version: 1
project:
  name: "{project_name}"
  pipeline_dir: ".pipeline"
  agents_file: "AGENTS.md"
  agent_roles_dir: ".agents"
  anti_laziness_file: ".agents/anti_laziness_openai.md"
  tasks_file: "tasks.json"
  repo_map_ignore:
    - ".git"
    - ".pipeline"
    - ".venv"
    - "venv"
    - "node_modules"
    - "__pycache__"
    - ".pytest_cache"
    - ".mypy_cache"
    - ".ruff_cache"
    - "dist"
    - "app/build"
    - "android/build"
    - "web/build"
    - "**/build/outputs"
{repo_map_extra}    - ".next"

{providers_roles}

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
    - "**/tests/**"
    - "**/test_*.py"
    - "**/*_test.py"
    - "**/*.test.*"
    - "**/*.spec.*"
{yellow_block}
  enforce_allowed_paths_on_yellow: false

diff_guard:
  limits:
    easy:   {{ max_files: 5,  max_lines: 200 }}
    medium: {{ max_files: 12, max_lines: 600 }}
    hard:   {{ max_files: 25, max_lines: 1500 }}

placeholder_guard:
  enabled: true

{gates_block}

workflow:
  max_review_rounds: 3
  max_gate_retries_per_round: 3
  hard_split_after_rounds: 2
  batch_review_size: 3
  refactor_every_n_tasks: 6
  refactor_on_stage_complete: true
  cleanup_ports: []

notifications:
  telegram:
    enabled: false
    bot_token_env: "TELEGRAM_BOT_TOKEN"
    chat_id_env: "TELEGRAM_CHAT_ID"
    events:
      - "run_started"
      - "task_done"
      - "task_failed"
      - "escalation"
      - "pause"
      - "run_finished"
      - "fatal_error"
"""


def _ensure_gitignore_has_pipeline(repo_root: Path, pipeline_dir_name: str = ".pipeline") -> bool:
    """Append `.pipeline/` to `.gitignore` if not already ignored. Returns True if modified."""
    gitignore = repo_root / ".gitignore"
    entry = f"{pipeline_dir_name.strip('/')}/"
    if gitignore.exists():
        lines = gitignore.read_text(encoding="utf-8", errors="replace").splitlines()
        if any(ln.strip() in (entry, pipeline_dir_name.strip("/")) for ln in lines):
            return False
        content = "\n".join(lines).rstrip() + f"\n\n# AFK Orchestrator runtime state\n{entry}\n"
        atomic_write_text(gitignore, content)
        return True
    atomic_write_text(gitignore, f"# AFK Orchestrator runtime state\n{entry}\n")
    return True


def cmd_init(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    if not repo_root.exists():
        print(f"[ERROR] Target directory does not exist: {repo_root}", file=sys.stderr)
        return 1

    templates_root = find_templates_root()
    copied: list[str] = []
    skipped: list[str] = []

    # 1. Copy AGENTS.md
    dst_agents = repo_root / "AGENTS.md"
    if not dst_agents.exists() or args.force:
        shutil.copy2(templates_root / "AGENTS.md", dst_agents)
        copied.append("AGENTS.md")
    else:
        skipped.append("AGENTS.md")

    # 2. Copy .agents/*.md
    src_agents_dir = templates_root / ".agents"
    dst_agents_dir = repo_root / ".agents"
    dst_agents_dir.mkdir(parents=True, exist_ok=True)
    for src_file in sorted(src_agents_dir.glob("*.md")):
        dst_file = dst_agents_dir / src_file.name
        if not dst_file.exists() or args.force:
            shutil.copy2(src_file, dst_file)
            copied.append(f".agents/{src_file.name}")
        else:
            skipped.append(f".agents/{src_file.name}")

    # 3. Scaffold .pipeline/
    pipeline_dir = repo_root / ".pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    (pipeline_dir / "control_inbox").mkdir(exist_ok=True)
    (pipeline_dir / "control_processed").mkdir(exist_ok=True)

    cfg_path = pipeline_dir / "pipeline.yaml"
    if not cfg_path.exists() or args.force:
        atomic_write_text(
            cfg_path,
            _stack_preset_yaml(
                repo_root.name,
                args.stack,
                provider_preset=args.provider_preset,
            ),
        )
        copied.append(".pipeline/pipeline.yaml")
    else:
        skipped.append(".pipeline/pipeline.yaml")

    tasks_path = pipeline_dir / "tasks.json"
    if not tasks_path.exists() or args.force:
        starter_tasks = TasksFile(
            schema_version=1,
            tasks=[
                Task(
                    id="T-001",
                    title="Example starter task",
                    description="Replace this starter task with your real project tasks.",
                    complexity="easy",  # type: ignore[arg-type]
                    allowed_paths=["src/**"],
                    context_files=[],
                    acceptance_criteria=["Task passes all configured gates"],
                )
            ],
        )
        atomic_write_json(tasks_path, starter_tasks.model_dump(mode="json"))
        copied.append(".pipeline/tasks.json")
    else:
        skipped.append(".pipeline/tasks.json")

    inbox_path = pipeline_dir / "INBOX.md"
    if not inbox_path.exists() or args.force:
        atomic_write_text(inbox_path, INBOX_TEMPLATE)
        copied.append(".pipeline/INBOX.md")

    progress_path = pipeline_dir / "PROGRESS.md"
    if not progress_path.exists() or args.force:
        atomic_write_text(
            progress_path,
            "# Pipeline Progress Handoff (PROGRESS.md)\n\nInitial repository onboarding complete.\n",
        )
        copied.append(".pipeline/PROGRESS.md")

    if _ensure_gitignore_has_pipeline(repo_root):
        copied.append(".gitignore (added .pipeline/)")

    print(f"Initialized AFK orchestrator in {repo_root}")
    if copied:
        print("Created/updated:")
        for item in copied:
            print(f"  + {item}")
    if skipped:
        print("Preserved existing (use --force to overwrite):")
        for item in skipped:
            print(f"  - {item}")

    print("\nRunning preflight doctor check...")
    doctor_args = argparse.Namespace(repo=str(repo_root), config=str(cfg_path))
    return cmd_doctor(doctor_args)


def run_doctor_checks(repo_root: Path, config_path: Path) -> list[tuple[str, bool, str]]:
    """Run all preflight readiness checks. Returns list of (check_name, ok, detail)."""
    checks: list[tuple[str, bool, str]] = []

    git_bin = shutil.which("git")
    checks.append(("git_binary", git_bin is not None, git_bin or "git not found in PATH"))

    git_ops = GitOps(repo_root)
    is_repo = git_ops.is_git_repo() if git_bin else False
    checks.append(("git_repository", is_repo, str(repo_root) if is_repo else "Not a git repository"))

    config: PipelineConfig | None = None
    try:
        config = load_pipeline_config(config_path)
        checks.append(("pipeline_yaml", True, f"Valid config ({len(config.providers)} providers)"))
    except Exception as exc:
        checks.append(("pipeline_yaml", False, str(exc)))

    if config is not None:
        git_ops = GitOps(repo_root, pipeline_dir=config.project.pipeline_dir)
        if is_repo:
            ignored = git_ops.is_pipeline_dir_ignored()
            checks.append(
                (
                    "pipeline_dir_gitignored",
                    ignored,
                    f"{config.project.pipeline_dir}/ is ignored"
                    if ignored
                    else f"Add '{config.project.pipeline_dir}/' to .gitignore",
                )
            )
            clean = not git_ops.has_uncommitted_changes()
            checks.append(
                (
                    "working_tree_clean",
                    clean,
                    "Clean working tree" if clean else "Uncommitted changes present",
                )
            )

        agents_file = repo_root / config.project.agents_file
        checks.append(("agents_md", agents_file.exists(), str(agents_file)))

        roles_dir = repo_root / config.project.agent_roles_dir
        missing_roles = sorted(
            {
                fname
                for fname in DEFAULT_ROLE_PROMPT_FILES.values()
                if not (roles_dir / fname).exists()
            }
        )
        checks.append(
            (
                "role_templates",
                len(missing_roles) == 0,
                "All role templates present"
                if not missing_roles
                else f"Missing in {roles_dir}: {', '.join(missing_roles)}",
            )
        )

        anti_file = repo_root / config.project.anti_laziness_file
        needs_anti = any(p.anti_laziness for p in config.providers.values())
        checks.append(
            (
                "anti_laziness_template",
                (not needs_anti) or anti_file.exists(),
                str(anti_file) if anti_file.exists() else "Missing anti_laziness template",
            )
        )

        tasks_path = config.tasks_path(repo_root)
        try:
            store = LocalJsonTaskStore(tasks_path)
            checks.append(("tasks_json", True, f"Valid DAG ({len(store.list_tasks())} tasks)"))
        except Exception as exc:
            checks.append(("tasks_json", False, str(exc)))

        # Provider healthchecks only for active roles (plus warn for inactive profiles)
        registry = ProviderRegistry.from_config(config)
        active_profiles = set(config.roles.as_dict().values())
        for profile_name in sorted(active_profiles):
            provider = registry.get(profile_name)
            ok, msg = provider.healthcheck()
            checks.append((f"provider:{profile_name}", ok, msg))

        # Quality gates preflight checks (executable availability, cwd, required env)
        for gate in config.gates:
            gate_cwd = repo_root / gate.cwd if gate.cwd else repo_root
            resolved_env, missing_env = resolve_env_map(gate.env, strict=False)
            lookup_path = resolved_env.get("PATH", os.environ.get("PATH"))
            exe = gate.command[0]
            resolved_exe = shutil.which(exe, path=lookup_path)
            if resolved_exe is None and ("/" in exe or "\\" in exe):
                local_candidate = Path(exe) if Path(exe).is_absolute() else (gate_cwd / exe)
                if local_candidate.is_file():
                    resolved_exe = str(local_candidate)

            problems: list[str] = []
            if not gate_cwd.is_dir():
                problems.append(f"Working directory not found: {gate_cwd}")
            if resolved_exe is None:
                problems.append(f"Executable '{exe}' not found in PATH")
            if missing_env:
                problems.append(f"Missing required env var(s): {', '.join(missing_env)}")

            if problems:
                detail = "; ".join(problems)
                if gate.optional:
                    checks.append((f"gate:{gate.name}", True, f"WARN (optional): {detail}"))
                else:
                    checks.append((f"gate:{gate.name}", False, detail))
            else:
                checks.append((f"gate:{gate.name}", True, f"Executable resolved: {resolved_exe}"))

        # Telegram notifications env preflight (non-blocking warning when env is injected at runtime)
        tg = config.notifications.telegram
        if tg.enabled:
            missing_tg = [
                env_name
                for env_name in (tg.bot_token_env, tg.chat_id_env)
                if not os.environ.get(env_name, "").strip()
            ]
            if missing_tg:
                checks.append(
                    (
                        "notifications:telegram",
                        True,
                        f"WARN: Telegram enabled, but missing env var(s): {', '.join(missing_tg)}",
                    )
                )
            else:
                checks.append(
                    (
                        "notifications:telegram",
                        True,
                        f"Telegram enabled ({len(tg.events)} event(s); env vars set)",
                    )
                )

    return checks


def cmd_doctor(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    config_path = _default_config_path(repo_root, args.config)
    checks = run_doctor_checks(repo_root, config_path)

    all_ok = True
    for name, ok, detail in checks:
        tag = "OK  " if ok else "FAIL"
        print(f"[{tag}] {name}: {detail}")
        if not ok:
            all_ok = False
    return 0 if all_ok else 1


def cmd_run(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    config_path = _default_config_path(repo_root, args.config)
    config = load_pipeline_config(config_path)
    store = LocalJsonTaskStore(config.tasks_path(repo_root))
    registry = ProviderRegistry.from_config(config)

    if args.dry_run:
        next_task = store.get_next_available_task()
        print(f"Dry-run validated config '{config_path}' and {len(store.list_tasks())} task(s).")
        if next_task:
            print(f"Next available task: #{next_task.id} ({next_task.complexity.value}) — {next_task.title}")
        else:
            print("No eligible TODO tasks available.")
        return 0

    runner = OrchestratorRunner(
        repo_root=repo_root,
        config=config,
        store=store,
        registry=registry,
        quiet=bool(args.quiet),
    )
    status = runner.run(max_tasks=args.max_tasks)
    print(f"Run finished with status: {status.value} (report: {runner.report_path})", flush=True)
    return 0 if status in (RunStatus.COMPLETED, RunStatus.ABORTED) else 1


def cmd_status(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    config_path = _default_config_path(repo_root, args.config)
    config = load_pipeline_config(config_path)
    pipeline_dir = config.pipeline_path(repo_root)
    state_path = pipeline_dir / "state.json"
    tasks_path = config.tasks_path(repo_root)

    if state_path.exists():
        state = PipelineState.model_validate(json.loads(state_path.read_text(encoding="utf-8")))
        print(f"Run ID:   {state.run_id}")
        print(f"Status:   {state.status.value} (paused={state.paused}, reason={state.pause_reason})")
        print(
            f"Active:   task={state.active_task_id} phase={state.active_phase} "
            f"role={state.active_role.value if state.active_role else None} "
            f"provider={state.active_provider}"
        )
        print(f"Batch:    {state.batch_queue}")
        print("Provider rolling windows:")
        for pname, q in state.provider_quotas.items():
            limit = f"/{q.max_requests}" if q.max_requests else ""
            cd = f" cooldown_until={q.cooldown_until}" if q.cooldown_until else ""
            print(f"  - {pname}: {q.requests_in_window}{limit} reqs in {q.window_seconds}s{cd}")
    else:
        print("No state.json found yet (pipeline has not been run).")

    if tasks_path.exists():
        store = LocalJsonTaskStore(tasks_path)
        counts = Counter(t.status.value for t in store.list_tasks())
        summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"Tasks:    {summary}")
    return 0


def cmd_control(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    config_path = _default_config_path(repo_root, args.config)
    pipeline_dir = (
        load_pipeline_config(config_path).pipeline_path(repo_root)
        if config_path.exists()
        else (repo_root / ".pipeline")
    )
    action_map = {
        "pause": ControlAction.PAUSE_AFTER_CURRENT,
        "pause_after_current": ControlAction.PAUSE_AFTER_CURRENT,
        "resume": ControlAction.RESUME,
        "abort": ControlAction.ABORT,
        "retry": ControlAction.RETRY_TASK,
        "retry_task": ControlAction.RETRY_TASK,
        "skip": ControlAction.SKIP_TASK,
        "skip_task": ControlAction.SKIP_TASK,
    }
    action = action_map[args.action]
    if action in (ControlAction.RETRY_TASK, ControlAction.SKIP_TASK) and not args.task_id:
        print(f"[ERROR] Action '{action.value}' requires --task-id", file=sys.stderr)
        return 1

    cmd, path = enqueue_control_command(
        pipeline_dir,
        action=action,
        task_id=args.task_id,
        reason=args.reason,
    )
    print(f"Enqueued control command {cmd.id} ({cmd.action.value}) -> {path}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    config_path = _default_config_path(repo_root, args.config)
    config = load_pipeline_config(config_path)
    pipeline_dir = config.pipeline_path(repo_root)
    store = LocalJsonTaskStore(config.tasks_path(repo_root))
    event_bus = EventBus(pipeline_dir, config)
    report_path = write_report(
        pipeline_dir / "REPORT.md",
        project_name=config.project.name,
        state=event_bus.state,
        tasks=store.list_tasks(),
    )
    print(f"Generated report: {report_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="orchestrator",
        description="Modular, fault-tolerant local AFK AI development pipeline orchestrator",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Copy templates into a target repository and run doctor")
    p_init.add_argument("--repo", default=".", help="Path to target git repository")
    p_init.add_argument(
        "--stack",
        choices=["generic", "python", "typescript"],
        default="generic",
        help="Starter pipeline.yaml preset",
    )
    p_init.add_argument(
        "--provider-preset",
        choices=["default", "agy-only"],
        default="default",
        help="Starter provider profile preset (default mixed providers or agy-only Gemini 4)",
    )
    p_init.add_argument("--force", action="store_true", help="Overwrite existing templates/config")
    p_init.set_defaults(func=cmd_init)

    p_doc = sub.add_parser("doctor", help="Run preflight readiness checks on target repository")
    p_doc.add_argument("--repo", default=".", help="Path to target git repository")
    p_doc.add_argument("--config", default=None, help="Path to pipeline.yaml")
    p_doc.set_defaults(func=cmd_doctor)

    p_run = sub.add_parser("run", help="Run the AFK pipeline orchestrator")
    p_run.add_argument("--repo", default=".", help="Path to target git repository")
    p_run.add_argument("--config", default=None, help="Path to pipeline.yaml")
    p_run.add_argument("--max-tasks", type=int, default=None, help="Stop after N tasks")
    p_run.add_argument("--dry-run", action="store_true", help="Validate config/tasks and exit")
    p_run.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress live progress lines in stdout during run",
    )
    p_run.set_defaults(func=cmd_run)

    p_status = sub.add_parser("status", help="Inspect current state.json, tasks, and quotas")
    p_status.add_argument("--repo", default=".", help="Path to target git repository")
    p_status.add_argument("--config", default=None, help="Path to pipeline.yaml")
    p_status.set_defaults(func=cmd_status)

    p_ctrl = sub.add_parser("control", help="Send a race-free control command to a running pipeline")
    p_ctrl.add_argument(
        "action",
        choices=["pause", "pause_after_current", "resume", "abort", "retry", "retry_task", "skip", "skip_task"],
    )
    p_ctrl.add_argument("--task-id", default=None, help="Task ID for retry/skip commands")
    p_ctrl.add_argument("--reason", default=None, help="Optional operator note")
    p_ctrl.add_argument("--repo", default=".", help="Path to target git repository")
    p_ctrl.add_argument("--config", default=None, help="Path to pipeline.yaml")
    p_ctrl.set_defaults(func=cmd_control)

    p_rep = sub.add_parser("report", help="Regenerate .pipeline/REPORT.md from current state/tasks")
    p_rep.add_argument("--repo", default=".", help="Path to target git repository")
    p_rep.add_argument("--config", default=None, help="Path to pipeline.yaml")
    p_rep.set_defaults(func=cmd_report)

    return parser


def _configure_stdio_buffering() -> None:
    """Enable line buffering on stdout/stderr so piped output (e.g., PowerShell Tee-Object) flushes immediately."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            with contextlib.suppress(Exception):
                reconfigure(line_buffering=True, write_through=True)


def main(argv: list[str] | None = None) -> int:
    _configure_stdio_buffering()
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
