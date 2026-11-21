"""Zero-token quality gates runner (linters, typechecks, tests, Playwright, verify_cmd) and log trimming."""

from __future__ import annotations

import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path

from orchestrator.config import GateConfig, PipelineConfig, resolve_env_map
from orchestrator.process_utils import run_noninteractive_process

_ERROR_LINE_RE = re.compile(
    r"(?:\bERROR\b|\bError\b|\bFAIL\b|\bFAILED\b|Traceback|AssertionError|"
    r"SyntaxError|TypeError|ValueError|ImportError|Exception:|panic:|ERR!|error\[E\d+\])"
)
_IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".webp"})


@dataclass(frozen=True)
class GateResult:
    name: str
    kind: str
    command: list[str]
    exit_code: int
    passed: bool
    optional: bool
    duration_seconds: float
    timed_out: bool
    raw_output: str
    trimmed_output: str
    screenshots: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GatesRunResult:
    passed: bool
    results: list[GateResult]
    combined_trimmed_log: str
    screenshots: list[str]


def trim_error_log(
    text: str,
    *,
    max_lines: int = 80,
    context_before: int = 15,
    context_after: int = 35,
) -> str:
    """Extract 50-80 focused lines around Error/FAIL/Traceback markers for token-efficient feedback."""
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return "\n".join(lines)

    hit_indices = [idx for idx, line in enumerate(lines) if _ERROR_LINE_RE.search(line)]
    if not hit_indices:
        return "... [earlier log lines truncated] ...\n" + "\n".join(lines[-max_lines:])

    # Merge windows around error matches
    windows: list[tuple[int, int]] = []
    for hit in hit_indices:
        start = max(0, hit - context_before)
        end = min(len(lines), hit + context_after + 1)
        if windows and start <= windows[-1][1]:
            windows[-1] = (windows[-1][0], max(windows[-1][1], end))
        else:
            windows.append((start, end))

    # Assemble slices prioritizing the last (most decisive) failure windows up to max_lines
    selected: list[str] = []
    budget = max_lines
    chosen_windows: list[tuple[int, int]] = []
    for start, end in reversed(windows):
        span = end - start
        if span <= budget:
            chosen_windows.append((start, end))
            budget -= span
        elif budget > 0:
            # Keep the tail of this window around the error
            chosen_windows.append((end - budget, end))
            budget = 0
        if budget <= 0:
            break

    chosen_windows.reverse()
    prev_end = 0
    for start, end in chosen_windows:
        if start > prev_end:
            selected.append(f"... [{start - prev_end} lines omitted] ...")
        selected.extend(lines[start:end])
        prev_end = end
    if prev_end < len(lines):
        selected.append(f"... [{len(lines) - prev_end} trailing lines omitted] ...")

    return "\n".join(selected)


def _split_verify_cmd(verify_cmd: str) -> list[str]:
    """Parse a task-level verify_cmd string into argv without shell interpolation when possible."""
    tokens = shlex.split(verify_cmd, posix=(sys.platform != "win32"))
    if sys.platform == "win32":
        # Strip wrapping quotes left by posix=False shlex on Windows
        cleaned: list[str] = []
        for token in tokens:
            if len(token) >= 2 and token[0] == token[-1] and token[0] in ('"', "'"):
                cleaned.append(token[1:-1])
            else:
                cleaned.append(token)
        return cleaned
    return tokens


class GateRunner:
    """Executes configured quality gates and per-task verify_cmd."""

    def __init__(self, repo_root: Path, config: PipelineConfig) -> None:
        self.repo_root = repo_root.resolve()
        self.config = config

    def run_all(
        self,
        *,
        attempt_dir: Path,
        verify_cmd: str | None = None,
        fail_fast: bool = True,
    ) -> GatesRunResult:
        gates_to_run: list[GateConfig] = list(self.config.gates)
        if verify_cmd and verify_cmd.strip():
            gates_to_run.append(
                GateConfig(
                    name="task_verify_cmd",
                    command=_split_verify_cmd(verify_cmd.strip()),
                    timeout_seconds=300,
                    kind="command",
                    optional=False,
                )
            )

        screenshots_dir = attempt_dir / "screenshots"
        results: list[GateResult] = []
        collected_screenshots: list[str] = []
        all_passed = True

        for gate in gates_to_run:
            result = self._run_single_gate(gate, screenshots_dir=screenshots_dir)
            results.append(result)
            collected_screenshots.extend(result.screenshots)
            if not result.passed and not result.optional:
                all_passed = False
                if fail_fast:
                    break

        trimmed_blocks: list[str] = []
        for res in results:
            status_label = "PASS" if res.passed else ("WARN(optional)" if res.optional else "FAIL")
            header = f"=== GATE: {res.name} [{status_label}, exit={res.exit_code}, {res.duration_seconds:.1f}s] ==="
            if res.passed:
                trimmed_blocks.append(header)
            else:
                trimmed_blocks.append(f"{header}\n{res.trimmed_output}")

        return GatesRunResult(
            passed=all_passed,
            results=results,
            combined_trimmed_log="\n\n".join(trimmed_blocks),
            screenshots=sorted(set(collected_screenshots)),
        )

    def _run_single_gate(self, gate: GateConfig, *, screenshots_dir: Path) -> GateResult:
        gate_cwd = self.repo_root / gate.cwd if gate.cwd else self.repo_root
        resolved_env, _ = resolve_env_map(gate.env, strict=True)
        if gate.kind == "playwright":
            screenshots_dir.mkdir(parents=True, exist_ok=True)
        resolved_env["ORCHESTRATOR_SCREENSHOTS_DIR"] = str(screenshots_dir)

        before_shots = self._list_screenshots(screenshots_dir)
        proc_res = run_noninteractive_process(
            gate.command,
            cwd=gate_cwd,
            env=resolved_env,
            stdin_data=None,
            timeout_seconds=gate.timeout_seconds,
        )
        after_shots = self._list_screenshots(screenshots_dir)
        new_shots = sorted(after_shots - before_shots)

        combined_raw = proc_res.stdout
        if proc_res.stderr:
            combined_raw = (
                f"{combined_raw}\n--- STDERR ---\n{proc_res.stderr}"
                if combined_raw
                else proc_res.stderr
            )
        if proc_res.timed_out:
            combined_raw += f"\n[GATE TIMED OUT after {gate.timeout_seconds}s]"

        passed = (not proc_res.timed_out) and (proc_res.exit_code == 0)
        trimmed = "" if passed else trim_error_log(combined_raw, max_lines=80)

        return GateResult(
            name=gate.name,
            kind=gate.kind,
            command=list(gate.command),
            exit_code=proc_res.exit_code,
            passed=passed,
            optional=gate.optional,
            duration_seconds=proc_res.duration_seconds,
            timed_out=proc_res.timed_out,
            raw_output=combined_raw,
            trimmed_output=trimmed,
            screenshots=new_shots,
        )

    def _list_screenshots(self, screenshots_dir: Path) -> set[str]:
        if not screenshots_dir.exists():
            return set()
        found: set[str] = set()
        for path in screenshots_dir.rglob("*"):
            if path.is_file() and path.suffix.lower() in _IMAGE_SUFFIXES:
                found.add(path.as_posix())
        return found
