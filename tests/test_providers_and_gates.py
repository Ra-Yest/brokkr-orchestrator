"""Tests for CliAgentProvider (modes, parsers, 429, timeout/kill, Windows .cmd), GateRunner, and Windows atomic retry."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest

from orchestrator.config import GateConfig, ProviderProfileConfig
from orchestrator.fs_utils import atomic_write_text
from orchestrator.gates import GateRunner, trim_error_log
from orchestrator.models import AgentRole
from orchestrator.process_utils import run_noninteractive_process
from orchestrator.providers.base import ProviderRequest, ProviderResponse
from orchestrator.providers.cli_provider import CliAgentProvider
from tests.conftest import make_test_config


def test_cli_provider_stdin_arg_tempfile_and_json_parsers(tmp_path: Path) -> None:
    helper = tmp_path / "echo_agent.py"
    helper.write_text(
        "import json, pathlib, sys\n"
        "mode = sys.argv[1]\n"
        "if mode == 'stdin':\n"
        "    text = sys.stdin.read()\n"
        "    print(json.dumps({'result': {'text': f'STDIN:{text.strip()}'}}))\n"
        "elif mode == 'arg':\n"
        "    print(f'ARG:{sys.argv[2]}')\n"
        "elif mode == 'tempfile':\n"
        "    prompt_text = pathlib.Path(sys.argv[2]).read_text(encoding='utf-8').strip()\n"
        "    out_file = pathlib.Path(sys.argv[3])\n"
        "    out_file.write_text(f'FILE:{prompt_text}', encoding='utf-8')\n"
        "    print('ignored stdout')\n"
        "elif mode == 'rate_limit':\n"
        "    sys.stderr.write('HTTP 429 Too Many Requests; retry after 12s\\n')\n"
        "    sys.exit(1)\n",
        encoding="utf-8",
    )

    attempt_dir = tmp_path / "attempt_1"
    attempt_dir.mkdir()

    # 1. stdin + json_field parser
    p_stdin = CliAgentProvider(
        "p_stdin",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(helper), "stdin"],
            prompt_mode="stdin",
            output_parser="json_field",
            output_json_path="result.text",
        ),
    )
    r1 = p_stdin.invoke(
        ProviderRequest(
            run_id="r1",
            task_id="T1",
            role=AgentRole.WORKER_EASY,
            prompt="hello-stdin",
            workdir=tmp_path,
            timeout_seconds=30,
            attempt_dir=attempt_dir,
        )
    )
    assert r1.exit_code == 0
    assert r1.parsed_text == "STDIN:hello-stdin"

    # 2. arg delivery
    p_arg = CliAgentProvider(
        "p_arg",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(helper), "arg"],
            prompt_mode="arg",
            args_template=["{prompt}"],
        ),
    )
    r2 = p_arg.invoke(
        ProviderRequest(
            run_id="r1",
            task_id="T1",
            role=AgentRole.WORKER_EASY,
            prompt="hello-arg",
            workdir=tmp_path,
            timeout_seconds=30,
            attempt_dir=attempt_dir,
        )
    )
    assert r2.exit_code == 0
    assert r2.parsed_text.strip() == "ARG:hello-arg"

    # 3. tempfile + {output_file}
    p_file = CliAgentProvider(
        "p_file",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(helper), "tempfile"],
            prompt_mode="tempfile",
            args_template=["{prompt_file}", "{output_file}"],
        ),
    )
    r3 = p_file.invoke(
        ProviderRequest(
            run_id="r1",
            task_id="T1",
            role=AgentRole.WORKER_EASY,
            prompt="hello-file",
            workdir=tmp_path,
            timeout_seconds=30,
            attempt_dir=attempt_dir,
        )
    )
    assert r3.exit_code == 0
    assert r3.parsed_text.strip() == "FILE:hello-file"
    assert r3.metadata["used_output_file"] is True

    # 4. rate limit regex + retry-after extraction
    p_rl = CliAgentProvider(
        "p_rl",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(helper), "rate_limit"],
            prompt_mode="stdin",
        ),
    )
    r4 = p_rl.invoke(
        ProviderRequest(
            run_id="r1",
            task_id="T1",
            role=AgentRole.WORKER_EASY,
            prompt="hi",
            workdir=tmp_path,
            timeout_seconds=30,
            attempt_dir=attempt_dir,
        )
    )
    assert r4.rate_limited is True
    assert r4.retry_after_seconds == 12


def test_cli_provider_hard_timeout_kills_process(tmp_path: Path) -> None:
    sleeper = tmp_path / "hang.py"
    sleeper.write_text("import time; time.sleep(30)\n", encoding="utf-8")
    provider = CliAgentProvider(
        "p_hang",
        ProviderProfileConfig(
            kind="cli",
            command=[sys.executable, str(sleeper)],
            prompt_mode="arg",
            timeout_seconds=1,
        ),
    )
    res = provider.invoke(
        ProviderRequest(
            run_id="r1",
            task_id="T1",
            role=AgentRole.WORKER_EASY,
            prompt="x",
            workdir=tmp_path,
            timeout_seconds=1,
            attempt_dir=tmp_path,
        )
    )
    assert res.timed_out is True
    assert res.duration_seconds < 10.0


def test_windows_cmd_wrapper_resolution_via_which(tmp_path: Path) -> None:
    if sys.platform != "win32":
        return
    wrapper_dir = tmp_path / "bin"
    wrapper_dir.mkdir()
    cmd_file = wrapper_dir / "fake_agent_cli.cmd"
    cmd_file.write_text("@echo off\r\necho CMD_WRAPPER_OK\r\n", encoding="utf-8")

    provider = CliAgentProvider(
        "p_cmd",
        ProviderProfileConfig(
            kind="cli",
            command=["fake_agent_cli"],
            prompt_mode="stdin",
        ),
    )
    old_path = os.environ.get("PATH", "")
    new_path = f"{wrapper_dir};{old_path}"
    with mock.patch.dict(os.environ, {"PATH": new_path}):
        ok, msg = provider.healthcheck()
        assert ok is True
        assert "fake_agent_cli.cmd" in msg.lower()

        res = provider.invoke(
            ProviderRequest(
                run_id="r1",
                task_id="T1",
                role=AgentRole.WORKER_EASY,
                prompt="ignored",
                workdir=tmp_path,
                timeout_seconds=15,
                attempt_dir=tmp_path,
            )
        )
        assert res.exit_code == 0
        assert "CMD_WRAPPER_OK" in res.parsed_text


def test_gate_runner_log_trimming_and_playwright_screenshots(tmp_path: Path) -> None:
    noisy_lines = [f"INFO line {i}" for i in range(150)]
    noisy_lines.append("Traceback (most recent call last):")
    noisy_lines.append("  File 'app.py', line 42, in handle")
    noisy_lines.append("AssertionError: expected 200, got 500")
    noisy_lines.extend([f"DEBUG post {i}" for i in range(60)])
    trimmed = trim_error_log("\n".join(noisy_lines), max_lines=70)
    assert "AssertionError: expected 200, got 500" in trimmed
    assert len(trimmed.splitlines()) <= 75

    # Playwright gate collects screenshots written to ORCHESTRATOR_SCREENSHOTS_DIR
    shot_script = tmp_path / "make_shot.py"
    shot_script.write_text(
        "import os, pathlib\n"
        "d = pathlib.Path(os.environ['ORCHESTRATOR_SCREENSHOTS_DIR'])\n"
        "(d / 'home.png').write_bytes(b'\\x89PNG')\n",
        encoding="utf-8",
    )
    cfg = make_test_config(
        gates=[
            GateConfig(
                name="ui_playwright",
                command=[sys.executable, str(shot_script)],
                kind="playwright",
            )
        ]
    )
    runner = GateRunner(tmp_path, cfg)
    attempt_dir = tmp_path / "attempt_1"
    attempt_dir.mkdir()
    res = runner.run_all(
        attempt_dir=attempt_dir,
        verify_cmd=f"{json.dumps(sys.executable)} -c \"print('verify ok')\"",
    )
    assert res.passed is True
    assert any(p.endswith("home.png") for p in res.screenshots)
    assert len(res.results) == 2


def test_atomic_write_retries_on_windows_permission_error(tmp_path: Path) -> None:
    target = tmp_path / "state.json"
    real_replace = os.replace
    attempts = {"count": 0}

    def flaky_replace(src: Path, dst: Path) -> None:
        attempts["count"] += 1
        if attempts["count"] <= 2:
            raise PermissionError(13, "File locked by Web UI reader")
        real_replace(src, dst)

    with mock.patch("orchestrator.fs_utils.os.replace", side_effect=flaky_replace):
        atomic_write_text(target, '{"ok": true}\n', max_retries=5, retry_delay_seconds=0.005)

    assert attempts["count"] == 3
    assert target.read_text(encoding="utf-8") == '{"ok": true}\n'


def test_cli_provider_blocks_arg_mode_for_windows_cmd_and_bat(tmp_path: Path) -> None:
    wrapper_dir = tmp_path / "wrappers"
    wrapper_dir.mkdir()
    cmd_path = wrapper_dir / "agent_wrap.cmd"
    bat_path = wrapper_dir / "agent_wrap.BAT"
    cmd_path.write_text("@echo off\r\necho hi\r\n", encoding="utf-8")
    bat_path.write_text("@echo off\r\necho hi\r\n", encoding="utf-8")

    for target_file in (cmd_path, bat_path):
        provider = CliAgentProvider(
            "p_unsafe_cmd",
            ProviderProfileConfig(
                kind="cli",
                command=[str(target_file)],
                prompt_mode="arg",
                args_template=["{prompt}"],
            ),
        )
        with (
            mock.patch(
                "orchestrator.providers.cli_provider.shutil.which",
                return_value=str(target_file),
            ),
            mock.patch(
                "orchestrator.providers.cli_provider.resolve_executable",
                return_value=[str(target_file)],
            ),
        ):
            with pytest.raises(ValueError, match="prompt_mode='arg' is unsafe"):
                provider.healthcheck()

            with pytest.raises(ValueError, match="prompt_mode='arg' is unsafe"):
                provider.invoke(
                    ProviderRequest(
                        run_id="r1",
                        task_id="T1",
                        role=AgentRole.WORKER_EASY,
                        prompt="hello",
                        workdir=tmp_path,
                        timeout_seconds=15,
                        attempt_dir=tmp_path,
                    )
                )


def test_subprocess_utf8_cyrillic_roundtrip_and_python_utf8_env(tmp_path: Path) -> None:
    echo_script = tmp_path / "utf8_echo.py"
    echo_script.write_text(
        "import os, sys\n"
        "payload = sys.stdin.read()\n"
        "assert os.environ.get('PYTHONUTF8') == '1'\n"
        "assert os.environ.get('PYTHONIOENCODING') == 'utf-8'\n"
        "print(f'ECHO: {payload.strip()}')\n",
        encoding="utf-8",
    )
    cyrillic_prompt = "Привет, мир! Проверка кириллицы: ёж, щука, статус — ГОТОВО."
    res = run_noninteractive_process(
        [sys.executable, str(echo_script)],
        cwd=tmp_path,
        stdin_data=cyrillic_prompt,
        timeout_seconds=15,
    )
    assert res.exit_code == 0
    assert res.timed_out is False
    assert f"ECHO: {cyrillic_prompt}" in res.stdout


def test_cli_provider_rate_limit_detection_avoids_false_positives_on_long_success_stdout(
    tmp_path: Path,
) -> None:
    helper = tmp_path / "rl_agent.py"
    helper.write_text(
        "import sys\n"
        "mode = sys.argv[1]\n"
        "if mode == 'long_ok':\n"
        "    body = 'Implemented HTTP 429 handling and quota_exceeded retry logic. ' * 20\n"
        "    assert len(body.strip()) > 800\n"
        "    print(body)\n"
        "    sys.exit(0)\n"
        "elif mode == 'nonzero_stderr':\n"
        "    sys.stderr.write('429 Too Many Requests\\n')\n"
        "    sys.exit(1)\n"
        "elif mode == 'zero_short_stdout':\n"
        "    print('Error 429: Too Many Requests')\n"
        "    sys.exit(0)\n",
        encoding="utf-8",
    )
    attempt_dir = tmp_path / "attempt_1"
    attempt_dir.mkdir()

    def _invoke(mode: str) -> ProviderResponse:
        provider = CliAgentProvider(
            f"p_{mode}",
            ProviderProfileConfig(
                kind="cli",
                command=[sys.executable, str(helper), mode],
                prompt_mode="stdin",
            ),
        )
        return provider.invoke(
            ProviderRequest(
                run_id="r1",
                task_id="T1",
                role=AgentRole.WORKER_EASY,
                prompt="go",
                workdir=tmp_path,
                timeout_seconds=30,
                attempt_dir=attempt_dir,
            )
        )

    res_long_ok = _invoke("long_ok")
    assert res_long_ok.exit_code == 0
    assert res_long_ok.raw_stderr == ""
    assert len(res_long_ok.raw_stdout.strip()) > 800
    assert res_long_ok.rate_limited is False

    res_nonzero = _invoke("nonzero_stderr")
    assert res_nonzero.exit_code == 1
    assert res_nonzero.rate_limited is True

    res_short_zero = _invoke("zero_short_stdout")
    assert res_short_zero.exit_code == 0
    assert res_short_zero.rate_limited is True


