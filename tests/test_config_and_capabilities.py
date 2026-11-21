"""Tests for PipelineConfig validation, provider capabilities, optional env vars, and example YAML."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from orchestrator.config import (
    EnvEntryConfig,
    MissingEnvVarError,
    PipelineConfig,
    ProviderProfileConfig,
    RoleMappingConfig,
    load_pipeline_config,
    resolve_env_map,
)


def test_api_provider_defaults_to_non_editing_and_blocks_writing_roles() -> None:
    api_profile = ProviderProfileConfig(
        kind="api",
        base_url="https://api.example.com/v1",
        model="gpt-5",
    )
    assert api_profile.can_edit_files is False

    cli_profile = ProviderProfileConfig(kind="cli", command=["codex", "exec"])
    assert cli_profile.can_edit_files is True

    # Valid: API provider assigned only to read-only roles
    cfg = PipelineConfig(
        providers={"cli_w": cli_profile, "api_r": api_profile},
        roles=RoleMappingConfig(
            planner="api_r",
            worker_easy="cli_w",
            worker_medium="cli_w",
            worker_hard="cli_w",
            pre_reviewer="api_r",
            reviewer="api_r",
            architect="cli_w",
            refactorer="cli_w",
        ),
    )
    assert cfg.roles.reviewer == "api_r"

    # Invalid: API provider (or CLI with can_edit_files=False) assigned to writing roles
    for writing_role in ("worker_easy", "worker_medium", "worker_hard", "architect", "refactorer"):
        role_kwargs = {
            "planner": "api_r",
            "worker_easy": "cli_w",
            "worker_medium": "cli_w",
            "worker_hard": "cli_w",
            "pre_reviewer": "api_r",
            "reviewer": "api_r",
            "architect": "cli_w",
            "refactorer": "cli_w",
        }
        role_kwargs[writing_role] = "api_r"
        with pytest.raises(ValidationError, match="can_edit_files is False"):
            PipelineConfig(
                providers={"cli_w": cli_profile, "api_r": api_profile},
                roles=RoleMappingConfig(**role_kwargs),
            )


def test_api_provider_cannot_set_can_edit_files_true() -> None:
    with pytest.raises(ValidationError, match="can_edit_files must be false"):
        ProviderProfileConfig(
            kind="api",
            can_edit_files=True,
            base_url="https://api.example.com/v1",
            model="gpt-5",
        )


def test_cli_provider_with_can_edit_files_false_rejected_for_worker() -> None:
    ro_cli = ProviderProfileConfig(kind="cli", can_edit_files=False, command=["claude", "-p"])
    rw_cli = ProviderProfileConfig(kind="cli", can_edit_files=True, command=["codex", "exec"])
    with pytest.raises(ValidationError, match="can_edit_files is False"):
        PipelineConfig(
            providers={"ro": ro_cli, "rw": rw_cli},
            roles=RoleMappingConfig(
                planner="ro",
                worker_easy="ro",
                worker_medium="rw",
                worker_hard="rw",
                pre_reviewer="ro",
                reviewer="ro",
                architect="rw",
                refactorer="rw",
            ),
        )


def test_optional_env_var_syntax_for_oauth_codex() -> None:
    # Unset optional ${OPENAI_API_KEY:-} is cleanly omitted (not empty string, no error)
    resolved, missing = resolve_env_map(
        {
            "OPENAI_API_KEY": "${OPENAI_API_KEY:-}",
            "FALLBACK_VAR": "${UNSET_VAR:-fallback_val}",
            "OPT_ENTRY": EnvEntryConfig(from_env="ALSO_UNSET", optional=True),
        },
        host_env={},
        strict=True,
    )
    assert "OPENAI_API_KEY" not in resolved
    assert "OPT_ENTRY" not in resolved
    assert resolved["FALLBACK_VAR"] == "fallback_val"
    assert missing == []

    # When OPENAI_API_KEY IS set in host env, it is forwarded
    resolved_set, _ = resolve_env_map(
        {"OPENAI_API_KEY": "${OPENAI_API_KEY:-}"},
        host_env={"OPENAI_API_KEY": "sk-test-123"},
        strict=True,
    )
    assert resolved_set["OPENAI_API_KEY"] == "sk-test-123"

    # Required ${SECRET_TOKEN} fails when missing
    with pytest.raises(MissingEnvVarError, match="SECRET_TOKEN"):
        resolve_env_map({"SECRET_TOKEN": "${SECRET_TOKEN}"}, host_env={}, strict=True)


def test_example_pipeline_yaml_is_valid() -> None:
    example_path = Path(__file__).resolve().parent.parent / "pipeline.example.yaml"
    cfg = load_pipeline_config(example_path)
    assert cfg.providers["provider_master"].kind == "cli"
    assert cfg.providers["provider_master"].can_edit_files is True
    assert cfg.providers["provider_reviewer_api"].kind == "api"
    assert cfg.providers["provider_reviewer_api"].can_edit_files is False
