"""Race-free Control Plane (`control_inbox/*.json` -> `control_processed/`) and `INBOX.md` handoff."""

from __future__ import annotations

import contextlib
import json
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path

from orchestrator.fs_utils import (
    atomic_write_json,
    atomic_write_text,
    normalize_newlines,
    sha256_text,
)
from orchestrator.models import ControlAction, ControlCommand, utc_now_iso

_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)

INBOX_TEMPLATE = """# Operator Inbox (INBOX.md)

<!--
Add ad-hoc instructions or mid-run course corrections below.
The orchestrator reads this file before the next task, injects it into the
dynamic prompt tail, archives it to .pipeline/inbox_archive/, and resets this file.
-->
"""


def enqueue_control_command(
    pipeline_dir: Path,
    *,
    action: ControlAction,
    task_id: str | None = None,
    reason: str | None = None,
    command_id: str | None = None,
) -> tuple[ControlCommand, Path]:
    """Atomically drop a command file into `.pipeline/control_inbox/` (Web UI & CLI contract)."""
    inbox_dir = pipeline_dir / "control_inbox"
    inbox_dir.mkdir(parents=True, exist_ok=True)
    cmd_id = command_id or f"cmd_{uuid.uuid4().hex[:10]}"
    cmd = ControlCommand(
        id=cmd_id,
        action=action,
        task_id=task_id,
        reason=reason,
    )
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    target_path = inbox_dir / f"{ts}_{cmd.id}.json"
    atomic_write_json(target_path, cmd.model_dump(mode="json"))
    return cmd, target_path


class ControlPlane:
    """One-way, race-free command consumer for external Web UI and CLI control.

    Primary contract: external producers atomically write individual command files into
    `.pipeline/control_inbox/<ts>_<cmd_id>.json`. The runner processes them in sorted order
    and writes the completed record into `.pipeline/control_processed/<cmd_id>.json`, removing
    the inbox file. No single file is ever read-modified-written concurrently by UI and runner.

    Backward-compatible fallback: also accepts a legacy drop-in `.pipeline/control.json`
    without modifying it in place, tracking processed command IDs in `state.json`.
    """

    def __init__(self, pipeline_dir: Path) -> None:
        self.pipeline_dir = pipeline_dir
        self.inbox_dir = pipeline_dir / "control_inbox"
        self.processed_dir = pipeline_dir / "control_processed"
        self.legacy_control_path = pipeline_dir / "control.json"
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def poll_pending(
        self,
        processed_ids: set[str],
    ) -> list[tuple[ControlCommand, Path | None]]:
        """Return unprocessed commands in deterministic order."""
        pending: list[tuple[ControlCommand, Path | None]] = []

        # 1. Primary race-free inbox directory
        for file_path in sorted(self.inbox_dir.glob("*.json")):
            if file_path.name.startswith("."):
                continue
            try:
                raw = json.loads(file_path.read_text(encoding="utf-8"))
                cmd = ControlCommand.model_validate(raw)
            except Exception:
                continue
            if cmd.id in processed_ids:
                self._remove_inbox_file(file_path)
                continue
            pending.append((cmd, file_path))

        # 2. Optional legacy single-file drop-in `.pipeline/control.json` (read-only by runner)
        if self.legacy_control_path.exists():
            for cmd in self._parse_legacy_control_file(self.legacy_control_path):
                if cmd.id not in processed_ids and all(c.id != cmd.id for c, _ in pending):
                    pending.append((cmd, None))

        return pending

    def _parse_legacy_control_file(self, path: Path) -> list[ControlCommand]:
        try:
            text = path.read_text(encoding="utf-8")
            raw = json.loads(text)
        except Exception:
            return []
        if not isinstance(raw, dict):
            return []

        commands: list[ControlCommand] = []
        if isinstance(raw.get("commands"), list):
            for idx, entry in enumerate(raw["commands"]):
                if not isinstance(entry, dict):
                    continue
                if "id" not in entry:
                    digest = sha256_text(json.dumps(entry, sort_keys=True))[:10]
                    entry = {**entry, "id": f"legacy_{idx}_{digest}"}
                try:
                    commands.append(ControlCommand.model_validate(entry))
                except Exception:
                    continue
        elif "action" in raw:
            entry = dict(raw)
            if "id" not in entry:
                entry["id"] = f"legacy_{sha256_text(text)[:10]}"
            with contextlib.suppress(Exception):
                commands.append(ControlCommand.model_validate(entry))
        elif raw.get("pause_after_current") is True:
            digest = sha256_text(text)[:10]
            commands.append(
                ControlCommand(
                    id=f"legacy_pause_{digest}",
                    action=ControlAction.PAUSE_AFTER_CURRENT,
                    reason="Legacy control.json pause_after_current=true",
                )
            )
        return commands

    def mark_processed(
        self,
        command: ControlCommand,
        source_path: Path | None,
        *,
        result: str,
    ) -> ControlCommand:
        """Archive processed command to control_processed/ and remove it from control_inbox/."""
        completed = command.model_copy(
            update={
                "processed_at": utc_now_iso(),
                "result": result,
            }
        )
        safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", completed.id)
        archive_path = self.processed_dir / f"{safe_id}.json"
        atomic_write_json(archive_path, completed.model_dump(mode="json"))

        if source_path is not None:
            self._remove_inbox_file(source_path)
        return completed

    @staticmethod
    def _remove_inbox_file(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass


class InboxManager:
    """Manages operator mid-run instructions in `.pipeline/INBOX.md`."""

    def __init__(self, pipeline_dir: Path) -> None:
        self.pipeline_dir = pipeline_dir
        self.inbox_path = pipeline_dir / "INBOX.md"
        self.archive_dir = pipeline_dir / "inbox_archive"

    def peek(self) -> str | None:
        """Return active operator instructions if INBOX.md has non-comment content."""
        if not self.inbox_path.exists():
            return None
        raw = normalize_newlines(self.inbox_path.read_text(encoding="utf-8", errors="replace"))
        without_comments = _HTML_COMMENT_RE.sub("", raw)
        # Strip default template heading if nothing else was written
        meaningful_lines = [
            ln
            for ln in without_comments.splitlines()
            if ln.strip() and ln.strip().lower() != "# operator inbox (inbox.md)"
        ]
        if not meaningful_lines:
            return None
        return "\n".join(meaningful_lines).strip()

    def consume(self, *, task_id: str | None) -> str | None:
        """Read, archive, and reset INBOX.md before starting the next task."""
        content = self.peek()
        if content is None:
            return None
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        safe_task = re.sub(r"[^A-Za-z0-9_.-]+", "_", task_id or "global")
        archive_file = self.archive_dir / f"{stamp}_{safe_task}.md"
        atomic_write_text(archive_file, content + "\n")
        atomic_write_text(self.inbox_path, INBOX_TEMPLATE)
        return content
