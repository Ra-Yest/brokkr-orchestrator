"""Filesystem utilities: atomic writes with Windows retry, hashing, and snapshots."""

from __future__ import annotations

import contextlib
import fnmatch
import hashlib
import json
import os
import time
import uuid
from collections.abc import Collection
from pathlib import Path
from typing import Any


def normalize_newlines(text: str) -> str:
    """Normalize CRLF/CR line endings to LF for deterministic hashing and caching."""
    return text.replace("\r\n", "\n").replace("\r", "\n")


def sha256_text(text: str) -> str:
    """Compute SHA-256 hex digest of UTF-8 text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file in chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(65536):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_replace_with_retry(
    src: Path,
    dst: Path,
    *,
    max_retries: int = 5,
    retry_delay_seconds: float = 0.05,
) -> None:
    """Atomically replace dst with src, retrying on transient Windows PermissionError."""
    last_exc: OSError | None = None
    for attempt in range(max_retries):
        try:
            os.replace(src, dst)
            return
        except PermissionError as exc:
            last_exc = exc
            if attempt + 1 >= max_retries:
                break
            time.sleep(retry_delay_seconds * (attempt + 1))
        except OSError as exc:
            # WinError 5 (Access Denied) / WinError 32 (Sharing Violation)
            winerror = getattr(exc, "winerror", None)
            if winerror in (5, 32) and attempt + 1 < max_retries:
                last_exc = exc
                time.sleep(retry_delay_seconds * (attempt + 1))
                continue
            raise
    if last_exc is not None:
        raise last_exc


def atomic_write_text(
    path: Path,
    content: str,
    *,
    encoding: str = "utf-8",
    max_retries: int = 5,
    retry_delay_seconds: float = 0.05,
) -> None:
    """Write text to a temp file in the target directory and atomically replace target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    tmp_path = path.parent / tmp_name
    try:
        with tmp_path.open("w", encoding=encoding, newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        atomic_replace_with_retry(
            tmp_path,
            path,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
    finally:
        if tmp_path.exists():
            with contextlib.suppress(OSError):
                tmp_path.unlink()


def atomic_write_json(
    path: Path,
    payload: Any,
    *,
    indent: int = 2,
    max_retries: int = 5,
    retry_delay_seconds: float = 0.05,
) -> None:
    """Serialize payload to JSON and atomically write to path."""
    serialized = json.dumps(payload, ensure_ascii=False, indent=indent, sort_keys=False) + "\n"
    atomic_write_text(
        path,
        serialized,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
    )


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    """Append a single JSON object line to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line)
        handle.flush()


def _normalize_snapshot_pattern(pattern: str) -> str:
    norm = pattern.replace("\\", "/")
    while norm.startswith("./"):
        norm = norm[2:]
    return norm.lstrip("/")


def _is_snapshot_excluded(rel_posix: str, patterns: tuple[str, ...]) -> bool:
    for pat in patterns:
        if not pat:
            continue
        if pat.endswith("/**"):
            prefix = pat[:-3].rstrip("/")
            if rel_posix == prefix or rel_posix.startswith(prefix + "/"):
                return True
            continue
        if pat.endswith("/"):
            prefix = pat.rstrip("/")
            if rel_posix == prefix or rel_posix.startswith(prefix + "/"):
                return True
            continue
        if "*" in pat or "?" in pat or "[" in pat:
            if fnmatch.fnmatchcase(rel_posix, pat):
                return True
            continue
        if rel_posix == pat or rel_posix.startswith(pat + "/"):
            return True
    return False


def snapshot_directory_hashes(
    root: Path,
    *,
    exclude: Collection[str] | None = None,
) -> dict[str, str]:
    """Capture SHA-256 hashes of every non-excluded file under root using POSIX relative paths."""
    if not root.exists():
        return {}
    norm_patterns = tuple(_normalize_snapshot_pattern(p) for p in (exclude or ()))
    hashes: dict[str, str] = {}
    root_resolved = root.resolve()

    for dirpath, dirnames, filenames in os.walk(root_resolved):
        current = Path(dirpath)
        rel_dir = "" if current == root_resolved else current.relative_to(root_resolved).as_posix()

        # Prune excluded directories before walking into them (prevents races with control_inbox/)
        dirnames.sort()
        if norm_patterns:
            dirnames[:] = [
                d
                for d in dirnames
                if not _is_snapshot_excluded(
                    f"{rel_dir}/{d}" if rel_dir else d,
                    norm_patterns,
                )
            ]

        for filename in sorted(filenames):
            rel_file = f"{rel_dir}/{filename}" if rel_dir else filename
            if norm_patterns and _is_snapshot_excluded(rel_file, norm_patterns):
                continue
            item = current / filename
            if item.is_file():
                hashes[rel_file] = sha256_file(item)

    return {k: hashes[k] for k in sorted(hashes)}


def diff_directory_snapshots(
    before: dict[str, str],
    after: dict[str, str],
) -> list[str]:
    """Return sorted descriptions of any created, deleted, or modified files between snapshots."""
    changes: list[str] = []
    all_keys = sorted(set(before.keys()) | set(after.keys()))
    for key in all_keys:
        if key not in before:
            changes.append(f"created:{key}")
        elif key not in after:
            changes.append(f"deleted:{key}")
        elif before[key] != after[key]:
            changes.append(f"modified:{key}")
    return changes
