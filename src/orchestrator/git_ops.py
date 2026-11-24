"""Git operations with base_sha protection, untracked intent-to-add, revert, and safe rollback."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from orchestrator.process_utils import ProcessRunResult, run_noninteractive_process


class GitOperationError(RuntimeError):
    """Raised when a required git command fails."""


@dataclass(frozen=True)
class GitChange:
    status: str  # A, M, D, R, C, T, U
    path: str
    old_path: str | None = None


class GitOps:
    """Safe, non-interactive git wrapper scoped to a target repository."""

    def __init__(self, repo_root: Path, pipeline_dir: str = ".pipeline") -> None:
        self.repo_root = repo_root.resolve()
        self.pipeline_dir = pipeline_dir.strip("/\\")

    def _run(self, args: list[str], *, check: bool = True, timeout: int = 120) -> ProcessRunResult:
        result = run_noninteractive_process(
            ["git", *args],
            cwd=self.repo_root,
            env={"GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"},
            timeout_seconds=timeout,
        )
        if check and result.exit_code != 0:
            raise GitOperationError(
                f"git {' '.join(args)} failed (exit={result.exit_code}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        return result

    def is_git_repo(self) -> bool:
        res = self._run(["rev-parse", "--is-inside-work-tree"], check=False)
        return res.exit_code == 0 and res.stdout.strip().lower() == "true"

    def get_head_sha(self) -> str:
        res = self._run(["rev-parse", "HEAD"])
        return res.stdout.strip()

    def commit_exists(self, sha: str) -> bool:
        if not sha or not sha.strip():
            return False
        res = self._run(["rev-parse", "--verify", "--quiet", f"{sha}^{{commit}}"], check=False)
        return res.exit_code == 0

    def get_current_branch(self) -> str:
        res = self._run(["rev-parse", "--abbrev-ref", "HEAD"])
        return res.stdout.strip()

    def _is_in_pipeline_dir(self, rel_path: str) -> bool:
        norm = rel_path.replace("\\", "/").lstrip("./")
        prefix = self.pipeline_dir.replace("\\", "/").strip("/")
        return norm == prefix or norm.startswith(prefix + "/")

    def list_untracked_files(self, *, exclude_pipeline_dir: bool = True) -> list[str]:
        """List untracked files outside .gitignore (and outside pipeline_dir by default)."""
        res = self._run(["status", "--porcelain=v1", "-z", "-uall"])
        entries = res.stdout.split("\0")
        untracked: list[str] = []
        idx = 0
        while idx < len(entries):
            entry = entries[idx]
            if not entry:
                idx += 1
                continue
            status_code = entry[:2]
            path = entry[3:]
            if status_code[0] in ("R", "C") or status_code[1] in ("R", "C"):
                idx += 2
                continue
            idx += 1
            if status_code == "??":
                norm_path = path.replace("\\", "/")
                if exclude_pipeline_dir and self._is_in_pipeline_dir(norm_path):
                    continue
                untracked.append(norm_path)
        return sorted(untracked)

    def has_uncommitted_changes(self, *, exclude_pipeline_dir: bool = True) -> bool:
        """Return True if working tree or index has changes outside pipeline_dir."""
        res = self._run(["status", "--porcelain=v1", "-z", "-uall"])
        entries = res.stdout.split("\0")
        idx = 0
        while idx < len(entries):
            entry = entries[idx]
            if not entry:
                idx += 1
                continue
            status_code = entry[:2]
            path = entry[3:].replace("\\", "/")
            if status_code[0] in ("R", "C") or status_code[1] in ("R", "C"):
                idx += 2
            else:
                idx += 1
            if exclude_pipeline_dir and self._is_in_pipeline_dir(path):
                continue
            return True
        return False

    def restore_soft_to_base(self, base_sha: str) -> bool:
        """If an agent created unauthorized commits (HEAD != base_sha), soft-reset back to base_sha.

        Returns True if a soft reset was performed.
        """
        current_sha = self.get_head_sha()
        if current_sha == base_sha:
            return False
        self._run(["reset", "--soft", base_sha])
        return True

    def mark_untracked_intent_to_add(self, *, exclude_pipeline_dir: bool = True) -> list[str]:
        """Run `git add -N` on untracked files so git diff / --name-status include them."""
        untracked = self.list_untracked_files(exclude_pipeline_dir=exclude_pipeline_dir)
        if not untracked:
            return []
        batch_size = 200
        for start in range(0, len(untracked), batch_size):
            batch = untracked[start : start + batch_size]
            self._run(["add", "-N", "--", *batch])
        return untracked

    def get_name_status(self, base_sha: str) -> list[GitChange]:
        """Return all changes relative to base_sha (call mark_untracked_intent_to_add first)."""
        res = self._run(["diff", "--name-status", "-z", base_sha])
        tokens = res.stdout.split("\0")
        changes: list[GitChange] = []
        idx = 0
        while idx < len(tokens):
            raw_status = tokens[idx]
            if not raw_status:
                idx += 1
                continue
            code = raw_status[0].upper()
            if code in ("R", "C"):
                if idx + 2 >= len(tokens):
                    break
                old_path = tokens[idx + 1].replace("\\", "/")
                new_path = tokens[idx + 2].replace("\\", "/")
                changes.append(GitChange(status=code, path=new_path, old_path=old_path))
                idx += 3
            else:
                if idx + 1 >= len(tokens):
                    break
                path = tokens[idx + 1].replace("\\", "/")
                changes.append(GitChange(status=code, path=path))
                idx += 2
        return changes

    def get_diff(self, base_sha: str, paths: list[str] | None = None) -> str:
        """Return unified diff relative to base_sha (includes intent-to-add files)."""
        args = ["diff", "--no-ext-diff", "--no-color", base_sha]
        if paths:
            args.extend(["--", *paths])
        res = self._run(args)
        return res.stdout

    def get_diff_stat(self, base_sha: str, paths: list[str] | None = None) -> str:
        """Return full `git diff --stat` relative to base_sha (includes intent-to-add files)."""
        args = ["diff", "--stat", "--no-ext-diff", "--no-color", base_sha]
        if paths:
            args.extend(["--", *paths])
        res = self._run(args)
        return res.stdout

    def get_commit_diff_stat(self, commit_sha: str) -> str:
        """Return `git show --stat --format=` for a single commit."""
        res = self._run(
            ["show", "--stat", "--format=", "--no-ext-diff", "--no-color", commit_sha],
            check=False,
        )
        return res.stdout if res.exit_code == 0 else ""

    def commit_all(self, message: str, *, exclude_pipeline_dir: bool = True) -> str:
        """Stage all non-ignored repository changes (excluding pipeline_dir) and commit."""
        self._run(["add", "-A"])
        if exclude_pipeline_dir:
            # Ensure nothing inside pipeline_dir is staged even if .gitignore omitted it
            self._run(["reset", "HEAD", "--", self.pipeline_dir], check=False)

        staged_check = self._run(["diff", "--cached", "--quiet"], check=False)
        if staged_check.exit_code == 0:
            raise GitOperationError("Nothing staged to commit")

        self._run(["commit", "--no-verify", "-m", message])
        return self.get_head_sha()

    def revert_commit(self, commit_sha: str) -> str:
        """Revert a single commit cleanly (used for rejecting a single task from a batch)."""
        res = self._run(["revert", "--no-edit", commit_sha], check=False)
        if res.exit_code != 0:
            self._run(["revert", "--abort"], check=False)
            raise GitOperationError(
                f"Failed to cleanly revert commit {commit_sha}: "
                f"{res.stderr.strip() or res.stdout.strip()}"
            )
        return self.get_head_sha()

    def hard_rollback(self, base_sha: str) -> None:
        """Reset repository to base_sha and clean untracked files while preserving pipeline_dir."""
        self._run(["reset", "--hard", base_sha])
        self._run(
            [
                "clean",
                "-fd",
                "-e",
                f"{self.pipeline_dir}/",
                "-e",
                self.pipeline_dir,
            ]
        )

    def discard_paths(self, base_sha: str, paths: list[str]) -> list[str]:
        """Revert/delete ONLY the specified relative paths back to base_sha without touching other edits."""
        discarded: list[str] = []
        seen: set[str] = set()
        for raw_path in paths:
            norm = raw_path.replace("\\", "/").lstrip("./")
            if not norm or norm in seen or self._is_in_pipeline_dir(norm):
                continue
            seen.add(norm)
            exists_in_base = (
                self._run(["cat-file", "-e", f"{base_sha}:{norm}"], check=False).exit_code == 0
            )
            if exists_in_base:
                self._run(["checkout", base_sha, "--", norm], check=False)
            else:
                self._run(["reset", "HEAD", "--", norm], check=False)
                target_file = self.repo_root / norm
                if target_file.is_file() or target_file.is_symlink():
                    target_file.unlink(missing_ok=True)
            discarded.append(norm)
        return discarded

    def is_pipeline_dir_ignored(self) -> bool:
        res = self._run(["check-ignore", "-q", self.pipeline_dir], check=False)
        return res.exit_code == 0
