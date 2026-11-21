"""Abstract BaseTaskStore interface (ready for GitHubIssuesTaskStore in team mode)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Literal

from orchestrator.models import Task, TaskStatus


class BaseTaskStore(ABC):
    """Storage abstraction for tasks, dependencies, comments, and subtask decomposition."""

    @abstractmethod
    def list_tasks(self) -> list[Task]:
        """Return all tasks in deterministic order."""

    @abstractmethod
    def get_task(self, task_id: str) -> Task:
        """Return a task by ID or raise KeyError."""

    @abstractmethod
    def get_next_available_task(self, *, exclude_ids: set[str] | None = None) -> Task | None:
        """Return the next TODO task whose dependencies are all DONE."""

    @abstractmethod
    def update_status(self, task_id: str, status: TaskStatus, **fields: Any) -> Task:
        """Transition a task to a new status and atomically update provided fields."""

    @abstractmethod
    def update_task_fields(self, task_id: str, **fields: Any) -> Task:
        """Update task metadata fields without changing status."""

    @abstractmethod
    def add_subtasks(self, parent_id: str, subtasks: list[Task]) -> list[Task]:
        """Decompose parent_id into atomic subtasks, rewire downstream depends_on, mark parent split."""

    @abstractmethod
    def add_comment(
        self,
        task_id: str,
        *,
        author: str,
        body: str,
        kind: Literal["note", "rejection", "escalation", "architect", "system", "control"] = "note",
    ) -> Task:
        """Append a structured comment/rejection reason/escalation note to a task."""

    @abstractmethod
    def cascade_block_dependents(self, failed_task_id: str) -> list[str]:
        """Mark all transitively dependent TODO tasks as BLOCKED. Returns newly blocked IDs."""

    @abstractmethod
    def retry_task(self, task_id: str) -> list[str]:
        """Reset a FAILED/BLOCKED task to TODO and unblock newly eligible downstream tasks."""

    @abstractmethod
    def depends_on_any(self, task_id: str, target_ids: set[str]) -> bool:
        """Return True if task_id directly or transitively depends on any ID in target_ids."""
