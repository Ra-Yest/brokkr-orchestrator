"""Local JSON file implementation of BaseTaskStore with atomic persistence and DAG validation."""

from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any, Literal

from orchestrator.fs_utils import atomic_write_json
from orchestrator.models import Task, TaskComment, TasksFile, TaskStatus, utc_now_iso
from orchestrator.stores.base import BaseTaskStore


class TaskStoreError(RuntimeError):
    """Raised when tasks.json violates structural or dependency DAG invariants."""


class LocalJsonTaskStore(BaseTaskStore):
    """Persists tasks in `.pipeline/tasks.json` using atomic writes with Windows retry."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: TasksFile = self._load()

    def _load(self) -> TasksFile:
        if not self.path.exists():
            raise FileNotFoundError(f"Tasks file not found: {self.path}")
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        data = TasksFile.model_validate(raw)
        self._validate_dag(data.tasks)
        return data

    def reload(self) -> None:
        self._data = self._load()

    def _save(self) -> None:
        self._validate_dag(self._data.tasks)
        atomic_write_json(self.path, self._data.model_dump(mode="json"))

    def _validate_dag(self, tasks: list[Task]) -> None:
        by_id: dict[str, Task] = {}
        for task in tasks:
            if task.id in by_id:
                raise TaskStoreError(f"Duplicate task ID in tasks.json: '{task.id}'")
            by_id[task.id] = task

        for task in tasks:
            for dep_id in task.depends_on:
                if dep_id not in by_id:
                    raise TaskStoreError(
                        f"Task '{task.id}' depends on unknown task ID '{dep_id}'"
                    )

        # Cycle detection (0=unvisited, 1=visiting, 2=visited)
        state: dict[str, int] = {tid: 0 for tid in by_id}

        def _dfs(tid: str) -> None:
            state[tid] = 1
            for dep in by_id[tid].depends_on:
                if state[dep] == 1:
                    raise TaskStoreError(
                        f"Dependency cycle detected involving '{tid}' -> '{dep}'"
                    )
                if state[dep] == 0:
                    _dfs(dep)
            state[tid] = 2

        for tid in by_id:
            if state[tid] == 0:
                _dfs(tid)

    def _index(self) -> dict[str, Task]:
        return {task.id: task for task in self._data.tasks}

    def list_tasks(self) -> list[Task]:
        return [task.model_copy(deep=True) for task in self._data.tasks]

    def get_task(self, task_id: str) -> Task:
        idx = self._index()
        if task_id not in idx:
            raise KeyError(f"Task not found: '{task_id}'")
        return idx[task_id].model_copy(deep=True)

    def get_next_available_task(self, *, exclude_ids: set[str] | None = None) -> Task | None:
        excluded = exclude_ids or set()
        idx = self._index()
        for task in self._data.tasks:
            if task.status != TaskStatus.TODO or task.id in excluded:
                continue
            if all(idx[dep].status == TaskStatus.DONE for dep in task.depends_on):
                return task.model_copy(deep=True)
        return None

    def update_status(self, task_id: str, status: TaskStatus, **fields: Any) -> Task:
        idx = self._index()
        if task_id not in idx:
            raise KeyError(f"Task not found: '{task_id}'")
        target = idx[task_id]
        updates = dict(fields)
        updates["status"] = status
        updates["updated_at"] = utc_now_iso()
        updated = target.model_copy(update=updates)
        self._replace_task(updated)
        self._save()
        return updated.model_copy(deep=True)

    def update_task_fields(self, task_id: str, **fields: Any) -> Task:
        idx = self._index()
        if task_id not in idx:
            raise KeyError(f"Task not found: '{task_id}'")
        target = idx[task_id]
        updates = dict(fields)
        updates["updated_at"] = utc_now_iso()
        updated = target.model_copy(update=updates)
        self._replace_task(updated)
        self._save()
        return updated.model_copy(deep=True)

    def add_subtasks(self, parent_id: str, subtasks: list[Task]) -> list[Task]:
        if not subtasks:
            raise TaskStoreError("add_subtasks requires at least one subtask")
        idx = self._index()
        if parent_id not in idx:
            raise KeyError(f"Parent task not found: '{parent_id}'")
        parent = idx[parent_id]

        existing_ids = set(idx.keys())
        new_ids = [st.id for st in subtasks]
        if len(set(new_ids)) != len(new_ids):
            raise TaskStoreError("Duplicate IDs inside subtasks list")
        overlap = existing_ids & set(new_ids)
        if overlap:
            raise TaskStoreError(f"Subtask ID(s) already exist: {sorted(overlap)}")

        # Inherit parent dependencies for root subtasks (those without internal dependencies)
        new_id_set = set(new_ids)
        normalized_subtasks: list[Task] = []
        for st in subtasks:
            deps = list(st.depends_on)
            internal_deps = [d for d in deps if d in new_id_set]
            if not internal_deps:
                for p_dep in parent.depends_on:
                    if p_dep not in deps:
                        deps.append(p_dep)
            normalized_subtasks.append(
                st.model_copy(
                    update={
                        "parent_task_id": parent_id,
                        "status": TaskStatus.TODO,
                        "depends_on": deps,
                        "stage": st.stage or parent.stage,
                        "updated_at": utc_now_iso(),
                    }
                )
            )

        # Find leaf subtasks (no other subtask in this group depends on them)
        depended_upon: set[str] = set()
        for st in normalized_subtasks:
            for dep in st.depends_on:
                if dep in new_id_set:
                    depended_upon.add(dep)
        leaf_ids = [st.id for st in normalized_subtasks if st.id not in depended_upon]
        if not leaf_ids:
            leaf_ids = [normalized_subtasks[-1].id]

        # Rewire any downstream task that depended on parent_id to depend on leaf_ids
        rewired_tasks: list[Task] = []
        for existing in self._data.tasks:
            if existing.id == parent_id:
                parent_updated = existing.model_copy(
                    update={
                        "status": TaskStatus.DONE,
                        "resolution": "split",
                        "subtask_ids": new_ids,
                        "updated_at": utc_now_iso(),
                    }
                )
                rewired_tasks.append(parent_updated)
                rewired_tasks.extend(normalized_subtasks)
            elif parent_id in existing.depends_on:
                new_deps: list[str] = []
                for dep in existing.depends_on:
                    if dep == parent_id:
                        for leaf_id in leaf_ids:
                            if leaf_id not in new_deps:
                                new_deps.append(leaf_id)
                    elif dep not in new_deps:
                        new_deps.append(dep)
                rewired_tasks.append(
                    existing.model_copy(
                        update={"depends_on": new_deps, "updated_at": utc_now_iso()}
                    )
                )
            else:
                rewired_tasks.append(existing)

        self._data = TasksFile(schema_version=self._data.schema_version, tasks=rewired_tasks)
        self._save()
        return [st.model_copy(deep=True) for st in normalized_subtasks]

    def add_comment(
        self,
        task_id: str,
        *,
        author: str,
        body: str,
        kind: Literal["note", "rejection", "escalation", "architect", "system", "control"] = "note",
    ) -> Task:
        idx = self._index()
        if task_id not in idx:
            raise KeyError(f"Task not found: '{task_id}'")
        target = idx[task_id]
        comments = list(target.comments)
        comments.append(TaskComment(author=author, kind=kind, body=body))
        updated = target.model_copy(update={"comments": comments, "updated_at": utc_now_iso()})
        self._replace_task(updated)
        self._save()
        return updated.model_copy(deep=True)

    def cascade_block_dependents(self, failed_task_id: str) -> list[str]:
        idx = self._index()
        dependents: dict[str, list[str]] = {tid: [] for tid in idx}
        for task in self._data.tasks:
            for dep in task.depends_on:
                dependents[dep].append(task.id)

        queue: deque[str] = deque([failed_task_id])
        visited: set[str] = {failed_task_id}
        newly_blocked: list[str] = []

        while queue:
            current = queue.popleft()
            for child_id in dependents.get(current, []):
                if child_id in visited:
                    continue
                visited.add(child_id)
                queue.append(child_id)
                child = idx[child_id]
                if child.status == TaskStatus.TODO:
                    comments = list(child.comments)
                    comments.append(
                        TaskComment(
                            author="orchestrator",
                            kind="system",
                            body=f"Automatically blocked because dependency #{failed_task_id} failed.",
                        )
                    )
                    updated = child.model_copy(
                        update={
                            "status": TaskStatus.BLOCKED,
                            "failure_reason": f"Blocked by failed dependency #{failed_task_id}",
                            "comments": comments,
                            "updated_at": utc_now_iso(),
                        }
                    )
                    idx[child_id] = updated
                    self._replace_task(updated)
                    newly_blocked.append(child_id)

        if newly_blocked:
            self._save()
        return newly_blocked

    def retry_task(self, task_id: str) -> list[str]:
        idx = self._index()
        if task_id not in idx:
            raise KeyError(f"Task not found: '{task_id}'")
        target = idx[task_id]
        comments = list(target.comments)
        comments.append(
            TaskComment(
                author="control_plane",
                kind="control",
                body="Task reset to TODO via retry_task command.",
            )
        )
        reset_task = target.model_copy(
            update={
                "status": TaskStatus.TODO,
                "attempts": 0,
                "review_rounds": 0,
                "pre_review_approved": False,
                "failure_reason": None,
                "wip_commit_sha": None,
                "changed_files": [],
                "touched_yellow_zone": False,
                "pending_progress_note": None,
                "resolution": None,
                "comments": comments,
                "updated_at": utc_now_iso(),
            }
        )
        idx[task_id] = reset_task
        self._replace_task(reset_task)

        # Iteratively unblock BLOCKED tasks whose dependencies are no longer FAILED or BLOCKED
        unblocked: list[str] = []
        changed = True
        while changed:
            changed = False
            for task in list(self._data.tasks):
                if task.status != TaskStatus.BLOCKED:
                    continue
                if all(
                    idx[dep].status not in (TaskStatus.FAILED, TaskStatus.BLOCKED)
                    for dep in task.depends_on
                ):
                    t_comments = list(task.comments)
                    t_comments.append(
                        TaskComment(
                            author="control_plane",
                            kind="control",
                            body=f"Unblocked after retry_task #{task_id}.",
                        )
                    )
                    unblocked_task = task.model_copy(
                        update={
                            "status": TaskStatus.TODO,
                            "failure_reason": None,
                            "comments": t_comments,
                            "updated_at": utc_now_iso(),
                        }
                    )
                    idx[task.id] = unblocked_task
                    self._replace_task(unblocked_task)
                    unblocked.append(task.id)
                    changed = True

        self._save()
        return unblocked

    def depends_on_any(self, task_id: str, target_ids: set[str]) -> bool:
        if not target_ids:
            return False
        idx = self._index()
        if task_id not in idx:
            return False
        queue: deque[str] = deque(idx[task_id].depends_on)
        visited: set[str] = set()
        while queue:
            dep = queue.popleft()
            if dep in target_ids:
                return True
            if dep in visited or dep not in idx:
                continue
            visited.add(dep)
            queue.extend(idx[dep].depends_on)
        return False

    def _replace_task(self, updated: Task) -> None:
        for i, existing in enumerate(self._data.tasks):
            if existing.id == updated.id:
                self._data.tasks[i] = updated
                return
        raise KeyError(f"Task not found during replace: '{updated.id}'")
