"""SQLite transactions are the authority for state, versions, and message receipts."""

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from control_plane.models import ProgressEvent, timestamp

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    status TEXT NOT NULL CHECK (status IN
        ('provisioning','active','updating','destroying','destroyed','failed')),
    version INTEGER NOT NULL CHECK (version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK (type IN ('deploy','update','destroy')),
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    status TEXT NOT NULL CHECK (status IN ('accepted','in_progress','done','failed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_tenant ON tasks(tenant_id, created_at, id);
CREATE TABLE IF NOT EXISTS outbox (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
    payload TEXT NOT NULL,
    published_at TEXT
);
CREATE TABLE IF NOT EXISTS inbox (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    status TEXT NOT NULL CHECK (status IN ('in_progress','done','failed')),
    disposition TEXT NOT NULL CHECK (disposition IN ('pending','applied','ignored')),
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_pending ON inbox(task_id, disposition, sequence);
"""


class DomainError(Exception):
    def __init__(self, code: str, message: str, status: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class PoisonMessage(ValueError):
    """A validly encoded message which cannot belong to this workflow."""


class Store:
    def __init__(self, path: str):
        self.path = path

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
        try:
            yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            # Take the write reservation BEFORE checking status/version. No check/write race.
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    @staticmethod
    def _get(conn: sqlite3.Connection, resource: str, resource_id: str) -> dict[str, Any]:
        # Table names are internal constants; all client-supplied values are parameters.
        query = (
            "SELECT * FROM tenants WHERE id = ?"
            if resource == "tenant"
            else ("SELECT * FROM tasks WHERE id = ?")
        )
        row = conn.execute(query, (resource_id,)).fetchone()
        if row is None:
            raise DomainError(f"{resource}_not_found", f"{resource.capitalize()} not found", 404)
        return dict(row)

    def get(self, resource: str, resource_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            return self._get(conn, resource, resource_id)

    def list_resources(
        self,
        resource: Literal["tenant", "task"],
        limit: int,
        offset: int,
        status: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        query = (
            "SELECT * FROM tenants WHERE 1=1"
            if resource == "tenant"
            else ("SELECT * FROM tasks WHERE 1=1")
        )
        values: list[Any] = []
        if status:
            query += " AND status = ?"
            values.append(status)
        if tenant_id and resource == "task":
            query += " AND tenant_id = ?"
            values.append(tenant_id)
        query += " ORDER BY created_at, id LIMIT ? OFFSET ?"
        with self.connection() as conn:
            rows = conn.execute(query, (*values, limit + 1, offset)).fetchall()
        return {
            "items": [dict(row) for row in rows[:limit]],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit if len(rows) > limit else None,
        }

    @staticmethod
    def _new_task(conn: sqlite3.Connection, tenant_id: str, task_type: str) -> dict[str, Any]:
        now = timestamp()
        task = {
            "id": str(uuid4()),
            "type": task_type,
            "tenant_id": tenant_id,
            "status": "accepted",
            "created_at": now,
            "updated_at": now,
        }
        conn.execute("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?)", tuple(task.values()))
        conn.execute(
            "INSERT INTO outbox(task_id, payload) VALUES (?, ?)",
            (task["id"], json.dumps({"schema_version": 1, **task})),
        )
        return task

    def create(self, slug: str, name: str) -> dict[str, Any]:
        with self.transaction() as conn:
            tenant_id, now = str(uuid4()), timestamp()
            try:
                conn.execute(
                    "INSERT INTO tenants VALUES (?, ?, ?, 'provisioning', 1, ?, ?)",
                    (tenant_id, slug, name, now, now),
                )
            except sqlite3.IntegrityError as exc:
                if conn.execute("SELECT 1 FROM tenants WHERE slug = ?", (slug,)).fetchone():
                    raise DomainError("tenant_already_exists", "Slug is already reserved") from exc
                raise
            task = self._new_task(conn, tenant_id, "deploy")
            return {"tenant": self._get(conn, "tenant", tenant_id), "task": task}

    def mutate(
        self,
        tenant_id: str,
        operation: Literal["update", "destroy"],
        version: int | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as conn:
            tenant = self._get(conn, "tenant", tenant_id)
            if operation == "update" and version != tenant["version"]:
                raise DomainError(
                    "tenant_version_conflict", "Version does not match current tenant"
                )
            allowed = {"active"} if operation == "update" else {"active", "failed"}
            if tenant["status"] not in allowed:
                raise DomainError(
                    "tenant_update_not_allowed",
                    f"Cannot {operation} tenant in status {tenant['status']}",
                )
            conn.execute(
                "UPDATE tenants SET status = ?, name = ?, version = version + 1, "
                "updated_at = ? WHERE id = ?",
                (
                    "updating" if operation == "update" else "destroying",
                    name if name is not None else tenant["name"],
                    timestamp(),
                    tenant_id,
                ),
            )
            task = self._new_task(conn, tenant_id, operation)
            return {"tenant": self._get(conn, "tenant", tenant_id), "task": task}

    def pending_event(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT task_id, payload FROM outbox WHERE published_at IS NULL "
                "ORDER BY rowid LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    def mark_published(self, task_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET published_at = ? WHERE task_id = ?", (timestamp(), task_id)
            )

    def apply_progress(self, event: ProgressEvent) -> str:
        event_id, task_id = str(event.id), str(event.task_id)
        with self.transaction() as conn:
            receipt = conn.execute("SELECT * FROM inbox WHERE event_id = ?", (event_id,)).fetchone()
            if receipt:
                if receipt["task_id"] != task_id or receipt["status"] != event.status:
                    raise PoisonMessage("Event id reused for different contents")
                return "duplicate"
            try:
                task = self._get(conn, "task", task_id)
            except DomainError as exc:
                raise PoisonMessage("Unknown task") from exc
            disposition = "ignored" if task["status"] in {"done", "failed"} else "pending"
            conn.execute(
                "INSERT INTO inbox(event_id, task_id, status, disposition, received_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_id, task_id, event.status, disposition, timestamp()),
            )
            if disposition == "ignored":
                return disposition
            if event.status == "in_progress":
                if task["status"] == "accepted":
                    conn.execute(
                        "UPDATE tasks SET status = 'in_progress', updated_at = ? WHERE id = ?",
                        (timestamp(), task_id),
                    )
                conn.execute(
                    "UPDATE inbox SET disposition = 'applied' WHERE event_id = ?", (event_id,)
                )
            # A terminal message that overtakes in_progress waits durably in the inbox.
            # This preserves the exact state machine and survives restarts/acknowledgements.
            task = self._get(conn, "task", task_id)
            terminal = conn.execute(
                "SELECT * FROM inbox WHERE task_id = ? AND disposition = 'pending' "
                "AND status IN ('done', 'failed') ORDER BY sequence LIMIT 1",
                (task_id,),
            ).fetchone()
            if task["status"] == "in_progress" and terminal:
                now = timestamp()
                tenant_status = (
                    "failed"
                    if terminal["status"] == "failed"
                    else ("destroyed" if task["type"] == "destroy" else "active")
                )
                conn.execute(
                    "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
                    (terminal["status"], now, task_id),
                )
                conn.execute(
                    "UPDATE tenants SET status = ?, version = version + 1, "
                    "updated_at = ? WHERE id = ?",
                    (tenant_status, now, task["tenant_id"]),
                )
                conn.execute(
                    "UPDATE inbox SET disposition = CASE WHEN event_id = ? THEN 'applied' "
                    "ELSE 'ignored' END WHERE task_id = ? AND disposition = 'pending'",
                    (terminal["event_id"], task_id),
                )
            return conn.execute(
                "SELECT disposition FROM inbox WHERE event_id = ?", (event_id,)
            ).fetchone()["disposition"]
