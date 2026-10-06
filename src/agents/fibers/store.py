"""SQL storage for fibers: run rows, the managed ledger, and the facet index.

Port of the fiber statements in upstream ``index.ts`` and the facet-run index
in ``dynamic-agents.ts`` (``.design/sql_schemas.md`` §11.2, §12).
"""

import json
from collections.abc import Sequence
from typing import Any

from ..core.sql import Sql
from ..core.timing import from_epoch_ms, now_ms
from .types import FiberInspection, FiberLedgerRow, FiberRunRow, FiberStatus

__all__ = ("FiberStore", "inspection_from_row")

_RUNS = """CREATE TABLE IF NOT EXISTS cf_agents_runs (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  snapshot TEXT,
  created_at INTEGER NOT NULL,
  completed_at INTEGER,
  outcome TEXT,
  error_message TEXT
)"""

_FIBERS = """CREATE TABLE IF NOT EXISTS cf_agents_fibers (
  fiber_id TEXT PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  snapshot TEXT,
  metadata_json TEXT,
  error_message TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  completed_at INTEGER
)"""
_FIBERS_BY_STATUS = """CREATE INDEX IF NOT EXISTS idx_fibers_status_created
  ON cf_agents_fibers(status, created_at, fiber_id)"""
_FIBERS_BY_NAME = """CREATE INDEX IF NOT EXISTS idx_fibers_name_status_created
  ON cf_agents_fibers(name, status, created_at, fiber_id)"""
_FIBERS_BY_COMPLETION = """CREATE INDEX IF NOT EXISTS idx_fibers_status_completed
  ON cf_agents_fibers(status, completed_at, created_at)"""

_FACET_RUNS = """CREATE TABLE IF NOT EXISTS cf_agents_facet_runs (
  owner_path TEXT NOT NULL,
  owner_path_key TEXT NOT NULL,
  run_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (owner_path_key, run_id)
)"""
_FACET_RUNS_BY_OWNER = """CREATE INDEX IF NOT EXISTS idx_facet_runs_owner_path_key
  ON cf_agents_facet_runs(owner_path_key)"""

_LEDGER_COLUMNS = """fiber_id, idempotency_key, name, status, snapshot, metadata_json,
  error_message, created_at, started_at, completed_at"""

LIVE: tuple[FiberStatus, ...] = ("pending", "running")
TERMINAL: tuple[FiberStatus, ...] = ("completed", "aborted", "interrupted", "error")


class FiberStore:
    """Every SQL statement the fiber engine runs."""

    __slots__ = ("sql",)

    def __init__(self, sql: Sql) -> None:
        self.sql = sql

    def ensure_tables(self) -> None:
        """Create the three tables and their indexes (idempotent)."""
        for statement in (
            _RUNS,
            _FIBERS,
            _FIBERS_BY_STATUS,
            _FIBERS_BY_NAME,
            _FIBERS_BY_COMPLETION,
            _FACET_RUNS,
            _FACET_RUNS_BY_OWNER,
        ):
            self.sql(statement)

    # Run rows (one per fiber executing, or left by a dead isolate)

    def insert_run(self, id: str, name: str) -> None:
        """Insert the row of a fiber starting now."""
        self.sql(
            "INSERT INTO cf_agents_runs (id, name, snapshot, created_at)"
            " VALUES (?, ?, NULL, ?)",
            id,
            name,
            now_ms(),
        )

    def write_snapshot(self, id: str, snapshot: str, *, managed: bool) -> None:
        """Save a checkpoint (on the ledger record too, for a managed fiber)."""
        self.sql("UPDATE cf_agents_runs SET snapshot = ? WHERE id = ?", snapshot, id)
        if managed:
            self.sql(
                "UPDATE cf_agents_fibers SET snapshot = ? WHERE fiber_id = ?",
                snapshot,
                id,
            )

    def record_outcome(self, id: str, outcome: str, error: str | None) -> None:
        """Record how a fiber's body ended, before its row is deleted."""
        self.sql(
            "UPDATE cf_agents_runs SET completed_at = ?, outcome = ?,"
            " error_message = ? WHERE id = ?",
            now_ms(),
            outcome,
            error,
            id,
        )

    def delete_run(self, id: str) -> None:
        """Delete a fiber's run row."""
        self.sql("DELETE FROM cf_agents_runs WHERE id = ?", id)

    def runs(self) -> Sequence[FiberRunRow]:
        """Return every run row."""
        return self.sql(
            "SELECT id, name, snapshot, created_at, completed_at, outcome,"
            " error_message FROM cf_agents_runs",
            row=FiberRunRow,
        )

    def run_ids(self) -> Sequence[str]:
        """Return the ids of every run row."""
        return [row["id"] for row in self.sql("SELECT id FROM cf_agents_runs")]

    def count_runs(self) -> int:
        """Return how many run rows there are."""
        return self.sql("SELECT COUNT(*) AS count FROM cf_agents_runs")[0]["count"]

    # The managed-fiber ledger

    def get(self, fiber_id: str) -> FiberLedgerRow | None:
        """Return a ledger record by id."""
        rows = self.sql(
            "SELECT " + _LEDGER_COLUMNS + " FROM cf_agents_fibers"
            " WHERE fiber_id = ? LIMIT 1",
            fiber_id,
            row=FiberLedgerRow,
        )
        return rows[0] if rows else None

    def get_by_key(self, idempotency_key: str) -> FiberLedgerRow | None:
        """Return a ledger record by idempotency key."""
        rows = self.sql(
            "SELECT " + _LEDGER_COLUMNS + " FROM cf_agents_fibers"
            " WHERE idempotency_key = ? LIMIT 1",
            idempotency_key,
            row=FiberLedgerRow,
        )
        return rows[0] if rows else None

    def insert_pending(
        self,
        fiber_id: str,
        name: str,
        idempotency_key: str | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        """Insert a ``pending`` ledger record."""
        self.sql(
            "INSERT INTO cf_agents_fibers (fiber_id, idempotency_key, name, status,"
            " metadata_json, created_at) VALUES (?, ?, ?, 'pending', ?, ?)",
            fiber_id,
            idempotency_key,
            name,
            json.dumps(metadata) if metadata is not None else None,
            now_ms(),
        )

    def mark_running(self, fiber_id: str) -> bool:
        """Move ``pending`` to ``running``; ``False`` if it wasn't pending."""
        return bool(
            self.sql(
                "UPDATE cf_agents_fibers SET status = 'running', started_at = ?"
                " WHERE fiber_id = ? AND status = 'pending' RETURNING fiber_id",
                now_ms(),
                fiber_id,
            )
        )

    def settle(
        self,
        fiber_id: str,
        status: FiberStatus,
        error: str | None,
        *,
        when: tuple[FiberStatus, ...],
        completed_at: int | None = None,
        snapshot: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Set a terminal (or ``interrupted``) status if the record is in ``when``.

        ``snapshot`` and ``metadata`` replace the stored ones when given.
        """
        self.sql(
            "UPDATE cf_agents_fibers SET status = ?, error_message = ?,"
            " completed_at = ?, snapshot = COALESCE(?, snapshot),"
            " metadata_json = COALESCE(?, metadata_json)"
            " WHERE fiber_id = ? AND status IN (SELECT value FROM json_each(?))",
            status,
            error,
            completed_at if completed_at is not None else now_ms(),
            snapshot,
            json.dumps(metadata) if metadata is not None else None,
            fiber_id,
            json.dumps(when),
        )

    def interrupt(self, fiber_id: str, snapshot: str | None) -> None:
        """Mark a live record ``interrupted``, keeping the run's last snapshot."""
        self.settle(fiber_id, "interrupted", None, when=LIVE, snapshot=snapshot)

    def orphaned_records(self) -> Sequence[FiberLedgerRow]:
        """Live records with no run row: their isolate died before running."""
        return self.sql(
            "SELECT f.fiber_id, f.idempotency_key, f.name, f.status, f.snapshot,"
            " f.metadata_json, f.error_message, f.created_at, f.started_at,"
            " f.completed_at FROM cf_agents_fibers f"
            " LEFT JOIN cf_agents_runs r ON r.id = f.fiber_id"
            " WHERE f.status IN ('pending', 'running') AND r.id IS NULL",
            row=FiberLedgerRow,
        )

    def list(
        self, statuses: Sequence[FiberStatus] | None, name: str | None, limit: int
    ) -> Sequence[FiberLedgerRow]:
        """Return ledger records, newest first, optionally filtered."""
        return self.sql(
            "SELECT " + _LEDGER_COLUMNS + " FROM cf_agents_fibers"
            " WHERE (? IS NULL OR status IN (SELECT value FROM json_each(?)))"
            " AND (? IS NULL OR name = ?)"
            " ORDER BY created_at DESC, fiber_id DESC LIMIT ?",
            *_json_filter(statuses),
            name,
            name,
            limit,
            row=FiberLedgerRow,
        )

    def delete_settled(
        self,
        statuses: Sequence[FiberStatus],
        settled_before_ms: int | None,
        limit: int,
    ) -> int:
        """Delete up to ``limit`` terminal records, oldest settled first."""
        rows = self.sql(
            "SELECT fiber_id FROM cf_agents_fibers"
            " WHERE status IN (SELECT value FROM json_each(?))"
            " AND (? IS NULL OR (completed_at IS NOT NULL AND completed_at < ?))"
            " ORDER BY completed_at ASC, created_at ASC LIMIT ?",
            json.dumps(list(statuses)),
            settled_before_ms,
            settled_before_ms,
            limit,
        )
        for row in rows:
            self.sql(
                "DELETE FROM cf_agents_fibers WHERE fiber_id = ? AND status IN"
                " ('completed', 'aborted', 'interrupted', 'error')",
                row["fiber_id"],
            )
        return len(rows)

    # The root's index of fibers running in facets

    def register_facet_run(self, owner_key: str, owner_data: str, run_id: str) -> None:
        """Index a facet's running fiber on the root."""
        self.sql(
            "INSERT OR REPLACE INTO cf_agents_facet_runs"
            " (owner_path, owner_path_key, run_id, created_at) VALUES (?, ?, ?, ?)",
            owner_data,
            owner_key,
            run_id,
            now_ms(),
        )

    def unregister_facet_run(self, owner_key: str, run_id: str) -> None:
        """Remove one facet fiber from the root's index."""
        self.sql(
            "DELETE FROM cf_agents_facet_runs WHERE owner_path_key = ? AND run_id = ?",
            owner_key,
            run_id,
        )

    def facet_owners(self) -> Sequence[tuple[str, str]]:
        """Each indexed facet once, as ``(key, data)``, oldest run first."""
        rows = self.sql(
            "SELECT owner_path_key, owner_path, MIN(created_at) AS first"
            " FROM cf_agents_facet_runs GROUP BY owner_path_key ORDER BY first"
        )
        return [(row["owner_path_key"], row["owner_path"]) for row in rows]

    def forget_facet(self, owner_key: str) -> None:
        """Remove every index entry of one facet."""
        self.sql("DELETE FROM cf_agents_facet_runs WHERE owner_path_key = ?", owner_key)

    def forget_facets_under(self, prefix: str) -> None:
        """Remove the index entries of a facet subtree."""
        self.sql(
            "DELETE FROM cf_agents_facet_runs"
            " WHERE owner_path_key = ? OR substr(owner_path_key, 1, ?) = ?",
            prefix,
            len(prefix) + 1,
            prefix + "/",
        )

    def has_facet_runs(self) -> bool:
        """Return whether any facet fiber is indexed."""
        return bool(self.sql("SELECT 1 FROM cf_agents_facet_runs LIMIT 1"))


def inspection_from_row(row: FiberLedgerRow) -> FiberInspection:
    """Build the public view of a ledger row."""
    return FiberInspection(
        fiber_id=row["fiber_id"],
        name=row["name"],
        status=row["status"],
        created_at=from_epoch_ms(row["created_at"]),
        idempotency_key=row["idempotency_key"],
        snapshot=json.loads(row["snapshot"]) if row["snapshot"] is not None else None,
        error=row["error_message"],
        metadata=_json_object(row["metadata_json"]),
        started_at=_moment(row["started_at"]),
        settled_at=_moment(row["completed_at"]),
    )


def _json_filter(
    statuses: Sequence[FiberStatus] | None,
) -> tuple[str | None, str | None]:
    encoded = json.dumps(list(statuses)) if statuses is not None else None
    return encoded, encoded


def _json_object(text: str | None) -> dict[str, Any] | None:
    if text is None:
        return None
    value = json.loads(text)
    return value if isinstance(value, dict) else None


def _moment(ms: int | None) -> Any:
    return from_epoch_ms(ms) if ms is not None else None
