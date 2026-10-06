"""SQL storage for the Tasks capability: the run and step tables.

Port of upstream ``tasks/store.ts``. The engine in ``tasks.py`` holds the
state machine; every statement that touches SQLite is here or in
``engine.py``. Fenced writes use ``RETURNING`` to learn whether the fence
let them through (``.design/sql_schemas.md`` §5).
"""

import json
from collections.abc import Sequence

from ..core.sql import Sql
from ..core.timing import from_epoch_ms
from .serialization import deserialize_task_value
from .types import (
    CancelledRun,
    CompletedRun,
    FailedRun,
    PendingRun,
    RunningRun,
    TaskError,
    TaskRun,
    TaskRunRow,
    TaskRunState,
    WaitingRun,
)

__all__ = ("TaskStore",)

_RUNS = """CREATE TABLE IF NOT EXISTS cf_agents_task_runs (
  run_id TEXT PRIMARY KEY,
  definition TEXT NOT NULL,
  input TEXT,
  state TEXT NOT NULL CHECK (state IN (
    'pending', 'running', 'waiting', 'completed', 'failed', 'cancelled'
  )),
  result TEXT,
  error_name TEXT,
  error_message TEXT,
  status_message TEXT,
  metadata TEXT,
  idempotency_key TEXT UNIQUE,
  retain INTEGER NOT NULL DEFAULT 1,
  attempt INTEGER NOT NULL DEFAULT 0,
  generation TEXT,
  next_at INTEGER,
  wait_reason TEXT,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  cancel_reason TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  updated_at INTEGER NOT NULL,
  settled_at INTEGER
) WITHOUT ROWID"""

# No (state, next_at) index: every claim, refresh, and settle rewrites
# next_at, and each touched index is a billed row write (upstream).
_RUNS_BY_DEFINITION = """CREATE INDEX IF NOT EXISTS cf_agents_task_runs_definition
  ON cf_agents_task_runs (definition, created_at)"""

_STEPS = """CREATE TABLE IF NOT EXISTS cf_agents_task_steps (
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('do', 'sleep')),
  state TEXT NOT NULL CHECK (state IN ('running', 'waiting', 'completed', 'failed')),
  result TEXT,
  error_name TEXT,
  error_message TEXT,
  attempt INTEGER NOT NULL DEFAULT 0,
  next_at INTEGER,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER,
  PRIMARY KEY (run_id, step_name)
) WITHOUT ROWID"""


class TaskStore:
    """Row access for one Tasks capability.

    Parameters
    ----------
    sql
        The object's typed SQL helper.
    """

    __slots__ = ("sql",)

    def __init__(self, sql: Sql) -> None:
        self.sql = sql

    def ensure_tables(self) -> None:
        """Create the run and step tables (idempotent)."""
        self.sql(_RUNS)
        self.sql(_RUNS_BY_DEFINITION)
        self.sql(_STEPS)

    def get_run(self, run_id: str) -> TaskRunRow | None:
        """Return one run row, or ``None``."""
        rows = self.sql(
            "SELECT * FROM cf_agents_task_runs WHERE run_id = ?", run_id, row=TaskRunRow
        )
        return rows[0] if rows else None

    def get_run_by_key(self, idempotency_key: str) -> TaskRunRow | None:
        """Return the run with ``idempotency_key``, or ``None``."""
        rows = self.sql(
            "SELECT * FROM cf_agents_task_runs WHERE idempotency_key = ?",
            idempotency_key,
            row=TaskRunRow,
        )
        return rows[0] if rows else None

    def next_at(self, run_id: str) -> int | None:
        """Return a live run's deadline (epoch ms), or ``None`` once it's settled."""
        rows = self.sql(
            """SELECT next_at FROM cf_agents_task_runs
               WHERE run_id = ? AND state IN ('pending', 'waiting', 'running')""",
            run_id,
        )
        return rows[0]["next_at"] if rows else None

    def delete_run(self, run_id: str) -> None:
        """Delete a run and its step journal."""
        self.sql("DELETE FROM cf_agents_task_steps WHERE run_id = ?", run_id)
        self.sql("DELETE FROM cf_agents_task_runs WHERE run_id = ?", run_id)

    def list_runs(
        self,
        definition: str | None,
        states: Sequence[TaskRunState],
        limit: int,
    ) -> Sequence[TaskRunRow]:
        """Return runs newest first, optionally by definition and states."""
        return self.sql(
            """SELECT * FROM cf_agents_task_runs
               WHERE (? IS NULL OR definition = ?)
                 AND (? = 0 OR state IN (SELECT value FROM json_each(?)))
               ORDER BY created_at DESC, run_id DESC
               LIMIT ?""",
            definition,
            definition,
            len(states),
            json.dumps(list(states)),
            limit,
            row=TaskRunRow,
        )

    def settled_runs(
        self, states: Sequence[TaskRunState], settled_before_ms: int | None, limit: int
    ) -> Sequence[TaskRunRow]:
        """Return settled runs in ``states``, oldest settled first."""
        return self.sql(
            """SELECT * FROM cf_agents_task_runs
               WHERE state IN (SELECT value FROM json_each(?))
                 AND (? IS NULL OR settled_at < ?)
               ORDER BY settled_at ASC
               LIMIT ?""",
            json.dumps(list(states)),
            settled_before_ms,
            settled_before_ms,
            limit,
            row=TaskRunRow,
        )

    def live_runs_with_deadlines(self) -> Sequence[str]:
        """Return the ids of non-terminal runs that have a deadline."""
        rows = self.sql(
            """SELECT run_id FROM cf_agents_task_runs
               WHERE state IN ('pending', 'waiting', 'running')
                 AND next_at IS NOT NULL"""
        )
        return [row["run_id"] for row in rows]


def row_to_run(row: TaskRunRow) -> TaskRun[object]:
    """Return the snapshot of a run row (one class per state)."""
    metadata = json.loads(row["metadata"]) if row["metadata"] is not None else None
    created_at = from_epoch_ms(row["created_at"])
    settled_at = from_epoch_ms(row["settled_at"] or row["updated_at"])
    common = {
        "run_id": row["run_id"],
        "definition": row["definition"],
        "created_at": created_at,
        "metadata": metadata,
    }
    match row["state"]:
        case "pending":
            return PendingRun(**common)
        case "running":
            return RunningRun(
                **common,
                attempt=row["attempt"],
                started_at=from_epoch_ms(row["started_at"] or row["created_at"]),
                status_message=row["status_message"],
            )
        case "waiting":
            return WaitingRun(
                **common,
                reason=row["wait_reason"] or "sleep",
                wake_at=from_epoch_ms(row["next_at"] or row["updated_at"]),
                status_message=row["status_message"],
            )
        case "completed":
            return CompletedRun(
                **common,
                result=deserialize_task_value(row["result"]),
                settled_at=settled_at,
            )
        case "failed":
            return FailedRun(
                **common,
                error=TaskError(
                    name=row["error_name"] or "Error",
                    message=row["error_message"] or "Task run failed",
                ),
                settled_at=settled_at,
            )
        case "cancelled":
            return CancelledRun(
                **common, reason=row["cancel_reason"], settled_at=settled_at
            )
