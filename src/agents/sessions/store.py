"""SQL storage for Sessions: message rows and their continuation slices.

Port of the phase-1 statements in upstream ``sessions/core.ts``
(``.design/sessions_engine.md``, ``.design/sql_schemas.md`` §7). Both tables
are ``WITHOUT ROWID`` with no secondary indexes: on DO SQLite a row write
costs far more than a read, and every index would charge every append.
"""

import json
from collections.abc import Sequence
from typing import Any, NamedTuple

from ..core.sql import Sql
from ..core.timing import now_ms

__all__ = ("MAX_PATH_DEPTH", "PathRow", "SessionStore")

MAX_PATH_DEPTH = 10_000
"""The most rows a path walk visits; older rows read as truncated (the
queries below spell it out, as SQL text must be literal)."""

_MESSAGES = """CREATE TABLE IF NOT EXISTS cf_agents_session_messages (
  session_id TEXT NOT NULL,
  id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  parent_id TEXT,
  type TEXT NOT NULL DEFAULT 'message',
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  content_chunks INTEGER NOT NULL DEFAULT 0,
  token_estimate INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  content_hash TEXT,
  PRIMARY KEY (session_id, id)
) WITHOUT ROWID"""
_CHUNKS = """CREATE TABLE IF NOT EXISTS cf_agents_session_message_chunks (
  session_id TEXT NOT NULL,
  id TEXT NOT NULL,
  idx INTEGER NOT NULL,
  content TEXT NOT NULL,
  PRIMARY KEY (session_id, id, idx)
) WITHOUT ROWID"""

# The path from a leaf to the root, oldest first, with each row's stored size
# (row plus continuations, in bytes). Parameters: session, leaf, session,
# session.
_PATH = """WITH RECURSIVE path(id, parent_id, depth) AS (
  SELECT id, parent_id, 0 FROM cf_agents_session_messages
  WHERE session_id = ? AND id = ?
  UNION ALL
  SELECT m.id, m.parent_id, p.depth + 1 FROM cf_agents_session_messages m
  JOIN path p ON m.id = p.parent_id
  WHERE m.session_id = ? AND p.depth < 10000
)
SELECT path.id AS id, m.role AS role, m.token_estimate AS token_estimate,
  LENGTH(CAST(m.content AS BLOB)) + CASE WHEN m.content_chunks = 0 THEN 0
    ELSE COALESCE((
      SELECT SUM(LENGTH(CAST(c.content AS BLOB)))
      FROM cf_agents_session_message_chunks c
      WHERE c.session_id = m.session_id AND c.id = m.id
    ), 0) END AS bytes
FROM path JOIN cf_agents_session_messages m
  ON m.session_id = ? AND m.id = path.id
ORDER BY path.depth DESC"""

# Point every child of a deleted row at its nearest surviving ancestor, so
# the chain stays connected. Parameters: ids (JSON), session, session,
# session.
_REWIRE = """WITH RECURSIVE
  deleted(id) AS (SELECT value FROM json_each(?)),
  rewire(child_id, ancestor_id, depth) AS (
    SELECT child.id, child.parent_id, 0
    FROM cf_agents_session_messages AS child
    JOIN deleted ON deleted.id = child.parent_id
    WHERE child.session_id = ? AND child.id NOT IN (SELECT id FROM deleted)
    UNION ALL
    SELECT rewire.child_id, parent.parent_id, rewire.depth + 1
    FROM rewire
    JOIN cf_agents_session_messages AS parent ON parent.id = rewire.ancestor_id
    JOIN deleted ON deleted.id = parent.id
    WHERE parent.session_id = ? AND rewire.depth < 10000
  ),
  nearest(child_id, ancestor_id) AS (
    SELECT child_id, ancestor_id FROM rewire
    WHERE ancestor_id IS NULL OR ancestor_id NOT IN (SELECT id FROM deleted)
  )
UPDATE cf_agents_session_messages
SET parent_id = (
  SELECT nearest.ancestor_id FROM nearest
  WHERE nearest.child_id = cf_agents_session_messages.id
)
WHERE session_id = ? AND id IN (SELECT child_id FROM nearest)"""


class PathRow(NamedTuple):
    """One message on a path, without its content."""

    id: str
    role: str
    bytes: int
    token_estimate: int


class Tail(NamedTuple):
    """The newest row's id and the next ``seq``."""

    leaf_id: str | None
    next_seq: int


class SessionStore:
    """Every SQL statement Sessions runs. Writes run inside callers' transactions."""

    __slots__ = ("sql",)

    def __init__(self, sql: Sql) -> None:
        self.sql = sql

    def ensure_tables(self) -> None:
        """Create both tables (idempotent)."""
        self.sql(_MESSAGES)
        self.sql(_CHUNKS)

    # Reads

    def exists(self, session_id: str, id: str) -> bool:
        """Return whether the session has a message with ``id`` (key-only probe)."""
        return bool(
            self.sql(
                "SELECT id FROM cf_agents_session_messages"
                " WHERE session_id = ? AND id = ?",
                session_id,
                id,
            )
        )

    def content(self, session_id: str, id: str) -> str | None:
        """Return one message's full JSON text."""
        rows = self.sql(
            "SELECT content, content_chunks FROM cf_agents_session_messages"
            " WHERE session_id = ? AND id = ?",
            session_id,
            id,
        )
        if not rows:
            return None
        row = rows[0]
        if row["content_chunks"] == 0:
            return row["content"]
        return row["content"] + self.continuations(session_id, [id]).get(id, "")

    def key_columns(self, session_id: str, id: str) -> dict[str, Any] | None:
        """Return a row's ``content_chunks``, ``token_estimate``, ``content_hash``."""
        rows = self.sql(
            "SELECT content_chunks, token_estimate, content_hash"
            " FROM cf_agents_session_messages WHERE session_id = ? AND id = ?",
            session_id,
            id,
        )
        return rows[0] if rows else None

    def has_parent(self, session_id: str, id: str) -> bool:
        """Return whether the row has a parent."""
        rows = self.sql(
            "SELECT parent_id FROM cf_agents_session_messages"
            " WHERE session_id = ? AND id = ?",
            session_id,
            id,
        )
        return bool(rows) and rows[0]["parent_id"] is not None

    def tail(self, session_id: str) -> Tail:
        """Read the newest row (the leaf of a linear session)."""
        rows = self.sql(
            "SELECT id, seq FROM cf_agents_session_messages WHERE session_id = ?"
            " ORDER BY seq DESC LIMIT 1",
            session_id,
        )
        if not rows:
            return Tail(leaf_id=None, next_seq=1)
        return Tail(leaf_id=rows[0]["id"], next_seq=rows[0]["seq"] + 1)

    def path(self, session_id: str, leaf_id: str) -> list[PathRow]:
        """Return the path from the root to ``leaf_id``, without content."""
        rows = self.sql(_PATH, session_id, leaf_id, session_id, session_id)
        return [
            PathRow(row["id"], row["role"], row["bytes"], row["token_estimate"])
            for row in rows
        ]

    def contents(self, session_id: str, ids: Sequence[str]) -> dict[str, str]:
        """Return the full JSON text of each message in ``ids`` that exists."""
        if not ids:
            return {}
        rows = self.sql(
            "SELECT id, content, content_chunks FROM cf_agents_session_messages"
            " WHERE session_id = ? AND id IN (SELECT value FROM json_each(?))",
            session_id,
            json.dumps(list(ids)),
        )
        continued = self.continuations(
            session_id, [row["id"] for row in rows if row["content_chunks"] > 0]
        )
        return {
            row["id"]: row["content"] + continued.get(row["id"], "")
            if row["content_chunks"]
            else row["content"]
            for row in rows
        }

    def continuations(self, session_id: str, ids: Sequence[str]) -> dict[str, str]:
        """Return the joined continuation slices of each message in ``ids``."""
        if not ids:
            return {}
        rows = self.sql(
            "SELECT id, content FROM cf_agents_session_message_chunks"
            " WHERE session_id = ? AND id IN (SELECT value FROM json_each(?))"
            " ORDER BY id ASC, idx ASC",
            session_id,
            json.dumps(list(ids)),
        )
        joined: dict[str, str] = {}
        for row in rows:
            joined[row["id"]] = joined.get(row["id"], "") + row["content"]
        return joined

    # Writes (callers wrap them in one transaction)

    def insert(
        self,
        session_id: str,
        id: str,
        *,
        seq: int,
        parent_id: str | None,
        role: str,
        slices: Sequence[str],
        token_estimate: int,
        content_hash: str,
    ) -> None:
        """Insert a message row and its continuation slices."""
        self.sql(
            "INSERT INTO cf_agents_session_messages (session_id, id, seq,"
            " parent_id, role, content, content_chunks, token_estimate,"
            " created_at, content_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            session_id,
            id,
            seq,
            parent_id,
            role,
            slices[0],
            len(slices) - 1,
            token_estimate,
            now_ms(),
            content_hash,
        )
        self._write_continuations(session_id, id, slices)

    def rewrite(
        self,
        session_id: str,
        id: str,
        *,
        role: str,
        slices: Sequence[str],
        token_estimate: int,
        content_hash: str,
        old_chunks: int,
    ) -> None:
        """Replace a message's content, dropping continuations it no longer has."""
        self.sql(
            "UPDATE cf_agents_session_messages SET role = ?, content = ?,"
            " content_chunks = ?, token_estimate = ?, content_hash = ?"
            " WHERE session_id = ? AND id = ?",
            role,
            slices[0],
            len(slices) - 1,
            token_estimate,
            content_hash,
            session_id,
            id,
        )
        if old_chunks > len(slices) - 1:
            self.sql(
                "DELETE FROM cf_agents_session_message_chunks"
                " WHERE session_id = ? AND id = ? AND idx > ?",
                session_id,
                id,
                len(slices) - 1,
            )
        self._write_continuations(session_id, id, slices)

    def delete(self, session_id: str, ids: Sequence[str]) -> None:
        """Delete messages, first rewiring their children to surviving ancestors."""
        encoded = json.dumps(list(ids))
        self.sql(_REWIRE, encoded, session_id, session_id, session_id)
        self.sql(
            "DELETE FROM cf_agents_session_messages WHERE session_id = ?"
            " AND id IN (SELECT value FROM json_each(?))",
            session_id,
            encoded,
        )
        self.sql(
            "DELETE FROM cf_agents_session_message_chunks WHERE session_id = ?"
            " AND id IN (SELECT value FROM json_each(?))",
            session_id,
            encoded,
        )

    def clear(self, session_id: str) -> None:
        """Delete every message in the session."""
        self.sql(
            "DELETE FROM cf_agents_session_messages WHERE session_id = ?", session_id
        )
        self.sql(
            "DELETE FROM cf_agents_session_message_chunks WHERE session_id = ?",
            session_id,
        )

    def _write_continuations(
        self, session_id: str, id: str, slices: Sequence[str]
    ) -> None:
        for idx in range(1, len(slices)):
            self.sql(
                "INSERT OR REPLACE INTO cf_agents_session_message_chunks"
                " (session_id, id, idx, content) VALUES (?, ?, ?, ?)",
                session_id,
                id,
                idx,
                slices[idx],
            )
