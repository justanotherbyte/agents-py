"""A parent's registry of the sub-agents it created (``cf_agents_sub_agents``).

Port of upstream ``dynamic-agents/registry.ts``. Python has no legacy
(bare-name) facets, so every row is a path-scoped identity
(``.design/subagents_engine.md`` §3.2 item 1).
"""

from collections.abc import Sequence

from ..core.sql import Sql
from ..core.timing import from_epoch_ms, now_ms
from .types import SubAgentInfo

__all__ = ("SubAgentRegistry",)

_IDENTITY_VERSION = "path-v2"


class SubAgentRegistry:
    """The sub-agents one parent created, in its own SQLite.

    Parameters
    ----------
    sql
        The parent's SQL helper.
    """

    __slots__ = ("_ready", "_sql")

    def __init__(self, sql: Sql) -> None:
        self._sql = sql
        self._ready = False

    def _ensure(self) -> None:
        if self._ready:
            return
        self._sql(
            """CREATE TABLE IF NOT EXISTS cf_agents_sub_agents (
                 class TEXT NOT NULL,
                 name TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 identity_version TEXT,
                 identity_name TEXT,
                 PRIMARY KEY (class, name)
               )"""
        )
        self._ready = True

    def record(self, class_name: str, name: str, identity: str) -> None:
        """Record a child (no-op if it's already recorded)."""
        self._ensure()
        self._sql(
            """INSERT OR IGNORE INTO cf_agents_sub_agents
                 (class, name, created_at, identity_version, identity_name)
               VALUES (?, ?, ?, ?, ?)""",
            class_name,
            name,
            now_ms(),
            _IDENTITY_VERSION,
            identity,
        )

    def identity(self, class_name: str, name: str) -> str | None:
        """Return a recorded child's identity name, or ``None``."""
        self._ensure()
        rows = self._sql(
            """SELECT identity_name FROM cf_agents_sub_agents
               WHERE class = ? AND name = ?""",
            class_name,
            name,
        )
        return rows[0]["identity_name"] if rows else None

    def forget(self, class_name: str, name: str) -> None:
        """Remove a child's row."""
        self._ensure()
        self._sql(
            "DELETE FROM cf_agents_sub_agents WHERE class = ? AND name = ?",
            class_name,
            name,
        )

    def has(self, class_name: str, name: str) -> bool:
        """Return whether a child is recorded."""
        return self.identity(class_name, name) is not None

    def list(self, class_name: str | None = None) -> Sequence[SubAgentInfo]:
        """Return the recorded children, oldest first, optionally of one class."""
        self._ensure()
        rows = self._sql(
            """SELECT class, name, created_at FROM cf_agents_sub_agents
               WHERE ? IS NULL OR class = ?
               ORDER BY created_at ASC""",
            class_name,
            class_name,
        )
        return [
            SubAgentInfo(
                class_name=row["class"],
                name=row["name"],
                created_at=from_epoch_ms(row["created_at"]),
            )
            for row in rows
        ]
