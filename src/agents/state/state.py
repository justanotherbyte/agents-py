"""Durable state: the State capability.

Port of upstream ``state/index.ts``. State owns the ``cf_agents_state`` row,
lazy loading with an in-memory cache, and validated persistence. Validation
and the change hook stay on the host, which passes them in; State never
touches connections (``.design/agent_api.md`` §1.6).
"""

import json
import logging
from typing import override

from ..lifecycle.capability import LifecycleCapability
from .types import StateChangeHandler, StateSource, StateValidator

__all__ = ("State",)

_log = logging.getLogger("agents.state")

_SCHEMA_VERSION_KEY = "cf_agents:state_schema_version"
_SCHEMA_VERSION = 1
_ROW_ID = "cf_state_row_id"


class State[T](LifecycleCapability):
    """One JSON value per object, saved in SQLite and cached in memory.

    ``None`` means "no state": it's what `get` returns when nothing is
    stored, and a ``None`` state isn't pushed to connections.

    Parameters
    ----------
    initial_state
        Saved (through `set`) on first access when nothing is stored.
    validate_state_change
        Called before a change is saved; raise to reject it.
    on_changed
        Called after a change is saved (e.g. to broadcast it).
    """

    def __init__(
        self,
        *,
        initial_state: T | None = None,
        validate_state_change: StateValidator[T] | None = None,
        on_changed: StateChangeHandler[T] | None = None,
    ) -> None:
        super().__init__("state")
        self._initial_state = initial_state
        self._validate = validate_state_change
        self._on_changed = on_changed
        self._state: T | None = None
        self._loaded = False
        self._table_ensured = False

    @override
    async def on_start(self) -> None:
        """Create the state table on an object's first start."""
        storage = self.lifecycle.storage
        if (await storage.get(_SCHEMA_VERSION_KEY) or 0) >= _SCHEMA_VERSION:
            self._table_ensured = True
            return
        self._ensure_table()
        await storage.put(_SCHEMA_VERSION_KEY, _SCHEMA_VERSION)

    def _ensure_table(self) -> None:
        if self._table_ensured:
            return
        self.lifecycle.sql(
            """CREATE TABLE IF NOT EXISTS cf_agents_state (
                 id TEXT PRIMARY KEY NOT NULL,
                 state TEXT
               )"""
        )
        self._table_ensured = True

    def get(self) -> T | None:
        """Return the current state, loading it on first access.

        When nothing is stored, the initial state (if any) is saved through
        `set` and returned. A row that isn't valid JSON is replaced by the
        initial state, or deleted when there is none.
        """
        if self._loaded:
            return self._state
        self._ensure_table()
        rows = self.lifecycle.sql(
            "SELECT state FROM cf_agents_state WHERE id = ?", _ROW_ID
        )
        if rows:
            try:
                self._state = json.loads(rows[0]["state"])
            except (TypeError, json.JSONDecodeError):
                _log.exception("Stored state is not valid JSON; resetting it")
                return self._reset_corrupt_state()
            self._loaded = True
            return self._state
        initial = self._initial_value()
        if initial is not None:
            self.set(initial, "server")
        return self._state

    def _initial_value(self) -> T | None:
        """Return the state to seed when nothing is stored."""
        return self._initial_state

    def _reset_corrupt_state(self) -> T | None:
        initial = self._initial_value()
        if initial is not None:
            self.set(initial, "server")
            return self._state
        self.lifecycle.sql("DELETE FROM cf_agents_state WHERE id = ?", _ROW_ID)
        return None

    def set(self, state: T, source: StateSource = "server") -> None:
        """Validate, save, and cache a new state, then call ``on_changed``.

        Saved first and cached second, so a value that fails to serialize or
        write is never served. The cache holds a fresh copy (parsed from the
        saved JSON), so the caller's object can't change it afterwards.

        Raises
        ------
        Exception
            Whatever ``validate_state_change`` raises, rejecting the change.
        TypeError, ValueError
            If ``state`` isn't JSON-serializable.
        """
        if self._validate is not None:
            self._validate(state, source)
        self._ensure_table()
        serialized = json.dumps(state)
        self.lifecycle.sql(
            "INSERT OR REPLACE INTO cf_agents_state (id, state) VALUES (?, ?)",
            _ROW_ID,
            serialized,
        )
        saved: T = json.loads(serialized)
        self._state = saved
        self._loaded = True
        if self._on_changed is None:
            return
        try:
            self._on_changed(saved, source)
        except Exception:
            # The change is already saved; a failing observer can't undo it.
            _log.exception("State on_changed hook failed")
