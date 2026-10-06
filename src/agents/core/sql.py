"""Typed, synchronous SQL over a Durable Object's SQLite storage.

Replaces upstream's ``this.sql`` tagged template with ``?`` placeholders
(``.design/utilities.md`` §6). Runs on the raw JS storage, not the Workers
SDK's wrapper, because this is a hot path (§3.8 there).
"""

from collections.abc import Mapping, Sequence
from typing import Any, LiteralString, overload

from .. import _ffi
from .errors import SqlError
from .types import SqlValue

__all__ = ("Sql",)

_MAX_SAFE_INTEGER = 2**53 - 1


class Sql:
    """Run SQL statements against one Durable Object's SQLite database.

    Parameters
    ----------
    storage
        The Durable Object's ``ctx.storage``, wrapped by the Workers SDK or
        raw.

    Examples
    --------
    >>> rows = sql("SELECT id, age FROM users WHERE id = ?", user_id)
    >>> users = sql("SELECT id, age FROM users WHERE id = ?", user_id, row=User)
    """

    __slots__ = ("_exec",)

    def __init__(self, storage: Any) -> None:
        self._exec = _ffi.unwrap(storage).sql.exec

    @overload
    def __call__(
        self, query: LiteralString, *params: SqlValue
    ) -> Sequence[dict[str, Any]]: ...

    @overload
    def __call__[R: Mapping[str, Any]](
        self, query: LiteralString, *params: SqlValue, row: type[R]
    ) -> Sequence[R]: ...

    def __call__(
        self,
        query: LiteralString,
        *params: SqlValue,
        row: type[Mapping[str, Any]] | None = None,
    ) -> Sequence[Any]:
        """Run ``query`` and return its rows.

        Parameters
        ----------
        query
            A literal SQL statement, with ``?`` placeholders for values.
        *params
            One value per placeholder.
        row
            The type to annotate each row with, e.g. a ``TypedDict``. Static
            only: rows aren't validated.

        Returns
        -------
        list
            One ``dict`` per row; ``BLOB`` columns as ``bytes``. Empty for
            statements that return no rows.

        Raises
        ------
        SqlError
            If SQLite rejects the statement.
        TypeError
            If a parameter isn't a `SqlValue`.
        ValueError
            If an integer parameter is outside SQLite's safe range (±2**53).
        """
        js_params = [_sql_param(param) for param in params]
        try:
            rows = self._exec(query, *js_params).toArray()
        except _ffi.JsException as error:
            raise SqlError(query, str(error)) from error
        return _ffi.js_to_py(rows)


def _sql_param(value: SqlValue) -> Any:
    # bool is an int subclass, so type checkers accept it; through the runtime
    # it would be stored as the text 'true' (platform_verification.md §3.3).
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and abs(value) > _MAX_SAFE_INTEGER:
        raise ValueError(f"SQL integer parameter out of range (±2**53): {value}")
    if value is None or isinstance(value, int | float | str | bytes):
        return _ffi.py_to_js(value)
    raise TypeError(f"{type(value).__name__} isn't a SQL value")
