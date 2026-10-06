from typing import Any, TypedDict

import pytest
from _fake_ffi import JsException

from agents.core import Sql, SqlError


class FakeCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def toArray(self) -> list[dict[str, Any]]:  # noqa: N802  (the JS method name)
        return self._rows


class FakeSqlStorage:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.rows = rows or []
        self.error: str | None = None

    def exec(self, query: str, *params: Any) -> FakeCursor:
        self.calls.append((query, params))
        if self.error is not None:
            raise JsException(self.error)
        return FakeCursor(self.rows)


class FakeStorage:
    def __init__(self, sql: FakeSqlStorage) -> None:
        self.sql = sql


class User(TypedDict):
    id: str
    age: int


def test_runs_query_with_params_and_returns_rows() -> None:
    backend = FakeSqlStorage(rows=[{"id": "u1", "age": 30}])
    sql = Sql(FakeStorage(backend))
    users = sql("SELECT id, age FROM users WHERE id = ?", "u1", row=User)
    assert users == [{"id": "u1", "age": 30}]
    assert backend.calls == [("SELECT id, age FROM users WHERE id = ?", ("u1",))]


def test_unwraps_the_workers_sdk_storage_wrapper() -> None:
    backend = FakeSqlStorage()

    class Wrapper:
        _binding = FakeStorage(backend)

    Sql(Wrapper())("SELECT 1")
    assert backend.calls == [("SELECT 1", ())]


def test_bool_params_become_ints() -> None:
    backend = FakeSqlStorage()
    Sql(FakeStorage(backend))("INSERT INTO t VALUES (?, ?)", True, False)
    assert backend.calls[0][1] == (1, 0)
    assert all(type(param) is int for param in backend.calls[0][1])


def test_integers_beyond_safe_range_are_rejected() -> None:
    sql = Sql(FakeStorage(FakeSqlStorage()))
    with pytest.raises(ValueError, match="out of range"):
        sql("INSERT INTO t VALUES (?)", 2**53)


def test_non_sql_values_are_rejected() -> None:
    sql = Sql(FakeStorage(FakeSqlStorage()))
    with pytest.raises(TypeError):
        sql("INSERT INTO t VALUES (?)", [1, 2])  # ty: ignore[invalid-argument-type]


def test_sqlite_errors_become_sql_error_with_query() -> None:
    backend = FakeSqlStorage()
    backend.error = 'Error: near "SELEC": syntax error'
    with pytest.raises(SqlError, match="SQL query failed") as caught:
        Sql(FakeStorage(backend))("SELEC 1")
    assert caught.value.query == "SELEC 1"
    assert isinstance(caught.value.__cause__, JsException)
