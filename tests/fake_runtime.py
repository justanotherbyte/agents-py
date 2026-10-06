"""A fake Durable Object runtime for CPython tests.

SQL runs on an in-memory ``sqlite3`` database (the same engine Durable Objects
use), KV storage is a dict, and the alarm is recorded rather than fired.
"""

import json
import sqlite3
from collections.abc import Awaitable, Callable
from typing import Any

from _fake_ffi import JsException

from agents.lifecycle import Lifecycle, RouteAddress
from agents.lifecycle.types import RouteEnvelope


class FakeCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def toArray(self) -> list[dict[str, Any]]:  # noqa: N802  (JS name)
        return self._rows


class FakeSqlStorage:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row

    def exec(self, query: str, *params: Any) -> FakeCursor:
        rows = self.db.execute(query, params).fetchall()
        return FakeCursor([dict(row) for row in rows])


class FakeStorage:
    def __init__(self) -> None:
        self.sql = FakeSqlStorage()
        self.kv: dict[str, Any] = {}
        self.alarm: int | None = None
        self.alarm_history: list[int | None] = []

    async def get(self, key: str) -> Any:
        return self.kv.get(key)

    async def put(self, key: str, value: Any) -> None:
        self.kv[key] = value

    async def delete(self, key: str) -> bool:
        return self.kv.pop(key, None) is not None

    async def setAlarm(self, time: int) -> None:  # noqa: N802
        self.alarm = time
        self.alarm_history.append(time)

    async def deleteAlarm(self) -> None:  # noqa: N802
        self.alarm = None
        self.alarm_history.append(None)

    async def sync(self) -> None:
        pass

    def transactionSync(self, fn: Callable[[], Any]) -> Any:  # noqa: N802
        # A savepoint: what fn wrote is undone if it raises, as on Workers.
        self.sql.db.execute("SAVEPOINT tx")
        try:
            result = fn()
        except BaseException:
            self.sql.db.execute("ROLLBACK TO tx")
            self.sql.db.execute("RELEASE tx")
            raise
        self.sql.db.execute("RELEASE tx")
        return result

    async def deleteAll(self) -> None:  # noqa: N802
        self.kv.clear()
        self.alarm = None
        tables = self.sql.exec(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).toArray()
        for table in tables:
            self.sql.exec(f"DROP TABLE {table['name']}")


CONNECTING, OPEN, CLOSING, CLOSED = 0, 1, 2, 3


class FakeWebSocket:
    """A hibernatable server socket; records what the SDK sends and closes."""

    def __init__(self, peer: "FakeWebSocket | None" = None) -> None:
        self.peer = peer
        self.readyState = OPEN
        self.sent: list[str | bytes] = []
        self.closed: tuple[int | None, str | None] | None = None
        self._attachment: Any = None

    def serializeAttachment(self, value: Any) -> None:  # noqa: N802
        self._attachment = value

    def deserializeAttachment(self) -> Any:  # noqa: N802
        return self._attachment

    def send(self, message: str | bytes) -> None:
        if self.readyState != OPEN:
            raise JsException("Error: WebSocket is not open")
        self.sent.append(message)

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        if self.readyState == CLOSED:
            raise JsException("Error: WebSocket already closed")
        self.readyState = CLOSED
        self.closed = (code, reason)

    def frames(self) -> list[Any]:
        """Return the JSON text frames sent so far, parsed."""
        return [json.loads(m) for m in self.sent if isinstance(m, str)]


class FakeId:
    def __init__(self, name: str | None) -> None:
        self.name = name

    def __str__(self) -> str:
        return f"id-{self.name}"


class FakeCtx:
    def __init__(
        self,
        name: str | None = "test-object",
        *,
        world: "FakeWorld | None" = None,
        storage: "FakeStorage | None" = None,
    ) -> None:
        self.storage = storage if storage is not None else FakeStorage()
        self.id = FakeId(name)
        self.world = world
        self.exports: dict[str, Any] = world.exports if world is not None else {}
        self.facets = FakeFacets(world) if world is not None else None
        self.accepted: list[tuple[Any, Any]] = []
        self.blocked_calls = 0

    async def blockConcurrencyWhile(  # noqa: N802
        self, fn: Callable[[], Awaitable[Any]]
    ) -> Any:
        self.blocked_calls += 1
        return await fn()

    def acceptWebSocket(self, ws: Any, tags: Any) -> None:  # noqa: N802
        self.accepted.append((ws, tags))

    def getWebSockets(self, tag: str | None = None) -> list[Any]:  # noqa: N802
        return [ws for ws, tags in self.accepted if tag is None or tag in tags]


class Host:
    """A plain Durable Object host with no hooks."""

    def __init__(self, name: str | None = "test-object") -> None:
        self.ctx = FakeCtx(name)


class LocalTransport:
    """Connects a facet's Lifecycle to the root's, in-process."""

    def __init__(self, address: RouteAddress | None) -> None:
        self.address = address
        self.root: Lifecycle | None = None
        self.peers: dict[str, Lifecycle] = {}

    @property
    def source(self) -> RouteAddress | None:
        return self.address

    async def to_root(self, envelope: RouteEnvelope) -> Any:
        assert self.root is not None
        return await self.root.route(envelope)

    async def to(self, target: RouteAddress, envelope: RouteEnvelope) -> Any:
        return await self.peers[target.key].route(envelope)


class FakeNamespace:
    """A Durable Object binding: one instance per name, shared by the world."""

    def __init__(self, world: "FakeWorld", cls: type) -> None:
        self.world = world
        self.cls = cls

    def idFromName(self, name: str) -> FakeId:  # noqa: N802
        return FakeId(name)

    def get(self, id: FakeId, options: Any = None) -> Any:
        assert id.name is not None
        return self.world.instance(self.cls, id.name)


class FakeFacets:
    """``ctx.facets``: children with their own storage, kept across aborts."""

    def __init__(self, world: "FakeWorld") -> None:
        self.world = world
        self.live: dict[str, Any] = {}
        self.storages: dict[str, FakeStorage] = {}

    def get(self, key: str, getter: Callable[[], dict[str, Any]]) -> Any:
        if key not in self.live:
            spec = getter()
            storage = self.storages.setdefault(key, FakeStorage())
            ctx = FakeCtx(spec["id"].name, world=self.world, storage=storage)
            self.live[key] = spec["class"](ctx, env=self.world.env)
        return self.live[key]

    def abort(self, key: str, reason: Exception) -> None:
        self.live.pop(key, None)

    def delete(self, key: str) -> None:
        self.live.pop(key, None)
        self.storages.pop(key, None)


class FakeWorld:
    """A Worker's exports: bound classes get namespaces, the rest are facet-only."""

    def __init__(self) -> None:
        self.exports: dict[str, Any] = {}
        self.instances: dict[tuple[str, str], Any] = {}
        self.env: Any = None

    def export(self, cls: type, *, bound: bool = True) -> None:
        self.exports[cls.__name__] = FakeNamespace(self, cls) if bound else cls

    def instance(self, cls: type, name: str) -> Any:
        key = (cls.__name__, name)
        if key not in self.instances:
            self.instances[key] = cls(FakeCtx(name, world=self), env=self.env)
        return self.instances[key]
