import asyncio
import json
from collections.abc import Callable
from typing import Any

import pytest
from fake_runtime import CLOSED, FakeWebSocket, Host
from workers import Request

from agents.lifecycle import Lifecycle
from agents.lifecycle.host_context import current_host_context
from agents.lifecycle.types import HostContext
from agents.state import State, StateSource
from agents.websockets import (
    Connection,
    ConnectionContext,
    ConnectionStateTooLargeError,
    DuplicateConnectionIdError,
    WebSocketHandlers,
    WebSockets,
)


class ChatRoom(Host):
    """A host named like a real Durable Object class."""


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []
        self.contexts: list[HostContext | None] = []

    async def on_connect(self, connection: Connection, ctx: ConnectionContext) -> None:
        self.contexts.append(current_host_context())
        self.events.append(("connect", connection.id))

    async def on_message(self, connection: Connection, message: str | bytes) -> None:
        self.events.append(("message", message))

    async def on_close(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None:
        self.events.append(("close", code))

    async def on_error(self, connection: Connection, error: BaseException) -> None:
        self.events.append(("error", str(error)))

    def handlers(self) -> WebSocketHandlers:
        return WebSocketHandlers(
            on_connect=self.on_connect,
            on_message=self.on_message,
            on_close=self.on_close,
            on_error=self.on_error,
        )


def setup(**options: Any) -> tuple[Lifecycle, WebSockets, Recorder]:
    recorder = Recorder()
    options.setdefault("handlers", recorder.handlers())
    websockets = WebSockets(**options)
    lifecycle = Lifecycle(ChatRoom("room-1")).use(websockets)
    return lifecycle, websockets, recorder


def with_state(
    initial: Any = None,
    *,
    validate: Callable[[Any, StateSource], None] | None = None,
    **options: Any,
) -> tuple[Lifecycle, WebSockets, State[Any], Recorder]:
    holder: dict[str, WebSockets] = {}

    def broadcast(value: Any, source: StateSource) -> None:
        holder["ws"].broadcast_state(source if isinstance(source, Connection) else None)

    state: State[Any] = State(
        initial_state=initial, validate_state_change=validate, on_changed=broadcast
    )
    recorder = Recorder()
    websockets = WebSockets(handlers=recorder.handlers(), state=state, **options)
    holder["ws"] = websockets
    lifecycle = Lifecycle(ChatRoom("room-1")).use(state).use(websockets)
    return lifecycle, websockets, state, recorder


async def connect(lifecycle: Lifecycle, query: str = "_pk=c1") -> FakeWebSocket:
    request = Request(
        f"https://example.com/agents/chat-room/room-1?{query}",
        headers={"Upgrade": "websocket"},
    )
    response = await lifecycle.fetch(request)
    assert response.status == 101
    server = response.web_socket.peer
    assert server is not None
    return server


async def send(lifecycle: Lifecycle, server: FakeWebSocket, frame: Any) -> None:
    await lifecycle.websocket_message(server, json.dumps(frame))


# Connecting
def test_connect_sends_identity_then_runs_on_connect_in_host_context() -> None:
    lifecycle, _, recorder = setup()

    async def scenario() -> FakeWebSocket:
        return await connect(lifecycle)

    server = asyncio.run(scenario())
    assert server.frames() == [
        {"type": "cf_agent_identity", "name": "room-1", "agent": "chat-room"}
    ]
    assert recorder.events == [("connect", "c1")]
    context = recorder.contexts[0]
    assert context is not None and context.connection is not None
    assert context.connection.id == "c1"
    assert context.request is not None


def test_missing_or_empty_pk_gets_a_generated_id() -> None:
    lifecycle, websockets, _ = setup()

    async def scenario() -> list[str]:
        await connect(lifecycle, "")
        await connect(lifecycle, "_pk=")
        return [c.id for c in websockets.get_connections()]

    ids = asyncio.run(scenario())
    assert len(ids) == 2 and all(len(i) == 22 for i in ids) and ids[0] != ids[1]


def test_state_follows_identity_and_seeding_reaches_nobody_twice() -> None:
    lifecycle, _, _, _ = with_state({"count": 0})
    server = asyncio.run(connect(lifecycle))
    assert server.frames() == [
        {
            "type": "cf_agent_identity",
            "name": "room-1",
            "agent": "chat-room",
            "stateFollows": True,
        },
        {"type": "cf_agent_state", "state": {"count": 0}},
    ]


def test_protocol_decision_false_marks_the_connection_no_protocol() -> None:
    async def only_c2(connection: Connection, ctx: ConnectionContext) -> bool:
        return connection.id == "c2"

    lifecycle, websockets, state, _ = with_state({"n": 0}, protocol=only_c2)

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        quiet = await connect(lifecycle, "_pk=c1")
        loud = await connect(lifecycle, "_pk=c2")
        state.set({"n": 1})
        return quiet, loud

    quiet, loud = asyncio.run(scenario())
    assert quiet.frames() == []
    assert [f["type"] for f in loud.frames()] == [
        "cf_agent_identity",
        "cf_agent_state",
        "cf_agent_state",
    ]
    connection = websockets.get_connection("c1")
    assert connection is not None and not connection.protocol_enabled


def test_protocol_false_leaves_frames_and_state_frames_to_the_host() -> None:
    lifecycle, _, _, recorder = with_state({"n": 0}, protocol=False)

    async def scenario() -> FakeWebSocket:
        server = await connect(lifecycle)
        await send(lifecycle, server, {"type": "cf_agent_state", "state": {"n": 5}})
        return server

    server = asyncio.run(scenario())
    assert server.frames() == []
    assert recorder.events[-1][0] == "message"


def test_tags_put_the_id_first_and_are_queryable() -> None:
    async def tags(connection: Connection, ctx: ConnectionContext) -> list[str]:
        return ["room:a", connection.id]

    lifecycle, websockets, _ = setup(connection_tags=tags)

    async def scenario() -> None:
        await connect(lifecycle, "_pk=c1")
        connection = websockets.get_connection("c1")
        assert connection is not None
        assert connection.tags == ("c1", "room:a")
        assert [c.id for c in websockets.get_connections("room:a")] == ["c1"]

    asyncio.run(scenario())


def test_invalid_tags_fail_the_upgrade(caplog: pytest.LogCaptureFixture) -> None:
    async def too_many(connection: Connection, ctx: ConnectionContext) -> list[str]:
        return [f"t{i}" for i in range(10)]

    lifecycle, _, _ = setup(connection_tags=too_many)
    request = Request("https://example.com/?_pk=c1", headers={"Upgrade": "websocket"})
    response = asyncio.run(lifecycle.fetch(request))
    assert response.status == 101  # the error is reported over the socket
    assert "at most 10 tags" in caplog.text


def test_readonly_decision_sets_the_flag_before_on_connect() -> None:
    seen: list[bool] = []

    async def readonly(connection: Connection, ctx: ConnectionContext) -> bool:
        return "mode=view" in ctx.request.url

    async def on_connect(connection: Connection, ctx: ConnectionContext) -> None:
        seen.append(connection.readonly)

    lifecycle, _, _ = setup(
        readonly=readonly, handlers=WebSocketHandlers(on_connect=on_connect)
    )

    async def scenario() -> None:
        await connect(lifecycle, "_pk=a&mode=view")
        await connect(lifecycle, "_pk=b")

    asyncio.run(scenario())
    assert seen == [True, False]


# State sync
def test_client_state_is_saved_and_broadcast_to_everyone_else() -> None:
    lifecycle, _, state, _ = with_state({"n": 0})

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        sender = await connect(lifecycle, "_pk=a")
        other = await connect(lifecycle, "_pk=b")
        await send(lifecycle, sender, {"type": "cf_agent_state", "state": {"n": 7}})
        return sender, other

    sender, other = asyncio.run(scenario())
    assert state.get() == {"n": 7}
    assert other.frames()[-1] == {"type": "cf_agent_state", "state": {"n": 7}}
    assert len(sender.frames()) == 2  # identity and initial state only


def test_readonly_and_rejected_state_frames_get_state_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def no_negatives(value: Any, source: StateSource) -> None:
        if value["n"] < 0:
            raise ValueError("negative")

    lifecycle, websockets, _, _ = with_state({"n": 0}, validate=no_negatives)

    async def scenario() -> FakeWebSocket:
        server = await connect(lifecycle, "_pk=a")
        connection = websockets.get_connection("a")
        assert connection is not None
        await send(lifecycle, server, {"type": "cf_agent_state", "state": {"n": -1}})
        connection.readonly = True
        await send(lifecycle, server, {"type": "cf_agent_state", "state": {"n": 1}})
        return server

    server = asyncio.run(scenario())
    errors = [f for f in server.frames() if f["type"] == "cf_agent_state_error"]
    assert errors == [
        {"type": "cf_agent_state_error", "error": "State update rejected"},
        {"type": "cf_agent_state_error", "error": "Connection is readonly"},
    ]
    assert "negative" in caplog.text


# Messages, close, errors
def test_unconsumed_messages_reach_on_message_and_added_handlers_can_claim() -> None:
    claimed: list[Any] = []

    async def claim_pings(connection: Connection, message: str | bytes) -> bool:
        if message == "ping":
            claimed.append(message)
            return True
        return False

    lifecycle, websockets, recorder = setup()
    websockets.use(WebSocketHandlers(on_message=claim_pings))

    async def scenario() -> None:
        server = await connect(lifecycle)
        await lifecycle.websocket_message(server, "ping")
        await lifecycle.websocket_message(server, "hello")
        await lifecycle.websocket_message(server, b"\x00\x01")

    asyncio.run(scenario())
    assert claimed == ["ping"]
    assert [e for e in recorder.events if e[0] == "message"] == [
        ("message", "hello"),
        ("message", b"\x00\x01"),
    ]


def test_close_runs_handlers_and_echoes_the_close_frame() -> None:
    lifecycle, _, recorder = setup()

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        normal = await connect(lifecycle, "_pk=a")
        dropped = await connect(lifecycle, "_pk=b")
        await lifecycle.websocket_close(normal, 1000, "bye", True)
        await lifecycle.websocket_close(dropped, 1006, "", False)
        return normal, dropped

    normal, dropped = asyncio.run(scenario())
    assert normal.closed == (1000, "bye")
    assert dropped.closed is None  # reserved code: nothing to echo
    assert [e for e in recorder.events if e[0] == "close"] == [
        ("close", 1000),
        ("close", 1006),
    ]


def test_sockets_the_capability_did_not_accept_are_ignored() -> None:
    lifecycle, _, recorder = setup()
    stranger = FakeWebSocket()
    asyncio.run(lifecycle.websocket_message(stranger, "hi"))
    assert recorder.events == []


def test_errors_reach_on_error() -> None:
    lifecycle, _, recorder = setup()

    async def scenario() -> None:
        server = await connect(lifecycle)
        await lifecycle.websocket_error(server, RuntimeError("boom"))

    asyncio.run(scenario())
    assert recorder.events[-1] == ("error", "boom")


# Connections
def test_broadcast_excludes_by_id_or_by_connection() -> None:
    lifecycle, websockets, _ = setup()

    async def scenario() -> list[FakeWebSocket]:
        return [
            await connect(lifecycle, "_pk=a"),
            await connect(lifecycle, "_pk=b"),
            await connect(lifecycle, "_pk=b"),  # a second socket with the same id
            await connect(lifecycle, "_pk=c"),
        ]

    a, b1, b2, c = asyncio.run(scenario())
    b2_connection = next(
        conn for conn in websockets.get_connections() if conn._ws is b2
    )
    websockets.broadcast("hi", exclude=["a", b2_connection])
    assert ["hi" in s.sent for s in (a, b1, b2, c)] == [False, True, False, True]
    websockets.broadcast("all", exclude=["b"])
    assert ["all" in s.sent for s in (a, b1, b2, c)] == [True, False, False, True]


def test_get_connection_rejects_a_shared_id_and_skips_closed_sockets() -> None:
    lifecycle, websockets, _ = setup()

    async def scenario() -> FakeWebSocket:
        first = await connect(lifecycle, "_pk=dup")
        await connect(lifecycle, "_pk=dup")
        return first

    first = asyncio.run(scenario())
    with pytest.raises(DuplicateConnectionIdError):
        websockets.get_connection("dup")
    first.readyState = CLOSED
    assert websockets.get_connection("dup") is not None
    assert websockets.get_connection("nobody") is None


def test_connection_state_and_flags_live_in_the_attachment() -> None:
    lifecycle, websockets, _ = setup()

    async def scenario() -> None:
        await connect(lifecycle, "_pk=a")

    asyncio.run(scenario())
    connection = websockets.get_connection("a")
    assert connection is not None and connection.state is None
    connection.set_state({"user": "ada"})
    connection.readonly = True
    again = websockets.get_connection("a")  # a fresh wrapper, as after a wake
    assert again is not None
    assert again.state == {"user": "ada"}
    assert again.readonly
    assert again == connection and hash(again) == hash(connection)
    again.readonly = False
    assert again.state == {"user": "ada"}
    with pytest.raises(ConnectionStateTooLargeError):
        again.set_state({"blob": "x" * 20_000})
    assert again.state == {"user": "ada"}


def test_dispose_closes_every_connection() -> None:
    lifecycle, _, _ = setup()

    async def scenario() -> list[FakeWebSocket]:
        sockets = [await connect(lifecycle, "_pk=a"), await connect(lifecycle, "_pk=b")]
        await lifecycle.dispose()
        return sockets

    for server in asyncio.run(scenario()):
        assert server.closed == (1001, "Durable Object destroyed")
