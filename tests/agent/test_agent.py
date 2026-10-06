import asyncio
import json
from typing import Any, TypedDict

import _fake_ffi
import pytest
from fake_runtime import FakeCtx, FakeWebSocket
from workers import Request, Response

from agents import (
    Agent,
    AgentOptions,
    Connection,
    ConnectionContext,
    ObservabilityEvent,
    QueueItem,
    StateSource,
    callable,
    get_current_agent,
)


class Counter(TypedDict):
    count: int


class Recording:
    """An observability sink that keeps every event."""

    def __init__(self) -> None:
        self.events: list[ObservabilityEvent] = []

    def emit(self, event: ObservabilityEvent) -> None:
        self.events.append(event)


class CounterAgent(Agent[Counter]):
    initial_state = Counter(count=0)

    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.log: list[Any] = []
        self.errors: list[tuple[Connection | None, Exception]] = []
        self.observability = Recording()

    @callable
    async def increment(self) -> int:
        current = self.state
        assert current is not None
        self.set_state(Counter(count=current["count"] + 1))
        return current["count"] + 1

    @callable
    async def whoami(self) -> str | None:
        current = get_current_agent()
        assert current is not None and current.agent is self
        return current.connection.id if current.connection is not None else None

    async def on_connect(self, connection: Connection, ctx: ConnectionContext) -> None:
        current = get_current_agent()
        self.log.append(
            (
                "connect",
                connection.id,
                current is not None and current.connection == connection,
            )
        )

    async def on_message(self, connection: Connection, message: str | bytes) -> None:
        if message == "explode":
            raise RuntimeError("message hook failed")
        self.log.append(("message", message))

    async def on_close(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None:
        self.log.append(("close", code))

    async def on_state_changed(self, state: Counter, source: StateSource) -> None:
        self.log.append(
            ("state", state["count"], source if source == "server" else "client")
        )

    async def should_connection_be_readonly(
        self, connection: Connection, ctx: ConnectionContext
    ) -> bool:
        return "mode=view" in ctx.request.url

    def validate_state_change(self, next_state: Counter, source: StateSource) -> None:
        if next_state["count"] < 0:
            raise ValueError("negative")

    async def process(self, payload: Any, item: QueueItem) -> None:
        current = get_current_agent()
        self.log.append(
            ("queued", payload, current is not None and current.agent is self)
        )

    def plain_helper(self) -> bool:
        current = get_current_agent()
        return (
            current is not None and current.agent is self and current.connection is None
        )


def make(cls: type[Agent[Any]] = CounterAgent, name: str = "room-1") -> Any:
    return cls(FakeCtx(name), env=None)


async def connect(agent: Agent[Any], query: str = "_pk=a") -> FakeWebSocket:
    request = Request(
        f"https://example.com/agents/counter-agent/room-1?{query}",
        headers={"Upgrade": "websocket"},
    )
    response = await agent.fetch(request)  # ty: ignore[unresolved-attribute]
    assert response.status == 101
    return response.web_socket.peer


async def send(agent: Agent[Any], ws: FakeWebSocket, frame: Any) -> None:
    text = frame if isinstance(frame, str) else json.dumps(frame)
    await agent.webSocketMessage(ws, text)  # ty: ignore[unresolved-attribute]


def run[T](coro: Any) -> T:
    return asyncio.run(coro)


# Connect sequence


def test_connect_sends_identity_state_and_empty_mcp_then_on_connect() -> None:
    agent = make()
    ws = run(connect(agent))
    assert ws.frames() == [
        {
            "type": "cf_agent_identity",
            "name": "room-1",
            "agent": "counter-agent",
            "stateFollows": True,
        },
        {"type": "cf_agent_state", "state": {"count": 0}},
        {
            "type": "cf_agent_mcp_servers",
            "mcp": {"servers": {}, "tools": [], "prompts": [], "resources": []},
        },
    ]
    assert ("connect", "a", True) in agent.log
    assert [e.type for e in agent.observability.events] == ["state:update", "connect"]


class Quiet(CounterAgent):
    options = AgentOptions(send_identity_on_connect=False)

    async def should_send_protocol_messages(
        self, connection: Connection, ctx: ConnectionContext
    ) -> bool:
        return "binary" not in ctx.request.url


def test_identity_can_be_turned_off_and_protocol_skipped_per_connection() -> None:
    agent = make(Quiet)

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        return await connect(agent, "_pk=a"), await connect(agent, "_pk=b&binary=1")

    with_protocol, binary = run(scenario())
    assert [f["type"] for f in with_protocol.frames()] == [
        "cf_agent_state",
        "cf_agent_mcp_servers",
    ]
    assert binary.frames() == []
    connection = agent.get_connection("b")
    assert connection is not None and not connection.protocol_enabled


# State


def test_rpc_set_state_broadcasts_to_everyone_and_runs_on_state_changed() -> None:
    agent = make()

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        a, b = await connect(agent, "_pk=a"), await connect(agent, "_pk=b")
        a.sent.clear()
        b.sent.clear()
        await send(
            agent, a, {"type": "rpc", "id": "1", "method": "increment", "args": []}
        )
        await asyncio.sleep(0)  # on_state_changed runs as a task
        return a, b

    a, b = run(scenario())
    assert {"type": "cf_agent_state", "state": {"count": 1}} in a.frames()
    assert b.frames() == [{"type": "cf_agent_state", "state": {"count": 1}}]
    assert a.frames()[-1] == {
        "type": "rpc",
        "id": "1",
        "success": True,
        "done": True,
        "result": 1,
    }
    assert ("state", 1, "server") in agent.log


def test_client_state_frames_are_validated_and_readonly_is_enforced() -> None:
    agent = make()

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        writer = await connect(agent, "_pk=w")
        viewer = await connect(agent, "_pk=v&mode=view")
        writer.sent.clear()
        viewer.sent.clear()
        await send(agent, writer, {"type": "cf_agent_state", "state": {"count": 5}})
        await send(agent, writer, {"type": "cf_agent_state", "state": {"count": -1}})
        await send(agent, viewer, {"type": "cf_agent_state", "state": {"count": 9}})
        await send(
            agent, viewer, {"type": "rpc", "id": "r", "method": "increment", "args": []}
        )
        await asyncio.sleep(0)
        return writer, viewer

    writer, viewer = run(scenario())
    assert agent.state == {"count": 5}
    assert writer.frames() == [
        {"type": "cf_agent_state_error", "error": "State update rejected"}
    ]
    assert viewer.frames() == [
        {"type": "cf_agent_state", "state": {"count": 5}},
        {"type": "cf_agent_state_error", "error": "Connection is readonly"},
        {"type": "rpc", "id": "r", "success": False, "error": "Connection is readonly"},
    ]
    assert ("state", 5, "client") in agent.log


def test_each_agent_gets_its_own_copy_of_initial_state() -> None:
    first, second = make(name="a"), make(name="b")

    async def scenario() -> None:
        state = first.state
        state["count"] = 100
        assert second.state == {"count": 0}

    run(scenario())
    assert CounterAgent.initial_state == {"count": 0}


class ComputedInitial(Agent):
    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.initial_state = {"seeded_for": self.name}


def test_initial_state_set_in_init_is_used() -> None:
    agent = make(ComputedInitial)

    async def scenario() -> Any:
        return agent.state

    assert run(scenario()) == {"seeded_for": "room-1"}


# Messages, RPC, context


def test_messages_rpc_and_context() -> None:
    agent = make()

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        ws.sent.clear()
        await send(agent, ws, "hello")
        await send(
            agent, ws, {"type": "rpc", "id": "1", "method": "whoami", "args": []}
        )
        return ws

    ws = run(scenario())
    assert ("message", "hello") in agent.log
    assert ws.frames()[-1]["result"] == "a"
    assert agent.get_callable_methods().keys() == {"increment", "whoami"}


def test_public_methods_run_as_the_current_agent() -> None:
    agent = make()
    assert get_current_agent() is None
    assert agent.plain_helper() is True
    assert run(agent.whoami()) is None  # as if called over native RPC


def test_message_errors_go_to_on_error_and_a_handled_error_keeps_the_socket() -> None:
    class Handles(CounterAgent):
        async def on_error(
            self, connection: Connection | None, error: Exception
        ) -> None:
            self.errors.append((connection, error))

    agent = make(Handles)

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(agent, ws, "explode")
        return ws

    ws = run(scenario())
    assert [(c, str(e)) for c, e in agent.errors] == [(None, "message hook failed")]
    assert ws.closed is None


def test_unhandled_message_errors_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    agent = make()

    async def scenario() -> None:
        ws = await connect(agent)
        await send(agent, ws, "explode")

    run(scenario())
    assert "message hook failed" in caplog.text


def test_close_emits_disconnect_and_runs_on_close() -> None:
    agent = make()

    async def scenario() -> None:
        ws = await connect(agent)
        await agent.webSocketClose(ws, 1000, "bye", True)

    run(scenario())
    assert ("close", 1000) in agent.log
    assert agent.observability.events[-1].payload == {
        "connectionId": "a",
        "code": 1000,
        "reason": "bye",
    }


# HTTP and startup


class WebAgent(Agent):
    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.errors: list[Exception] = []
        self.handle = False
        self.starts = 0

    async def on_start(self) -> None:
        self.starts += 1
        if self.starts == 1:
            raise RuntimeError("first start fails")

    async def on_request(self, request: Request) -> Response:
        if request.url.endswith("/boom"):
            raise ValueError("request failed")
        current = get_current_agent()
        assert current is not None and current.request is request
        return Response("ok")

    async def on_error(self, connection: Connection | None, error: Exception) -> None:
        self.errors.append(error)
        if not self.handle:
            raise error


def test_on_start_failure_is_reported_then_retried() -> None:
    agent = make(WebAgent)

    async def scenario() -> tuple[Any, Any]:
        first = await agent.fetch(Request("https://example.com/x"))
        second = await agent.fetch(Request("https://example.com/x"))
        return first, second

    first, second = run(scenario())
    assert first.status == 500
    assert second.body == "ok"
    assert [str(e) for e in agent.errors] == ["first start fails"]


def test_request_errors_go_to_on_error() -> None:
    agent = make(WebAgent)
    agent.starts = 1  # skip the failing start

    async def boom() -> Any:
        return await agent.fetch(Request("https://example.com/boom"))

    assert run(boom()).status == 500
    agent.handle = True
    assert run(boom()).status == 500
    assert [str(e) for e in agent.errors] == ["request failed", "request failed"]


def test_default_on_request_is_404() -> None:
    response = run(make().fetch(Request("https://example.com/")))
    assert response.status == 404


# Queue


def test_queue_runs_methods_as_the_current_agent() -> None:
    agent = make()

    async def scenario() -> None:
        item_id = await agent.queue(agent.process, {"n": 1})
        assert [i.id for i in await agent.queue_items()] == [item_id]
        await asyncio.sleep(0.01)
        await agent.alarm()

    run(scenario())
    assert ("queued", {"n": 1}, True) in agent.log
    types = [e.type for e in agent.observability.events]
    assert "queue:create" in types


def test_dequeue_helpers() -> None:
    agent = make()

    async def scenario() -> None:
        a = await agent.queue("process", 1)
        await agent.queue("process", 2)
        assert (await agent.get_queue(a)) is not None
        assert await agent.dequeue(a)
        assert await agent.dequeue_all_by_callback(agent.process) == 1
        assert await agent.dequeue_all() == 0

    run(scenario())


# Destroy


def test_destroy_wipes_storage_closes_sockets_and_resets() -> None:
    _fake_ffi.aborts.clear()
    agent = make()

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await agent.queue("process", 1)
        await agent.destroy()
        return ws

    ws = run(scenario())
    assert ws.closed == (1001, "Durable Object destroyed")
    assert agent.ctx.storage.kv == {}
    assert agent.ctx.storage.alarm is None
    assert _fake_ffi.aborts == ["destroyed"]
    assert agent.observability.events[-1].type == "destroy"


def test_a_pending_destroy_is_finished_by_the_next_alarm() -> None:
    _fake_ffi.aborts.clear()
    agent = make()
    agent.ctx.storage.kv["cf_agents_destroy_pending"] = True
    run(agent.alarm())
    assert _fake_ffi.aborts == ["destroyed"]
    assert agent.log == []


def test_ensure_initialized_starts_the_agent() -> None:
    agent = make()
    run(getattr(agent, "__unsafe_ensureInitialized")())
    assert agent.lifecycle.is_started()


def test_agent_is_generic_and_subclasses_keep_the_sdk_wrapping() -> None:
    assert Agent[Counter] is not None
    assert getattr(CounterAgent, "wrapped_by_sdk", False)
