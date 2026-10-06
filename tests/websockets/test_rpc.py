import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from fake_runtime import CLOSED, FakeWebSocket, Host
from workers import Request

from agents.lifecycle import Lifecycle, LifecycleEvent
from agents.lifecycle.host_context import current_host_context
from agents.websockets import StreamingResponse, WebSockets, callable
from agents.websockets.rpc import callable_methods


@dataclass
class Point:
    x: int
    when: datetime


class Api:
    def __init__(self) -> None:
        self.contexts: list[Any] = []
        self.closed_generator = False

    @callable
    async def add(self, a: int, b: int) -> int:
        self.contexts.append(current_host_context())
        return a + b

    @callable(description="Nothing back")
    async def nothing(self) -> None:
        return None

    @callable
    async def point(self) -> Point:
        return Point(x=1, when=datetime(2026, 1, 1, tzinfo=UTC))

    @callable
    async def unserializable(self) -> object:
        return object()

    @callable
    async def fails(self) -> None:
        raise ValueError("bad input")

    @callable
    async def fails_silently(self) -> None:
        raise ValueError

    @callable(streaming=True)
    async def countdown(self, response: StreamingResponse, n: int) -> None:
        for i in range(n, 0, -1):
            response.send(i)
        response.end({"done": True})

    @callable(streaming=True)
    async def forgets_to_end(self, response: StreamingResponse) -> None:
        response.send("only chunk")

    @callable(streaming=True)
    async def breaks_midway(self, response: StreamingResponse) -> None:
        response.send(1)
        raise RuntimeError("midway")

    @callable
    async def ticks(self, n: int):
        try:
            for i in range(n):
                yield {"tick": i}
        finally:
            self.closed_generator = True

    async def helper(self) -> str:
        return "not callable"


class Child(Api):
    async def add(self, a: int, b: int) -> int:  # undecorated override
        return 0


def setup(target: object | None = None) -> tuple[Lifecycle, list[LifecycleEvent]]:
    api = target if target is not None else Api()
    lifecycle = Lifecycle(Host()).use(WebSockets(callables=api))
    events: list[LifecycleEvent] = []
    lifecycle._set_event_sink(events.append)
    return lifecycle, events


async def connect(lifecycle: Lifecycle) -> FakeWebSocket:
    request = Request("https://example.com/?_pk=c", headers={"Upgrade": "websocket"})
    response = await lifecycle.fetch(request)
    server = response.web_socket.peer
    server.sent.clear()
    return server


def call(lifecycle: Lifecycle, method: str, *args: Any) -> list[dict[str, Any]]:
    async def scenario() -> list[dict[str, Any]]:
        server = await connect(lifecycle)
        frame = {"type": "rpc", "id": "1", "method": method, "args": list(args)}
        await lifecycle.websocket_message(server, json.dumps(frame))
        return server.frames()

    return asyncio.run(scenario())


def test_result_frame_and_host_context() -> None:
    api = Api()
    lifecycle, events = setup(api)
    assert call(lifecycle, "add", 2, 3) == [
        {"type": "rpc", "id": "1", "success": True, "done": True, "result": 5}
    ]
    context = api.contexts[0]
    assert context.connection.id == "c"
    assert [(e.type, e.payload) for e in events] == [("rpc", {"method": "add"})]


def test_none_result_leaves_result_out() -> None:
    lifecycle, _ = setup()
    assert call(lifecycle, "nothing") == [
        {"type": "rpc", "id": "1", "success": True, "done": True}
    ]


def test_dataclasses_and_datetimes_are_serialized() -> None:
    lifecycle, _ = setup()
    assert call(lifecycle, "point")[0]["result"] == {"x": 1, "when": 1767225600000}


def test_unserializable_result_still_settles_the_call() -> None:
    lifecycle, _ = setup()
    frame = call(lifecycle, "unserializable")[0]
    assert frame["success"] is False
    assert frame["error"].startswith("Result is not JSON-serializable")


def test_errors_carry_only_the_message() -> None:
    lifecycle, events = setup()
    assert call(lifecycle, "fails") == [
        {"type": "rpc", "id": "1", "success": False, "error": "bad input"}
    ]
    assert events[-1].type == "rpc:error"
    assert call(lifecycle, "fails_silently")[0]["error"] == "ValueError"


def test_unknown_and_undecorated_methods() -> None:
    lifecycle, _ = setup()
    assert call(lifecycle, "missing")[0]["error"] == "Method missing does not exist"
    assert call(lifecycle, "helper")[0]["error"] == "Method helper is not callable"
    lifecycle, _ = setup(Child())
    assert call(lifecycle, "add", 1, 1)[0]["error"] == "Method add is not callable"


def test_streaming_response_sends_chunks_then_the_final_result() -> None:
    lifecycle, events = setup()
    frames = call(lifecycle, "countdown", 2)
    assert [(f["done"], f.get("result")) for f in frames] == [
        (False, 2),
        (False, 1),
        (True, {"done": True}),
    ]
    assert events[0].payload == {"method": "countdown", "streaming": True}


def test_a_stream_left_open_is_ended_and_an_error_closes_it() -> None:
    lifecycle, _ = setup()
    assert [f["done"] for f in call(lifecycle, "forgets_to_end")] == [False, True]
    frames = call(lifecycle, "breaks_midway")
    assert frames[-1] == {"type": "rpc", "id": "1", "success": False, "error": "midway"}


def test_async_generators_stream_and_end_without_a_result() -> None:
    api = Api()
    lifecycle, _ = setup(api)
    frames = call(lifecycle, "ticks", 2)
    assert frames == [
        {
            "type": "rpc",
            "id": "1",
            "success": True,
            "done": False,
            "result": {"tick": 0},
        },
        {
            "type": "rpc",
            "id": "1",
            "success": True,
            "done": False,
            "result": {"tick": 1},
        },
        {"type": "rpc", "id": "1", "success": True, "done": True},
    ]
    assert api.closed_generator


def test_a_departed_client_stops_the_generator() -> None:
    class Endless:
        def __init__(self) -> None:
            self.yielded = 0
            self.socket: FakeWebSocket | None = None

        @callable
        async def forever(self):
            while True:
                self.yielded += 1
                if self.yielded == 3 and self.socket is not None:
                    self.socket.readyState = CLOSED
                yield self.yielded

    endless = Endless()
    lifecycle, _ = setup(endless)

    async def scenario() -> None:
        server = await connect(lifecycle)
        endless.socket = server
        frame = {"type": "rpc", "id": "1", "method": "forever", "args": []}
        await lifecycle.websocket_message(server, json.dumps(frame))

    asyncio.run(asyncio.wait_for(scenario(), timeout=1))
    assert endless.yielded == 3


def test_metadata_and_decorator_checks() -> None:
    methods = callable_methods(Api())
    assert methods["nothing"].description == "Nothing back"
    assert methods["ticks"].streaming and methods["countdown"].streaming
    assert "helper" not in methods
    assert "add" not in callable_methods(Child())

    with pytest.raises(TypeError, match="must be async"):

        @callable
        def sync_method(self: object) -> None: ...

    with pytest.raises(TypeError, match="async generator"):

        @callable(streaming=True)
        async def gen(self: object):
            yield 1
