import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict

import _fake_ffi
import pytest
from fake_runtime import FakeWebSocket, FakeWorld
from workers import Request, Response

from agents import (
    Agent,
    AgentRoute,
    Connection,
    ConnectionContext,
    TaskStep,
    TaskStepAttempt,
    callable,
    get_sub_agent_by_name,
    route_sub_agent_request,
    task,
)
from agents.tasks import CompletedRun


class Inbox(Agent):
    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.facts: list[str] = []

    async def on_before_sub_agent(self, request: Request, child: AgentRoute) -> Any:
        if child.name == "forbidden":
            return Response("Forbidden", status=403)
        return None

    async def add_fact(self, fact: str) -> int:
        self.facts.append(fact)
        return len(self.facts)


class ChatState(TypedDict):
    messages: list[str]


class Chat(Agent[ChatState]):
    initial_state = ChatState(messages=[])

    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.log: list[Any] = []

    async def summary(self) -> dict[str, Any]:
        return {"name": self.name, "parent": [list(step) for step in self.parent_path]}

    @callable
    async def add(self, text: str) -> int:
        state = self.state
        assert state is not None
        messages = [*state["messages"], text]
        self.set_state(ChatState(messages=messages))
        return len(messages)

    async def remember(self, fact: str) -> int:
        inbox = await self.parent_agent(Inbox)
        return await inbox.add_fact(fact)

    async def plan(self) -> None:
        await self.schedule(
            datetime.now(UTC) - timedelta(seconds=1), self.remind, {"n": 1}
        )
        await self.queue(self.remind, {"n": 2})

    async def remind(self, payload: Any, item: Any) -> None:
        self.log.append(("remind", payload))

    async def start_job(self) -> str:
        return (await self.job.run(5)).run_id

    async def job_state(self, run_id: str) -> Any:
        run = await self.job.get(run_id)
        return run.result if isinstance(run, CompletedRun) else type(run).__name__

    @task
    async def job(self, n: int, step: TaskStep) -> int:
        async def double(_attempt: TaskStepAttempt) -> int:
            return n * 2

        value = await step.do("double", double)
        await step.sleep("pause", 0.02)
        return value

    async def open_note(self, name: str) -> dict[str, Any]:
        note = await self.dynamic_agents.get(Note, name)
        return await note.describe()

    async def on_request(self, request: Request) -> Response:
        return Response(f"chat {self.name} {request.url}")

    async def on_connect(self, connection: Connection, ctx: ConnectionContext) -> None:
        self.log.append(("connect", connection.id, connection.uri))

    async def on_message(self, connection: Connection, message: str | bytes) -> None:
        self.log.append(("message", message))
        connection.send(f"echo:{message}")

    async def on_close(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None:
        self.log.append(("close", code))

    async def leave(self) -> None:
        await self.destroy()


class Note(Agent):
    async def describe(self) -> dict[str, Any]:
        return {"name": self.name, "path": [list(step) for step in self.self_path]}

    async def on_message(self, connection: Connection, message: str | bytes) -> None:
        connection.send(f"note:{message}")


def make_world() -> tuple[FakeWorld, Any]:
    world = FakeWorld()
    world.export(Inbox)
    world.export(Chat, bound=False)
    world.export(Note, bound=False)
    return world, world.instance(Inbox, "alice")


def chat_of(world: FakeWorld, root: Any, name: str) -> Any:
    return root.ctx.facets.live[f"Chat\0{name}"]


async def connect(root: Any, path: str) -> FakeWebSocket:
    request = Request(
        f"https://x.dev/agents/inbox/alice{path}", headers={"Upgrade": "websocket"}
    )
    response = await root.fetch(request)
    assert response.status == 101
    return response.web_socket.peer


# Creating and reaching sub-agents


def test_get_creates_a_facet_with_its_own_identity_and_storage() -> None:
    world, inbox = make_world()

    async def scenario() -> dict[str, Any]:
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        return await chat.summary()

    assert asyncio.run(scenario()) == {"name": "c1", "parent": [["Inbox", "alice"]]}
    facet = chat_of(world, inbox, "c1")
    assert facet.ctx.id.name.startswith("cf-agents:v2:c1:")
    assert facet.ctx.storage is not inbox.ctx.storage
    assert inbox.dynamic_agents.has(Chat, "c1") and inbox.dynamic_agents.has(
        "Chat", "c1"
    )
    assert [info.name for info in inbox.dynamic_agents.list("Chat")] == ["c1"]


def test_invalid_sub_agents_are_rejected() -> None:
    _, inbox = make_world()

    class Unexported(Agent): ...

    with pytest.raises(ValueError, match="isn't exported"):
        asyncio.run(inbox.dynamic_agents.get(Unexported, "x"))
    with pytest.raises(ValueError, match="NUL"):
        asyncio.run(inbox.dynamic_agents.get(Chat, "a\0b"))


def test_parent_agent_and_nested_paths() -> None:
    world, inbox = make_world()

    async def scenario() -> tuple[int, dict[str, Any]]:
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        count = await chat.remember("likes tea")
        return count, await chat.open_note("n1")

    count, note = asyncio.run(scenario())
    assert count == 1 and inbox.facts == ["likes tea"]
    assert note == {
        "name": "n1",
        "path": [["Inbox", "alice"], ["Chat", "c1"], ["Note", "n1"]],
    }
    with pytest.raises(RuntimeError, match="only available on a sub-agent"):
        asyncio.run(inbox.parent_agent(Inbox))
    chat = chat_of(world, inbox, "c1")
    with pytest.raises(TypeError, match="parent is a Inbox"):
        asyncio.run(chat.parent_agent(Chat))


def test_worker_side_helpers() -> None:
    world, _ = make_world()
    parent = world.exports["Inbox"].get(world.exports["Inbox"].idFromName("alice"))

    async def scenario() -> tuple[Any, Response]:
        chat = await get_sub_agent_by_name(parent, Chat, "c9")
        summary = await chat.summary()
        request = Request("https://x.dev/custom/route")
        response = await route_sub_agent_request(
            request, parent, from_path="/sub/chat/c9/hello"
        )
        return summary, response

    summary, response = asyncio.run(scenario())
    assert summary["name"] == "c9"
    assert response.body == "chat c9 https://x.dev/hello"


# Lifecycle and routed work


def test_abort_keeps_storage_and_delete_wipes_it() -> None:
    _, inbox = make_world()

    async def scenario() -> list[Any]:
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        await chat.add("one")
        inbox.dynamic_agents.abort(Chat, "c1")
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        after_abort = (await chat.add("two"),)
        await inbox.dynamic_agents.delete(Chat, "c1")
        assert not inbox.dynamic_agents.has(Chat, "c1")
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        return [after_abort, await chat.add("fresh")]

    assert asyncio.run(scenario()) == [(2,), 1]


def test_a_facets_schedules_and_queue_run_on_the_roots_alarm() -> None:
    world, inbox = make_world()

    async def scenario() -> list[Any]:
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        await chat.plan()
        facet = chat_of(world, inbox, "c1")
        assert facet.ctx.storage.alarm is None  # the facet has no alarm
        assert inbox.ctx.storage.alarm is not None
        await asyncio.sleep(0.01)
        await inbox.alarm()
        return facet.log

    log = asyncio.run(scenario())
    assert sorted(log, key=lambda entry: entry[1]["n"]) == [
        ("remind", {"n": 1}),
        ("remind", {"n": 2}),
    ]


def test_a_facets_task_sleeps_on_the_roots_alarm() -> None:
    world, inbox = make_world()

    async def scenario() -> Any:
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        run_id = await chat.start_job()
        facet = chat_of(world, inbox, "c1")
        while facet.tasks._background or facet.tasks._active:
            await asyncio.sleep(0.005)
        assert await chat.job_state(run_id) == "WaitingRun"
        await asyncio.sleep(0.03)
        await inbox.alarm()
        while facet.tasks._background or facet.tasks._active:
            await asyncio.sleep(0.005)
        return await chat.job_state(run_id)

    assert asyncio.run(scenario()) == 10


def test_deleting_a_sub_agent_cancels_its_routed_work() -> None:
    _, inbox = make_world()

    async def scenario() -> None:
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        await chat.schedule_every(60, "remind")
        assert len(inbox._scheduler.lifecycle.jobs.list()) == 1
        await inbox.dynamic_agents.delete(Chat, "c1")
        assert inbox._scheduler.lifecycle.jobs.list() == []

    asyncio.run(scenario())


def test_destroy_on_a_facet_asks_the_root_to_delete_it() -> None:
    _, inbox = make_world()

    async def scenario() -> None:
        chat = await inbox.dynamic_agents.get(Chat, "c1")
        await chat.leave()
        assert not inbox.dynamic_agents.has(Chat, "c1")

    asyncio.run(scenario())


# HTTP


def test_http_requests_reach_the_sub_agent_through_the_gate() -> None:
    _, inbox = make_world()

    async def scenario() -> list[Response]:
        return [
            await inbox.fetch(
                Request("https://x.dev/agents/inbox/alice/sub/chat/c1/hello?x=1")
            ),
            await inbox.fetch(
                Request("https://x.dev/agents/inbox/alice/sub/chat/forbidden")
            ),
        ]

    allowed, refused = asyncio.run(scenario())
    assert allowed.body == "chat c1 https://x.dev/hello?x=1"
    assert refused.status == 403


# WebSockets: the root keeps the socket


def test_a_sub_agent_connection_gets_the_childs_connect_sequence() -> None:
    world, inbox = make_world()

    async def scenario() -> FakeWebSocket:
        return await connect(inbox, "/sub/chat/c1?_pk=a")

    ws = asyncio.run(scenario())
    frames = ws.frames()
    assert frames[0] == {
        "type": "cf_agent_identity",
        "name": "c1",
        "agent": "chat",
        "stateFollows": True,
    }
    assert frames[1] == {"type": "cf_agent_state", "state": {"messages": []}}
    assert frames[2]["type"] == "cf_agent_mcp_servers"
    chat = chat_of(world, inbox, "c1")
    assert chat.log[0][:2] == ("connect", "a")
    assert list(inbox.get_connections()) == []  # the root keeps it but doesn't own it
    assert [c.id for c in chat.get_connections()] == ["a"]


def test_messages_rpc_state_and_close_are_forwarded() -> None:
    world, inbox = make_world()

    async def scenario() -> FakeWebSocket:
        ws = await connect(inbox, "/sub/chat/c1?_pk=a")
        other = await connect(inbox, "?_pk=root-client")
        ws.sent.clear()
        await inbox.webSocketMessage(ws, "hello")
        await inbox.webSocketMessage(
            ws, json.dumps({"type": "rpc", "id": "1", "method": "add", "args": ["hi"]})
        )
        await inbox.webSocketClose(ws, 1000, "bye", True)
        assert (
            other.frames()[-1]["type"] == "cf_agent_mcp_servers"
        )  # root state isn't leaked
        return ws

    ws = asyncio.run(scenario())
    assert ws.sent[0] == "echo:hello"
    frames = [json.loads(m) for m in ws.sent[1:]]
    assert {"type": "cf_agent_state", "state": {"messages": ["hi"]}} in frames
    assert {
        "type": "rpc",
        "id": "1",
        "success": True,
        "done": True,
        "result": 1,
    } in frames
    chat = chat_of(world, inbox, "c1")
    assert ("message", "hello") in chat.log and ("close", 1000) in chat.log
    assert ws.closed == (1000, "bye")


def test_a_refused_sub_agent_socket_closes_with_4000_plus_status() -> None:
    _, inbox = make_world()
    _fake_ffi.rejections.clear()

    async def scenario() -> None:
        request = Request(
            "https://x.dev/agents/inbox/alice/sub/chat/forbidden",
            headers={"Upgrade": "websocket"},
        )
        await inbox.fetch(request)

    asyncio.run(scenario())
    assert _fake_ffi.rejections == [(4403, "Sub-agent connection rejected (403)")]


def test_nested_sockets_reach_the_grandchild() -> None:
    _, inbox = make_world()

    async def scenario() -> FakeWebSocket:
        ws = await connect(inbox, "/sub/chat/c1/sub/note/n1?_pk=a")
        identity = ws.frames()[0]
        assert (identity["name"], identity["agent"]) == ("n1", "note")
        ws.sent.clear()
        await inbox.webSocketMessage(ws, "ping")
        return ws

    assert asyncio.run(scenario()).sent == ["note:ping"]


def test_a_pass_through_socket_is_not_the_middle_agents_own() -> None:
    world, inbox = make_world()

    async def scenario() -> None:
        await connect(inbox, "/sub/chat/c1?_pk=mine")
        await connect(inbox, "/sub/chat/c1/sub/note/n1?_pk=passing")

    asyncio.run(scenario())
    chat = chat_of(world, inbox, "c1")
    assert [c.id for c in chat.get_connections()] == ["mine"]


def test_binary_frames_reach_the_sub_agent_as_bytes() -> None:
    world, inbox = make_world()

    async def scenario() -> None:
        ws = await connect(inbox, "/sub/chat/c1?_pk=a")
        await inbox.webSocketMessage(ws, memoryview(b"\x01\x02"))

    asyncio.run(scenario())
    assert ("message", b"\x01\x02") in chat_of(world, inbox, "c1").log


def test_deleting_a_sub_agent_closes_its_sockets() -> None:
    _, inbox = make_world()

    async def scenario() -> FakeWebSocket:
        ws = await connect(inbox, "/sub/chat/c1?_pk=a")
        await inbox.dynamic_agents.delete(Chat, "c1")
        await inbox.webSocketMessage(ws, "late")  # must not recreate the chat
        return ws

    ws = asyncio.run(scenario())
    assert ws.closed == (1001, "Sub-agent deleted")
    assert not inbox.dynamic_agents.has(Chat, "c1")
