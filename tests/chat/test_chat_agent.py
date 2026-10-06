import asyncio
import itertools
import json
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from fake_runtime import FakeCtx, FakeWebSocket
from workers import Request

from agents import (
    AIChatAgent,
    AIChatAgentOptions,
    ChatMessageOptions,
    ChatResponseResult,
    ChatStreamError,
    Connection,
    Debounce,
    ObservabilityEvent,
    TextPart,
    UIMessage,
)
from agents.chat import chunks

RESPONSE = "cf_agent_use_chat_response"

_ids = itertools.count()
_CORE_FRAMES = frozenset(
    {"cf_agent_identity", "cf_agent_state", "cf_agent_mcp_servers"}
)

type Reply = Callable[[ChatMessageOptions], AsyncIterator[Any]]


class Recording:
    def __init__(self) -> None:
        self.events: list[ObservabilityEvent] = []

    def emit(self, event: ObservabilityEvent) -> None:
        self.events.append(event)

    def types(self) -> list[str]:
        return [event.type for event in self.events]


class Chat(AIChatAgent):
    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.observability = Recording()
        self.reply: Reply = hello
        self.options_seen: list[ChatMessageOptions] = []
        self.responses: list[ChatResponseResult] = []
        self.errors: list[Exception] = []

    def on_chat_message(self, options: ChatMessageOptions) -> AsyncIterator[Any]:
        self.options_seen.append(options)
        return self.reply(options)

    async def on_chat_response(self, result: ChatResponseResult) -> None:
        self.responses.append(result)

    async def on_error(self, connection: Connection | None, error: Exception) -> None:
        self.errors.append(error)


async def hello(_options: ChatMessageOptions) -> AsyncIterator[Any]:
    yield chunks.TextStart(id="t")
    yield chunks.TextDelta(id="t", delta="Hello")
    yield chunks.TextDelta(id="t", delta=" world")
    yield chunks.TextEnd(id="t")


def user(id: str, text: str) -> dict[str, Any]:
    return {"id": id, "role": "user", "parts": [{"type": "text", "text": text}]}


def chat_request(
    request_id: str, messages: list[dict[str, Any]], **extra: Any
) -> dict[str, Any]:
    body = {"messages": messages, "trigger": "submit-message", **extra}
    return {
        "type": "cf_agent_use_chat_request",
        "id": request_id,
        "init": {"method": "POST", "body": json.dumps(body)},
    }


def make(cls: type[Chat] = Chat, name: str = "chat", **kwargs: Any) -> Any:
    return cls(FakeCtx(name, **kwargs), env=None)


async def connect(agent: Any) -> FakeWebSocket:
    request = Request(
        f"https://example.com/agents/chat/chat?_pk=c{next(_ids)}",
        headers={"Upgrade": "websocket"},
    )
    response = await agent.fetch(request)
    assert response.status == 101
    ws = response.web_socket.peer
    # Keep only chat's frames (drop identity, state, and MCP).
    ws.sent[:] = [m for m in ws.sent if json.loads(m)["type"] not in _CORE_FRAMES]
    return ws


async def send(agent: Any, ws: FakeWebSocket, frame: Any) -> None:
    await agent.webSocketMessage(ws, json.dumps(frame))


def chat_frames(
    ws: FakeWebSocket, request_id: str | None = None
) -> list[dict[str, Any]]:
    return [
        f
        for f in ws.frames()
        if f["type"] == RESPONSE and (request_id is None or f["id"] == request_id)
    ]


def bodies(ws: FakeWebSocket, request_id: str | None = None) -> list[dict[str, Any]]:
    return [
        json.loads(f["body"])
        for f in chat_frames(ws, request_id)
        if not f["done"] and not f.get("error")
    ]


def texts(agent: Any) -> list[tuple[str, str]]:
    out = []
    for message in agent.messages:
        text = "".join(p.text for p in message.parts if isinstance(p, TextPart))
        out.append((message.role, text))
    return out


def stream_rows(agent: Any) -> list[Any]:
    return list(agent.sql("SELECT stream_id, state FROM cf_agents_streams"))


def run[T](coro: Any) -> T:
    return asyncio.run(coro)


# A turn


def test_a_turn_streams_its_chunks_saves_the_reply_and_ends_with_done() -> None:
    agent = make()

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        ws = await connect(agent)
        other = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        return ws, other

    ws, other = run(scenario())
    saved = agent.messages[-1]
    assert texts(agent) == [("user", "hi"), ("assistant", "Hello world")]
    assert bodies(ws) == [
        {"type": "start", "messageId": saved.id},
        {"type": "text-start", "id": "t"},
        {"type": "text-delta", "id": "t", "delta": "Hello"},
        {"type": "text-delta", "id": "t", "delta": " world"},
        {"type": "text-end", "id": "t"},
        {"type": "finish"},
    ]
    frames = chat_frames(ws)
    assert [f["seq"] for f in frames[:-1]] == list(range(6))
    assert frames[-1] == {
        "body": "",
        "done": True,
        "id": "r1",
        "type": RESPONSE,
        "messageIds": ["u1"],
    }
    # The requester isn't sent the transcript it already has; the other tab
    # gets the user's message first and the saved transcript before done.
    assert [f["type"] for f in ws.frames() if f["type"] != RESPONSE] == []
    kinds = [f["type"] for f in other.frames()]
    assert kinds[0] == "cf_agent_chat_messages"
    assert kinds[-2:] == ["cf_agent_chat_messages", RESPONSE]
    assert stream_rows(agent) == []  # the cutover deleted the stream
    assert [r.status for r in agent.responses] == ["completed"]
    assert agent.options_seen[0].request_id == "r1"
    events = agent.observability.types()
    assert "message:request" in events
    assert "message:response" in events


def test_plain_text_replies_get_one_text_part_and_the_sdk_start_and_finish() -> None:
    agent = make()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[str]:
        yield "Hel"
        yield "lo"

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        return ws

    ws = run(scenario())
    assert [b["type"] for b in bodies(ws)] == [
        "start",
        "text-start",
        "text-delta",
        "text-delta",
        "text-end",
        "finish",
    ]
    assert bodies(ws)[1] == {"type": "text-start", "id": "r1"}
    assert texts(agent)[-1] == ("assistant", "Hello")


def test_a_finish_reason_is_sent_as_message_metadata() -> None:
    agent = make()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.Start(message_id="provider-id")
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="x")
        yield chunks.Finish(finish_reason="stop")

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        return ws

    ws = run(scenario())
    assert bodies(ws)[0] == {"type": "start", "messageId": "provider-id"}
    assert bodies(ws)[-1] == {
        "type": "finish",
        "messageMetadata": {"finishReason": "stop"},
    }
    assert agent.messages[-1].id == "provider-id"


def test_an_empty_reply_sends_only_done_and_saves_nothing() -> None:
    agent = make()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        return
        yield

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        return ws

    ws = run(scenario())
    assert [f["done"] for f in chat_frames(ws)] == [True]
    assert texts(agent) == [("user", "hi")]
    assert [r["state"] for r in stream_rows(agent)] == ["completed"]


# Failures


def test_an_error_chunk_ends_the_turn_as_failed() -> None:
    agent = make()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="partial")
        yield chunks.Error(error_text="boom")
        yield chunks.TextDelta(id="t", delta="never sent")

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        return ws

    ws = run(scenario())
    tail = chat_frames(ws)[-2:]
    assert tail == [
        {
            "error": True,
            "body": "boom",
            "done": False,
            "id": "r1",
            "type": RESPONSE,
            "messageIds": ["u1"],
        },
        {
            "body": "",
            "done": True,
            "id": "r1",
            "type": RESPONSE,
            "outcome": "error",
            "messageIds": ["u1"],
        },
    ]
    assert texts(agent)[-1] == ("assistant", "partial")  # the partial is saved
    assert [(r.status, r.error) for r in agent.responses] == [("error", "boom")]
    assert len(agent.errors) == 1 and isinstance(agent.errors[0], ChatStreamError)
    assert [r["state"] for r in stream_rows(agent)] == ["errored"]
    assert "message:error" in agent.observability.types()


def test_an_exception_ends_the_turn_with_an_error_frame_and_one_on_error() -> None:
    agent = make()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="partial")
        raise RuntimeError("model down")

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        return ws

    ws = run(scenario())
    assert chat_frames(ws)[-1] == {
        "body": "model down",
        "done": True,
        "error": True,
        "id": "r1",
        "type": RESPONSE,
        "messageIds": ["u1"],
    }
    assert texts(agent)[-1] == ("assistant", "partial")
    assert [type(e).__name__ for e in agent.errors] == ["RuntimeError"]
    assert agent.responses[0].status == "error"


def test_a_failure_before_the_first_chunk_still_sends_the_error() -> None:
    class Unanswered(AIChatAgent):
        pass

    agent = make(Unanswered)  # ty: ignore[invalid-argument-type]

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        return ws

    ws = run(scenario())
    last = chat_frames(ws)[-1]
    assert last["error"] is True and last["done"] is True
    assert "override on_chat_message" in last["body"]


@pytest.mark.parametrize(
    ("items", "message"),
    [
        (["text", chunks.TextDelta(id="t", delta="x")], "chunk after str"),
        ([chunks.TextStart(id="t"), "text"], "str after chunks"),
        ([chunks.TextStart(id="t"), chunks.Start()], "must come first"),
        (
            [chunks.TextDelta(id="t", delta="x")],
            "TextDelta(id='t') without a TextStart",
        ),
        (
            [chunks.TextStart(id="t"), chunks.TextEnd(id="t"), chunks.TextEnd(id="t")],
            "TextEnd(id='t') without a TextStart",
        ),
        (
            [chunks.ReasoningStart(id="t"), chunks.ReasoningDelta(id="u", delta="x")],
            "ReasoningDelta(id='u') without a ReasoningStart",
        ),
        (
            [chunks.ToolInputDelta(tool_call_id="c", input_text_delta="{")],
            "ToolInputDelta(toolCallId='c') without a ToolInputStart",
        ),
        ([42], "not int"),
    ],
)
def test_mixing_or_misplacing_items_fails_the_turn(
    items: list[Any], message: str
) -> None:
    agent = make()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        for item in items:
            yield item

    agent.reply = reply

    async def scenario() -> None:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))

    run(scenario())
    assert len(agent.errors) == 1
    assert isinstance(agent.errors[0], TypeError)
    assert message in str(agent.errors[0])


# Stopping


def test_a_cancel_frame_stops_the_turn_and_keeps_the_partial_reply() -> None:
    agent = make()
    gate = asyncio.Event()
    closed: list[bool] = []

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        try:
            yield chunks.TextStart(id="t")
            yield chunks.TextDelta(id="t", delta="partial")
            await gate.wait()
            yield chunks.TextDelta(id="t", delta="never")
        finally:
            closed.append(True)

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        turn = asyncio.ensure_future(
            send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        )
        await asyncio.sleep(0.01)
        await send(agent, ws, {"type": "cf_agent_chat_request_cancel", "id": "r1"})
        await turn
        return ws

    ws = run(scenario())
    assert chat_frames(ws)[-1] == {
        "body": "",
        "done": True,
        "id": "r1",
        "type": RESPONSE,
        "outcome": "aborted",
        "messageIds": ["u1"],
    }
    assert texts(agent)[-1] == ("assistant", "partial")
    assert closed == [True]
    assert agent.responses[0].status == "aborted"
    assert agent.errors == []
    assert "message:cancel" in agent.observability.types()


def test_cancelling_save_messages_stops_the_turn_and_raises() -> None:
    agent = make()
    gate = asyncio.Event()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="partial")
        await gate.wait()

    agent.reply = reply

    async def scenario() -> bool:
        await agent.lifecycle.start()
        call = asyncio.ensure_future(
            agent.save_messages(
                [UIMessage(id="u1", role="user", parts=[TextPart(text="hi")])]
            )
        )
        await asyncio.sleep(0.01)
        call.cancel()
        try:
            await call
        except asyncio.CancelledError:
            return True
        return False

    assert run(scenario()) is True
    assert texts(agent) == [("user", "hi"), ("assistant", "partial")]
    assert agent.responses[0].status == "aborted"


def test_abort_request_ends_save_messages_as_aborted() -> None:
    agent = make()

    async def reply(options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="partial")
        agent.abort_request(options.request_id)
        await asyncio.sleep(1)

    agent.reply = reply

    async def scenario() -> Any:
        await agent.lifecycle.start()
        return await agent.save_messages(
            [UIMessage(id="u1", role="user", parts=[TextPart(text="hi")])]
        )

    result = run(scenario())
    assert result.status == "aborted"


# Programmatic turns


def test_save_messages_runs_a_turn_with_the_last_request_context() -> None:
    agent = make()

    async def scenario() -> Any:
        ws = await connect(agent)
        await send(
            agent,
            ws,
            chat_request(
                "r1",
                [user("u1", "hi")],
                clientTools=[{"name": "locate", "description": "Where am I"}],
                mood="cheerful",
            ),
        )

        async def add(messages: list[UIMessage]) -> list[UIMessage]:
            return [
                *messages,
                UIMessage(id="u2", role="user", parts=[TextPart(text="again")]),
            ]

        return await agent.save_messages(add)

    result = run(scenario())
    assert result.status == "completed" and result.error is None
    assert [r for r, _ in texts(agent)] == ["user", "assistant", "user", "assistant"]
    options = agent.options_seen[-1]
    assert options.request_id == result.request_id
    assert options.body == {"mood": "cheerful"}
    assert [t.name for t in options.client_tools] == ["locate"]


def test_the_request_context_survives_a_restart() -> None:
    agent = make()

    async def first() -> None:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")], mood="calm"))

    run(first())
    restarted = make(storage=agent.ctx.storage)

    async def second() -> Any:
        await restarted.lifecycle.start()
        assert texts(restarted) == texts(agent)  # hydrated
        return await restarted.save_messages(
            [UIMessage(id="u2", role="user", parts=[TextPart(text="more")])]
        )

    assert run(second()).status == "completed"
    assert restarted.options_seen[-1].body == {"mood": "calm"}


def test_continue_last_turn_appends_to_the_last_assistant_message() -> None:
    agent = make()

    async def scenario() -> tuple[Any, Any, FakeWebSocket]:
        ws = await connect(agent)
        skipped = await agent.continue_last_turn()
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        ws.sent.clear()

        async def more(_options: ChatMessageOptions) -> AsyncIterator[Any]:
            yield chunks.TextStart(id="t2")
            yield chunks.TextDelta(id="t2", delta="!")
            yield chunks.TextEnd(id="t2")

        agent.reply = more
        result = await agent.continue_last_turn(body={"step": 2})
        return skipped, result, ws

    skipped, result, ws = run(scenario())
    assert (skipped.request_id, skipped.status) == ("", "skipped")
    assert result.status == "completed"
    assert agent.options_seen[-1].continuation is True
    assert agent.options_seen[-1].body == {"step": 2}
    assert len(agent.messages) == 2
    parts = agent.messages[-1].parts
    assert [p.text for p in parts if isinstance(p, TextPart)] == ["Hello world", "!"]
    frames = chat_frames(ws)
    assert all(f.get("continuation") is True for f in frames)
    assert json.loads(frames[0]["body"]) == {"type": "start"}  # no messageId


def test_a_continuation_resumes_a_text_part_still_streaming() -> None:
    agent = make()

    async def interrupted(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="Hel")
        await asyncio.Event().wait()

    async def resumed(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="lo")
        yield chunks.TextEnd(id="t")

    async def scenario() -> None:
        ws = await connect(agent)
        agent.reply = interrupted
        turn = asyncio.ensure_future(
            send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        )
        await asyncio.sleep(0.01)
        agent.abort_request("r1")
        await turn
        agent.reply = resumed
        await agent.continue_last_turn()

    run(scenario())
    parts = agent.messages[-1].parts
    assert [(p.text, p.state) for p in parts if isinstance(p, TextPart)] == [
        ("Hello", "done")
    ]


def test_persist_and_delete_messages_write_and_broadcast_without_a_turn() -> None:
    agent = make()

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        note = UIMessage(id="n1", role="assistant", parts=[TextPart(text="note")])
        await agent.persist_messages([*agent.messages, note])
        await agent.persist_messages([*agent.messages])  # unchanged: no write
        await agent.delete_messages(["n1", "missing"])
        return ws

    ws = run(scenario())
    transcripts = [
        f["messages"] for f in ws.frames() if f["type"] == "cf_agent_chat_messages"
    ]
    assert [[m["id"] for m in t] for t in transcripts] == [["n1"], ["n1"], []]
    assert agent.options_seen == []
    appended = [
        e for e in agent.observability.events if e.type == "session:message:appended"
    ]
    assert len(appended) == 1


# Clearing and regenerating


def test_clear_empties_the_history_and_tells_the_other_clients() -> None:
    agent = make()

    async def scenario() -> tuple[FakeWebSocket, FakeWebSocket]:
        ws = await connect(agent)
        other = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")], mood="x"))
        other.sent.clear()
        await send(agent, ws, {"type": "cf_agent_chat_clear"})
        return ws, other

    ws, other = run(scenario())
    assert len(agent.messages) == 0
    assert other.frames() == [{"type": "cf_agent_chat_clear"}]
    assert {"type": "cf_agent_chat_clear"} not in ws.frames()
    assert list(agent.sql("SELECT * FROM cf_ai_chat_request_context")) == []
    assert "message:clear" in agent.observability.types()


def test_a_regenerate_deletes_the_reply_it_replaces() -> None:
    agent = make()

    async def scenario() -> None:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        await send(
            agent,
            ws,
            chat_request("r2", [user("u1", "hi")], trigger="regenerate-message"),
        )

    run(scenario())
    assert texts(agent) == [("user", "hi"), ("assistant", "Hello world")]
    assert len({m.id for m in agent.messages}) == 2


# Concurrency


def with_policy(policy: Any, base: type[Chat] = Chat) -> Any:
    cls = type(
        "Policy", (base,), {"options": AIChatAgentOptions(message_concurrency=policy)}
    )
    return make(cls)


def overlapping(agent: Any) -> Any:
    """Run r1 (held open), then r2 and r3 while it runs; return ws."""
    gate = asyncio.Event()

    async def held(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="first")
        await gate.wait()

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        agent.reply = held
        first = asyncio.ensure_future(
            send(agent, ws, chat_request("r1", [user("u1", "a")]))
        )
        await asyncio.sleep(0.01)
        agent.reply = hello
        later = [
            asyncio.ensure_future(
                send(agent, ws, chat_request(rid, [user("u1", "a"), user(uid, text)]))
            )
            for rid, uid, text in (("r2", "u2", "b"), ("r3", "u3", "c"))
        ]
        await asyncio.sleep(0.01)
        gate.set()
        await first
        await asyncio.gather(*later)
        return ws

    return scenario()


def outcome(ws: FakeWebSocket, request_id: str) -> Any:
    done = [f for f in chat_frames(ws, request_id) if f["done"]]
    return done[-1].get("outcome", "completed") if done else None


def test_latest_skips_superseded_sends_but_keeps_their_messages() -> None:
    agent = with_policy("latest")
    ws = run(overlapping(agent))
    assert [outcome(ws, r) for r in ("r1", "r2", "r3")] == [
        "completed",
        "skipped",
        "completed",
    ]
    users = [t for r, t in texts(agent) if r == "user"]
    assert users == ["a", "b", "c"]


def test_merge_folds_queued_sends_into_one_user_message() -> None:
    agent = with_policy("merge")
    ws = run(overlapping(agent))
    assert [outcome(ws, r) for r in ("r2", "r3")] == ["skipped", "completed"]
    users = [t for r, t in texts(agent) if r == "user"]
    assert users == ["a", "b\n\nc"]


def test_drop_rejects_overlapping_sends_and_rolls_the_client_back() -> None:
    agent = with_policy("drop")
    ws = run(overlapping(agent))
    assert [outcome(ws, r) for r in ("r2", "r3")] == ["skipped", "skipped"]
    assert [t for r, t in texts(agent) if r == "user"] == ["a"]
    rollback = [f for f in ws.frames() if f["type"] == "cf_agent_chat_messages"]
    assert rollback, "the dropped send is answered with the transcript"


def test_debounce_runs_only_the_last_send_after_the_quiet_window() -> None:
    agent = with_policy(Debounce(seconds=0.02))
    ws = run(overlapping(agent))
    assert [outcome(ws, r) for r in ("r2", "r3")] == ["skipped", "completed"]


def test_debounce_rejects_negative_windows() -> None:
    with pytest.raises(ValueError, match="seconds"):
        Debounce(seconds=-1)


# Reconnecting


def test_a_client_reconnecting_mid_turn_gets_the_replay_then_live_chunks() -> None:
    agent = make()
    gate = asyncio.Event()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="one")
        await gate.wait()
        yield chunks.TextDelta(id="t", delta="two")
        yield chunks.TextEnd(id="t")

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        turn = asyncio.ensure_future(
            send(agent, ws, chat_request("r1", [user("u1", "hi")]))
        )
        await asyncio.sleep(0.01)
        late = await connect(agent)  # offered the stream on connect
        offered = late.frames()
        await send(
            agent, late, {"type": "cf_agent_stream_resume_request", "probeId": "p1"}
        )
        await send(agent, late, {"type": "cf_agent_stream_resume_ack", "id": "r1"})
        gate.set()
        await turn
        assert offered == [{"type": "cf_agent_stream_resuming", "id": "r1"}]
        return late

    late = run(scenario())
    frames = [f for f in late.frames() if f["type"] != "cf_agent_chat_messages"]
    assert frames[:2] == [
        {"type": "cf_agent_stream_resuming", "id": "r1"},
        {"type": "cf_agent_stream_resuming", "id": "r1", "probeId": "p1"},
    ]
    replay = [f for f in frames if f.get("replay")]
    assert [json.loads(f["body"])["type"] for f in replay[:-1]] == [
        "start",
        "text-start",
        "text-delta",
    ]
    assert [f["seq"] for f in replay[:-1]] == [0, 1, 2]
    assert replay[-1] == {
        "body": "",
        "done": False,
        "id": "r1",
        "type": RESPONSE,
        "replay": True,
        "replayComplete": True,
    }
    live = [f for f in frames if f["type"] == RESPONSE and not f.get("replay")]
    assert [f.get("seq") for f in live] == [3, 4, 5, None]
    assert live[-1]["done"] is True


def test_an_idle_agent_answers_a_resume_probe_with_resume_none() -> None:
    agent = make()

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        await send(
            agent, ws, {"type": "cf_agent_stream_resume_request", "probeId": "p"}
        )
        await send(agent, ws, {"type": "cf_agent_stream_resume_ack", "id": "gone"})
        return ws

    ws = run(scenario())
    assert ws.frames() == [
        {"type": "cf_agent_stream_resume_none", "reason": "idle", "probeId": "p"},
        {"body": "", "done": True, "id": "gone", "type": RESPONSE, "replay": True},
    ]


def test_a_turn_that_failed_unseen_is_replayed_through_the_handshake() -> None:
    agent = make()

    async def reply(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="partial")
        yield chunks.Error(error_text="boom")

    agent.reply = reply

    async def scenario() -> FakeWebSocket:
        await agent.lifecycle.start()
        await agent.save_messages(
            [UIMessage(id="u1", role="user", parts=[TextPart(text="hi")])]
        )
        request_id = agent.responses[0].request_id
        ws = await connect(agent)
        await send(agent, ws, {"type": "cf_agent_stream_resume_request"})
        await send(agent, ws, {"type": "cf_agent_stream_resume_ack", "id": request_id})
        return ws

    ws = run(scenario())
    request_id = agent.responses[0].request_id
    frames = ws.frames()
    assert frames[0] == {"type": "cf_agent_stream_resuming", "id": request_id}
    assert [json.loads(f["body"])["type"] for f in frames[1:-1]] == [
        "start",
        "text-start",
        "text-delta",
    ]
    # A programmatic turn has no origin messages (as upstream).
    assert frames[-1] == {
        "body": "boom",
        "done": True,
        "error": True,
        "id": request_id,
        "type": RESPONSE,
    }


def test_a_later_turn_clears_the_stored_terminal_error() -> None:
    agent = make()

    async def failing(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.Error(error_text="boom")

    async def scenario() -> FakeWebSocket:
        await agent.lifecycle.start()
        agent.reply = failing
        await agent.save_messages(
            [UIMessage(id="u1", role="user", parts=[TextPart(text="hi")])]
        )
        agent.reply = hello
        await agent.save_messages([*agent.messages])
        ws = await connect(agent)
        await send(agent, ws, {"type": "cf_agent_stream_resume_request"})
        return ws

    ws = run(scenario())
    assert ws.frames() == [{"type": "cf_agent_stream_resume_none", "reason": "idle"}]


def test_a_client_reconnecting_before_the_stream_starts_is_told_to_wait() -> None:
    agent = with_policy(Debounce(seconds=0.05))
    gate = asyncio.Event()

    async def held(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="first")
        await gate.wait()

    async def scenario() -> FakeWebSocket:
        ws = await connect(agent)
        agent.reply = held
        first = asyncio.ensure_future(
            send(agent, ws, chat_request("r1", [user("u1", "a")]))
        )
        await asyncio.sleep(0.01)
        agent.reply = hello
        second = asyncio.ensure_future(
            send(agent, ws, chat_request("r2", [user("u1", "a"), user("u2", "b")]))
        )
        await asyncio.sleep(0.01)
        gate.set()
        await first  # r2 is now debouncing: accepted, not streaming
        late = await connect(agent)
        await second
        return late

    late = run(scenario())
    frames = [f for f in late.frames() if f["type"] != "cf_agent_chat_messages"]
    assert frames[0] == {"type": "cf_agent_stream_pending", "id": "r2"}
    assert frames[1] == {"type": "cf_agent_stream_resuming", "id": "r2"}


# HTTP


async def read_body(response: Any) -> str:
    return "".join([piece async for piece in response.body])


def test_get_messages_streams_the_whole_stored_transcript() -> None:
    agent = make()

    async def scenario() -> tuple[int, str, list[Any]]:
        await agent.lifecycle.start()
        many = [
            UIMessage(id=f"m{i}", role="user", parts=[TextPart(text="é" * 10)])
            for i in range(120)
        ]
        await agent.persist_messages(many)
        response = await agent.fetch(
            Request("https://example.com/agents/chat/chat/get-messages")
        )
        return (
            response.status,
            response.headers.get("content-type"),
            json.loads(await read_body(response)),
        )

    status, content_type, messages = run(scenario())
    assert (status, content_type) == (200, "application/json")
    assert [m["id"] for m in messages] == [f"m{i}" for i in range(120)]


def test_hydration_holds_the_newest_messages_within_the_budget() -> None:
    class Small(Chat):
        options = AIChatAgentOptions(hydration_byte_budget=300)

    agent = make(Small)

    async def scenario() -> tuple[list[str], str]:
        await agent.lifecycle.start()
        await agent.persist_messages(
            [
                UIMessage(id=f"m{i}", role="user", parts=[TextPart(text="x" * 50)])
                for i in range(10)
            ]
        )
        restarted = make(Small, storage=agent.ctx.storage)
        await restarted.lifecycle.start()
        response = await restarted.fetch(
            Request("https://example.com/agents/chat/chat/get-messages")
        )
        return [m.id for m in restarted.messages], await read_body(response)

    window, full = run(scenario())
    assert 0 < len(window) < 10 and window[-1] == "m9"
    assert len(json.loads(full)) == 10


def test_max_persisted_messages_keeps_only_the_newest() -> None:
    class Capped(Chat):
        options = AIChatAgentOptions(max_persisted_messages=3)

    agent = make(Capped)

    async def scenario() -> None:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "a")]))
        await send(agent, ws, chat_request("r2", [user("u1", "a"), user("u2", "b")]))

    run(scenario())
    # u1, its reply, u2, its reply: the oldest is deleted.
    assert [m.role for m in agent.messages] == ["assistant", "user", "assistant"]
    assert agent.messages[1].id == "u2"


# Hooks and the transcript


def test_an_override_of_persist_messages_still_saves_atomically() -> None:
    class Auditing(Chat):
        def __init__(self, ctx: Any, env: Any) -> None:
            super().__init__(ctx, env)
            self.persisted: list[list[str]] = []

        async def persist_messages(self, messages: Any, *, exclude: Any = ()) -> None:
            self.persisted.append([m.id for m in messages])
            await super().persist_messages(messages, exclude=exclude)

    agent = make(Auditing)

    async def scenario() -> None:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))

    run(scenario())
    assert agent.persisted[0] == ["u1"]
    assert agent.persisted[-1] == ["u1", agent.messages[-1].id]
    assert stream_rows(agent) == []  # settled and deleted with the message


def test_sanitize_message_for_persistence_sees_and_returns_typed_messages() -> None:
    class Redacting(Chat):
        def sanitize_message_for_persistence(self, message: UIMessage) -> UIMessage:
            parts = [
                TextPart(text="[redacted]") if isinstance(p, TextPart) else p
                for p in message.parts
            ]
            return UIMessage(id=message.id, role=message.role, parts=parts)

    agent = make(Redacting)

    async def scenario() -> None:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "secret")]))

    run(scenario())
    assert texts(agent) == [("user", "[redacted]"), ("assistant", "[redacted]")]


def test_messages_are_typed_read_only_and_decoded_once() -> None:
    agent = make()

    async def scenario() -> None:
        ws = await connect(agent)
        await send(agent, ws, chat_request("r1", [user("u1", "hi")]))

    run(scenario())
    first, again = agent.messages, agent.messages
    assert isinstance(first[0], UIMessage)
    assert first[0] is again[0]
    assert list(first[0:1]) == [first[0]]
    assert not hasattr(first, "append")


def test_interrupted_tool_calls_are_repaired_before_the_next_turn() -> None:
    agent = make()

    async def scenario() -> None:
        await agent.lifecycle.start()
        await agent._save(
            [
                user("u1", "hi"),
                {
                    "id": "a1",
                    "role": "assistant",
                    "parts": [
                        {
                            "type": "tool-search",
                            "toolCallId": "c1",
                            "state": "input-available",
                            "input": {"q": "x"},
                        }
                    ],
                },
            ]
        )
        await agent.save_messages(
            [
                *agent.messages,
                UIMessage(id="u2", role="user", parts=[TextPart(text="?")]),
            ]
        )

    run(scenario())
    tool = agent.messages[1].parts[0]
    assert tool.state == "output-error"
    assert "interrupted" in tool.error_text


def test_options_must_be_chat_options() -> None:
    from agents import AgentOptions

    class Misconfigured(Chat):
        options = AgentOptions()

    with pytest.raises(TypeError, match="AIChatAgentOptions"):
        make(Misconfigured)


# The producing turn is left hanging on purpose (a dead isolate's).
HUNG: list[asyncio.Future[Any]] = []


def test_a_stream_a_dead_isolate_left_is_replayed_and_its_partial_saved() -> None:
    agent = make()

    async def hangs(_options: ChatMessageOptions) -> AsyncIterator[Any]:
        yield chunks.TextStart(id="t")
        yield chunks.TextDelta(id="t", delta="half")
        yield chunks.TextDelta(id="t", delta=" done")
        yield chunks.ToolOutputAvailable(tool_call_id="c", output=1)  # flushes
        await asyncio.Event().wait()

    agent.reply = hangs

    async def scenario() -> tuple[Any, FakeWebSocket]:
        ws = await connect(agent)
        HUNG.append(
            asyncio.ensure_future(
                send(agent, ws, chat_request("r1", [user("u1", "hi")]))
            )
        )
        await asyncio.sleep(0.01)
        restarted = make(storage=agent.ctx.storage)
        late = await connect(restarted)
        await send(restarted, late, {"type": "cf_agent_stream_resume_ack", "id": "r1"})
        return restarted, late

    restarted, late = run(scenario())
    frames = late.frames()
    assert frames[0] == {"type": "cf_agent_stream_resuming", "id": "r1"}
    replay = [f for f in frames if f["type"] == RESPONSE]
    assert replay[-1] == {
        "body": "",
        "done": True,
        "id": "r1",
        "type": RESPONSE,
        "replay": True,
        "messageIds": ["u1"],
        "outcome": "aborted",
    }
    assert texts(restarted)[-1] == ("assistant", "half done")
    assert restarted.messages[-1].id == json.loads(replay[0]["body"])["messageId"]
