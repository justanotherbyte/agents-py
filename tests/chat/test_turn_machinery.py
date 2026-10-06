import asyncio
import json
import logging
from typing import Any

import _fake_ffi
import pytest
from fake_runtime import FakeCtx

from agents import AIChatAgent, Debounce
from agents.chat.concurrency import SubmitConcurrencyController
from agents.chat.handshake import PreStreamTurns, ResumeHandshake
from agents.chat.protocol import (
    Cancel,
    ChatRequest,
    Clear,
    Messages,
    StreamResumeAck,
    StreamResumeRequest,
    ToolApproval,
    ToolResult,
    origin_message_ids,
    parse_protocol_message,
    with_origin_message_ids,
)
from agents.chat.resumable_stream import CHUNK_MAX_BYTES, ResumableStream
from agents.chat.terminal import TerminalRecord
from agents.chat.turn_queue import TurnQueue

RESPONSE = "cf_agent_use_chat_response"


def run[T](coro: Any) -> T:
    return asyncio.run(coro)


class FakeConnection:
    def __init__(self, id: str = "c1", *, open_for: int | None = None) -> None:
        self.id = id
        self.sent: list[dict[str, Any]] = []
        self._open_for = open_for

    def send(self, text: str) -> None:
        if self._open_for is not None and len(self.sent) >= self._open_for:
            raise _fake_ffi.JsException("WebSocket send() after close")
        self.sent.append(json.loads(text))


def connection(id: str = "c1", *, open_for: int | None = None) -> Any:
    """A connection stand-in (typed loosely: it isn't a `Connection`)."""
    return FakeConnection(id, open_for=open_for)


# TurnQueue


def test_turns_run_one_at_a_time_in_order() -> None:
    queue = TurnQueue()
    log: list[str] = []

    async def turn(name: str) -> str:
        log.append(f"start {name}")
        assert queue.active_request_id == name
        await asyncio.sleep(0.001)
        log.append(f"end {name}")
        return name

    async def scenario() -> list[Any]:
        results = await asyncio.gather(
            *(queue.enqueue(name, lambda n=name: turn(n)) for name in "abc")
        )
        return [(r.status, r.value) for r in results]

    assert run(scenario()) == [
        ("completed", "a"),
        ("completed", "b"),
        ("completed", "c"),
    ]
    assert log == ["start a", "end a", "start b", "end b", "start c", "end c"]
    assert not queue.is_active


def test_reset_makes_queued_turns_stale() -> None:
    queue = TurnQueue()
    gate = asyncio.Event()
    ran: list[str] = []

    async def first() -> None:
        await gate.wait()
        ran.append("a")

    async def scenario() -> list[str]:
        a = asyncio.ensure_future(queue.enqueue("a", first))
        b = asyncio.ensure_future(queue.enqueue("b", lambda: _record(ran, "b")))
        await asyncio.sleep(0)
        queue.reset()  # a clear while a runs and b waits
        gate.set()
        results = await asyncio.gather(a, b)
        after = await queue.enqueue("c", lambda: _record(ran, "c"))
        return [r.status for r in (*results, after)]

    assert run(scenario()) == ["completed", "stale", "completed"]
    assert ran == ["a", "c"]


def test_a_caller_cancelled_while_waiting_keeps_the_order_behind_it() -> None:
    queue = TurnQueue()
    gate = asyncio.Event()
    ran: list[str] = []

    async def scenario() -> int:
        async def first() -> None:
            await gate.wait()
            ran.append("a")

        a = asyncio.ensure_future(queue.enqueue("a", first))
        b = asyncio.ensure_future(queue.enqueue("b", lambda: _record(ran, "b")))
        c = asyncio.ensure_future(queue.enqueue("c", lambda: _record(ran, "c")))
        await asyncio.sleep(0)
        assert queue.queued_count() == 3
        b.cancel()
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(a, c)
        await queue.wait_for_idle()
        return queue.queued_count()

    assert run(scenario()) == 0
    assert ran == ["a", "c"]


async def _record(log: list[str], name: str) -> None:
    log.append(name)


# Submit concurrency


def test_only_overlapping_user_sends_are_subject_to_the_policy() -> None:
    async def scenario() -> list[Any]:
        controller = SubmitConcurrencyController()
        return [
            controller.decide("drop", is_submit_message=True, queued_turns=0).action,
            controller.decide("drop", is_submit_message=False, queued_turns=1).action,
            controller.decide("drop", is_submit_message=True, queued_turns=1).action,
            controller.decide(
                "queue", is_submit_message=True, queued_turns=1
            ).submit_sequence,
        ]

    assert run(scenario()) == ["execute", "execute", "drop", None]


def test_newer_overlapping_sends_supersede_older_ones() -> None:
    async def scenario() -> tuple[bool, bool, float | None]:
        controller = SubmitConcurrencyController()
        first = controller.decide("latest", is_submit_message=True, queued_turns=1)
        second = controller.decide(
            Debounce(seconds=5), is_submit_message=True, queued_turns=1
        )
        return (
            controller.is_superseded(first.submit_sequence),
            controller.is_superseded(second.submit_sequence),
            second.debounce_until,
        )

    first, second, until = run(scenario())
    assert (first, second) == (True, False)
    assert until is not None


def test_pending_enqueues_count_as_overlap_and_reset_forgets_them() -> None:
    async def scenario() -> list[Any]:
        controller = SubmitConcurrencyController()
        release = controller.begin_enqueue()
        overlapped = controller.decide("drop", is_submit_message=True, queued_turns=0)
        controller.reset()
        later = controller.begin_enqueue()
        release()  # from before the reset: ignored
        count = controller.pending_enqueue_count
        later()
        later()
        return [overlapped.action, count, controller.pending_enqueue_count]

    assert run(scenario()) == ["drop", 1, 0]


def test_a_debounce_wait_ends_early_when_cancelled() -> None:
    async def scenario() -> float:
        controller = SubmitConcurrencyController()
        loop = asyncio.get_running_loop()
        start = loop.time()
        waiting = asyncio.ensure_future(controller.wait_until(start + 10))
        await asyncio.sleep(0.01)
        controller.cancel_active_debounce()
        await waiting
        return loop.time() - start

    assert run(scenario()) < 1


# Protocol


@pytest.mark.parametrize(
    ("frame", "event"),
    [
        (
            {
                "type": "cf_agent_use_chat_request",
                "id": "r",
                "init": {"method": "POST"},
            },
            ChatRequest(id="r", init={"method": "POST"}),
        ),
        ({"type": "cf_agent_chat_clear"}, Clear()),
        ({"type": "cf_agent_chat_request_cancel", "id": "r"}, Cancel(id="r")),
        (
            {"type": "cf_agent_tool_result", "toolCallId": "c", "output": 1},
            ToolResult(tool_call_id="c", tool_name="", output=1),
        ),
        (
            {"type": "cf_agent_tool_approval", "toolCallId": "c", "approved": True},
            ToolApproval(tool_call_id="c", approved=True),
        ),
        (
            {"type": "cf_agent_stream_resume_request", "probeId": 3},
            StreamResumeRequest(),
        ),
        ({"type": "cf_agent_stream_resume_ack", "id": "r"}, StreamResumeAck(id="r")),
        ({"type": "cf_agent_chat_messages"}, Messages()),
    ],
)
def test_chat_frames_parse_into_events(frame: dict[str, Any], event: Any) -> None:
    assert parse_protocol_message(json.dumps(frame)) == event


@pytest.mark.parametrize("raw", ["not json", "[1]", '{"type": "cf_agent_state"}', "{}"])
def test_other_frames_are_not_chat_events(raw: str) -> None:
    assert parse_protocol_message(raw) is None


def test_origin_ids_are_the_trailing_user_messages() -> None:
    messages = [
        {"id": "u0", "role": "user"},
        {"id": "a0", "role": "assistant"},
        {"id": "u1", "role": "user"},
        {"id": "", "role": "user"},
        {"id": "u2", "role": "user"},
    ]
    assert origin_message_ids(messages) == ["u1", "u2"]
    assert origin_message_ids([{"id": "a", "role": "assistant"}]) is None
    assert origin_message_ids("nope") is None
    done = {"done": True, "id": "r"}
    assert with_origin_message_ids(done, ["u1"]) == {**done, "messageIds": ["u1"]}
    assert with_origin_message_ids({"done": False}, ["u1"]) == {"done": False}


# Resume handshake: frames as upstream's frozen fixture
# (chat/__tests__/resume-handshake-frames.ts)


def resuming_frame(request_id: str, probe_id: str | None = None) -> dict[str, Any]:
    return {
        "type": "cf_agent_stream_resuming",
        "id": request_id,
        **({"probeId": probe_id} if probe_id else {}),
    }


def resume_none_frame(probe_id: str | None = None) -> dict[str, Any]:
    return {
        "type": "cf_agent_stream_resume_none",
        "reason": "idle",
        **({"probeId": probe_id} if probe_id else {}),
    }


def pending_frame(
    request_id: str | None, probe_id: str | None = None
) -> dict[str, Any]:
    return {
        "type": "cf_agent_stream_pending",
        **({"id": request_id} if request_id else {}),
        **({"probeId": probe_id} if probe_id else {}),
    }


def replay_done_frame(request_id: str) -> dict[str, Any]:
    return {
        "body": "",
        "done": True,
        "id": request_id,
        "type": RESPONSE,
        "replay": True,
    }


def terminal_error_frame(request_id: str, body: str) -> dict[str, Any]:
    return {
        "body": body,
        "done": True,
        "error": True,
        "id": request_id,
        "type": RESPONSE,
    }


class StreamHost:
    """A started chat agent's resumable stream, driven directly."""

    def __init__(self) -> None:
        self.agent: Any = AIChatAgent(FakeCtx("h"), env=None)
        self.pre_stream = PreStreamTurns()
        self.pending_resume: set[str] = set()
        self.terminal: TerminalRecord | None = None
        self.orphans: list[str] = []
        self.held: set[str] = set()

    async def start(self) -> ResumableStream:
        await self.agent.lifecycle.start()
        return self.agent._stream

    def handshake(self) -> ResumeHandshake:
        async def pending_terminal() -> TerminalRecord | None:
            return self.terminal

        async def persist(stream_id: str) -> None:
            self.orphans.append(stream_id)

        return ResumeHandshake(
            stream=self.agent._stream,
            pre_stream=self.pre_stream,
            pending_resume=self.pending_resume,
            pending_terminal=pending_terminal,
            persist_orphaned_stream=persist,
            holds_terminal_frames=self.held.__contains__,
        )


def test_resume_requests_get_the_fixture_frames_for_each_state() -> None:
    host = StreamHost()

    async def scenario() -> list[list[dict[str, Any]]]:
        stream = await host.start()
        handshake = host.handshake()
        idle, pending, active, terminal = (connection(f"c{i}") for i in range(4))
        await handshake.handle_resume_request(idle, "p0")
        host.pre_stream.begin("r1")
        await handshake.handle_resume_request(pending, "p1")
        stream.start("r1")
        host.pre_stream.flush_on_stream_start(handshake.notify_stream_resuming)
        await handshake.handle_resume_request(active, "p2")
        stream.complete(stream.active_stream_id or "")
        host.terminal = TerminalRecord(request_id="r9", body="boom")
        await handshake.handle_resume_request(terminal)
        return [c.sent for c in (idle, pending, active, terminal)]

    idle, pending, active, terminal = run(scenario())
    assert idle == [resume_none_frame("p0")]
    assert pending == [pending_frame("r1", "p1"), resuming_frame("r1")]
    assert active == [resuming_frame("r1", "p2")]
    assert terminal == [resuming_frame("r9")]
    assert host.pending_resume == {"c1", "c2"}


def test_resume_acks_replay_or_close_the_stream() -> None:
    host = StreamHost()

    async def scenario() -> list[list[dict[str, Any]]]:
        stream = await host.start()
        handshake = host.handshake()
        live = stream.start("r1", origin_message_ids=["u1"])
        stream.store_chunk(live, '{"type":"text-delta","id":"t","delta":"a"}')
        on_live, other_id, after = (
            connection("a"),
            connection("b"),
            connection("c"),
        )
        await handshake.handle_resume_ack(on_live, "r1")
        await handshake.handle_resume_ack(other_id, "r2")
        stream.complete(live)
        host.terminal = TerminalRecord(request_id="r1", body="boom")
        await handshake.handle_resume_ack(after, "r1")
        missing = connection("d")
        await handshake.handle_resume_ack(missing, "gone")
        return [on_live.sent, other_id.sent, after.sent, missing.sent]

    on_live, other_id, after, missing = run(scenario())
    assert [f.get("replay") for f in on_live] == [True, True]
    assert on_live[-1]["replayComplete"] is True
    assert other_id == []
    # The stored terminal (an errored stream would be replayed first).
    assert after == [terminal_error_frame("r1", "boom") | {"messageIds": ["u1"]}]
    assert missing == [replay_done_frame("gone")]


def test_parked_connections_are_released_when_no_stream_starts() -> None:
    turns = PreStreamTurns()
    parked = connection()
    assert turns.park(parked) is False
    turns.begin("r1")
    turns.begin("r2")
    assert turns.park(parked, "p") is True
    assert turns.settle("r1") is False
    assert turns.settle("r2") is True
    turns.release_awaiting()
    assert parked.sent == [
        pending_frame("r2", "p"),
        {"type": "cf_agent_stream_resume_none"},
    ]


# ResumableStream


def test_chunks_are_packed_into_segments_of_ten() -> None:
    host = StreamHost()

    async def scenario() -> tuple[list[str], list[int | None], int]:
        stream = await host.start()
        stream_id = stream.start("r1")
        seqs = [stream.store_chunk(stream_id, f'"{i}"') for i in range(23)]
        stream.flush_buffer()
        segments = host.agent.streams._cursor(stream_id)
        return stream.stream_chunks(stream_id), seqs, segments

    bodies, seqs, segments = run(scenario())
    assert bodies == [f'"{i}"' for i in range(23)]
    assert seqs == list(range(23))
    assert segments == 3  # 10 + 10 + 3


def test_an_oversized_chunk_is_sent_live_but_not_stored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    host = StreamHost()

    async def scenario() -> tuple[int | None, list[str]]:
        stream = await host.start()
        stream_id = stream.start("r1")
        with caplog.at_level(logging.WARNING, logger="agents.chat"):
            seq = stream.store_chunk(stream_id, "x" * (CHUNK_MAX_BYTES + 1))
        return seq, stream.stream_chunks(stream_id)

    seq, bodies = run(scenario())
    assert (seq, bodies) == (None, [])
    assert "Not storing" in caplog.text


def test_a_restarted_request_continues_its_sequence() -> None:
    host = StreamHost()

    async def scenario() -> list[int | None]:
        stream = await host.start()
        first = stream.start("r1")
        stream.store_chunk(first, '"a"')
        stream.store_chunk(first, '"b"')
        stream.mark_error(first)
        second = stream.start("r1")
        return [stream.store_chunk(second, '"c"')]

    assert run(scenario()) == [2]


def test_a_stream_left_streaming_is_restored_and_replayed_as_aborted() -> None:
    host = StreamHost()

    async def scenario() -> tuple[list[dict[str, Any]], str | None, list[str]]:
        stream = await host.start()
        stream_id = stream.start("r1", continuation=True)
        stream.store_chunk(stream_id, '{"type":"text-delta","id":"t","delta":"a"}')
        stream.flush_buffer()
        restarted = ResumableStream(host.agent.streams)
        client = connection()
        orphan = restarted.replay_chunks(client, "r1")
        return (
            client.sent,
            orphan,
            [r["state"] for r in host.agent.sql("SELECT state FROM cf_agents_streams")],
        )

    sent, orphan, states = run(scenario())
    assert orphan is not None
    assert [f.get("continuation") for f in sent] == [True, True]
    assert sent[-1]["done"] is True and sent[-1]["outcome"] == "aborted"
    assert states == ["completed"]


def test_reclaim_deletes_settled_and_long_silent_streams_only() -> None:
    host = StreamHost()

    async def scenario() -> tuple[int, list[str]]:
        stream = await host.start()
        streams = host.agent.streams
        stream.start("live")
        streams._insert_stream("old", "o", {"cfChat": 1})
        streams._settle("old", "completed", None)
        streams._insert_stream("stale", "s", {"cfChat": 1})
        streams._insert_stream("theirs", None, {"other": True})
        # Silent for longer than the retention (the active one is kept).
        host.agent.sql("UPDATE cf_agents_streams SET updated_at = 0")
        count = stream.reclaim()
        return count, sorted(
            r["tag"] or "-" for r in host.agent.sql("SELECT tag FROM cf_agents_streams")
        )

    count, left = run(scenario())
    assert count == 2
    assert left == ["-", "live"]


def test_a_closed_connection_stops_the_replay() -> None:
    host = StreamHost()

    async def scenario() -> tuple[str | None, bool]:
        stream = await host.start()
        stream_id = stream.start("r1")
        for i in range(3):
            stream.store_chunk(stream_id, f'"{i}"')
        result = stream.replay_chunks(connection(open_for=1), "r1")
        return result, stream.has_active_stream()

    assert run(scenario()) == (None, True)
