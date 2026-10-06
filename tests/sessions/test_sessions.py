import asyncio
from functools import partial
from typing import Any

import pytest
from fake_runtime import Host

import agents.sessions.sessions as sessions_module
from agents.lifecycle import Lifecycle, LifecycleEvent
from agents.sessions import (
    AppendEvent,
    ClearEvent,
    DeleteEvent,
    SessionChangeEvent,
    SessionMessage,
    Sessions,
    UpdateEvent,
)
from agents.sessions.chunking import split_content
from agents.streams import Streams


def setup(**options: Any) -> tuple[Lifecycle, Sessions, list[LifecycleEvent]]:
    sessions = Sessions(**options)
    lifecycle = Lifecycle(Host()).use(sessions)
    events: list[LifecycleEvent] = []
    lifecycle._set_event_sink(events.append)
    return lifecycle, sessions, events


def run(sessions: Sessions, scenario: Any) -> Any:
    async def main() -> Any:
        await sessions.lifecycle.ready()
        return await scenario()

    return asyncio.run(main())


def msg(
    id: str, text: str = "", role: str = "user", metadata: Any = None
) -> SessionMessage:
    message: SessionMessage = {
        "id": id,
        "role": role,
        "parts": [{"type": "text", "text": text or id}],
    }
    if metadata is not None:
        message["metadata"] = metadata
    return message


def ids(messages: list[SessionMessage]) -> list[str]:
    return [m["id"] for m in messages]


def lifecycle_types(events: list[LifecycleEvent]) -> list[str]:
    return [e.type for e in events if e.type.startswith("session:")]


# Writes


def test_append_is_ordered_and_idempotent() -> None:
    _, sessions, events = setup()
    feed: list[SessionChangeEvent] = []

    async def scenario() -> list[Any]:
        sessions.subscribe(feed.append)
        session = sessions.session()
        first = await session.append_message(msg("a", "hello"))
        await session.append_message(msg("b"))
        again = await session.append_message(msg("a", "different"))
        return [first, again, await session.get_history()]

    first, again, history = run(sessions, scenario)
    assert first.inserted and not again.inserted
    assert again.message["parts"] == [
        {"type": "text", "text": "hello"}
    ]  # the stored one
    assert ids(history) == ["a", "b"]
    assert [(type(e).__name__, getattr(e, "inserted", None)) for e in feed] == [
        ("AppendEvent", True),
        ("AppendEvent", True),
        ("AppendEvent", False),
    ]
    assert lifecycle_types(events) == ["session:message:appended"] * 2


def test_upsert_and_update() -> None:
    _, sessions, events = setup()
    feed: list[SessionChangeEvent] = []

    async def scenario() -> list[Any]:
        sessions.subscribe(feed.append)
        session = sessions.session()
        inserted = await session.upsert_message(msg("a", "v1"))
        updated = await session.upsert_message(msg("a", "v2"))
        unchanged = await session.update_message(msg("a", "v2"))
        missing = await session.update_message(msg("nope"))
        return [
            inserted.inserted,
            updated.inserted,
            unchanged,
            missing,
            await session.get_history(),
        ]

    inserted, updated, unchanged, missing, history = run(sessions, scenario)
    assert inserted and not updated and missing is None
    assert unchanged is not None and unchanged["parts"][0]["text"] == "v2"
    assert history[0]["parts"][0]["text"] == "v2"
    # The identical update sent no event and wrote nothing.
    assert [type(e).__name__ for e in feed] == ["AppendEvent", "UpdateEvent"]
    assert lifecycle_types(events) == [
        "session:message:appended",
        "session:message:updated",
    ]


def test_client_writes_lose_reserved_metadata() -> None:
    _, sessions, _ = setup(reserved_metadata_keys=["billing"])

    async def scenario() -> list[Any]:
        session = sessions.session()
        await session.append_message(
            msg("a", metadata={"billing": 1, "x": 2}), source="client"
        )
        await session.append_message(msg("b", metadata={"billing": 1}), source="client")
        await session.append_message(msg("c", metadata={"billing": 1}))
        return await session.get_history()

    a, b, c = run(sessions, scenario)
    assert a["metadata"] == {"x": 2} and "metadata" not in b
    assert c["metadata"] == {"billing": 1}  # server writes keep it


def test_large_messages_span_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sessions_module, "split_content", partial(split_content, budget=40)
    )
    lifecycle, sessions, _ = setup()
    big = "🌍" * 30 + "x" * 50

    async def scenario() -> list[Any]:
        session = sessions.session()
        await session.append_message(msg("big", big))
        chunks_before = lifecycle.sql(
            "SELECT COUNT(*) AS n FROM cf_agents_session_message_chunks"
        )
        stats = await session.get_history_row_stats()
        await session.update_message(msg("big", "small now"))
        chunks_after = lifecycle.sql(
            "SELECT COUNT(*) AS n FROM cf_agents_session_message_chunks"
        )
        return [
            chunks_before[0]["n"],
            stats,
            chunks_after[0]["n"],
            await session.get_history(),
        ]

    before, stats, after, history = run(sessions, scenario)
    stored = sessions_module._dumps(msg("big", big))
    assert before == len(split_content(stored, 40)) - 1 > 0
    assert stats[0].bytes == len(stored.encode())
    small = sessions_module._dumps(msg("big", "small now"))
    # The shrunk message dropped the continuations it no longer needs.
    assert after == len(split_content(small, 40)) - 1 < before
    assert history[0]["parts"][0]["text"] == "small now"


def test_delete_keeps_the_chain_connected() -> None:
    lifecycle, sessions, _ = setup()

    async def scenario() -> list[Any]:
        session = sessions.session()
        for id in "abcd":
            await session.append_message(msg(id))
        await session.delete_messages(["b", "c", "b"])
        middle = ids(await session.get_history())
        await session.delete_messages(["d"])  # the leaf
        await session.append_message(msg("e"))
        parents = {
            r["id"]: r["parent_id"]
            for r in lifecycle.sql(
                "SELECT id, parent_id FROM cf_agents_session_messages"
            )
        }
        return [middle, ids(await session.get_history()), parents]

    middle, final, parents = run(sessions, scenario)
    assert middle == ["a", "d"] and final == ["a", "e"]
    assert parents == {"a": None, "e": "a"}


def test_clear_starts_over() -> None:
    _, sessions, _ = setup()

    async def scenario() -> list[Any]:
        session = sessions.session()
        await session.append_message(msg("a"))
        await session.clear_messages()
        empty = await session.get_history()
        await session.append_message(msg("b"))
        return [empty, ids(await session.get_history())]

    assert run(sessions, scenario) == [[], ["b"]]


def test_sessions_are_separate() -> None:
    _, sessions, _ = setup()

    async def scenario() -> list[Any]:
        await sessions.session("x").append_message(msg("a"))
        await sessions.session("y").append_message(msg("a"))
        await sessions.session("y").append_message(msg("b"))
        assert sessions.session("x") is sessions.session("x")
        return [ids(await sessions.session(s).get_history()) for s in "xy"]

    assert run(sessions, scenario) == [["a"], ["a", "b"]]


# Reads


def test_recent_history_fits_the_budget_and_keeps_the_leaf() -> None:
    _, sessions, _ = setup()

    async def scenario() -> list[Any]:
        session = sessions.session()
        for i in range(5):
            await session.append_message(msg(f"m{i}", "x" * 100))
        stats = await session.get_history_row_stats()
        size = stats[0].bytes
        two = await session.get_recent_history(size * 2 + 1)
        tiny = await session.get_recent_history(1)
        everything = await session.get_recent_history(10**9)
        return [size, two, tiny, everything]

    size, two, tiny, everything = run(sessions, scenario)
    assert ids(two.messages) == ["m3", "m4"] and two.truncated
    assert ids(tiny.messages) == ["m4"] and tiny.truncated
    assert len(everything.messages) == 5 and not everything.truncated
    assert two.total_content_bytes == size * 5


def test_history_batches_by_count_and_bytes() -> None:
    _, sessions, _ = setup()

    async def scenario() -> list[Any]:
        session = sessions.session()
        for i in range(5):
            await session.append_message(msg(f"m{i}", "x" * 100))
        by_count = [ids(list(b)) async for b in session.history_batches(batch_size=2)]
        by_bytes = [
            ids(list(b)) async for b in session.history_batches(max_batch_bytes=350)
        ]
        return [by_count, by_bytes]

    by_count, by_bytes = run(sessions, scenario)
    assert by_count == [["m0", "m1"], ["m2", "m3"], ["m4"]]
    assert by_bytes == [["m0", "m1"], ["m2", "m3"], ["m4"]]  # ~150 bytes each


def test_unparseable_rows_are_skipped() -> None:
    lifecycle, sessions, _ = setup()

    async def scenario() -> list[str]:
        session = sessions.session()
        await session.append_message(msg("a"))
        await session.append_message(msg("b"))
        lifecycle.sql(
            "UPDATE cf_agents_session_messages SET content = '{bad' WHERE id = 'a'"
        )
        return ids(await session.get_history())

    assert run(sessions, scenario) == ["b"]


# The change feed


def test_mirror_follows_writes() -> None:
    _, sessions, _ = setup()
    cache: list[SessionMessage] = []

    def replace(items: list[SessionMessage]) -> None:
        cache[:] = items

    async def scenario() -> list[Any]:
        session = sessions.session()
        session.mirror(get=lambda: cache, set=replace)
        await sessions.session("other").append_message(msg("elsewhere"))
        for id in "abc":
            await session.append_message(msg(id))
        await session.append_message(msg("a", "ignored"))  # exists: no change
        await session.update_message(msg("b", "edited"))
        await session.delete_messages(["a"])
        snapshot = [(m["id"], m["parts"][0]["text"]) for m in cache]
        cache.pop()  # the host's window no longer holds "c"
        await session.update_message(msg("c", "not mirrored"))
        held = ids(cache)
        await session.clear_messages()
        return [snapshot, held, list(cache)]

    snapshot, held, cleared = run(sessions, scenario)
    assert snapshot == [("b", "edited"), ("c", "c")]
    assert held == ["b"] and cleared == []


def test_mirror_transforms_and_matches_by_attribute() -> None:
    _, sessions, _ = setup()

    class Message:
        def __init__(self, stored: SessionMessage) -> None:
            self.id = stored["id"]
            self.text = str(stored["parts"][0]["text"])

    cache: list[Message] = []

    async def scenario() -> list[str]:
        session = sessions.session()
        session.mirror(get=lambda: cache, set=lambda items: None, transform=Message)
        await session.append_message(msg("a"))
        await session.update_message(msg("a", "new"))
        return [m.text for m in cache]

    assert run(sessions, scenario) == ["new"]


def test_a_failing_listener_is_reported_and_isolated() -> None:
    _, sessions, events = setup()
    seen: list[str] = []

    async def scenario() -> list[str]:
        def broken(event: SessionChangeEvent) -> None:
            raise RuntimeError("listener broke")

        async def working(event: SessionChangeEvent) -> None:
            seen.append(event.type)

        sessions.subscribe(broken)
        sessions.subscribe(working)
        await sessions.session().append_message(msg("a"))
        return ids(await sessions.session().get_history())

    assert run(sessions, scenario) == ["a"]
    assert seen == ["append"]
    error = next(e for e in events if e.type == "session:error")
    assert error.payload == {
        "sessionId": "",
        "event": "append",
        "error": "listener broke",
    }


# The synchronous upsert inside a Streams cutover


def test_the_synchronous_upsert_lands_in_a_stream_cutover() -> None:
    sessions = Sessions()
    streams = Streams()
    lifecycle = Lifecycle(Host()).use(streams).use(sessions)
    feed: list[SessionChangeEvent] = []

    async def scenario() -> list[Any]:
        await lifecycle.start()
        sessions.subscribe(feed.append)
        session = sessions.session()
        await session.append_message(msg("q", "question"))

        # A turn whose commit fails: nothing lands, and the cache is reset.
        failing = await streams.open("turn1")
        afters: list[Any] = []

        def commit_then_fail() -> None:
            afters.append(session._upsert_sync(msg("r1", "lost", role="assistant"))[1])
            raise KeyError("rolled back")

        with pytest.raises(KeyError):
            failing.close(commit=commit_then_fail, discard=True)
        session._abandon()
        after_failure = ids(await session.get_history())

        # A turn that commits: the reply and the stream's deletion land together.
        writer = await streams.open("turn2")
        writer.append("streamed reply")
        afters.clear()
        writer.close(
            commit=lambda: afters.append(
                session._upsert_sync(msg("r2", "reply", role="assistant"))[1]
            ),
            discard=True,
        )
        assert [type(e).__name__ for e in feed] == ["AppendEvent"]  # not yet
        for after in afters:
            await after()
        return [
            after_failure,
            ids(await session.get_history()),
            await streams.status("turn2"),
            [type(e).__name__ for e in feed],
        ]

    after_failure, history, stream, events = asyncio.run(scenario())
    assert after_failure == ["q"]
    assert history == ["q", "r2"]
    assert stream is None
    assert events == ["AppendEvent", "AppendEvent"]


def test_events_are_frozen_dataclasses() -> None:
    event = DeleteEvent(session_id="", message_ids=["a"])
    assert event.type == "delete"
    assert ClearEvent(session_id="").type == "clear"
    assert UpdateEvent(session_id="", message=msg("a")).type == "update"
    assert AppendEvent(session_id="", message=msg("a"), inserted=True).type == "append"
