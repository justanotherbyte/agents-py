import asyncio
import math
from typing import Any

import pytest
from fake_runtime import Host

import agents.streams.store as store_module
from agents.lifecycle import Lifecycle, LifecycleEvent
from agents.streams import (
    StreamClosedError,
    StreamNotFoundError,
    Streams,
    StreamSerializationError,
)


def setup(**options: Any) -> tuple[Lifecycle, Streams, list[LifecycleEvent]]:
    streams = Streams(**options)
    lifecycle = Lifecycle(Host()).use(streams)
    events: list[LifecycleEvent] = []
    lifecycle._set_event_sink(events.append)
    return lifecycle, streams, events


def run(streams: Streams, scenario: Any) -> Any:
    async def main() -> Any:
        await streams.lifecycle.ready()
        return await scenario()

    return asyncio.run(main())


def types(events: list[LifecycleEvent]) -> list[str]:
    return [e.type for e in events if e.type.startswith("stream:")]


async def collect(streams: Streams, stream_id: str, start: int = 0) -> list[Any]:
    return [c.chunk async for c in streams.read(stream_id, start=start)]


# Producing


def test_append_close_and_status() -> None:
    _, streams, events = setup()

    async def scenario() -> list[Any]:
        writer = await streams.open("s", tag="t", metadata={"m": 1})
        seqs = [writer.append({"n": i}) for i in range(3)]
        live = await streams.status("s")
        writer.close()
        done = await streams.status("s")
        return [seqs, writer.cursor, live, done]

    seqs, cursor, live, done = run(streams, scenario)
    assert seqs == [0, 1, 2] and cursor == 3
    assert (live.state, live.cursor, live.tag, live.metadata) == (
        "streaming",
        3,
        "t",
        {"m": 1},
    )
    assert (done.state, done.cursor) == ("completed", 3)
    assert done.closed_at is not None and done.updated_at >= done.created_at
    assert types(events) == ["stream:opened", "stream:closed"]


def test_open_resumes_a_live_stream_and_refuses_a_settled_one() -> None:
    _, streams, _ = setup()

    async def scenario() -> None:
        first = await streams.open("s", tag="t")
        first.append("a")
        again = await streams.open("s")
        assert again.cursor == 1
        with pytest.raises(ValueError, match="already open with tag"):
            await streams.open("s", tag="other")
        again.error("boom")
        with pytest.raises(StreamClosedError, match="already settled as errored"):
            await streams.open("s")
        with pytest.raises(StreamClosedError):
            first.append("late")
        with pytest.raises(ValueError, match="non-empty"):
            await streams.open("")
        with pytest.raises(ValueError, match="at most 256"):
            await streams.open("x" * 257)

    run(streams, scenario)


def test_settling_is_idempotent() -> None:
    _, streams, events = setup()

    async def scenario() -> Any:
        writer = await streams.open("s")
        writer.close()
        writer.close()
        writer.error("ignored")
        return await streams.status("s")

    status = run(streams, scenario)
    assert (status.state, status.error) == ("completed", None)
    assert types(events) == ["stream:opened", "stream:closed"]


def test_bad_chunks_are_refused() -> None:
    _, streams, _ = setup(max_chunk_bytes=20)

    async def scenario() -> int:
        writer = await streams.open("s")
        writer.append(None)  # JSON null is a chunk
        for bad in ({1, 2}, math.nan, "x" * 30):
            with pytest.raises(StreamSerializationError):
                writer.append(bad)  # ty: ignore[invalid-argument-type]
        with pytest.raises(StreamSerializationError, match="metadata"):
            await streams.open("t", metadata={"big": "y" * 30})
        return writer.cursor

    assert run(streams, scenario) == 1


def test_chunks_pack_into_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(store_module, "BLOCK_MAX_CHARS", 20)
    _, streams, _ = setup()

    async def scenario() -> list[Any]:
        writer = await streams.open("s")
        for i in range(10):
            writer.append(f"chunk{i}")  # 8 characters each
        writer.close()
        blocks = streams.lifecycle.sql(
            "SELECT block, seq_from, seq_to FROM cf_agents_stream_blocks ORDER BY block"
        )
        return [
            [(b["seq_from"], b["seq_to"]) for b in blocks],
            await collect(streams, "s", start=3),
        ]

    blocks, tail = run(streams, scenario)
    assert blocks == [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10)]
    assert tail == [f"chunk{i}" for i in range(3, 10)]


def test_non_ascii_text_is_stored_as_is() -> None:
    _, streams, _ = setup(max_chunk_bytes=14)

    async def scenario() -> list[Any]:
        writer = await streams.open("s")
        writer.append("héllo 🌍")  # 13 bytes of JSON text, quotes included
        with pytest.raises(StreamSerializationError):
            writer.append("🌍🌍🌍🌍")  # 18 bytes (escaped, it'd be 50)
        writer.close()
        body = streams.lifecycle.sql("SELECT body FROM cf_agents_stream_blocks")
        return [body[0]["body"], await collect(streams, "s")]

    assert run(streams, scenario) == ['"héllo 🌍"', ["héllo 🌍"]]


# Reading


def test_read_replays_then_follows_until_settled() -> None:
    _, streams, _ = setup()

    async def scenario() -> list[Any]:
        writer = await streams.open("s")
        writer.append(0)
        reader = asyncio.ensure_future(collect(streams, "s"))
        for i in range(1, 4):
            await asyncio.sleep(0.005)
            writer.append(i)
        writer.close()
        return await asyncio.wait_for(reader, 1)

    assert run(streams, scenario) == [0, 1, 2, 3]


def test_read_batches_groups_a_backlog_and_signals_up_to_date() -> None:
    _, streams, _ = setup()
    signals: list[int] = []

    async def scenario() -> list[list[Any]]:
        writer = await streams.open("s")
        for i in range(5):
            writer.append(i)
        batches: list[list[Any]] = []

        def up_to_date() -> None:
            signals.append(len(batches))
            # Appending from the callback must not be a lost wakeup.
            writer.append(99)

        async def consume() -> None:
            async for batch in streams.read_batches(
                "s", batch_size=2, on_up_to_date=up_to_date
            ):
                batches.append([c.chunk for c in batch])
                if 99 in batches[-1]:
                    for i in range(5, 8):
                        writer.append(i)
                    writer.close()

        await asyncio.wait_for(consume(), 1)
        return batches

    batches = run(streams, scenario)
    assert batches[:3] == [[0, 1], [2, 3], [4]]
    assert batches[3] == [99] and batches[4] == [5, 6]
    assert [n for batch in batches for n in batch] == [0, 1, 2, 3, 4, 99, 5, 6, 7]
    assert signals == [3]


def test_reading_an_errored_stream_yields_its_chunks() -> None:
    _, streams, _ = setup()

    async def scenario() -> list[Any]:
        writer = await streams.open("s")
        writer.append("a")
        writer.error("failed")
        status = await streams.status("s")
        return [await collect(streams, "s"), status.error if status else None]

    assert run(streams, scenario) == [["a"], "failed"]


def test_reading_a_missing_stream_raises_and_deletion_ends_a_reader() -> None:
    _, streams, _ = setup()

    async def scenario() -> list[Any]:
        with pytest.raises(StreamNotFoundError):
            await collect(streams, "nope")
        writer = await streams.open("s")
        writer.append(1)
        reader = asyncio.ensure_future(collect(streams, "s"))
        await asyncio.sleep(0.01)
        streams._delete_unchecked("s")
        return await asyncio.wait_for(reader, 1)

    assert run(streams, scenario) == [1]


def test_a_cancelled_reader_leaves_no_waiter() -> None:
    _, streams, _ = setup()

    async def scenario() -> dict[str, Any]:
        await streams.open("s")
        reader = asyncio.ensure_future(collect(streams, "s"))
        await asyncio.sleep(0.01)
        assert "s" in streams._waiters
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        return streams._waiters

    assert run(streams, scenario) == {}


# Status, list, delete


def test_list_filters_newest_first() -> None:
    _, streams, _ = setup()

    async def scenario() -> list[Any]:
        for i in range(4):
            writer = await streams.open(f"s{i}", tag="even" if i % 2 == 0 else "odd")
            if i < 2:
                writer.close()
            await asyncio.sleep(0.002)
        everything = [s.stream_id for s in await streams.list()]
        live = [s.stream_id for s in await streams.list(state="streaming")]
        newest_even = [s.stream_id for s in await streams.list(tag="even", limit=1)]
        settled = [
            s.stream_id for s in await streams.list(state=["completed", "errored"])
        ]
        return [everything, live, newest_even, settled]

    assert run(streams, scenario) == [
        ["s3", "s2", "s1", "s0"],
        ["s3", "s2"],
        ["s2"],
        ["s1", "s0"],
    ]


def test_delete_only_settled_streams() -> None:
    _, streams, events = setup()

    async def scenario() -> list[Any]:
        writer = await streams.open("s")
        with pytest.raises(RuntimeError, match="live stream"):
            await streams.delete("s")
        writer.close()
        return [
            await streams.delete("s"),
            await streams.delete("s"),
            await streams.status("s"),
        ]

    assert run(streams, scenario) == [True, False, None]
    assert types(events)[-1] == "stream:deleted"


# The cutover


def test_the_cutover_commits_and_discards_together() -> None:
    lifecycle, streams, events = setup()
    seen: list[tuple[str, int]] = []

    async def scenario() -> list[Any]:
        lifecycle.sql("CREATE TABLE saved (body TEXT)")
        streams._on_delete(lambda row, cursor: seen.append((row["stream_id"], cursor)))
        writer = await streams.open("s")
        writer.append("hello")

        def save() -> None:
            lifecycle.sql("INSERT INTO saved VALUES ('hello')")

        writer.close(commit=save, discard=True)
        return [
            await streams.status("s"),
            list(lifecycle.sql("SELECT body FROM saved")),
        ]

    status, saved = run(streams, scenario)
    assert status is None and saved == [{"body": "hello"}]
    assert seen == [("s", 1)]
    assert types(events) == ["stream:opened", "stream:closed", "stream:deleted"]


def test_a_failing_commit_rolls_the_settle_back() -> None:
    lifecycle, streams, events = setup()

    async def scenario() -> list[Any]:
        lifecycle.sql("CREATE TABLE saved (body TEXT)")
        writer = await streams.open("s")

        def save_then_fail() -> None:
            lifecycle.sql("INSERT INTO saved VALUES ('x')")
            raise KeyError("nope")

        async def not_sync() -> None:
            pass

        with pytest.raises(KeyError):
            writer.close(commit=save_then_fail, discard=True)
        with pytest.raises(TypeError, match="synchronous"):
            writer.close(commit=not_sync)  # ty: ignore[invalid-argument-type]
        status = await streams.status("s")
        writer.append("still live")
        return [
            status.state if status else None,
            list(lifecycle.sql("SELECT * FROM saved")),
        ]

    assert run(streams, scenario) == ["streaming", []]
    assert types(events) == ["stream:opened"]


def test_settling_a_settled_stream_does_not_commit() -> None:
    _, streams, _ = setup()
    commits: list[int] = []

    async def scenario() -> Any:
        writer = await streams.open("s")
        writer.close()
        writer.close(commit=lambda: commits.append(1), discard=True)
        return await streams.status("s")

    status = run(streams, scenario)
    assert commits == [] and status is not None


# The synchronous surface


def test_the_synchronous_surface() -> None:
    _, streams, _ = setup()

    async def scenario() -> list[Any]:
        streams._insert_stream("c1", "turn", {"cfChat": True})
        streams._insert_stream("c2", "turn", None)
        seq = streams._append("c1", {"part": 1})
        streams._set_metadata("c2", {"x": 2})
        page = streams._read_chunks("c1", 0, 10)
        rows = [r["stream_id"] for r in streams._rows_by_tag("turn")]
        live = [r["stream_id"] for r in streams._rows_by_tag("turn", "streaming")]
        settled = streams._settle("c1", "completed", None)
        hook = streams._on_delete(lambda row, cursor: None)
        hook.dispose()
        streams._delete_many(["c1", "c2"])
        return [seq, page, rows, live, settled, streams._list_rows()]

    seq, page, rows, live, settled, remaining = run(streams, scenario)
    assert seq == 0 and settled
    assert [(r["seq"], r["chunk"]) for r in page] == [(0, '{"part":1}')]
    assert rows == ["c2", "c1"] and live == ["c2", "c1"]
    assert remaining == []
