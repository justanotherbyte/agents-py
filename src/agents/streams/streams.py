"""Durable incremental output: the Streams capability.

Port of upstream ``streams/streams.ts`` (``.design/streams_api.md``,
``.design/streams_engine.md``). Each stream is an ordered, durable chunk log
with a monotonic cursor, replay-then-tail reads, and a terminal state. It
needs no alarm, so it works on facets as is. Live fanout is in-isolate: a
Durable Object runs in one isolate at a time, so every concurrent reader
shares the producer's; a reader that outlives the isolate replays from its
cursor when it comes back.
"""

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any, cast, override

from ..core.events import Disposable
from ..core.timing import from_epoch_ms, now_ms
from ..core.types import JSONValue
from ..lifecycle.capability import LifecycleCapability
from .errors import StreamClosedError, StreamNotFoundError, StreamSerializationError
from .store import StreamStore
from .types import StreamChunk, StreamChunkRow, StreamRow, StreamState, StreamStatus

__all__ = ("DEFAULT_MAX_CHUNK_BYTES", "StreamWriter", "Streams")

DEFAULT_MAX_CHUNK_BYTES = 1_048_576
"""The default limit on one serialized chunk (1 MiB)."""

_SCHEMA_VERSION_KEY = "cf_agents:streams_schema_version"
_SCHEMA_VERSION = 1
_MAX_STREAM_ID_LENGTH = 256
_DEFAULT_BATCH_SIZE = 100
_DEFAULT_LIST_LIMIT = 100

type DeleteHook = Callable[[StreamRow, int], None]
"""Sees a stream's row and cursor just before its rows are deleted."""


class StreamWriter:
    """The producer's handle on one stream, from `Streams.open`.

    Appends are synchronous durable writes; once the stream has settled (or
    been deleted), appends raise `StreamClosedError`.
    """

    __slots__ = ("_streams", "stream_id")

    def __init__(self, streams: "Streams", stream_id: str) -> None:
        self._streams = streams
        self.stream_id = stream_id
        """The stream this writer appends to."""

    @property
    def cursor(self) -> int:
        """The next ``seq`` to be assigned (resume a producer from here)."""
        return self._streams._cursor(self.stream_id)

    def append(self, chunk: JSONValue) -> int:
        """Durably append ``chunk`` and wake live readers; return its ``seq``.

        Raises
        ------
        StreamClosedError
            If the stream settled or was deleted.
        StreamSerializationError
            If ``chunk`` isn't JSON or is over the size limit.
        """
        return self._streams._append(self.stream_id, chunk)

    def close(
        self, *, commit: Callable[[], None] | None = None, discard: bool = False
    ) -> None:
        """Settle the stream as ``completed``.

        Does nothing if it's already settled or deleted (``commit`` doesn't
        run then).

        Parameters
        ----------
        commit
            Synchronous writes (e.g. saving the finished message) committed
            in one transaction with the settle. If it raises, nothing is
            committed and the stream stays live.
        discard
            Delete the stream's rows in that same transaction (the chunks
            were handed off).
        """
        self._streams._settle(
            self.stream_id, "completed", None, commit=commit, discard=discard
        )

    def error(
        self,
        reason: str | None = None,
        *,
        commit: Callable[[], None] | None = None,
        discard: bool = False,
    ) -> None:
        """Settle the stream as ``errored``, recording ``reason`` (see `close`)."""
        self._streams._settle(
            self.stream_id, "errored", reason, commit=commit, discard=discard
        )


class Streams(LifecycleCapability):
    """Durable chunk logs with replay-then-tail reads.

    Install it on an agent with ``self.streams = self.use(Streams())``.

    Parameters
    ----------
    max_chunk_bytes
        The limit on one serialized chunk (and on a stream's metadata).
    """

    def __init__(self, *, max_chunk_bytes: int = DEFAULT_MAX_CHUNK_BYTES) -> None:
        super().__init__("streams")
        self._max_chunk_bytes = max_chunk_bytes
        self._store_instance: StreamStore | None = None
        # Readers tailing a live stream, woken (and cleared) by every append,
        # settle, and delete. Wakeups carry no data: readers re-read.
        self._waiters: dict[str, set[asyncio.Future[None]]] = {}
        self._delete_hooks: list[DeleteHook] = []

    @property
    def _store(self) -> StreamStore:
        if self._store_instance is None:
            self._store_instance = StreamStore(self.lifecycle.sql)
        return self._store_instance

    @override
    async def on_start(self) -> None:
        """Create the tables once."""
        storage = self.lifecycle.storage
        if (await storage.get(_SCHEMA_VERSION_KEY) or 0) < _SCHEMA_VERSION:
            self._store.ensure_tables()
            await storage.put(_SCHEMA_VERSION_KEY, _SCHEMA_VERSION)

    # Producing

    async def open(
        self,
        stream_id: str,
        *,
        tag: str | None = None,
        metadata: dict[str, JSONValue] | None = None,
    ) -> StreamWriter:
        """Open a stream for writing, or resume a live one.

        Reopening a live stream returns a writer at its current cursor.

        Parameters
        ----------
        stream_id
            1-256 characters.
        tag
            A lookup key, fixed at creation and not unique: successive streams
            of one operation (a retried turn) share it, and
            ``list(tag=..., limit=1)`` finds the newest.
        metadata
            JSON kept with the stream.

        Raises
        ------
        StreamClosedError
            If the stream has already settled.
        ValueError
            If ``stream_id`` is empty or too long, or the live stream has a
            different tag.
        StreamSerializationError
            If ``metadata`` isn't JSON or is over the size limit.
        """
        await self.lifecycle.ready()
        _validate_stream_id(stream_id)
        existing = self._store.get(stream_id)
        if existing is not None:
            if existing["state"] != "streaming":
                raise StreamClosedError(
                    stream_id, f"already settled as {existing['state']}"
                )
            # The tag is part of the stream's identity: a different one is a
            # conflict, not a resume.
            if tag is not None and tag != existing["tag"]:
                raise ValueError(
                    f"Stream {stream_id!r} is already open with tag "
                    f"{existing['tag']!r}; refusing to reopen it with tag {tag!r}"
                )
            return StreamWriter(self, stream_id)
        self._insert_stream(stream_id, tag, metadata)
        return StreamWriter(self, stream_id)

    # Reading

    async def read(
        self, stream_id: str, *, start: int = 0
    ) -> AsyncIterator[StreamChunk]:
        """Replay chunks from ``start`` (inclusive), then follow live appends.

        Ends once the stream has settled and every chunk has been yielded (an
        ``errored`` stream too: `status` tells the outcome) or it's deleted.
        Cancel the reading task to stop early.

        Raises
        ------
        StreamNotFoundError
            If the stream doesn't exist.
        """
        async for batch in self.read_batches(stream_id, start=start):
            for chunk in batch:
                yield chunk

    async def read_batches(
        self,
        stream_id: str,
        *,
        start: int = 0,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        on_up_to_date: Callable[[], None] | None = None,
    ) -> AsyncIterator[Sequence[StreamChunk]]:
        """`read`, in batches: one per replay page, one per live wakeup.

        A consumer paying per write (a socket send, an RPC hop) pays once per
        backlog rather than once per chunk.

        Parameters
        ----------
        start
            The first ``seq`` to yield.
        batch_size
            The most chunks in one replay batch.
        on_up_to_date
            Called once, synchronously, when the reader first catches up with
            everything stored so far (it may still be live).

        Raises
        ------
        StreamNotFoundError
            If the stream doesn't exist.
        """
        await self.lifecycle.ready()
        store = self._store
        size = max(1, batch_size)
        next_seq = max(0, start)
        caught_up = False
        if store.state(stream_id) is None:
            raise StreamNotFoundError(stream_id)
        while True:
            batch = [
                StreamChunk(seq=seq, chunk=value)
                for seq, value, _ in store.chunks(stream_id, next_seq, size)
            ]
            if batch:
                next_seq = batch[-1].seq + 1
                yield batch
            if len(batch) == size:
                continue
            # A short batch: everything stored until now has been yielded.
            if not caught_up:
                caught_up = True
                if on_up_to_date is not None:
                    on_up_to_date()
                    # It may have appended synchronously, before any waiter
                    # existed to be woken: read again instead of waiting.
                    continue
            state = store.state(stream_id)
            if state is None:
                return  # deleted while reading
            if state != "streaming":
                # Appends happen before the settle (the fence refuses later
                # ones), so an empty read here means nothing is left. A
                # non-empty batch was yielded, letting others run: drain.
                if not batch:
                    return
                continue
            if batch:
                # Appends made while the consumer held the batch woke no one.
                continue
            await self._wait(stream_id)

    async def status(self, stream_id: str) -> StreamStatus | None:
        """Return a stream's state and cursor, or ``None`` if it doesn't exist."""
        await self.lifecycle.ready()
        row = self._store.get(stream_id)
        return self._status(row) if row is not None else None

    async def list(
        self,
        *,
        state: StreamState | Sequence[StreamState] | None = None,
        tag: str | None = None,
        limit: int | None = None,
    ) -> Sequence[StreamStatus]:
        """Return streams newest first (default at most 100)."""
        await self.lifecycle.ready()
        states = (cast(StreamState, state),) if isinstance(state, str) else state
        rows = self._store.list(
            states, tag, limit if limit is not None else _DEFAULT_LIST_LIMIT
        )
        return [self._status(row) for row in rows]

    async def delete(self, stream_id: str) -> bool:
        """Delete a settled stream and its chunks; ``False`` if there's none.

        Raises
        ------
        RuntimeError
            If the stream is still live: settle it first.
        """
        await self.lifecycle.ready()
        row = self._store.get(stream_id)
        if row is None:
            return False
        if row["state"] == "streaming":
            raise RuntimeError(
                f"Can't delete live stream {stream_id!r}; close() or error() it first"
            )
        self._delete_rows(stream_id)
        self._emit("stream:deleted", {"streamId": stream_id})
        return True

    # Synchronous surface (first-party machinery, e.g. chat)
    #
    # Same-isolate and synchronous; it skips lifecycle.ready(), so callers
    # own startup ordering (upstream's __DO_NOT_USE_WILL_BREAK__sync). The
    # public methods are built on the same operations.

    def _ensure_tables(self) -> None:
        self._store.ensure_tables()

    def _get_stream(self, stream_id: str) -> StreamRow | None:
        return self._store.get(stream_id)

    def _insert_stream(
        self, stream_id: str, tag: str | None, metadata: dict[str, JSONValue] | None
    ) -> None:
        # No idempotency here: callers check first.
        _validate_stream_id(stream_id)
        encoded = (
            self._serialize(metadata, f"metadata for stream {stream_id!r}")
            if metadata is not None
            else None
        )
        self._store.insert(stream_id, tag, encoded)
        self._emit("stream:opened", {"streamId": stream_id})

    def _set_metadata(self, stream_id: str, metadata: dict[str, JSONValue]) -> None:
        encoded = self._serialize(metadata, f"metadata for stream {stream_id!r}")
        self._store.set_metadata(stream_id, encoded)

    def _append(self, stream_id: str, chunk: JSONValue) -> int:
        # Serialize before the fence: nothing between the state read and the
        # write may run user code or await, so no settle, delete, or other
        # append can interleave (one synchronous block at a time).
        chunk_json = self._serialize(chunk, f"chunk for stream {stream_id!r}")
        store = self._store
        state = store.state(stream_id)
        if state != "streaming":
            detail = f"already settled as {state}" if state else "it was deleted"
            raise StreamClosedError(stream_id, detail)
        seq = store.write_chunk(stream_id, chunk_json, now_ms())
        self._wake(stream_id)
        return seq

    def _last_chunk_at(self, stream_id: str) -> int | None:
        return self._store.tail(stream_id).last_chunk_at

    def _cursor(self, stream_id: str) -> int:
        return self._store.tail(stream_id).next_seq

    def _on_delete(self, hook: DeleteHook) -> Disposable:
        """Observe every deletion of a stream's rows (synchronous hooks only).

        A cutover's hooks run inside its transaction.
        """
        self._delete_hooks.append(hook)
        return Disposable(lambda: self._delete_hooks.remove(hook))

    def _settle(
        self,
        stream_id: str,
        state: StreamState,
        reason: str | None,
        *,
        commit: Callable[[], None] | None = None,
        discard: bool = False,
    ) -> bool:
        """Settle a live stream; return whether this call ended it.

        With ``commit`` or ``discard``, the settle, ``commit()``, and the
        deletion run in one transaction, and a raising ``commit`` rolls them
        all back. Events and wakeups follow only a committed settle.
        """
        if commit is None and not discard:
            settled = self._store.settle(stream_id, state, reason)
            if settled:
                self._emit_settled(stream_id, state, reason)
            self._wake(stream_id)
            return settled

        outcome = {"settled": False, "deleted": False}

        def cutover() -> None:
            if not self._store.settle(stream_id, state, reason):
                return
            outcome["settled"] = True
            if commit is not None:
                result = commit()
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise TypeError(
                        "A stream cutover's commit must be synchronous; it "
                        "runs inside a SQLite transaction"
                    )
            if discard:
                outcome["deleted"] = self._delete_rows(stream_id)

        self.lifecycle.storage.transactionSync(cutover)
        if outcome["settled"]:
            self._emit_settled(stream_id, state, reason)
        if outcome["deleted"]:
            self._emit("stream:deleted", {"streamId": stream_id})
        self._wake(stream_id)
        return outcome["settled"]

    def _delete_unchecked(self, stream_id: str) -> None:
        """Delete a stream in any state, waking its readers so they end."""
        if self._delete_rows(stream_id):
            self._emit("stream:deleted", {"streamId": stream_id})
        self._wake(stream_id)

    def _delete_many(self, stream_ids: Sequence[str]) -> None:
        """Delete streams in any state, without events."""
        for stream_id in stream_ids:
            self._delete_rows(stream_id)
            self._wake(stream_id)

    def _read_chunks(
        self, stream_id: str, start: int, limit: int
    ) -> Sequence[StreamChunkRow]:
        """Return one page of the log from ``start``, values as JSON text."""
        return [
            StreamChunkRow(
                stream_id=stream_id, seq=seq, chunk=_dumps(value), created_at=at
            )
            for seq, value, at in self._store.chunks(stream_id, start, limit)
        ]

    def _list_rows(self) -> Sequence[StreamRow]:
        return self._store.all_rows()

    def _rows_by_tag(
        self, tag: str, state: StreamState | None = None
    ) -> Sequence[StreamRow]:
        # Tags aren't unique and the table is shared across producers, so
        # callers apply their own ownership check.
        return self._store.rows_by_tag(tag, state)

    # Internals

    def _delete_rows(self, stream_id: str) -> bool:
        # Every deletion passes here, so hooks see each one, with the row and
        # cursor as they were.
        if self._delete_hooks:
            row = self._store.get(stream_id)
            if row is not None:
                cursor = self._cursor(stream_id)
                for hook in list(self._delete_hooks):
                    hook(row, cursor)
        return self._store.delete(stream_id)

    def _serialize(self, value: Any, context: str) -> str:
        try:
            text = _dumps(value)
        except (TypeError, ValueError) as error:
            raise StreamSerializationError(context, str(error)) from error
        # surrogatepass: a lone surrogate (from JS text) is measured, not raised.
        size = len(text.encode("utf-8", "surrogatepass"))
        if size > self._max_chunk_bytes:
            raise StreamSerializationError(
                context,
                f"its {size} bytes are over the {self._max_chunk_bytes}-byte limit",
            )
        return text

    def _status(self, row: StreamRow) -> StreamStatus:
        # A live row's counters are stale by design; derive them from the tail.
        cursor = row["chunk_count"]
        updated_at = row["updated_at"]
        if row["state"] == "streaming":
            tail = self._store.tail(row["stream_id"])
            cursor = tail.next_seq
            if tail.last_chunk_at is not None:
                updated_at = max(updated_at, tail.last_chunk_at)
        return StreamStatus(
            stream_id=row["stream_id"],
            state=row["state"],
            cursor=cursor,
            created_at=from_epoch_ms(row["created_at"]),
            updated_at=from_epoch_ms(updated_at),
            tag=row["tag"],
            metadata=_metadata(row["metadata"]),
            error=row["error_message"],
            closed_at=(
                from_epoch_ms(row["closed_at"])
                if row["closed_at"] is not None
                else None
            ),
        )

    def _wake(self, stream_id: str) -> None:
        for waiter in self._waiters.pop(stream_id, ()):
            if not waiter.done():
                waiter.set_result(None)

    async def _wait(self, stream_id: str) -> None:
        waiters = self._waiters.setdefault(stream_id, set())
        waiter = asyncio.get_running_loop().create_future()
        waiters.add(waiter)
        try:
            await waiter
        finally:
            # A cancelled reader leaves nothing behind; the last one removes
            # the stream's set too.
            waiters.discard(waiter)
            if not waiters and self._waiters.get(stream_id) is waiters:
                del self._waiters[stream_id]

    def _emit_settled(
        self, stream_id: str, state: StreamState, reason: str | None
    ) -> None:
        payload: dict[str, Any] = {"streamId": stream_id}
        if reason is not None:
            payload["reason"] = reason
        self._emit(
            "stream:closed" if state == "completed" else "stream:errored", payload
        )

    def _emit(self, type: str, payload: dict[str, Any]) -> None:
        self.lifecycle.emit(type, payload)


def _validate_stream_id(stream_id: str) -> None:
    if not stream_id:
        raise ValueError("Stream ids must be non-empty strings")
    if len(stream_id) > _MAX_STREAM_ID_LENGTH:
        raise ValueError(f"Stream ids are at most {_MAX_STREAM_ID_LENGTH} characters")


def _dumps(value: Any) -> str:
    # Compact and keeping non-ASCII characters, like JSON.stringify (escaping
    # them would store 2-3x the bytes). NaN and infinities aren't JSON, so
    # they're refused rather than stored as text no other reader could parse.
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _metadata(text: str | None) -> dict[str, JSONValue] | None:
    # Stored metadata is always a JSON object when present.
    return json.loads(text) if text is not None else None
