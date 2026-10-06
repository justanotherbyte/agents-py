"""Durable conversation history: the Sessions capability.

Port of the phase-1 surface of upstream ``sessions/`` (``sessions.ts``,
``handle.ts``, ``core.ts``, ``mirror.ts``; ``.design/sessions_api.md``,
``.design/sessions_engine.md``). A session is a chain of messages in wire
form; one too large for a row is split across continuation rows. Reads walk
the path first without content, then fetch it in bounded windows. Every
write is followed by the change feed, which hosts mirror into memory. It
needs no alarm, so it works on facets as is.
"""

import hashlib
import inspect
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from functools import partial
from typing import Any, Literal, cast, override

from ..core.events import Disposable, Emitter
from ..lifecycle.capability import LifecycleCapability
from .chunking import split_content
from .sanitize import byte_length, sanitize_message
from .store import MAX_PATH_DEPTH, PathRow, SessionStore, Tail
from .tokens import estimate_row_tokens
from .types import (
    AppendEvent,
    AppendResult,
    ClearEvent,
    DeleteEvent,
    RecentHistoryResult,
    SessionChangeEvent,
    SessionChangeListener,
    SessionMessage,
    SessionRowStat,
    Source,
    UpdateEvent,
)

__all__ = ("Session", "Sessions")

_log = logging.getLogger("agents.sessions")

_SCHEMA_VERSION_KEY = "cf_agents:sessions_schema_version"
_SCHEMA_VERSION = 1
# Content is fetched in windows of at most this many rows and bytes: workerd
# SQLite shares the isolate's memory, so an unbounded read can fail.
_WINDOW_ROWS = 50
_WINDOW_BYTES = 4 * 1024 * 1024

type _UpdateOutcome = Literal["missing", "unchanged", "updated"]


class Sessions(LifecycleCapability):
    """Durable conversation history for a Durable Object.

    Install it with ``self.sessions = self.use(Sessions())``; one conversation
    per object uses the default `session`.

    Parameters
    ----------
    reserved_metadata_keys
        Metadata keys only the server may set: ``source="client"`` writes
        lose them.
    """

    def __init__(self, *, reserved_metadata_keys: Sequence[str] = ()) -> None:
        super().__init__("sessions")
        self._reserved = tuple(reserved_metadata_keys)
        self._handles: dict[str, Session] = {}
        self._feed = Emitter[SessionChangeEvent]()
        self._store_instance: SessionStore | None = None
        # Per session: the newest row and the next seq, read once per wake.
        self._tails: dict[str, Tail] = {}

    @property
    def _store(self) -> SessionStore:
        if self._store_instance is None:
            self._store_instance = SessionStore(self.lifecycle.sql)
        return self._store_instance

    @override
    async def on_start(self) -> None:
        """Create the tables once."""
        storage = self.lifecycle.storage
        if (await storage.get(_SCHEMA_VERSION_KEY) or 0) < _SCHEMA_VERSION:
            self._store.ensure_tables()
            await storage.put(_SCHEMA_VERSION_KEY, _SCHEMA_VERSION)

    def session(self, session_id: str = "") -> "Session":
        """Return the (cached) handle for one session; ``""`` is the default."""
        handle = self._handles.get(session_id)
        if handle is None:
            handle = self._handles[session_id] = Session(self, session_id)
        return handle

    def subscribe(self, listener: SessionChangeListener) -> Disposable:
        """Follow every committed write, in order, across all sessions.

        A listener that raises is logged and reported as ``session:error``;
        the write still succeeded, and the other listeners still run.
        """

        async def guarded(event: SessionChangeEvent) -> None:
            try:
                result = listener(event)
                if inspect.isawaitable(result):
                    await result
            except Exception as error:
                _log.warning("A session change listener failed", exc_info=True)
                self.lifecycle.emit(
                    "session:error",
                    {
                        "sessionId": event.session_id,
                        "event": event.type,
                        "error": str(error),
                    },
                )

        return self._feed.subscribe(guarded)

    # Writes (synchronous; the handle runs the feed afterwards)

    def _prepare(
        self, message: SessionMessage, source: Source
    ) -> tuple[SessionMessage, int]:
        prepared = sanitize_message(message)
        if source == "client":
            prepared = self._strip_reserved(prepared)
        return prepared, estimate_row_tokens(prepared)

    def _strip_reserved(self, message: SessionMessage) -> SessionMessage:
        metadata = message.get("metadata")
        if not self._reserved or not isinstance(metadata, dict):
            return message
        kept = {k: v for k, v in metadata.items() if k not in self._reserved}
        if len(kept) == len(metadata):
            return message
        stripped: SessionMessage = {
            "id": message["id"],
            "role": message["role"],
            "parts": message["parts"],
        }
        if kept:
            stripped["metadata"] = kept
        return stripped

    def _append(
        self, session_id: str, message: SessionMessage, token_estimate: int
    ) -> AppendResult:
        store = self._store
        id = message["id"]
        # A repeated append is answered from storage; a fresh id costs only a
        # key probe.
        if store.exists(session_id, id):
            existing = self._read_message(session_id, id)
            if existing is not None:
                return AppendResult(inserted=False, message=existing)
        tail = self._tail(session_id)
        text = _dumps(message)
        insert = partial(
            store.insert,
            session_id,
            id,
            seq=tail.next_seq,
            parent_id=tail.leaf_id,
            role=message["role"],
            slices=split_content(text),
            token_estimate=token_estimate,
            content_hash=_digest(text),
        )
        self.lifecycle.storage.transactionSync(insert)
        self._tails[session_id] = Tail(leaf_id=id, next_seq=tail.next_seq + 1)
        self.lifecycle.emit(
            "session:message:appended",
            {"sessionId": session_id, "messageId": id, "tokenEstimate": token_estimate},
        )
        return AppendResult(inserted=True, message=message)

    def _update(
        self, session_id: str, message: SessionMessage, token_estimate: int
    ) -> _UpdateOutcome:
        store = self._store
        id = message["id"]
        # Key columns only: the stored content stays in SQLite.
        old = store.key_columns(session_id, id)
        if old is None:
            return "missing"
        text = _dumps(message)
        digest = _digest(text)
        if old["content_hash"] == digest:
            return "unchanged"
        rewrite = partial(
            store.rewrite,
            session_id,
            id,
            role=message["role"],
            slices=split_content(text),
            token_estimate=token_estimate,
            content_hash=digest,
            old_chunks=old["content_chunks"],
        )
        self.lifecycle.storage.transactionSync(rewrite)
        self.lifecycle.emit(
            "session:message:updated", {"sessionId": session_id, "messageId": id}
        )
        return "updated"

    def _delete(self, session_id: str, message_ids: Sequence[str]) -> None:
        ids = list(dict.fromkeys(message_ids))
        if not ids:
            return
        self.lifecycle.storage.transactionSync(
            partial(self._store.delete, session_id, ids)
        )
        # The leaf may be gone; read it again on the next write.
        self._tails.pop(session_id, None)
        self.lifecycle.emit(
            "session:messages:deleted", {"sessionId": session_id, "count": len(ids)}
        )

    def _clear(self, session_id: str) -> None:
        self.lifecycle.storage.transactionSync(partial(self._store.clear, session_id))
        self._tails[session_id] = Tail(leaf_id=None, next_seq=1)
        self.lifecycle.emit("session:cleared", {"sessionId": session_id})

    # Reads

    def _tail(self, session_id: str) -> Tail:
        tail = self._tails.get(session_id)
        if tail is None:
            tail = self._tails[session_id] = self._store.tail(session_id)
        return tail

    def _path(self, session_id: str) -> list[PathRow]:
        leaf = self._tail(session_id).leaf_id
        return self._store.path(session_id, leaf) if leaf is not None else []

    def _read_message(self, session_id: str, id: str) -> SessionMessage | None:
        text = self._store.content(session_id, id)
        return _parse(text) if text is not None else None

    async def _messages(
        self, session_id: str, rows: Sequence[PathRow]
    ) -> AsyncIterator[SessionMessage]:
        for window in _windows(rows):
            contents = self._store.contents(session_id, [row.id for row in window])
            for row in window:
                text = contents.get(row.id)
                message = _parse(text) if text is not None else None
                if message is not None:
                    yield message


class Session:
    """One conversation: its reads, writes, and change feed.

    Get it from `Sessions.session`. Messages form a chain: each is appended
    after the newest, and reads return the chain oldest first.
    """

    __slots__ = ("_sessions", "session_id")

    def __init__(self, sessions: Sessions, session_id: str) -> None:
        self._sessions = sessions
        self.session_id = session_id
        """This session's id (``""`` for the default)."""

    # Writes

    async def append_message(
        self, message: SessionMessage, *, source: Source = "server"
    ) -> AppendResult:
        """Append ``message`` after the newest one.

        An id already stored writes nothing and returns the stored message
        with ``inserted=False``.

        Raises
        ------
        TypeError, ValueError
            If ``message`` isn't JSON (e.g. holds NaN or an object).
        """
        await self._sessions.lifecycle.ready()
        result, after = self._append_now(message, source)
        await after()
        return result

    async def update_message(
        self, message: SessionMessage, *, source: Source = "server"
    ) -> SessionMessage | None:
        """Replace the stored message with ``message``'s id.

        Returns the stored form, or ``None`` if the id isn't in the session.
        An identical message writes nothing and sends no change event.
        """
        await self._sessions.lifecycle.ready()
        prepared, tokens = self._sessions._prepare(message, source)
        outcome = self._sessions._update(self.session_id, prepared, tokens)
        if outcome == "missing":
            return None
        if outcome == "updated":
            await self._notify(
                UpdateEvent(session_id=self.session_id, message=prepared)
            )
        return prepared

    async def upsert_message(
        self, message: SessionMessage, *, source: Source = "server"
    ) -> AppendResult:
        """Append ``message`` if its id is new, otherwise update it."""
        await self._sessions.lifecycle.ready()
        result, after = self._upsert_sync(message, source=source)
        await after()
        return result

    async def delete_messages(self, message_ids: Sequence[str]) -> None:
        """Delete messages; the messages after them follow their parent."""
        await self._sessions.lifecycle.ready()
        self._sessions._delete(self.session_id, message_ids)
        await self._notify(
            DeleteEvent(session_id=self.session_id, message_ids=list(message_ids))
        )

    async def clear_messages(self) -> None:
        """Delete every message in the session."""
        await self._sessions.lifecycle.ready()
        self._sessions._clear(self.session_id)
        await self._notify(ClearEvent(session_id=self.session_id))

    # Reads

    async def history(self) -> AsyncIterator[SessionMessage]:
        """Yield the conversation, oldest first, fetching content in windows."""
        await self._sessions.lifecycle.ready()
        rows = self._sessions._path(self.session_id)
        async for message in self._sessions._messages(self.session_id, rows):
            yield message

    async def history_batches(
        self, *, batch_size: int = 50, max_batch_bytes: int = 4 * 1024 * 1024
    ) -> AsyncIterator[Sequence[SessionMessage]]:
        """Yield `history` in batches of at most ``batch_size`` messages.

        A batch also ends once its messages' JSON reaches
        ``max_batch_bytes`` (a single larger message is a batch of its own).
        """
        size = max(1, batch_size)
        limit = max(1, max_batch_bytes)
        batch: list[SessionMessage] = []
        used = 0
        async for message in self.history():
            nbytes = byte_length(_dumps(message))
            if batch and (len(batch) >= size or used + nbytes > limit):
                yield batch
                batch, used = [], 0
            batch.append(message)
            used += nbytes
            if len(batch) >= size or used >= limit:
                yield batch
                batch, used = [], 0
        if batch:
            yield batch

    async def get_history(self) -> list[SessionMessage]:
        """Return the whole conversation, oldest first."""
        return [message async for message in self.history()]

    async def get_recent_history(self, max_content_bytes: int) -> RecentHistoryResult:
        """Return the newest messages whose stored size fits the budget.

        Each message costs its full stored size; the newest is always
        included, however large.
        """
        sessions = self._sessions
        await sessions.lifecycle.ready()
        rows = sessions._path(self.session_id)
        if not rows:
            return RecentHistoryResult(
                messages=[], truncated=False, total_content_bytes=0
            )
        start = len(rows) - 1
        used = rows[start].bytes
        while start > 0 and used + rows[start - 1].bytes <= max_content_bytes:
            start -= 1
            used += rows[start].bytes
        messages = [m async for m in sessions._messages(self.session_id, rows[start:])]
        # A walk that hit the cap is truncated only if its oldest row still
        # has a parent (a chain of exactly the cap's length is complete).
        capped = len(rows) > MAX_PATH_DEPTH and sessions._store.has_parent(
            self.session_id, rows[0].id
        )
        return RecentHistoryResult(
            messages=messages,
            truncated=start > 0 or capped,
            total_content_bytes=sum(row.bytes for row in rows),
        )

    async def get_history_row_stats(self) -> list[SessionRowStat]:
        """Return each stored message's size and token estimate, oldest first."""
        await self._sessions.lifecycle.ready()
        return [
            SessionRowStat(
                id=row.id,
                role=row.role,
                bytes=row.bytes,
                token_estimate=row.token_estimate,
            )
            for row in self._sessions._path(self.session_id)
        ]

    # Change feed

    def mirror[M](
        self,
        *,
        get: Callable[[], list[M]],
        set: Callable[[list[M]], None],
        transform: Callable[[SessionMessage], M] | None = None,
    ) -> Disposable:
        """Keep a host's in-memory list in step with this session's writes.

        Appends and updates edit the list ``get()`` returns in place; deletes
        and clears hand ``set`` a new list. ``transform`` turns stored
        messages into the list's type (whose items have an ``id``, as an
        attribute or a key).
        """
        session_id = self.session_id

        def convert(message: SessionMessage) -> M:
            return transform(message) if transform is not None else cast(M, message)

        def apply(event: SessionChangeEvent) -> None:
            if event.session_id != session_id:
                return
            match event:
                case AppendEvent(inserted=False):
                    return
                case AppendEvent(message=message) | UpdateEvent(message=message):
                    cache = get()
                    index = next(
                        (
                            i
                            for i, item in enumerate(cache)
                            if _id(item) == message["id"]
                        ),
                        None,
                    )
                    if index is None:
                        if isinstance(event, UpdateEvent):
                            return  # not in the window the host holds
                        cache.append(convert(message))
                    else:
                        cache[index] = convert(message)
                case DeleteEvent(message_ids=ids):
                    removed = frozenset(ids)
                    set([item for item in get() if _id(item) not in removed])
                case ClearEvent():
                    set([])

        return self._sessions.subscribe(apply)

    # Synchronous surface (first-party machinery: chat's cutover)

    def _upsert_sync(
        self, message: SessionMessage, *, source: Source = "server"
    ) -> tuple[AppendResult, Callable[[], Awaitable[None]]]:
        """Upsert now, synchronously; await the returned ``after`` for the feed.

        Chat calls it inside a Streams cutover's ``commit``, so the message
        lands in the same transaction that settles the stream. If that
        transaction rolls back, call `_abandon`.
        """
        sessions = self._sessions
        if not sessions._store.exists(self.session_id, message["id"]):
            return self._append_now(message, source)
        prepared, tokens = sessions._prepare(message, source)
        outcome = sessions._update(self.session_id, prepared, tokens)
        result = AppendResult(inserted=False, message=prepared)
        if outcome != "updated":
            return result, _nothing
        return result, partial(
            self._notify, UpdateEvent(session_id=self.session_id, message=prepared)
        )

    def _abandon(self) -> None:
        """Forget cached state after a rolled-back transaction counted writes."""
        self._sessions._tails.pop(self.session_id, None)

    def _append_now(
        self, message: SessionMessage, source: Source
    ) -> tuple[AppendResult, Callable[[], Awaitable[None]]]:
        sessions = self._sessions
        prepared, tokens = sessions._prepare(message, source)
        result = sessions._append(self.session_id, prepared, tokens)
        event = AppendEvent(
            session_id=self.session_id, message=result.message, inserted=result.inserted
        )
        return result, partial(self._notify, event)

    async def _notify(self, event: SessionChangeEvent) -> None:
        await self._sessions._feed.fire_async(event)


async def _nothing() -> None:
    pass


def _dumps(message: SessionMessage) -> str:
    # Compact and keeping non-ASCII characters, like JSON.stringify, so stored
    # sizes and byte budgets match upstream's.
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def _parse(text: str) -> SessionMessage | None:
    # Rows that don't parse into a message are skipped, as upstream.
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    if (
        isinstance(value, dict)
        and isinstance(value.get("id"), str)
        and isinstance(value.get("role"), str)
        and isinstance(value.get("parts"), list)
    ):
        return cast(SessionMessage, value)
    return None


def _windows(rows: Sequence[PathRow]) -> Iterator[Sequence[PathRow]]:
    # At most _WINDOW_ROWS rows and _WINDOW_BYTES bytes; a window always takes
    # at least one row, however large.
    start = 0
    while start < len(rows):
        end = start
        size = 0
        while end < len(rows) and end - start < _WINDOW_ROWS:
            if end > start and size + rows[end].bytes > _WINDOW_BYTES:
                break
            size += rows[end].bytes
            end += 1
        yield rows[start:end]
        start = end


def _id(item: Any) -> Any:
    return item["id"] if isinstance(item, dict) else item.id
