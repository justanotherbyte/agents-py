"""Types for the Streams capability (``.design/streams_api.md`` §3)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypedDict

from ..core.types import JSONValue

__all__ = (
    "StreamChunk",
    "StreamChunkRow",
    "StreamRow",
    "StreamState",
    "StreamStatus",
)

type StreamState = Literal["streaming", "completed", "errored"]
"""``streaming`` from ``open()`` until the producer settles it; both terminal
states keep the chunk log readable."""


@dataclass(slots=True, kw_only=True, frozen=True)
class StreamChunk:
    """One durable chunk: ``seq`` is 0-based, assigned at append, and stable."""

    seq: int
    chunk: JSONValue


@dataclass(slots=True, kw_only=True)
class StreamStatus:
    """One stream's state and cursor.

    Parameters
    ----------
    stream_id
        The stream's id.
    state
        Where it is in its life.
    cursor
        The next ``seq`` to be assigned (the durable chunk count).
    created_at
        When it was opened.
    updated_at
        Its last write: an append or the settle. A ``streaming`` stream whose
        ``updated_at`` is old has a producer that stopped appending.
    tag
        The lookup key it was opened with.
    metadata
        The metadata it was opened with.
    error
        The reason given to ``error()``.
    closed_at
        When it settled.
    """

    stream_id: str
    state: StreamState
    cursor: int
    created_at: datetime
    updated_at: datetime
    tag: str | None = None
    metadata: dict[str, JSONValue] | None = None
    error: str | None = None
    closed_at: datetime | None = None


class StreamRow(TypedDict):
    """A raw ``cf_agents_streams`` row (``.design/sql_schemas.md`` §6).

    While ``state`` is ``streaming``, ``chunk_count`` and ``updated_at`` are
    stale by design: appends write only the chunk log, whose tail is the
    truth. The settle stamps both exactly.
    """

    stream_id: str
    state: StreamState
    tag: str | None
    metadata: str | None
    error_message: str | None
    chunk_count: int
    created_at: int
    updated_at: int
    closed_at: int | None


class StreamChunkRow(TypedDict):
    """One chunk read back from the log, its value as JSON text (for chat)."""

    stream_id: str
    seq: int
    chunk: str
    created_at: int
