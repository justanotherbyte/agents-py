"""SQL storage for Streams: the stream rows and their block-packed chunk logs.

Port of the storage half of upstream ``streams/streams.ts``
(``.design/streams_engine.md``, ``.design/sql_schemas.md`` §6). A block's
``body`` is its chunks' JSON texts joined by ``,``, covering the half-open
``seq`` range ``[seq_from, seq_to)``; an append grows the last block or
starts the next one, so every append is one billed row write.
"""

import json
from collections.abc import Iterator, Sequence
from typing import Any, NamedTuple

from ..core.sql import Sql
from ..core.timing import now_ms
from .types import StreamRow, StreamState

__all__ = ("BLOCK_MAX_CHARS", "StreamStore", "Tail")

BLOCK_MAX_CHARS = 256 * 1024
"""A block grows until its body would pass this many characters. Well under
the 2 MiB row limit, so a 1 MiB chunk always fits in a fresh block, and small
enough that a replay page parses one block at a time."""

# The stream table stays a rowid table: rowid breaks "newest first" ties in
# insertion order when streams share a created_at millisecond.
_STREAMS = """CREATE TABLE IF NOT EXISTS cf_agents_streams (
  stream_id TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN ('streaming', 'completed', 'errored')),
  tag TEXT,
  metadata TEXT,
  error_message TEXT,
  chunk_count INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  closed_at INTEGER
)"""
_STREAMS_BY_TAG = """CREATE INDEX IF NOT EXISTS idx_cf_agents_streams_tag
  ON cf_agents_streams(tag, created_at)"""
# WITHOUT ROWID: the primary key is the table, so an append bills one row
# (a rowid table would also maintain a hidden index).
_BLOCKS = """CREATE TABLE IF NOT EXISTS cf_agents_stream_blocks (
  stream_id TEXT NOT NULL,
  block INTEGER NOT NULL,
  seq_from INTEGER NOT NULL,
  seq_to INTEGER NOT NULL,
  body TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (stream_id, block)
) WITHOUT ROWID"""

_ROW_COLUMNS = """stream_id, state, tag, metadata, error_message, chunk_count,
  created_at, updated_at, closed_at"""


class Tail(NamedTuple):
    """The chunk log's tail: the next ``seq``, and the newest chunk's time."""

    next_seq: int
    last_chunk_at: int | None


class _Block(NamedTuple):
    block: int
    seq_to: int
    updated_at: int
    length: int


class StreamStore:
    """Every SQL statement Streams runs."""

    __slots__ = ("sql",)

    def __init__(self, sql: Sql) -> None:
        self.sql = sql

    def ensure_tables(self) -> None:
        """Create the tables and the tag index (idempotent)."""
        for statement in (_STREAMS, _STREAMS_BY_TAG, _BLOCKS):
            self.sql(statement)

    # Stream rows

    def get(self, stream_id: str) -> StreamRow | None:
        """Return one stream's row."""
        rows = self.sql(
            "SELECT " + _ROW_COLUMNS + " FROM cf_agents_streams WHERE stream_id = ?",
            stream_id,
            row=StreamRow,
        )
        return rows[0] if rows else None

    def state(self, stream_id: str) -> StreamState | None:
        """Return one stream's state alone (the append fence's read)."""
        rows = self.sql(
            "SELECT state FROM cf_agents_streams WHERE stream_id = ?", stream_id
        )
        return rows[0]["state"] if rows else None

    def insert(self, stream_id: str, tag: str | None, metadata: str | None) -> None:
        """Insert a ``streaming`` row (callers check it doesn't exist)."""
        now = now_ms()
        self.sql(
            "INSERT INTO cf_agents_streams (stream_id, state, tag, metadata,"
            " chunk_count, created_at, updated_at)"
            " VALUES (?, 'streaming', ?, ?, 0, ?, ?)",
            stream_id,
            tag,
            metadata,
            now,
            now,
        )

    def set_metadata(self, stream_id: str, metadata: str | None) -> None:
        """Replace a row's metadata, whatever its state."""
        self.sql(
            "UPDATE cf_agents_streams SET metadata = ? WHERE stream_id = ?",
            metadata,
            stream_id,
        )

    def settle(self, stream_id: str, state: StreamState, reason: str | None) -> bool:
        """End a ``streaming`` stream, stamping its exact final count.

        Returns whether this call made the transition.
        """
        now = now_ms()
        return bool(
            self.sql(
                "UPDATE cf_agents_streams SET state = ?, error_message = ?,"
                " closed_at = ?, updated_at = ?, chunk_count = ?"
                " WHERE stream_id = ? AND state = 'streaming' RETURNING stream_id",
                state,
                reason,
                now,
                now,
                self.tail(stream_id).next_seq,
                stream_id,
            )
        )

    def delete(self, stream_id: str) -> bool:
        """Delete a stream's blocks and row; ``False`` if there was no row."""
        self.sql("DELETE FROM cf_agents_stream_blocks WHERE stream_id = ?", stream_id)
        return bool(
            self.sql(
                "DELETE FROM cf_agents_streams WHERE stream_id = ? RETURNING stream_id",
                stream_id,
            )
        )

    def list(
        self, states: Sequence[StreamState] | None, tag: str | None, limit: int
    ) -> Sequence[StreamRow]:
        """Return rows newest first, optionally filtered."""
        encoded = json.dumps(list(states)) if states else None
        return self.sql(
            "SELECT " + _ROW_COLUMNS + " FROM cf_agents_streams"
            " WHERE (? IS NULL OR state IN (SELECT value FROM json_each(?)))"
            " AND (? IS NULL OR tag = ?)"
            " ORDER BY created_at DESC, stream_id DESC LIMIT ?",
            encoded,
            encoded,
            tag,
            tag,
            limit,
            row=StreamRow,
        )

    def all_rows(self) -> Sequence[StreamRow]:
        """Return every row, newest first (ties in insertion order)."""
        return self.sql(
            "SELECT " + _ROW_COLUMNS + " FROM cf_agents_streams"
            " ORDER BY created_at DESC, rowid DESC",
            row=StreamRow,
        )

    def rows_by_tag(self, tag: str, state: StreamState | None) -> Sequence[StreamRow]:
        """Return the rows with ``tag``, newest first, optionally of one state."""
        return self.sql(
            "SELECT " + _ROW_COLUMNS + " FROM cf_agents_streams"
            " WHERE tag = ? AND (? IS NULL OR state = ?)"
            " ORDER BY created_at DESC, rowid DESC",
            tag,
            state,
            state,
            row=StreamRow,
        )

    # The chunk log

    def tail(self, stream_id: str) -> Tail:
        """Return the log's tail (the source of truth while a stream is live)."""
        block = self._last_block(stream_id)
        if block is None:
            return Tail(next_seq=0, last_chunk_at=None)
        return Tail(next_seq=block.seq_to, last_chunk_at=block.updated_at)

    def write_chunk(self, stream_id: str, chunk_json: str, at: int) -> int:
        """Append one serialized chunk and return its ``seq``.

        One row either way: an ``UPDATE`` growing the last block, or the
        ``INSERT`` of the next one.
        """
        last = self._last_block(stream_id)
        seq = last.seq_to if last is not None else 0
        if last is not None and last.length + len(chunk_json) + 1 <= BLOCK_MAX_CHARS:
            self.sql(
                "UPDATE cf_agents_stream_blocks SET body = body || ',' || ?,"
                " seq_to = ?, updated_at = ? WHERE stream_id = ? AND block = ?",
                chunk_json,
                seq + 1,
                at,
                stream_id,
                last.block,
            )
        else:
            self.sql(
                "INSERT INTO cf_agents_stream_blocks"
                " (stream_id, block, seq_from, seq_to, body, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                stream_id,
                last.block + 1 if last is not None else 0,
                seq,
                seq + 1,
                chunk_json,
                at,
                at,
            )
        return seq

    def chunks(
        self, stream_id: str, start: int, limit: int
    ) -> Iterator[tuple[int, Any, int]]:
        """Yield up to ``limit`` chunks from ``start`` as ``(seq, value, time)``.

        Parses one block at a time: ``[body]`` is the block's chunks as a JSON
        array. ``time`` is the block's last write.
        """
        block = -1
        count = 0
        while count < limit:
            rows = self.sql(
                "SELECT block, seq_from, seq_to, body, updated_at"
                " FROM cf_agents_stream_blocks"
                " WHERE stream_id = ? AND block > ? AND seq_to > ?"
                " ORDER BY block ASC LIMIT 1",
                stream_id,
                block,
                start,
            )
            if not rows:
                return
            row = rows[0]
            block = row["block"]
            values = json.loads(f"[{row['body']}]")
            for seq in range(max(start, row["seq_from"]), row["seq_to"]):
                if count == limit:
                    return
                yield seq, values[seq - row["seq_from"]], row["updated_at"]
                count += 1

    def _last_block(self, stream_id: str) -> _Block | None:
        rows = self.sql(
            "SELECT block, seq_to, updated_at, length(body) AS length"
            " FROM cf_agents_stream_blocks WHERE stream_id = ?"
            " ORDER BY block DESC LIMIT 1",
            stream_id,
        )
        if not rows:
            return None
        row = rows[0]
        return _Block(row["block"], row["seq_to"], row["updated_at"], row["length"])
