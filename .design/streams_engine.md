# Streams engine (design pass, step 10)

The machinery behind the `Streams` capability: storage, the append fence,
settlement and the cutover, replay-then-tail reads, and the synchronous
surface chat uses. The **API** is in [streams_api.md](./streams_api.md)
(decided); this doc is the internals, from reading upstream end to end.

Upstream: `packages/agents/src/streams/streams.ts` (1,078 lines), `types.ts`,
`errors.ts`; the chat adapter's use of it in `chat/resumable-stream.ts`.

Status: **decided and implemented** (2026-10-06). §5 records the question and
answer; §6 records how the implementation turned out.

---

## 1. How upstream works

- **A capability with no alarm and no jobs.** `Streams` uses only storage and
  events, so it works on facets unchanged: each facet's streams live in its
  own storage, and nothing is routed. **`Agent` doesn't install it.** A plain
  agent does `this.lifecycle.use(new Streams())`; `AIChatAgent` installs its
  own (`createChatStreams()`, with `maxChunkBytes` raised to 1,900,000).
- **Storage** ([sql_schemas.md](./sql_schemas.md) §6): one `cf_agents_streams`
  row per stream (a rowid table, for the "newest first" tiebreak) and
  `cf_agents_stream_blocks` (`WITHOUT ROWID`, so an append bills one row).
  A block's `body` is chunk JSON texts joined by `,`, covering the half-open
  range `[seq_from, seq_to)`.
- **The tail is the source of truth while live.** `#blockTail` reads the last
  block (`ORDER BY block DESC LIMIT 1`, served by the primary key). The cursor
  is its `seq_to` (0 with no blocks), and the newest chunk's time is its
  `updated_at`. The stream row's `chunk_count` / `updated_at` are written only
  at open and settle, so `status()` derives them from the tail for a live
  stream.
- **Append** (`#append`, synchronous): serialize first (user `toJSON` could
  re-enter), then in one synchronous block: read the state (the fence),
  read the tail, and either `UPDATE` the open block
  (`body = body || ',' || ?`, `seq_to + 1`) when it stays within 256 K
  characters, or `INSERT` the next block. Then wake readers. Nothing in that
  block awaits or calls user code, so nothing can interleave with it.
- **Settle** (`#settle`): one guarded `UPDATE … WHERE state = 'streaming'`
  stamps state, error, `closed_at`, `updated_at`, and the exact final
  `chunk_count` (read from the tail in the same block). It returns whether
  this call made the transition. With `commit` or `discard`, the settle, the
  caller's `commit()`, and the deletion run in one `transactionSync`; a
  raising `commit` rolls it all back. Events (`stream:closed` /
  `stream:errored`, then `stream:deleted`) and wakeups fire only after the
  transaction returns. Settling an already terminal or deleted stream does
  nothing and doesn't run `commit`.
- **Delete** (`#deleteRows`): the single path for every deletion (public
  `delete`, the cutover's discard, chat's unchecked deletes). Delete hooks
  see the row and its cursor first, then the blocks and the row are removed.
  The public `delete` refuses a live stream.
- **Reads** (`readBatches`; `read` flattens it): check the stream exists
  (`StreamNotFoundError` otherwise), then loop:
  1. read up to `batchSize` chunks from `next` (parsing one block at a
     time) and yield them;
  2. a full batch → read again;
  3. the first short batch means caught up: call `onUpToDate` once, then
     **read again rather than wait**, because the callback may append
     synchronously, and that append's wakeup would have no waiter yet;
  4. deleted → end; terminal → end once a read returns nothing (a non-empty
     short batch was a suspension point, so drain first);
  5. a non-empty batch → read again (appends made while the consumer held
     the batch woke no one);
  6. otherwise wait for a wakeup, then read again.

  Wakeups carry no data: one set of waiters per stream, all resolved and
  cleared on each append, settle, or delete. An aborted waiter removes
  itself (and the set, when it was the last).
- **Events:** `stream:opened`, `stream:closed`, `stream:errored` (with
  `reason` when given), and `stream:deleted`, each with `streamId`.
- **The synchronous surface for chat** (`__DO_NOT_USE_WILL_BREAK__sync()`):
  `ensureTables`, `getStream`, `insertStream`, `setMetadata`, `append`,
  `lastChunkAt`, `cursor`, `onDelete` (returns an unsubscribe), `settle`,
  `deleteUnchecked` (any state; wakes readers), `deleteMany` (silent),
  `readChunks` (one page), `listRows`, `rowsByTag`, plus `importStream` /
  `importChunk` for migrating legacy chat tables. It skips
  `lifecycle.ready()`, since chat calls it before startup.
- **Schema v1 → v2.** The old per-chunk table `cf_agents_stream_chunks` is
  folded into blocks lazily, one stream at a time.

---

## 2. Python design

### 2.1 A straight port (no choice involved)

- The two tables and their DDL, the block packing (256 K characters), the
  tail as the live source of truth, the fenced append, the guarded settle
  with its exact final count, the cutover in one `transactionSync`, the single
  delete path with its hooks, and the read loop's six steps, including
  re-reading after `on_up_to_date` and after a non-empty batch.
- Events with upstream's names and payloads (added to
  [observability.md](./observability.md)).
- Defaults: `max_chunk_bytes` 1 MiB, stream ids 1–256 characters,
  `batch_size` 100, `list` limit 100.
- **Not installed by `Agent`**, as upstream: `self.streams = self.use(Streams())`
  (the worked example in streams_api.md §4.1). `AIChatAgent` installs its
  own in step 12.
- **Facets need nothing.** Confirmed from the code: Streams touches only its
  own storage and events, so facet streams work without routing.
- ~~The append path uses raw JS SQL~~: superseded, see §6 item 1.

### 2.2 Already decided (streams_api.md §4)

`from` → `start`; the full public API in phase 1, `sse_response` deferred;
the chat surface as underscore-prefixed methods on `Streams`; wakeups as
`asyncio.Future`s; `commit` must be synchronous (an awaitable raises
`TypeError`); no `signal` (cancel the reading task).

### 2.3 Smaller choices made here

1. **No legacy fold and no import methods.** There are no old Python
   tables to migrate, so the schema starts at version 1 with blocks only, and
   `importStream` / `importChunk` aren't ported (streams_api.md §4 already
   drops the import methods).
2. **Serialization** is compact JSON (like `JSON.stringify`), and the
   size limit counts UTF-8 bytes, as upstream. A value JSON can't represent
   raises `StreamSerializationError` (wrapping the `TypeError` /
   `ValueError`); see §6 item 2 for NaN. `None` is a valid chunk (JSON `null`); upstream's
   "`undefined` chunk" error has no Python counterpart.
3. **Block length counts characters.** SQLite's `length(body)` and Python's
   `len()` both count code points; upstream compares UTF-16 lengths. The
   256 K threshold is a soft target, so this changes nothing that matters.
4. **"Did it change" comes from `RETURNING`.** The settle's guarded `UPDATE`
   and the delete use `RETURNING stream_id`, since `Sql` returns rows rather
   than `rowsWritten` (`RETURNING` works on DO SQLite, verified in step 7).
5. **Errors.** `StreamClosedError`, `StreamNotFoundError`, and
   `StreamSerializationError` are the catchable ones (streams_api.md §3).
   Misuse gets built-ins: `ValueError` for an empty or over-long stream id
   and for reopening a live stream with a different tag, `RuntimeError` for
   deleting a live stream.
6. **Read-only results return `Sequence`.** `list` returns
   `Sequence[StreamStatus]`, and `read_batches` yields
   `Sequence[StreamChunk]` (streams_api.md §3 said `list`).
7. **`_on_delete(hook)` returns a `Disposable`** (the core convention),
   in place of upstream's unsubscribe function.
8. **The chat surface's names** (underscore methods, per streams_api.md
   §4.3): `_ensure_tables`, `_get_stream`, `_insert_stream`, `_set_metadata`,
   `_append`, `_last_chunk_at`, `_cursor`, `_on_delete`, `_settle`,
   `_delete_unchecked`, `_delete_many`, `_read_chunks`, `_list_rows`,
   `_rows_by_tag`. The public methods are built on the same ones.

---

## 3. Module layout (`src/agents/streams/`)

| Module | Holds | Upstream |
| --- | --- | --- |
| `types.py` | `StreamState`, `StreamChunk`, `StreamStatus`, `StreamRow`, `StreamChunkRow` | `types.ts` |
| `errors.py` | the three errors | `errors.ts` |
| `store.py` | DDL and every statement, including the raw-JS append path | `streams.ts` (storage) |
| `streams.py` | `Streams` (the capability, wakeups, the read loop, the chat surface) and `StreamWriter` | `streams.ts` |

---

## 4. Finding: fiber recovery ran before late-installed capabilities started (fixed)

Found while checking where Streams goes in the install order. **This is a step 9
bug.**

- **Upstream** runs `_checkRunFibers` in the host's start path, after every
  capability's `onStart` and before the user's `onStart`.
- **Here** it runs in `Fibers.on_start`. `Fibers` is installed in
  `Agent.__init__`, so a capability a subclass installs in its own `__init__`
  (`self.use(Streams())`, chat's streams in step 12) starts **after** the
  recovery hook has run. A hook that opens or reads a stream would find its
  tables missing.
- **Reproduced:** a subclass installs a capability that records when it has
  started. On the next wake, `on_fiber_recovered` saw that capability not yet
  started.
- **Fixed (2026-10-06):** `Fibers.on_start` only creates its tables. The scan
  and arming housekeeping are `Fibers.recover_on_wake()`, called from Agent's
  host start hook just before the user's `on_start` (upstream's order). A
  host using a bare `Fibers` calls it itself after startup. Regression test
  `test_recovery_waits_for_capabilities_a_subclass_installs`; recorded in
  [fibers_engine.md](./fibers_engine.md) §5 item 1.

---

## 5. Questions to decide

**Q1. The §4 fiber fix: now, or with Streams?** **Decided: (a) now** (done).
- (a) **Now, as its own change, with a regression test.** **Recommended:**
  it's a step 9 bug, independent of Streams.
- (b) As part of the Streams implementation.

---

## 6. Implementation notes (step 10, 2026-10-06)

Done: `src/agents/streams/`; 461 tests in total (18 new, in `tests/streams/`).
Checked on `workerd` (`verify/results/streams_client.py`,
`streams_report.json`):
- a live reader in the producer's isolate got each append as it came, and
  `on_up_to_date` fired before the first one;
- the cutover through real `transactionSync`: a `commit` that raised
  `KeyError` rolled back both the settle and its own `INSERT` (the stream stayed
  `streaming`, the exception kept its type), and a successful one saved and
  deleted together;
- 600 chunks of about 1 KB packed into three blocks (`[0, 257)`, `[257, 514)`,
  `[514, 600)`, each under 256 K characters), and a read from seq 250 came back
  intact;
- **a real isolate crash** (`ctx.abort`) mid-stream: on the next request, the
  fiber recovery hook (on a subclass-installed `Streams`, so after the §4 fix)
  read the stream's cursor, appended where it left off, and closed it;
- Streams on a facet, with nothing routed.

How it turned out, beyond §2:

1. **Appends go through `Sql`, not a separate raw path.** §2.1 planned raw JS
   SQL for appends, from platform_verification.md §3.5. On a closer look, that
   measurement compared the *Workers SDK's* storage wrapper with raw storage,
   and `Sql` already calls raw storage (`_ffi.unwrap`). On `workerd`, 2,000
   appends took 150–190 µs each, three statements apiece (the fence, the tail,
   the write): about 50–60 µs per statement, inside the raw range measured
   there (25–80 µs). One path for every statement.
2. **NaN and infinities are refused.** Values are serialized with
   `json.dumps(..., separators=(",", ":"), allow_nan=False)` rather than
   `to_json`. Python would otherwise store `NaN` as text that isn't JSON, which
   no other reader could parse (upstream's `JSON.stringify` turns them into
   `null`). `to_json`'s dataclass and `datetime` conversions don't apply to
   chunks, which are plain JSON values.
3. **Public reads skip re-encoding.** Upstream's `readChunks` re-stringifies each
   chunk and the reader parses it again. Here the store yields parsed values
   from each block, and only the chat surface's `_read_chunks` re-encodes them
   (it returns JSON text, like upstream).
4. **The store's block walk is a generator**, materialized by its callers within
   one synchronous block, so a page is read without interleaving appends.
5. **The fake runtime's `transactionSync`** is a SQLite savepoint, so tests see
   the same rollback the platform does.
6. **Fiber errors subclass `AgentsException`.** Found while adding the stream
   errors: `FiberConflictError` and `FiberNotFoundError` (step 9) subclassed
   `Exception`, against the decided rule (utilities.md §1). Fixed.
7. **Non-ASCII text is stored as is** (fixed 2026-10-06, found in the Sessions
   design pass, [sessions_engine.md](./sessions_engine.md) §4). Values were
   serialized with Python's default `ensure_ascii=True`, which escapes every
   non-ASCII character (`\uXXXX`): still valid JSON, but 2–3 times
   upstream's stored size for accented, CJK, or emoji text, and counted that
   way against `max_chunk_bytes`. Now `ensure_ascii=False`, like
   `JSON.stringify`, and the size is measured with `surrogatepass`.
