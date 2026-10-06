# Streams (decided)

Durable incremental output: an ordered, durable chunk log per stream, with a
monotonic cursor, replay-then-tail reads, and a terminal status. Upstream:
`../agents/packages/agents/src/streams/`, design record
`../agents/design/rfc-streams.md`, docs `docs/agents/streams.md`.

Related: [scope.md](./scope.md) §2.2, [sql_schemas.md](./sql_schemas.md) §6,
[chat_models.md](./chat_models.md) §3 (what chat streams carry),
[scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) (Tasks).

---

## 1. Why it's in scope

**`AIChatAgent` requires it.** Chat's resumable streams (the store behind
reconnect-and-replay, wire §7.4) are an adapter over Streams
(`chat/resumable-stream.ts:81`, `createChatStreams()`). It also gives Tasks a
place to put output that must be visible while it's produced and survive the
producer's death mid-production.

---

## 2. What Streams is

- **A Lifecycle capability** (`class Streams(LifecycleCapability)`, id
  `"streams"`) that owns `cf_agents_streams` and `cf_agents_stream_blocks`.
- **One stream** = an id, a state (`streaming` → `completed` | `errored`), an
  optional indexed `tag`, optional JSON metadata, and an append-only list of
  JSON chunks numbered `0, 1, 2, …` (`seq`).
- **No alarm and no jobs.** Appends happen during the producer's own
  invocation, and they wake in-isolate readers directly. So it also works on
  facets.
- **Live fanout is in-isolate only.** A DO runs in one isolate at a time, so
  every concurrent reader shares the producer's isolate; a reader that outlives
  the isolate replays from its cursor when it reconnects.

### 2.1 Upstream API

```ts
const stream = await this.streams.open("reply:123", { tag: requestId, metadata });
stream.cursor;              // next seq to assign
stream.append(chunk);       // synchronous durable write; wakes live readers; returns its seq
stream.close();             // or stream.error(reason); both accept { commit, discard }

for await (const chunk of this.streams.read("reply:123", { from, signal })) { … }   // replay, then tail
for await (const batch of this.streams.readBatches("reply:123", { from, batchSize, onUpToDate, signal })) { … }

await this.streams.status("reply:123");   // { streamId, state, cursor, tag?, metadata?, error?, createdAt, updatedAt, closedAt? }
await this.streams.list({ state, tag, limit });   // newest first
await this.streams.delete("reply:123");   // terminal streams only

sseResponse(this.streams, "reply:123", { request });   // serve a stream over SSE
```

### 2.2 Rules (upstream)

- **`open()` is idempotent on the id.** Reopening a live stream returns a
  writer at its current cursor; reopening a terminal stream raises
  `StreamClosedError`; reopening a live stream with a *different* tag raises.
- **Cursors:** `seq` is 0-based and assigned at append; `from` is inclusive;
  `status().cursor` is the next seq to be assigned (= the chunk count).
- **Appends are synchronous and fenced by a read.** One synchronous block
  checks the state, reads the chunk-log tail, and writes the chunk. A DO runs
  one synchronous block at a time, so an append can't interleave with a settle
  or another append. **One billed row per append** (the stream row is written
  only at open and settle).
- **Settling (`close` / `error`) is idempotent.** Settling a terminal or
  deleted stream does nothing.
- **The cutover** (`StreamSettleOptions`): `close(commit=…, discard=True)`
  settles the stream, runs the caller's **synchronous** writes (e.g. saving the
  finished message), and deletes the stream's rows, all in **one SQLite
  transaction**. A crash leaves either the live stream or the finished message,
  never neither. If `commit` raises, the settle is rolled back and the stream
  stays live. Events and reader wakeups fire only after the transaction
  commits.
- **Reads don't depend on the producer being alive.** Replay from any cursor,
  then tail live appends, ending once the stream is terminal and every chunk
  has been yielded. Reading an `errored` stream yields its chunks and ends;
  `status()` gives the outcome. `readBatches` yields one list per replay page
  and one per live wakeup (everything that accumulated), and calls
  `onUpToDate` once when the reader first catches up to the durable tail.
- **Tags** are a non-unique, indexed lookup key fixed at creation: an operation
  that produces successive streams (a retried turn) tags each the same way,
  and `list(tag=…, limit=1)` finds the newest.
- **Chunk size limit:** a chunk's JSON over `max_chunk_bytes` (default 1 MiB)
  raises `StreamSerializationError`.
- **Storage:** chunks are packed into blocks of up to 256 K characters, so an
  append either extends the open block or starts a new one
  ([sql_schemas.md](./sql_schemas.md) §6).
- **While a stream is live, its row's `chunk_count` / `updated_at` are
  deliberately stale.** Cursor and liveness are derived from the chunk log's
  tail; settling stamps them exact.
- **No retention sweeping in the capability.** Chat sweeps its own streams.

### 2.3 The Tasks composition contract

A task step appends to a stream and starts at the stream's own durable
cursor, so a replay after an interruption resumes instead of duplicating:

```python
async def generate(self, input, step):
    async def produce(_attempt):
        stream = await self.streams.open(input["stream_id"])
        for i in range(stream.cursor, input["total"]):
            stream.append(await make_chunk(i))
        stream.close()

    await step.do("stream", produce)
```

Neither capability imports the other.

### 2.4 How chat uses it

The chat adapter (`ResumableStream`) does **not** use the public async API. It
uses a synchronous internal surface (upstream:
`Streams.__DO_NOT_USE_WILL_BREAK__sync()`), because chat's own API is
synchronous and is constructed before Lifecycle starts. It uses: `getStream`,
`insertStream`, `setMetadata`, `append`, `cursor`, `lastChunkAt`, `settle`
(with the cutover), `readChunks` (paged replay), `listRows`, `deleteMany`,
`onDelete`, `ensureTables` (plus `importStream` / `importChunk`, only for
migrating legacy chat tables, which Python doesn't need).

Chat-specific policy lives in the adapter, not in Streams:
- **coalescing:** ~10 wire chunks per stored segment (up to 512,000 raw bytes),
  to save storage writes;
- **paged replay:** 10 stored segments per page;
- **retention:** a finished stream is deleted by the cutover when its message is
  saved; an abandoned `streaming` stream is reaped after 1 hour without a
  chunk.

---

## 3. Python API (decided)

```python
class Streams(LifecycleCapability):
    def __init__(self, *, max_chunk_bytes: int = 1_048_576) -> None: ...

    async def open(
        self,
        stream_id: str,
        *,
        tag: str | None = None,
        metadata: dict[str, JSONValue] | None = None,
    ) -> StreamWriter: ...
    def read(self, stream_id: str, *, start: int = 0) -> AsyncIterator[StreamChunk]: ...
    def read_batches(
        self,
        stream_id: str,
        *,
        start: int = 0,
        batch_size: int = 100,
        on_up_to_date: Callable[[], None] | None = None,
    ) -> AsyncIterator[Sequence[StreamChunk]]: ...  # read-only batches
    async def status(self, stream_id: str) -> StreamStatus | None: ...
    async def list(
        self,
        *,
        state: StreamState | Sequence[StreamState] | None = None,
        tag: str | None = None,
        limit: int | None = None,
    ) -> Sequence[StreamStatus]: ...  # read-only result
    async def delete(self, stream_id: str) -> bool: ...


class StreamWriter:
    stream_id: str

    @property
    def cursor(self) -> int: ...
    def append(self, chunk: JSONValue) -> int: ...  # synchronous durable write
    def close(
        self, *, commit: Callable[[], None] | None = None, discard: bool = False
    ) -> None: ...
    def error(
        self,
        reason: str | None = None,
        *,
        commit: Callable[[], None] | None = None,
        discard: bool = False,
    ) -> None: ...


StreamState = Literal["streaming", "completed", "errored"]


@dataclass(slots=True, kw_only=True)
class StreamChunk:
    seq: int
    chunk: JSONValue


@dataclass(slots=True, kw_only=True)
class StreamStatus:
    stream_id: str
    state: StreamState
    cursor: int
    created_at: datetime  # timezone-aware UTC
    updated_at: datetime
    tag: str | None = None
    metadata: dict[str, JSONValue] | None = None
    error: str | None = None
    closed_at: datetime | None = None


class StreamClosedError(AgentsException): ...  # utilities.md §1


class StreamNotFoundError(AgentsException): ...


class StreamSerializationError(AgentsException): ...
```

Applies the decided conventions: snake_case, keyword-only options,
dataclasses, asyncio cancellation instead of `AbortSignal` (a reader stops when
its task is cancelled, so there is no `signal` option).

---

## 4. Decisions

1. **`from` → `start`** (inclusive, same meaning as upstream's `from`), because
   `from` is a Python keyword.
2. **The full public API is ported in phase 1** (`open` / `StreamWriter` /
   `read` / `read_batches` / `status` / `list` / `delete`), for the Tasks
   composition contract and plain-DO users. **`sse_response` is deferred**:
   nothing in scope uses it.
3. **The internal synchronous surface for chat** is a set of underscore-prefixed
   methods on `Streams` (first-party, same package), mirroring upstream's
   `__DO_NOT_USE_WILL_BREAK__sync()` operations (§2.4, minus the legacy
   `import*` methods). The public async methods are built on the same private
   core, so appends, settlement, wakeups, and events behave identically on both
   paths.
4. **Live-tail wakeups:** per-stream sets of `asyncio.Future`s resolved after each
   append or settle commits (implementation detail).
5. **The cutover's `commit` must be synchronous.** It runs inside
   `ctx.storage.transactionSync(...)`; a `commit` that returns an awaitable
   raises `TypeError` (same rule as `Emitter.fire`). Verified: a Python
   callable works there, and an exception raised inside keeps its type and
   rolls the transaction back ([platform_verification.md](./platform_verification.md) §3.4).
6. **No `signal` option:** a reader stops when its task is cancelled.

### 4.1 Worked example

A `ReportAgent` that produces a report from a task (resuming from
`stream.cursor`), lets clients replay-then-follow it through a streaming
`@callable`, inspects it with `status` / `list`, and saves the finished report
with the cutover:

```python
class ReportAgent(Agent):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.streams = self.use(Streams())  # agent_api.md §2.3

    @task
    async def report(self, input: dict, step: TaskStep) -> None:
        async def write(_attempt: TaskStepAttempt) -> None:
            stream = await self.streams.open(
                f"report:{input['report_id']}", tag=input["report_id"]
            )
            for i in range(stream.cursor, len(input["sections"])):
                stream.append(
                    {
                        "section": i,
                        "text": await self.write_section(input["sections"][i]),
                    }
                )

            def save() -> None:  # synchronous
                self.sql(
                    "INSERT INTO reports (id, body) VALUES (?, ?)",
                    input["report_id"],
                    "…",
                )

            stream.close(commit=save, discard=True)

        await step.do("write", write)

    @callable(streaming=True)
    async def follow_report(
        self, response: StreamingResponse, report_id: str, start: int = 0
    ):
        def caught_up() -> None:
            response.send({"type": "live"})

        async for batch in self.streams.read_batches(
            f"report:{report_id}",
            start=start,
            batch_size=50,
            on_up_to_date=caught_up,
        ):
            response.send(
                {
                    "type": "chunks",
                    "chunks": [{"seq": c.seq, "chunk": c.chunk} for c in batch],
                }
            )
        response.end({"type": "done"})
```

(`@callable`, `StreamingResponse`, and `self.sql` belong to the core `Agent`
API: [agent_api.md](./agent_api.md).)

---

## 5. Verified ([platform_verification.md](./platform_verification.md))

- `ctx.storage.transactionSync(fn)` with a Python callable commits, returns
  the result, and rolls back on a raised exception (keeping its Python type)
  (§3.4 there).
- `sql.exec` is synchronous through the FFI, so `append` is a plain method
  (§3.3 there). Appends go through `Sql`, which already calls raw JS storage
  (§3.5 there measured the Workers SDK's storage wrapper against raw); a
  `workerd` run of 2,000 appends took 150–190 µs each, three statements
  apiece ([streams_engine.md](./streams_engine.md) §6).
