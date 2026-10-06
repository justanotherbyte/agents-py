# Sessions (Python, phase 1)

Durable conversation storage. **Phase 1 implements only what `AIChatAgent`
needs.** Everything else in upstream `agents/sessions` is deferred (§7).

Upstream: `../agents/packages/agents/src/sessions/` and its design record
`../agents/design/sessions.md`.
Related: [scope.md](./scope.md) §2.2 and §2.4,
[lifecycle_capabilities.md](./lifecycle_capabilities.md).

---

## 1. What Sessions is

- **A Lifecycle capability**: `class Sessions(LifecycleCapability)`, capability
  id `"sessions"` (upstream `sessions/sessions.ts:36`).
- **It stores conversation messages and nothing else.** It does no prompt
  assembly (upstream `agents/context`), no trimming for model calls (upstream
  `truncateOlderMessages` in `agents/chat`), and it is not a file store.
- **Messages are AI SDK `UIMessage`-compatible:** `{id, role, parts, metadata?}`.
  In Python, Sessions stores and returns them in wire form: `SessionMessage`,
  a `TypedDict` of JSON values (decided 2026-10-06,
  [sessions_engine.md](./sessions_engine.md) §5 Q1). The chat layer converts
  to and from its typed `UIMessage` ([chat_models.md](./chat_models.md) §5)
  at its boundary. Wherever §3 below says `UIMessage`, read `SessionMessage`. Upstream's `SessionMessage` also
  accepts an optional `createdAt` on input; Python's `UIMessage` has no
  `created_at` (decided, chat_models §7), and the write time lives only in the
  row's `created_at` column.
- **One conversation per Durable Object.** The default session id is the empty
  string, and `AIChatAgent` uses only that one.
- **Not installed by `Agent`.** The host installs it, as `AIChatAgent` does
  upstream (`ai-chat/src/index.ts:447`, `:1078`).

### Upstream users (for reference)

| User | Kind |
| --- | --- |
| `@cloudflare/ai-chat` (`AIChatAgent`) | Installs it. **The only user in our scope.** |
| `@cloudflare/think` | Installs it (branches, compaction). Out of scope. |
| `examples/next/sessions`, `examples/next/harnesses/codex` | Install it. Examples only. |
| `agents/chat/sanitize.ts` | Imports `sanitizeMessage`, `byteLength` from `sessions/sanitize` |
| `agents/chat/truncate-older-messages.ts` | Imports the `SessionMessage` type |
| `agents/context/blocks.ts` | Imports `estimateStringTokens` from `sessions/tokens`. Out of scope. |

---

## 2. How `AIChatAgent` uses it (the phase-1 surface)

All calls go to the default handle, used as a **linear chain**: no branches,
and regenerating a reply overwrites it.

| Upstream call | Where | Purpose |
| --- | --- | --- |
| `sessions.session()` | `index.ts:468` | Get the default handle |
| `session.mirror({...})` | `index.ts:2563` | Keep `this.messages` in sync from the change feed |
| `getRecentHistory(budget)` / `getHistory()` | `index.ts:2542` | Load `this.messages` once per wake under `hydrationByteBudget` (default 32 MiB); `getHistory()` when the budget is unbounded |
| `historyBatches()` | `index.ts:2591` | Stream the full transcript as a JSON array for the HTTP `…/get-messages` route, without holding it all in memory |
| `upsertMessage(msg)` | `index.ts:6347`, `:7098`, `:7149` | Save incoming and streamed messages |
| `updateMessage(msg)` | `index.ts:6694` | Apply a change to an existing message (e.g. a tool result) |
| `getHistoryRowStats()` | `index.ts:6371` | Enforce `maxPersistedMessages`. It counts **stored** rows, not the loaded window. |
| `deleteMessages(ids)` | `index.ts:6570` | Retention, and deleting regenerated replies |
| `clearMessages()` | `index.ts:1588` | `cf_agent_chat_clear` |
| `importMessage(...)` | (legacy) | One-time migration of the old `cf_ai_chat_agent_messages` table. **Not needed in Python.** |

`AIChatAgent` also skips calling `upsertMessage` when a message's JSON equals
its in-memory copy. Upstream does this to avoid a row read per message.

---

## 3. Python API

```python
class Sessions(LifecycleCapability):
    def __init__(self, *, reserved_metadata_keys: Sequence[str] = ()) -> None: ...
    def session(self, session_id: str = "") -> Session: ...  # cached handle
    def subscribe(self, listener: SessionChangeListener) -> Disposable: ...
    async def on_start(self, ctx) -> None: ...  # create tables


class Session:
    session_id: str

    # Writes
    async def append_message(
        self, message: UIMessage, *, source: Source = "server"
    ) -> AppendResult: ...
    async def upsert_message(
        self, message: UIMessage, *, source: Source = "server"
    ) -> AppendResult: ...
    async def update_message(
        self, message: UIMessage, *, source: Source = "server"
    ) -> UIMessage | None: ...
    async def delete_messages(self, message_ids: Sequence[str]) -> None: ...
    async def clear_messages(self) -> None: ...

    # Reads (the active path, root → leaf)
    def history(self) -> AsyncIterator[UIMessage]: ...
    def history_batches(
        self, *, batch_size: int = 50, max_batch_bytes: int = 4 * 1024 * 1024
    ) -> AsyncIterator[list[UIMessage]]: ...
    async def get_history(self) -> list[UIMessage]: ...
    async def get_recent_history(
        self, max_content_bytes: int
    ) -> RecentHistoryResult: ...
    async def get_history_row_stats(self) -> list[SessionRowStat]: ...

    # Change feed
    def mirror(
        self,
        *,
        get: Callable[[], list[M]],
        set: Callable[[list[M]], None],
        transform: Callable[[UIMessage], M] | None = None,
    ) -> Disposable: ...


Source = Literal["server", "client"]


@dataclass(slots=True, kw_only=True)
class AppendResult:
    inserted: bool  # False: the id already existed (idempotent no-op)
    message: UIMessage  # the STORED (sanitized) form


@dataclass(slots=True, kw_only=True)
class RecentHistoryResult:
    messages: list[
        UIMessage
    ]  # newest messages that fit the budget, root → leaf; always ≥ the leaf
    truncated: bool  # older messages were left out
    total_content_bytes: int  # stored size of the FULL path


@dataclass(slots=True, kw_only=True)
class SessionRowStat:
    id: str
    role: str
    bytes: int  # message row + its continuation rows
    token_estimate: int  # stamped at write time
```

Notes:
- `append_message` is kept, even though `AIChatAgent` only calls `upsert`,
  because `upsert_message` is implemented as "append if new, otherwise update"
  (upstream `handle.ts`).
- `append_message` always appends after the active leaf. Upstream's
  `parentId` option (explicit parent or new root) exists only for branching,
  which is deferred.
- `history()` is the base that `history_batches()` and `get_history()` are
  built on.
- Removing listeners: upstream returns an unsubscribe function. Python returns
  a `Disposable`, which fits an `AsyncExitStack`
  ([core_disposable_store.md](./core_disposable_store.md)).
- The change listener type and `mirror` defaults follow §5.

---

## 4. Storage

### 4.1 Tables

Upstream's table names, keyed the same way. All tables are `WITHOUT ROWID`
with composite primary keys and **no secondary indexes**: on DO SQLite a row
write costs about 1000× a row read, so every index would charge every append.

Two tables: `cf_agents_session_messages` (one row per message: `seq`,
`parent_id`, `type`, `role`, slice 0 of the JSON, `content_chunks`,
`token_estimate`, `created_at`, `content_hash`) and
`cf_agents_session_message_chunks` (continuation slices). The exact DDL,
checked against upstream `sessions/core.ts:147`, is in
[sql_schemas.md](./sql_schemas.md) §7. `content_hash` is the digest of the
full message JSON that the no-op update check compares (§4.2). Compaction,
attachment, config, and FTS tables are not created in phase 1 (§7).

**`parent_id` and `seq` stay in phase 1** even though `AIChatAgent` is linear.
They cost nothing extra, and keeping them means branches can be added later
without a migration.

### 4.2 Write rules

- **Idempotent append:** appending an existing id writes nothing and returns
  `inserted=False` with the stored row.
- **No-op updates write nothing:** `update_message` compares a digest of the new
  message JSON with the stored `content_hash` (which covers the **full** JSON,
  not just slice 0); if they're equal, no write and no event. Returns `None`
  when the id isn't in the session.
- **One transaction per write:** a message and its continuation rows are
  written, replaced, and deleted together in a single synchronous SQLite
  transaction (`ctx.storage.transactionSync`, verified to work with a Python
  callable, [platform_verification.md](./platform_verification.md) §3.4).
- **The tail is in memory:** the active leaf id and the next `seq` are read once
  per wake and then kept in memory, so a linear append writes only the message
  row(s). There are no counter rows.
- **Sanitization** before every write (§6).
- **Token estimate** stamped on every row (§6.2).

### 4.3 Row splitting

- **The row budget is 1.5 MiB** (`1536 * 1024` bytes) of message JSON per row.
  A message under it is one row with `content_chunks = 0`, which is the common
  case.
- **A larger message is cut into slices by UTF-8 byte length.** Slice 0 goes in
  the message row, slices 1..n in `cf_agents_session_message_chunks`. Reads
  concatenate them.
- **Slices are cut on code-point boundaries.** In Python, `str` is a sequence of
  code points (there are no surrogate pairs to keep together, unlike JS), so
  cut by accumulated `len(ch.encode("utf-8"))` per code point. The invariant to
  test: `"".join(split_content(s)) == s` for multi-byte and emoji content that
  straddles a boundary.
- **No maximum size and no truncation.** Sessions stores whatever it's given.
  Bounding untrusted input is the application's job.

### 4.4 Reads

Upstream bounds reads because workerd SQLite shares the isolate's memory
budget, so an oversized result set fails with `SQLITE_NOMEM`. Phase 1 keeps
the same bounds:

- **Walk the path first, without content:** one recursive CTE from the leaf
  over `id, parent_id, role, token_estimate`, and stored sizes, capped at
  10,000 rows.
- **Then fetch content in windows** of at most 50 rows **and** 4 MiB of stored
  JSON. Continuation rows are fetched in one extra query, only for ids in the
  window with `content_chunks > 0`.
- **`get_recent_history(max_content_bytes)`** charges each message its full
  stored size (row plus continuation rows), works back from the leaf until the
  budget is spent, and **always includes the leaf**. There is no minimum message
  count.
- **Unparseable rows are skipped.**

---

## 5. Change feed

Listeners run **after** the write commits. A listener that raises is reported
(upstream: a `session:error` event) and the other listeners still run; it never
turns a committed write into a failed call. That is a concrete failure the
isolation handles, so it stays.

**Implementation:** the feed is an `Emitter` dispatched with `fire_async`
(listeners awaited one at a time, in subscription order, errors isolated;
[core_disposable_store.md](./core_disposable_store.md) §5.4). Sessions also
emits `session:error` through the Lifecycle `events` service when a listener
fails.

**Phase-1 events:**

```python
SessionChangeEvent = (
    AppendEvent(session_id, message, inserted: bool)
    | UpdateEvent(session_id, message)
    | DeleteEvent(session_id, message_ids: list[str])
    | ClearEvent(session_id)
)
```

(Upstream also has `compact`, `compaction`, and `import`, all deferred.)

**`mirror` reduction** (upstream `sessions/mirror.ts`):

| Event | Effect on the cached list |
| --- | --- |
| `append`, `inserted=True` | Replace the entry with that id, or append it |
| `append`, `inserted=False` | Nothing |
| `update` | Replace the entry with that id; ignore it if the cache doesn't hold that id |
| `delete` | `set(...)` the list without the removed ids |
| `clear` | `set([])` |

`get` is called on every event, so a host that reassigns its list is followed
rather than shadowed. Upstream's `intercept` and `on_applied` hooks exist for
Think's branch and compaction cases, and are deferred.

---

## 6. Sanitization and token estimates

These are needed in phase 1 for two reasons: every write uses them, and the
chat layer imports them directly (upstream `chat/sanitize.ts` imports from
`sessions/sanitize`).

### 6.1 `sessions/sanitize.py`

- `byte_length(s) -> int`: UTF-8 length (upstream measures without making a
  full encoded copy; in Python, `len(s.encode("utf-8"))` is the simple
  version).
- `sanitize_message(message) -> UIMessage` (upstream `sanitize.ts`):
  1. remove OpenAI's ephemeral `itemId` and `reasoningEncryptedContent` from
     `providerMetadata.openai` / `callProviderMetadata.openai`, dropping keys
     left empty;
  2. drop reasoning parts that are truly empty (blank text and no remaining
     `providerMetadata`).
- `source="client"` writes also remove the configured
  `reserved_metadata_keys` from `message.metadata`.

### 6.2 `sessions/tokens.py`

The token estimate stamped on each row. It's a heuristic, because a real
tokenizer would cost about 100 MB of heap. Upstream constants:
`CHARS_PER_TOKEN = 4`, `WORDS_TOKEN_MULTIPLIER = 1.3`,
`TOKENS_PER_MESSAGE = 4`, `IMAGE_ATTACHMENT_TOKENS = 1_600`,
`MAX_ATTACHMENT_TOKENS = 20_000`. A message's estimate is the larger of
characters/4 and words×1.3, plus the per-message charge, with attachments
charged a flat or capped amount. In phase 1 it is stamped and reported by
`get_history_row_stats`; nothing triggers on it until compaction exists.

---

## 7. Deferred (not in phase 1)

| Upstream feature | Why it's deferred |
| --- | --- |
| **Branches**: `parentId` on append, `getBranches`, `getLatestLeaf`, `getMessage`, reading by `leafId`, `newestFirst` | `AIChatAgent` is linear. The schema keeps `parent_id`, so nothing blocks adding them. |
| **Compaction**: overlays, `addCompaction`, `getCompactions`, `compact`, `onCompaction`, `compactAfter`, `createCompactFunction` | Think and examples only |
| **FTS search**: `search()`, `cf_agents_session_fts` | Neither upstream host calls it |
| **Separate media storage** (content-addressed attachment tables, `attachment:sha256:` pointers) | Not required for correctness: large media stays inline and is handled by row splitting, and byte budgets stay accurate because rows reflect their real size. This gives up upstream's small rows and payload de-duplication. Revisit if image-heavy chats become a target. |
| `importMessage` and every legacy migration | No legacy Python data |
| Named multi-session usage beyond the default handle | `session(id)` works, but phase 1 is designed and tested for the default handle |
| `intercept` / `on_applied` mirror hooks | Only needed for branch and compaction events |

---

## 8. Decisions

1. **`AIChatAgent.messages` is read-only.** It is the conversation history: an
   in-memory mirror of the stored transcript, loaded at startup within
   `hydrationByteBudget` (so it can be the most recent window of a longer
   transcript) and kept in sync through `mirror`. Python exposes it as a
   read-only property. Writes go through `AIChatAgent`'s `persist_messages` /
   `save_messages` ([ai_chat_agent_api.md](./ai_chat_agent_api.md) §1).
   (Upstream keeps a public, writable array only for backwards
   compatibility, `ai-chat/src/index.ts:940`.)
2. **The `get-messages` HTTP route is in scope.** `useAgentChat` fetches its
   initial messages from it (`agents/src/chat/react.tsx:978`): any HTTP
   request to the agent whose **last path segment is `get-messages`** returns
   the full transcript as a JSON array (`content-type: application/json`),
   streamed in `history_batches()` batches so the whole transcript is never
   held in memory (`ai-chat/src/index.ts:1742`, `:2589`). Documented in
   [agents_wire_protocol.md](./agents_wire_protocol.md) §7.6.
3. **The schema is confirmed** against upstream `sessions/core.ts`; see
   [sql_schemas.md](./sql_schemas.md) §7.
