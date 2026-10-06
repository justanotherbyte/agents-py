# Sessions engine (design pass, step 11)

The machinery behind the phase-1 `Sessions` capability: storage and row
splitting, the write paths (including the synchronous one chat needs), path
reads under byte budgets, deletion that keeps the chain intact, the change
feed, sanitization, and token estimates. The **API and scope** are in
[sessions_api.md](./sessions_api.md) (decided); this doc is the internals,
from reading upstream.

Upstream: `packages/agents/src/sessions/` (`core.ts` 1,515 lines, `handle.ts`,
`sessions.ts`, `mirror.ts`, `sanitize.ts`, `tokens.ts`, `chunking.ts`,
`types.ts`); chat's use in `ai-chat/src/index.ts` (`:6300`–`:6345`).

Status: **decided and implemented** (2026-10-06). §5 records the questions and
answers; §6 records how the implementation turned out.

---

## 1. How upstream works (phase-1 parts)

- **A capability with no alarm and no jobs**, like Streams: storage and events
  only, so it works on facets unchanged. Not installed by `Agent`.
  `session(id)` returns a cached handle; handles and subscriptions may be
  taken before the capability is installed (services are reached lazily).
- **Messages are loose JSON objects** (`SessionMessage`: `id`, `role`,
  `parts`, optional `metadata`). Sessions never interprets parts beyond what
  sanitization and token estimates read, so new part types pass through.
- **Rows.** One `cf_agents_session_messages` row per message (`seq`,
  `parent_id`, `role`, slice 0 of the JSON, `content_chunks`,
  `token_estimate`, `created_at`, `content_hash` = SHA-256 hex of the full
  JSON), and continuation slices in `cf_agents_session_message_chunks`. JSON
  over 1.5 MiB is cut by UTF-8 bytes, never inside a character.
- **The tail is cached.** `(leaf_id, next_seq)` per session is read once
  (`ORDER BY seq DESC LIMIT 1`; `next_seq` starts at 1) and then kept in
  memory: an append writes only its rows. Deletes forget it; clear resets it.
- **Append**: an id that exists returns the stored message
  (`inserted=False`). Otherwise one `transactionSync` writes the row (parent =
  the cached leaf) and its continuations; then the tail moves to it.
- **Update**: reads only the key columns; an equal `content_hash` writes
  nothing (`unchanged`, no event). Otherwise one transaction rewrites the
  row, deletes surplus continuations of a message that shrank, and writes the
  new ones. A missing id returns `None`.
- **Upsert** = append if the id is new, otherwise update.
- **The synchronous upsert** (`__DO_NOT_USE_WILL_BREAK__sync()` on a
  session): `upsert(message)` does the write now and returns
  `{result, after}`, where `after()` runs the change feed later; `abandon()`
  forgets the cached tail. **Chat uses it** to save the finished turn's
  messages inside the Streams cutover's `commit`, so the message lands in the
  same transaction that settles and drops the stream (`index.ts:6328`). If
  that transaction rolls back, chat calls `abandon()`, because the tail cache
  already counted the writes. This nests Sessions' `transactionSync` inside
  Streams'.
- **Delete** (`deleteMessages(ids)`): one transaction first **rewires the
  children** of deleted rows to their nearest surviving ancestor (a recursive
  CTE), then deletes the rows and their continuations, so the chain stays
  connected. Then the tail cache is forgotten.
- **Clear**: deletes the session's rows and continuations; the tail resets
  to empty.
- **Reads walk the path first, without content**: one recursive CTE from the
  leaf over ids and stored sizes (row plus continuations, in UTF-8 bytes),
  capped at 10,000 rows. Content is then fetched in windows of at most 50 rows
  and 4 MiB, with continuations in one extra query per window. A row that
  doesn't parse into `{id: str, role: str, parts: list}` is skipped.
  - `get_recent_history(budget)` charges each message its stored size, works
    back from the leaf until the budget is spent, always includes the leaf,
    and reports `truncated` (budget, or the path cap with an older parent
    still there) and the full path's `total_content_bytes`.
  - `history_batches` groups `history()` into batches of at most 50 messages
    and 4 MiB of JSON (measured on each message's JSON text).
- **Change feed**: listeners are awaited in subscription order after the
  write; one that raises is logged and reported as `session:error`
  (`{sessionId, event, error}`), and the rest still run. `mirror` reduces
  events onto a host list (sessions_api.md §5).
- **Writes are prepared** by `sanitize_message`, then stripping the reserved
  metadata keys for `source="client"`, then estimating tokens: the message
  estimate plus, for `file` parts with a `data:` URL, an attachment estimate.
- **Events**: `session:message:appended` (`{sessionId, messageId,
  tokenEstimate}`), `session:message:updated` (`{sessionId, messageId}`),
  `session:messages:deleted` (`{sessionId, count}`), `session:cleared`
  (`{sessionId}`), and `session:error`.

---

## 2. Python design

### 2.1 A straight port

- The two tables (sql_schemas.md §7), the 1.5 MiB row split, the cached tail,
  append/update/upsert with the hash check, the rewiring delete, clear, the
  path walk with its cap, the read windows, `get_recent_history`'s budget and
  `truncated` rules, `history_batches`, skipping unparseable rows,
  `mirror`, the feed's error isolation, sanitization, token estimates, and the
  five events with upstream's payloads (added to observability.md).
- **The synchronous upsert is ported** as `Session._upsert_sync(message, *,
  source) -> tuple[AppendResult, Callable[[], Awaitable[None]]]` and
  `Session._abandon()`. Not in sessions_api.md, which listed only the public
  surface; step 12 needs it for the cutover.
- **Reads `await lifecycle.ready()`; the synchronous upsert doesn't** (chat
  calls it inside its own write path).
- Schema version 1, no legacy migration, no `import_message`
  (sessions_api.md §7).

### 2.2 Already decided (sessions_api.md)

Phase-1 scope and deferrals (§7: no branches, compaction, FTS, attachment
tables, import); `subscribe` / `mirror` return `Disposable`; the feed is an
`Emitter` dispatched with `fire_async`; no `signal` (cancel the reading task).

### 2.3 Smaller choices made here

1. **JSON text is compact and keeps non-ASCII characters**
   (`separators=(",", ":"), ensure_ascii=False`), like `JSON.stringify`, so
   stored sizes, byte budgets, and row splits match upstream. Byte lengths use
   `encode("utf-8", "surrogatepass")`, so a string holding a lone surrogate
   (possible in text that came from JS) is measured rather than raising.
2. **Token estimates count code points.** Upstream divides UTF-16 length by 4;
   Python's `len()` counts code points, so text outside the Basic
   Multilingual Plane (emoji) estimates slightly lower. It's a heuristic
   either way. Words are counted with `str.split()`.
3. **No path-token memo.** Upstream memoizes the path's token total for
   auto-compaction, which is deferred.
4. **Feed listener failures are logged at `WARNING`** on `agents.sessions`,
   plus the `session:error` event, as upstream (`console.warn`).
5. **Every write needs a started capability**, except the synchronous upsert.
   Upstream creates tables lazily on first use too; here `on_start` creates
   them, and `_upsert_sync` is only reached from chat code after startup.

---

## 3. Module layout (`src/agents/sessions/`)

| Module | Holds | Upstream |
| --- | --- | --- |
| `types.py` | `SessionMessage`, `AppendResult`, `RecentHistoryResult`, `SessionRowStat`, the change events, `Source` | `types.ts` |
| `sanitize.py` | `byte_length`, `sanitize_message` | `sanitize.ts` |
| `tokens.py` | the estimate constants and functions | `tokens.ts` |
| `chunking.py` | `split_content` | `chunking.ts` |
| `store.py` | DDL and every statement | `core.ts` |
| `sessions.py` | `Sessions` (the capability, the feed) and `Session` (the handle, `mirror`, the synchronous upsert) | `sessions.ts`, `handle.ts`, `mirror.ts` |

---

## 4. Finding: Streams stored non-ASCII text escaped (step 10, fixed)

Found comparing JSON encodings for this pass.

- Streams serializes chunks with `json.dumps(..., separators=(",", ":"),
  allow_nan=False)`. Python's default `ensure_ascii=True` writes every
  non-ASCII character as `\uXXXX`. `JSON.stringify` keeps them as they are.
- The result is still valid JSON and reads back the same, but it's **larger**:
  an emoji takes 12 bytes instead of 4, and accented or CJK text 6 bytes per
  character instead of 2–3. Chat streams carry model output in any language,
  so a stream's blocks and its `max_chunk_bytes` check would count 2–3 times
  upstream's size for such text.
- **Fixed (2026-10-06):** `ensure_ascii=False` in Streams' `_dumps`, and the
  size check measures with `surrogatepass` (§2.3 item 1). Test
  `test_non_ascii_text_is_stored_as_is`: the text is stored unescaped, and
  the size limit counts 4 bytes per emoji. Recorded in
  [streams_engine.md](./streams_engine.md) §6 item 7.

---

## 5. Questions to decide

**Q1. What Sessions stores and returns.** **Decided: (a) wire-form JSON
objects.** sessions_api.md §3 says
`UIMessage`: the typed dataclass tree from chat_models.md §5 (one class per
part type and per tool state, wire names). That model doesn't exist yet; it
belongs to the chat layer, step 12.

- (a) **Wire-form JSON objects.** **Recommended.** `SessionMessage` is a
  `TypedDict` (`id`, `role`, `parts: list[dict[str, JSONValue]]`, optional
  `metadata`), as upstream's own `SessionMessage` is a loose object. Chat
  (step 12) converts between it and the typed `UIMessage` at its boundary.
  - Sessions never depends on the chat layer, and part types it doesn't know
    (a new AI SDK version) pass through untouched.
  - Sanitization and token estimates port directly, working on the same
    dictionaries upstream's do.
  - Reads don't build a dataclass tree for every message: hydrating up to
    32 MiB of history at chat startup would otherwise decode every part.
  - The cost: someone using Sessions directly gets dictionaries, not typed
    messages.
- (b) **Typed `UIMessage`.** Build `agents/chat/messages.py` (the model and
  its JSON codec) now, in step 11, and type Sessions with it, as
  sessions_api.md §3 says. Sessions then depends on the chat model, and
  every read decodes into dataclasses.

**Q2. The §4 Streams fix: now, or with Sessions?** **Decided: (a) now.**
- (a) **Now, as its own change, with a test.** **Recommended:** it's a
  step 10 issue, independent of Sessions.
- (b) As part of the Sessions implementation.

---

## 6. Implementation notes (step 11, 2026-10-06)

Done: `src/agents/sessions/`; 480 tests in total (18 new, in
`tests/sessions/`, plus 1 for the §4 Streams fix). Checked on `workerd`
(`verify/results/sessions_client.py`, `sessions_report.json`):
- appends, an edit, an identical update (no write), and deleting a middle
  message: the history read back in order, the deleted message's child now
  points at its grandparent, and a `mirror`ed list followed every change;
  accented and emoji text round-tripped unescaped;
- `get_recent_history(1)` returned only the leaf, marked `truncated`;
- a ~2 MiB message became a 1.5 MiB row plus one 528 KB continuation, read
  back intact, with `get_history_row_stats` reporting the full 2,101,214 bytes;
- **the synchronous upsert nested in a Streams cutover** (`transactionSync`
  inside `transactionSync`): a `commit` that upserted and then raised rolled back
  both the session row and the stream settle (the stream stayed `streaming`,
  the history unchanged after `_abandon`), and a successful one stored the reply
  and dropped the stream's rows in one transaction;
- Streams stored `"héllo 🌍"` as 13 bytes (the §4 fix, on the real runtime).

How it turned out, beyond §2:

1. **The engine lives on `Sessions`, the API on `Session`.** Upstream splits
   `SessionsCore` (storage, the tail cache, the feed) from the handle. Here
   the core's operations are private methods of the capability, and the
   handle calls them; every SQL statement is in `store.py`.
2. **The feed wraps each listener** to log and emit `session:error` (with the
   event's type), then dispatches through `Emitter.fire_async` as decided.
   The `Emitter` alone logs a failure but can't say which event it was.
3. **Splitting works on the encoded bytes.** The message is encoded once, and
   cuts back up off UTF-8 continuation bytes, rather than walking characters
   in Python (slow for a multi-megabyte message under Pyodide).
4. **An empty `openai` metadata object is still stripped**, as upstream (in
   JavaScript `{}` is truthy): the key goes, and so does the metadata if
   nothing else is left.
5. **Token estimates measure non-string values as compact JSON**, as
   `JSON.stringify` writes them.
6. **A message that isn't JSON raises** `TypeError` or `ValueError` from the
   write (for example NaN in a tool output): misuse, so built-in exceptions.
   Upstream's `JSON.stringify` would write NaN as `null`.
7. **`mirror` matches ids by key or attribute**, so a host can mirror stored
   dictionaries or typed messages from its `transform` (chat's case).
8. **The SQL text is literal** throughout, so the path cap (10,000) is spelled
   out in the queries, next to `MAX_PATH_DEPTH`.

