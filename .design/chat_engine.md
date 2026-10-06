# Chat engine: plan for step 12 (`agents.chat` + `AIChatAgent`)

Step 12 is larger than every earlier step together: upstream's server side is
about 7,900 lines of `ai-chat/src/index.ts` plus about 30 server modules in
`agents/src/chat/` (another ~10,000 lines). The user-facing surface is already
designed ([ai_chat_agent_api.md](./ai_chat_agent_api.md),
[chat_models.md](./chat_models.md), wire §7 in
[agents_wire_protocol.md](./agents_wire_protocol.md), scope.md §2.4). The
internals aren't. This doc splits the step into sub-steps, each opened by
its own design pass (as for Tasks, sub-agents, fibers, Streams, and
Sessions), and sorts every upstream module into one of them.

Status: **plan decided** (2026-10-06; §3). **12a and 12b done** (§4.5,
§5.6). Each sub-step's design pass is added below as it starts.

---

## 1. Upstream module inventory

### 1.1 Server modules (`agents/src/chat/`) and where each goes

| Module | Lines | What it is | Sub-step |
| --- | --- | --- | --- |
| `message-builder.ts` | 785 | `applyChunkToParts`: folds chunks into message parts (tool states only move forward, late tool input after approval, `data-*` reconciliation) | 12a |
| `stream-accumulator.ts` | 265 | Builds a whole message from chunks (start / finish / metadata / error around the builder) | 12a |
| `message-reconciler.ts` | 470 | Aligns client-sent messages with stored ones before saving | 12a |
| `tool-state.ts` | 332 | Applying a tool result / approval to a message's tool part | 12a |
| `repair-transcript.ts` | 236 | Settles interrupted tool calls so the next model call doesn't fail | 12a |
| (`index.ts` `ai-chat`) sanitize-for-persistence, `_truncateLargeStrings` | — | Chat's write-side hygiene on top of Sessions' | 12a |
| `protocol.ts`, `wire-types.ts`, `parse-protocol.ts` | 445 | Frame constants, terminal outcomes, parsing incoming chat frames | 12b |
| `lifecycle.ts` | 355 | Public result / option types (`SaveMessagesResult`, `MessageConcurrency`, recovery config types) | 12b (recovery types in 12d) |
| `turn-queue.ts` | 117 | Serial turn queue with generation invalidation | 12b |
| `submit-concurrency.ts` | 188 | Policies for a send arriving mid-turn (queue, latest, merge, drop, debounce) | 12b |
| `abort-registry.ts` | 122 | Per-request abort signals | 12b (becomes task cancellation) |
| `resumable-stream.ts` | 1,255 | Chat's adapter over Streams: segment coalescing, replay, the cutover | 12b |
| `chunk-size.ts`, `replay-frames.ts`, `origin-message-ids.ts`, `connection.ts`, `async-helpers.ts` | 270 | Small helpers for the above | 12b |
| `resume-handshake.ts` | 349 | Server side of the resume handshake (wire §7.4) | 12b |
| `pre-stream-turns.ts` | 143 | Accepted turns that haven't started streaming, and connections parked on them | 12b |
| `turn-task.ts` | 132 | Every turn runs as one journaled Task step | 12b (the replay branch's recovery in 12d) |
| `continuation-state.ts`, `auto-continuation-controller.ts` | 459 | Continuing a turn automatically after client tool results / approvals | 12c |
| `client-tools.ts` | 119 | Client tool schemas; upstream turns them into AI SDK tools (Python passes the schemas through `ChatMessageOptions.client_tools`) | 12c |
| `recovery.ts`, `recovery-codec.ts`, `recovery-engine.ts`, `recovery-incident.ts`, `recovery-task.ts` | 2,539 | Recovering interrupted turns: snapshots, incident budgets, backoff, continue-or-retry, exhaustion | 12d |
| `orphan-persist.ts`, `orphan-store.ts` | 125 | Saving the partial message of a stream left with no producer | 12d |
| `stall-watchdog.ts` | 99 | Turning a silent model stream into bounded recovery | 12d |
| `sanitize.ts` (row size for non-session tables), `sql-batch.ts` | 180 | Helpers; ported where a sub-step needs them | as needed |

### 1.2 Client-only (not ported: the browser client is upstream's own)

`ws-chat-transport.ts`, `broadcast-state.ts`, `chat-throttle.ts`,
`replay-batch.ts`, `replay-dedupe.ts`, `transport.ts`, `text-segment-joiner.ts`.

### 1.3 Out of scope (already decided)

- **Agents as tools** (`agent-tools.ts`, `runAgentTool`, detached delivery,
  milestones, the `agent-tool-event` frames): scope.md §3, sql_schemas.md
  §13. About 1,500 lines of `index.ts` are this.
- **Legacy migrations** (`ai-chat-v5-migration.ts`, the legacy message table):
  no legacy Python data.
- **MCP** (`waitForMcpConnections`) and **provider adapters** (scope.md §3).

### 1.4 Read-time helpers (not used by the server)

`truncate-older-messages.ts` and `tool-output-truncation.ts` are exported for
user code to trim context before a model call. `AIChatAgent` doesn't import
them. See §3 Q2.

---

## 2. Sub-steps

Each opens with a design pass (a section of this doc), then is implemented,
tested, and checked on `workerd` before the next.

**12a. Chat models and pure transforms.** `agents/chat/chunks.py` and
`messages.py` (the typed models from chat_models.md, with their wire codecs to
and from Sessions' `SessionMessage`), the message builder, the stream
accumulator, the reconciler, tool-state updates, transcript repair, and chat's
persistence sanitization. No I/O: unit tests against upstream's behavior,
plus a round-trip check of AI SDK v6 shapes.

**12b. `AIChatAgent`: the turn path.** Chat frames, persistence through
Sessions with `self.messages` mirrored, `get-messages`, the turn queue and
submit concurrency, consuming `on_chat_message` (typed chunks or `str`) with
the `start` / `finish` rewrites, resumable streams on Streams with the
cutover, the resume handshake and pre-stream turns, clear and cancel,
`persist_messages` / `save_messages` / `continue_last_turn` /
`delete_messages`, `on_error`, and every turn running as a Task. Checked on
`workerd` with upstream's own browser client (`useAgentChat` /
`WsChatTransport`) against a Python server.

**12c. Client tools and approvals.** Tool results and approvals from the
client, `cf_agent_message_updated`, waiting for pending interactions, and
auto-continuation (the debounced continue after a complete tool batch).

**12d. Recovery.** The turn Task's replay branch, the recovery engine and its
incidents (budgets, backoff, continue vs. retry, exhaustion), the recovery
Task, orphan persistence, the stall watchdog, `on_chat_recovery` and its
config, the `cf_agent_chat_recovering` frames, facet-hosted turns on fibers
(scope.md §2.7), and the memory-limit seal. Checked on `workerd` with a real
isolate crash mid-turn.

---

## 3. Questions to decide

**Q1. The sub-step plan (§2).** **Decided: (a) sub-steps 12a → 12d.**
- (a) **12a → 12d as above, each with its own design pass.** **Recommended.**
  Each sub-step leaves a working, tested layer: 12b alone is a usable chat
  agent (without tool interaction or crash recovery).
- (b) One design pass for all of step 12, then implement it in one go.

**Q2. The read-time truncation helpers (§1.4).** **Decided: (a) defer.**
- (a) **Defer.** **Recommended.** Nothing on the server or the wire needs
  them, and they can be added later as plain functions without touching the
  rest.
- (b) Port them in 12a, alongside the other pure transforms.

---

## 4. Sub-step 12a: chat models and pure transforms (design pass)

Status: **decided** (2026-10-06). §4.4 records the questions and answers.

### 4.1 How upstream works

- **Everything inside the chat layer works on plain wire-form objects.** The
  AI SDK's `UIMessage` *is* a JSON object, so the builder, accumulator,
  reconciler, tool-state updates, and transcript repair all read and write
  `{type, toolCallId, state, …}` dictionaries, in camelCase, as stored and
  sent.
- **The builder** (`applyChunkToParts`, 785 lines) mutates a message's parts
  in place, one chunk at a time (text and reasoning `streaming` → `done`,
  tool states moving only forward, `data-*` parts updated by `type` + `id`,
  `transient` data skipped, `start-step` → a `step-start` part). Two side
  tables keyed by part identity hold a tool call's raw input text (so a
  partial JSON string is never saved) and whether its input is still
  provisional (from a delta, replaceable by a late `tool-input-available`
  after an approval request). `normalizeToolInput` forces tool input to a
  JSON object. `isReplayChunk` spots chunks that would move a tool part
  backwards.
- **The accumulator** wraps the builder for a whole message: `start`
  (message id, metadata), `finish` (finish reason, metadata),
  `message-metadata`, `error`, a tool result for a call in an *earlier*
  message (a "cross-message" update the host applies), and continuation
  (merging into the last assistant message, replaying chunks that arrived
  before the host knew which message that was).
- **Tool-state updates** (`tool-state.ts`): small builders that apply a
  client tool result, a cross-message result, or an approval to the matching
  part, plus predicates for pending client interaction and incomplete tool
  batches.
- **The reconciler** aligns client-sent messages with stored ones before
  saving: exact id, then the same tool calls, then the same content (for
  assistant messages); merges server-known tool outputs into stale client
  copies; drops stale duplicate assistant copies echoed in one submit.
- **Transcript repair** settles tool calls left without a result (cut off
  mid-stream) and normalizes malformed tool input, before a recovered turn
  calls the model again.
- **Persistence sanitization** (in `AIChatAgent`): provider-executed tool parts
  get strings over 500 characters truncated with a marker, except `web_search`
  / `web_fetch` and keys starting with `encrypted`; then the overridable
  `sanitizeMessageForPersistence` hook.

### 4.2 Python design

**The split: wire form inside, typed at the edges (§4.4 Q1).** The chat
layer's internals work on wire-form dictionaries, ported close to line for
line, exactly as Sessions stores them. The typed models from chat_models.md
are the public surface, converted at the edges:

| Edge | Conversion |
| --- | --- |
| `on_chat_message` yields typed chunks | each chunk → wire dict once (it is sent and stored as JSON anyway) |
| `self.messages` | stored dicts → typed `UIMessage`s, in the Sessions `mirror`'s `transform` |
| `persist_messages` / `save_messages` arguments | typed → wire dicts |
| hooks that receive messages (`sanitize_message_for_persistence`, `on_chat_response`, …) | typed in and out |
| incoming client frames | already wire dicts; stay so |

**Module layout (`src/agents/chat/`):**

| Module | Holds | Upstream |
| --- | --- | --- |
| `chunks.py` | the chunk dataclasses (chat_models.md §2) | AI SDK types |
| `messages.py` | `UIMessage`, the part dataclasses, `UnknownPart` (§4.4 Q2), `ChatMessageOptions`, `ClientToolSchema` | AI SDK types, `index.ts` |
| `codec.py` | `chunk_to_wire`, `message_to_wire`, `message_from_wire`, `part_from_wire` | (Python) |
| `builder.py` | `apply_chunk_to_parts`, `normalize_tool_input`, late tool input, `is_replay_chunk`, `partial_stream_text` | `message-builder.ts` |
| `accumulator.py` | `StreamAccumulator`, `ChunkAction` | `stream-accumulator.ts` |
| `tool_state.py` | the update builders and predicates | `tool-state.ts` |
| `reconciler.py` | `reconcile_messages`, `resolve_tool_merge_id`, `reconcile_orphan_partial` | `message-reconciler.ts` |
| `repair.py` | `repair_interrupted_tool_parts` | `repair-transcript.ts` |
| `persistence.py` | the provider-tool payload truncation | `index.ts` |

**Codec rules.**
- The wire `type` comes from each class's `ClassVar`, or is computed:
  `data-{name}` for data parts and chunks, `tool-{tool_name}` (or
  `dynamic-tool`) for tool parts, whose `state` comes from the class. Static
  tool parts also carry `toolName`, as upstream's builder stores it.
- Optional fields that are `None` are left out; required fields are always
  written, even `None` (a tool's `output: null`). camelCase through `wire()`.
- Decoding picks the class by `type`, then (for tools) by `state`.
- **Anything that doesn't decode cleanly stays raw**: a part type the models
  don't know (an AI SDK v7 part from a newer client) or a known type missing a
  required field becomes `UnknownPart(fields=…)`, which encodes back exactly.
  Unknown *fields* on a known part are dropped when it's decoded (the v6 set
  is complete per `ai@6.0.300`; v7 additions are deferred, chat_models.md
  §2.4).

**The builder's side tables.** Upstream keys them by part identity in module
`WeakMap`s. Python dictionaries can't be weakly referenced, so the tables live
in a small scratch object (keyed by `id(part)`, holding the part so the id
can't be reused) that the accumulator owns. A bare `apply_chunk_to_parts`
call gets a fresh one.

**JSON comparisons** (the reconciler's content keys, chat's "changed?" check)
use compact `json.dumps(..., ensure_ascii=False)`, like `JSON.stringify`.
Upstream's `stableStringify` becomes `json.dumps(sort_keys=True)`.

**Out of 12a**: the `start` / `finish` rewrites and the plain-text path
(chat_models.md §3) belong to the turn path (12b), which applies them before
chunks reach the builder.

### 4.3 Verification

- **Upstream's own tests, ported**: `message-builder-approval-input`,
  `stream-accumulator`, `tool-state`, `message-reconciler`, and
  `repair-transcript` (their cases become pytest cases).
- **An oracle from upstream's code**, like the cron port's: Node 24 runs
  `message-builder.ts`, `stream-accumulator.ts`, and
  `message-reconciler.ts` directly (they import only types), over a fixture
  set of chunk sequences and message lists; the Python results must match
  their JSON exactly (`tests/chat/*_oracle.json`, with the generator script).
- **Round-trips**: every part and chunk class through the codec, AI SDK v6
  shapes from the `ai` package's type definitions, and unknown parts surviving
  decode → encode unchanged.

### 4.4 Questions to decide

**Q1. Where the typed models are used.** **Decided: (a) dictionaries inside,
typed at the edges.**
- (a) **Wire-form dictionaries inside the chat layer, typed models at the
  edges** (§4.2). **Recommended.**
  - The internals port line for line and match upstream on the same inputs,
    which the oracle can check.
  - Chunks are converted once, on the way in. Everything after that (the
    wire, the stream store, Sessions) is JSON anyway.
  - Client frames arrive as dictionaries and never need decoding.
  - The cost: internal code works with dictionaries rather than typed objects.
- (b) **Typed models throughout**, as chat_models.md §5's "a state change
  replaces the part with an instance of the next state's class" assumed. The
  builder and reconciler are rewritten for dataclasses; every chunk, client
  message, and stored message is decoded first; and the builder's side tables
  must follow a part across each replacement.

**Q2. Parts the models don't know.** **Decided: (a) keep them raw.**
- (a) **Keep them raw** (`UnknownPart`, re-encoded exactly; §4.2).
  **Recommended:** a newer client's part (or a malformed one) can't break
  loading `self.messages`, and isn't lost when the transcript is saved again.
- (b) Raise on decode. Simpler, but one unknown part in storage would break
  every load of that conversation.

### 4.5 Implementation notes (12a, 2026-10-06)

Done: `src/agents/chat/` (`chunks`, `messages`, `codec`, `builder`,
`accumulator`, `tool_state`, `reconciler`, `repair`, `persistence`, `_json`)
and `core/records.py` (`wire()`); 564 tests in total (84 new, in
`tests/chat/`).

- **The oracle.** `tests/chat/oracle/generate.mjs` runs upstream's own
  `message-builder.ts`, `stream-accumulator.ts`, `message-reconciler.ts`,
  `tool-state.ts`, and `repair-transcript.ts` under Node 24 (types stripped,
  relative imports given `.ts`) over `cases.json` (61 cases written by
  `make_cases.py` from upstream's test scenarios), and writes `oracle.json`.
  `test_oracle.py` runs the port on the same inputs: every case matches
  upstream's JSON exactly. To check the check, a mutation that changes
  observable output failed 7 cases. A mutation that set only an intermediate
  tool state passed until two cases ending mid-tool-call were added; now it
  fails one.
- **On `workerd`:** the package imports under Pyodide; 1,004 typed chunks
  built a message in 17 ms (about 17 µs a chunk), stored through Sessions,
  decoded into typed parts, and encoded back to exactly the stored form.

How it turned out, beyond §4.2:

1. **`wire()` lives in `core/records.py`.** Ruff's RUF009 can't see that it
   returns a `dataclasses.field()` (configuring it as an immutable call
   didn't resolve through the relative import), so the two model modules
   ignore RUF009, with the reason in `pyproject.toml`.
2. **`ChatMessageOptions` is frozen, and `client_tools` is a `Sequence`**
   (default `()`): it's read-only data handed to `on_chat_message`.
3. **JavaScript's `undefined` is an absent key.** The builder copies a chunk
   field only when the chunk has it, so the dictionaries it builds equal what
   upstream stores. The reconciler tells an absent `input` from `null`, as
   upstream's `stableStringify` does.
4. **The accumulator's actions are a dataclass** (`ChunkAction`, one type
   with optional fields) rather than upstream's tagged union of objects.
5. **Not a "changed?" signal yet.** Dictionaries from the codec and from the
   builder can hold the same content with keys in a different order, so their
   JSON differs. Chat's "only write changed messages" check (12b) should
   compare in a canonical form, or it will rewrite unchanged messages.

---

## 5. Sub-step 12b: `AIChatAgent`, the turn path (design pass)

Status: **done** (2026-10-06). §5.5 records the questions and answers;
§5.6 how the implementation turned out.

### 5.1 How upstream works

**Installation.** `AIChatAgent` installs `Sessions` and a chat `Streams`
(`max_chunk_bytes` 1,900,000), keeps `cf_ai_chat_request_context` (the last
request's body and client tools, so they survive hibernation), builds a
`ResumableStream`, and mirrors the default session into `this.messages`. On
start it hydrates the messages: `getRecentHistory(hydrationByteBudget)`
(32 MiB), or the whole history when the budget is unbounded.

**Frames** (`parseProtocolMessage`, then one handler each):
- `cf_agent_use_chat_request`: parse `init.body` (`messages`, `trigger`,
  `clientTools`, the rest as `body`); ask submit concurrency (`queue`,
  `latest`, `merge`, `drop`, or debounce) whether to run; mark the turn
  accepted-but-not-streaming (pre-stream turns, for clients that reconnect
  early); broadcast the client's messages to the other tabs and
  `persistMessages(…, _deleteStaleRows)` (a regenerate can delete a
  tail); then queue the turn. Inside the queue it re-checks supersession
  and debounce, saves the request context, and runs
  `onChatMessage(…, {requestId, abortSignal, clientTools, body,
  continuation: false})`, then `_reply` with what it returns.
- `cf_agent_chat_clear`: reset turn state, clear the session and every chat
  stream, forget the request context, broadcast the clear.
- `cf_agent_chat_messages` (from a client): persist them.
- `cf_agent_chat_request_cancel`: abort that request.
- `cf_agent_stream_resume_request` / `_ack`: the resume handshake (wire §7.4,
  `ResumeHandshake`).
- tool results and approvals: 12c.
- On connect: notify `stream_resuming` if a stream is active, or park the
  connection if a turn is accepted but not streaming; on close, forget the
  connection's resume state.
- `get-messages` (HTTP): stream `historyBatches()` as one JSON array.

**The turn queue** (`TurnQueue`) runs one turn at a time; a generation
counter (bumped by clear) turns queued work stale. **Submit concurrency**
(`SubmitConcurrencyController`) decides for a send that arrives while turns
are queued: `queue` (run after), `latest` (only the newest runs), `merge`
(queued user messages merged into one), `drop` (rejected, the client rolled
back), or debounce. A turn that ends runs `onChatResponse` hooks after the
queue releases, and records (or clears) the durable "last terminal error"
record the handshake replays to a client that missed it.

**`_reply`** consumes the model's response:
- start a resumable stream (fresh id; metadata: message id, continuation,
  origin user-message ids, `seqBase`);
- read each chunk: drop provider *replays* (`isReplayChunk`; late tool input
  after an approval request is forwarded with the request again); apply it
  to the in-memory assistant message; persist the message early when a tool
  enters `approval-requested` (so the approval UI survives a reload); route
  tool results for calls in *earlier* messages to those messages (12c);
  rewrite `start` (stamp the server's message id, or remove it on a
  continuation) and `finish` (`finishReason` → `messageMetadata`); store it
  in the stream and broadcast it as `cf_agent_use_chat_response` with its
  `seq`;
- an `error` chunk **ends the turn**: error frame, stream marked errored,
  `outcome: "error"`;
- on a continuation, the first `text-start` / `reasoning-start` merge into a
  part that was still streaming instead of opening a new one;
- the end of the stream sends `done`; an abort sends `done` with
  `outcome: "aborted"`;
- **the cutover**: the finished message is saved through `persistMessages`
  in the same transaction that settles the stream and deletes its rows (the
  pending cutover rides on the instance, so an overriding `persistMessages`
  still lands atomically);
- terminal frames are held until the message is saved, then released (as an
  error if the save failed).
The plain-text path wraps text in one `text` part.

**`ResumableStream`** (chat's adapter over Streams): chunks are coalesced into
stored segments (up to 10 chunks or 512 KB; a tool output flushes at once);
an oversized chunk (over 1.8 MB) is broadcast but not stored; replays send
stored bodies with `replay: true` and `seq`; `restore` finds a stream left
`streaming` by a dead isolate; abandoned streams are reclaimed after an hour;
a small progress table counts retired segments (recovery evidence, 12d).

**Programmatic turns**: `saveMessages` (queued; persist, then a turn with the
last request's tools and body), `continueLastTurn` (a continuation turn), and
`persistMessages` (reconcile, sanitize, write only what changed, the
cutover, the regenerate deletion, `maxPersistedMessages` retention, and the
`cf_agent_chat_messages` broadcast).

### 5.2 Python design

**Module layout** (`src/agents/chat/`, beside 12a's):

| Module | Holds | Upstream |
| --- | --- | --- |
| `protocol.py` | frame type constants, outcomes, `parse_protocol_message` | `protocol.ts`, `wire-types.ts`, `parse-protocol.ts` |
| `turn_queue.py` | `TurnQueue` | `turn-queue.ts` |
| `concurrency.py` | `SubmitConcurrencyController`, `MessageConcurrency` | `submit-concurrency.ts` |
| `resumable_stream.py` | `ResumableStream` (+ replay frames, chunk size, origin ids) | `resumable-stream.ts`, `replay-frames.ts`, `chunk-size.ts`, `origin-message-ids.ts` |
| `handshake.py` | `ResumeHandshake`, `PreStreamTurns` | `resume-handshake.ts`, `pre-stream-turns.ts` |
| `terminal.py` | the durable last-terminal record | `recovery-incident.ts` (its storage glue) |
| `agent.py` | `AIChatAgent` | `ai-chat/src/index.ts` |

**The turn as a task.** Each turn's `on_chat_message` consumption runs in an
`asyncio` task registered under its request id. Cancelling a request
(`cf_agent_chat_request_cancel`, `abort_request`, a cancelled awaiter of
`save_messages`) cancels that task; the reply loop catches the
`CancelledError` at its boundary and finishes the turn as `aborted`
(chat_models.md §6: no `abort_signal`). Upstream's `AbortRegistry` isn't
needed.

**`on_chat_message`** is an async generator yielding typed chunks or `str`
(chat_models.md §3). Each chunk is turned into its wire dict once
(`chunk_to_wire`) and then follows upstream's `_reply` loop unchanged on
dicts: the replay filter, the builder, the `start` / `finish` rewrites,
storing and broadcasting. Python adds the SDK-owned `Start` / `Finish`
(chat_models.md §3.1) and the plain-text path (§3.2). A stream that yields
nothing ends with `done`, as upstream's empty body.

**`self.messages`.** The Sessions mirror keeps the transcript as wire dicts
(what the reconciler and builder use). `self.messages` returns a read-only
`Sequence[UIMessage]`, decoded lazily and cached per message (an unchanged
message isn't decoded again), so code that never reads it pays nothing.
Hooks receive typed messages (`sanitize_message_for_persistence`,
`on_chat_response`).

**"Changed?" uses a canonical form** (§4.5 item 5): `persist_messages`
compares a message with its stored copy by sorted-key JSON, so the same
content with keys in another order isn't rewritten.

**`get-messages`** streams `history_batches()` as one JSON array through a JS
`ReadableStream` built by `_ffi` (the SDK's `Response` accepts one), so the
transcript is never held whole. Checked on `workerd` first; if the pull
callback doesn't work, the fallback is joining the batches into one string.

**Names** (upstream → Python): `onChatMessage` → `on_chat_message`,
`onChatResponse` → `on_chat_response(result: ChatResponseResult)`,
`sanitizeMessageForPersistence` → `sanitize_message_for_persistence`,
`persistMessages` / `saveMessages` / `continueLastTurn` → as decided in
ai_chat_agent_api.md §1, `abortRequest` → `abort_request(request_id)`,
`abortAllRequests` → `abort_all_requests()`, `resetTurnState` →
`reset_turn_state()`, `hasPendingInteraction` / `waitUntilStable` (12c).

**Events** (observability.md): `message:request`, `message:response`,
`message:clear`, `message:cancel`, `message:error`, `chat:request:failed`.

**Fixes from reading upstream:** the `chunks.Error` docstring said the
stream continues; an `error` chunk ends the turn. Corrected.

### 5.3 Left for 12c and 12d

- **12c:** tool results and approvals, `cf_agent_message_updated`, results
  for calls in earlier messages, the early persist at `approval-requested`
  (needed by approvals), pending-interaction waits, auto-continuation.
- **12d:** running each turn as a Task (or a fiber on a facet) with its
  recovery snapshot and `stash()`, the stall watchdog, orphan persistence
  of a stream left by a dead isolate (12b only reclaims it after an hour),
  `cf_agent_chat_recovering`, and `on_chat_recovery`.

### 5.4 Verification

- Unit tests per module (turn queue, concurrency, resumable stream,
  handshake decisions against upstream's frozen `resume-handshake-frames`
  fixture), and agent tests on the fake runtime: a turn's frames, the
  persisted message, clear, cancel, regenerate, each concurrency strategy,
  reconnect mid-turn (resume and replay), the terminal-error replay,
  `save_messages`, `continue_last_turn`, `get-messages`.
- On `workerd`: a Python WebSocket client speaking wire §7 through a whole
  turn, a cancel, a reconnect mid-stream with its replay, and `get-messages`
  streaming a large transcript. And, per §5.5 Q3, upstream's real client.

### 5.5 Questions to decide

**Q1. When turns start running as Tasks.** **Decided: (a), with a guard.**
In 12b the turn body is one function with a single boundary, and its cancel
path is designed knowing a Task step will wrap it. 12d's first step wraps
that function in the Task and re-runs every 12b turn test (cancellation
included) through it, before building recovery. This covers (b)'s one real
benefit: testing the final execution path, notably a cancelled turn inside a
Task step settling as `aborted` rather than being retried as a failed
attempt. (b)'s other benefit, a basic crash outcome between 12b and 12d,
doesn't matter, since nothing ships in between.
- (a) **In 12d, with recovery.** **Recommended.** In 12b a turn runs inline
  in the turn queue. Upstream's Task wrapper exists only so a turn
  interrupted by a dead isolate is noticed and recovered; without the
  recovery engine it would add a durable run per turn and nothing else. 12d
  adds both together.
- (b) In 12b, with a placeholder recovery that 12d replaces.

**Q2. Where chat settings live.** **Decided: (a) `AIChatAgentOptions`.** Upstream uses instance fields
(`messageConcurrency`, `maxPersistedMessages`, `hydrationByteBudget`, and in
12d `chatRecovery`, `chatStreamStallTimeoutMs`).
- (a) **`options = AIChatAgentOptions(...)`**, a frozen dataclass extending
  `AgentOptions`. **Recommended:** one place, like every other agent setting
  (agent_api.md §1.7).
- (b) Class attributes, one per setting, like upstream's fields.

**Q3. Checking against upstream's real client.** **Decided: (a), with the
Node dependencies installed in their own folder under `verify/`** (its own
`package.json`), not in the upstream repo or the project's root. Upstream's
`WsChatTransport` / `useAgentChat` is the client users will run.
- (a) **Install the upstream repo's Node dependencies** (`npm install` in
  `../agents`, network access) and drive a Python agent with upstream's own
  transport from a Node script. **Recommended:** it tests compatibility,
  not just our reading of the protocol.
- (b) Only a Python protocol client plus upstream's frozen frame fixtures.


### 5.6 Implementation notes (12b, 2026-10-06)

Done: `src/agents/chat/` gains `protocol`, `turn_queue`, `concurrency`,
`types` (`AIChatAgentOptions`, `Debounce`, `MessageConcurrency`,
`SaveMessagesResult`, `ChatResponseResult`), `resumable_stream`,
`handshake`, `terminal`, `errors` (`ChatStreamError`), and `agent`
(`AIChatAgent`); `_ffi.streaming_response`. 637 tests in total (73 new:
`tests/chat/test_chat_agent.py`, `test_turn_machinery.py`).

**Verified on `workerd`** (`verify/results/chat_client.py` →
`chat_report.json`, a Python client speaking wire §7):
- a turn's frames to the requester and to another tab (the transcript before
  the terminal `done`, `messageIds` on it), plain text, an `Error` chunk, an
  exception, a client cancel (`outcome: "aborted"`, partial saved);
- a client reconnecting mid-turn: offered on connect and again with its
  `probeId`, replay with `seq` 0–5, `replayComplete`, then live frames from
  `seq` 6 (contiguous);
- an idle probe (`resume_none`, `reason: "idle"`); a turn that errored after
  its only client left, replayed through the handshake (its chunks, then the
  error frame);
- `save_messages` and `continue_last_turn` (`continuation: true` frames),
  clear, and the cutover (no stream rows left after each turn);
- `get-messages` on 300 messages of 40 KB: 12 MB, `Transfer-Encoding:
  chunked`, in 0.6 s through a JS `ReadableStream` with a Python `pull`;
- a chat agent as a sub-agent (`/sub/chat-room/…`): a turn and
  `get-messages`.

**Upstream's real client** (§5.5 Q3; `verify/chat-client/`, with its own
`package.json`: `ai` 6.0.301, `nanoid`, `esbuild`). `build.mjs` bundles
upstream's `ws-chat-transport.ts` as is; `run.mjs` sends turns with its
`sendMessages` and reads them with the AI SDK's `readUIMessageStream`, and
resumes with `reconnectToStream` (routing the resume frames to the
transport as `useAgentChat` does). Result (`chat_client_js_report.json`):
the echo reply (with `messageMetadata`, `finishReason` merged), plain text,
`boom` and `model down` surfacing as errors with the partial text kept, an
abort ending the turn, a second client resuming a turn mid-stream into the
same message id, an idle `reconnectToStream` resolving to `null`, and the
client's message ids equal to the stored ones.

How it turned out, beyond §5.2:

1. **The stream starts before `on_chat_message` is called.** So every
   failure, even before the first chunk, is a stream failure (error frame,
   stream errored). The pre-stream window a reconnecting client is parked in
   is the queue, debounce, merge, and the pre-turn repair.
2. **The cancellable stretch is exactly the consumption of
   `on_chat_message`.** The turn's task registers itself while consuming;
   `abort_request` cancels it then. An abort that lands earlier (repair) is
   remembered and ends the turn as soon as consumption would start; an abort
   during the save afterwards does nothing, so a save is never cut short.
   Queued turns can't be aborted (as upstream: no controller exists yet). A
   cancelled caller of `save_messages` / `continue_last_turn` is turned into
   `abort_request`; the caller waits for the turn to settle (so the queue
   stays one at a time) and then gets `CancelledError`.
3. **Typed hooks cost nothing unless overridden.** `persist_messages`,
   `sanitize_message_for_persistence`, and the new
   `repair_interrupted_tool_part` (upstream's protected
   `repairInterruptedToolPart`) are typed; the turn machinery stays on wire
   dicts and converts only when a subclass overrides one. An overriding
   `persist_messages` still saves atomically (the cutover rides on the
   instance), and the regenerate's stale-row deletion runs after it.
4. **`self.messages`** is a `Sequence` view over a snapshot of the mirrored
   list, decoding each message on first access and caching it per stored
   dict (an unchanged message isn't decoded again).
5. **Orphan persistence moved here from 12d.** The handshake's ACK path
   saves the partial message of a stream a dead isolate left streaming; it's
   small (the accumulator plus `reconcile_orphan_partial`) and the handshake
   is incomplete without it.
6. **Left for 12c:** the handshake's continuation branches (and
   `ContinuationState`), so `continuation-owned` isn't sent yet. **For 12d:**
   the progress marker table (`cf_agents_chat_progress`) and the stream
   lookups recovery uses.
7. **`on_error` for an `Error` chunk** receives a `ChatStreamError`
   (`AgentsException`) carrying the error text, so every failed turn reports
   one exception (ai_chat_agent_api.md §2.3).
8. **`chat:request:failed` isn't emitted:** upstream only emits it from
   `Think`; `AIChatAgent` emits `message:error`.
9. **Agent seams:** `_after_connect_frames` (chat's resume offer between the
   connect frames and `on_connect`); chat frames are handled by overriding
   `_message_locally` / `_close_locally`; `get-messages` is a selective
   `on_request` on chat's own capability. SDK subclasses (`AIChatAgent`)
   add their names to `Agent._sdk_names`, so a user's overrides of its hooks
   aren't wrapped by `_in_agent_context`.
10. **The terminal record is stored as JSON text** under
    `cf:chat:last-terminal`, as the facet record is.
11. **Settings are checked at construction:** an `AIChatAgent` whose
    `options` isn't an `AIChatAgentOptions` raises `TypeError`.

12. **A delta or end chunk without its start fails the turn** (decided
    2026-10-06, found with upstream's client). The AI SDK client rejects a
    `text-delta` / `text-end` with no open `text-start` for its id (likewise
    reasoning, and `tool-input-delta` without `tool-input-start`) and drops
    the whole reply, while the server's builder accepts it. Upstream never
    hits this (`streamText` always sends starts); hand-written Python streams
    can. Options were: (a) the SDK sends the missing start, (b) the turn
    fails with a `TypeError` naming the chunk, (c) pass it through as
    upstream. **Decided: (b)**, nothing ignored or patched silently. The
    check follows the user's own stream (a `TextStart` the server drops to
    resume a still-streaming part on a continuation still counts). Tool
    outputs aren't checked: in 12c they may target calls in earlier
    messages. On `workerd`, upstream's client shows the `TypeError`'s
    message as the turn's error, and nothing is saved.
