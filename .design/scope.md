# Scope: Python Agents SDK (current phase)

What the first phase of the Python SDK includes, what it leaves out, and what
still needs deciding. Upstream reference: `../agents/packages/agents` (v0.25.0)
and `../agents/packages/ai-chat`.

Related design docs:
- [agents-subpackages.md](./agents-subpackages.md): map of upstream folders
- [lifecycle_capabilities.md](./lifecycle_capabilities.md): Lifecycle and the capability model
- [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md): Python API for schedules, queues, tasks
- [core_disposable_store.md](./core_disposable_store.md): `core/` events and cleanup
- [utilities.md](./utilities.md): method lookup, FFI, SDK-wide principles
- [agents_wire_protocol.md](./agents_wire_protocol.md): the WebSocket protocol
- [agent_api.md](./agent_api.md): the core `Agent` API
- [chat_models.md](./chat_models.md), [sessions_api.md](./sessions_api.md), [streams_api.md](./streams_api.md), [fibers_api.md](./fibers_api.md)
- [platform_verification.md](./platform_verification.md): runtime facts measured on Python Workers

---

## 1. Goals

1. **Wire-compatible with the existing JS clients.** `AgentClient`, `useAgent`,
   and `useAgentChat` must work against a Python server unchanged
   ([agents_wire_protocol.md](./agents_wire_protocol.md)).
2. **A Python-style API, not a transliteration.** snake_case, keyword-only
   options, `async def` callbacks, method references as well as strings
   ([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md)).
3. **The same architecture as upstream.** Lifecycle as the base, features as
   capabilities, and `Agent` as the class that builds them
   ([lifecycle_capabilities.md](./lifecycle_capabilities.md)). Capabilities
   are usable on plain Durable Objects.
4. **Runs on Python Workers (Pyodide) and is testable under CPython.** All
   `js` / `pyodide` imports live in one FFI module
   ([utilities.md](./utilities.md) §3).

---

## 2. In scope

| # | Area | Upstream | Status |
| --- | --- | --- | --- |
| 1 | Lifecycle substrate | `src/lifecycle/` | Designed ([lifecycle_capabilities.md](./lifecycle_capabilities.md)) |
| 2 | Lifecycle capabilities | `state/`, `websockets/`, `schedules/`, `queue/`, `tasks/`, `streams/`, `sessions/` | Designed (§2.2); Tasks engine designed ([tasks_engine.md](./tasks_engine.md)) |
| 3 | Core `Agent` class | `src/index.ts`, `agent-routing.ts`, `callable-decorator.ts`, `retries.ts` | **Implemented**, except sub-agents (step 8) ([agent_api.md](./agent_api.md) §1.19) |
| 4 | `AIChatAgent` + shared chat layer | `packages/ai-chat`, `src/chat/` | Protocol documented; models and options designed ([chat_models.md](./chat_models.md)) |
| 5 | Shared utilities | `src/core/` + new | Decided ([utilities.md](./utilities.md)) |
| 6 | Sub-agents / facets | `dynamic-agents/`, `sub-routing.ts`, Agent sub-agent methods | In scope; API designed (§2.6) |
| 7 | Fibers (legacy durable-execution engine + public API) | `runFiber`, `startFiber`, … in `src/index.ts` | In scope, including the public managed-fiber API (§2.7) |

### 2.1 Lifecycle substrate

The DO's single entry point: it replaces `fetch`, `alarm`, and the
`webSocket*` methods, runs startup, owns the job table (`cf_agents_jobs`) and
the one physical alarm, and dispatches events to capabilities.
Hook dispatch, the services each capability gets, and the job driver (retries,
deferral, the memory-limit circuit breaker) are all in scope.

### 2.2 Lifecycle capabilities

| Capability | Why it's in scope | Status |
| --- | --- | --- |
| **State** | `Agent.state` / `set_state`, synced to clients | **Implemented** ([agent_api.md](./agent_api.md) §1.6, §1.18) |
| **WebSockets** | Every client connection, the connect sequence, RPC frames, per-connection flags | **Implemented** ([agent_api.md](./agent_api.md) §1.9, §1.18, §1.19) |
| **Scheduler** | `schedule`, `schedule_every` | **Implemented** ([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.14) |
| **Queue** | `queue` | **Implemented** (same doc, §2.13) |
| **Tasks** | `@task`, `step.do`, durable replay; also runs every chat turn | **Implemented** ([tasks_engine.md](./tasks_engine.md)), including runs on sub-agents (verified on real facets in step 8) |
| **Streams** | **Required by `AIChatAgent`**: chat's resumable streams are built on it (`chat/resumable-stream.ts:81`, `createChatStreams()`) | **Designed** ([streams_api.md](./streams_api.md)) |
| **Sessions** | Conversation storage; **`AIChatAgent` installs it** (`ai-chat/src/index.ts:447`) | **Designed**, scoped to `AIChatAgent` ([sessions_api.md](./sessions_api.md)) |

### 2.3 Core `Agent` class

- Builds the capabilities above on a `Lifecycle`.
- State sync, and `@callable` RPC, including streaming methods
  (`StreamingResponse` and async generators).
- The connect sequence and message dispatch (wire §3–4); readonly and
  no-protocol connections.
- Routing: `route_agent_request`, `get_agent_by_name`, the URL scheme, and the
  kebab-case class names (wire §2.1).
- `RetryOptions`, shared by schedule and queue; the `retry()` SDK utility
  ([utilities.md](./utilities.md) §7).
- `cf_agent_mcp_servers` is still sent on every connect for wire
  compatibility, with an empty state while MCP is out of scope (§3).

### 2.4 `AIChatAgent` and the shared chat layer

`AIChatAgent` depends on most of the other in-scope pieces, which is why
Streams and Sessions are in this phase. Upstream `ai-chat` imports `agents`,
`agents/chat`, `agents/streams`, `agents/sessions`, and `agents/observability`.

- **The chat wire protocol:** all `cf_agent_*` chat frames (wire §7).
- **From the shared chat layer (`src/chat/`):**
  - the turn queue and concurrency;
  - resumable streams (on Streams);
  - the resume handshake;
  - recovery through Tasks (`chat/turn-task.ts`);
  - message reconciliation and sanitization;
  - client tool results and approvals.
- **Persistence through Sessions.** `self.messages: list[UIMessage]` is the
  conversation history, provided by `AIChatAgent` as a **read-only property**.
  It is loaded from Sessions at startup and kept in sync through the Sessions
  change feed, as upstream (`ai-chat/src/index.ts:946`, `:2563`)
  ([sessions_api.md](./sessions_api.md) §8).
- **The `get-messages` HTTP route** that `useAgentChat` uses to load the
  initial transcript (wire §7.6).
- **Storage:** every table and KV key is listed in
  [sql_schemas.md](./sql_schemas.md).
- **Provider-agnostic model integration (§2.4.1).**

#### 2.4.1 `on_chat_message` and `UIMessageChunk` (decided; details in [chat_models.md](./chat_models.md))

```python
class MyAgent(AIChatAgent):
    async def on_chat_message(
        self, options: ChatMessageOptions
    ) -> AsyncIterator[
        UIMessageChunk
    ]: ...  # call any model however the user likes; yield valid UIMessageChunks
```

- **`on_chat_message` returns `AsyncIterator[UIMessageChunk]` or
  `AsyncIterator[str]`.** This replaces upstream's AI SDK `Response`
  (`ai-chat/src/index.ts:7570`), which is either an SSE stream of
  `UIMessageChunk`s or plain text. A `str` iterator is the plain-text path:
  the SDK wraps it in `text-start` / `text-delta` / `text-end` chunks, as
  upstream's `_sendPlaintextReply` does (`index.ts:7389`).
- **It is provider-agnostic.** The SDK never calls a model or creates a
  provider client. Users call whichever model they want, with their own client,
  and are responsible for yielding valid chunks. That includes any multi-step
  tool loop, which upstream's `streamText` runs for the user.
- **`UIMessageChunk` is a typed object.** It is a union of dataclasses
  (`@dataclass(slots=True, kw_only=True)`, per the convention in
  [utilities.md](./utilities.md) §5), one per chunk type (`chunks.TextStart`, `chunks.TextDelta`,
  `chunks.ToolInputStart`, …, `chunks.Data`, `chunks.Error`), mirroring the
  **AI SDK v6** chunk types. The SDK sends `Start` / `Finish` itself. Attributes are snake_case; one serializer converts to and from the
  camelCase wire format (`tool_call_id` ↔ `toolCallId`).

**In scope: the internals that handle the iterator:**
1. Typed models for `UIMessage`, `UIMessagePart`, and `UIMessageChunk`, plus the
   wire serializer. These shapes must match AI SDK v6 exactly, because Sessions
   stores the messages and the browser's AI SDK parses the chunks.
2. A port of `chat/message-builder.ts` (`apply_chunk_to_parts` and its guards:
   tool states only move forward, `normalize_tool_input`, late tool input after
   an approval, `data-*` reconciliation, `is_replay_chunk`). It folds chunks into
   the assistant message.
3. The consumption pipeline. For each chunk: apply it to the message, store it
   in the resumable stream (Streams), and broadcast it as
   `cf_agent_use_chat_response`. At the end of the stream, save the message to
   Sessions and send the terminal frame.

**Out of scope for now:** provider adapters, both message conversion
(`UIMessage` → provider format) and stream conversion (provider events →
`UIMessageChunk`), plus a tool-loop helper. These may come later as an
optional subpackage that doesn't affect the core import.

### 2.5 Shared utilities

| Utility | Status |
| --- | --- |
| `get_bound_method`, `method_name` | Decided |
| `agents/_ffi.py` (`py_to_js`, `js_to_py`, `to_rpc`, `from_rpc`, `proxies()`) | Defined; runtime facts verified ([platform_verification.md](./platform_verification.md) §3) |
| `MISSING` sentinel | Dropped (`None` means "no result") |
| Typed SQL helper, `retry()` utility | Decided ([utilities.md](./utilities.md) §6, §7) |
| `AsyncExitStack` in place of `DisposableStore`; `Emitter` | Decided ([core_disposable_store.md](./core_disposable_store.md)) |

### 2.6 Sub-agents and facets

A sub-agent is a child agent that runs as a **facet** of its parent: its own
isolate and its own SQLite database, on the same machine, supervised by the
parent ([agents-subpackages.md](./agents-subpackages.md) `dynamic-agents/`).

- **The `Agent` API** (designed, [agent_api.md](./agent_api.md) §1.15):
  `dynamic_agents` (`get` / `abort` / `delete` / `has` / `list`),
  `parent_agent(cls)`, `parent_path` / `self_path`, and the
  `on_before_sub_agent` gate. Upstream's deprecated `subAgent` /
  `abortSubAgent` / `deleteSubAgent` / `hasSubAgent` / `listSubAgents` are not
  ported.
- **Routing:**
  - `route_sub_agent_request` and `get_sub_agent_by_name`;
  - `/sub/{child-class}/{child-name}` URL segments;
  - forwarding WebSocket connect, message, and close events to the child
    (wire §6);
  - RPC reply bridging for `@callable` methods on facets.
- **Lifecycle routing becomes required.** Facets have no alarm of their own, so
  their scheduled work runs through the root: `routes.to_root()` / `on_route`,
  and the `owner_path` routing paths in Scheduler, Queue, and Tasks. Leaving
  those out was only possible while facets were out of scope.
- **Storage:** `cf_agents_sub_agents` (child registry) and
  `cf_agents_facet_runs` (the root's index of fibers running in facets);
  see [sql_schemas.md](./sql_schemas.md).
- **Platform:** DO facets (`ctx.facets`) work on Python Workers (confirmed),
  so there is no blocker.

### 2.7 Legacy fiber engine ([fibers_api.md](./fibers_api.md))

Upstream labels `runFiber` as the "legacy fibers: durable execution" engine,
with Tasks as its replacement (`index.ts:4067`, `design/rfc-fibers.md`). It
is **not deprecated, though**: deprecation is deferred "until the facet
migration lands" (`rfc-fibers.md:193`).

**Why it's needed in this phase:** **chat turns hosted in a facet run on the
fiber engine, not on Tasks.** Upstream `AIChatAgent` checks
`this.parentPath.length > 0` and uses `_runFiberWithStashWrapper` there,
because "the Tasks capability does not accept runs on routed sub-agents yet,
and facet recovery routes through the root's facet-run index"
(`ai-chat/src/index.ts:893`). Since `AIChatAgent` and facets are both in
scope, the engine is too.

- **Needed for facet chat:** `run_fiber` (internally, the stash-wrapper
  variant), `stash()`, recovery of interrupted fibers on wake (the hook upstream
  calls `onFiberRecovered`, with framework fibers filtered out before user
  ones), `keep_alive` / `keep_alive_while`, and the root-side facet-run index.
  Storage: `cf_agents_runs`, `cf_agents_facet_runs`.
- **Also used outside facets:** `stash()` also works inside chat turns that
  run on Tasks. Upstream runs the turn under the fiber stash context
  (`_withFiberStash`, `chat/turn-task.ts`), so the recovery snapshot is
  persisted the same way on both engines.
- **The public managed-fiber API is in scope too (decided):** `start_fiber`,
  `inspect_fiber`, `inspect_fiber_by_key`, `list_fibers`, `cancel_fiber`,
  `cancel_fiber_by_key`, `resolve_fiber`, `delete_fibers`; storage
  `cf_agents_fibers`.

---

## 3. Out of scope (this phase)

| Area | Upstream | Note |
| --- | --- | --- |
| MCP client and server | `mcp/` | Still send an empty `cf_agent_mcp_servers` on connect (§2.3) |
| Agents as tools | `agent-tools.ts` | No `agent-tool-event` frames yet |
| `RoutedAgents` capability | `routing/routed-agents.ts` | |
| Think | `packages/think` | `AIChatAgent` comes first |
| Voice | `voice/` | Its wire protocol is documented for later (wire §8) |
| Channels, email, workflows, browser | `channels/`, `email.ts`, `workflows.ts`, `browser/` | |
| Skills, context, models, codemode | `skills/`, `context/`, `models/`, `codemode/` | |
| Tracing (spans) | `observability/` | Structured events **are** in scope ([observability.md](./observability.md)); only tracing is out |
| Cap'n Web transport | `websockets/transport*.ts` | Experimental upstream |
| x402, WebMCP, pi harness, chat-sdk adapter | various | |
| Legacy `ai-chat-agent`, `ai-react`, deprecated APIs | | Not ported, by decision |
| Provider adapters for chat (message conversion, stream conversion, tool loop) | (upstream relies on the AI SDK) | `on_chat_message` is provider-agnostic (§2.4.1); possibly an optional subpackage later |
| Client-side code (React hooks, `AgentClient`) | `react.tsx`, `client.ts` | The JS clients are used as they are |

---

## 4. Open questions

### 4.1 How `AIChatAgent` calls the model: resolved

Decided in §2.4.1 and [chat_models.md](./chat_models.md): typed chunks from a
provider-agnostic `on_chat_message`, `ChatMessageOptions` (upstream's fields
including `continuation`; cancellation replaces `abortSignal`), and
`self.messages` as a read-only property.

### 4.2 Sessions design: resolved

Designed in [sessions_api.md](./sessions_api.md), scoped to what
`AIChatAgent` needs.

### 4.3 Streams design

Decided: [streams_api.md](./streams_api.md).

### 4.4 Platform verification on Python Workers (done: [platform_verification.md](./platform_verification.md))

- The Hibernation API: `acceptWebSocket` with tags, `getWebSockets`,
  `serialize/deserializeAttachment`.
- FFI conversions for storage, `sql.exec`, and attachments
  ([utilities.md](./utilities.md) §3.8).
- Whether cancelling a Python task aborts a JS `fetch`
  ([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.6).
- ~~DO facet support (`ctx.facets`)~~: **confirmed working on Python
  Workers.** No longer an open item.

### 4.5 Smaller decisions

- **Observability: decided** ([observability.md](./observability.md)). Structured
  events are ported: capabilities emit through the Lifecycle `events` service,
  `Agent.observability` receives `ObservabilityEvent`s, event names and
  payloads match upstream exactly, a Python `subscribe()` registry is included,
  and the default sink is Python `logging` (logger `agents.events`, `DEBUG`).
  Tracing (spans) is out of scope.
- **`Emitter`: decided, ported in phase 1** ([core_disposable_store.md](./core_disposable_store.md)
  §5.4). Used by the observability `subscribe()` registry and the Sessions
  change feed; listener errors are isolated.
- **Storage compatibility (decided):** keep upstream's table names
  (`cf_agents_jobs`, `cf_agents_state`, …) for readability, but **migrating a
  Durable Object between the TypeScript and Python SDKs, in either direction,
  is not supported.** Cloudflare has no official guidance for moving a DO
  between languages, so the SDK makes no promises about it.
- **Carried over from**
  [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §4: all
  decided (`queue_items`, `timedelta`, and two constructor parameters,
  `callbacks` + `target`).

---

## 5. Suggested build order

Each step depends on the ones before it:

1. **Utilities:** `_ffi.py`, method lookup. **Done (2026-10-05):**
   `agents/_ffi.py`, and in `core/`: `errors.py` (`AgentsException`,
   `SqlError`), `types.py` (`JSONValue`, `Duration`, `SqlValue`,
   `RetryOptions`), `timing.py`, `methods.py`, `retry.py`, `sql.py`, plus the
   existing `events.py`. 49 tests; the real `_ffi.py` and `Sql` checked on the
   runtime (verify probe `sdk_ffi`).
2. **Lifecycle:** entry points, startup, job queue and alarm driver, capability
   dispatch. **Done (2026-10-05):** `src/agents/lifecycle/` (see
   [lifecycle_capabilities.md](./lifecycle_capabilities.md) §10); 87 tests in
   total; checked on `workerd` (startup once, dispatch, a real alarm firing a
   job).
3. **Queue:** the simplest capability; checks the job queue end to end.
   **Done (2026-10-05):** `src/agents/queue/` (see
   [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.13);
   105 tests in total; checked on `workerd`.
4. **State + WebSockets:** connections, the connect sequence, state sync.
   **Done (2026-10-05):** `src/agents/state/`, `src/agents/websockets/` (see
   [agent_api.md](./agent_api.md) §1.18); 132 tests in total; checked on
   `workerd` with a real WebSocket client. The `callables` option moves to
   step 5 with the RPC engine.
5. **`Agent`:** builds the above; `@callable` RPC, routing.
   **Done (2026-10-05):** `src/agents/agent/`, `websockets/rpc.py`,
   `observability/`, `core/encoding.py` (see [agent_api.md](./agent_api.md)
   §1.19); 172 tests in total; checked on `workerd` with real WebSocket and
   HTTP clients, including native RPC through `get_agent_by_name`.
6. **Scheduler.** **Done (2026-10-05):** `src/agents/schedules/` (see
   [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.14);
   359 tests in total (162 of them cron parity with the JS library);
   checked on `workerd` with real alarms.
7. **Tasks.** **Done (2026-10-05):** `src/agents/tasks/` (see
   [tasks_engine.md](./tasks_engine.md) §5); 384 tests in total; checked on
   `workerd`, including a real isolate crash mid-step that replays with
   `step.interrupted`.
8. **Sub-agents / facets.** **Done (2026-10-05):** `src/agents/dynamic_agents/`
   (see [subagents_engine.md](./subagents_engine.md) §6); 416 tests in total;
   checked on `workerd` with real facets, HTTP, and WebSocket clients,
   including routed schedules, queue, and tasks on the root's alarm.
9. **Fibers and keep-alive.** **Done (2026-10-05):** `src/agents/fibers/` (see
   [fibers_engine.md](./fibers_engine.md) §5); 438 tests in total; checked on
   `workerd`, including a real isolate crash mid-fiber and a facet fiber
   recovered by the root's housekeeping.
10. **Streams.** **Done (2026-10-06):** `src/agents/streams/` (see
    [streams_engine.md](./streams_engine.md) §6); 461 tests in total; checked
    on `workerd`, including the cutover's rollback through real
    `transactionSync` and a stream resumed by fiber recovery after a real
    isolate crash.
11. **Sessions.** **Done (2026-10-06):** `src/agents/sessions/` (see
    [sessions_engine.md](./sessions_engine.md) §6); 480 tests in total; checked
    on `workerd`, including the synchronous upsert inside a Streams cutover
    (nested `transactionSync`, committed and rolled back) and a 2 MiB message
    split across rows.
12. **Chat shared layer + `AIChatAgent`**, including facet-hosted turns on
    fibers, in sub-steps 12a–12d ([chat_engine.md](./chat_engine.md) §2).
    **12a done (2026-10-06):** the chat models, codec, and pure transforms,
    matching upstream's own code on 61 oracle cases.
    **12b done (2026-10-06):** `AIChatAgent`'s turn path (frames, the turn
    queue and concurrency policies, resumable streams with the cutover, the
    resume handshake, programmatic turns, `get-messages` streaming); 637
    tests; checked on `workerd` with a Python protocol client and with
    upstream's own `WsChatTransport` and the AI SDK's stream reader.
