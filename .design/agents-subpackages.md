# `packages/agents` — what each folder is responsible for

Source studied: `../agents/packages/agents` (npm package `agents`, v0.25.0).

This is a map of the package, organized by folder, to help with porting. Each
section answers three questions: what the folder owns, which import path it is
published as, and which other folders it depends on.

---

## 1. Mental model: four layers

The package is easiest to understand as layers stacked on a Durable Object (DO):

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Client side       client.ts · react.tsx · chat/transport · voice/client  │
│                   experimental/webmcp                                    │
├──────────────────────────────────────────────────────────────────────────┤
│ Integrations      mcp/ · chat/ · chat-sdk/ · models/ · observability/    │
│                   browser/ · voice/ · channels/ · skills/ · context/     │
│                   harness/pi · workflows.ts · email.ts                   │
├──────────────────────────────────────────────────────────────────────────┤
│ The Agent class   index.ts (~10k lines): composes everything below and   │
│                   adds @callable RPC, SQL, sub-agents, email, workflows  │
├──────────────────────────────────────────────────────────────────────────┤
│ Capabilities      state/ · websockets/ · schedules/ · queue/ · tasks/    │
│                   streams/ · sessions/ · routing/ · dynamic-agents/      │
├──────────────────────────────────────────────────────────────────────────┤
│ Substrate         lifecycle/  (DO fetch/alarm/WebSocket entry points,    │
│                   job queue, alarm loop, capability plumbing)            │
└──────────────────────────────────────────────────────────────────────────┘
```

Key ideas:

- **`Lifecycle` is the substrate.** `Agent` extends the platform `DurableObject`
  and installs a `Lifecycle` that owns the request, alarm, and
  hibernating-WebSocket entry points. Lifecycle also owns the job table
  (`cf_agents_jobs`) and the single physical DO alarm.
- **Features are "capabilities."** Each one is a `LifecycleCapability`
  subclass that owns its own SQLite tables and plugs into Lifecycle's services
  (storage, job queue, events, routing). `Agent` builds them in its constructor
  (`new State(...)`, `new WebSockets(...)`, `new Scheduler(...)`,
  `new Queue(...)`, `new Tasks(...)`, …). You can also use them without
  `Agent` on any `LifecycleObject`.
- **Scheduler and Queue don't store anything themselves.** They validate input
  and turn it into jobs on Lifecycle's queue. Tasks, Streams, and Sessions do
  own tables.
- **Optional peers live behind separate subpaths.** The AI SDK, React, Zod,
  pi-ai, TanStack AI, and similar libraries are optional peer dependencies.
  Voice, Channels, Browser, and similar features are kept out of `src/index.ts`
  so the core import stays light.

---

## 2. Top-level files in `src/` (not folders, but central)

| File | Import path | Responsibility |
| --- | --- | --- |
| `index.ts` | `agents` | **The `Agent` class.** Builds the capabilities; provides state sync, `@callable` RPC, the `sql` template tag, scheduling/queue/retry APIs, sub-agents (facets), MCP client wiring, email handling, workflow tracking, and broadcasting. About 10.4k lines. The upstream AGENTS.md still says ~6k, which is out of date. |
| `client.ts` | `agents/client` | `AgentClient`: the browser/Node WebSocket client, built on partysocket. Handles state sync, `call()`, and typed `stub` RPC. |
| `react.tsx` | `agents/react` | The `useAgent` hook: state sync and RPC from React components, plus cache/TTL handling. |
| `agent-routing.ts` | `agents/routing` (re-exported) | `routeAgentRequest`, `getAgentByName`, URL → DO routing, CORS, placement, routing retries. |
| `sub-routing.ts` | `agents` | Routing for sub-agents (facets): `routeSubAgentRequest`, `getSubAgentByName`, and parsing of `/sub/{Class}/{name}` path segments. |
| `callable-decorator.ts` | `agents` | The `@callable()` decorator and its metadata registry. It has no dependencies so that both the JSON RPC path and the Cap'n Web path can import it. |
| `serializable.ts` | `agents` | Type-level `Serializable<T>` checks that limit which values may cross RPC. |
| `types.ts` | `agents/types` | The `MessageType` wire enums (`CF_AGENT_STATE`, RPC frames, …). |
| `retries.ts` | `agents` | `RetryOptions`, exponential-backoff `tryN`, and validation. Used by schedule, queue, and `this.retry()`. |
| `agent-tools.ts`, `agent-tool-types.ts` | `agents/agent-tools` | **Agents as tools.** Lets one agent run another as a tool call. Covers detached and reattached runs, recovery, and the "interrupted" reasons. |
| `workflows.ts`, `workflow-types.ts` | `agents/workflows` | `AgentWorkflow`: a `WorkflowEntrypoint` subclass with typed access back to the agent that started it, plus progress reporting. |
| `email.ts`, `email-send.ts` | `agents/email` | Inbound email routing: resolvers, secure-reply header signing, and the send path behind `Agent.sendEmail()`. |
| `schedule.ts` | `agents/schedule` | Deprecated. A re-export of the scheduling parser, kept for compatibility. |
| `internal_context.ts` | (internal) | Compatibility re-export of the Lifecycle host context (`getCurrentAgent`). |
| `socket-address.ts`, `sql-error.ts`, `utils.ts` | (internal) | Small helpers: a socket-staleness marker for `useAgent`, a `SqlError` that carries the failing query, and string helpers. |
| `ai-chat-agent.ts`, `ai-react.tsx`, `ai-types.ts`, `ai-chat-v5-migration.ts` | `agents/ai-chat-agent`, … | **Legacy.** The original AI chat agent and hooks. New code should use the `@cloudflare/ai-chat` package. |
| `vite.ts` | `agents/vite` | Vite plugin that transforms decorators and resolves the `agents:skills` virtual module (bundled skills). |

---

## 3. Substrate

### `lifecycle/` — `agents/lifecycle`

**Owns the Durable Object runtime contract.** Everything else builds on this
folder.

- `durable-object-lifecycle.ts`: the `Lifecycle` class. `Lifecycle.install(this)`
  wires up `fetch`, `alarm`, and the hibernating WebSocket handlers
  (`webSocketMessage/Close/Error`). It also manages `Connection` objects,
  per-connection state, and startup (a single in-flight `start()` shared by
  concurrent callers, with retry on failure).
- `capability.ts`, `capability-runner.ts`: the `LifecycleCapability` base class
  and the runner that sends each lifecycle event (start, request, WS upgrade,
  alarm, dispose) to every registered capability.
- `job-queue.ts`, `job-driver.ts`: the durable job table (`cf_agents_jobs`) and
  the alarm event loop. It picks due jobs, runs them with retry and deferral,
  applies a memory-limit circuit breaker, and re-arms the single physical
  alarm from the queue's contents.
- `current-agent.ts`: `getCurrentAgent()`, an AsyncLocalStorage host context.
- `abort.ts`, `transport-errors.ts`, `types.ts`: supporting types and errors.
- `UPSTREAM.md`: records that this code was **vendored from PartyKit's
  `partyserver@0.5.10`** (ISC license) and lists the intentional differences.
  WebSocket hibernation is mandatory, `ctx.id.name` is the authoritative name,
  and Lifecycle owns the alarm.

---

## 4. Capabilities (each a `LifecycleCapability`)

### `state/` — `agents/state`
**A single persisted state value with change ordering.** It stores one row in
`cf_agents_state`, seeds `initialState`, runs `validateStateChange` before
writing and `onChanged` after, and runs its own schema migrations. It records
the source of each change (`"server"` or a `Connection`) so the host can leave
the originating client out of the broadcast. `Agent.setState()` sits on top of
this.

### `websockets/` — `agents/websockets`
**The client connection protocol.** It is opt-in on a Lifecycle object; Agent
always enables it.
- `websockets.ts`: hibernating WebSocket handling, the Agent identity
  handshake, and the `rpc` frame protocol.
- `transport*.ts`, `capnweb-socket.ts`, `callables-target.ts`: an alternative
  **Cap'n Web** RPC transport, chosen by a query-string flag, that exposes
  `@callable` methods.
- `connection.ts`, `connection-flags.ts`: per-connection flags such as
  read-only and no-protocol.

### `schedules/` — `agents/schedules`, `agents/schedules/parser`
**Persistent scheduling vocabulary.** `Scheduler` validates delayed, dated,
cron, and interval schedules (using `cron-schedule`), resolves named callbacks,
and pushes jobs onto Lifecycle's queue. It owns **no** tables. `parser.ts` is a
separate Zod-based entry point with the natural-language scheduling prompt and
schema, used by LLMs to produce schedules.

### `queue/` — `agents/queue`
**Durable background work, run as soon as possible.** Like Scheduler, it owns
no storage. Each queued item becomes a Lifecycle job that is due immediately,
with the callback name as `fn`. It backs `Agent.queue()`.

### `tasks/` — `agents/tasks`
**Durable, replayable execution with a step journal.** It plays the same role
as Temporal or Workflows, but inside the DO. It owns `cf_agents_task_runs` and
`cf_agents_task_steps`, and is responsible for:
- registering definitions and accepting runs
- generation-fenced claiming
- running steps, with step results journaled (`replay.ts`)
- retries and durations
- serialization limits
- errors such as `TaskReplayDivergedError` and `NonRetryableError`

A run's deadline is copied as one Lifecycle job, routed to the root DO when the
run was accepted on a sub-agent. Chat turns in `chat/` run as Tasks.

### `streams/` — `agents/streams`
**Durable incremental output.** For each stream it keeps an ordered chunk log
(`cf_agents_streams`, `cf_agents_stream_blocks`) with a monotonic cursor,
replay-then-tail readers, and a terminal status. It also provides
`sseResponse()`. It needs no alarm, so it works on facets. Tasks uses the
terminal status as recovery evidence.

### `sessions/` — `agents/sessions`
**Durable conversation history.**
- Tree-structured messages (branching, regeneration, latest-leaf paths)
- Compaction overlays and helpers (`createCompactFunction`)
- Full-text search
- Chunking for messages larger than one SQLite row
- Offloading of media and attachments (`attachment-*.ts`)
- Token estimation and a change feed for mirroring caches

It needs no alarm, so it works on facets.

### `routing/` — `agents/routing`
**Agent lookup and request routing.** It re-exports `routeAgentRequest` and
`getAgentByName` from `agent-routing.ts` and adds **`RoutedAgents`**: a
capability through which one owner DO keeps a catalog of the independent
top-level agents it created (for example, "my chats" or "my documents") and
routes to them.

### `dynamic-agents/` — internal, reached through `this.dynamicAgents`
**Facet-backed child agents.** These are children that run in their own isolate
with their own SQLite, colocated with the parent and supervised by it.
- `api.ts`: the public `DynamicAgents` surface (`get`, `abort`, `delete`).
- `dynamic-agents.ts`: the machinery (~1.9k lines). It covers:
  - the registry
  - identity and paths
  - keep-alive tokens and tracking of run fibers
  - forwarding Lifecycle jobs to the root (facets have no alarm of their own)
  - recursive destroy
  - virtual connections and broadcasting to the parent
- `bridges.ts`: delivers RPC replies from a facet back over the frame that
  carried the request.
- `host.ts`: the explicit interface listing which Agent internals this module
  may reach into.

---

## 5. Integrations

### `mcp/` — `agents/mcp`, `agents/mcp/client`, `agents/mcp/server`, `agents/x402`
**Model Context Protocol, both directions.**
- `client/`: **`MCPClientManager`**, which lets an Agent connect to remote MCP
  servers. It handles connection lifecycle, transports, the tool and resource
  catalog, invocation, persisted connections, OAuth through
  `do-oauth-client-provider.ts`, and x402 payments (`x402.ts`, also exported as
  `agents/x402`).
- `server/`: hosting MCP servers.
  - `handler-stateless.ts` wraps an SDK v2 stateless server in a Worker.
  - `legacy-agent.ts` is the deprecated SDK v1 `McpAgent`, which is
    DO-backed and supports SSE and Streamable HTTP.
  - The compatibility shims are `handler-compat.ts`,
    `handler-legacy-compat.ts`, and `handler-legacy.ts`.
  - The supporting pieces are the event store, SSE keepalive, and auth context.
- `index.ts`: a compatibility barrel for the legacy imports. `rpc.ts` and
  `types.ts` are shared by client and server.

### `chat/` — `agents/chat`, `agents/chat/transport`, `agents/chat/react`
**Shared chat-engine building blocks** for the sibling packages
`@cloudflare/ai-chat` and `@cloudflare/think`. It is not meant to be a broad
user-facing API. It covers:
- **Turns:** `turn-queue.ts` (serialized turns and concurrency strategies),
  `submit-concurrency.ts`, and `turn-task.ts` (each chat turn is one journaled
  Task).
- **Streaming:** `stream-accumulator.ts`, `message-builder.ts` (chunk → message
  parts), `resumable-stream.ts` (chunk persistence and replay),
  `stall-watchdog.ts`, and the `replay-*` files.
- **Recovery after isolate death:** the `recovery-*.ts` files, `orphan-*.ts`,
  `repair-transcript.ts`, and `continuation-state.ts`.
- **Persistence hygiene:** `sanitize.ts` (row-size limits),
  `message-reconciler.ts`, `tool-output-truncation.ts`, and
  `truncate-older-messages.ts`.
- **Tools:** `client-tools.ts`, `tool-state.ts`, and `agent-tools.ts`.
- **Wire and client:** `protocol.ts`, `wire-types.ts`, `ws-chat-transport.ts`
  (a WebSocket transport for AI SDK clients that doesn't depend on any UI
  framework), and `react.tsx`.

### `chat-sdk/` — `agents/chat-sdk`
**A state adapter for the third-party `chat` library (Chat SDK).**
`ChatSdkStateAgent` is an Agent that stores thread subscriptions, locks, and
queues in SQLite and runs scheduled cleanup. `ChatSdkStateAdapter` shards keys
and threads across those agents.

### `models/` — `agents/models/ai-sdk`, `agents/models/pi-ai`
**Experimental model providers (`createAI()`)** for Workers AI, and for vendor
models routed through AI Gateway. There is one implementation per framework,
and both follow the same layout:
- `core/` (not exported): the framework-neutral base layer.
  - Transport over the `env.AI` binding and the gateway
  - Settings, the Workers AI model-id catalog, and the AI Gateway provider table
  - A translation layer between Workers AI and OpenAI chat-completions,
    including a table of known quirks
  - An SSE decoder, errors, and image helpers
- `ai-sdk/`: providers for Vercel AI SDK v4. Covers language, embedding, image,
  speech, transcription, reranking, fallback chains, and gateway-routed vendor
  models.
- `pi-ai/`: the same idea for pi-ai, with its own wire converters (chat
  completions, responses, anthropic).

### `observability/` — `agents/observability`, `agents/observability/ai`
**Events and tracing.**
- `index.ts`, `base.ts`, `agent.ts`, `mcp.ts`, `diagnostics.ts`: typed agent
  and MCP observability events, published on `node:diagnostics_channel`. They
  are silent unless something subscribes or a Tail Worker is attached.
- `tracing/`: a minimal span tracer built on AsyncLocalStorage, plus a
  Cloudflare runtime backend.
- `genai/`: OpenTelemetry GenAI semantic-convention attributes (token usage,
  request settings).
- `ai/`: `wrapAISDK()`, which wraps the AI SDK namespace to trace model calls,
  streams, and tools. Also includes extraction of AI Gateway metadata.
- `agent-span-attributes.ts`: span attributes for agents.

### `browser/` — `agents/browser`, `/browser/ai`, `/browser/ai-sdk`, `/browser/tanstack-ai`
**Cloudflare Browser Run (headless Chrome) integration.**
- `browser-run.ts`: low-level REST and binding calls.
- `cdp-connection.ts`: one Chrome DevTools Protocol WebSocket. `spec.ts` loads
  the CDP spec.
- `browser.ts`: `Browser`, a named persistent browser implemented as a
  Lifecycle capability.
- `connector.ts`, `session-connector.ts`, `session-store.ts`: a codemode
  connector with durable session ids and sweeping.
- `live-view.ts`: creates Live View URLs.
- `quick-actions.ts`: stateless one-shot actions such as `browserMarkdown`.
- `browser-tool.ts`, `tool-helpers.ts`, `ai.ts`, `ai-sdk.ts`, `tanstack-ai.ts`:
  the model-facing tool wrappers for each framework.

### `voice/` — `agents/voice` (+ `/types`, `/client`, `/react`, `/errors`, `/workers-ai`, `/sfu`, `/text`)
**Real-time voice agents.**
- `withVoice(Agent)` is a mixin that adds continuous STT, streaming TTS,
  barge-in, conversation persistence, and the voice WebSocket protocol.
- Supporting pieces:
  - Provider contracts (`types.ts`) and Workers AI STT/TTS providers
    (`workers-ai.ts`)
  - `sentence-chunker.ts` and the `text*.ts` files
  - `audio-pipeline.ts`, `voice-input.ts`, and diagnostics
  - The Realtime SFU adapter (`sfu.ts`), which handles protobuf framing and
    resampling between 48 kHz stereo and 16 kHz mono
- Client side: `client.ts` (works in any browser, no UI framework) and
  `react.tsx` (hooks).

### `channels/` — `agents/channels` (+ `/email`, `/slack`, `/telegram`, `/voice`, `/ai-sdk`, `/tanstack-ai`)
**Messaging contracts that don't depend on any particular transport.**
- **Core** (`channel.ts`, `ingress.ts`, `identity.ts`, `routes.ts`,
  `surface.ts`, `stream.ts`, `fallback.ts`, `fanout.ts`, `host/`):
  - Outbound messages are canonical Markdown.
  - Delivery results are safe to show to a model.
  - It also defines inbound ingress envelopes, user and channel identity, and
    approval requests.
  - `fallbackChannel` and `fanoutChannel` are combinators over channels.
  - `host/` is the channel host.
- **`adapters/`**: provider implementations: email (Workers Email plus MIME
  parsing), Slack, Telegram, and output-only browser voice.
- **`ai-sdk.ts`, `tanstack-ai.ts`, `tool-schema.ts`**: expose channels to
  models as tools.
- `live-tests/`: opt-in delivery tests against real providers, with recorded
  snapshots.

### `skills/` — `agents/skills`, `agents/skills/compile`
**Agent Skills engine (SKILL.md) that doesn't depend on any AI framework.**
- `frontmatter.ts`: parses YAML frontmatter.
- `registry.ts`: `SkillRegistry`, which builds the catalog prompt and the
  activation tools.
- Skill sources: `manifest.ts` (`fromManifest`, for skills bundled through the
  Vite plugin) and `r2.ts` (skills stored in R2).
- `runner.ts`: an experimental runner that executes skill scripts in a sandbox.
- `workspace.ts`: projects skills into a workspace.
- `compile.ts`: a build-time esbuild compiler for skill scripts. It runs in
  Node only.

### `context/` — `agents/context` (experimental)
**Prompt assembly, separate from conversation storage.** `ContextBlocks`
(`blocks.ts`) builds labelled blocks into a system prompt. It keeps a frozen
snapshot so the provider's prefix cache stays warm, and it provides the tools
a model uses to read and write those blocks. `search.ts` and
`sqlite-provider.ts` are providers backed by the agent's SQLite. It is designed
to work alongside `sessions/`.

### `harness/pi/` — `agents/harness/pi` (experimental)
**Hosts the `pi-durable` agent runtime inside a DO.** `PiHarness` is a
Lifecycle capability. It opens pi over the DO's SQLite and wakes it after
eviction. pi owns the transcript, the inbox, and the runs. It also includes
session-store and skills glue.

### `harnesses/pi/` — `agents/harnesses/pi`
Deprecated alias that re-exports `harness/pi`.

### `codemode/` — `agents/codemode/ai`
**Removed entry point.** Importing it throws an error that points you to
`createCodeTool()` in `@cloudflare/codemode/ai`.

### `experimental/` — `agents/experimental/webmcp`
**WebMCP adapter, browser side.** It bridges tools from an MCP server to
Chrome's `navigator.modelContext`. The code is labelled "do not use in
production."

### `core/` — internal only
Small shared utilities: `events.ts` (`DisposableStore`) and
`base64-redaction.ts` (removes large base64 payloads from model-facing output
and from traces).

---

## 6. Test folders inside `src/`

Each folder has its own vitest config and runtime.

| Folder | Runtime | Covers |
| --- | --- | --- |
| `tests/` | Workers runtime (`vitest-pool-workers`, `wrangler.jsonc`) | The main suite: state, scheduling, queue, tasks, streams, sessions, sub-agents, callables, routing, MCP, email, workflows, browser, skills, observability. `tests/lifecycle/` has one file per Lifecycle function. `tests/capabilities/` has one harness DO per capability. `tests/agents/` holds fixture Agent classes. |
| `tests-d/` | `tsc` only | Type-level tests (`*.test-d.ts`) for exports, typed stubs, serializable rules, and similar. |
| `react-tests/` | Playwright Chromium | `useAgent`, cache and TTL, state sync. |
| `voice/tests/`, `voice/react-tests/` | Workers / Chromium | Voice mixins, protocol, providers, hooks. |
| `channels/__tests__/`, `channels/live-tests/` | Workers / real providers | Channels core and adapters. Live delivery tests are opt-in. |
| `chat/__tests__/` | Workers | Chat building blocks: turn queue, resumable streams, recovery fixtures. |
| `node-tests/` | Node | Checks that each entry point's bundle stays isolated, and tests the Vite skills plugin. |
| `webmcp-tests/` | Playwright Chromium | WebMCP adapter. |
| `x402-tests/` | — | x402 payment and auth. |
| `browser-tests/` | Real `wrangler dev` + Chromium | End-to-end tests of `BrowserConnector`. Not in the default test target. |
| `e2e-tests/` | Real workers | Recovery of managed fibers and facets after the process is killed. |

## 7. Folders at the package root (outside `src/`)

| Folder | Responsibility |
| --- | --- |
| `conformance/` | Runs the official `@modelcontextprotocol/conformance` referee against Agents' MCP client and server inside workerd. There are separate lanes for each protocol date, and expected failures are recorded in `baseline-*.yml` files. |
| `evals/` | `evalite` AI evals, for example how accurately LLMs produce schedules. Needs Cloudflare credentials. |
| `scripts/` | `build.ts` (tsdown build; **every export must be listed here**) and import-size reporting. |

---

## 8. Notes for the Python port

What depends on what (lower items need the items above them). Written before
scoping; the phase-1 scope and build order are in [scope.md](./scope.md) (§2,
§3, §5).

1. **`lifecycle/`**: fetch, alarm, and WebSocket entry points; the job queue
   and alarm loop; the capability base class. Everything else depends on it.
2. **`state/`, `websockets/`, `schedules/`, `queue/`**, plus `types.ts`,
   `retries.ts`, and `callable-decorator.ts`. These are what a minimal working
   `Agent` needs: state sync, RPC, scheduling.
3. **`index.ts` (`Agent`)**, `agent-routing.ts`/`routing/`, `client.ts`. This
   step needs the wire protocol in `types.ts` to stay byte-compatible so that
   the existing JS clients (`agents/client`, `useAgent`) still work against a
   Python server.
4. **`tasks/`, `streams/`, `sessions/`**: durable execution and storage. These
   are self-contained and rely only on Lifecycle services.
5. **`dynamic-agents/`, `sub-routing.ts`**: depend on DO facets, which work
   on Python Workers (confirmed).
6. **`mcp/`**, then `chat/`, `observability/`, and `models/`. These lean
   heavily on JS libraries (MCP SDK, AI SDK). A Python port would target
   Python counterparts rather than translating the code line by line. (Phase 1
   ports `chat/` without the AI SDK, structured events from `observability/`,
   and leaves `mcp/` and `models/` out.)
7. **Probably out of scope or deferred:** `react.tsx`, `voice/react`,
   `chat/react`, `experimental/webmcp`, `vite.ts`, `skills/compile.ts` (these
   are client-side or build tools), plus the legacy `ai-*` files and
   `codemode/`.
