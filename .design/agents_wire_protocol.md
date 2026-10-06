# Agents WebSocket wire protocol

The bytes that travel over an Agent WebSocket, as implemented by the
TypeScript SDK (`../agents/packages/agents`, v0.25.0, with chat frames shared
by `@cloudflare/ai-chat` and `@cloudflare/think`). A Python server that should
work with the unchanged JS clients (`AgentClient`, `useAgent`, `useAgentChat`,
`VoiceClient`) has to reproduce this exactly.

Related: [agents-subpackages.md](./agents-subpackages.md) (`websockets/`,
`chat/`, `voice/`), [lifecycle_capabilities.md](./lifecycle_capabilities.md)
(how the WebSockets capability claims sockets).

**Sources** (upstream paths under `packages/agents/src/` unless noted):
`types.ts` (`MessageType`), `index.ts` (Agent `onConnect`/`onMessage`, RPC,
`StreamingResponse`), `websockets/*` (capability, connection, Cap'n Web),
`client.ts` (`AgentClient`), `react.tsx` (`useAgent`), `agent-routing.ts`,
`sub-routing.ts`, `agent-tool-types.ts`, `chat/protocol.ts`,
`chat/wire-types.ts`, `chat/resume-handshake.ts`, `chat/replay-frames.ts`,
`chat/ws-chat-transport.ts`, `chat/__tests__/resume-handshake-frames.ts`
(golden frames), `voice/types.ts`, `voice/index.ts`, `voice/client.ts`,
`mcp/server/transport.ts`.

---

## 1. Conventions

- **Text frames are JSON objects with a `type` string.** The exception is the
  `agent-tool-event` frame, which also has `type`, but uses a hyphenated value
  rather than the `cf_` prefix.
- **Binary frames** carry no JSON envelope. The core protocol never sends them;
  they are passed to the user's `onMessage`. Voice uses them for audio (§8).
- **Unknown frames are ignored by clients.** Several frames are documented
  upstream as "backward-compatible — clients that don't understand it ignore
  it" (`cf_agent_stream_pending`, `cf_agent_chat_recovering`). New optional
  fields follow the same rule.
- **Optional fields are omitted, not `null`.** Upstream builds frames with
  conditional spreads (`...(probeId ? { probeId } : {})`). A Python server
  should leave absent fields out rather than send `null`.
- **Field order doesn't matter** (clients use `JSON.parse`). The golden chat
  fixtures compare with `toEqual`, so only the set of keys and their values
  matter.
- A server receiving **non-JSON text, or JSON that is not a protocol frame**,
  passes it to the user's `onMessage` unchanged.

---

## 2. Connection

### 2.1 URL

```
ws(s)://{host}/{prefix}/{agent-class}/{name}[/sub/{child-class}/{child-name}]*[/{path}]?_pk={connection-id}&{query…}
```

| Part | Rule |
| --- | --- |
| `prefix` | Default `agents`. Configurable in `routeAgentRequest({ prefix })` and on the client. |
| `agent-class` | The Durable Object binding / class name in kebab case, via `camelCaseToKebabCase` (`utils.ts:43`): `ChatAgent` → `chat-agent`. A name that is all upper case is lowercased and `_` becomes `-` (`MY_AGENT` → `my-agent`). `sub` is reserved and rejected as a class segment. |
| `name` | The instance name; the DO id is `idFromName(name)`. Client default: `"default"`. |
| `/sub/{child-class}/{child-name}` | Zero or more sub-agent (facet) hops; see §6. |
| `path` | Optional extra path (`useAgent({ path })`), after any `sub` hops. |
| `basePath` | Client option that replaces the whole `/{prefix}/{class}/{name}` part; the server then routes with its own logic (e.g. `getAgentByName`). |

Server matching (`agent-routing.ts:304`): split the path, check the prefix
segments, then take the next two segments as `namespace` and `name`. An
unknown namespace returns `400 Invalid request`. A non-matching prefix returns
`null`, meaning "not an agent route".

### 2.2 Query parameters

| Parameter | Meaning |
| --- | --- |
| `_pk` | **Connection id.** Chosen by the client (PartySocket); if empty or missing, the server generates one with `nanoid()` (`websockets.ts:404`). It is client-controlled, so two live sockets can share an id; upstream excludes connections by object identity, not id, for that reason. |
| `__agents_transport=capnweb` | Selects the Cap'n Web wire (§9). Absent means the default `cf-websocket` wire. |
| Anything else | User query (`useAgent({ query })`), e.g. auth tokens, or `mode=view` for readonly. Visible to the server through `ctx.request.url`. |

### 2.3 Internal headers (Worker → DO, never from browsers)

| Header | Set by | Purpose |
| --- | --- | --- |
| `x-agents-lifecycle-props` | `routeAgentRequest({ props })` | Encoded startup props; Lifecycle strips it on receipt. |
| `x-cf-agents-subagent-url` | Sub-agent forwarding | The outer client URL, recorded on the connection as `_cf_subAgentOuterUrl`. |

### 2.4 Close codes

| Code | Who | Meaning |
| --- | --- | --- |
| `1008`, `4000`–`4999` | Server → client | **Terminal.** The client (`isTerminalCloseEvent`, `client.ts:39`) stops reconnecting and surfaces an `AgentConnectionError`. Use these to reject a client permanently (auth failure, etc.). |
| any other | Server → client | The client reconnects (PartySocket backoff) until `maxRetries`. |
| `1001` `"Durable Object destroyed"` | Server | Sent to every connection when the agent is destroyed (`WebSockets.dispose`). |
| `1011` `"Uncaught exception during session setup"` | Server | When the upgrade throws, the server still completes the handshake (`101`), sends one text frame `{"error": "<stack>"}`, then closes with 1011 (`durable-object-lifecycle.ts` `fetch`). This is done because browsers hide HTTP error bodies for WebSocket requests. |
| `1005`, `1006`, `1015` | Runtime | Synthesized when there was no real close frame. The server does **not** echo them back; for every other peer-initiated close, it echoes the same code and reason to complete the close handshake. |

### 2.5 Hibernation

The default wire is accepted with the Hibernation API: the socket stays
connected while the DO is evicted from memory. The connection's metadata
lives in the socket attachment as `{ __pk: { id, tags, uri }, __user: <connection state> }`.
Tags: the connection id is always the first tag; at most 10 tags, each a
non-empty string of at most 256 characters. Internal per-connection flags are
stored inside `__user` under hidden `_cf_*` keys (`_cf_readonly`,
`_cf_no_protocol`, `_cf_subAgentOuterUrl`, …).

None of this is visible on the wire, and DOs can't migrate between the
TypeScript and Python SDKs ([scope.md](./scope.md) §4.5), so the Python
server needn't match it byte for byte. **Python keeps internal flags in their
own `__flags` key** instead of inside `__user`
([agent_api.md](./agent_api.md) §1.18 item 7). The runtime limits each socket's
attachment to **16,384 bytes** ([platform_verification.md](./platform_verification.md) §2.1).

---

## 3. Connect sequence (Agent)

When an upgrade is accepted, `Agent.onConnect` (`index.ts:2257`) runs, in
this order:

1. **Sub-agent check.** If the URL targets a facet, the connection is handed to
   the child and steps 2–6 run there instead (§6).
2. **Readonly flag** is set if `shouldConnectionBeReadonly(connection, ctx)`
   returns true, before anything is sent.
3. **Protocol frames**, if `shouldSendProtocolMessages(connection, ctx)` returns
   true (default true):
   1. `cf_agent_identity`, with `stateFollows: true` when state exists, unless
      `static options = { sendIdentityOnConnect: false }`;
   2. `cf_agent_state`, if state exists (whether or not identity was sent);
   3. `cf_agent_mcp_servers`, always.

   If it returns false, the connection is marked no-protocol and **none of
   the three is sent**, now or later (no state broadcasts, no MCP updates).
4. **`agent-tool-event` replay** of stored agent-tool runs (§5.6), with
   `replay: true`. This is sent even to no-protocol connections.
5. User `onConnect(connection, ctx)`.
6. Mixins add their own frames by wrapping `onConnect`: voice sends `welcome`
   and `status` (§8); chat hosts send their connect payload and possibly a
   proactive `cf_agent_stream_resuming` (§7.5).

**Client side** (`client.ts:690`): `ready` resolves on `cf_agent_identity`. If
the identity frame has `stateFollows: true`, the client waits for the next
`cf_agent_state` frame first, so `ready` never resolves with state missing.
On reconnect, a different `name`/`agent` in the identity frame triggers
`onIdentityChange` (or a console warning). The server's identity is
authoritative.

Example: the first frames on a new connection to an agent with state:

```json
{"type":"cf_agent_identity","name":"alice","agent":"chat-agent","stateFollows":true}
{"type":"cf_agent_state","state":{"count":0}}
{"type":"cf_agent_mcp_servers","mcp":{"servers":{},"tools":[],"prompts":[],"resources":[]}}
```

---

## 4. Server message dispatch (Agent)

For each incoming frame, `Agent.onMessage` (`index.ts:2117`) decides in this
order. Mixins wrap `onMessage` before Agent's wrapper sees the frame: voice
intercepts its types and binary audio first (§8), chat hosts intercept the
`cf_agent_*` chat types (§7).

1. **Sub-agent forwarding:** a frame on a connection bound to a facet is
   forwarded to the child.
2. **Binary frame:** to user `onMessage`.
3. **Text that isn't JSON:** to user `onMessage`.
4. **`cf_agent_state`:** applied as a state update (§5.2). Consumed.
5. **`rpc` request:** handled as RPC (§5.4). Consumed.
6. **Anything else:** to user `onMessage`.

---

## 5. Core frames

### 5.1 Catalog

| `type` | Direction | Purpose |
| --- | --- | --- |
| `cf_agent_identity` | S → C | Which agent and instance the client is connected to |
| `cf_agent_state` | S ↔ C | Full state snapshot (push or update) |
| `cf_agent_state_error` | S → C | A client state update was refused |
| `rpc` | C → S | Call a `@callable` method |
| `rpc` | S → C | RPC result, stream chunk, or error |
| `cf_agent_mcp_servers` | S → C | MCP client state (servers, tools, prompts, resources) |
| `agent-tool-event` | S → C | Progress of agent-as-tool runs |

`MessageType` (`types.ts`) also defines `cf_agent_session`,
`cf_agent_session_error`, and `cf_mcp_agent_event`. The first two have no
users anywhere in `packages/`. `cf_mcp_agent_event` is internal (§10).

### 5.2 State

```json
{"type":"cf_agent_identity","name":"alice","agent":"chat-agent","stateFollows":true}
{"type":"cf_agent_state","state":<any JSON>}
{"type":"cf_agent_state_error","error":"Connection is readonly"}
```

- **Identity:** `name` is the instance's logical name; `agent` is the class name
  in kebab case. `stateFollows` is present only when a state frame comes
  next.
- **State is always a full snapshot.** There are no patches. The server pushes
  it on connect and broadcasts it after every change, **excluding the
  connection that caused the change** (which already has the value it sent).
  It is not sent to no-protocol connections.
- **Client → server** `cf_agent_state` replaces the state. The server:
  - refuses it from a readonly connection with
    `{"type":"cf_agent_state_error","error":"Connection is readonly"}`;
  - otherwise runs the host's `validateStateChange`; if that throws, the full
    error is logged server-side and the client gets a generic
    `{"type":"cf_agent_state_error","error":"State update rejected"}`;
  - otherwise saves the state and broadcasts it to everyone else.
- The client applies its own update optimistically (`setState` sends the frame
  and sets local state immediately).

### 5.3 Readonly connections

A connection is readonly if `shouldConnectionBeReadonly` returned true, or if
the server later called `setConnectionReadonly`. It still **receives** state
and **can call** RPC methods. It **cannot change state**: neither with client
`cf_agent_state` frames nor through a `@callable` method that calls
`setState` (upstream docs, `docs/agents/readonly-connections.md`).

### 5.4 RPC

**Request** (client → server):

```json
{"type":"rpc","id":"<string>","method":"<name>","args":[...]}
```

All four fields are required (`id` and `method` strings, `args` an array),
otherwise the frame is not treated as RPC and goes to user `onMessage`. The
client generates `id`.

**Response** (server → client):

```json
{"type":"rpc","id":"<same id>","success":true,"done":true,"result":<any JSON>}
{"type":"rpc","id":"<same id>","success":false,"error":"<message>"}
```

**Server rules** (`index.ts:2170`):
- The method must exist on the agent, otherwise
  `error: "Method <name> does not exist"`.
- The method must be marked `@callable()`, otherwise
  `error: "Method <name> is not callable"`.
- An exception thrown by the method becomes `success: false` with
  `error = err.message` (`"Unknown error occurred"` for non-`Error` throws).
  Only the message travels; no stack and no error type.
- Non-streaming success always has `done: true`.
- A response for a connection that has already closed is dropped silently.

**Streaming methods** (`@callable({ streaming: true })`): the method receives a
`StreamingResponse` as its first argument, before the client's `args`.

```json
{"type":"rpc","id":"7","success":true,"done":false,"result":"chunk 1"}
{"type":"rpc","id":"7","success":true,"done":false,"result":"chunk 2"}
{"type":"rpc","id":"7","success":true,"done":true,"result":<final or omitted>}
```

- `send(chunk)` → `done: false`.
- `end(final?)` → `done: true` with `result: final`. If `final` is undefined,
  `result` is omitted (`JSON.stringify` drops undefined).
- `error(message)` → `success: false`, which ends the stream.
- If the method throws before closing the stream, the server sends
  `error(message)` automatically. Calls after the stream is closed do nothing.

**Client rules** (`client.ts:725`):
- `success: false` rejects the call (`new Error(error)`) and calls
  `stream.onError`.
- `done: false` calls `stream.onChunk(result)`.
- `done: true` resolves the call with `result` and calls `stream.onDone`.
- **A response without `done` is treated as final.** Older servers send this,
  so a server may omit `done` on non-streaming results.
- A response with no matching pending call is dropped with a warning (for
  example, after a client-side timeout).
- Timeouts are client-side only: 30 s default for non-streaming calls
  (`defaultCallTimeout`), none for streaming calls. The server is never told
  about them.
- On a temporary disconnect, calls already transmitted are rejected
  (`"Connection closed"`); calls still in the send buffer are re-sent on
  reconnect.

**Serialization:** results are serialized with `JSON.stringify`. Values JSON
can't represent are not round-tripped (the `Serializable` types
enforce this at compile time).

**Python server differences (same frames on the wire,
[agent_api.md](./agent_api.md) §1.8):** a streaming method that returns
without closing gets an automatic `done: true` (upstream leaves the client
waiting); async-generator callables stream one `done: false` frame per
`yield`; dataclass and `datetime` results are serialized automatically
(`datetime` as epoch milliseconds).

### 5.5 MCP servers

```json
{
  "type": "cf_agent_mcp_servers",
  "mcp": {
    "servers": {
      "<serverId>": {
        "name": "github",
        "server_url": "https://…",
        "auth_url": "https://…" | null,
        "state": "authenticating" | "connecting" | "connected" | "discovering" | "ready" | …,
        "error": "…" | null,
        "instructions": "…" | null,
        "capabilities": { … } | null
      }
    },
    "tools":     [ { …MCP Tool…,     "serverId": "<serverId>" } ],
    "prompts":   [ { …MCP Prompt…,   "serverId": "<serverId>" } ],
    "resources": [ { …MCP Resource…, "serverId": "<serverId>" } ]
  }
}
```

Sent on connect (to protocol-enabled connections) and broadcast whenever MCP
server state changes. These fields really are snake_case (`server_url`,
`auth_url`). `error` can contain untrusted text from OAuth providers.
`useAgent` exposes this frame as `onMcpUpdate`.

### 5.6 Agent-tool events

These report progress when an agent runs another agent as a tool
(`agent-tool-types.ts:431`).

```json
{"type":"agent-tool-event","parentToolCallId":"call_1","sequence":0,"event":{"kind":"started","runId":"r1","agentType":"researcher","inputPreview":{…},"order":0,"display":{…}}}
{"type":"agent-tool-event","parentToolCallId":"call_1","sequence":1,"event":{"kind":"chunk","runId":"r1","body":"<child chunk>"}}
{"type":"agent-tool-event","parentToolCallId":"call_1","sequence":2,"event":{"kind":"finished","runId":"r1","summary":"…"}}
```

| `event.kind` | Fields |
| --- | --- |
| `started` | `runId`, `agentType`, `inputPreview?`, `order`, `display?` |
| `chunk` | `runId`, `body` (string), `unstoredId?` |
| `finished` | `runId`, `summary` |
| `error` | `runId`, `error` |
| `aborted` | `runId`, `reason?` |
| `interrupted` | `runId`, `error`, `reason?` (`no-progress`, `window-exceeded`, `not-tailable`, `inspect-timeout`, `inspect-failed`, `recovery-deadline`, `budget-exceeded`), `childStillRunning?` |

- `parentToolCallId` is omitted when the run has none.
- `sequence` numbers events within one run: `started` is 0, and stored chunk
  *i* is *i + 1*, on both the live and replay paths. That lets clients
  de-duplicate replayed events against live ones.
- `replay: true` is set on the connect-time replay (§3, step 4).
- These are sent with plain `broadcast`, **not** the protocol-gated broadcast.
  No-protocol connections receive them too. This may be unintentional
  upstream; check before relying on it.

---

## 6. Sub-agents (facets)

A client reaches a child agent through its parent:

```
/agents/{parent-class}/{parent-name}/sub/{child-class}/{child-name}[/sub/…][/{path}]
```

`useAgent({ agent, name, sub: [{ agent: "chat", name: chatId }] })` builds this
URL. The parent DO accepts the socket (so hibernation and the socket itself
stay on the parent) and forwards connect, message, and close events to the
child over internal RPC. Parents can refuse access with `onBeforeSubAgent`.

**On the wire this is transparent.** The client receives the **child's**
connect sequence. The identity frame carries the child's logical `name` and
kebab-case class as `agent`, and all later frames are the child's. A Python
server needs the same forwarding only if it implements facets.

---

## 7. Chat protocol (`cf_agent_chat_*`)

Defined in `packages/agents/src/chat/` (`protocol.ts`, `wire-types.ts`) and
spoken by `@cloudflare/ai-chat` and `@cloudflare/think` servers and by
`useAgentChat` / `WsChatTransport` clients. These frames share the agent
socket with the core frames.

### 7.1 Catalog

| `type` | Direction | Purpose |
| --- | --- | --- |
| `cf_agent_use_chat_request` | C → S | Start a chat turn |
| `cf_agent_use_chat_response` | S → C | Stream chunk, replay chunk, or terminal frame for a turn |
| `cf_agent_chat_messages` | S ↔ C | Full transcript snapshot |
| `cf_agent_chat_clear` | S ↔ C | Clear history |
| `cf_agent_chat_request_cancel` | C → S | Stop a running turn |
| `cf_agent_tool_result` | C → S | Result of a client-side tool |
| `cf_agent_tool_approval` | C → S | Approve or deny a tool that needs approval |
| `cf_agent_message_updated` | S → C | One message changed (e.g. a tool result applied) |
| `cf_agent_stream_resume_request` | C → S | "Is there a stream I should resume?" |
| `cf_agent_stream_resuming` | S → C | "Yes, ACK to get the replay" |
| `cf_agent_stream_resume_ack` | C → S | "Send me the replay" |
| `cf_agent_stream_resume_none` | S → C | "Nothing to resume for you" |
| `cf_agent_stream_pending` | S → C | "A turn is accepted but hasn't started streaming; keep waiting" |
| `cf_agent_chat_recovering` | S → C | Progress hint: a durable turn is being recovered |

### 7.2 Turn request and response

**Request:**

```json
{
  "type": "cf_agent_use_chat_request",
  "id": "<requestId, nanoid(8)>",
  "init": {
    "method": "POST",
    "body": "{\"messages\":[…UIMessage…],\"trigger\":\"submit-message\",…extra body…}"
  }
}
```

- `init.body` is a **JSON string** (it mirrors `fetch`'s `RequestInit`). It
  decodes to `{ messages, trigger, ...extra }`, where `trigger` is
  `"submit-message"` or `"regenerate-message"`, and the extra fields come from
  `prepareBody` / the `body` option (for example the client tool schemas).
- `init` may also carry other `RequestInit` fields (`headers`, …); the
  standard client sends `method` and `body`.

**Response frames:**

```json
{"type":"cf_agent_use_chat_response","id":"<requestId>","body":"<UIMessageChunk JSON>","done":false,"seq":0}
{"type":"cf_agent_use_chat_response","id":"<requestId>","body":"","done":true,"messageIds":["u1"],"outcome":"completed"}
{"type":"cf_agent_use_chat_response","id":"<requestId>","body":"<error text>","done":true,"error":true}
```

| Field | Meaning |
| --- | --- |
| `id` | The request id |
| `body` | One **JSON-serialized AI SDK `UIMessageChunk`** (the client runs `JSON.parse(body)`), or the error text when `error: true`. Empty on control frames. |
| `done` | Last frame for this request |
| `error?` | The turn failed; `body` is the message |
| `continuation?` | Append to the last assistant message instead of starting a new one |
| `replay?` | The chunk is replayed from storage (§7.4) |
| `replayComplete?` | Replay finished; the stream is still live |
| `seq?` | Chunk index within the stream; a live chunk carries the index its replay would carry, so clients can skip duplicates |
| `messageIds?` | On terminal frames: ids of the user messages the request carried, so the client settles exactly those sends |
| `outcome?` | On the terminal `done` frame: `completed`, `error`, `aborted`, `skipped`, or `recovering`. If absent, it means `error` when `error` was set on this or an earlier frame, otherwise `completed`. |

**`start` and `finish` chunks are rewritten by the server** before they're
stored and broadcast (`ai-chat/src/index.ts:7303`):
- on a new turn, a `start` with no `messageId` gets the server's assistant
  message id stamped on; on a continuation, `messageId` is removed;
- a `finish` with `finishReason` is sent as
  `{"type":"finish","messageMetadata":{"finishReason":…}}` (`#677`).

The Python SDK also guarantees one `start` and one `finish` per turn, including
on the plain-text path ([chat_models.md](./chat_models.md) §3).

### 7.3 Other chat frames

```json
{"type":"cf_agent_chat_messages","messages":[…UIMessage…],"connect":true}
{"type":"cf_agent_chat_clear"}
{"type":"cf_agent_chat_request_cancel","id":"<requestId>"}
{"type":"cf_agent_message_updated","message":{…UIMessage…}}
{"type":"cf_agent_chat_recovering","recovering":true,"id":"<requestId>"}
```

- `cf_agent_chat_messages` from the server is authoritative, **except** when
  `connect: true` (the transcript sent to a newly connected client). That
  snapshot predates anything the client buffered while disconnected, so the
  client keeps its optimistic sends.
- `cf_agent_chat_recovering`: `recovering: false` clears the hint. `id` is the
  recovery-root request id, when known.

**Tool result** (client → server):

```json
{
  "type": "cf_agent_tool_result",
  "toolCallId": "call_1",
  "toolName": "getLocation",
  "output": <any>,
  "state": "output-available" | "output-error",
  "errorText": "…",
  "autoContinue": true,
  "clientTools": [ { "name": "…", "description": "…", "parameters": { …JSON Schema… } } ]
}
```

`state`, `errorText`, `autoContinue`, and `clientTools` are optional.
`clientTools` sends the client's tool schemas for the continuation (the client
is the source of truth for them).

**Tool approval** (client → server):

```json
{"type":"cf_agent_tool_approval","toolCallId":"call_1","approved":true,"autoContinue":true}
```

### 7.4 Stream resume handshake

This is how a client that reconnects mid-turn picks the stream back up. The
server side is shared in `chat/resume-handshake.ts`; the frame shapes are
frozen in `chat/__tests__/resume-handshake-frames.ts`.

```
client                                             server
  │  (connect)                                        │
  │◀── cf_agent_stream_resuming {id}                  │  proactive notify, if a stream is active
  │                                                   │
  │── cf_agent_stream_resume_request {probeId} ──────▶│  sent once the client's handler is registered
  │◀── one of:                                        │
  │      cf_agent_stream_resuming {id, probeId}       │  active stream (or a pending terminal)
  │      cf_agent_stream_pending  {id?, probeId}      │  accepted turn, not streaming yet: keep waiting
  │      cf_agent_stream_resume_none {reason, probeId}│  nothing for this connection
  │                                                   │
  │── cf_agent_stream_resume_ack {id} ───────────────▶│
  │◀── cf_agent_use_chat_response {replay:true, seq…} │  stored chunks, then either
  │◀──   {done:false, replay:true, replayComplete:true}│  … stream still live: live frames follow
  │◀──   {done:true, replay:true, …}                  │  … or the stream already ended
```

Decisions when the server receives `cf_agent_stream_resume_request`, in order:

1. **There is an active stream.** If the active turn is a continuation owned
   by **another** connection that is still present, send
   `resume_none {reason:"continuation-owned"}`. Otherwise send `resuming {id}`
   and mark the connection as pending: it is left out of live broadcasts until
   it ACKs.
2. **A continuation turn is accepted for this connection (or any connection)
   but hasn't started.** Park the connection and send `stream_pending {id}`.
   It is switched to `resuming` once the stream starts.
3. **A turn ended while no client was connected, with a stored terminal
   result.** Send `resuming {id}`. The terminal error frame is delivered after
   the ACK.
4. **A normal turn is accepted but hasn't started streaming.** Send
   `stream_pending`. It later becomes `resuming`, or `resume_none` if the turn
   ends without streaming.
5. **Otherwise** send `resume_none {reason:"idle"}`. This is the only reply
   that proves the agent is idle.

When the server receives `cf_agent_stream_resume_ack {id}`:

- **Active stream with this id:** replay the stored chunks as `use_chat_response` frames with
  `replay: true` and `seq` (plus `continuation: true` if the live stream had
  it). If the stream was left without a reader after hibernation, the partial
  assistant message is rebuilt from the stored chunks and saved.
- **Active stream with a different id:** ignore the ACK.
- **Stream closed, message still being saved:** replay the stored chunks
  without a terminal frame; the live terminal frame follows the transcript.
- **Pending terminal error:** replay the partial chunks, then
  `{body, done:true, error:true, id, type}` **without** `replay`. That
  matches a live terminal exactly, and is the only path that surfaces as
  `useChat.error` on the client.
- **Otherwise:** replay completed chunks if any are stored; if none, send
  `{body:"", done:true, id, type, replay:true, messageIds?, outcome?}`.

**Invariants** (from the golden fixture, `HANDSHAKE_INVARIANTS`):
- **`resuming` can be sent twice for one request**: once as the proactive
  notify on connect and again in reply to the explicit request. The server must
  not de-duplicate; the client de-duplicates its ACK (#1733).
- **Terminal frames are never part of the connect payload.** They are delivered
  only through the handshake (#1645).
- **Direct replies echo `probeId`; proactive notifies omit it.** The client
  ignores a `resume_none` whose `probeId` doesn't match its probe.
- **Client timeouts:** a short safety timeout for the probe, extended (and
  refreshed) after each `stream_pending`. If the socket closes before the
  terminal `done`, the client reports an interrupted turn rather than a
  completed one.

### 7.5 What chat hosts send on connect

This is the one place the two hosts differ (`HANDSHAKE_INVARIANTS.idleConnectPayloadDiverges`):

| Host | Connect payload |
| --- | --- |
| `@cloudflare/ai-chat` | Only `cf_agent_chat_recovering`, if a recovery is in progress |
| `@cloudflare/think` | `cf_agent_chat_messages {connect:true}` (the transcript), then `cf_agent_chat_recovering` if recovering |

Both also send the proactive `cf_agent_stream_resuming` when a stream is
active.

### 7.6 HTTP: `get-messages`

This isn't a WebSocket frame, but it's part of the chat protocol.
`useAgentChat` fetches its initial transcript over HTTP
(`agents/src/chat/react.tsx:978`) from the agent's URL with `/get-messages`
appended:

```
GET /agents/{agent-class}/{name}/get-messages
→ 200, content-type: application/json
  [ {…UIMessage…}, {…UIMessage…}, … ]
```

- **The server matches any request whose last path segment is `get-messages`**
  (`ai-chat/src/index.ts:1742`), before the user's `on_request`.
- **The body is the full stored transcript** (not just the in-memory window),
  streamed in bounded batches from Sessions (`historyBatches()`), so it is
  never held in memory as one string.
- CORS headers come from `routeAgentRequest({ cors })` as for any agent HTTP
  route.

---

## 8. Voice protocol

`withVoice(Agent)` / `withVoiceInput(Agent)` on the server, and `VoiceClient`
on the client (`voice/types.ts`). Protocol version: **1**
(`VOICE_PROTOCOL_VERSION`). These frames share the agent socket; the voice
mixin wraps `onConnect`/`onMessage` and passes anything that isn't voice on
to Agent.

### 8.1 Frames

**Client → server:**

| Frame | Meaning |
| --- | --- |
| `{"type":"hello","protocol_version":1}` | Sent on every open. The server accepts it and does nothing. |
| `{"type":"start_call","preferred_format":"mp3"\|"pcm16"\|"wav"\|"opus"}` | Start a call. Opens the STT session. `preferred_format` is optional. |
| `{"type":"end_call"}` | End the call; closes the STT session |
| `{"type":"start_of_speech"}` / `{"type":"end_of_speech"}` | Client voice-activity hints; accepted, currently unused by the server (the STT model detects turns) |
| `{"type":"interrupt"}` | Barge-in: abort the current reply |
| `{"type":"text_message","text":"…"}` | A typed turn instead of speech |
| **binary** | Microphone audio: raw PCM, **16 kHz mono 16-bit little-endian** |

**Server → client:**

| Frame | Meaning |
| --- | --- |
| `{"type":"welcome","protocol_version":1,"diagnostics":{"browser_console":true}}` | First voice frame on connect; `diagnostics` present only if enabled |
| `{"type":"status","status":"idle"\|"listening"\|"thinking"\|"speaking"}` | Pipeline state; `idle` is sent right after `welcome` |
| `{"type":"audio_config","format":"mp3"\|"pcm16"\|"wav"\|"opus","sampleRate":16000}` | Format of the binary audio that follows; sent at call start. The server always includes `sampleRate` (default `16000`, `voice/index.ts:165`), but the type marks it optional. |
| `{"type":"transcript","role":"user"\|"assistant","text":"…"}` | A complete transcript line |
| `{"type":"transcript_start","role":…}` / `transcript_delta {text}` / `transcript_end {text}` | A streamed assistant transcript |
| `{"type":"transcript_interim","text":"…"}` | Unstable interim user transcript |
| `{"type":"playback_interrupt"}` | Stop playing queued audio now |
| `{"type":"metrics","llm_ms":…,"tts_ms":…,"first_audio_ms":…,"total_ms":…}` | Compact per-turn latency summary |
| `{"type":"turn_metrics", …VoiceTurnMetrics}` | Detailed per-turn timing (`turnId`, `source`, `outcome`, `turnTotalMs`, optional durations) |
| `{"type":"completion_outcome","code":…,"stage":"llm","finishReason"?,"partialOutput":bool}` | Non-ordinary LLM completion (`no_output`, `output_limit`, `content_filtered`, `model_error`) |
| `{"type":"error","message":"…","code"?,"stage"?,"retryable"?}` | Voice error (`stt_startup_failed`, `stt_connection_lost`) |
| `{"type":"diagnostic","event":"…","timestamp":…,"data"?}` | Optional diagnostics; event names are not stable |
| **binary** | Synthesized speech in the `audio_config` format (default `mp3`) |

### 8.2 Flow

An illustrative happy path. The exact `status` transitions depend on the
mixin (`withVoice` vs `withVoiceInput`) and on errors and interrupts; see the
`status` sends in `voice/index.ts`.

```
C: (open) → hello
S: welcome, status{idle}, …then the normal Agent connect frames
C: start_call → S: audio_config, status{listening}
C: binary PCM … (continuous)
S: transcript_interim …, transcript{user}, status{thinking},
   transcript_start / transcript_delta … / transcript_end, status{speaking},
   binary audio …, turn_metrics, status{listening}
C: interrupt → S: playback_interrupt
C: end_call → S: status{idle}
```

If the socket drops during a call, the client sends `hello` and then
`start_call` again as soon as it reconnects. The microphone keeps running
throughout.

---

## 9. Cap'n Web transport (experimental)

Selected with `?__agents_transport=capnweb` (`AgentClient({ transport: "capnweb" })`).
The same JSON protocol frames travel inside a [Cap'n Web](https://github.com/cloudflare/capnweb)
RPC session instead of as raw WebSocket text frames:

- **Client → server:** the server's session root has one method,
  `__cf_agent_send(message)`, through which the client sends every frame
  (`string | ArrayBuffer | ArrayBufferView`).
- **Server → client:** the client exposes a root with `message(value)`, and the
  server delivers each frame through it.
- **Callables:** on this wire, the host's `callables` are native methods on the
  session root. An `RpcTarget` result arrives as a live remote object, and
  calls can be pipelined. On the JSON wire, a method that returns an `RpcTarget`
  fails with `"Method X returns an RpcTarget, which only the capnweb transport can carry"`.
- **Not hibernating:** the DO stays in memory while the session is open. A
  reconnect with the same `_pk` replaces the previous session.
- Method names `then` and `__cf_agent_send` cannot be exposed as callables.

---

## 10. Internal frames (not client-facing)

- **`cf_mcp_agent_event`** (`mcp/server/transport.ts:333`): the legacy SDK v1
  `McpAgent` tunnels its SSE / Streamable HTTP responses from the DO to the
  Worker over an internal WebSocket.
  `{"type":"cf_mcp_agent_event","event":"event: message\nid: …\ndata: <JSON-RPC>\n\n","close"?:true}`.
  The Worker writes `event` straight into the HTTP response and closes when
  `close` is true. It is only needed if the legacy `McpAgent` is ported.
- **`cf_agent_session` / `cf_agent_session_error`**: defined in `MessageType`,
  but nothing in `packages/` uses them.

---

## 11. Notes for the Python port

**Must match exactly** for the existing JS clients to work:
- the URL scheme, including the kebab-case rule and the `sub` segments, and
  `_pk` handling (§2);
- the connect order: identity (with `stateFollows`) → state → MCP servers
  (§3). `ready` depends on it;
- the `rpc` frame shapes and the streaming `done` rules; errors carry only the
  message string (§5.4);
- full-snapshot state, with the sender excluded from the broadcast, and the two
  `cf_agent_state_error` strings (§5.2);
- the meaning of terminal close codes (`1008`, `4000`–`4999`) (§2.4);
- for chat: `init.body` as a JSON **string**, `body` as a JSON-serialized
  `UIMessageChunk`, and the resume handshake invariants (§7.4);
- facet forwarding (§6): sub-agents are in scope ([scope.md](./scope.md) §2.6).

**Can wait:**
- Voice (§8, out of scope): binary PCM 16 kHz mono s16le in, and audio in the
  `audio_config` format out.
- Cap'n Web (§9): experimental and depends on the JS `capnweb` library.
- `cf_mcp_agent_event` (§10): only for the legacy `McpAgent`.
- MCP servers (§5.5): MCP is out of scope; `cf_agent_mcp_servers` is still
  sent with an empty state on connect.

**Worth checking in upstream before porting:**
- whether `agent-tool-event` frames reaching no-protocol connections is
  intended (§5.6);
- what the client does when `sendIdentityOnConnect: false`: `ready` resolves on
  the identity frame, so confirm `useAgent`/`AgentClient` behavior when it is
  never sent (§3).
