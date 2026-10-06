# Observability (decided; implemented in step 5)

**Implemented** in `src/agents/observability/` and `_ffi.ConsoleHandler`
([agent_api.md](./agent_api.md) §1.19 item 11).

How the Python SDK reports what agents do. Upstream:
`../agents/packages/agents/src/observability/` and
`docs/agents/observability.md`.

Related: [scope.md](./scope.md) §4.5,
[lifecycle_capabilities.md](./lifecycle_capabilities.md) §4.3 (the `events`
service), [utilities.md](./utilities.md) §3 (FFI).

---

## 1. What "observability" covers upstream

Three separate systems:

| System | What it is | Phase 1 |
| --- | --- | --- |
| **Structured events** | `{type, agent, name, payload, timestamp}` records for RPC calls, state changes, schedules, fibers, chat turns, … | **This doc** |
| **Tracing (spans)** | Workers' native custom spans (shown in Workers Observability's traces view and exported over OTLP): `Agent`'s own spans via `_withAgentSpan` (`agent_initialization`, `agent_start`, `initialize_agent_storage`, `restore_agent_state`, `run_user_on_start`, `recover_agent_work`, `alarm`, `schedule_agent_alarm`, `initialize_fiber`, `persist_fiber_snapshot`, `finalize_fiber`), plus `wrapAISDK()` OpenTelemetry GenAI spans. A no-op when the runtime has no tracing API (`observability/tracing/cloudflare.ts`). | **Out of scope** (decided, §5.5); when porting, `_withAgentSpan` wrappers are dropped and the wrapped code kept |
| **Console logs** | `console.error` / `console.warn` in error paths | Ported as Python `logging` (`error` / `warning`) |

---

## 2. How structured events work upstream

```
capability ──self.lifecycle.events.emit(type, payload)──▶ Lifecycle event sink ──▶ Agent._emit(type, payload)
Agent code ───────────────────────────────────────────────────────────────────────▶ Agent._emit(type, payload)
                                                                                        │
                                       event = {type, agent: <class name>, name: <instance name>, payload, timestamp}
                                                                                        ▼
                                                  self.observability.emit(event)
                                                  default: publish to node:diagnostics_channel
                                                  channel picked by prefix: agents:rpc, agents:schedule, …
```

- **Overridable.** `observability` is a property on `Agent`
  (`index.ts:1484`). Assign a custom `{ emit(event) }` to send events
  elsewhere, or `undefined` to disable them.
- **The default publishes to diagnostics channels** (`observability/diagnostics.ts`):
  `agents:state`, `agents:rpc`, `agents:message`, `agents:chat`,
  `agents:transcript`, `agents:fiber`, `agents:task`, `agents:stream`,
  `agents:agent_tool`, `agents:schedule` (schedules and queues),
  `agents:lifecycle`, `agents:workflow`, `agents:mcp`, `agents:email`,
  `agents:channel`.
- **What `node:diagnostics_channel` is:** a Node.js built-in module for
  publish/subscribe of diagnostic messages. Code creates a named channel
  (`channel("agents:rpc")`) and calls `.publish(message)`; anyone can
  `subscribe("agents:rpc", callback)` to receive messages. Publishing to a
  channel with no subscribers is essentially free (`channel.hasSubscribers`),
  which is why libraries use it for opt-in telemetry. Cloudflare Workers
  implements it under the `nodejs_compat` compatibility flag, and **forwards
  every published message to attached Tail Workers**.
- **Silent unless consumed:**
  - in-process, through `subscribe("rpc", cb)` from `agents/observability` (or
    raw `node:diagnostics_channel` `subscribe`);
  - in production, **Tail Workers receive every diagnostics-channel message
    automatically** as `event.diagnosticsChannelEvents` (`{channel, message,
    timestamp}`), with no code in the agent.

---

## 3. In-scope events

Event names and payloads from upstream (`docs/agents/observability.md`
"Event reference" and the emitting code). Payload keys are camelCase upstream.

| Source | Events |
| --- | --- |
| `Agent` core | `connect` `{connectionId}`, `disconnect` `{connectionId, code, reason}`, `destroy` `{}`, `rpc` `{method, streaming?}`, `rpc:error` `{method, error}`, `state:update` `{}` |
| Scheduler | `schedule:create` / `:execute` / `:cancel` `{callback, id}`, `schedule:retry` `{callback, id, attempt, maxAttempts}`, `schedule:error` `{callback, id, error, attempts}`, `schedule:duplicate_warning` `{callback, count, type}` |
| Queue | `queue:create` `{callback, id}`, `queue:retry` `{callback, id, attempt, maxAttempts}`, `queue:error` `{callback, id, error, attempts}` |
| Tasks | `task:accepted`, `task:attempt:started`, `task:attempt:interrupted`, `task:step:started`, `task:step:retry`, `task:step:completed`, `task:waiting`, `task:completed`, `task:failed`, `task:cancelled`, `task:deleted` |
| Sessions | `session:message:appended` (`{sessionId, messageId, tokenEstimate}`), `session:message:updated` (`{sessionId, messageId}`), `session:messages:deleted` (`{sessionId, count}`), `session:cleared` (`{sessionId}`), `session:error` (`{sessionId, event, error}`) ([sessions_engine.md](./sessions_engine.md)) |
| Streams | `stream:opened`, `stream:closed`, `stream:errored`, `stream:deleted` |
| Sessions | `session:message:appended`, `session:message:updated`, `session:messages:deleted`, `session:cleared`, `session:error` (deferred features also emit `session:compacted`, `session:migration:incomplete`) |
| Lifecycle job driver | `job:slow_dispatch`, `job:backlog_warning`, `alarm:memory_limit_reset` |
| Streams | `stream:opened`, `stream:closed`, `stream:errored` (with `reason` when given), `stream:deleted`; each `{streamId}` ([streams_engine.md](./streams_engine.md)) |
| Fibers | `fiber:run:started` / `:completed` / `:failed` / `:interrupted`, `fiber:recovery:detected` / `:attempt` / `:handled` / `:skipped` / `:failed` (payloads in [fibers_api.md](./fibers_api.md)) |
| `AIChatAgent` | `message:request` / `:response` / `:clear` / `:cancel` (`{requestId}`) / `:error` (`{error}`) (12b), `tool:result`, `tool:approval` (12c), `chat:recovery:*`, `chat:stream:stalled` (12d). (`chat:request:failed` is emitted by upstream's `Think` only, not `AIChatAgent`: [chat_engine.md](./chat_engine.md) §5.6.) |

---

## 4. Python design (decided)

```python
@dataclass(slots=True, kw_only=True)
class ObservabilityEvent:
    type: str                    # upstream name, e.g. "rpc", "fiber:run:started"
    agent: str                   # agent class name
    name: str                    # agent instance name
    payload: dict[str, Any]      # upstream payload keys (see §5.3)
    timestamp: datetime          # timezone-aware UTC; serialized as epoch ms to match upstream's JSON

class Observability(Protocol):
    def emit(self, event: ObservabilityEvent) -> None: ...

class Agent:
    observability: Observability | None = <default>   # override per class; None disables
```

(A class attribute is fine here, unlike capabilities: the default sink holds
no per-instance state, and `agent` / `name` are added per event.)

- **Capabilities emit through the Lifecycle `events` service**
  (`self.lifecycle.events.emit(type, payload)`), which is already part of the
  Lifecycle design. Capabilities never import observability.
- **`Agent` installs the Lifecycle event sink** and forwards to
  `self.observability.emit(...)`, adding `agent`, `name`, and `timestamp`.
- **A plain DO** using capabilities without `Agent` sets its own event sink, or
  none.
- **Emitting never raises into the caller.** An exception from a custom `emit`
  is logged and dropped: a telemetry failure must not fail an RPC call or a
  write. (This is a specific failure being handled, not blanket defensiveness.)

---

## 5. Decisions

1. ~~Port event emission in phase 1, or leave it out?~~ **Decided: port it**,
   with the §4 shape (`ObservabilityEvent`, the `Observability` protocol,
   `Agent.observability`, capabilities emitting through the Lifecycle `events`
   service).
2. ~~The default `observability`~~: **decided: native Python `logging`.**
   - The default implementation writes each event to the standard-library
     logger **`agents.events`**, at **`DEBUG`**, as one JSON object per event
     (`{"type", "agent", "name", "payload", "timestamp"}`), and notifies the
     in-process `subscribe` registry (§5.4).
   - **Why `DEBUG`:** error paths already log on their own (upstream's
     `console.error` calls are ported as `logging` errors), so events at a
     higher level would duplicate them. Users turn events on with standard
     logging configuration (`logging.getLogger("agents.events").setLevel(logging.DEBUG)`).
   - **Where it shows up:** Python logging output on Workers goes to the
     console, so to Workers Logs, `wrangler tail`, and a Tail Worker's
     `event.logs`. **Not** to `event.diagnosticsChannelEvents`: Tail Workers
     or dashboards built on upstream's diagnostics channels won't see Python
     agents' events.
   - **Rejected for the default:** a no-op (events would be invisible without
     extra code), and JS `node:diagnostics_channel` through FFI (needs
     `import_from_javascript` in a request context and the `nodejs_compat`
     flag, adds an FFI conversion per event, and is unverified). A
     diagnostics-channel `Observability` could still be offered later as an
     opt-in implementation.

3. ~~Payload key casing~~: **decided: events are identical to upstream.** The
   same event `type` names and the same payload keys (camelCase, e.g.
   `{"fiberId": …, "fiberName": …}`), so TypeScript and Python agents emit one
   schema and Tail Workers / dashboards don't care which language emitted an
   event.
4. ~~A Python `subscribe`~~: **decided: yes.** `subscribe(channel, callback) ->
   Disposable`, a small Python-side registry that the default `emit` also
   notifies, for tests and local debugging. Implemented with one `Emitter`
   per channel, dispatched with sync `fire`
   ([core_disposable_store.md](./core_disposable_store.md) §5.4). Independent of the JS channel.
   Channel names match upstream (`"rpc"`, `"schedule"`, `"fiber"`, …).
5. ~~Tracing~~: **decided: out of scope** for phase 1 (§1).

---

## 6. Production findings ([platform_verification.md](./platform_verification.md) §7.4)

- Python `logging` lines reach Workers Logs as **strings at level `error`**,
  whatever their Python level (stderr). `DEBUG` lines are dropped unless the
  app configures logging, so the default sink (`DEBUG`) is invisible by
  default, as intended.
- `js.console.log(<JS object>)` arrives as a **structured object**.
- **JSON strings are indexed** (dashboard check): a JSON log line's keys
  become top-level searchable fields, so the default sink keeps writing JSON
  lines through `logging`.
- **Level problem:** everything Python `logging` writes is recorded at
  `error` (stderr), so enabled `agents.events` debug lines and the SDK's
  warnings would all appear as errors.
- **Decided (verified in production, [platform_verification.md](./platform_verification.md) §7.4.1):**
  a small `logging.Handler` attached only to the SDK's own `agents.*` loggers
  (never the root logger or the app's configuration), installed only when
  running on Workers, that writes each record's message with the matching JS
  console method: `DEBUG` → `console.debug`, `INFO` → `console.info`,
  `WARNING` → `console.warn`, `ERROR`/`CRITICAL` → `console.error`. Levels
  arrive intact, and JSON messages stay indexed. It lives in the FFI module
  ([utilities.md](./utilities.md) §3), since it touches `js.console`.
  ```python
  class ConsoleHandler(logging.Handler):
      def emit(self, record: logging.LogRecord) -> None:
          message = self.format(record)
          if record.levelno >= logging.ERROR:
              js.console.error(message)
          elif record.levelno >= logging.WARNING:
              js.console.warn(message)
          elif record.levelno >= logging.INFO:
              js.console.info(message)
          else:
              js.console.debug(message)
  ```

Original question:

- How Workers Logs indexes Python logging output: whether a JSON-formatted log
  line is parsed into searchable fields, or stored as a plain string.
