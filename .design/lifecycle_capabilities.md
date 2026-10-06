# Lifecycle and Capabilities

How the TypeScript Agents SDK splits one Durable Object (DO) across many
features. This explains the two core abstractions under
`packages/agents/src/lifecycle/` that everything else builds on, and what they
mean for the Python port.

Related: [agents-subpackages.md](./agents-subpackages.md) (a map of each
folder).

---

## 1. The problem

The Cloudflare runtime only ever calls a fixed set of method names on a DO
class. These are the DO's **entry points**:

| Runtime calls… | When |
| --- | --- |
| `fetch(request)` | An HTTP request is routed to the object, including WebSocket upgrades |
| `alarm()` | The object's alarm time is reached |
| `webSocketMessage(ws, msg)` | A message arrives on a hibernated WebSocket |
| `webSocketClose(ws, code, reason, wasClean)` | A hibernated socket closes |
| `webSocketError(ws, error)` | A hibernated socket errors |

There is **one** of each. The alarm is even more limited:
`ctx.storage.setAlarm(t)` keeps a **single** timestamp per object, and every
call overwrites the last.

An `Agent` needs many features to use those entry points at the same time:

- **Scheduler**: wants to wake at 09:00 for a cron job
- **Queue**: wants to wake *now* for a background item
- **Tasks**: wants to wake at 09:05 for a retry deadline
- **WebSockets**: wants every upgrade and every socket message
- **MCP client, State**: want to run setup code when the object starts

If each feature called `setAlarm()` itself, they would overwrite each other's
alarms. If each wanted to *be* `fetch`, only one could.

Originally, `Agent` inherited routing and WebSockets from PartyServer's
`Server` and hard-coded every feature into the class. That made features such
as MCP impossible to reuse in a plain DO; see
`design/rfc-durable-object-lifecycle.md` upstream. **Lifecycle +
capabilities** is the fix for that.

---

## 2. Analogy

A DO is a **tiny office**. It has one front door (`fetch`), one phone line
(WebSockets), and one alarm clock (`alarm`).

The **capabilities** are the specialists who work in the office: Scheduler for
appointments, Queue for chores, State for the notebook, WebSockets for phone
calls, MCP for outside services, and Tasks for long jobs that may be
interrupted.

The **Lifecycle** is the **receptionist**:

- **In the morning (startup):** turns the lights on and gives every specialist
  time to set up.
- **Door knock:** asks each specialist in turn "is this yours?" The first to
  say yes takes it. If nobody does, the boss (your `Agent` code) handles it.
- **Phone call:** goes to whoever owns that line.
- **Alarm clock:** specialists never touch it. They write reminders on the
  receptionist's **one shared to-do list**. The receptionist sets the clock for
  the earliest item, and when it rings, hands each due item to the specialist
  who wrote it.

---

## 3. Lifecycle

`Lifecycle` (`lifecycle/durable-object-lifecycle.ts`) is the DO's front door
and dispatcher. There is **exactly one per DO instance**.

### 3.0 It is SDK code, not a platform feature

The Durable Object platform has nothing called `Lifecycle`. What the platform
provides is:

- the `DurableObject` base class, with `this.ctx` (a `DurableObjectState`) and
  `this.env`;
- on `ctx`: `storage` (KV/SQLite, `setAlarm`/`getAlarm`/`deleteAlarm`), `id`
  (including `id.name`), `acceptWebSocket`/`getWebSockets` for hibernation,
  `blockConcurrencyWhile`, `waitUntil`, and so on;
- the runtime calling the fixed entry-point methods (§1), plus native RPC to
  any method (verified on Python Workers: underscore names too,
  [platform_verification.md](./platform_verification.md) §2.6).

`Lifecycle` is ordinary user-space code: a class the SDK wrote, starting from
code copied from PartyServer and then changed. A DO creates one and keeps it as
a **normal instance field** (`lifecycle = Lifecycle.install(this)`). It is
built on the platform primitives above. It sets itself up as the entry-point
methods, keeps the job table in `ctx.storage`, calls `setAlarm` on behalf of
every capability, and uses `acceptWebSocket` and socket attachments for
hibernated connections.

**Where the name comes from.** Cloudflare's docs talk about the DO
"lifecycle" informally: the object is created on first request, can
hibernate, gets evicted, and is rebuilt by its constructor on the next wake.
The SDK's `Lifecycle` is named after that idea, as the code that handles
everything across it.

**Startup differs from the platform's.** The platform's own "startup" is just
your constructor, optionally with `ctx.blockConcurrencyWhile`. Lifecycle's
startup (every `onStart`, then the host's `onStart`) is a separate SDK step
that runs on the **first event after a wake**, not in the constructor.

The same applies in Python: Python DOs provide `DurableObject`, `self.ctx`, and
the fixed method names. `Lifecycle` is a class `agents-py` writes and assigns
with `self.lifecycle = Lifecycle(self)`.

### 3.1 It replaces all five entry points

```ts
class MyObject extends DurableObject {
  lifecycle = Lifecycle.install(this);   // = new Lifecycle(this) + installHandlers()
}
```

`installHandlers()` (`durable-object-lifecycle.ts:254`) uses
`Object.defineProperty` to set the host's entry points to Lifecycle's own
methods:

```
DO core method          →  becomes
───────────────────────────────────────────
fetch(request)          →  lifecycle.fetch
alarm()                 →  lifecycle.alarm
webSocketMessage(...)   →  lifecycle.webSocketMessage
webSocketClose(...)     →  lifecycle.webSocketClose
webSocketError(...)     →  lifecycle.webSocketError
```

It skips any method the class already defines (`if (name in this.#host)
continue`). `Agent` relies on that to wrap a few entry points itself, for
example for sub-agent routing.

Lifecycle **replaces** these methods; it doesn't run alongside them. Once
installed, Lifecycle's code is the only code the runtime runs, and it decides
what to call next.

**Exception:** native DO RPC (`stub.someMethod()`) goes straight to the method
and skips Lifecycle. A plain Lifecycle host has to call `lifecycle.start()`
itself from any RPC method that needs startup to have run.

### 3.2 What it owns beyond the five methods

- **Startup:** guarantees that every capability's `onStart`, then the host's
  `onStart`, runs **once per wake**, before any other event is handled.
  Concurrent callers share one in-flight startup, and a failed startup can be
  retried.
- **The job table** (`cf_agents_jobs`, `job-queue.ts`) and **the one physical
  alarm** (`job-driver.ts`).
- **The ordered list of capabilities** and the rules that decide which one gets
  each event.

### 3.3 Picture

```
                runtime
                   │  fetch / alarm / webSocket*
                   ▼
            ┌─────────────┐
            │  Lifecycle  │  owns: startup, the job table, the one alarm
            └─────┬───────┘
     dispatches by rules (order, first-Response-wins, socket ownership, job owner)
   ┌──────┬───────┼────────┬─────────┬────────┐
Scheduler Queue  State  WebSockets  Tasks    MCP …   (capabilities)
                  │
                  ▼  if no capability handled it
            host hooks: onStart / onRequest / onAlarm  (Agent's own code)
```

---

## 4. Capabilities

A **capability** is a pluggable feature installed into a Lifecycle with
`.use(...)`. Concretely, it's any object that implements
`DurableObjectCapability` (`lifecycle/capability-runner.ts`). Usually it does
this by extending `LifecycleCapability` (`lifecycle/capability.ts`), which
supplies a `capabilityId` and access to the shared services.

`Agent` is the main **composition root**. Its constructor (`src/index.ts:~2037`)
does:

```ts
this.lifecycle
  .use(this.scheduler)       // schedules/
  .use(this._queue)          // queue/
  .use(this.mcp)             // mcp/client
  .use(this._state)          // state/
  .use(this._webSockets)     // websockets/
  .use(this.tasks)           // tasks/
  .use(this._dynamicAgents); // dynamic-agents/
```

`this.schedule()`, `this.queue()`, `this.setState()` and the like mostly
delegate to these objects. Capabilities do **not** depend on `Agent`. They can
be installed on any plain `DurableObject` that calls `Lifecycle.install(this)`,
and that is how upstream tests them (`src/tests/capabilities/`).

### 4.1 Three channels, nothing else

The source states that a capability talks to Lifecycle only through:

1. **Hooks** that the capability implements (Lifecycle → capability), §4.2
2. **`LifecycleServices`**, which the capability calls through
   `this.lifecycle` (capability → Lifecycle), §4.3
3. **`set*()` setters** that a composition root such as `Agent` calls to inject
   host-specific behavior

Any other direct access in either direction is considered a design smell.

### 4.2 Hooks (all optional)

| Hook | Fires on | Dispatch rule |
| --- | --- | --- |
| `onStart(ctx)` | First event after the DO wakes | **Every** capability, in install order, then the host's `onStart` |
| `onRequest(ctx)` | Non-upgrade HTTP request | In order; the **first to return a `Response`** wins. If none does, the host's `onRequest`, then 404 |
| `onWebSocketUpgrade(ctx)` | WebSocket upgrade | In order; the first to return a `Response` owns the socket **for its whole lifetime** |
| `onWebSocketMessage/Close/Error` | Hibernated socket wakes | In order; the first to return `true` consumes the event |
| `onJob(ctx)` / `onJobError(ctx, err)` | One of **this capability's** jobs is due or has exhausted its retries | Sent only to the job's owner |
| `onMemoryLimit(ctx)` | The alarm's out-of-memory circuit breaker records a strike | Each capability applies its own policy, then the host |
| `onRoute(ctx)` | A message routed from another Lifecycle (e.g. sub-agent → root) | Sent to one capability, by id |
| `dispose()` | Explicit host destruction | Release in-memory resources |

**Ordering rule.** Capabilities are dispatched in install order, except that
one declaring `claims: "catch-all"` always goes **last**, whenever it was
installed. Only one catch-all is allowed per hook. `WebSockets` is the
catch-all for upgrades.

**Context rule.** Hooks run *outside* the host's ambient context. Before a
capability calls user code (e.g. a scheduled callback), it must enter that
context through `runInHostContext`, so that `getCurrentAgent()` and tracing
work there.

### 4.3 Services (`this.lifecycle`)

`LifecycleServices` (`lifecycle/capability.ts`):

| Service | Purpose |
| --- | --- |
| `name`, `className` | The host DO's identity |
| `storage` | DO storage/SQLite. Each capability owns its own tables and migrations |
| `sockets` | `accept(ws, tags)` / `get(tag?)` for hibernated sockets |
| `ready()`, `status()` | Wait for or inspect startup (`"zero" \| "starting" \| "started"`) |
| `jobs` | **This capability's** view of the shared job queue |
| `trackAlarmWork(promise)` | Keep handed-off work inside the alarm's memory-limit breaker |
| `runInHostContext(fn, scope?)` | The only way to call user code, inside host context |
| `events.emit(type, payload)` | Best-effort telemetry |
| `routes.toRoot()` / `routes.to(addr)` | Send messages to other Lifecycles (facets have no alarm of their own) |

### 4.4 Two kinds of capabilities

- **Vocabulary only (no tables):** `Scheduler`, `Queue`. They validate input,
  resolve named callbacks, and push jobs onto Lifecycle's queue. A queued item
  is literally `this.lifecycle.jobs.push({ fn: callbackName, payload })`.
- **Own their data:** `State`, `Tasks`, `Streams`, `Sessions`, `RoutedAgents`,
  `PiHarness`, and others, each with its own tables. Some (`Streams`,
  `Sessions`) need no alarm, so they also work on facets.

### 4.5 What each `Agent` capability implements

| | `onStart` | `onRequest` | `onWebSocketUpgrade` / `onWebSocketMessage` | `onJob` |
| --- | --- | --- | --- | --- |
| Scheduler | ✅ | | | ✅ |
| Queue | ✅ | | | ✅ |
| MCP client | ✅ | ✅ (OAuth callbacks only) | | |
| State | ✅ | | | |
| WebSockets | | | ✅ (catch-all) | |
| Tasks | ✅ | | | ✅ |

---

## 5. Sharing the single alarm: the job queue

This is the most important mechanism.

- Capabilities **never call `setAlarm`**. They push jobs into one shared table,
  `cf_agents_jobs`, through their scoped `this.lifecycle.jobs`:
  ```
  { capability: "scheduler", fn: "remind", due: <epoch ms>, payload: {...} }
  ```
  The `capability` column is filled in automatically by the scoped view. A
  capability cannot see or replace another capability's jobs.
- After **every** change to the queue, Lifecycle re-arms the one physical alarm
  to the **earliest due job across all capabilities**.
- When the alarm fires, the job driver (`job-driver.ts`):
  1. ensures startup has run;
  2. loads the due jobs and sends each one to its owner's `onJob`, with retries
     and backoff, deferral of jobs that aren't due, and the memory-limit
     circuit breaker;
  3. calls the host's `onAlarm`;
  4. re-arms the alarm for the next earliest job, or sets none if the queue is
     empty.

`onJob` must finish within a bounded time: the loop waits for each job, so a
slow job delays every other job on the object. Work with no time bound should
be started detached, with durable evidence recorded, and `onJob` should return.

> ⚠️ Upstream `design/durable-object-lifecycle.md` still describes an **older**
> alarm design, with a `getNextAlarm()`/`onAlarm()` hook on each capability.
> The current code uses the job queue above (see
> `design/lifecycle-work-queue.md`, `lifecycle/job-queue.ts`,
> `lifecycle/job-driver.ts`). **Port from the code, not that doc.**

---

## 6. How a capability knows an event is its own

There is **no separate `canHandle()` method. The hook itself is the check.**
Lifecycle calls the hook, and the return value tells it whether the capability
took the event.

From `capability-runner.ts:276`:

```ts
async request(context) {
  for (const capability of this.#getCapabilities()) {
    const response = await capability.onRequest?.(context);   // just call it
    if (response !== undefined) return response;               // a Response means "mine, done"
  }
  return undefined;                                            // nobody took it, so it goes to the host
}
```

`?.` skips capabilities that don't implement the hook. The WebSocket loops
(`:310`) work the same way, using `=== true` as the claim signal.

Inside each hook, the capability checks its own rules and returns early if the
event isn't its own:

| Event | How the capability recognizes it | Example |
| --- | --- | --- |
| HTTP request | **Inspects the request** (method, URL, params) | MCP's `onRequest` starts with `if (!this.isCallbackRequest(request)) return undefined;`. That check requires a `GET` whose `state` param decodes to a known server id, and whose origin and path match that server's registered OAuth callback URL |
| WS upgrade | **No check needed.** The catch-all takes everything | `WebSockets.onWebSocketUpgrade` only chooses *how* to accept: JSON frames or Cap'n Web |
| WS message/close/error | **A name tag on the socket.** At accept time the capability writes a private key into the socket's hibernation attachment, which survives hibernation | `WebSockets` writes `__pk: { id, tags }`. `isManagedWebSocket(ws)` (`websockets/connection.ts:78`) checks for it, and `onWebSocketMessage` starts with `if (!isManagedWebSocket(ws)) return false;` |
| Alarm job | **No check.** The owner's id is stored on the job row, and Lifecycle calls `findById(owner).onJob(...)` | `{ capability: "scheduler", … }` → `Scheduler.onJob` |
| Routed message | **No check.** Addressed by capability id | `routes.to(addr, payload)` → `onRoute` on the matching id |
| Startup | **No decision.** Everyone gets `onStart` | |

Return-value contract:

| Hook | "Not mine" | "Mine" |
| --- | --- | --- |
| `onRequest`, `onWebSocketUpgrade` | `undefined` | a `Response` |
| `onWebSocketMessage/Close/Error` | `false` / nothing | `true` |
| `onJob`, `onRoute` | (Lifecycle has already picked the owner) | |

---

## 7. Worked example

```ts
export class ReminderAgent extends Agent<Env, { count: number }> {
  initialState = { count: 0 };

  async onStart()      { console.log("woke up"); }
  async onRequest(req) { return new Response("hello"); }

  @callable()
  async remindMe(text: string) {
    await this.schedule(60, "remind", { text });   // in 60 seconds
  }

  async remind({ text }) {
    this.setState({ count: this.state.count + 1 });
  }
}
```

**Event 1: `GET /agents/reminder-agent/alice` while the DO is asleep.**
The runtime calls `agent.fetch` (i.e. `lifecycle.fetch`).
1. *Startup:* `onStart` runs for Scheduler (schema migration), Queue, MCP
   (restores servers), State (loads or seeds `{count: 0}`), and Tasks. Then
   `ReminderAgent.onStart` logs "woke up".
2. *`onRequest` chain:* only MCP implements it. The request isn't an OAuth
   callback, so MCP returns `undefined`. Nobody claimed it, so it goes to
   `ReminderAgent.onRequest`, which returns `"hello"`.

**Event 2: the browser connects with `useAgent(...)`.**
`fetch` is called with `Upgrade: websocket`.
1. *Startup:* already done, so skipped.
2. *Upgrade chain:* only WebSockets implements it, so it claims the socket. It
   accepts the socket into hibernation with its `__pk` tag and sends the
   current state.

**Event 3: the client calls `agent.stub.remindMe("stretch")`.**
The runtime calls `agent.webSocketMessage`.
1. *Message chain:* WebSockets sees `__pk` and returns `true`; no other
   capability sees the message. It parses the `rpc` frame, checks that
   `remindMe` is marked `@callable()`, and invokes it.
2. `this.schedule(...)` → Scheduler pushes
   `{ capability: "scheduler", fn: "remind", due: now+60s, payload: {text} }`.
3. Lifecycle sees the queue change and **re-arms the alarm** to `now+60s`.

The DO may now hibernate or be evicted. The job and the alarm are both
durable.

**Event 4: 60 seconds later.**
The runtime calls `agent.alarm` (i.e. `lifecycle.alarm`).
1. *Startup:* if the DO was evicted, every `onStart` runs again.
2. *Job driver:* one due job, owner `"scheduler"`, so **only**
   `Scheduler.onJob` is called.
3. Scheduler resolves `"remind"` and calls `this.remind({text})` through
   `runInHostContext`.
4. `setState` → the State capability saves `{count: 1}`, and Agent broadcasts it
   over WebSockets to the browser.
5. The one-shot job is deleted, then the host's `onAlarm` runs if defined. The
   queue is empty, so no alarm is set.

**The pattern across the four events:** only startup reaches every capability,
once per wake. HTTP stops at the first capability that answers. WebSocket
events go to the owner of the socket. Alarm jobs go only to the owner of the
job. Many capabilities can share one alarm, and each is called only for its
own jobs.

---

## 8. Terminology: what "capability" does *not* mean here

- The name and this design are **specific to the Agents SDK**. Cloudflare's
  platform has no such concept, and neither does PartyServer. The underlying
  pattern is ordinary plugin/middleware composition: ASGI/Express middleware
  (`onRequest`, first response wins), ASGI lifespan or Django `ready()`
  (`onStart`), and several jobs sharing one scheduler (the job queue). What is
  specific to DOs is the *reason* for it: single entry points and a single
  alarm.
- **MCP "capabilities"** (`mcp/`) are feature negotiation in the MCP protocol,
  for example "this server supports tools." They are unrelated. The MCP client
  manager just happens to be *installed as* a Lifecycle capability.
- **Object-capability security** (capabilities as unforgeable access tokens,
  as in Cap'n Proto) is also unrelated. Keep the two apart, because
  `websockets/` uses **Cap'n Web** as an RPC transport.

---

## 9. Implications for the Python port

- **Port order:** `Lifecycle` first (entry-point dispatch, startup, the job
  queue and alarm driver), then the capabilities, then `Agent` as composition
  on top. Python DOs have the same fixed `fetch`/`alarm`/WebSocket methods, so
  the motivation carries over directly.
- **Delegate explicitly** instead of patching methods at runtime. Python
  methods are plain class methods, so this is clearer than copying
  `Object.defineProperty`. (Sketch only: the decided entry-point mechanism,
  name-mangled implementations attached under the camelCase names, is in
  [agent_api.md](./agent_api.md) §1.5; capability constructors are in their
  own docs.)
  ```python
  class Lifecycle:
      def __init__(self, host):
          self.host, self.capabilities = host, []
          self.jobs = JobQueue(host.ctx.storage)      # one shared table + one alarm

      def use(self, cap):
          cap._bind(LifecycleServices(self, cap.capability_id))
          self.capabilities.append(cap)               # (+ catch-all goes last)
          return self

      async def fetch(self, request):
          await self.ensure_started()                 # every on_start, then host.on_start
          for cap in self.capabilities:
              resp = await cap.on_request(request)
              if resp is not None:
                  return resp                         # first Response wins
          return await self.host.on_request(request)

      async def alarm(self):
          await self.ensure_started()
          for job in self.jobs.due():                 # route by job owner
              await self.capability(job.owner).on_job(job)
          await self.host.on_alarm()
          await self.jobs.rearm()                     # setAlarm(earliest job)

  class Agent(DurableObject):
      def __init__(self, ctx, env):
          super().__init__(ctx, env)
          self.lifecycle = Lifecycle(self).use(Scheduler()).use(Queue()).use(State()) ...
      async def fetch(self, req): return await self.lifecycle.fetch(req)
      async def alarm(self):      return await self.lifecycle.alarm()
  ```
- **Dispatch is just loops.** A hook returns `None`/`False` to pass and a
  value to claim, so each dispatcher is a `for` loop with an early return. No
  separate `can_handle()` is needed. Each loop visits only capabilities that
  override the hook (`_implements`, §9.1), computed once per capability class.
- **Capability API shape:** a `LifecycleCapability` base class, with each
  feature as a subclass (`class Tasks(LifecycleCapability)`). See §9.1.
- **Translate these with the most care:**
  - ordering rules (install order, catch-all last, one catch-all per hook);
  - single-flight startup, with nested calls from inside startup returning
    immediately;
  - the job driver: retries and backoff, deferral, the memory-limit circuit
    breaker, and re-arming after every queue change;
  - the socket-attachment ownership tag (`__pk`). It's server-side only (not
    on the wire), so it needn't match upstream byte for byte (migrating DOs
    between the SDKs isn't supported, [scope.md](./scope.md) §4.5); it must
    fit the 16 KiB attachment limit alongside connection state
    ([platform_verification.md](./platform_verification.md) §2.1).
- **Host context:** `getCurrentAgent()` relies on AsyncLocalStorage. In Python
  the natural equivalent is `contextvars.ContextVar`, set inside
  `run_in_host_context`.
- **Name:** keeping "capability" preserves parity with the TypeScript SDK.
  "Plugin" or "extension" would read more naturally in Python.

### 9.1 Python `LifecycleCapability` API

Each feature is a subclass of `LifecycleCapability` that passes its id to the
base class, as the TypeScript does (`super("tasks")` in `tasks/tasks.ts:251`).
Get these three details right:

**1. TypeScript has two layers.** `DurableObjectCapability` is an *interface*
(the optional hooks), and `Lifecycle.use()` accepts anything that implements
it. `LifecycleCapability` is a *base class* that adds `capabilityId`, `claims`,
a no-op `onStart`, and the `this.lifecycle` services. `use()` only binds
services to instances of the base class. Every built-in capability extends the
base, so Python can merge the two layers, or keep them as a `typing.Protocol`
plus a base class.

**Decided: merge into one base class, `LifecycleCapability`.** In upstream,
every production capability extends `LifecycleCapability` (Streams,
WebSockets, Scheduler, Sessions, Queue, Tasks, State, routed agents, dynamic
agents, MCP client, browser, the harnesses, channels); the only bare
`DurableObjectCapability` is a test probe (`tests/capabilities/lifecycle.ts`).
A bare implementation also gets no capability id, so it can't own jobs or
receive routed messages. `Lifecycle.use` (and `Agent.use`) would accept
`LifecycleCapability` only.

A `typing.Protocol` is the closest Python equivalent of a TypeScript
interface (both are structural), but it **can't express optional members**:
every method declared on a Protocol is required. `DurableObjectCapability`'s
hooks are all optional,
so a Protocol listing them would reject a capability that implements only
`on_start` and `on_job`. With one base class, optional hooks are methods a
subclass may override, and Lifecycle detects which ones it overrode
(`_implements`, below).

**Decided with the merge: typed no-op hooks on the base, plus override
detection.** The base class declares every hook with its typed
signature as a no-op, and Lifecycle treats a hook as implemented only if the
subclass overrides it:

```python
def _implements(cap: LifecycleCapability, hook: str) -> bool:
    return getattr(type(cap), hook) is not getattr(LifecycleCapability, hook)
```

Computed once per capability class. This keeps the presence semantics point 2
below relies on (dispatch skips non-implementers; the catch-all check), while
giving every hook a typed signature.

**Assume users don't write `@typing.override`** (it's uncommon in practice).
What the typed defaults still give without it:
- **signature checking:** pyright / mypy report an incompatible override (e.g.
  `on_request(self, request: int)`) whether or not `@override` is present;
- **discoverability:** hooks show up in autocomplete and docs on the base class.

What it doesn't catch without `@override`: a **misspelled hook name**
(`on_requests`) is just a new method and is silently never called. That's no
worse than `getattr` discovery. The SDK's own capabilities may use `@override`
internally; users aren't required to. This replaces point 2's "leave the hooks
undefined" mechanism (the reasoning about presence still holds).

**Merge: pros**
1. One concept, matching reality upstream (only a test probe uses the bare
   interface).
2. Every capability has an id, so it can own jobs, receive routed messages,
   emit events under its own name, and be deduplicated by `use()`; upstream's
   "installed but no id" case (`lifecycleCapabilityId()` → `undefined`)
   disappears.
3. Services are always attached; no capability without `self.lifecycle`.
4. Typed, checkable hook signatures (with the refinement above).
5. No `Protocol` limitation on optional members.
6. Simple typing: `use[C: LifecycleCapability](self, capability: C) -> C`.

**Merge: cons**
1. Inheritance is required: nothing can be plugged in by shape alone (a thin
   adapter subclass covers third-party objects).
2. Diverges from upstream's structure: upstream branches for bare
   implementations (no id, no services) have no Python equivalent.
3. Every capability needs an id, and ids are unique per Lifecycle, so two
   anonymous capabilities can't be installed (upstream allows that for bare
   interface implementations).
4. The override check needs documenting: a subclass that deliberately
   reassigns a hook to the base's own function reads as "not implemented"
   (unlikely).

**2. Hook presence is information (superseded mechanism; reasoning kept).** The TypeScript base
defines **only** `onStart`. Every other hook is left out on purpose, because
Lifecycle uses whether a hook exists as information:
- dispatch skips capabilities that lack the hook (`capability.onRequest?.(…)`);
- the catch-all check in `use()` asks whether a capability implements
  `onRequest`/`onWebSocketUpgrade` (`if (!capability[hook]) continue`) to refuse
  a second catch-all for the same hook.

If the base defined `on_request` returning `None` and Lifecycle checked mere
presence, every capability would appear to implement every hook, and the
catch-all check would break. Upstream leaves the hooks undefined. **Python
instead declares typed no-op hooks and checks for overrides** (`_implements`,
above), which preserves these semantics.

**3. Services are bound at `use()` time, not at construction.** TypeScript
attaches them through a `WeakMap` when the capability is installed.
`this.lifecycle` throws if the capability was never installed, and
`lifecycleServices` returns `undefined` for isolated unit tests. In Python, a
private attribute set by `use()` does the same job.

```python
from typing import ClassVar, Literal


class LifecycleCapability:
    """Base for every capability (decided: no separate DurableObjectCapability).

    Every hook has a typed no-op default; a capability overrides the ones it
    needs. Lifecycle treats a hook as implemented only if it is overridden
    (_implements), so dispatch and the catch-all check still see presence.
    """

    claims: ClassVar[Literal["selective", "catch-all"]] = "selective"

    def __init__(self, capability_id: str) -> None:
        if not capability_id.strip():
            raise ValueError("Lifecycle capability IDs must be non-empty")
        self.capability_id = capability_id
        self._services: "LifecycleServices | None" = None  # bound by Lifecycle.use()

    @property
    def lifecycle(self) -> "LifecycleServices":
        if self._services is None:
            raise RuntimeError(
                f"{type(self).__name__} must be installed with Lifecycle.use() before use"
            )
        return self._services

    async def on_start(self, ctx: "CapabilityStartContext") -> None: ...
    async def on_request(
        self, ctx: "CapabilityRequestContext"
    ) -> "Response | None": ...
    async def on_websocket_upgrade(
        self, ctx: "CapabilityWebSocketUpgradeContext"
    ) -> "Response | None": ...
    async def on_websocket_message(
        self, ws: "WebSocket", message: str | bytes
    ) -> bool: ...
    async def on_websocket_close(
        self, ws: "WebSocket", code: int, reason: str, was_clean: bool
    ) -> bool: ...
    async def on_websocket_error(
        self, ws: "WebSocket", error: BaseException
    ) -> bool: ...
    async def on_job(
        self, ctx: "LifecycleJobContext"
    ) -> "LifecycleJobOutcome | None": ...
    async def on_job_error(
        self, ctx: "LifecycleJobContext", error: BaseException
    ) -> "LifecycleJobOutcome | None": ...
    async def on_memory_limit(self, ctx: "MemoryLimitContext") -> None: ...
    async def on_route(self, ctx: "LifecycleRouteContext") -> object: ...
    async def dispose(self) -> None: ...


def _implements(cap: LifecycleCapability, hook: str) -> bool:
    return getattr(type(cap), hook) is not getattr(LifecycleCapability, hook)


class Tasks(LifecycleCapability):
    def __init__(
        self,
        *,
        definitions=None,
        target=None,
        retries=None,
        step_timeout=None,
        on_error=None,
    ):
        super().__init__("tasks")
        ...

    async def on_start(
        self, ctx
    ): ...  # migrate cf_agents_task_runs / cf_agents_task_steps tables

    async def on_job(
        self, ctx: "LifecycleJobContext"
    ): ...  # drive the due run's deadline

    # public API used by Agent / apps
    async def run(self, name, input, **opts): ...
```

The Lifecycle side of installation, which mirrors
`durable-object-lifecycle.ts:291`:

```python
CATCH_ALL_HOOKS = ("on_request", "on_websocket_upgrade")


def use(self, cap: LifecycleCapability) -> "Lifecycle":
    if self._locked:
        raise RuntimeError("Lifecycle capabilities must be added before startup")
    if any(c.capability_id == cap.capability_id for c in self._caps):
        raise RuntimeError(
            f"Lifecycle capability {cap.capability_id!r} is already installed"
        )
    if cap.claims == "catch-all":
        for hook in CATCH_ALL_HOOKS:
            if _implements(cap, hook) and any(
                c.claims == "catch-all" and _implements(c, hook) for c in self._caps
            ):
                raise RuntimeError(f"Lifecycle already has a catch-all for {hook}")
        self._caps.append(cap)  # catch-alls stay last
    else:
        idx = next(
            (i for i, c in enumerate(self._caps) if c.claims == "catch-all"),
            len(self._caps),
        )
        self._caps.insert(idx, cap)
    cap._services = LifecycleServices(
        self, cap.capability_id
    )  # jobs view scoped to this id
    return self
```

**Setters.** The host-specific setters (§4.1, channel 3) are **module-level
functions** in the TypeScript, not methods: for example
`setTaskDefinitionResolver(tasks, fn)` and
`setTaskRoutedMemoryLimitHandler(...)` in `tasks/tasks.ts:79,98`. That keeps
them off the public class surface. In Python, use plain module functions or
`_set_*` methods that only `Agent` calls.


---

## 10. Implementation (2026-10-05)

`src/agents/lifecycle/`: `types.py`, `host_context.py`, `capability.py`,
`capability_runner.py`, `services.py`, `job_queue.py`, `job_driver.py`,
`lifecycle.py`; platform-failure classifiers in `core/platform_errors.py`.
Checked by CPython tests (a fake runtime whose SQL runs on in-memory
`sqlite3`) and on `workerd` (verify Worker, `LifecycleHost`: startup once,
dispatch, a real alarm firing a job).

Decisions made while porting (differences from upstream):

1. **Startup runs inside `blockConcurrencyWhile`, catching everything inside
   the callback and re-raising after it**, as upstream; verified necessary
   ([platform_verification.md](./platform_verification.md) §2.9).
   Single-flight via one `asyncio.Task` (callers `shield` it); a call from
   inside startup returns at once (a `ContextVar` of the Lifecycles whose
   startup is running).
2. **Hook signatures drop single-field context objects:** `on_start(self)`
   (no props), `on_request(self, request)`, `on_websocket_upgrade(self,
   request)`. `JobContext`, `MemoryLimitContext`, and `RouteContext` stay
   dataclasses.
3. **Times are `datetime`:** `LifecycleJob.time` / `created_at`,
   `jobs.push(time=...)`, `jobs.reschedule(id, time)`, `Reschedule(at=...)`,
   `MemoryLimitContext.next_time`; `hung_timeout` is a `Duration`. Storage and
   `retry_options` JSON keep upstream's epoch ms and field names.
4. **`LifecycleServices` is flatter:** `name`, `class_name`, `storage`, `sql`
   (new: the typed SQL helper), `accept_websocket(ws, tags)` /
   `websockets(tag)` (upstream `sockets.accept` / `sockets.get`), `ready()`,
   `status()`, `jobs`, `track_alarm_work`, `run_in_host_context(fn, *,
   connection, request)`, `emit(type, payload)` (upstream `events.emit`), and
   `routes` (`source`, `to_root`, `to`).
5. **No handler patching:** hosts delegate entry points explicitly (`fetch`,
   `alarm`, `websocket_message` / `close` / `error`); `Agent` attaches them
   under the runtime's names ([agent_api.md](./agent_api.md) §1.5).
6. **Host hooks are optional, found by name:** `on_start`, `on_request`,
   `on_alarm`, `on_job`, `_on_alarm_memory_limit`.
7. **A failed request returns `500 "Internal Server Error"`** and logs the
   exception; upstream sends the error's stack trace to the client, which can
   reveal code, paths, and data. **Opt-in (decided):** `Lifecycle(...,
   expose_error_details=True)` (from `AgentOptions.expose_error_details`)
   sends the traceback, for local development. A failed upgrade still answers
   with a socket carrying an error frame (as upstream), generic unless
   opted in.
8. **No legacy name migration** (`__ps_name`) and no props header: neither
   exists for Python objects.
9. **Composition-root setters are `_set_*` methods** on `Lifecycle`
   (`_set_event_sink`, `_set_host_invoker`, `_set_route_transport`,
   `_set_name`) instead of module-level `WeakMap` setters.
10. **Default event sink:** without a sink, events are logged as JSON to
    `agents.events` at `DEBUG` ([observability.md](./observability.md)); `Agent`
    installs its own sink.
11. **`websocket` is one word in Python names** (decided):
    `on_websocket_upgrade` / `on_websocket_message` / `on_websocket_close` /
    `on_websocket_error`, `Lifecycle.websocket_message` (etc.),
    `accept_websocket`, `websockets(tag)`. The runtime's own JS names
    (`webSocketMessage`, …) and the Workers SDK's `Response(web_socket=...)`
    keyword are unchanged.
