# Core `Agent` API

The user-facing API of the `Agent` class and its building blocks. Behavior is
documented in [agents_wire_protocol.md](./agents_wire_protocol.md) (connect
sequence, state, RPC) and [lifecycle_capabilities.md](./lifecycle_capabilities.md);
this doc records the Python API decisions. Runtime facts the design relies on
are in [platform_verification.md](./platform_verification.md).

Related: [scope.md](./scope.md) §2.3, [utilities.md](./utilities.md).

---

## 1. Decided

### 1.1 `@callable` keeps its name

The decorator that makes a method callable over RPC is named **`callable`**, as
upstream's `@callable()`:

```python
from agents import Agent, callable


class MyAgent(Agent):
    @callable  # or @callable(description=...)
    async def get_weather(self, city: str) -> dict: ...

    @callable(streaming=True)
    async def follow(self, response: StreamingResponse, topic: str) -> None: ...
```

- **It shadows Python's builtin `callable()`** in any module that imports it.
  Users who need the builtin can rename on import:
  `from agents import callable as agent_callable`, or use `builtins.callable`.
- **Inside the SDK,** the module that defines `callable` (and any module that
  imports it unaliased) must use `builtins.callable` for the builtin check.
  Elsewhere (e.g. `get_bound_method` in [utilities.md](./utilities.md) §2) the
  builtin is unaffected.

### 1.2 Class shape (decided)

```python
State = TypeVar(
    "State", bound=Mapping[str, Any], default=dict[str, Any]
)  # typing_extensions


class Agent(DurableObject, Generic[State]): ...


class Counter(TypedDict):
    count: int


class MyAgent(Agent[Counter]):
    initial_state = Counter(count=0)  # see §1.4
```

**Written for Python 3.12** ([code_semantics.md](./code_semantics.md) §1):
the PEP 696 form `class Agent[State: Mapping[str, Any] = dict[str, Any]]` is
3.13-only syntax, so the default comes from `typing_extensions.TypeVar(...,
default=...)` with `Generic[State]`. Same meaning; checked with pyright, ty,
and mypy on 3.12.

- **One type parameter, `State`, bounded by `Mapping[str, Any]`.** A `dict`
  bound was considered and rejected: a `TypedDict` is not assignable to
  `dict` for type checkers (PEP 589 makes it compatible with
  `Mapping[str, object]`, not `dict`). Checked with pyright:
  `Agent[Counter]` errors under a `dict` bound and passes under
  `Mapping[str, Any]`.
- **No `Env` type parameter for now** (an `Env: EnvStub` parameter was
  considered and removed). Upstream's `Props` parameter is not included either.
- **`self.env` and `self.ctx` are the objects the Workers runtime SDK
  provides**, with no further wrapping by the Agents SDK. Concretely
  (`workers/entrypoints.py`, `workers/rpc.py` in the SDK): `self.ctx` is the
  SDK's `DurableObjectContext` and `self.env` its `_EnvWrapper` (see §1.5).
- **`State` defaults to `dict[str, Any]`** (PEP 696 semantics, via
  `typing_extensions` on 3.12), so `class MyAgent(Agent)` works without a
  type parameter.
- **Extra capabilities are created in `__init__`**, not as class attributes (§1.3,
  §2).

### 1.3 Why capabilities aren't class attributes, even with one instance at a time

Recorded because it came up again. A class-level `streams = Streams()` is
one object shared by every instance of the class *in the isolate*, for the
isolate's whole lifetime. That's wrong even if only one instance exists at a
time:

1. **More than one instance can exist in an isolate.** The Workers runtime can
   colocate several Durable Objects of the same class in one isolate, and
   module/class-level state is shared between them. Local tests and
   `wrangler dev` also routinely construct several instances. The design
   doesn't depend on how often that happens.
2. **One-at-a-time still means "one after another".** A DO can be evicted and
   constructed again in the **same** isolate. The class attribute survives,
   still bound to the **previous** instance: its `_services` point at the old
   Lifecycle, and its in-memory state (Streams' reader wakeups, Sessions'
   cached leaf and next `seq`, Scheduler's resolved callbacks with
   `target=self`) belongs to an object that no longer exists. The new
   instance's `use()` would then rebind it, or refuse it as already installed.
3. **Capabilities are per-object by design.** Each one is bound to exactly one
   Lifecycle (one DO) and often to its host (`target=self`). TypeScript's
   `readonly streams = new Streams()` is a per-instance field; Python's class
   attribute isn't.

### 1.4 `initial_state` is a class attribute (decided)

```python
class MyAgent(Agent[Counter]):
    initial_state = Counter(count=0, items=[])
```

- **A class attribute is fine here, unlike capabilities.** `initial_state` is a
  plain value, only read to seed the state the first time nothing is stored;
  it isn't bound to a Lifecycle or an instance. Same as upstream's
  `initialState = {…}`.
- **The SDK must never hand out the class-level object.** A mutable dict on the
  class is shared by every instance, so if `state` returned it, mutating
  `self.state` in place would change every other agent's starting state. The
  SDK seeds a **fresh copy**: a JSON round trip, which is natural because state
  is stored as JSON anyway. Checked in the scratchpad (`initial_state.py`):
  mutating one agent's state leaves other agents and the class default
  untouched.
- **Declared on `Agent` as `initial_state: State | None = None`.** (`ClassVar`
  can't be used: PEP 526 doesn't allow type variables inside `ClassVar`.)
- **Typing caveat with `TypedDict` states** (re-checked on 3.12 with
  pyright, ty, and mypy; corrected 2026-10-05):

  | Subclass declares | pyright | ty | mypy |
  | --- | --- | --- | --- |
  | `initial_state = {"count": 0, "items": []}` | error (infers a plain `dict`) | ok | ok |
  | `initial_state: Counter = {...}` | **error** (narrowing the inherited `State \| None` attribute) | ok | ok |
  | `initial_state: Counter \| None = {...}` | ok | ok | ok |
  | `initial_state = Counter(count=0, items=[])` | ok | ok | ok |
  | untyped agent: `initial_state = {"count": 0}` | ok | ok | ok |

  **Docs show the `TypedDict` constructor form** (`Counter(count=0, …)`):
  it passes every checker and doesn't repeat the type. (Earlier text
  recommended `initial_state: Counter = {...}`, which pyright rejects.)
- **Computed initial state** (depending on `self`): overriding `initial_state`
  with a `@property` works at runtime, but pyright reports overriding a
  variable with a property. Assigning `self.initial_state = {...}` in
  `__init__` works without that error, since state is seeded lazily after
  construction.

### 1.5 Remaining class-shape details (decided)

**What the Workers Python SDK gives us** (read from `workers_runtime_sdk`
1.9.0, `workers/entrypoints.py` and `workers/rpc.py`):
- `class DurableObject` with `__init__(self, ctx, env)` storing `self.ctx` /
  `self.env`, and an `__init_subclass__` that **wraps every subclass's
  `__init__`**, so the constructor receives:
  - `ctx` as `DurableObjectContext`: attribute access passes through to the JS
    `DurableObjectState`; `ctx.storage` comes back wrapped in `_BindingWrapper`,
    which converts every call's arguments with `python_to_rpc` and results
    with `python_from_rpc` (and turns JS promises into awaitables);
    `ctx.abort()` is made safe for asyncio;
  - `env` as `_EnvWrapper`, which wraps bindings (`Fetcher`, DO namespaces, KV,
    R2, D1, AI, …).
  The wrapping is idempotent, so wrapping both `Agent.__init__` and a user's
  `__init__` is harmless.
- **The runtime calls the JS method names on a Python DO**: `fetch`, `alarm`,
  `webSocketMessage`, `webSocketClose`, `webSocketError` (camelCase; the
  earlier port `agents-python-cloudflare` defines exactly these).

**How the runtime finds a Python DO's handlers** (workerd
`src/pyodide/python-entrypoint-helper.ts`, `src/pyodide/internal/introspection.py`):
- At load, `collect_methods(cls)` takes `dir(cls)` **on the class** and keeps
  names that are **public** (no leading `_`) and whose class-level value is a
  plain **`FunctionType`**. These become dummy methods on the JS class's
  prototype "so that the validator can detect them".
- At call time a JS `Proxy` looks up `pyInstance[prop]` and calls it (`fetch`
  through `relaxed_call`; other public methods through `wrapper_func`, which
  converts arguments and results with `python_from_rpc` / `python_to_rpc`).
- **Dispatch reaches *any* attribute name** (verified, [platform_verification.md](./platform_verification.md) §2.6),
  including `_private` ones, names attached with `setattr` after the class
  body, and attributes set on the instance: `collect_methods` only shapes the
  prototype declaration. So **every method on a DO class is callable over
  native DO RPC**, as in TypeScript.

**Decided:**

```python
from workers import DurableObject


class Agent(DurableObject, Generic[State]):
    def __init__(self, ctx, env) -> None:
        super().__init__(ctx, env)  # SDK sets self.ctx / self.env
        self._lifecycle = Lifecycle(self)
        ...  # install the built-in capabilities

    # Implementations, name-mangled so they don't show up in autocomplete.
    async def __fetch(self, request):
        return await self._lifecycle.fetch(request)

    async def __alarm(self, *args):
        return await self._lifecycle.alarm()

    async def __websocket_message(self, ws, message): ...
    async def __websocket_close(self, ws, code, reason, was_clean): ...
    async def __websocket_error(self, ws, error): ...

    @property
    def name(self) -> str: ...
    @property
    def lifecycle(self) -> Lifecycle: ...


# The runtime's camelCase names, attached to the CLASS after its body, so
# static analysis (IDE autocomplete, type checkers) doesn't see them while
# workerd's dir(cls) introspection does.
for _js_name, _impl in {
    "fetch": Agent._Agent__fetch,
    "alarm": Agent._Agent__alarm,
    "webSocketMessage": Agent._Agent__websocket_message,
    "webSocketClose": Agent._Agent__websocket_close,
    "webSocketError": Agent._Agent__websocket_error,
}.items():
    setattr(Agent, _js_name, _impl)
```

1. **Base class:** `Agent` subclasses the SDK's `DurableObject`, so the SDK's
   constructor wrapping, `ctx`, and `env` apply unchanged. If `Agent` ever
   defines `__init_subclass__`, it must call `super().__init_subclass__()` to
   keep the SDK's wrapping.
2. **Runtime entry points hidden from autocomplete:**
   implementations are name-mangled (`__fetch`, …) and the camelCase names are
   attached with `setattr`. Either form dispatches correctly (verified):
   instance-level (`setattr(self, "webSocketMessage", self.__websocket_message)`
   in `__init__`) or class-level (once, after the class body). **Class-level is
   preferred** for being done once rather than per instance, and for also
   appearing in workerd's declared method list, but it isn't required for
   correctness. Subclasses inherit either way. Users override the
   snake_case hooks (`on_request`, `on_alarm`, `on_connect`, `on_message`, …).
   - **No guard (decided):** a subclass that defines one of these runtime
     names (e.g. a helper called `fetch`) replaces the agent's HTTP routing.
     That's documented rather than checked at class creation.
3. **`self.name: str`**, read-only: the logical instance name. Top-level:
   `ctx.id.name` (clear error if the DO wasn't addressed by name:
   `idFromName()` / `getByName()` are required). Facets: the logical sub-agent
   name, not the internal routed name (upstream `index.ts:5396`).
4. **`self.lifecycle`**, read-only property, public for advanced use. Durable
   Objects themselves have no lifecycle attribute: the platform gives `ctx`,
   `env`, and handler methods; "Lifecycle" is purely an Agents SDK concept.
5. **`self.parent_path` / `self.self_path`:** tuples of `AgentPathStep`, root
   first, a **`NamedTuple`**:
   ```python
   class AgentPathStep(NamedTuple):
       class_name: str
       name: str
   ```
   An immutable pair where tuple behavior is natural (`cls, name = step`,
   hashable as a dict key, paths are tuples of steps), so it's a deliberate
   exception to the dataclass convention ([utilities.md](./utilities.md) §5).

**Native RPC exposure:** every method, public or underscore-prefixed, is an
RPC method (verified), so a user's helper methods are callable by any Worker
holding the DO binding. That's the platform's model (bindings are trusted) and matches
TypeScript DOs, but worth stating in the docs.

**Consequence for the FFI design (decided):** the SDK's `ctx.storage` converts
every call (`python_to_rpc` / `python_from_rpc`), which costs roughly 2–6× the
raw JS storage per SQL call ([platform_verification.md](./platform_verification.md) §3.5). **Hot paths do what's
efficient:** SDK internals use the raw JS storage with their own conversions
([utilities.md](./utilities.md) §3) wherever per-call cost matters.

### 1.6 State (decided: `set_state` only)

**Upstream** (`index.ts:1401`, `:2623`; `state/index.ts`):
- `state` is a getter returning the cached in-memory value (seeding
  `initialState` on first access when nothing is stored).
- `setState(next)` replaces the whole value: it raises "Connection is readonly"
  if called while handling a readonly connection; then the State capability
  runs `validateStateChange(next, source)` (sync; raising rejects), **persists
  first, caches second** (a value that fails to serialize or write is never
  served), broadcasts `cf_agent_state` to protocol-enabled connections except
  the source, and calls `onStateChanged(state, source)` (may be async; errors
  logged, not raised).
- State is treated as an immutable snapshot: there's no in-place mutation API.

**Decided: `set_state` is the only way to change state.**

```python
@callable()
async def increment(self) -> None:
    self.set_state({**self.state, "count": self.state["count"] + 1})
```

- **`set_state(new_state)`** follows upstream's pipeline: raise "Connection is
  readonly" in a readonly connection's context → `validate_state_change` →
  persist → cache → broadcast `cf_agent_state` (except to the source) →
  `on_state_changed`.
- **`self.state` is a read-only property** returning the current value. There
  is no setter.
- **Rejected: in-place mutation plus `sync_state()`.** It needed rollback
  after a failed validation, no-op detection, and documentation for forgotten
  syncs, interleaving at `await`s, and stale references. Not worth it for now.

**Decided: aliasing.** `self.state` returns the cached value (as upstream)
and the docs say to treat it as read-only. **`set_state` caches a fresh copy**
(parsed from the JSON it just wrote), so the dict a caller passed in can't
change the cached state afterwards. Nearly free, since the JSON already
exists. (A copy on every read was rejected as too costly.)

**Decided: the hooks.**

```python
type StateSource = Connection | Literal["server"]


class Agent(DurableObject, Generic[State]):
    def set_state(self, state: State) -> None: ...

    def validate_state_change(self, next_state: State, source: StateSource) -> None:
        """Synchronous. Raise to reject the change. Default: accept."""

    async def on_state_changed(self, state: State, source: StateSource) -> None:
        """Runs after the change is saved and broadcast. Default: nothing."""
```

- **`set_state` is synchronous**, like upstream: saving is a synchronous SQLite
  write, and broadcasting is a synchronous send.
- **`validate_state_change` is synchronous** because it gates a synchronous
  write (upstream: "sync only"). For a server-side `set_state`, its exception
  propagates to the caller. For a client's `cf_agent_state` frame, the full
  error is logged and the client gets `cf_agent_state_error` "State update
  rejected" (wire §5.2).
- **`on_state_changed` is `async def`** (the SDK-wide async-callback rule). It
  doesn't gate the change: `set_state` schedules it as a task and returns. A
  failure is logged and passed to `on_error(None, error)` (anything that
  raises is swallowed), not raised, since the state is already saved and
  broadcast (upstream `index.ts:2609` does the same; §1.10). The SDK keeps a strong reference to the task
  until it finishes, so it isn't garbage-collected mid-run.
- **`source`** is the `Connection` a client change came from, or `"server"` for
  `set_state` (upstream's value, kept for parity).
- **Seeding `initial_state`** on first access goes through the same pipeline
  (validate, save, broadcast, hook), as upstream.
- **Readonly:** `set_state` in a readonly connection's context raises
  **`ReadonlyConnectionError`**, a dedicated exception (upstream throws a plain
  `Error("Connection is readonly")`). See the exceptions rule in
  [utilities.md](./utilities.md) §1.

### 1.7 Options (decided)

**Upstream** (`index.ts:820`, `:1429`): `static options: AgentStaticOptions = {}`
on the class. Only changed fields are given; each field falls back to
`DEFAULT_AGENT_STATIC_OPTIONS`, resolved once per instance and cached. A
subclass's `static options` replaces its parent's (merged only with defaults).

**In-scope options** (agent-tool options are out of scope):

| Upstream | Default | Meaning |
| --- | --- | --- |
| `sendIdentityOnConnect` | `true` | Send `cf_agent_identity` on connect (wire §3) |
| `hungScheduleTimeoutSeconds` | `30` | A running interval schedule older than this is considered hung and reset |
| `keepAliveIntervalMs` | `30_000` | `keep_alive()` heartbeat interval |
| `retry` | `{maxAttempts: 3, baseDelayMs: 100, maxDelayMs: 3000}` | Default retries for schedule, queue (upstream also `this.retry()`; not in Python, §1.13) |
| `fiberRecoveryHookTimeoutMs` | `10_000` | Timeout for framework fiber recovery hooks |
| `fiberRecoveryScanDeadlineMs` | `10_000` | Soft deadline for one recovery scan |
| `fiberRecoveryMaxAgeMs` | 24 h (`0` = keep forever) | Give up on an interrupted fiber whose hook keeps raising |
| `maxAlarmMemoryLimitStrikes` | `3` | Alarm out-of-memory resets tolerated before the circuit breaker seals the work |

**Shapes considered:**

(A) **A frozen options dataclass as a class attribute (decided):** fields are
visible at a glance, IDEs autocomplete them, and type checkers check them.
```python
type Duration = timedelta | float  # a number is seconds (utilities.md §1)


@dataclass(frozen=True, slots=True, kw_only=True)
class AgentOptions:
    send_identity_on_connect: bool = True
    hung_schedule_timeout: Duration = 30
    keep_alive_interval: Duration = 30
    retry: RetryOptions = field(default_factory=RetryOptions)
    fiber_recovery_hook_timeout: Duration = 10
    fiber_recovery_scan_deadline: Duration = 10
    fiber_recovery_max_age: Duration | None = timedelta(hours=24)  # None = keep forever
    max_alarm_memory_limit_strikes: int = 3
    expose_error_details: bool = False     # send tracebacks to clients (local dev)


class MyAgent(Agent):
    options = AgentOptions(keep_alive_interval=2)  # or timedelta(seconds=2)
```
- **`expose_error_details`** (decided, Python addition): an unhandled error in
  a request sends its traceback in the 500 response (or the upgrade's error
  frame) instead of a generic message. Off by default; upstream always sends
  the stack trace ([lifecycle_capabilities.md](./lifecycle_capabilities.md)
  §10 item 7).
- Unset fields keep their defaults (upstream's merge, for free); unknown
  fields are a `TypeError` at class definition.
- **`frozen=True` here, unlike other records:** the object is shared by every
  instance of the class, so it must not be mutated, and it's built once at
  import, so frozen's construction cost doesn't matter.
- A subclass's `options` replaces its parent's, as upstream. To extend a
  parent's options: `options = replace(Parent.options, keep_alive_interval=…)`.

(B) **Flat class attributes** (`keep_alive_interval = timedelta(seconds=2)`):
inherits per attribute naturally, but adds eight names to every agent's
namespace (autocomplete clutter, collisions with user attributes).

(C) **Class keyword arguments** (`class MyAgent(Agent, keep_alive_interval=…)`,
via `__init_subclass__`): idiomatic for class-level configuration and
type-checked, but less familiar, and needs its own inheritance rule.

**Units (decided: option 1).** Upstream uses three conventions for durations:

| Upstream API | A bare number means |
| --- | --- |
| `schedule(when)`, `scheduleEvery(intervalSeconds)` | seconds |
| options `keepAliveIntervalMs`, `fiberRecovery*Ms`, `retry.baseDelayMs` / `maxDelayMs` | milliseconds |
| option `hungScheduleTimeoutSeconds` | seconds |
| Tasks `step.sleep`, step `timeout`, retry `delay` (`tasks/duration.ts`) | milliseconds, or a string like `"10 seconds"` |

So the same bare number means seconds in one API and milliseconds in another.
Python has already decided that `schedule()` / `schedule_every()` accept
`timedelta` or a number of **seconds**.

Options:
1. **One SDK-wide rule: a duration is `timedelta | float`, where a number is
   seconds** (chosen; recorded in [utilities.md](./utilities.md) §1). Matches Python's own convention (`time.sleep`,
   `asyncio.sleep`, `asyncio.timeout` all take seconds) and the `schedule()`
   decision. Names drop the unit suffix: `keep_alive_interval`,
   `hung_schedule_timeout`, `retry=RetryOptions(base_delay=0.1, max_delay=3)`,
   `step.sleep("cooldown", 600)`. Values are normalized to `timedelta`
   internally, and to milliseconds where storage or the wire needs them.
   Upstream's duration strings (`"10 seconds"`) aren't ported: `timedelta`
   covers them.
2. **`timedelta` only**: unambiguous and self-documenting, but verbose for
   the common case (`timedelta(seconds=30)` everywhere).
3. **Keep upstream's units** with `_ms` / `_seconds` suffixes: easiest to port,
   but keeps the seconds/milliseconds mix.

Porting note for option 1: upstream Tasks durations are milliseconds, so a
bare `5000` in upstream code is `5` (seconds) in Python.

**Timestamps (decided): timezone-aware UTC `datetime`s** in all records
(`StreamStatus`, `FiberInspection`, `FiberRecoveryContext`, …), converted from
the epoch milliseconds kept in storage ([utilities.md](./utilities.md) §1).

### 1.8 RPC (decided)

Two kinds of RPC reach an agent (upstream `docs/agents/callable-methods.md`):

| | `@callable` RPC | Native DO RPC |
| --- | --- | --- |
| Who calls | External clients (browsers, apps) over the agent WebSocket | Workers and other agents holding the DO binding |
| How | `rpc` frames (wire §5.4); `AgentClient.call()` / `useAgent().stub` | `stub = await get_agent_by_name(...)`; `await stub.method(...)` |
| Opt-in | Only methods decorated `@callable` | Every method, including `_private` ones (§1.5) |

This section is about `@callable`. Native RPC needs no decorator.

**Upstream** (`callable-decorator.ts`, `index.ts:2170`):
- `@callable({ description?, streaming? })` records metadata for the method
  (in a `WeakMap` keyed by the function). A subclass override is callable only
  if it's decorated again (the nearest declaration wins).
- A frame calling an unknown method gets `"Method X does not exist"`; a
  non-decorated one gets `"Method X is not callable"`.
- Arguments arrive as a JSON array, passed positionally. Results are sent with
  `JSON.stringify`; TypeScript's `Serializable` types check this at compile
  time only.
- A thrown error becomes `{success: false, error: err.message}` (only the
  message; `"Unknown error occurred"` for non-`Error` throws).
- **Streaming:** `@callable({ streaming: true })` passes a `StreamingResponse`
  as the first argument: `send(chunk)` → `done: false`; `end(final?)` →
  `done: true` (with `result` omitted when `final` is undefined);
  `error(message)` → `success: false`. If the method throws before closing the
  stream, the SDK sends `error(message)`. If it **returns without closing**,
  nothing is sent, and the client waits forever (streaming calls have no
  client timeout).
- `getCallableMethods()` → `Map<name, metadata>`.

**Decided:**

```python
class MyAgent(Agent):
    @callable(description="Look up the weather")
    async def get_weather(self, city: str) -> dict:
        return {"city": city, "temp": 21}

    @callable(streaming=True)
    async def process(self, response: StreamingResponse, items: list[str]) -> None:
        for i, item in enumerate(items):
            await self.handle(item)
            response.send({"progress": (i + 1) / len(items)})
        response.end({"done": True})
```

1. **Decorator:** `callable(*, description: str | None = None, streaming: bool
   = False)`, storing a `CallableMetadata` dataclass on the function (e.g. as
   `fn.__agents_callable__`). **Both `@callable` and `@callable(...)` work**
   (decided), like `@dataclass`.
2. **Async only:** the decorator raises `TypeError("Callable methods must be
   async")` on a non-`async def`, at class definition (same rule as `@task`).
3. **Overrides** must be decorated again to stay callable (as upstream).
4. **Arguments:** positional, from the frame's JSON array. A method with
   required keyword-only parameters can't be called from a client. Type hints
   aren't validated at runtime (as upstream).
5. **Results:** serialized by the SDK's JSON encoder, which also converts
   **dataclasses** and **`datetime`s** automatically (decided):
   - SDK records (e.g. `StreamStatus`) use their camelCase wire names;
   - a user's own dataclasses keep their field names;
   - `datetime` → epoch milliseconds (upstream's records use epoch ms).

   Anything else JSON can't represent gets
   `{success: false, error: "Result is not JSON-serializable: …"}` (upstream's
   WebSockets capability does the same).
6. **Errors:** `{success: false, error: str(exc)}`; if `str(exc)` is empty, the
   exception's class name. Only the message crosses the wire.
7. **`StreamingResponse`:**
   ```python
   class StreamingResponse:
       @property
       def is_closed(self) -> bool: ...
       def send(self, chunk: JSONValue) -> bool: ...  # False if closed
       def end(self, final: JSONValue = None) -> bool: ...
       def error(self, message: str) -> bool: ...
   ```
   - Synchronous, like upstream (a WebSocket send is synchronous).
   - **`None` means "no result"** (decided, see "Final results" below):
     `end()` and `end(None)` both leave `result` out, so the client resolves
     with `undefined`.
   - If the method raises before closing, the SDK calls `error(...)` (as
     upstream).
   - **If the method returns without closing, the SDK calls `end()`**
     (decided), instead of leaving the client waiting forever (upstream).
8. **`get_callable_methods() -> dict[str, CallableMetadata]`.**

   **Final results: `None` means "no result"** (decided). The final frame
   (`done: true`) leaves `result` out when the result is `None`, so the JS
   client resolves the call with `undefined`:
   - a callable returning `None`, like a TypeScript `void` method;
   - `response.end()` / `response.end(None)`;
   - an async generator finishing.

   Only the top-level final result is affected: nested `None`s are still
   `null` (`{"a": None}` → `{"a": null}`), and `response.send(None)` still
   sends a `null` chunk. **Given up:** making a call resolve to an explicit JS
   `null`; upstream's `end(null)` has no Python spelling. Rejected
   alternatives: a `MISSING` sentinel default for `end()` (a public sentinel,
   and a `None` return would still differ from TypeScript's `void`), and an
   `end_null()` method (can't help non-streaming returns). If a real need
   appears, a `JS_NULL` marker can be added later without breaking anything.
9. **Context:** inside a callable, `get_current_agent()` gives the calling
   connection. A readonly connection can call methods, but `set_state` raises
   `ReadonlyConnectionError`, which reaches the client as an RPC error.
10. **Streaming as async generators** (decided). Both styles exist, selected
    by the decorator's `streaming` flag: **`streaming=True` → the
    `StreamingResponse` style** (item 7); **an async generator with the
    default `streaming=False` → the generator style** (detected with
    `inspect.isasyncgenfunction`). `streaming=True` on an async generator is a
    `TypeError` at class definition.
    ```python
    @callable
    async def process(self, items: list[str]):
        for i, item in enumerate(items):
            await self.handle(item)
            yield {"progress": (i + 1) / len(items)}
    ```
    - each `yield` → a `done: false` chunk;
    - the generator finishing → `end()` (`done: true`, no `result`);
    - an exception → `error(str(exc))`;
    - if the client disconnects, the SDK stops iterating and closes the
      generator (`aclose()`), so `finally` blocks run. Upstream can't stop a
      streaming method this way.
    - **A final value:** an async generator can't `return` a value (Python
      forbids `return <value>` in async generators), so `end(final)` isn't
      expressible from a generator. The `StreamingResponse` style (item 7)
      stays for that, and for explicit `error()`.
    - `get_callable_methods()` reports `streaming=True` for both styles.

### 1.9 Connections (decided)

**Upstream** (`lifecycle/types.ts`, `websockets/`, `index.ts`):
- **`Connection`** is the hibernated `WebSocket` extended with: `id` (from the
  client's `_pk`), `uri` (the upgrade URL), `tags` (connection id first; at
  most 10, each at most 256 characters), and per-connection **`state`** with
  `setState(value | updaterFn)`. State is stored in the socket's hibernation
  attachment, so it survives hibernation; it's typed immutable (read it,
  replace it with `setState`).
- **`ConnectionContext`** = `{ request }` (the upgrade request).
- **Hooks:** `onConnect(conn, ctx)`, `onMessage(conn, message)`,
  `onClose(conn, code, reason, wasClean)`, `onError(conn, error)` (§3 item 6).
  `onMessage` only sees frames the SDK didn't consume (state frames, `rpc`,
  chat frames for `AIChatAgent`, voice frames, …).
- **Decision hooks, called during connect:** `getConnectionTags(conn, ctx)`
  (sync or async), `shouldConnectionBeReadonly(conn, ctx)` (sync),
  `shouldSendProtocolMessages(conn, ctx)` (sync).
- **Methods:** `getConnections(tag?)`, `getConnection(id)` (throws if two
  sockets share the id), `broadcast(message, without?: string[])`,
  `setConnectionReadonly(conn, readonly = true)`, `isConnectionReadonly(conn)`,
  `isConnectionProtocolEnabled(conn)`.
- Messages are `string | ArrayBuffer | ArrayBufferView`.

**Decided:**

```python
@dataclass(slots=True, kw_only=True)
class ConnectionContext:
    request: Request  # the Workers SDK's Request


class Connection(
    Generic[ConnState]
):  # ConnState = TypeVar(..., bound=Mapping[str, Any], default=dict[str, Any])
    id: str
    uri: str | None
    tags: Sequence[str]  # connection id first (a tuple)

    @property
    def state(
        self,
    ) -> ConnState | None: ...  # read-only; from the hibernation attachment
    def set_state(self, state: ConnState | None) -> None: ...

    @property
    def readonly(self) -> bool: ...
    @readonly.setter
    def readonly(self, value: bool) -> None: ...
    @property
    def protocol_enabled(self) -> bool: ...  # read-only; decided at connect

    def send(self, message: str | bytes) -> None: ...
    def close(self, code: int | None = None, reason: str | None = None) -> None: ...


class Agent(DurableObject, Generic[State]):
    # Hooks (async, SDK-wide rule)
    async def on_connect(
        self, connection: Connection, ctx: ConnectionContext
    ) -> None: ...
    async def on_message(
        self, connection: Connection, message: str | bytes
    ) -> None: ...
    async def on_close(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None: ...

    # Decision hooks, called during connect before any frame is sent
    async def get_connection_tags(
        self, connection: Connection, ctx: ConnectionContext
    ) -> list[str]: ...
    async def should_connection_be_readonly(
        self, connection: Connection, ctx: ConnectionContext
    ) -> bool: ...
    async def should_send_protocol_messages(
        self, connection: Connection, ctx: ConnectionContext
    ) -> bool: ...

    # Methods
    def get_connections(self, tag: str | None = None) -> Iterator[Connection]: ...
    def get_connection(self, id: str) -> Connection | None: ...
    def broadcast(
        self, message: str | bytes, *, exclude: Iterable[str | Connection] = ()
    ) -> None: ...
```

- **Messages are `str | bytes`** (binary frames arrive as `bytes`).
- **Per-connection state mirrors agent state:** read-only `state`, replaced
  with `set_state`, JSON-serializable, stored in the hibernation attachment.
  `set_state` takes a value only; upstream's updater-function form isn't
  needed in Python (read `state`, build the new value, set it).
- **Readonly and protocol flags as `Connection` properties** instead of
  upstream's agent methods (`setConnectionReadonly(conn)`,
  `isConnectionReadonly(conn)`, `isConnectionProtocolEnabled(conn)`).
- **Decision hooks are `async def`** (the SDK-wide rule); they run inside the
  already-async connect path, so this costs nothing and allows lookups.
- **`broadcast(..., exclude=...)`** accepts connection ids (like upstream's
  `without`) **and `Connection` objects** (decided). An id excludes every live
  socket with that id; a `Connection` excludes exactly that socket.
- **Equality is the underlying socket, not the id** (decided). Two
  `Connection` objects are equal when they wrap the same WebSocket. Not the
  id, because ids are client-chosen (`_pk`) and two live sockets can share one
  (a client that sets its own id, or a reconnect while the old socket hasn't
  closed yet). Upstream relies on the same rule: one wrapper per socket, and
  the WebSockets capability excludes "by object identity, not id"
  (`websockets/websockets.ts`, `broadcastState`). Wrappers are recreated after
  hibernation, so it can't be Python object identity. **Implementation
  (verified, [platform_verification.md](./platform_verification.md) §2.2):** `__eq__` compares the wrapped JS sockets
  with `==` (JS identity); `JsProxy` is unhashable, so `__hash__` hashes the
  connection **id** (equal connections always share an id; sockets sharing an
  id only collide in the hash table).
- **Attachment budget (verified, [platform_verification.md](./platform_verification.md) §2.1):** a socket's
  attachment is limited to **16,384 bytes**, shared by per-connection state
  and the SDK's metadata (`__pk`, `_cf_*` flags). `set_state` raises if the
  serialized attachment would exceed it. Attachments are written with
  `to_js(..., dict_converter=Object.fromEntries)` (a plain `dict` fails with
  `DataCloneError`).
- **Duplicate ids:** `get_connection(id)` raises a dedicated
  `DuplicateConnectionIdError` (an `AgentsException`) when two live sockets
  share the id; upstream throws a plain `Error`.
- **Tag limits** (10 tags including the id, 256 characters each) raise
  `ValueError` at connect: they come from the user's `get_connection_tags`,
  so it's misuse.

### 1.10 Errors: `on_error` (decided)

**Upstream** (`index.ts:3046`): one `onError` overloaded by arity, which
JavaScript tells apart at runtime: `onError(connection, error)` for WebSocket
connection errors, `onError(error)` for everything else. The default logs and
rethrows. User-hook failures go through `_tryCatch`, which does
`throw this.onError(e)`, so whatever the override *returns* is thrown (even
`undefined`): errors always propagate. Scheduler, Queue, and Tasks call
`onError(error)` for terminal failures and swallow anything it throws.

**Decided: one signature, both arguments always passed** (the same rule as
schedule/queue callbacks, §2.1 of the scheduling doc):

```python
async def on_error(self, connection: Connection | None, error: Exception) -> None: ...
```

`connection` is `None` when the error isn't tied to a connection. Users ignore
it with `_` when they don't need it.

**Only `Exception`s reach `on_error`** (decided): `BaseException`s that
aren't `Exception`s (`asyncio.CancelledError` from a cancelled turn, step, or
fiber; the Workers SDK's `DurableObjectAbort` from `ctx.abort`, which
`destroy()` uses; `KeyboardInterrupt`, `SystemExit`, `GeneratorExit`) are
control-flow signals, not errors in user code, and propagate untouched. So an
`on_error` that logs and returns can't swallow a cancellation or an abort.
Same rule as `retry()` and `Emitter` (only `Exception` is caught). Upstream
has no equivalent distinction in JavaScript.

**Decided: called in exactly the cases upstream calls `onError`**, no more.
Upstream's call sites (in scope; `index.ts` unless noted):

| Case | Arguments | What happens to the error | Upstream |
| --- | --- | --- | --- |
| WebSocket `error` event on a connection | `(connection, error)` | handler errors are logged | `websockets/websockets.ts` `#error` |
| `on_request` raises | `(None, error)` | **propagates** | `:2122` via `_tryCatch` |
| `on_connect` raises | `(None, error)` | **propagates** | `:2359` via `_tryCatch` |
| `on_message` raises (only frames the SDK didn't consume) | `(None, error)` | **propagates** | `:2147`, `:2155`, `:2251` via `_tryCatch` |
| `on_start` (the startup block) raises | `(None, error)` | **propagates** | `:2413` via `_tryCatch` |
| `on_state_changed` raises | `(None, error)` | notification only; anything `on_error` raises is swallowed | `:2609` |
| A scheduled callback can't be dispatched, or fails after its last retry | `(None, error)` | notification only, swallowed | `schedules/scheduler.ts:339`, `:382`, `:504` |
| A queued callback: same two cases | `(None, error)` | notification only, swallowed | `queue/queue.ts:268`, `:306`, `:395` |
| A task run fails terminally | `(None, error)` | notification only, swallowed | `tasks/tasks.ts:1300` |
| **`AIChatAgent`:** `on_chat_message` raises, or streaming its output fails (new request, continuation, saved messages, recovery), and its `get-messages` wrapper | `(None, error)` | **notification only** (decided; the turn ends as failed either way, [ai_chat_agent_api.md](./ai_chat_agent_api.md) §2) | `ai-chat/src/index.ts:2622` (`_tryCatchChat`) |

Notes:
- **Upstream passes only the error for hook failures**, even when the hook
  (`onConnect`, `onMessage`) has a connection. The port does the same:
  `connection` is non-`None` only for WebSocket error events. (The connection
  is still available through `get_current_agent()`.)
- **Not called:** `on_close` errors (not wrapped upstream), `@callable` errors
  (sent to the client as an RPC error and `rpc:error`), native RPC methods,
  and platform-class failures that Lifecycle defers (out-of-memory, reset).
- **Out of scope:** email (`onEmail`, `replyToEmail`, `sendEmail`) and
  agent-tool delivery/recovery (`:6318`, `:6342`, `:6861`, `:8207`), which also
  call it upstream.

**Propagation (decided): `on_error` is the error handler.** In the
propagating cases (`on_request`, `on_connect`, `on_message`; `on_start` is the
exception, below), the SDK calls `on_error` and does nothing more: whatever
`on_error` does decides the outcome.
- **The default logs and re-raises the error**, so behavior out of the box
  matches upstream (the error propagates).
- **An override that raises** propagates what it raises (`raise error` to
  re-raise, or raise something else).
- **An override that returns normally handles the error**; it doesn't
  propagate. Upstream can't do this (`throw this.onError(e)` always throws).

```python
async def on_error(self, connection: Connection | None, error: Exception) -> None:
    if isinstance(error, PaymentRequired):
        return  # handled
    raise error  # everything else propagates, as by default
```

What "handled" means per case (decided):
| Case | If `on_error` returns normally |
| --- | --- |
| `on_request` | the SDK responds `500 Internal Server Error` (a request needs a response; return a `Response` from `on_request` for anything else) |
| `on_connect` | the connection stays open and the connect completes |
| `on_message` | the message is dropped; the connection stays open |
| `on_start` | **can't be handled** (decided; see below) |

**`on_start` failures (decided): `on_error` is notified, but the error
always propagates.** Startup can't be half-done:
- **Upstream** (`lifecycle/durable-object-lifecycle.ts:695`): capability start
  and `onStart` run inside `blockConcurrencyWhile`. If either throws, the
  status goes back to "not started", events queued during startup are
  dropped, and the error is rethrown **outside** `blockConcurrencyWhile` (so
  the DO isn't reset and its input gate isn't broken). The invocation that
  triggered startup fails; **the next invocation (request, socket message,
  alarm, RPC) retries the whole startup**. Upstream's `onError` can't
  suppress it, since `_tryCatch` always rethrows.
- **Why it can't be handled:** after a failed `on_start`, whatever it was
  meant to set up (tables, state, connections to other services) may be
  missing, and the SDK can't know what's safe. Letting a handler return would
  let the agent run half-initialized, the failure upstream's design avoids.
  So for `on_start`, `on_error` is notification only (like the
  schedule/queue/task cases), and the SDK re-raises the original error
  afterwards.
- **Consequences for users** (to document):
  - `on_start` must be safe to run again: it reruns after every failure (use
    `CREATE TABLE IF NOT EXISTS`, check before seeding, …). The SDK's own
    capability start is idempotent for the same reason.
  - While `on_start` keeps failing, every invocation retries it and fails, so
    the agent is unavailable until the cause is fixed. Failing loudly is the
    intended behavior.
  - Work that may fail and isn't required to serve requests (warming a cache
    from an external API, …) shouldn't be in `on_start`: schedule or queue it
    from `on_start` (it gets retries), or catch that specific failure inside
    `on_start`.
  - `on_start` runs inside `blockConcurrencyWhile`, so it must finish within
    the platform's 30-second limit.
- **Later (not phase 1):** exponential backoff between startup retries.
  Upstream has none: while `on_start` keeps failing, every incoming
  invocation reruns it, including any external calls it makes.

### 1.11 HTTP: `on_request` (decided)

**Upstream:**
- `onRequest(request: Request): Response | Promise<Response>` (`index.ts:1170`)
  handles a plain HTTP request (not a WebSocket upgrade) that no capability
  claimed. **The default returns `404 "Not implemented"`.**
- **Dispatch** (`lifecycle/durable-object-lifecycle.ts:525`): the DO's `fetch`
  → ensure startup → if not an upgrade, offer the request to each capability's
  `onRequest` in order (`selective` first, `catch-all` last); the first one
  that returns a `Response` wins; otherwise the agent's `onRequest`. Upgrades
  never reach `onRequest`: they go to capabilities' `onWebSocketUpgrade`
  (the WebSockets capability), then `onConnect`.
- Runs in the agent context (`getCurrentAgent()` has the `request`), and
  errors go through `onError` (§1.10).
- **The URL is the client's original one**, prefix included
  (`/agents/<agent>/<name>/...`): `routeAgentRequest` forwards the request
  unchanged (`agent-routing.ts:226`), so the agent matches on the path's tail.
- **Who claims requests before `onRequest`** (in scope): no core capability
  claims HTTP requests. Requests whose path contains a
  `/sub/{child-class}/{child-name}` marker are forwarded to that sub-agent
  (after `onBeforeSubAgent`, §1.15) before `onRequest`. `AIChatAgent`
  intercepts paths ending in `get-messages` *inside* its `onRequest` wrapper,
  before the user's (`ai-chat/src/index.ts:1739`). Out of scope: the
  `RoutedAgents` capability and the MCP client's OAuth callback.
- **Streams over HTTP:** `sseResponse(this.streams, stream_id, request=...)`
  (`streams/sse.ts`) is meant to be returned from `onRequest`.

**Decided:** `async def on_request(self, request: Request) -> Response`,
taking and returning the Workers Python SDK's `Request` / `Response`; the
default returns `Response("Not implemented", status=404)`. Same dispatch
order. `AIChatAgent` serves `get-messages` before calling the user's
`on_request` (as upstream).

**No path-routing helpers in the SDK** (decided): users match on
`request.url` themselves. A general router for Python Workers is planned as a
separate library, outside this SDK.

### 1.12 Lifecycle hooks (decided)

**Upstream:**
- **`onStart(props?)`** (`index.ts:1167`): runs once per in-memory lifetime,
  after capabilities start and before any event is handled, inside
  `blockConcurrencyWhile`. Failure rule: §1.10.
- **`onAlarm()`** (`index.ts:4907`; `lifecycle/durable-object-lifecycle.ts:852`):
  runs **once per alarm invocation, after due jobs** (schedules, queue items,
  task steps) have been driven; then Agent housekeeping (fiber recovery,
  keep-alive) runs and the alarm is re-armed from the job table. Users don't
  control when the alarm fires: the SDK owns the single DO alarm, and
  `schedule()` is how work gets a wake. Errors don't go through `onError`.
- **`onAlarmMemoryLimit(context)`** (`index.ts:4966`): **`protected` and
  `@internal`** upstream: chat hosts override it to seal recovery work when the
  alarm out-of-memory circuit breaker trips (`MemoryLimitContext`: `sealed`,
  `nextTime`, `job`). Capabilities get the same signal through
  `on_memory_limit`.
- **`destroy()`** (`index.ts:8395`), on a top-level agent:
  1. writes a "destroy pending" KV marker first, so a teardown that's cut short
     is finished by the next wake (the `alarm()` preamble checks the marker
     *before* startup, so a condemned agent never re-runs `onStart`);
  2. disables alarms; disposes the Lifecycle (every capability) and the
     agent's own disposables; `storage.deleteAll()`;
  3. emits `destroy`, then on the next tick aborts the isolate
     (`ctx.abort`, suppressing the alarm retry). `ctx.abort` is uncatchable,
     which is why it's deferred.

  On a sub-agent, it asks the root to delete the facet
  (`ctx.facets.delete`), which aborts the facet's isolate, so the call may
  throw an abort error or never return: callers treat it as fire-and-forget.

**Decided:**

```python
class Agent(...):
    async def on_start(self) -> None: ...  # no props (§1.2 dropped Props)
    async def on_alarm(self) -> None: ...  # after due jobs, every alarm wake
    async def destroy(self) -> None: ...  # deletes everything; doesn't return normally
```

- **`on_start(self)`**: no arguments, since `Props` isn't ported.
- **`on_alarm(self)`**: kept for parity; documented as rarely needed (use
  `schedule()` / capabilities' `on_job` for timed work). Errors propagate to
  the alarm invocation (no `on_error`, as upstream).
- **`on_alarm_memory_limit` stays internal** (`_on_alarm_memory_limit`), used
  by `AIChatAgent`; not part of the public API (as upstream).
- **`destroy()`**: same sequence, including the pending-destroy marker and
  the alarm preamble. Documented as "the agent is gone after this; don't use
  it again", and as not returning normally on a sub-agent.
- `_cf_scheduleDestroy` (deferred destroy, used by MCP) is out of scope.
- **Aborting without an alarm retry (verified in production,
  [platform_verification.md](./platform_verification.md) §7.5):** an abort
  inside an alarm is otherwise retried up to 6 times. When `destroy()` runs
  from the alarm preamble it aborts with `{retryAlarm: false}` (upstream's
  `abortWithoutAlarmRetry`). The Workers SDK's `ctx.abort(reason)` takes no
  options, so the SDK calls the raw JS `abort` through `self.ctx._ctx` (a
  private SDK attribute), queued in a microtask and raising
  `DurableObjectAbort` as the SDK's own wrapper does.

**Verified ([platform_verification.md](./platform_verification.md) §2.3):** the SDK's `ctx.abort(reason)` already defers the
JS abort to a microtask and raises `DurableObjectAbort` (a `BaseException`);
`destroy()` calls it directly with no extra deferral. The caller always gets
an abort error, so `destroy()` is fire-and-forget on top-level agents too.

### 1.13 `retry()` (decided: not an `Agent` method)

Upstream's `this.retry(...)` becomes the SDK utility `agents.retry(...)`
([utilities.md](./utilities.md) §7). `Agent` has no `retry` method.

### 1.14 Routing helpers (decided)

Worker-side functions that get a request or a call to the right agent
instance. Not a path router (that's a separate, planned library; §1.11).

**Upstream** (`agent-routing.ts`):
- **`routeAgentRequest(request, env, options?) → Response | null`**:
  - matches `/{prefix}/{binding}/{name}/...` (prefix default `agents`);
    `{binding}` is the env binding name in kebab case (`MyAgent` →
    `my-agent`), found by scanning `env` for objects with `idFromName`
    (cached per `env`);
  - returns `null` if the path doesn't match; `400 "Invalid request"` if the
    binding segment matches no binding;
  - `cors: true | HeadersInit`: answers `OPTIONS` preflights and adds the
    headers to non-WebSocket responses (`true` = permissive defaults);
  - hooks: `onBeforeConnect(request, {className, name})` for upgrades,
    `onBeforeRequest(...)` for everything else; each may return a `Response`
    (short-circuit, e.g. auth failure), a `Request` (replace), or nothing;
  - placement: `jurisdiction`, `locationHint`; `props` (sent in an
    `x-agents-lifecycle-props` header);
  - **`routingRetry`** (on by default; `false` disables; 3 attempts, 100 ms
    base, 800 ms max, optional `onRetry` callback) retries the
    `stub.fetch` only for errors the platform marks transient
    (`error.retryable === true` and not `overloaded`).
  - Forwards the request unchanged (URL included) to `stub.fetch`.
- **`getAgentByName(namespace, name, options?) → stub`**: `idFromName` +
  `get`, then calls `stub.__unsafe_ensureInitialized(props)` over RPC so startup
  has run before the caller's RPC (native RPC bypasses `fetch`, where startup
  normally happens), with the same routing retry. Options: `jurisdiction`,
  `locationHint`, `props`, `routingRetry`.
- The stub is typed `DurableObjectStub<T>` (every method of `T`, promisified).

**Decided:**

```python
class AgentRoute(NamedTuple):
    class_name: str  # the env binding name, e.g. "MyAgent"
    name: str


type RoutingRetry = RoutingRetryOptions | Literal[False] | None  # None = defaults
type BeforeHook = Callable[[Request, AgentRoute], Awaitable[Response | Request | None]]


async def route_agent_request(
    request: Request,
    env: Any,
    *,
    prefix: str = "agents",
    cors: bool | Mapping[str, str] = False,
    jurisdiction: str | None = None,
    location_hint: str | None = None,
    routing_retry: RoutingRetry = None,
    on_before_connect: BeforeHook | None = None,
    on_before_request: BeforeHook | None = None,
) -> Response | None: ...


async def get_agent_by_name(
    namespace: Any,
    name: str,
    *,
    jurisdiction: str | None = None,
    location_hint: str | None = None,
    routing_retry: RoutingRetry = None,
) -> Any: ...  # the DO stub
```

Worker usage:
```python
class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return await route_agent_request(request, self.env) or Response(
            "Not found", status=404
        )
```

- **No `props`** (`Props` isn't ported, §1.2).
- **Hooks are `async def`** (SDK-wide rule) and receive `(request, route)`,
  where `route` is an `AgentRoute` `NamedTuple` (small immutable pair,
  utilities §5) saying which instance the request is for: for
  `/agents/chat-room/lobby/...`, `AgentRoute(class_name="ChatRoom",
  name="lobby")` (decided). `class_name` is the env **binding** name, which
  may differ from the Python class name. Typical use: authorization, returning
  `Response("Forbidden", status=403)` to stop the request.
- **Routing retry is built on `retry()`** (utilities §7) with
  `should_retry` = "the platform marked it transient" (decided). Its options,
  including upstream's `onRetry` callback (kept, decided):
  ```python
  @dataclass(slots=True, kw_only=True)
  class RoutingRetryEvent:
      error: Exception
      attempt: int
      max_attempts: int
      delay: timedelta
      name: str
      class_name: str | None  # None from get_agent_by_name, as upstream


  @dataclass(slots=True, kw_only=True)
  class RoutingRetryOptions:
      max_attempts: int = 3
      base_delay: Duration = 0.1
      max_delay: Duration = 0.8
      on_retry: Callable[[RoutingRetryEvent], Awaitable[None]] | None = None
  ```
  `routing_retry: RoutingRetryOptions | Literal[False] | None` (`None` = the
  defaults, `False` disables). `on_retry` is `async def` (SDK-wide rule),
  called before each backoff sleep; if it raises, the error is logged and the
  retry continues (as upstream).
- **`cors=True`** gives upstream's permissive defaults; a mapping gives
  explicit headers.
- **Stub typing (decided): untyped (`Any`) for now.** A typed stub would need
  a "every method becomes async" mapped type, which Python's type system can't
  express. **Later (typing only):** an optional class argument,
  `get_agent_by_name(env.WeatherAgent, "london", WeatherAgent)`, typed as
  returning that class: accurate for `async def` methods, wrong for plain
  `def` methods (awaitable over RPC, but typed as returning the value).

**Verified ([platform_verification.md](./platform_verification.md) §2.4–2.6):** bindings are listed with
`js.Object.keys(env._env)` (a private SDK attribute: a known dependency);
`exc.retryable` / `exc.overloaded` are readable on `JsException`; the startup
method is reachable over native RPC under upstream's name
(`__unsafe_ensureInitialized`, attached with `setattr` since a name starting
with `__` would be mangled inside the class body).

### 1.15 Sub-agents (decided)

A sub-agent is a child agent running as a **facet** of its parent: its own
isolate and SQLite database, on the same machine, supervised by the parent
([scope.md](./scope.md) §2.6).

**Upstream** (`index.ts`, `dynamic-agents/api.ts`, `sub-routing.ts`):
- **`this.dynamicAgents`** (`index.ts:1290`, experimental) is the current API:
  - `get(Cls, name) → stub`: get or create; the first call runs the child's
    `onStart`;
  - `abort(Cls, name, reason?)`: stop it now (pending RPC calls get `reason`
    as an error; restarted on the next `get`); transitive;
  - `delete(Cls, name)`: abort, then wipe its storage; transitive;
  - `has(Cls | className, name)`, `list(Cls | className?) →
    [{className, name, createdAt}]`: read the parent's registry table
    (`cf_agents_sub_agents`).
- **`subAgent`, `abortSubAgent`, `deleteSubAgent`, `hasSubAgent`,
  `listSubAgents` are `@deprecated`** ("Use `this.dynamicAgents.…` instead");
  each just delegates.
- **`parentAgent(Cls) → stub`** (`:5487`): the **immediate** parent. Throws if
  this agent isn't a facet, or if `Cls.name` doesn't match the recorded
  parent class (guards against reaching the wrong DO). A top-level parent is
  found via `env[Cls.name]`, then the Worker's exports; a facet parent through
  a bridge via the root.
- **`parentPath` / `selfPath`**: ancestor chain, root first (decided, §1.5).
- **`onBeforeSubAgent(request, {className, name})`** (`:5304`): a gate on the
  parent for external requests to `/.../sub/{child-class}/{child-name}/...`;
  returns a `Response` (short-circuit, e.g. 404), a `Request` (replace;
  headers/body flow through, the child's path is always the tail), or nothing.
  Default: allow.
- **Worker-side** (`sub-routing.ts`):
  - `routeSubAgentRequest(request, parentStub, { fromPath? }) → Response`:
    for custom URL shapes; forwards the `/sub/...` tail through the parent
    (so `onBeforeSubAgent` runs). `routeAgentRequest` already does this for
    the default `/agents/...` shape.
  - `getSubAgentByName(parentStub, Cls, name) → stub`: RPC to a child from
    outside the parent, one extra hop per call through the parent. RPC only
    (no `.fetch()`), and **doesn't run `onBeforeSubAgent`** (like
    `getAgentByName` not running `onBeforeConnect`).

**Decided:**

```python
class Agent(...):
    @property
    def dynamic_agents(self) -> DynamicAgents: ...

    async def parent_agent(
        self, cls: type[Agent]
    ) -> Any: ...  # immediate parent's stub
    async def on_before_sub_agent(
        self, request: Request, child: AgentRoute
    ) -> Request | Response | None: ...


class DynamicAgents:
    async def get(
        self, cls: type[Agent], name: str
    ) -> Any: ...  # stub, untyped (§1.14)
    def abort(
        self, cls: type[Agent], name: str, reason: Exception | None = None
    ) -> None: ...
    async def delete(self, cls: type[Agent], name: str) -> None: ...
    def has(self, cls: type[Agent] | str, name: str) -> bool: ...
    def list(self, cls: type[Agent] | str | None = None) -> list[SubAgentInfo]: ...


@dataclass(slots=True, kw_only=True)
class SubAgentInfo:
    class_name: str
    name: str
    created_at: datetime


# Worker side
async def route_sub_agent_request(
    request: Request, parent: Any, *, from_path: str | None = None
) -> Response: ...
async def get_sub_agent_by_name(parent: Any, cls: type[Agent], name: str) -> Any: ...
```

```python
class Inbox(Agent):
    async def open_chat(self, chat_id: str) -> str:
        chat = await self.dynamic_agents.get(Chat, chat_id)
        return await chat.summary()

    async def on_before_sub_agent(self, request, child):
        if not self.dynamic_agents.has(child.class_name, child.name):
            return Response("Not found", status=404)
        return None


class Chat(Agent):
    async def remember(self, fact: str) -> None:
        inbox = await self.parent_agent(Inbox)
        await inbox.add_fact(fact)
```

- **Only `dynamic_agents` is ported** (decided), not the deprecated `sub_agent` /
  `abort_sub_agent` / `delete_sub_agent` / `has_sub_agent` /
  `list_sub_agents` (the rule from scheduling doc §2.9: deprecated upstream
  functions aren't ported).
- **`on_before_sub_agent`'s `child` is the `AgentRoute` from §1.14** (decided)
  (`class_name`, `name`): same shape as `route_agent_request`'s hooks. Here
  `class_name` is the child's **class** name (`cls.__name__`, parsed from the
  `/sub/{child-class}/...` segment), not an env binding name.
- **Class identity is `cls.__name__`**, as upstream uses `Cls.name`; the child
  class must be exported from the Worker module (facets are created from
  `ctx.exports`).
- **`parent_agent` misuse raises built-ins:** `RuntimeError` when called on a
  non-facet, `TypeError` when `cls` isn't the recorded parent class.
- **`abort`'s `reason`** is an exception (raised in pending callers); the
  default is a new `SubAgentAbortedError` (an `AgentsException`; decided, not
  upstream).
- **Stubs are untyped (`Any`)**, as decided for `get_agent_by_name` (§1.14).

### 1.16 `get_current_agent()` (decided)

**Upstream** (`lifecycle/current-agent.ts:84`, `index.ts:1035`, `:2989`,
`docs/agents/get-current-agent.md`):
- `getCurrentAgent<T>() → { agent, connection, request, email }`, read from
  an `AsyncLocalStorage`. Outside any agent context, every field is
  `undefined` (it never throws). The type parameter is a cast only.
- **The SDK sets it** around every host hook: `onStart` / `onAlarm` (agent
  only), `onRequest` (+ `request`), `onConnect` (+ `connection`, and the
  upgrade `request`), `onMessage` / `onClose` (+ `connection`), `@callable`
  methods (+ the calling connection), schedule/queue/task callbacks, state
  hooks. **Capability hooks run outside it** (`runWithoutCurrentAgent`).
- **Custom methods are auto-wrapped:** at construction, every public
  (non-`_`) method a subclass defines is replaced on the prototype with a
  wrapper that enters `{agent: this}` (connection/request unset) unless that
  agent is already current. This is what makes it work in native-RPC entry
  points, which bypass the hooks. `@callable` metadata is copied to the
  wrapper. Crossing into a *different* agent never carries the previous
  connection/request along.
- **Where context is lost:** code reached outside the invocation's call tree
  (callbacks invoked over RPC from another isolate, service bindings, queue
  consumers). The documented fix: route through a public agent method, which
  re-enters context.
- Main use: helper functions and libraries (e.g. AI SDK tools) that need the
  agent without having it passed in.

**Decided:**

```python
@dataclass(slots=True, kw_only=True)
class CurrentAgent(Generic[A]):  # A = TypeVar("A", bound=Agent, default=Agent)
    agent: A
    connection: Connection | None
    request: Request | None


def get_current_agent() -> CurrentAgent | None: ...
```

```python
async def lookup_weather(city: str) -> dict:  # a tool function, no agent passed in
    current = get_current_agent()
    if current is None:
        raise RuntimeError("must run inside an agent")
    return await current.agent.weather_cache.get(city)
```

- **Backed by a `contextvars.ContextVar`**, Python's equivalent of
  `AsyncLocalStorage`: each asyncio task sees its own value, and tasks
  started with `asyncio.create_task` inherit the context they were created
  in (so background work keeps the agent). One isolate can host several DO
  instances; the context keeps them apart.
- **Same places as upstream:** set around every host hook, `@callable`, and
  schedule/queue/task callback with the same fields; capability hooks run
  with it cleared. `email` is dropped (out of scope).
- **Outside any agent: `None`** (decided) instead of upstream's object of `undefined`s,
  so `agent` is never `None` inside a `CurrentAgent` and type checkers make
  callers handle the outside case once.
- **Auto-wrapping public methods, at class definition** (decided). Its only
  purpose is making `get_current_agent()` work in methods entered through
  native RPC (which bypasses every SDK hook), and in the helpers they call.
  `Agent.__init_subclass__`
  wraps each public plain function a subclass defines (once per class, not
  per instance as upstream's prototype patching). **Names defined on `Agent`
  itself are skipped** (as upstream skips `Agent.prototype` names): overridden
  hooks like `on_request` already run inside the context the SDK sets, and
  must keep their `connection` / `request`. The wrapper enters
  `CurrentAgent(agent=self, connection=None, request=None)` unless `self` is
  already the current agent. It keeps everything else working: it's a plain
  function (workerd's `collect_methods` still exposes it to native RPC),
  `functools.wraps` copies `@callable` metadata, async generators get an
  async-generator wrapper (so `inspect.isasyncgenfunction` still holds),
  `async def` an `async def` wrapper, and sync methods a sync wrapper.
- **Typing:** `get_current_agent()` returns `CurrentAgent[Agent]`; narrowing
  to a subclass is the caller's `isinstance` (or `cast`), as upstream's type
  parameter is only a cast.

**Verified ([platform_verification.md](./platform_verification.md) §2.7):** the context is **lost** in callbacks the JS runtime
calls directly (`setTimeout`, `queueMicrotask`; event listeners likewise),
and kept in `asyncio` tasks, `call_soon`, and `Promise.then`. Documented as a
"context lost" case; the fix is calling an (auto-wrapped) agent method.

### 1.17 Small members and the Worker entrypoint

**`get_mcp_servers()`: not ported** (decided). MCP is out of scope, so
nothing would ever call it. The same goes for every other MCP member.

**`session_affinity` (decided): kept**, as the documented way to get a stable,
unique key per agent instance (Workers AI session affinity being the main
use):

```python
@property
def session_affinity(self) -> str:
    return str(self.ctx.id)
```

Upstream (`index.ts:1391`) is a
read-only property returning `ctx.id.toString()`: a stable, globally unique
key per agent instance. Passed to Workers AI as the session-affinity option
(sent as the `x-session-affinity` header), it routes every request from one
agent to the same model replica, so the conversation's prompt prefix stays in
that replica's cache across turns (faster, cheaper). Python equivalent:
`str(self.ctx.id)`. Since `on_chat_message` is provider-agnostic, the user
makes the Workers AI call and would pass it themselves.

**Worker entrypoint (decided): the Workers Python SDK's own
`WorkerEntrypoint`**, with no SDK wrapper. Agent classes are exported from the
Worker's main module (the binding's `class_name` names them), and `fetch`
calls `route_agent_request`:

```python
from workers import Response, WorkerEntrypoint
from agents import Agent, route_agent_request


class Chat(Agent): ...  # exported: bound as a DO class in wrangler config


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return await route_agent_request(request, self.env) or Response(
            "Not found", status=404
        )
```

### 1.18 The `State` and `WebSockets` capabilities (decided, step 4)

Implemented in `src/agents/state/` and `src/agents/websockets/` (ports of
upstream `state/index.ts` and `websockets/`). Both work on a plain Durable
Object; `Agent` (step 5) builds on them.

```python
state = State(initial_state={"count": 0}, validate_state_change=..., on_changed=...)
websockets = WebSockets(
    handlers=WebSocketHandlers(on_connect=..., on_message=..., on_close=..., on_error=...),
    protocol=True,            # or an async per-connection decision, or False
    readonly=...,             # async decision
    state=state,              # sync this state to clients
    connection_tags=...,      # async, returns list[str]
)
lifecycle = Lifecycle(self).use(state).use(websockets)
```

1. **No Cap'n Web wire** ([scope.md](./scope.md) §3): every upgrade is a
   hibernatable JSON-frame connection.
2. **The `callables` option waits for step 5.** It needs the RPC engine
   (`@callable`, `StreamingResponse`, async generators, §1.8), which `Agent`
   uses too; it will live in `websockets/` and serve both.
3. **`None` means "no state"** (the same rule as RPC results, §1.8):
   `State.get()` returns `None` when nothing is stored, `initial_state=None`
   seeds nothing, and a `None` state isn't pushed on connect or broadcast.
   Upstream distinguishes `undefined` (nothing) from a stored `null`.
4. **`validate_state_change` and `on_changed` are synchronous** on `State`
   (upstream's `onChanged` may return a promise). `on_changed` is where
   `broadcast_state` is wired, which must happen inside `set`. A failing
   `on_changed` is logged. `Agent`'s async `on_state_changed` (§1.6) is
   scheduled by `Agent`'s own wiring.
5. **`WebSocketHandlers` is a dataclass of optional async callables**;
   `WebSockets.use(handlers)` adds more, run first, whose `on_message` can
   claim a frame by returning `True` (upstream's `use`).
6. **The decision hooks are async and run in the host context** with the
   connection and request (upstream calls them directly), so
   `get_current_agent()` works in them like in every other user callback.
7. **The attachment is `{"__pk": {id, tags, uri}, "__user": state,
   "__flags": {...}}`.** Internal flags (`readonly`, `no_protocol`, and later
   `Agent`'s own) get their own namespace instead of upstream's hidden
   `_cf_*` keys inside the user's state, so no wrapper has to strip them from
   `state` or merge them back on `set_state`. `Connection._flag` /
   `_set_flag` replace `registerInternalConnectionKeys`.
8. **The attachment is written before `acceptWebSocket`** (verified on
   `workerd`), so the tags hook can already use the connection, including
   its state (upstream needs a pre-accept shim for that).
9. **`Connection` caches only `id`, `uri`, and `tags`** (fixed at accept);
   `state` and the flags are read from the attachment on each access, so two
   wrappers of one socket never disagree (upstream caches attachments per
   socket in a `WeakMap`, which needs hashable sockets).
10. **Identity overrides are keywords:** `send_connect_frames(connection, *,
    name=None, agent=None)` and the same for `send_identity` (for facets).
    The default `agent` is the host class in kebab case
    (`core.naming.camel_case_to_kebab_case`, upstream's
    `camelCaseToKebabCase`).
11. **`broadcast(message, *, exclude=())` lives on `WebSockets`**; `Agent`'s
    adds the facet filtering.
12. **Generated connection ids** are `secrets.token_urlsafe(16)`: 22
    characters from nanoid's alphabet.
13. **`Connection.send` raises if the socket has closed** (the platform's
    `JsException`, as upstream's raw `send`); the SDK's own protocol frames
    tolerate a client that disconnected meanwhile.
14. **Errors:** `DuplicateConnectionIdError` and
    `ConnectionStateTooLargeError` (both `AgentsException`s, in
    `websockets/errors.py`); bad tags from the tags hook are a `ValueError`
    that fails the upgrade.

Verified on `workerd` with a real WebSocket client (`verify/results/ws_client.py`):
the connect sequence with `stateFollows`, client state broadcast to others
but not the sender, server changes to everyone, connection state and flags
across wrappers, the 16 KiB error (state left unchanged), binary and non-JSON
frames reaching `on_message`, tags, generated ids, and close handshakes in
both directions.

### 1.19 `Agent` implementation decisions (decided, step 5)

Implemented in `src/agents/agent/` (`Agent`, `AgentOptions`,
`get_current_agent`, routing), `src/agents/websockets/rpc.py` (the RPC
engine), `src/agents/observability/`, and `src/agents/core/encoding.py`.
`Agent` installs State, WebSockets, and Queue; Scheduler, Tasks, and
sub-agents join in their own build steps.

1. **`class Agent(Generic[S], DurableObject)`, `Generic` first (verified on
   `workerd`).** The Workers SDK's `DurableObject.__init_subclass__` doesn't
   chain to `super()`, so with `Generic` second, `Generic.__init_subclass__`
   never runs, `__parameters__` is missing, and `Agent[...]` fails at import.
   `Generic` first chains on to the SDK's, which still wraps every subclass.
   (ruff's `UP046` is suppressed there: PEP 695 syntax puts `Generic` last
   and can't express the default on 3.12.) The test fake `DurableObject`
   now skips chaining too, so the suite catches this.
2. **The RPC engine lives in `websockets/rpc.py`** (`@callable`,
   `CallableMetadata`, `StreamingResponse`, `RpcDispatcher`), not `agent/`:
   the `WebSockets` capability's `callables` option uses it, and capabilities
   can't import `agent/`. `Agent` passes `callables=self`, so plain DOs and
   agents answer `rpc` frames the same way. Only `@callable` methods are
   exposed in both cases (upstream exposes every method of a plain host's
   `RpcTarget`). `from agents import callable` is unchanged.
3. **`Lifecycle(host, *, hooks=...)`:** the object whose `on_start` /
   `on_request` / `on_alarm` / `on_job` Lifecycle calls (default: the host).
   `Agent` passes an internal object that applies its error policy around
   the user's overrides (upstream reassigns the hooks on the instance).
4. **The default `on_error` re-raises without logging:** where the error
   propagates, the SDK logs it once (Lifecycle's entry points); where
   `on_error` is only notified (`on_start`, `on_state_changed`, queued
   callbacks), the source has already logged it. Same outcome as "log and
   re-raise", without double log lines.
5. **`destroy()` resets the object on the next tick, without an alarm retry,
   from every path** (upstream's `abortWithoutAlarmRetry` after a
   `setTimeout`), instead of the SDK's raising `ctx.abort`. Verified on
   `workerd`: an RPC caller of `destroy()` still gets `Error: destroyed`, so
   "fire-and-forget" (§1.12) holds; a fresh instance starts from
   `initial_state`.
6. **`initial_state` is read when first needed**, so assigning
   `self.initial_state` in `__init__` (§1.4) works; each agent seeds a JSON
   copy.
7. **Queue methods:** `queue(callback, payload, *, retry, id) -> str`,
   `dequeue(id)`, `dequeue_all()`, `dequeue_all_by_callback(callback)`,
   `get_queue(id)`, `queue_items(callback=None)`. Names pending the naming
   pass ([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §3).
8. **Routing retry is its own loop**, a port of upstream's
   `retryDurableObjectOperation`: its backoff (`base * 2 ** (attempt - 1)`)
   differs from `retry()`'s, and `on_retry` gets the computed delay before
   the sleep, which `retry()` has no hook for (§1.14 said "built on
   `retry()`").
9. **CORS, request cloning, and listing bindings go through `_ffi`**
   (`with_headers`, `clone_request`, `env_binding_names`): fetched responses
   have immutable headers, and the SDK's `Request` has no `clone`.
10. **JSON:** `core.encoding.to_json` (compact; dataclasses by wire name,
    omitting optional `None` wire fields; aware `datetime` → epoch ms) is the
    one encoder for frames, RPC results, and events.
11. **Observability** as decided ([observability.md](./observability.md)):
    `LoggingObservability` (JSON lines on `agents.events` at `DEBUG`),
    `subscribe(channel, listener)` with upstream's prefix-to-channel
    mapping, and the `ConsoleHandler`, installed on the `agents` logger when
    the real `_ffi` loads (only on Workers). Verified on `workerd`: event
    lines arrive at their own level.
12. **`send_state` guards the seeding broadcast** too (as
    `send_connect_frames` does), so a connection whose state read seeds
    `initial_state` gets it once.
13. **No synchronous `call_in_host_context` service, for now** (decided):
    capabilities run sync work in the host context through a small `async`
    wrapper (`WebSockets._apply_state_frame_async`), accepting a coroutine
    per state frame. Rejected: making `run_in_host_context` accept sync and
    async functions (an "await it if it's awaitable" branch, against
    [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.3,
    with types that can't be expressed precisely). A sync counterpart can be
    added later if the wrappers multiply.

Verified on `workerd` with real WebSocket and HTTP clients
(`verify/results/agent_client.py`): routing (including CORS, preflight, and
400 for an unknown agent), the connect sequence with the empty MCP frame,
`@callable` results, `StreamingResponse` and async-generator streams, the
calling connection in `get_current_agent()`, "is not callable", `on_message`,
`queue()` running a method that broadcasts state, a client state frame, and
`get_agent_by_name` with native RPC to an auto-wrapped method (current agent
set), `destroy()`, and a fresh instance afterwards.

---

## 2. Declaring extra capabilities (decided: `__init__` + `use()`)

How a user adds capabilities beyond the ones `Agent` installs itself (Scheduler,
Queue, Tasks, State, WebSockets), e.g. `Streams` or `Sessions`.

### 2.0 What's installed by default

Users only install capabilities **beyond** these:

| Class | Installs (upstream) | Python phase 1 |
| --- | --- | --- |
| `Agent` | Scheduler, Queue, MCP client, State, WebSockets, Tasks, DynamicAgents (`src/index.ts:2037`) | the same, minus the MCP client (out of scope) |
| `AIChatAgent` | everything `Agent` installs, plus Sessions and Streams (`ai-chat/src/index.ts:1078`) | the same |

So `use()` is for extras: Streams or Sessions on a plain `Agent`,
or a custom capability. A typical `Agent` or `AIChatAgent` never calls
`use()`.

### 2.1 Why a plain class attribute doesn't work

```python
class MyAgent(Agent):
    streams = Streams()  # evaluated once, when the class body runs
```

Every instance would share one `Streams` object. Several Durable Object
instances of the same class can live in one isolate, so they would share
wakeups, bound Lifecycle services, and caches (demonstrated in the scratchpad,
`classattr.py`). TypeScript's `readonly streams = new Streams()` looks the same
but is a per-instance field; Python's isn't.

### 2.2 Options

**(a) Explicit `__init__` with `Agent.use()` (chosen).** Upstream's model,
one line per capability:

```python
class MyAgent(Agent):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.streams = self.use(Streams())
        self.sessions = self.use(Sessions())
```

`use()` is defined in §2.3. Plain Python: the constructor runs once,
arguments can refer to `self`, and plain DOs follow the same model.

**(b) Declarative, with an explicit factory** (in the style of
`dataclasses.field(default_factory=…)`):

```python
class MyAgent(Agent):
    streams = capability(Streams, max_chunk_bytes=262_144)
    sessions = capability(Sessions)
```

`capability(...)` is visibly a per-instance declaration, not an object, so
nothing is cloned; `Agent.__init__` constructs one per agent. Still needs a
descriptor (with typing overloads), plus inheritance and ordering rules.

**(c) Class-level instances, cloned per agent** (`streams = Streams()`).
Prototyped (scratchpad `declarative.py`) and **not recommended**:
- it contradicts how Python class attributes behave, and the standard
  library's own stance (`dataclasses` rejects mutable class-level defaults and
  requires `default_factory`);
- each capability is constructed twice (template, then per agent), so
  constructor side effects run twice and every constructor must be safe to
  call again;
- constructor arguments are shared by reference across clones;
- the template and the instance differ (`MyAgent.streams is not agent.streams`),
  which confuses introspection, copying, and tests;
- it adds a second way to declare capabilities, and plain DOs would differ from
  `Agent`.

(`@task` doesn't have these problems: decorating a method is ordinary Python,
and methods are per-class definitions, not per-instance state.)

### 2.2.0 Decided: no class-level declaration

**Options (b) `capability(...)` and (c) cloned class attributes are rejected.**
Both need descriptor machinery (`__set_name__`, `__get__` overloads, collection
along the MRO, ordering rules) for little benefit. A `capability(...)`
descriptor was prototyped (scratchpad `capfactory.py`), then dropped as
unnecessary complexity.

**Decided: override `__init__` and call `self.use(...)`** after
`super().__init__(ctx, env)`:

```python
class MyAgent(Agent):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.streams = self.use(Streams())
```

Extra capabilities are rare (§2.0: `Agent` and `AIChatAgent` already install
everything most agents need), so a dedicated hook isn't worth a new concept.
A synchronous `setup()` hook was adopted briefly, then dropped for this reason
(§2.2.2); an async one was rejected (§2.4).

### 2.2.1 Precedents in Python libraries

| Pattern | Library | Matches |
| --- | --- | --- |
| Install a thing and get it back in one call | stdlib `contextlib.ExitStack.enter_context(cm)` returns the context manager's value | `self.streams = self.use(Streams())` (option a) |
| Explicit registration, refused after startup | Starlette `app.add_middleware(cls, **kwargs)` raises "Cannot add middleware after an application has started" | `Lifecycle.use` refusing after startup |
| Plugins implementing optional hooks, with ordering and "first non-`None` result wins" | `pluggy` (pytest's plugin system): `PluginManager.register(plugin)` returns its name; hook implementations are optional; `tryfirst` / `trylast`; `firstresult=True` hooks | Capability hook dispatch: optional hooks, catch-alls last, first `Response` wins |
| Shared extension object, per-app state, explicit `init_app(app)` | Flask extensions | A different answer to the same sharing problem: one object, state keyed by app |
| Pass the class and its arguments; the framework constructs it | Starlette `add_middleware(cls, **kwargs)`; `dataclasses.field(default_factory=…)`; attrs `Factory`; pydantic `Field(default_factory=…)` | Option (b) `capability(Streams, **kwargs)` |
| Declarative class attributes made per-instance | Django model fields, SQLAlchemy declarative columns | Only with a **different declaration type** (`Field`, `Column`); none clone a live object, as option (c) would |

### 2.2.2 Rejected: a synchronous `setup()` hook

```python
class MyAgent(Agent):
    def setup(self) -> None:
        self.streams = self.use(Streams())
        self.sessions = self.use(Sessions())
```

- **Why:** less boilerplate than overriding `__init__` (no `(ctx, env)`
  signature, no `super().__init__(ctx, env)` call). For comparison, upstream
  also needs a constructor override for extra capabilities
  (`docs/agents/streams.md`: a field plus `constructor(ctx, env) { super(ctx,
  env); this.lifecycle.use(this.streams); }`).
- **Synchronous, and called at the end of `Agent.__init__`**, after
  `self.lifecycle` and the built-in capabilities exist. So the timing is
  always right (before Lifecycle can start), and the attributes exist as soon
  as the agent is constructed. An async `setup()` was considered and rejected
  (§2.4).
- **Async startup work goes in `on_start`**, which runs after capabilities have
  started.
- **The SDK's own classes don't use `setup()`.** `Agent`, `AIChatAgent`, and
  other SDK subclasses install their capabilities in `__init__`, so a user's
  `setup()` never has to call `super().setup()` to keep the SDK working. A
  user's own agent hierarchy chains with `super().setup()` as usual.
- **`use()` after startup** still raises "Lifecycle capabilities must be added
  before startup".
- Upstream Think has a similar shape (`configureSession(session)`,
  `configureContext()`).

**Why it was dropped:** once it was clear that `Agent` and `AIChatAgent`
install their capabilities by default (§2.0), the boilerplate it saved only
affects the rare agent with extras, which didn't justify a second way to
install capabilities, a guard for using capabilities before they start, or a
name that sits close to `on_start`.

### 2.3 `Agent.use()`

```python
class Agent:
    def use[C: LifecycleCapability](self, capability: C) -> C:
        """Install a capability into this agent's Lifecycle and return it."""
        self.lifecycle.use(capability)
        return capability
```

- **It's a thin wrapper over `Lifecycle.use`** that returns the *capability*
  instead of the Lifecycle, so installing and assigning take one line.
- **`Lifecycle.use` does the work** ([lifecycle_capabilities.md](./lifecycle_capabilities.md) §4):
  refuses to install after startup, refuses a
  duplicate capability id, places catch-alls last, binds the Lifecycle
  services (storage, the job-queue view scoped to this capability, events,
  routing, …), and registers the capability for hook dispatch (`on_start`,
  `on_request`, `on_job`, …).
- **Call it in `__init__`.** Lifecycle starts on the first event after
  construction; calling `use()` later raises "Lifecycle capabilities must be
  added before startup".
- **The return type is the argument's type** (PEP 695 generic), so
  `self.streams` is typed as `Streams`.
- **`Lifecycle.use` keeps upstream's chaining** (returns the Lifecycle), for
  plain DOs: `self.lifecycle = Lifecycle(self).use(self.streams).use(self.sessions)`.

### 2.4 Rejected: an async `setup()`

The idea: install extra capabilities in an `async def setup(self)` hook, so
startup can do async work (HTTP requests, computed configuration) first.

```python
class MyAgent(Agent):
    async def setup(self) -> None:
        config = await fetch_config(self.env)
        self.streams = self.use(Streams(max_chunk_bytes=config.max_chunk_bytes))
```

**How upstream startup works** (`durable-object-lifecycle.ts:679`):
1. Startup runs **once per wake**, on the first event (fetch, alarm, socket
   message, or native RPC via `ensureInitialized`). Concurrent callers share
   it; a failure resets it so the next event retries.
2. The capability list is **locked**, then everything runs inside
   **`ctx.blockConcurrencyWhile(...)`**: every capability's `on_start`
   (migrations, restoring state), then the host's `on_start`.
3. `blockConcurrencyWhile` holds back **all** other events until startup
   finishes, and the runtime resets the object if the callback takes more
   than 30 seconds.

**What this means for `setup()`:**
- **Async work at startup is already possible today, in `on_start`**: it is
  async and runs after capabilities have started, so it can call `set_state`,
  `schedule`, and so on. HTTP requests to initialize state belong there.
- **The only new ability `setup()` adds** is choosing capabilities (or their
  arguments) from async work. To allow that, `setup()` must run **before** the
  capability list is locked and before any capability's `on_start`:
  `setup()` → lock → capabilities' `on_start` → `on_start`.
- **Capabilities aren't started during `setup()`**, so state, schedules,
  queues, tasks, and Sessions aren't usable there (their tables may not exist
  yet). Calling them should raise a clear error ("not available during
  setup(); use on_start()"), not fail obscurely.
- **It runs on every wake and blocks all events** (inside
  `blockConcurrencyWhile`, 30-second limit), so slow HTTP in `setup()` adds
  latency to the first request after every eviction.
- **Native RPC** also triggers startup, so `setup()` runs before the first
  RPC call too.

**Options (decided: (i)):**
- **(i) Keep `__init__` + `use()`** (chosen), and do async work in
  `on_start`. No new hook; capabilities can't depend on async results.
- **(ii) Add `async def setup()`** with the ordering above, keeping
  `__init__` + `use()` working too. Capabilities can depend on async results;
  costs one more hook, a "not available during setup" guard, and two places a
  capability can be installed.
- **(iii) Replace `__init__` + `use()` with `setup()` only.** One place to
  install capabilities, but every agent with extras needs an async hook even
  when nothing async is needed.

## 3. Index

Every in-scope member of upstream's `Agent` class (from `src/index.ts`) is
decided. Designed elsewhere: scheduling, queues, tasks
([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md)); fibers,
`keep_alive`, `stash` ([fibers_api.md](./fibers_api.md)); `observability`
([observability.md](./observability.md)); SQL (`self.sql`, [utilities.md](./utilities.md) §6);
`retry()` ([utilities.md](./utilities.md) §7).

| Topic | Section |
| --- | --- |
| Class shape, `self.env` / `self.ctx`, entry points, `name`, `lifecycle`, paths | §1.2, §1.5 |
| `initial_state`; why capabilities aren't class attributes | §1.4, §1.3 |
| State: `state`, `set_state`, `validate_state_change`, `on_state_changed` | §1.6 |
| `AgentOptions`, durations, timestamps | §1.7 |
| `@callable`, `StreamingResponse`, async-generator streaming | §1.1, §1.8 |
| `Connection`, connection hooks, `broadcast` | §1.9 |
| `on_error` | §1.10 |
| `on_request` | §1.11 |
| `on_start`, `on_alarm`, `destroy` | §1.12 |
| `retry` (not on `Agent`) | §1.13 |
| `route_agent_request`, `get_agent_by_name` | §1.14 |
| Sub-agents: `dynamic_agents`, `parent_agent`, `on_before_sub_agent` | §1.15 |
| `get_current_agent()` | §1.16 |
| `session_affinity`, Worker entrypoint, MCP members | §1.17 |
| Extra capabilities: `__init__` + `use()` | §2 |

Not ported: `render()` (unimplemented upstream), upstream's deprecated
`onStateUpdate` and sub-agent methods, and the workflow, email, MCP, and
agent-tool members.
