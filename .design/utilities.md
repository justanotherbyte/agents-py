# Shared utilities

Helpers and conventions shared across the Python port: SDK-wide principles,
method lookup, the FFI boundary, the record-type
convention, typed SQL, and `retry()`. Everything here is decided; runtime
facts come from [platform_verification.md](./platform_verification.md).

Related:
- [core_disposable_store.md](./core_disposable_store.md): `DisposableStore` →
  plain `AsyncExitStack`, and `Emitter`
- [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md): the main
  user of method lookup

---

## 1. Principles

- **Plain functions, not classes**, unless there is state to hold. A helper
  that only needs `obj` takes `obj` as its first argument.
- **Internal by default.** Users pass `self` or a dict to public APIs and
  don't call the lookup or FFI helpers directly. The public exceptions are
  `retry()` (§7) and the SQL helper, exposed as `Agent.sql` (§6).
- **Raise like the standard library does**, and catch only at call sites that
  handle a specific failure (for example, a renamed callback at fire time).
  No blanket try/except.
- **Dedicated exception classes wherever a caller might want to catch the
  error** (decided). A runtime condition a caller can reasonably handle gets
  its own class, e.g. `ReadonlyConnectionError`, `SqlError`,
  `StreamClosedError`, `StreamNotFoundError`, `NonRetryableError`,
  `DuplicateConnectionIdError`, `SubAgentAbortedError`. Pure misuse
  (programming errors such as a wrong argument type, or calling `use()` after
  startup) keeps the built-in types (`TypeError`, `ValueError`,
  `RuntimeError`).
- **Durations are `timedelta | float`, where a number is seconds** (decided,
  SDK-wide). Matches `time.sleep` / `asyncio.sleep` / `asyncio.timeout`.
  Parameter and field names carry no unit suffix (`keep_alive_interval`, not
  `keep_alive_interval_ms`). Values are normalized to `timedelta` internally,
  and to milliseconds where storage or the wire needs them. Upstream's
  duration strings (`"10 seconds"`) aren't ported. Porting note: upstream mixes
  seconds (`schedule`, `scheduleEvery`) and milliseconds (options, retries,
  Tasks durations), so a bare upstream `5000` in a Tasks API is `5` here
  ([agent_api.md](./agent_api.md) §1.7).
- **Timestamps are timezone-aware UTC `datetime`s** (decided, SDK-wide) in
  every user-facing record (`created_at`, `updated_at`, `settled_at`, …).
  Storage keeps upstream's epoch-millisecond integers (schemas unchanged,
  [sql_schemas.md](./sql_schemas.md)); conversion happens at the storage
  boundary. Anything serialized to match upstream's JSON (wire frames,
  observability events) keeps epoch milliseconds in its serialized form.
- **Every SDK exception subclasses `AgentsException`** (decided), so
  `except AgentsException` catches anything the SDK raises on purpose:
  ```python
  class AgentsException(Exception): ...


  class ReadonlyConnectionError(AgentsException): ...


  class SqlError(AgentsException): ...


  class StreamClosedError(AgentsException): ...
  ```
  Same shape as `requests` (`RequestException` base, `*Error` subclasses).
  Exceptions raised by **user code** inside hooks and callbacks propagate
  unchanged; they aren't wrapped.
- **Misuse stays built-in (decided):** programming errors raise plain
  `TypeError` / `ValueError` / `RuntimeError`, not `AgentsException`
  subclasses, following the Python convention that programming errors are
  fixed rather than caught. (Rejected: multiple-inheritance classes like
  `AgentsTypeError(AgentsException, TypeError)`.)

---

## 2. Method lookup: `get_bound_method`, `method_name` (decided, confirmed)

**Why a shared helper:** the same lookup is needed in at least six places, and
getting it slightly wrong in any of them causes subtle bugs:

| Where | What it needs |
| --- | --- |
| `Scheduler.set` / `every` | `str \| Callable` → stored name, and check the name exists |
| `Queue.push` | the same |
| `Scheduler.on_job` / `Queue.on_job` | stored name → bound method when the job fires |
| By-callback inspect and cancel functions (`dequeue_all_by_callback`, `queue_items(callback=)`, …) | `str \| Callable` → name, for filtering |
| Capability constructors given a `target` (`Scheduler(target=self)`) | the method-target path |
| `@callable` RPC over WebSockets | client-sent name → bound method, only if decorated (§2.3) |

### 2.1 `get_bound_method(obj, name)`

```python
def get_bound_method(obj: object, name: str) -> Callable[..., Any]:
    method = getattr(obj, name)  # AttributeError if missing
    if not inspect.ismethod(method) or method.__self__ is not obj:
        raise TypeError(f"{type(obj).__name__}.{name} is not a method")
    return method


meth = get_bound_method(self, "remind")
await meth(payload, schedule)
```

- **It raises, like `getattr`**, so the return type is not `Optional` and the
  result can be called directly. A call site that wants different behavior
  catches the specific error. For example, `on_job` catches `AttributeError`
  and logs `callback remind not found`, as upstream does.
- **Bound methods of `obj` only.** `inspect.ismethod` plus
  `__self__ is obj` rejects:
  - callable attributes that aren't methods (a function stored on the
    instance, or an object with `__call__`);
  - `classmethod`s, where `__self__` is the class;
  - `staticmethod`s, which are plain functions.

  This strictness will matter when RPC passes in names sent by clients.
- **Return type is `Callable[..., Any]`.** A type checker can't work out a
  method's signature from a string (Python has no `keyof`). Callers who want
  the exact type should pass the method reference itself.
- **Objects only.** For a `Mapping`, lookup is just `callbacks[name]`, done
  inside each capability.

### 2.2 `method_name(obj, callback)`

The reverse direction: `str | Callable` → the name that gets stored.

```python
def method_name(obj: object, callback: str | Callable[..., Any]) -> str:
    name = callback if isinstance(callback, str) else callback.__name__
    method = get_bound_method(obj, name)
    if not isinstance(callback, str) and method != callback:
        raise ValueError(f"{callback!r} is not a method of {type(obj).__name__}")
    return name
```

- **Compare with `==`, not `is`.** Bound methods are created fresh on every
  attribute access, so `self.remind is self.remind` is `False`. `==` compares
  the underlying function and `__self__`.
- **It handles the rules in [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md)
  §2.2 with no special cases:** the name must exist, the callable must be a
  bound method of `obj`, and mangled names fail (passing `self.__remind` gives
  the name `"__remind"`, which `getattr` doesn't find), so the mistake shows up
  at the call site.

### 2.3 RPC lookup (decided with `@callable`, [agent_api.md](./agent_api.md) §1.8)

Client-sent names are untrusted, and `getattr(self, "__init__")` returns a
bound method. RPC resolves a name with `get_bound_method(obj, name)` and then
requires the `@callable` marker (`CallableMetadata` stored on the function):
a missing attribute gives upstream's `"Method X does not exist"`, an
undecorated one `"Method X is not callable"`. So dunder methods, private
helpers, and SDK internals are never reachable from a client, whatever native
RPC can reach ([agent_api.md](./agent_api.md) §1.5). Upstream's equivalent is
`assertExposable` (`websockets/callables-target.ts`) plus the `@callable`
registry.

---

## 3. FFI boundary: `agents/_ffi.py` (defined; runtime facts verified, §3.8)

### 3.1 Why

**There's evidence from the earlier port** (`../../agents-python-cloudflare`,
used for reference). The same conversions are repeated across files:
- `to_js(value, dict_converter=Object.fromEntries)` in
  `lifecycle/_runtime.py:241`, `:1199`, `lifecycle/websockets.py:211`, and
  `core/agent.py:2037`;
- the "call `.to_py()` if it has one" pattern in `core/agent.py:165`
  (`_rpc_to_python`, used around 10 times), `websockets.py:204`, and
  `_runtime.py:249`.

Each copy is a chance for a conversion rule to drift.

**It also keeps the package testable under CPython**, which is the strongest
reason. `from js import ...` fails outside Pyodide. If only `_ffi.py` (and the
runtime entry point) import `js` / `pyodide`, then Lifecycle, the job driver,
the scheduling logic, and the protocol import and unit-test normally. The
earlier port needed `tests/_runtime_stubs.py` for this; with one FFI module,
the stubs only have to replace that module.

### 3.2 What the Workers SDK already provides

`workers_runtime_sdk` (1.9.0) publicly exports `python_to_rpc` and
`python_from_rpc` (`workers/rpc.py`). They already handle:

| Conversion | Note |
| --- | --- |
| `None` ↔ JS `null`, recursively | Pyodide's default converts `None` → `undefined`, which disappears when stored in a JS object |
| `dict` → plain JS `Object` (`Object.fromEntries`) | **Verified: the runtime's Pyodide 0.28.2 converts `dict` → JS `Map` by default** ([platform_verification.md](./platform_verification.md) §3.1), so `Object.fromEntries` must be passed explicitly. |
| `datetime` ↔ `Date` | Uses naive `fromtimestamp`; only correct because Workers run in UTC |
| `Exception` ↔ `Error` | |
| `Request` / `Response` / `Blob` / `File` / `FormData` ↔ SDK wrappers | |
| Callables → `create_proxy` | |
| Rejects tuples, `bytearray`, awaitables, `RegExp` | |

**Build on these; don't reimplement them.**

### 3.3 What crosses the boundary in this SDK

| Boundary | Values that need converting |
| --- | --- |
| DO storage: `ctx.storage.get/put`, `sql.exec(query, *params)` | Params (`None`→`null`, `bytes`→`Uint8Array`); result rows → `dict` |
| Hibernation: `acceptWebSocket(ws, tags)`, `getWebSockets(tag)`, `serialize/deserializeAttachment` | Tags list → JS array; the `__pk` attachment `dict` ↔ JS `Object`; JS socket array → list |
| WebSocket messages | `str \| ArrayBuffer` ↔ `str \| bytes` |
| Alarm: `setAlarm(ms)` | epoch milliseconds, `datetime` |
| Native RPC between DOs and facets (sub-agents) | `python_to_rpc` / `python_from_rpc` |
| Callbacks into JS (`setTimeout`, event listeners) | `create_proxy` / `create_once_callable`, which must be destroyed |
| JS errors | `JsException` → Python exceptions; recognizing platform failures (upstream `isPlatformFailure`, DO reset errors) |

### 3.4 Surface

```python
# agents/_ffi.py: the only module (besides the runtime entry point) that imports `js` / `pyodide`


def py_to_js(
    value: Any,
) -> Any: ...  # structured-clone values: storage, sql params, attachments (§3.5)
def js_to_py(value: Any) -> Any: ...  # the reverse (§3.6)
def to_rpc(
    value: Any,
) -> Any: ...  # wraps workers.python_to_rpc (native DO RPC, facets)
def from_rpc(value: Any) -> Any: ...  # wraps workers.python_from_rpc


@contextmanager
def proxies() -> Iterator[
    ProxyScope
]: ...  # collects create_proxy handles and destroys them on exit
```

**Names don't shadow the platform's.** `to_js` is Pyodide's own conversion
(`pyodide.ffi.to_js`); our functions are `py_to_js` / `js_to_py` and build on
it. `python_to_rpc` / `python_from_rpc` come from the Workers SDK
(`workers/rpc.py`) and are wrapped, not reimplemented.

**`py_to_js` and `to_rpc` are separate on purpose.** `python_to_rpc` is designed
for RPC: it wraps callables in proxies and converts `Request`/`Response`.
Storage and hibernation attachments use structured clone, which can't hold
callables or proxies, so storage conversion has its own stricter rules.

### 3.5 `py_to_js`: the definition

| Python | JS | How |
| --- | --- | --- |
| `None` | `null` | Replaced with `pyodide.ffi.jsnull` before conversion. Pyodide's implicit conversion would give `undefined`, which disappears from JS objects. |
| `bool`, `int`, `float`, `str` | same | Pyodide's implicit conversion |
| `dict` with `str` keys | plain `Object` | `dict_converter=js.Object.fromEntries`, passed explicitly (the runtime's Pyodide 0.28.2 defaults to `Map`, [platform_verification.md](./platform_verification.md) §3.1). A non-`str` key raises `TypeError`. |
| `list` | `Array` | |
| `tuple`, `set`, `frozenset` | **`TypeError`** | They wouldn't read back as the same type; matches the Workers SDK's RPC rule for tuples |
| `bytes`, `bytearray`, `memoryview` | `Uint8Array` (a copy; verified) | |
| `int` beyond ±2^53 | `BigInt` | Pyodide's implicit conversion. Round-trips in KV storage; **SQL rejects it** ([platform_verification.md](./platform_verification.md) §3.3), so the SQL helper raises `ValueError` first. |
| timezone-aware `datetime` | `Date` | `js.Date.new(value.timestamp() * 1000)`. A naive `datetime` raises `TypeError`. |
| anything else | **`TypeError`** | `create_pyproxies=False`, so a `PyProxy` can never end up in storage |

Implementation outline:

```python
def py_to_js(value):
    return to_js(
        _none_to_jsnull(value),
        dict_converter=Object.fromEntries,
        create_pyproxies=False,
        default_converter=_convert_bytes_and_datetimes,
    )
```

### 3.6 `js_to_py`: the definition

| JS | Python |
| --- | --- |
| `null`, `undefined` | `None` |
| boolean, number, string | `bool`, `int` / `float`, `str` |
| plain `Object` | `dict` |
| `Array` | `list` |
| `ArrayBuffer`, `Uint8Array` (and other typed arrays) | `bytes` (`.to_bytes()`; `.to_py()` would give a `memoryview`) |
| `BigInt` | `int` |
| `Date` | timezone-aware UTC `datetime` (`datetime.fromtimestamp(ms / 1000, UTC)`) |
| anything else | `TypeError` |

### 3.7 Rules

1. **Internal, and user code never sees JS values.** Conversion happens where
   the SDK meets the runtime. Capabilities and user callbacks only get `dict`,
   `list`, `bytes`, `datetime`, and `None`, never a `JsProxy`.
2. **Only `_ffi.py` imports `js` / `pyodide`** (plus the runtime entry point).
3. **Every conversion rule lives in §3.5–3.6.** No other module calls
   `pyodide.ffi.to_js` directly.

### 3.8 Verified runtime facts ([platform_verification.md](./platform_verification.md) §3)

- `to_js(dict)` defaults to a JS `Map`; `Object.fromEntries` must be passed.
- Raw storage: `put(key, None)` fails (`undefined`); `dict`s and lists come back
  as `JsProxy`; `bytes` as `Uint8Array`.
- The SDK's wrapped storage round-trips Python values (`bytes` come back as
  `memoryview`) and returns SQL rows as `list[dict]`, but costs roughly 2–6×
  raw per call.
- **Decided: hot paths do what's efficient:** SDK internals use raw JS
  storage with `py_to_js` / `js_to_py` wherever per-call cost matters (job
  queue, Streams appends, Sessions writes, the SQL helper); cold paths may use
  the wrapper.
- `py_to_js` stays separate from `python_to_rpc` (structured clone vs. RPC
  rules, §3.4).
- **Production** ([platform_verification.md](./platform_verification.md) §7.3): the
  wrapper costs roughly 1.3–1.9× raw-plus-conversion; leaving results as
  `JsProxy` is slower than converting them immediately, so `js_to_py` runs
  right away and no proxies are kept.
- **Never compute timestamps at import:** `Date.now()` is 0 at module load in
  production, and clocks don't advance during CPU-only work (§7.1 there).

---

## 4. No `MISSING` sentinel (decided)

A library-wide `MISSING` sentinel was planned for `StreamingResponse.end`, the
one API where "not passed" and `None` differed on the wire (`undefined` vs.
`null`). Dropped: `None` means "no result" there instead
([agent_api.md](./agent_api.md) §1.8, "Final results"), and every option
elsewhere treats `None` as "use the default". The SDK defines no sentinel;
`dataclasses.MISSING` (used by `wire()`, §5) is unrelated.

---

## 5. Record types: dataclasses (decided)

Every typed record in the SDK, such as `UIMessageChunk` / `UIMessagePart` /
`UIMessage`, the fiber types, `RetryOptions`, `TaskStepAttempt`, and the
Sessions results, is a dataclass:

```python
@dataclass(slots=True, kw_only=True)
class TextDelta:
    id: str
    delta: str
    provider_metadata: dict[str, Any] | None = None
    type: ClassVar[str] = "text-delta"  # wire discriminator, a class constant
```

- **`slots=True`:** smaller objects, faster attribute access, and no accidental
  new attributes.
- **`kw_only=True`:** construct with keywords only (`TextDelta(id=…, delta=…)`).
  Field order isn't part of the API, and defaults can sit next to required
  fields.
- **Not `frozen`.** Frozen dataclasses construct noticeably slower (measured
  ~250–550 ns vs ~140 ns on CPython 3.14), and chunks are created per token
  while streaming. Immutability is a convention: the SDK never mutates a
  record it receives. Use `dataclasses.replace()` for modified copies.
- **Mutable defaults** use `field(default_factory=…)`.
- **Wire names:** where the wire format is camelCase, a field declares its wire
  name in field metadata (`field(default=None, metadata={"wire":
  "providerMetadata"})`), so one generic `to_wire` / `from_wire` built on
  `dataclasses.fields()` covers every class. No per-class name maps.
- **The `wire()` helper** keeps those declarations short, and has to support
  **required** fields too, because `field(default=…)` alone makes a field
  optional:
  ```python
  def wire(name: str, default: Any = MISSING) -> Any:
      # MISSING here is dataclasses.MISSING: no default, so the field stays required
      return field(default=default, metadata={"wire": name})


  tool_call_id: str = wire("toolCallId")  # required
  provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)  # optional
  ```
  (This is `dataclasses.MISSING`; the SDK has no sentinel of its own, §4.)
- **Serialization omits optional fields whose value is `None`**, because the
  wire format leaves optional fields out rather than sending `null`
  ([agents_wire_protocol.md](./agents_wire_protocol.md) §1). Required fields
  are always written, even when `None` is a legitimate JSON value (a tool's
  `output: null`, a data chunk's `data: null`).

**Exception: small immutable value pairs may be `NamedTuple`s** when tuple
behavior is natural, e.g. `AgentPathStep(class_name, name)` (unpacking,
hashing, paths as tuples of steps; [agent_api.md](./agent_api.md) §1.5) and
`AgentRoute(class_name, name)` ([agent_api.md](./agent_api.md) §1.14).
**Exception: `frozen=True`** for objects shared across instances, e.g.
`AgentOptions` ([agent_api.md](./agent_api.md) §1.7).

**Alternatives considered:**
- **Plain classes with `__slots__` and a small `Record` base** (deriving
  `__repr__` / `__eq__` / `__match_args__`): about 10× faster to *define*
  (~1.5 ms vs ~12–17 ms for 40 classes on CPython, a one-time import cost), but
  every field is written twice or three times and could drift. Rejected: the
  import cost is one-time per isolate (and possibly hidden by Python Workers'
  memory snapshot), while dataclasses' maintainability benefits are certain.
- **`NamedTuple`:** rejected because tuple behavior leaks (indexing, unpacking,
  equality with plain tuples).

---

## 6. Typed SQL (decided)

The Workers SDK's `ctx.storage.sql.exec(...)` is untyped. One typed helper,
used by `Agent.sql` and by the SDK's own capabilities.

```python
type SqlValue = None | int | float | str | bytes


class Sql:
    @overload
    def __call__(
        self, query: LiteralString, *params: SqlValue
    ) -> list[dict[str, Any]]: ...
    @overload
    def __call__[R: Mapping[str, Any]](
        self, query: LiteralString, *params: SqlValue, row: type[R]
    ) -> list[R]: ...


class User(TypedDict):
    id: str
    age: int


rows = self.sql(
    "SELECT id, age FROM users WHERE id = ?", user_id
)  # list[dict[str, Any]]
users = self.sql(
    "SELECT id, age FROM users WHERE id = ?", user_id, row=User
)  # list[User]
```

- **`?` placeholders with separate parameters**, the standard library
  `sqlite3` convention.
- **Replaces upstream's tagged template.** `` this.sql`SELECT … ${x}` `` builds
  a parameterized query from a template; Python 3.12 and 3.13 have no
  equivalent, so parameters are passed separately (`?` placeholders). Python
  3.14's template strings (PEP 750, `t"…"`) could restore the upstream form
  later, but only once the SDK's minimum version is 3.14 (it supports 3.12,
  [code_semantics.md](./code_semantics.md) §1).
- **`query: LiteralString`** (PEP 675) rejects queries built from runtime
  strings, e.g. an f-string with user input, steering users to parameters.
  Checked: pyright reports `sql(f"… '{user_id}'")` as an error; **mypy doesn't
  enforce `LiteralString`** (it treats it as `str`), so this protection is
  pyright-only.
- **Parameters are typed** as `SqlValue`; both pyright and mypy reject e.g. an
  `object()` argument.
- **Runtime details** ([platform_verification.md](./platform_verification.md) §3.3): `bool` params are converted to `int`
  (the wrapper would store the text `'true'`); `BLOB` results become `bytes`
  (the wrapper returns `memoryview`); integer params are limited to ±2^53;
  `SqlError` wraps the `JsException` and keeps SQLite's message.
- **Rows** are `dict[str, Any]` by default, or typed with `row=` (a
  `TypedDict` or any mapping type); the type is a static annotation only, with
  no runtime validation.
- **Synchronous**, like `sql.exec`.
- **Errors:** a failing query raises `SqlError` carrying the query (upstream
  `sql-error.ts`).
- **Implementation** uses raw JS storage through the FFI module (§3, §3.8:
  hot path).
- Possible later additions: a single-row form (`one()` / `first()`), and an
  iterator for large result sets.

Checked in the scratchpad (`sqltyping.py`) with pyright and mypy.

---

## 7. `retry()` (decided)

Upstream's `this.retry` is an `Agent` method, but it doesn't use the agent
except to read `static options.retry` for defaults. In Python it's a
**module-level utility**, usable anywhere (agents, plain DOs, Workers):

**Upstream** (`agents/src/index.ts:3091`, `retries.ts`, `docs/agents/retries.md`):
- `this.retry(fn, { maxAttempts?, baseDelayMs?, maxDelayMs?, shouldRetry? })`
  calls `fn(attempt)` (1-based) until it succeeds, waiting a "full jitter"
  backoff between attempts: a random delay in `[0, min(2^attempt × base,
  max))`. It rethrows the last error when attempts run out, or immediately
  when `shouldRetry(err, nextAttempt)` returns false. Retries every error by
  default.
- **In-memory only:** the waits are `setTimeout`s inside the current
  invocation. Nothing is persisted; if the DO is evicted mid-retry, the retry
  is gone. (Durable retries are `schedule()` / `queue()` / Tasks.)
- Unset options fall back to the class's `static options.retry`, then 3 /
  100 ms / 3000 ms; options are validated eagerly.
- **A user-facing helper only.** No SDK code calls `this.retry`. Internally
  the SDK uses the same engine (`tryN`) for schedule and queue dispatch
  retries and the Lifecycle job driver.

**Decided:**

```python
async def retry[T](
    fn: Callable[[int], Awaitable[T]],  # called with the 1-based attempt number
    *,
    max_attempts: int = 3,
    base_delay: Duration = 0.1,
    max_delay: Duration = 3,
    should_retry: Callable[[Exception, int], bool] | None = None,
) -> T: ...


async def fetch_data(url: str, attempt: int) -> dict: ...


data = await retry(partial(fetch_data, url))
```

- `fn` follows the `step.do` convention
  ([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.5): any
  callable returning an awaitable, called once per attempt; docs use
  `async def` / `partial`, never lambdas; passing a coroutine is a
  `TypeError`. It always receives the attempt number.
- **Defaults are the SDK's** (`RetryOptions()`: 3 / 0.1 s / 3 s). Since it's no
  longer an agent method, `AgentOptions.retry` doesn't apply to it; that option
  now only sets defaults for `schedule()` / `schedule_every()` / `queue()`.
- Invalid values raise `ValueError` eagerly (misuse).
- **Only `Exception`s are retried:** `asyncio.CancelledError` and other
  `BaseException`s propagate immediately, so cancelling the caller cancels the
  retry, including during the backoff sleep.
- `should_retry` is a synchronous predicate, as upstream.
- The same engine (upstream `tryN`) drives schedule/queue dispatch retries
  and the Lifecycle job driver internally.
- **Options are flat keyword arguments** (decided), not a `RetryOptions`
  object as `schedule()` / `queue()` take: retrying is the function's whole
  job, so its settings read best inline
  (`await retry(fn, max_attempts=5, base_delay=0.5)`).

## 8. Summary

| Utility | Status | Where |
| --- | --- | --- |
| `get_bound_method(obj, name)` | Decided | `agents/core` (internal) |
| `method_name(obj, callback)` | Decided | `agents/core` (internal) |
| RPC lookup (`@callable` marker required) | Decided (§2.3) | `agents/core` (internal) |
| `agents/_ffi.py`: `py_to_js`, `js_to_py`, `to_rpc`, `from_rpc`, `proxies()` | Defined; runtime facts verified (§3.8) | `agents/_ffi.py` |
| `MISSING` sentinel | Dropped (§4): `None` means "no result" | |
| Record types as `@dataclass(slots=True, kw_only=True)` | Decided | Convention, SDK-wide |
| Typed SQL helper (`sql(query, *params, row=…)`) | Decided (§6) | `agents/core` (internal), exposed as `Agent.sql` |
| `DisposableStore` → `AsyncExitStack` | Decided | [core_disposable_store.md](./core_disposable_store.md) |
| `Emitter` (`subscribe` / `fire` / `fire_async`) | Decided: ported | [core_disposable_store.md](./core_disposable_store.md) §5.4 |
| `retry(fn, ...)` | Decided: SDK utility, keyword options (§7) | `agents` (public) |
