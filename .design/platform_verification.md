# Platform verification results

Answers to the "to verify" items across the design docs, measured on the real
Python Workers runtime. Each finding links back to the doc it affects.

**How:** a probe Worker in [`verify/`](../verify) (`src/entry.py`, plus
`slow_server.py` for the fetch test), run locally with `pywrangler dev`
(wrangler 4.147.0, workers-py 1.17.6, workers-runtime-sdk 1.9.2) on
2026-10-04. Raw outputs: `verify/results/` (git-ignored). Rerun with
`uv run python slow_server.py results/slow_server.log &` then
`uv run pywrangler dev --port 8788` and `curl localhost:8788/run` (all probes),
`/probe?name=<probe>` (one probe, e.g. `fetch_cancel_libs`), or
`/bench?mode=wrapped|raw|raw_to_py&n=2000`. The Worker vendors the SDK from
`..`; after SDK changes run `uv run pywrangler sync --force` (the version
number doesn't change, so a plain sync keeps the stale copy).

**Runtime:** Python 3.13.2, **Pyodide 0.28.2**, compatibility date
2026-07-20. `ctx.id.name` and `ctx.facets` are available. The SDK also
supports Python 3.12 ([code_semantics.md](./code_semantics.md) §1); these
results were measured on 3.13 / Pyodide 0.28.2, and FFI details (e.g.
`to_js` defaults) can differ on runtimes with an older Pyodide.

**Production:** the same Worker was deployed on 2026-10-05
(`agents-py-platform-verify`, account "Reachvishm@gmail.com's Account",
workers.dev) and the suite re-run there; see §7. **It stays deployed** for
future checks (`https://agents-py-platform-verify.reachvishm8605.workers.dev`;
redeploy from `verify/` with `uv run pywrangler deploy`). **Every local finding in §2–3
held in production.**

---

## 1. Summary

| # | Question | Result | Affects |
| --- | --- | --- | --- |
| 1 | Hibernation API from Python | **Works**, with conversions (§2.1) | [agent_api.md](./agent_api.md) §1.9 |
| 2 | Attachment size limit | **16,384 bytes** per socket (§2.1) | agent_api §1.9 |
| 3 | `JsProxy` equality for `Connection` | `==` is JS identity; **`JsProxy` is unhashable** (§2.2) | agent_api §1.9 |
| 4 | `ctx.abort` from Python | **The SDK wraps it**: defers the JS abort, raises `DurableObjectAbort` (§2.3) | agent_api §1.12 |
| 5 | Listing env bindings | Only via the SDK's private `env._env` (§2.4) | agent_api §1.14 |
| 6 | Transient-error flags on JS errors | **Readable**: `exc.retryable`, `exc.overloaded` (§2.5) | agent_api §1.14 |
| 7 | Native RPC to non-public names | **Reachable, including `_private` names** (§2.6) | agent_api §1.5, §1.14 |
| 8 | `ContextVar` in JS-invoked callbacks | **Lost** in `setTimeout` / `queueMicrotask`; kept in `Promise.then`, `call_soon`, tasks (§2.7) | agent_api §1.16 |
| 9 | Cancelling a task aborts a `fetch`? | **Depends on the client:** `pyfetch` / `workers.fetch` yes (headers); **httpx never**; raw `js.fetch` and chunk-by-chunk body reads no; aiohttp didn't work (§2.8) | [scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.6, [fibers_api.md](./fibers_api.md) §3 |
| 10 | FFI: `to_js(dict)` default | **`Map`**, not `Object` (Pyodide 0.28.2) (§3.1) | [utilities.md](./utilities.md) §3 |
| 11 | Storage KV through the SDK wrapper | Round-trips everything; `bytes` come back as `memoryview` (§3.2) | utilities §3.8 |
| 12 | `sql.exec` through the wrapper | Works; `bool` params stored as text `'true'`; ints over 2^53 fail (§3.3) | utilities §3.8, §6 |
| 13 | `transactionSync` with a Python callback | **Works**; exceptions keep their Python type and roll back (§3.4) | utilities §6 |
| 15 | `blockConcurrencyWhile` with a Python callback | **Works and blocks**; an exception inside breaks the object, so catch inside and re-raise outside (§2.9) | [lifecycle_capabilities.md](./lifecycle_capabilities.md) |
| 14 | Per-call cost of the wrapper | Roughly **2–6× slower** than raw storage per SQL call (§3.5) | utilities §3.8 |

---

## 2. Runtime behavior

### 2.1 Hibernation API

- `ctx.acceptWebSocket(server, to_js(["room:a", "user:1"]))`,
  `ctx.getWebSockets()`, `ctx.getWebSockets(tag)`, `ctx.getTags(ws)` all work.
  Results are `JsProxy` arrays (iterate with `list(...)`).
- A Python `webSocketMessage(self, ws, message)` method receives messages.
- **Attachments must be converted to JS first.** `serializeAttachment(dict)`
  fails with `DataCloneError`; `serializeAttachment(to_js(d,
  dict_converter=js.Object.fromEntries))` works, and
  `deserializeAttachment().to_py()` returns the nested `dict` with Python
  types. `None` round-trips.
- **Size limit: 16,384 bytes** of serialized attachment ("A WebSocket
  'attachment' cannot be larger than 16384 bytes."). With the SDK's own
  connection metadata in the same attachment, per-connection state must stay
  well under 16 KiB. Exceeding it raises a `JsException` from
  `serializeAttachment`.

### 2.2 `JsProxy` equality (for `Connection.__eq__` / `__hash__`)

- Two proxies of the same `WebSocket` (two `getWebSockets()` calls, or the
  `server` end vs. the looked-up one): `a == b` is **True**, `a is b` is
  **False**. Different sockets compare unequal. So `==` is JS identity.
- **`JsProxy` is unhashable** (`TypeError: unhashable type`).
- `js_id` is equal for proxies of the same object, and was stable across calls
  while a proxy was kept alive; it isn't usable after hibernation anyway (new
  JS objects).
- **Design consequence:** `Connection.__eq__` compares the wrapped sockets
  with `==`; `Connection.__hash__` hashes the connection **id**. Equal
  connections always share an id, so this satisfies the hash contract; two
  sockets sharing an id only collide in the hash table.

### 2.3 `ctx.abort`

- The SDK's `DurableObjectContext.abort(reason)` (`workers/entrypoints.py`)
  **already defers the real abort**: it queues `ctx.abort(reason)` in a JS
  microtask (so Python's task cleanup can finish), then raises
  `DurableObjectAbort`, a `BaseException`, so `except Exception` can't
  swallow it.
- Observed: the RPC caller gets `JsException: Error: <reason>`; the next call
  reaches a **new instance** (constructor ran again).
- Deferring further with `loop.call_soon` doesn't help: the caller still gets
  the abort error, not the method's return value.
- **Design consequence (§1.12):** `destroy()` calls the SDK's
  `self.ctx.abort("destroyed")` directly; no extra deferral. Callers always see
  an error, so "fire-and-forget" is the documented contract on top-level agents
  too. Not tested: upstream's `abortWithoutAlarmRetry` variant.

### 2.4 Listing env bindings (`route_agent_request`)

- `self.env` is the SDK's `_EnvWrapper`. `dir(env)` lists only bindings
  already accessed (they're cached on first `getattr`), so it can't enumerate.
- `js.Object.keys(env._env)` lists every binding name (vars too).
  `getattr(env._env, key)` is the raw JS binding: DO namespaces are
  `[object DurableObjectNamespace]` and have `idFromName`.
  `getattr(env, key)` returns the SDK's `_DurableObjectNamespaceWrapper`.
- **Design consequence (§1.14):** binding discovery uses the SDK's private
  `_env` attribute (and checks the JS type). That's a dependency on an SDK
  internal: if it changes, discovery breaks. Recorded as a known risk.

### 2.5 JS error flags

- A JS error awaited from Python raises `JsException`. Its JS properties are
  readable **directly** (`exc.retryable`, `exc.overloaded`, `exc.message`,
  `exc.name`) and through `exc.js_error`.
- An exception raised by a Python method in another DO reaches the RPC caller
  as a `JsException` whose message contains the Python traceback; it has no
  `retryable` property.
- **Design consequence (§1.14):** the routing-retry predicate is
  `getattr(exc, "retryable", False) is True and not getattr(exc, "overloaded", False)`.

### 2.6 Native RPC reaches any attribute name

- `__unsafe_ensureInitialized` and `camelCaseViaSetattr` (attached with
  `setattr` after class creation) **and `_private_method`** were all callable
  over native RPC from the Worker.
- So the earlier reading of workerd's `collect_methods` (only public names)
  only describes the prototype declaration; **dispatch reaches any attribute,
  underscore-prefixed or not**. This matches TypeScript DOs, where any
  prototype method is RPC-callable, and upstream's `_cf_*` internal RPC
  methods rely on it.
- **Design consequence:** the SDK's internal RPC entry points
  (`get_agent_by_name`'s startup call, facet bridges) can keep upstream's
  names. Docs should say that **every method**, not only public ones, is
  callable by any Worker holding the binding.

### 2.7 `ContextVar` propagation

With a `ContextVar` set in the calling code:

| Callback registered via | Sees the value? |
| --- | --- |
| `asyncio.create_task` | yes |
| `loop.call_soon` | yes |
| `js.Promise.resolve().then(cb)` | yes |
| `js.setTimeout(cb, 0)` | **no** (default `None`) |
| `js.queueMicrotask(cb)` | **no** |

**Design consequence (§1.16):** `get_current_agent()` returns `None` inside
callbacks the JS runtime calls directly (timers, microtasks, event
listeners). Documented as a "context lost" case, with the same fix as
upstream: call a method on the agent (auto-wrapped) from the callback.

### 2.8 `fetch` cancellation

Probes: a local server holds each request open for 5 s (or streams a body in
10 chunks over 5 s) and logs when the client disconnects. The Python task is
cancelled after 0.5 s (1.5 s for body reads).

| Client code | Cancelled while… | Request aborted? |
| --- | --- | --- |
| `js.fetch(url)` (raw JS) | waiting for headers | **no**: ran the full 5 s |
| `js.fetch` with an `AbortController`, `controller.abort()` | waiting for headers | yes (0.5 s) |
| `workers.fetch(url)` (Workers SDK) | waiting for headers | **yes** (0.5 s) |
| `pyodide.http.pyfetch(url)` | waiting for headers | **yes** (0.5 s) |
| `pyfetch` → `await resp.string()` | reading the body | **yes** |
| `workers.fetch` → `await resp.text()` | reading the body | **no**: body streamed to the end |
| `js.fetch` or `pyfetch` → `resp.body.getReader()` loop | reading the body | **no** |
| JS reader loop with `reader.cancel()` in a `finally` | reading the body | **yes** (≈0.5 s after cancel) |
| **`httpx` 0.28.1** `AsyncClient.get()` | waiting for headers | **no**: ran the full 5 s |
| **`httpx`** `client.stream()` → `aiter_bytes()` | reading the body | **no** |
| **`httpx`** `break` out of `aiter_bytes()` and leave the `client.stream()` block | (no cancellation) | **no**: the body streamed to the end |
| **`aiohttp` 3.11.13** | any | **fails**: `NotImplementedError` from `loop.sock_connect` (not routed through `fetch` in this setup) |

**httpx and aiohttp** (the clients real code and provider SDKs use): the
runtime patches httpx to send requests through JS `fetch` (requests work),
but **the patched transport never aborts**: not on cancellation, not when the
response is closed early. aiohttp 3.11.13 (installed by `pywrangler sync`)
tried a raw socket and failed; whether a specific aiohttp version or a
compatibility flag enables the runtime's aiohttp support wasn't determined.

**Async-generator cleanup after an inline `break`:** a generator like
`iter_body`, consumed inline (`async for … in gen(): break`), ran its
`finally` immediately and the connection closed, even though Pyodide's loop
installs **no** async-generator hooks (`sys.get_asyncgen_hooks()` is
`(None, None)`). CPython finalizes the unreferenced generator on the spot;
this works because the `finally` doesn't `await`.

So **Pyodide's `pyfetch` ties its request to task cancellation** (it holds an
`AbortController` internally and aborts on `CancelledError`), both before the
headers and inside its own body methods. The Workers SDK's `fetch` goes
through `pyfetch`, so it's covered until the headers arrive, but its
`Response.text()` / `.json()` read through the JS response and aren't. Raw
`js.fetch`, and **reading a body stream chunk by chunk through the JS
reader** (how you consume a streaming LLM response), are not aborted.

**Design consequence** (scheduling §2.6, fibers §3, chat-turn cancel): when the
SDK cancels a step, fiber, or chat turn, the Python code always stops at its
`await`. Whether the outbound request stops too depends on how the user made
it. The gap that matters most is streaming bodies: a cancelled chat turn that
was streaming tokens from a model provider keeps the connection open (and the
provider keeps generating, and billing) until the stream ends. See §6.

### 2.9 `blockConcurrencyWhile` with a Python callback (2026-10-05, local)

Probe endpoints `/bcw` and `/bcw-raise` in `verify/src/entry.py`.

- **Works with a Python `async def`** passed through `create_proxy`: the
  runtime awaits it and `blockConcurrencyWhile` returns its result.
- **It blocks concurrent events:** an RPC call sent 50 ms into a 300 ms
  callback ran only after the callback finished.
- **An exception raised inside the callback breaks the object:** it can't be
  caught around the `await` inside the DO, the caller gets the error, and
  **every later call to that object fails with the same error** (the input
  gate stays broken).
- **Catching inside the callback and re-raising outside** keeps the object
  healthy (no reset) and preserves the original exception object.
- **Design consequence (Lifecycle startup):** as upstream
  (`durable-object-lifecycle.ts` `#runStartup`), startup runs inside
  `blockConcurrencyWhile`, catches *everything* inside the callback, and
  re-raises after it returns. The earlier port (`agents-python-cloudflare`)
  didn't use `blockConcurrencyWhile` at all.

---

## 3. FFI and storage

### 3.1 `to_js`

- **`to_js(dict)` gives a JS `Map` by default** on Pyodide 0.28.2. Passing
  `dict_converter=js.Object.fromEntries` gives a plain `Object`. (utilities §3
  had assumed Pyodide 0.29's behavior.)
- `bytes` / `bytearray` / `memoryview` → `Uint8Array`, **copied** (writing to
  the JS array doesn't change a `bytearray`).
- `Uint8Array.to_py()` → `memoryview`; `.to_bytes()` → `bytes`.
- Python ints above 2^53 → JS `BigInt`.

### 3.2 Storage KV (`ctx.storage.get` / `put`)

| Value | Through the SDK wrapper | Raw JS storage + `to_js(…, Object.fromEntries)` |
| --- | --- | --- |
| `int`, `float`, `str`, `bool` | round-trips | round-trips |
| `2**60` | round-trips as `int` | round-trips as `int` |
| `None` | round-trips | **fails**: `put() called with undefined value` |
| `list`, `dict`, nested, `{}` | round-trips as Python | comes back as `JsProxy` (needs `.to_py()`) |
| `bytes` | comes back as **`memoryview`** | comes back as `Uint8Array` proxy |

`get([keys])` through the wrapper returns a `dict` of the found keys.

### 3.3 SQL (`ctx.storage.sql.exec`)

Through the wrapper:
- Params `None`, `int`, `float`, `str`, `bytes` work.
- **`bool` params are stored as the text `'true'`**, not `1`.
- **Ints above 2^53 fail** (`TypeError: Cannot convert a BigInt value to a
  number`).
- Cursor: `.toArray()` → `list[dict]` with Python values; iteration yields
  `dict`s; `.one()`, `.raw()` (lists), `.rowsWritten`, `.columnNames` work.
  `BLOB` columns come back as **`memoryview`**.
- Errors are `JsException` with SQLite's message, e.g. `Error: near "SELEC":
  syntax error at offset 0: SQLITE_ERROR`.

Raw JS `sql.exec`: params `None` and `to_js(bytes)` work; `.toArray()` returns
a JS array of objects (`.to_py()` → `list[dict]`).

**Design consequences** (utilities §6, `Sql`):
- convert `bool` params to `int` before calling `exec` (`bool` is an `int`
  subclass, so type checkers accept it as a `SqlValue`);
- convert `memoryview` results to `bytes`;
- `SqlError` wraps the `JsException`, keeping SQLite's message;
- integer params are limited to ±2^53 (documented).

### 3.4 `transactionSync`

`ctx.storage.transactionSync(fn)` accepts a plain Python function through the
wrapper (and a `create_proxy` through raw storage), returns its result, and
**an exception raised inside keeps its Python type** (`ValueError`) and rolls
the transaction back.

### 3.5 Per-call cost

2,000 `INSERT`s + 2,000 `SELECT … toArray()` per run, in one DO, timed
in-isolate (local clocks do advance):

| Mode | Runs (ms) |
| --- | --- |
| SDK wrapper | 363, 501, 605, 605 |
| Raw JS `sql`, rows left as JS | 92, 157, 237, 299 |
| Raw JS `sql`, rows `.to_py()` | 98, 165 |

Noisy (the table grows between runs), but consistent: **the wrapper costs
roughly 2–6× raw per call**, about 90–150 µs vs. 25–80 µs per statement
locally.

---

## 4. Answers recorded elsewhere

- DO facets (`ctx.facets`) work on Python Workers (confirmed earlier).
- `loop.call_soon` and `js.queueMicrotask` are available.

## 5. Needed a deployed Worker (answered in §7)

- How Workers Logs indexes JSON log lines ([observability.md](./observability.md) §6):
  JSON strings are parsed into fields; Python `logging` lands at level
  `error` (§7.4).
- The memory snapshot: §7.2.
- Production timing and per-call cost: §7.1, §7.3.
- Upstream's `abortWithoutAlarmRetry`: §7.5.

## 6. Decisions these results raise

1. **Outbound requests on cancellation** (§2.8): **decided: document only**
   for now. User docs state that cancelling a step, fiber, or chat turn stops
   the Python code at its `await`, but requests made with httpx (and so the
   provider SDKs built on it), raw `js.fetch`, or chunk-by-chunk body reads
   keep running until they finish; `pyfetch` / `workers.fetch` abort while
   waiting for headers. Deferred, not rejected: an SDK httpx transport that
   aborts on cancellation (`FetchTransport`), a report to Cloudflare about
   the runtime's httpx patch, and checking aiohttp with a working setup.
2. **Wrapped vs. raw storage for SDK internals** (§3.5): **decided: hot paths
   do what's efficient**, i.e. raw JS storage with the SDK's own conversions
   (`None`, `dict` via `Object.fromEntries`, `bytes`, `bool` → `int`) where
   per-call cost matters (job queue, Streams appends, Sessions writes, SQL
   helper). Cold paths may use the wrapper.

---

## 7. Production results (2026-10-05)

Deployed with `pywrangler deploy` (Worker startup time reported at deploy:
1,195 ms, then 1,171 ms on redeploy). Endpoints added for production:
`/run-prod` (the suite minus the fetch probes, which need the local slow
server), `/snapshot`, `/logs`, `/alarm-test`, `/alarm-results`.

### 7.1 The suite

All 15 probes passed and matched the local results exactly, apart from
expected differences (no KV binding in production; line numbers in a
traceback). Attachment limit, `JsProxy` behavior, `ctx.abort`, `ContextVar`
propagation, FFI and storage conversions, SQL quirks, `transactionSync`,
binding discovery, and native-RPC reachability all hold.

**Clocks:** in production, `time.perf_counter()` and `Date.now()` don't
advance during pure CPU work (every CPU-only probe reported 0 ms), only
across I/O (Spectre mitigation). And **`Date.now()` at module load is `0`**
(the Unix epoch). Consequences: never compute a timestamp at import time;
durations measured in-isolate (e.g. upstream's `job:slow_dispatch`) only
reflect I/O waits.

### 7.2 Memory snapshot

- Module-level markers can't show it: `Date.now()` is 0 at module load in
  every case (§7.1).
- Timing: the first request after a redeploy took 0.64 s end to end (≈0.5 s
  over the 0.135 s round trip), well under the 1.17 s startup time measured at
  deploy, which includes running top-level code and `import httpx`. Warm
  requests: 0.13 s. A first request to a **new Durable Object** took
  0.95–1.04 s (vs 0.11–0.13 s warm), which also includes creating the object.
- **Conclusion:** consistent with fresh isolates restoring a deploy-time
  memory snapshot instead of re-running imports, but not provable from inside
  the Worker. Top-level imports stay the right place for heavy imports.

### 7.3 Per-call cost in production

4,000 statements per run (2,000 `INSERT` + 2,000 `SELECT … toArray()`), wall
time from outside, round trip ≈0.1 s included. Rounds 2–5:

| Mode | Wall time (s) |
| --- | --- |
| SDK wrapper | 1.05, 0.97, 1.41, 1.65 |
| Raw JS `sql`, rows left as `JsProxy` | 0.71, 1.02, 1.17, 1.30 |
| Raw JS `sql`, rows converted with `.to_py()` | 0.47, 0.77, 0.76, 0.87 |

- The wrapper is roughly **1.3–1.9×** raw-plus-conversion in production (2–6×
  locally): still slower, by less.
- **Leaving rows as `JsProxy` is slower than converting them right away**
  (proxies cost more to keep than to convert). SDK internals should convert
  results immediately and not hold proxies.
- The "hot paths do what's efficient" decision (§6) stands.

### 7.4 Workers Logs: what arrives

From `wrangler tail` (the same events Workers Logs stores):

| Emitted with | Level | Message |
| --- | --- | --- |
| `logging.getLogger(...).warning(json_line)` | **`error`** | a **string** |
| `logging.getLogger(...).debug(json_line)` | — | **not emitted** (Python's default threshold is `WARNING`) |
| `print(json_line)` | `log` | a string |
| `js.console.log(<JS object>)` | `log` | a **structured object** |
| `js.console.log(json_line)` | `log` | a string |

- Python `logging` writes to stderr, which the runtime records at level
  `error` **whatever the Python level**.
- So the observability design's default sink (`agents.events` at `DEBUG`) is
  invisible unless the app configures logging, and when visible, its lines are
  strings at `error` level.
- **Dashboard check (done):** Workers Logs **parses a JSON string message
  into top-level, searchable fields**, as it does a logged object. The
  `logging.warning` line came back with `type`, `marker`, `via`, `n`, and
  `nested` as fields (no `$metadata.message`), and with `"level": "error"`
  and `"$metadata.error": "error"`.

### 7.4.1 Log levels through the JS console (`/log-levels`, marker `levels-1791156719`)

| Written with (from Python) | Level recorded |
| --- | --- |
| `js.console.debug(json_line)` | `debug` |
| `js.console.info(json_line)` | `info` |
| `js.console.warn(json_line)` | `warn` |
| `js.console.error(json_line)` | `error` |
| prototype `logging.Handler` on `agents.levelprobe`: `DEBUG` / `INFO` / `WARNING` / `ERROR` | `debug` / `info` / `warn` / `error` |

Calling the console methods through the FFI preserves levels, and a
`logging.Handler` that dispatches on `record.levelno` works as designed
([observability.md](./observability.md) §6).

### 7.5 `abort` inside an alarm, and `retryAlarm`

Each mode arms an alarm 1 s out; the alarm handler records its run in another
Durable Object, then does the mode's action:

| Alarm handler does | Runs | Gaps between runs (s) |
| --- | --- | --- |
| `ctx.abort(reason)` (default options) | **7** (first + 6 retries) | 2.1, 5.1, 8.6, 18.9, 34.2, 73.6 |
| `ctx.abort(reason, {retryAlarm: false})` | **1** | — |
| raises an exception (control) | **7** | 2.3, 4.8, 8.1, 16.7, 33.1, 70.0 |

- The platform retries a failed or aborted alarm up to 6 times with roughly
  doubling backoff; `{retryAlarm: false}` stops that, as upstream's
  `abortWithoutAlarmRetry` relies on.
- **The Workers SDK's `ctx.abort(reason)` takes no options**, so the option is
  only reachable through the raw JS context (`self.ctx._ctx.abort(reason,
  to_js({"retryAlarm": False}, dict_converter=Object.fromEntries))`), queued
  in a microtask as the SDK does. `_ctx` is a private SDK attribute (like
  `env._env`, §2.4): a known dependency.
