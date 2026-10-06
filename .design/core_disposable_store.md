# `core/`: events, disposables, and the Python mapping

What upstream `packages/agents/src/core/` contains, how the SDK uses it, and
how it should be ported.

Related: [agents-subpackages.md](./agents-subpackages.md),
[lifecycle_capabilities.md](./lifecycle_capabilities.md).

---

## 1. What `core/` is

Two small internal files, about 160 lines in total. It is **not a public import
path**: `package.json` has no `agents/core` export, and only code inside the
package imports it.

| File | Contents | Used by |
| --- | --- | --- |
| `core/events.ts` (52 lines) | `Disposable`, `toDisposable`, `DisposableStore`, `Emitter<T>`, `Event<T>` | `Agent` (`index.ts`), MCP client (`mcp/client/index.ts`, `connection.ts`) |
| `core/base64-redaction.ts` (111 lines) | `redactBase64Payloads`, `redactBase64Replacer` | Browser tools (`browser/tool-helpers.ts`), AI tracing (`observability/ai/content.ts`) |

---

## 2. `core/events.ts`

A minimal version of the event/disposable pattern from VS Code.

```ts
interface Disposable { dispose(): void }
function toDisposable(fn: () => void): Disposable   // wrap a function as a Disposable

class DisposableStore implements Disposable {
  add<T extends Disposable>(d: T): T                 // collect
  dispose(): void                                    // pop in reverse order, each in try {} catch {}
}

type Event<T> = (listener: (e: T) => void) => Disposable   // subscribe → unsubscribe handle

class Emitter<T> implements Disposable {
  readonly event: Event<T>                           // public subscribe function
  fire(data: T): void                                // call every listener; log listener errors
  dispose(): void                                    // drop all listeners
}
```

**Convention:** a class keeps its `Emitter` private and exposes only `.event`,
so outside code can subscribe but can't fire:

```ts
private readonly _onServerStateChanged = new Emitter<void>();
public  readonly onServerStateChanged: Event<void> = this._onServerStateChanged.event;
```

---

## 3. How the SDK uses it: MCP server state reaching the browser

The clearest example is how a change to an MCP server's tool list reaches a
`useAgent` UI. It passes through three objects, each linked to the next by an
`Emitter`, and every subscription is kept in a `DisposableStore`.

```
MCPClientConnection        ──fires──▶  MCPClientManager           ──fires──▶  Agent
(one per remote server)                (this.mcp, a capability)              (broadcasts to clients)
  onListChanged                          onServerStateChanged                  broadcastMcpServers()
  onObservabilityEvent                   onObservabilityEvent                  observability.emit()
```

**The manager forwards each connection's events** (`mcp/client/index.ts:1381`).
It uses one `DisposableStore` **per connection**:

```ts
const store = new DisposableStore();
const existing = this._connectionDisposables.get(id);
if (existing) existing.dispose();          // reconnecting? drop the old subscriptions first
this._connectionDisposables.set(id, store);

store.add(this.mcpConnections[id].onListChanged(() => {
  this._onServerStateChanged.fire();       // re-fire upward
}));
store.add(this.mcpConnections[id].onObservabilityEvent((event) => {
  this._onObservabilityEvent.fire(event);
}));
```

The manager also fires `onServerStateChanged` itself, for example at the end of
its `onStart` once it has restored saved servers (`:457`).

**`Agent` subscribes** (`index.ts:2051`) and keeps the subscriptions in its own
`_disposables` store (`index.ts:1207`):

```ts
this._disposables.add(
  this.mcp.onServerStateChanged(() => {
    if (mcpBroadcastReady) this.broadcastMcpServers();   // sends cf_agent_mcp_servers
  })
);
this._disposables.add(
  this.mcp.onObservabilityEvent((event) => {
    this.observability?.emit({ ...event, agent: this._ParentClass.name, name: this.name });
  })
);
```

**Teardown** (`index.ts:8417`, in `destroy()`):

```ts
await this.ctx.storage.put(DESTROY_PENDING_KEY, true);  // marker so the next wake finishes an interrupted destroy
await this.lifecycle.disableAlarms();
await this.lifecycle.dispose();   // capabilities release resources (WebSockets closes sockets, etc.)
this._disposables.dispose();      // drop every subscription Agent made
await this.ctx.storage.deleteAll();
```

**End to end:** a remote MCP server sends `notifications/tools/list_changed`.
1. The connection fires `onListChanged`.
2. The manager's listener calls `_onServerStateChanged.fire()`.
3. Agent's listener calls `broadcastMcpServers()`.
4. Each browser receives `{type: "cf_agent_mcp_servers", mcp: {...}}`.

None of the three objects references the one above it. Each only fires events.

---

## 4. `core/base64-redaction.ts`

It replaces base64 strings of **4096 characters or more** with a placeholder
such as `[base64 image/png data omitted: 48,212 chars, approximately 36,159
bytes]`. A screenshot object's `data` field (`type: "browser_screenshot"`) is
redacted regardless of size.

- `redactBase64Payloads(value)` walks an object tree with limits: depth 20,
  10,000 nodes, and detection of circular references. Binary values, `Date`,
  `Map`, and `Set` pass through untouched. Used by the **browser tools**
  (`browser/tool-helpers.ts:84`) so a screenshot doesn't flood the model's
  context.
- `redactBase64Replacer` is a `JSON.stringify` replacer with the same rules.
  Used by **AI tracing** (`observability/ai/content.ts:352`) to keep tool
  payloads under workerd's 64 KiB span-attribute limit.

**Port when needed.** It only matters once the browser tools or AI tracing are
ported.

---

## 5. Python mapping

### 5.1 `DisposableStore` → plain `contextlib.AsyncExitStack`

**Decision:** use a plain `AsyncExitStack`, with **no subclass and no
error-silencing wrapper**.

| `core/events.ts` | `contextlib` |
| --- | --- |
| `new DisposableStore()` | `AsyncExitStack()` |
| `store.add(disposable)` | `stack.callback(handle.dispose)` |
| `store.dispose()` | `await stack.aclose()` |
| reverse order | reverse order |
| empty afterwards | empty afterwards; a second `aclose()` does nothing |
| ignores cleanup errors | runs **every** callback, then re-raises |

Illustration, mirroring upstream's MCP subscriptions (MCP itself is out of
scope in phase 1; the same pattern holds any subscription `Agent` makes):

```python
class Agent(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self._disposables = AsyncExitStack()
        ...
        self._disposables.callback(
            self.mcp.on_server_state_changed(self._on_mcp_changed).dispose
        )
        self._disposables.callback(
            self.mcp.on_observability_event(self._forward_mcp_event).dispose
        )

    async def destroy(self):
        await self.ctx.storage.put(DESTROY_PENDING_KEY, True)
        await self.lifecycle.disable_alarms()
        await self.lifecycle.dispose()
        await self._disposables.aclose()
        await self.ctx.storage.deleteAll()  # JS method name, through the SDK wrapper
```

A DO instance lives across many requests, so the stack is an **attribute that
gets closed in `destroy()`**, not an `async with` block.

The MCP manager's per-connection stores map to one `AsyncExitStack` per
connection, kept in a dict keyed by server id. On reconnect, it calls
`aclose()` on the old stack before creating a new one.

### 5.2 Why no try/except wrapper

The TypeScript `try {} catch {}` exists because `DisposableStore.dispose()` is
a hand-written loop:

```ts
while (this._items.length) {
  try { this._items.pop()!.dispose(); } catch {}
}
```

Without the `catch`, one failing `dispose()` would stop the loop and leave the
remaining items uncleaned. Its purpose is to make sure **every item still
runs**.

`AsyncExitStack` already guarantees that. This was verified:

```
callbacks registered: first, boom (raises), last
close() ran:          ['last-registered', 'boom', 'first-registered']   → then raised RuntimeError('x')
```

With that covered, the only thing a wrapper could still change is whether
`aclose()` raises at the end. The cleanups stored here are unsubscribe handles
(`set.discard(listener)`), which can't fail. So the wrapper would guard against
an error that can't happen. If a cleanup ever does raise, every other cleanup
still runs and the error reaches us, which is what should happen with a real
error.

If a cleanup that's genuinely allowed to fail ever appears, handle it **at
that one call site** with a stated reason. Don't build that into the store.

**Effect on `destroy()`:** if `aclose()` ever raised, `deleteAll()` would be
skipped. The `DESTROY_PENDING_KEY` marker already covers that case, because the
next wake finishes the destroy.

### 5.3 Rejected: a "quiet" `AsyncExitStack` subclass

An earlier draft had a `DisposableStore(AsyncExitStack)` subclass with:
- `add(fn)`: wrapped each callback in `try/except Exception` with
  `logger.exception`, and accepted sync or async functions;
- `add_context(cm)`: `enter_async_context` with the context manager's
  `__aexit__` errors swallowed. Its exit callback had to return `False` so it
  wouldn't hide an exception already in flight.

It was rejected:
- `add`'s wrapper only duplicates what `AsyncExitStack` already guarantees
  (§5.2).
- `add_context` solved a problem that doesn't exist upstream. In the
  TypeScript, `DisposableStore` only ever holds event subscriptions; MCP
  connections are closed separately. Putting resources in the store was
  speculative, and then guarding it doubled the speculation.

If Python resources (for example an MCP transport that is an async context
manager) end up in a stack later, use plain `await
stack.enter_async_context(cm)` and let close errors raise.

### 5.4 `Emitter` (decided: ported in phase 1)

A small internal pub/sub class in `agents/core/events.py`.

**Users in phase 1:**
- the **observability `subscribe()` registry** (sync listeners;
  [observability.md](./observability.md) §5.4);
- the **Sessions change feed** (async listeners, awaited in order;
  [sessions_api.md](./sessions_api.md) §5);
- later, the MCP client, as upstream.

**What the standard library offers:** building blocks, but no ready-made
observer/pub-sub class (`blinker`, the usual choice, is third-party). The
pieces used:
- an insertion-ordered `dict` for listeners: subscription order is dispatch
  order, and per-subscription tokens let the same callable subscribe twice;
- `inspect.isawaitable` to support sync and async listeners in one class;
- `logging.exception` for listener failures;
- `Disposable` handles, which fit `AsyncExitStack` (§5.1).

Considered and rejected: `weakref.WeakSet` / `WeakMethod` for weak listeners.
A bound method stored weakly disappears immediately unless wrapped in
`WeakMethod`, and weak references hide when a listener is dropped. Upstream
holds strong references and relies on explicit `dispose()`; so do we.
`sys.addaudithook` is process-global and can't be removed, and
`asyncio.Queue` suits streaming consumers rather than callbacks.

```python
class Emitter[T]:  # PEP 695, as implemented
    def __init__(self) -> None:
        self._listeners: dict[object, Callable[[T], object]] = {}  # insertion-ordered

    def subscribe(self, listener: Callable[[T], object]) -> Disposable:
        token = object()
        self._listeners[token] = listener
        return Disposable(lambda: self._listeners.pop(token, None))

    def fire(self, value: T) -> None:
        """Call sync listeners in subscription order."""
        for listener in list(
            self._listeners.values()
        ):  # copy: listeners may unsubscribe while firing
            try:
                result = listener(value)
            except Exception:
                logger.exception("Emitter listener failed")
                continue
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()  # no "never awaited" warning
                raise TypeError("fire() got an async listener; use fire_async()")

    async def fire_async(self, value: T) -> None:
        """Call sync or async listeners in subscription order, awaiting each."""
        for listener in list(self._listeners.values()):
            try:
                result = listener(value)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Emitter listener failed")

    def dispose(self) -> None:
        self._listeners.clear()
```

**Decisions in this design:**
- **Listener errors are isolated** (this settles the previously open
  question). A listener that raises is logged and the rest still run, and the
  error never reaches whoever called `fire`. The specific failures this
  prevents: a buggy subscriber failing a Sessions write that has already
  committed, an RPC call (through observability), or, once ported,
  `MCPClientManager.on_start` and with it Lifecycle startup. Only `Exception`
  is caught, so `CancelledError` still propagates.
- **`fire_async` awaits listeners one at a time, in order**, matching the
  Sessions change feed's ordered dispatch (a mirror sees an `append` before the
  `compact` it triggered). Not concurrent `gather`.
- **Passing an async listener to sync `fire` is a `TypeError`**, rather than
  silently creating an un-awaited coroutine.
- **`Disposable.dispose()` can be called more than once.**
- **The public surface is the bound `subscribe` method.** The owner keeps the
  `Emitter` private and exposes only subscription, as upstream exposes `.event`:
  `self._on_change = Emitter()` and `self.on_change = self._on_change.subscribe`.

**Implemented:** `src/agents/core/events.py` (`Disposable`, `Emitter[T]`, using
PEP 695 generics), with tests in `tests/core/test_events.py` (dispatch order,
subscribing the same callable twice, isolation for sync and async listeners,
`dispose()` twice, unsubscribing while firing, the `fire`/async `TypeError`,
and `CancelledError` propagating). When `fire` rejects an async listener, it
closes the coroutine first so no "never awaited" warning is left behind.

---

## 6. Summary

| Upstream | Python |
| --- | --- |
| `DisposableStore` | `contextlib.AsyncExitStack`, used as is |
| `Disposable` / `toDisposable` | a small `Disposable(fn)` class, or just pass the bound `dispose` to `stack.callback` |
| `Emitter<T>` / `Event<T>` | Ported: `agents/core/events.py` `Emitter` with `subscribe` / `fire` / `fire_async` (§5.4) |
| `base64-redaction.ts` | port with the browser tools / AI tracing |
