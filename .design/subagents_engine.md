# Sub-agents engine (design pass, step 8)

The machinery behind `self.dynamic_agents`, `parent_agent`, `/sub/` routing,
and facet-hosted scheduling. The **API** was designed earlier
([agent_api.md](./agent_api.md) §1.15, [scope.md](./scope.md) §2.6,
[agents_wire_protocol.md](./agents_wire_protocol.md) §6); this doc is the
internals.

Upstream: `packages/agents/src/dynamic-agents/` (`dynamic-agents.ts` 1872
lines, `registry.ts`, `bridges.ts`, `identity.ts`, `types.ts`, `api.ts`,
`host.ts`), `sub-routing.ts`, the `_cf_*` entry points in `index.ts`, and
`design/sub-agent-routing.md` (current behavior; `rfc-sub-agent-routing.md`
records the original proposal, whose "parent forwards the upgrade to the
facet" model was replaced).

Status: **decided and implemented** (2026-10-05). §5 records the questions
and answers; §6 records how the implementation turned out.

---

## 1. Platform spike (done, on `workerd`)

`verify/src/entry.py` `FacetParent` / `FacetChild`, `/facet-spike`:

| Question | Result |
| --- | --- |
| `ctx.facets.get(key, getter)` from Python, the getter returning `{class, id}` (a `create_proxy` callback returning a JS object) | ✅ |
| Python Durable Object classes in `ctx.exports` (bound or not) | ✅ all exported classes |
| An explicit facet id (`root_ns.idFromName(identity)`) makes the child's `ctx.id.name` that identity | ✅ |
| Native RPC and `fetch` into a facet | ✅ |
| A Python function passed to the facet, called back from there | ✅ |
| An object of functions (a bridge) passed to the facet | ✅ with one rule: the child receives raw JS functions, so its **arguments must be converted** (`_ffi.to_rpc`) before calling; a raw `dict` argument fails with `DataCloneError` |
| `ctx.facets.abort` keeps storage; `ctx.facets.delete` wipes it | ✅ |

No blocker: upstream's design ports.

---

## 2. How upstream works

**Identity and bootstrap.** A child is `ctx.facets.get("<Class>\0<name>",
() => ({class: ctx.exports[Class], id}))`. The id comes from the **root's**
namespace (`ctx.exports[root class].idFromName(identity_name)`), because an
intermediate parent is itself a facet without a namespace. The identity name
is path-scoped: `cf-agents:v2:<urlencoded name>:<sha256(JSON(child path))>`,
so two parents' children named alike never collide. The parent then calls
`_cf_initAsFacet(name, parent_path, identity_name)` on the child, which
checks `ctx.id.name == identity_name`, records `is_facet`, `facet_name`, and
`parent_path` in KV (restored on every later start), and runs startup. A
facet's `this.name` is its logical name.

**Registry.** Each parent keeps `cf_agents_sub_agents (class, name,
created_at, identity_version, identity_name)`: written (insert-or-ignore)
before the init RPC (rolled back if init fails on a new row), removed by
`delete`. It backs `has` / `list` and strict gates in `on_before_sub_agent`.

**Alarm-owning work goes to the root.** The Lifecycle route transport of a
facet: `source` = its address (`key` = the URL-encoded path
`Class:name/Class:name`, `data` = the path as JSON); `to_root(envelope)` =
RPC to the root (`_cf_routeLifecycle(None, envelope)`, the root found as
`ctx.exports[root class].get(idFromName(root name))`); `to(target, envelope)`
= walk down the tree one hop per facet (`_cf_routeLifecycle(target, …)` on
each child), cleaning up a target whose registry row is gone. This is what
makes the Queue, Scheduler, and Tasks facet paths live.

**HTTP.** `Agent.fetch` parses `/sub/{child-class}/{child-name}` (class
segments matched against `ctx.exports` keys in kebab case), runs the
parent's `on_before_sub_agent` (a `Response` short-circuits; a `Request`
replaces headers/body; the child's path is always the tail), then
`fetch`es the facet with the `/sub/…` prefix stripped. Recursive: the child
does the same for further `/sub/` hops.

**WebSockets.** The **root keeps the native socket** (hibernation, the
attachment, the 16 KiB budget). An upgrade to `/sub/…` passes the gate,
then is accepted by the root's own WebSockets capability with the outer URL
recorded (`x-cf-agents-subagent-url` header → a `_cf_subAgentOuterUrl` flag).
`Agent`'s connection handlers then forward instead of running locally:
- **connect / message / close** → `_cf_handleSubAgentWebSocket{Connect,
  Message,Close}(…, bridge, meta)` on the child (resolving one hop at a
  time), where `meta` = `{id, uri (the tail), tags, state, request headers}`
  and `bridge` = the root's `send` / `close` / `setState` / `broadcast` for
  that socket;
- the child builds a **virtual connection** (the same `Connection` surface)
  whose operations go through the live frame's bridge, or through the root
  over RPC once that frame has ended (`_cf_sendToSubAgentConnection`, …),
  queued per connection to keep their order;
- the child runs the normal connect sequence (readonly, protocol, tags,
  identity frames, `on_connect`), message dispatch (state frames, RPC,
  `on_message`), and `on_close` against the virtual connection;
- **RPC replies** from a facet's `@callable` go back through the frame's
  reply bridge (including streamed chunks);
- the root excludes child-targeted sockets from its own `get_connections` /
  `broadcast`; a facet's `get_connections` / `broadcast` see only its
  virtual connections (a facet must never touch the root's sockets: "Cannot
  perform I/O on behalf of a different Durable Object");
- a restarted facet **hydrates** its virtual connections from the root
  (`_cf_subAgentConnectionMetas`).

**Lifecycle operations.** `abort` = `ctx.facets.abort` (storage kept,
transitive). `delete` = close the subtree's sockets (1001 "Sub-agent
deleted", marked so stale frames can't recreate it), `ctx.facets.delete`,
forget the registry row, and clean the root's routed work for the subtree
(`_cleanup_route_prefix` on Scheduler, Queue, Tasks). `destroy()` on a facet
asks the root to delete it (`_cf_destroyDescendantFacet`, walking down).

**Reaching agents.** `parent_agent(Cls)`: the direct parent; a top-level
parent via `env[Cls.__name__]` or `ctx.exports`, a facet parent through the
root (`_cf_invokeSubAgentPath`, one hop at a time). `get_sub_agent_by_name(
parent, Cls, name)` (Worker side) returns a proxy whose method calls go
through the parent (`_cf_invokeSubAgent`), without the gate.
`route_sub_agent_request` forwards a custom-routed request through the
parent's `fetch`.

---

## 3. Python design

### 3.1 A straight port (no choice involved)

Everything in §2: path-v2 identities, the root namespace for ids, the init
handshake and its persisted facet metadata, the registry table (same
columns, [sql_schemas.md](./sql_schemas.md)), the route transport, `/sub/`
HTTP forwarding with the gate, root-owned sockets with forwarded connect /
message / close, virtual connections with per-connection ordering and
hydration, RPC reply bridging, the connection exclusions, abort / delete /
destroy, subtree cleanup, `parent_agent`, `get_sub_agent_by_name`,
`route_sub_agent_request`, and the `Sub`-class / NUL-name / unknown-class
checks. The capability is `DynamicAgents` (id `dynamic-agents`), installed by
`Agent`.

### 3.2 Where Python differs (proposed)

1. **No legacy identities.** Upstream keeps bare-name "legacy" facets
   working for agents created before path-v2. Python has none, so every child
   is path-v2; the `identity_version` column is kept and always `'path-v2'`.
2. **All JS interop goes through `_ffi`**: `facet_get(ctx, key, class_name,
   id_name, root_class)`, `facet_abort`, `facet_delete`, `export_names`,
   `root_stub`, and `call_rpc(stub, method, *args)` (converting arguments
   with `to_rpc`, as the spike requires, and results with `from_rpc`).
3. **Bridges are dicts of functions** (`{"send", "close", "set_state",
   "broadcast"}`), not `RpcTarget` subclasses: Python can't define a JS
   `RpcTarget`, and the spike shows functions cross RPC. The child calls them
   through `_ffi.call_rpc`.
4. **A virtual connection is a `Connection` subclass**
   (`VirtualConnection`), overriding the attachment read/write, `send`,
   `close`, and equality (by id: there is no local socket). Everything else
   (`state`, `set_state`, `readonly`, `protocol_enabled`, flags) comes from
   `Connection` unchanged, so handlers see one type. The state it forwards to
   the root is the attachment's `__user` and `__flags`, minus the root-only
   flags (outer URL, deleted).
5. **No separate RPC reply bridge** (decided while planning the code).
   Upstream routes a facet's `@callable` replies through a dedicated reply
   bridge so they land while the originating frame is live. In Python the
   replies are ordinary `VirtualConnection.send` calls on the per-connection
   ordered queue, and every forwarded connect / message / close **waits for
   its connection's queue to drain before returning**, so replies are
   delivered while the frame's bridge is still valid; anything sent later
   goes through the root over RPC.
6. **Envelopes cross RPC as plain dicts** (`{capability, source: {key, data}
   | None, payload}`); route payloads are already plain JSON (the facet paths
   of Queue, Scheduler, and Tasks were designed for this).
7. **`Agent` answers RPC frames itself** (as upstream's `Agent.onMessage`)
   instead of passing `callables=self` to `WebSockets`: a root socket that
   targets a child must be forwarded *before* any local consumption, and
   `WebSockets` consumes `rpc` frames before its handlers run. `WebSockets`'
   `callables` option stays for plain Durable Objects.
8. **Bridge function proxies are scoped to the frame** (`_ffi.proxies()`),
   so the JS proxies for a forwarded event's bridge are destroyed when the
   event returns (upstream's `RpcTarget`s are garbage-collected).

### 3.3 Left for later steps

- **Facet fibers** (`cf_agents_facet_runs`, `_cf_registerFacetRun`,
  `checkRunFibers` for facets) and **facet `keep_alive`** (root-held keep-alive
  tokens) belong to step 9: neither fibers nor `keep_alive` exist yet.
- **Workflows' `_cf_invokeAgentPath`**: workflows are out of scope.

### 3.4 Module layout (`src/agents/dynamic_agents/`)

| Module | Holds | Upstream |
| --- | --- | --- |
| `types.py` | `SubAgentInfo`, `AgentPathStep`, `AgentRoute` (both moved here from `agent/`), connection meta, wire envelopes, the `SubAgentsHost` protocol | `types.ts` |
| `errors.py` | `SubAgentAbortedError` | (Python) |
| `paths.py` | `/sub/` parsing, path keys, path-v2 identity names | `sub-routing.ts`, `identity.ts` |
| `registry.py` | the `cf_agents_sub_agents` table | `registry.ts` |
| `connections.py` | `VirtualConnection`, the root-only flags, forwarded state | `dynamic-agents.ts` (virtual connections) |
| `dynamic_agents.py` | `SubAgentsEngine` (the capability): identity, resolve / abort / delete, the route transport, forwarding, the per-connection operation queue, hydration, cleanup | `dynamic-agents.ts`, `bridges.ts` |
| `stubs.py` | `AgentStub`, `PathStub`, `SubAgentStub` (method calls over native RPC) | `api.ts`, `index.ts` (`getSubAgentByName`) |
| `api.py` | `DynamicAgents` (the public `self.dynamic_agents`) | `api.ts` |

The Worker-side `route_sub_agent_request` / `get_sub_agent_by_name` go in
`agent/routing.py`; the `_cf_*` entry points are methods on `Agent` (they
must be on the Durable Object to be reachable over native RPC).

---

## 4. Verification plan

- **Unit tests** (fake runtime with fake facets): identity names, the
  registry, the init handshake, routed Queue / Scheduler / Tasks through the
  real transport code, HTTP forwarding and the gate, WebSocket forwarding
  (connect sequence from the child, messages, RPC replies, close), virtual
  connection ordering, delete closing sockets and cleaning routed work,
  `parent_agent`, `get_sub_agent_by_name`.
- **On `workerd`:** a client over `/agents/<parent>/<name>/sub/<child>/<id>`
  (identity frame from the child, state sync, `@callable`, a streamed reply),
  a facet's schedule and task firing on the root's alarm, `delete` closing
  the socket, and the Queue / Scheduler / Tasks facet paths end to end.

---

## 5. Questions to decide

**Q1. Internal RPC entry-point names.** **Decided: (a) Python names**
(`_cf_init_as_facet`, …). Upstream uses camelCase `_cf_*`
names (`_cf_initAsFacet`, `_cf_handleSubAgentWebSocketMessage`, …). These
calls are only ever Python ↔ Python.
- (a) **Python names** (`_cf_init_as_facet`, `_cf_handle_sub_agent_websocket_message`,
  …). **Recommended:** nothing outside the SDK calls them, and they follow
  the SDK's naming everywhere else.
- (b) Upstream's names, attached with `setattr` like the runtime entry points.

**Q2. Public path helpers.** **Decided: (a) port them** as
`build_agent_path` / `build_agent_url`. Upstream exports `buildAgentPath(path, {prefix,
leafPath, rootBinding})` / `buildAgentUrl(origin, path, …)` (the canonical
`/agents/…/sub/…` serializer, e.g. for a Python Worker building client
URLs). They aren't in the decided API.
- (a) **Port them** (`build_agent_path`, `build_agent_url`): small, and the
  parser and serializer live in the same module anyway. **Recommended.**
- (b) Leave them out.

---

## 6. Implementation notes (step 8, 2026-10-05)

Done: `src/agents/dynamic_agents/`, the sub-agent half of `Agent`, and
`route_sub_agent_request` / `get_sub_agent_by_name` in `agent/routing.py`;
416 tests in total (29 new, in `tests/dynamic_agents/`, on a fake world of
namespaces and facets). Checked on `workerd` (`verify/results/subagents_client.py`):
HTTP through the gate (and a 403 from it), a client socket on
`/sub/facet-chat/c1` getting the child's identity, state, and MCP frames, a
`@callable` reply with its state broadcast, text and binary echoes, a
grandchild socket (`/sub/…/sub/facet-note/n1`), a refused upgrade closing
with 4403, a facet's schedule, queued callback, and task sleep firing on the
root's alarm, `parent_agent` calling back up, and `delete` closing the
socket with 1001.

How it turned out, beyond §3:

1. **The engine is a capability, installed first.** `SubAgentsEngine` is a
   `LifecycleCapability` (id `dynamic-agents`) that `Agent` installs before
   Scheduler, Queue, State, WebSockets, and Tasks, and its transport is set
   as the Lifecycle's route transport. Its `on_start` restores a facet's
   identity before the others start, since they route through it.
2. **One KV key for a facet's identity.** `cf_agents_facet` holds
   `{name, parent_path}`, written by `_cf_init_as_facet` and read on every
   wake. Upstream keeps the name and the parent path apart.
3. **Hydration runs in the background.** A woken facet rebuilds its
   virtual connections from the root (`_cf_sub_agent_connection_metas`)
   without holding up startup; events that arrive first bring their own
   metadata.
4. **`AgentRoute` moved to `dynamic_agents/types.py`.** Capabilities can't
   import `agent/`, and the engine needs it for the gate. It's still
   exported from `agents`.
5. **A facet's own connections exclude pass-through ones.** The metadata's
   `uri` is rewritten to the remaining path at each hop, so a connection
   whose `uri` still has a `/sub/` hop belongs to a sub-agent further down.
   `get_connections` on a middle facet leaves those out.
6. **Binary frames are normalized.** Bytes cross RPC as a `memoryview`; the
   engine turns them back into `bytes` (`as_message`) before `on_message`
   and before sending to a socket. Found on `workerd`.
7. **A facet parent's `PathStub` has no `fetch`.** `parent_agent` returns an
   `AgentStub` for a top-level parent and a `PathStub` (method calls only,
   through the root's `_cf_invoke_sub_agent_path`) for a facet parent.
8. **`Agent` answers RPC itself.** WebSockets no longer gets
   `callables=self`; `Agent._message_locally` applies state frames, then
   answers RPC with its own `RpcDispatcher`, so forwarded and native
   connections share one path (§3.2 item 7).
