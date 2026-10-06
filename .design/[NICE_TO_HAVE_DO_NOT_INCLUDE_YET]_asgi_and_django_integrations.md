# ASGI and Django integrations (nice to have; not in scope yet)

> **Not part of phase 1.** Ideas recorded for a later discussion; nothing here
> is decided or scheduled.

How agents could fit into a Python Worker that serves an ASGI app (FastAPI,
Starlette, Django via ASGI) or a WSGI app (Django, Flask).

Related: [agent_api.md](./agent_api.md) §1.11 (`on_request`), §1.14
(`route_agent_request`, `get_agent_by_name`), §1.16 (`get_current_agent`),
§1.17 (Worker entrypoint).

---

## 1. How the Workers SDK runs ASGI apps today

Source: `workers/asgi.py` in `workers-runtime-sdk` 1.9.2 (`import asgi` is an
alias for it).

```python
from workers import asgi


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return await asgi.fetch(app, request, self.env)


# or, equivalently:
Default = asgi.entrypoint(app)
```

- **HTTP:** `asgi.fetch(app, request, env)` turns the JS `Request` into an ASGI
  `http` scope (headers, path, query, `scheme`), feeds the body in as
  `http.request` messages, and turns `http.response.start` / `.body` back into
  a JS `Response` (streaming bodies go through a `TransformStream`).
- **`env` is in the scope:** `scope["env"]`. FastAPI gets a dependency for it:
  `from workers.asgi import env` → `async def route(env=env): …`
  (`request.scope["env"]` in Starlette).
- **Lifespan:** `asgi.fetch` runs the app's lifespan startup and shutdown
  **around every request** (`start_application`), since a Worker has no
  long-lived process.
- **WebSockets:** `asgi.websocket(app, request, env)` creates its own
  `WebSocketPair` **in the Worker**, calls `server.accept()` (the standard,
  non-hibernating API), bridges events to ASGI `websocket.*` messages, keeps
  the Worker alive with `waitUntil`, and returns the 101 response itself.
- **Durable Objects:** **not exposed specially.** There's no DO-aware part of
  the adapter. An ASGI route reaches a DO like any Worker code: take the
  namespace from `scope["env"]` and call `stub.fetch(...)` or native RPC.
- The adapter runs the app in tasks created with `ensure_future`, which copy
  the current `contextvars` context.

## 2. What this means for agents

`route_agent_request` needs the **original JS `Request`**: it forwards it to
the agent's DO with `stub.fetch(request)` and returns the DO's `Response`. For
a WebSocket upgrade, that `Response` is the DO's 101 carrying the client end
of a socket the DO accepted with the **hibernation** API.

Inside an ASGI app, the original request is gone (only the ASGI scope and body
messages remain; the adapter doesn't put the JS request in the scope), and for
WebSockets the adapter has **already** created and accepted its own socket in
the Worker. ASGI has no way to return "this other 101 response" instead.

So:

| Integration | HTTP | WebSocket |
| --- | --- | --- |
| A. Route agents **before** the ASGI app, in the Worker entrypoint | works | works (hibernating) |
| B. Call agents **from** ASGI routes (`get_agent_by_name` + RPC) | works | n/a |
| C. Mount agent routing **inside** the ASGI app (`app.mount("/agents", …)`) | feasible (rebuild a JS `Request` from the scope + body) | **not feasible** with the SDK's adapter; only by proxying frames through the Worker (§3) |
| D. Serve an agent's own HTTP with an ASGI app (`on_request` delegates to FastAPI) | works | n/a (agent sockets go through `on_connect`) |

### A. Route first, then the ASGI app (proposed default)

```python
from fastapi import FastAPI
from workers import WorkerEntrypoint, asgi
from agents import route_agent_request

app = FastAPI()


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return await route_agent_request(request, self.env) or await asgi.fetch(
            app, request, self.env
        )
```

`/agents/...` (HTTP and WebSockets) goes to the agents; everything else goes to
FastAPI. Nothing ASGI-specific in the SDK. The upgrade branch isn't needed for
`/agents/` paths: `route_agent_request` handles upgrades itself. If the ASGI
app also serves WebSockets, the entrypoint dispatches those to
`asgi.websocket` as `asgi.entrypoint` does.

Possible sugar (open): `agents.asgi.entrypoint(app, **route_options)`, the same
as `workers.asgi.entrypoint` with agent routing first.

### B. Call agents from routes

```python
from workers.asgi import env


@app.post("/rooms/{room}/messages")
async def post_message(room: str, body: Message, env=env):
    stub = await get_agent_by_name(env.ChatRoom, room)
    return await stub.add_message(body.text)
```

Native RPC from a route, like any Worker code. Works with any ASGI framework
(`request.scope["env"]` without FastAPI).

### D. An agent serving HTTP through FastAPI

```python
api = FastAPI()


@api.get("/summary")
async def summary():
    agent = get_current_agent().agent  # the adapter's tasks copy the context
    return {"messages": len(agent.messages)}


class ChatRoom(AIChatAgent):
    async def on_request(self, request):
        return await asgi.fetch(api, request, self.env)
```

Requests reaching the agent's `on_request` (after `route_agent_request`) are
handled by a FastAPI app. `get_current_agent()` should see the agent because
the adapter's `ensure_future` tasks copy the context (to verify). The route
paths include the `/agents/<agent>/<name>` prefix (§1.11), so the app either
uses that prefix (`APIRouter(prefix=...)`) or the request is rewritten first.
Caveat: the adapter runs lifespan startup/shutdown on every request.

## 3. Why C can't carry WebSockets

The only way would be a proxy: the Worker accepts the client socket (as the
adapter does), opens a second WebSocket to the agent's DO, and pipes frames
both ways. It works, but:
- the **Worker stays running for the whole connection** (no hibernation on the
  Worker side), which defeats the point of hibernating agents;
- every frame crosses an extra hop;
- the agent sees the Worker as its client (the `_pk` id, headers, and close
  codes must be forwarded by hand).

Not recommended. A cleaner fix would be upstream: the SDK's adapter exposing
the original JS request in the scope (e.g. `scope["workers.request"]`), plus a
way for an ASGI app to return a raw JS `Response`. Neither exists today.

## 4. Django (and other WSGI apps)

- **Django over ASGI** (`django.core.asgi.get_asgi_application()`) runs through
  the same `workers.asgi` adapter, so §2 applies unchanged: pattern A (route
  agents first) works, B (call agents from views) works, C has the same
  WebSocket limit. Django Channels' WebSocket consumers would run in the
  Worker's non-hibernating socket, not in an agent.
- **WSGI** (`workers/wsgi.py` in the SDK, `workers.wsgi`): builds a WSGI
  `environ` from the JS request and puts the Worker env at
  `environ["workers.env"]` (mirroring `scope["env"]`). WSGI views are
  synchronous, so they can't `await` agent RPC directly; pattern A (route
  agents before handing the request to the WSGI app) still works, since
  routing happens in the async entrypoint. Calling agents from a sync view
  would need an async bridge: to investigate.
- Not investigated yet: Django's own startup cost per request on Workers, and
  whether `django.setup()` belongs in the memory snapshot.

## 5. Open (for the later discussion)

1. Default integration: A (route first) as the documented pattern? (Leaning
   yes.)
2. `agents.asgi.entrypoint(app, ...)` sugar for A, or just document the
   three-line entrypoint?
3. C for HTTP only (a mountable ASGI app for `/agents/...` without WebSockets),
   or skip C?
4. To verify: `get_current_agent()` inside an ASGI app run from `on_request`
   (D); FastAPI/Starlette routing with the `/agents/...` prefix.
