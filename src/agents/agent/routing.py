"""Getting a request or a call to the right agent instance.

Port of upstream ``agent-routing.ts`` (``routeAgentRequest``,
``getAgentByName``), without ``props`` (``.design/agent_api.md`` §1.14).
Not a path router: requests match ``/{prefix}/{binding}/{name}/...``.
"""

import asyncio
import logging
import math
import random
import weakref
from collections.abc import Awaitable, Callable, Mapping
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit

from workers import Request, Response

from .. import _ffi
from ..core.naming import camel_case_to_kebab_case
from ..core.timing import to_seconds
from ..dynamic_agents.paths import parse_sub_agent_path, rewrite_pathname
from ..dynamic_agents.stubs import SubAgentStub
from ..lifecycle.lifecycle import is_upgrade_request
from .types import (
    AgentRoute,
    BeforeHook,
    RoutingRetry,
    RoutingRetryEvent,
    RoutingRetryOptions,
)

__all__ = (
    "get_agent_by_name",
    "get_sub_agent_by_name",
    "route_agent_request",
    "route_sub_agent_request",
)

_log = logging.getLogger("agents.routing")

_PERMISSIVE_CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "*",
    "Access-Control-Max-Age": "86400",
}

# kebab-case binding name -> (binding name, namespace), cached per env.
_namespaces: "weakref.WeakKeyDictionary[Any, dict[str, tuple[str, Any]]]" = (
    weakref.WeakKeyDictionary()
)


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
) -> Response | None:
    """Forward a request for ``/{prefix}/{agent}/{name}/...`` to that agent.

    ``{agent}`` is a Durable Object binding's name in kebab case
    (``ChatRoom`` → ``chat-room``); the agent is ``idFromName(name)``. The
    request is forwarded unchanged, URL included.

    Parameters
    ----------
    request
        The incoming request.
    env
        The Worker's ``env``, scanned for Durable Object bindings.
    prefix
        The path prefix (may contain ``/``).
    cors
        ``True`` for permissive CORS headers, or the headers to use; answers
        ``OPTIONS`` preflights and adds them to non-WebSocket responses.
    jurisdiction
        Restrict the agent to a jurisdiction (e.g. ``"eu"``).
    location_hint
        Where to create the agent if it doesn't exist yet.
    routing_retry
        Retries on errors the platform marks transient (``None`` for the
        defaults, ``False`` to disable).
    on_before_connect
        Runs before a WebSocket upgrade is forwarded (e.g. authorization).
    on_before_request
        Runs before any other request is forwarded.

    Returns
    -------
    Response | None
        The agent's response; ``None`` if the path isn't an agent route; a
        400 if it names no binding.
    """
    prefix_parts = prefix.split("/")
    parts = [part for part in urlsplit(request.url).path.split("/") if part]
    if parts[: len(prefix_parts)] != prefix_parts or len(parts) < len(prefix_parts) + 2:
        return None
    agent_segment, name = parts[len(prefix_parts)], parts[len(prefix_parts) + 1]
    binding = _bindings(env).get(agent_segment)
    if binding is None:
        _log.error(
            "The URL %s with namespace %r and name %r does not match any "
            "Durable Object binding",
            request.url,
            agent_segment,
            name,
        )
        return Response("Invalid request", status=400)
    class_name, namespace = binding

    cors_headers = _cors_headers(cors)
    upgrade = is_upgrade_request(request)
    if request.method == "OPTIONS" and cors_headers is not None:
        return Response(None, headers=cors_headers)

    def with_cors(response: Response) -> Response:
        if cors_headers is None or upgrade:
            return response
        return _ffi.with_headers(response, cors_headers)

    route = AgentRoute(class_name=class_name, name=name)
    hook = on_before_connect if upgrade else on_before_request
    if hook is not None:
        outcome = await hook(request, route)
        if isinstance(outcome, Response):
            return outcome if upgrade else with_cors(outcome)
        if isinstance(outcome, Request):
            request = outcome

    stub = _stub(namespace, name, jurisdiction, location_hint)
    forwarded = request
    response = await _with_routing_retry(
        lambda: stub.fetch(_ffi.clone_request(forwarded)),
        name=name,
        class_name=class_name,
        options=routing_retry,
    )
    return response if upgrade else with_cors(response)


async def get_agent_by_name(
    namespace: Any,
    name: str,
    *,
    jurisdiction: str | None = None,
    location_hint: str | None = None,
    routing_retry: RoutingRetry = None,
) -> Any:
    """Return a started agent's stub, for native RPC.

    Native RPC bypasses ``fetch``, where startup normally happens, so the
    agent's startup is run first.

    Parameters
    ----------
    namespace
        The agent's Durable Object binding (``self.env.MyAgent``).
    name
        The instance name.
    jurisdiction
        Restrict the agent to a jurisdiction.
    location_hint
        Where to create the agent if it doesn't exist yet.
    routing_retry
        Retries on errors the platform marks transient.

    Returns
    -------
    Any
        The Durable Object stub; ``await stub.method(...)`` calls the agent.
    """
    stub = _stub(namespace, name, jurisdiction, location_hint)
    await _with_routing_retry(
        lambda: getattr(stub, "__unsafe_ensureInitialized")(),
        name=name,
        class_name=None,
        options=routing_retry,
    )
    return stub


async def route_sub_agent_request(
    request: Request, parent: Any, *, from_path: str | None = None
) -> Response:
    """Forward a request for a sub-agent through its parent (custom routing).

    For URLs ``route_agent_request`` doesn't route: the parent's
    ``on_before_sub_agent`` gate runs, then the sub-agent gets the request.

    Parameters
    ----------
    request
        The incoming request.
    parent
        The parent agent's stub (e.g. from `get_agent_by_name`).
    from_path
        The ``/sub/{class}/{name}/...`` path to route on, when it isn't the
        request's own path.

    Returns
    -------
    Response
        The sub-agent's response; 400 if there's no ``/sub/`` path.
    """
    path = from_path if from_path is not None else urlsplit(request.url).path
    if parse_sub_agent_path(f"http://placeholder/{path.lstrip('/')}") is None:
        return Response("Sub-agent path not found in request URL", status=400)
    forward = (
        _ffi.request_with(request, url=rewrite_pathname(request.url, path))
        if from_path is not None
        else request
    )
    return await parent.fetch(forward)


async def get_sub_agent_by_name(parent: Any, cls: type, name: str) -> SubAgentStub:
    """Return a stub for a sub-agent, called through its parent.

    Each call is one extra hop through the parent, and the parent's
    ``on_before_sub_agent`` gate doesn't run (as `get_agent_by_name` doesn't
    run ``on_before_connect``). Method calls only; no ``fetch``.

    Raises
    ------
    ValueError
        If ``name`` contains NUL.
    """
    if "\0" in name:
        raise ValueError("Sub-agent names can't contain NUL (\\0)")
    return SubAgentStub(parent, cls.__name__, name)


def _bindings(env: Any) -> dict[str, tuple[str, Any]]:
    """Return the env's Durable Object bindings by kebab-case name."""
    cached = _namespaces.get(env)
    if cached is not None:
        return cached
    bindings: dict[str, tuple[str, Any]] = {}
    for binding_name in _ffi.env_binding_names(env):
        value = getattr(env, binding_name, None)
        if value is not None and hasattr(value, "idFromName"):
            bindings[camel_case_to_kebab_case(binding_name)] = (binding_name, value)
    _namespaces[env] = bindings
    return bindings


def _stub(
    namespace: Any, name: str, jurisdiction: str | None, location_hint: str | None
) -> Any:
    target = namespace.jurisdiction(jurisdiction) if jurisdiction else namespace
    id = target.idFromName(name)
    if location_hint:
        return target.get(id, {"locationHint": location_hint})
    return target.get(id)


def _cors_headers(cors: bool | Mapping[str, str]) -> dict[str, str] | None:
    if cors is True:
        return dict(_PERMISSIVE_CORS)
    if cors is False:
        return None
    return dict(cors)


async def _with_routing_retry[T](
    operation: Callable[[], Awaitable[T]],
    *,
    name: str,
    class_name: str | None,
    options: RoutingRetry,
) -> T:
    """Run ``operation``, retrying errors the platform marks transient.

    Port of upstream ``retryDurableObjectOperation``, including its backoff
    (a random wait up to ``min(max_delay, base_delay * 2 ** (attempt - 1))``).
    """
    if options is False:
        return await operation()
    resolved = options if options is not None else RoutingRetryOptions()
    _validate(resolved)
    attempt = 1
    while True:
        try:
            return await operation()
        except Exception as error:
            if attempt + 1 > resolved.max_attempts or not _is_transient(error):
                raise
            upper = min(
                to_seconds(resolved.max_delay),
                to_seconds(resolved.base_delay) * 2 ** (attempt - 1),
            )
            delay = timedelta(seconds=random.random() * upper)
            if resolved.on_retry is not None:
                event = RoutingRetryEvent(
                    error=error,
                    attempt=attempt,
                    max_attempts=resolved.max_attempts,
                    delay=delay,
                    name=name,
                    class_name=class_name,
                )
                try:
                    await resolved.on_retry(event)
                except Exception:
                    _log.warning("Routing retry callback failed", exc_info=True)
            await asyncio.sleep(delay.total_seconds())
            attempt += 1


def _is_transient(error: Exception) -> bool:
    # The platform's flags: retryable, but not because the object is overloaded.
    return (
        getattr(error, "retryable", False) is True
        and getattr(error, "overloaded", False) is not True
    )


def _validate(options: RoutingRetryOptions) -> None:
    """Check routing retry options where they're given.

    Raises
    ------
    ValueError
        If ``max_attempts`` is below 1, a delay isn't positive and finite, or
        ``base_delay`` exceeds ``max_delay``.
    """
    if options.max_attempts < 1:
        raise ValueError("routing_retry.max_attempts must be >= 1")
    base, maximum = to_seconds(options.base_delay), to_seconds(options.max_delay)
    for label, seconds in (("base_delay", base), ("max_delay", maximum)):
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError(f"routing_retry.{label} must be positive and finite")
    if base > maximum:
        raise ValueError("routing_retry.base_delay must be <= max_delay")
