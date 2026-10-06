"""The ambient host context: which object, connection, and request is running.

Port of upstream ``lifecycle/current-agent.ts``, with a ``ContextVar`` in place
of ``AsyncLocalStorage``. Host hooks and user callbacks run inside it;
capability hooks run outside it. The typed public ``get_current_agent()``
lives in ``agents.agent`` (``.design/agent_api.md`` §1.16).
"""

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any

from .types import Connection, HostContext

__all__ = (
    "call_in_host_context",
    "current_host_context",
    "run_in_host_context",
    "run_without_host_context",
)

_current: ContextVar[HostContext | None] = ContextVar(
    "agents_host_context", default=None
)


def current_host_context() -> HostContext | None:
    """Return the running host context, or ``None`` outside one."""
    return _current.get()


async def run_in_host_context[T](
    host: object,
    fn: Callable[[], Awaitable[T]],
    *,
    connection: Connection | None = None,
    request: Any | None = None,
) -> T:
    """Run ``fn`` with ``host`` (and the connection or request) as the context.

    Tasks ``fn`` starts inherit the context.
    """
    token = _current.set(HostContext(host=host, connection=connection, request=request))
    try:
        return await fn()
    finally:
        _current.reset(token)


def call_in_host_context[T](host: object, fn: Callable[[], T]) -> T:
    """Call synchronous ``fn`` with ``host`` as the context (no connection)."""
    token = _current.set(HostContext(host=host))
    try:
        return fn()
    finally:
        _current.reset(token)


async def run_without_host_context[T](fn: Callable[[], Awaitable[T]]) -> T:
    """Run ``fn`` with no host context (how capability hooks run)."""
    token = _current.set(None)
    try:
        return await fn()
    finally:
        _current.reset(token)
