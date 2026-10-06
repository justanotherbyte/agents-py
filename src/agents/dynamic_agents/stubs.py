"""Stubs for calling agents over native RPC: ``await stub.method(*args)``.

Arguments and results are converted with the Workers SDK's RPC rules.
"""

from collections.abc import Callable, Coroutine
from typing import Any

from workers import Request, Response

from .. import _ffi
from .paths import path_to_json
from .types import AgentPathStep

__all__ = ("AgentStub", "PathStub", "SubAgentStub")


class AgentStub:
    """A stub for one agent; every attribute is an async RPC method.

    Parameters
    ----------
    stub
        The runtime's stub (a facet or a Durable Object stub).
    """

    __slots__ = ("_stub",)

    def __init__(self, stub: Any) -> None:
        self._stub = stub

    def __getattr__(self, method: str) -> Callable[..., Coroutine[Any, Any, Any]]:
        """Return an async function calling ``method`` on the agent."""
        if method.startswith("__"):
            raise AttributeError(method)

        async def call(*args: Any) -> Any:
            return await _ffi.call_rpc(self._stub, method, *args)

        return call

    async def fetch(self, request: Request) -> Response:
        """Send an HTTP request to the agent's ``fetch``."""
        return await _ffi.facet_fetch(self._stub, request)


class PathStub:
    """A stub for an agent reached through the root (a facet parent).

    Parameters
    ----------
    root
        The root agent's stub.
    path
        The target agent's path, root first.
    """

    __slots__ = ("_path", "_root")

    def __init__(self, root: Any, path: list[AgentPathStep]) -> None:
        self._root = root
        self._path = path_to_json(path)

    def __getattr__(self, method: str) -> Callable[..., Coroutine[Any, Any, Any]]:
        """Return an async function calling ``method`` on the agent."""
        if method.startswith("__"):
            raise AttributeError(method)

        async def call(*args: Any) -> Any:
            return await _ffi.call_rpc(
                self._root, "_cf_invoke_sub_agent_path", self._path, method, list(args)
            )

        return call


class SubAgentStub:
    """A stub for a sub-agent, through its parent (``get_sub_agent_by_name``).

    Each call is one extra hop through the parent; the parent's
    ``on_before_sub_agent`` gate doesn't run.

    Parameters
    ----------
    parent
        The parent agent's stub.
    class_name
        The sub-agent's class name.
    name
        The sub-agent's name.
    """

    __slots__ = ("_class_name", "_name", "_parent")

    def __init__(self, parent: Any, class_name: str, name: str) -> None:
        self._parent = parent
        self._class_name = class_name
        self._name = name

    def __getattr__(self, method: str) -> Callable[..., Coroutine[Any, Any, Any]]:
        """Return an async function calling ``method`` on the sub-agent."""
        if method.startswith("__"):
            raise AttributeError(method)

        async def call(*args: Any) -> Any:
            return await _ffi.call_rpc(
                self._parent,
                "_cf_invoke_sub_agent",
                self._class_name,
                self._name,
                method,
                list(args),
            )

        return call
