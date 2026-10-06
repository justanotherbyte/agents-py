"""Types for sub-agents (facets)."""

from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol, TypedDict

if TYPE_CHECKING:
    from workers import Request, Response

    from ..lifecycle.lifecycle import Lifecycle
    from ..websockets.connection import Connection
    from ..websockets.websockets import WebSockets

__all__ = (
    "AgentPathStep",
    "AgentRoute",
    "ForwardedConnection",
    "SubAgentInfo",
    "SubAgentPathMatch",
    "SubAgentsHost",
    "WireEnvelope",
    "WireRouteAddress",
)


class AgentRoute(NamedTuple):
    """Which agent instance a request is for."""

    class_name: str
    """The class name: the env binding name from ``route_agent_request``,
    the sub-agent's class for ``on_before_sub_agent``."""
    name: str
    """The instance name."""


class AgentPathStep(NamedTuple):
    """One agent in a path from the root: its class name and instance name."""

    class_name: str
    name: str


@dataclass(slots=True, kw_only=True)
class SubAgentInfo:
    """A sub-agent a parent has created (from its registry)."""

    class_name: str
    name: str
    created_at: datetime


class SubAgentPathMatch(NamedTuple):
    """The first ``/sub/{class}/{name}`` hop of a URL, and what follows it."""

    child_class: str
    child_name: str
    remaining_path: str


class ForwardedConnection(TypedDict):
    """A root-owned connection as a sub-agent sees it (sent with each event)."""

    id: str
    uri: str | None
    tags: list[str]
    state: Any
    flags: dict[str, Any]
    request_headers: list[list[str]] | None


class WireRouteAddress(TypedDict):
    """A `RouteAddress` across RPC."""

    key: str
    data: str


class WireEnvelope(TypedDict):
    """A `RouteEnvelope` across RPC."""

    capability: str
    source: WireRouteAddress | None
    payload: Any


class SubAgentsHost(Protocol):
    """What the sub-agents engine needs from its agent (``Agent`` provides it)."""

    @property
    def ctx(self) -> Any:
        """The Durable Object context."""
        ...

    @property
    def lifecycle(self) -> "Lifecycle":
        """The agent's Lifecycle."""
        ...

    @property
    def _websockets(self) -> "WebSockets":
        """The agent's WebSockets capability (the root's native sockets)."""
        ...

    async def on_before_sub_agent(
        self, request: "Request", child: Any
    ) -> "Request | Response | None":
        """Gate a request to a sub-agent (the parent's hook)."""
        ...

    def _connect_virtual(
        self, connection: "Connection", request: "Request"
    ) -> Awaitable[None]:
        """Run the connect sequence for a forwarded connection."""
        ...

    def _message_locally(
        self, connection: "Connection", message: str | bytes
    ) -> Awaitable[None]:
        """Handle a frame without forwarding it."""
        ...

    def _close_locally(
        self, connection: "Connection", code: int, reason: str, was_clean: bool
    ) -> Awaitable[None]:
        """Handle a close without forwarding it."""
        ...

    def _cleanup_route_prefix(self, prefix: str) -> Awaitable[None]:
        """Cancel the root's routed work for a facet subtree."""
        ...
