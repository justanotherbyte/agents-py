"""Types for the WebSockets capability."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypedDict

from ..state.types import StateSource

if TYPE_CHECKING:
    from workers import Request

    from .connection import Connection

__all__ = (
    "Attachment",
    "CallableMetadata",
    "ConnectionContext",
    "ConnectionDecision",
    "ConnectionMeta",
    "ConnectionTagger",
    "RpcRequest",
    "SyncedState",
    "WebSocketHandlers",
)


@dataclass(slots=True, kw_only=True, frozen=True)
class CallableMetadata:
    """What ``@callable`` records about a method.

    Parameters
    ----------
    description
        What the method does.
    streaming
        Whether the method streams its result (a `StreamingResponse` first
        argument, or an async generator).
    """

    description: str | None = None
    streaming: bool = False


class RpcRequest(TypedDict):
    """An ``rpc`` frame from a client (wire §5.4)."""

    type: str
    id: str
    method: str
    args: list[Any]


@dataclass(slots=True, kw_only=True, frozen=True)
class ConnectionContext:
    """What a connection was opened with."""

    request: "Request"
    """The WebSocket upgrade request."""


@dataclass(slots=True, kw_only=True)
class WebSocketHandlers:
    """Connection hooks; each runs in the host context with the connection.

    Parameters
    ----------
    on_connect
        A connection was accepted (after the protocol frames, if any).
    on_message
        A frame the SDK didn't consume arrived. For handlers added with
        ``WebSockets.use``, returning ``True`` claims it, so later handlers
        don't see it.
    on_close
        The client closed the connection.
    on_error
        The connection failed.
    """

    on_connect: "Callable[[Connection, ConnectionContext], Awaitable[None]] | None" = (
        None
    )
    on_message: "Callable[[Connection, str | bytes], Awaitable[bool | None]] | None" = (
        None
    )
    on_close: "Callable[[Connection, int, str, bool], Awaitable[None]] | None" = None
    on_error: "Callable[[Connection, BaseException], Awaitable[None]] | None" = None


type ConnectionDecision = Callable[[Connection, ConnectionContext], Awaitable[bool]]
"""Decides something about a new connection, before any frame is sent."""

type ConnectionTagger = Callable[[Connection, ConnectionContext], Awaitable[list[str]]]
"""Returns the tags to accept a new connection under."""


class SyncedState(Protocol):
    """The part of a `State` capability that WebSockets syncs to clients."""

    def get(self) -> Any:
        """Return the current state, or ``None`` when nothing is stored."""
        ...

    def set(self, state: Any, source: StateSource) -> None:
        """Validate and save a change; raise to reject it."""
        ...


class ConnectionMeta(TypedDict):
    """The SDK's record of a connection, kept in its socket's attachment."""

    id: str
    tags: list[str]
    uri: str | None


Attachment = TypedDict(
    "Attachment",
    {"__pk": ConnectionMeta, "__user": Any, "__flags": dict[str, Any]},
    total=False,
)
"""A managed socket's hibernation attachment: the SDK's metadata
(``__pk``), the connection state (``__user``), and internal flags
(``__flags``). Limited to 16 KiB in total."""
