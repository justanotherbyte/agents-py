"""WebSocket connections and the Agents connect protocol (upstream ``websockets/``)."""

from .connection import Connection
from .errors import ConnectionStateTooLargeError, DuplicateConnectionIdError
from .rpc import StreamingResponse, callable
from .types import CallableMetadata, ConnectionContext, WebSocketHandlers
from .websockets import WebSockets

__all__ = (
    "CallableMetadata",
    "Connection",
    "ConnectionContext",
    "ConnectionStateTooLargeError",
    "DuplicateConnectionIdError",
    "StreamingResponse",
    "WebSocketHandlers",
    "WebSockets",
    "callable",
)
