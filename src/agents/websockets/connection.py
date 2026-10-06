"""A client's WebSocket connection, backed by a hibernatable socket.

Port of upstream ``websockets/connection.ts`` and ``connection-flags.ts``.
Everything the SDK knows about a connection lives in its socket's
hibernation attachment, so it survives the object leaving memory
(``.design/agent_api.md`` §1.9). Wrappers are cheap and recreated on each
wake; two wrappers of the same socket are equal.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Generic, Self, cast

from typing_extensions import TypeVar

from .. import _ffi
from ..lifecycle.types import WebSocket
from .errors import ConnectionStateTooLargeError
from .types import Attachment, ConnectionMeta

__all__ = ("Connection",)

ConnState = TypeVar("ConnState", bound=Mapping[str, Any], default=dict[str, Any])

_OPEN = 1
_READONLY = "readonly"
_NO_PROTOCOL = "no_protocol"


class Connection(Generic[ConnState]):
    """One client connection.

    ``id``, ``uri``, and ``tags`` are fixed when the connection is accepted;
    ``state`` and the flags are read from the socket's attachment each time.
    Equal connections wrap the same socket (ids are client-chosen, so two
    sockets can share one).
    """

    __slots__ = ("_id", "_tags", "_uri", "_ws")

    def __init__(self, ws: WebSocket, meta: ConnectionMeta) -> None:
        self._ws = ws
        self._id = meta["id"]
        self._uri = meta["uri"]
        self._tags = tuple(meta["tags"])

    @classmethod
    def _from_socket(cls, ws: WebSocket) -> Self | None:
        """Wrap a socket the SDK accepted; ``None`` for anyone else's socket."""
        attachment = _ffi.read_attachment(ws)
        if not isinstance(attachment, dict):
            return None
        meta = attachment.get("__pk")
        if not isinstance(meta, dict) or not isinstance(meta.get("id"), str):
            return None
        uri = meta.get("uri")
        return cls(
            ws,
            ConnectionMeta(
                id=meta["id"],
                tags=[tag for tag in meta.get("tags") or () if isinstance(tag, str)],
                uri=uri if isinstance(uri, str) else None,
            ),
        )

    @property
    def id(self) -> str:
        """The connection id (the client's ``_pk``, or generated)."""
        return self._id

    @property
    def uri(self) -> str | None:
        """The URL the connection was opened with."""
        return self._uri

    @property
    def tags(self) -> Sequence[str]:
        """The tags the connection was accepted under; the id comes first."""
        return self._tags

    # State
    def _attachment(self) -> Attachment:
        return cast(Attachment, _ffi.read_attachment(self._ws))

    def _write(self, attachment: Attachment) -> None:
        try:
            _ffi.write_attachment(self._ws, attachment)
        except _ffi.JsException as error:
            if "cannot be larger" not in str(error):
                raise
            raise ConnectionStateTooLargeError(
                f"Connection {self._id!r} state doesn't fit in its 16 KiB "
                "socket attachment"
            ) from error

    @property
    def state(self) -> ConnState | None:
        """The connection's own state; replace it with `set_state`."""
        return self._attachment().get("__user")

    def set_state(self, state: ConnState | None) -> None:
        """Replace this connection's state (JSON, kept across hibernation).

        Raises
        ------
        ConnectionStateTooLargeError
            If the state doesn't fit in the socket's 16 KiB attachment.
        """
        attachment = self._attachment()
        attachment["__user"] = state
        self._write(attachment)

    # Flags
    def _flag(self, key: str) -> Any:
        return self._attachment().get("__flags", {}).get(key)

    def _set_flag(self, key: str, value: Any) -> None:
        """Set an internal flag; ``None`` removes it."""
        attachment = self._attachment()
        flags = attachment.get("__flags", {})
        if value is None:
            flags.pop(key, None)
        else:
            flags[key] = value
        if flags:
            attachment["__flags"] = flags
        else:
            attachment.pop("__flags", None)
        self._write(attachment)

    @property
    def readonly(self) -> bool:
        """Whether the client is refused when it changes the host's state."""
        return bool(self._flag(_READONLY))

    @readonly.setter
    def readonly(self, value: bool) -> None:
        self._set_flag(_READONLY, True if value else None)

    @property
    def protocol_enabled(self) -> bool:
        """Whether protocol frames (identity, state) are sent to this client.

        Decided when the connection opens.
        """
        return not self._flag(_NO_PROTOCOL)

    def _set_protocol_enabled(self, enabled: bool) -> None:
        self._set_flag(_NO_PROTOCOL, None if enabled else True)

    # The socket
    @property
    def _is_open(self) -> bool:
        return self._ws.readyState == _OPEN

    def send(self, message: str | bytes) -> None:
        """Send a text or binary frame.

        Raises
        ------
        JsException
            If the socket has closed.
        """
        _ffi.send_message(self._ws, message)

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Close the connection.

        ``1008`` and ``4000``-``4999`` tell the client not to reconnect.
        """
        if code is None:
            self._ws.close()
        elif reason is None:
            self._ws.close(code)
        else:
            self._ws.close(code, reason)

    def __eq__(self, other: object) -> bool:
        """Return whether ``other`` wraps the same socket."""
        return isinstance(other, Connection) and bool(self._ws == other._ws)

    def __hash__(self) -> int:
        """Hash the id (equal connections share one; JS sockets are unhashable)."""
        return hash(self._id)

    def __repr__(self) -> str:
        """Show the id and tags."""
        return f"Connection(id={self._id!r}, tags={self._tags!r})"
