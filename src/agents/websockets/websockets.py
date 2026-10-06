"""WebSocket connections for a Lifecycle object: the WebSockets capability.

Port of upstream ``websockets/websockets.ts`` (without the Cap'n Web wire,
which is out of scope). The capability claims every upgrade, accepts it with
the Hibernation API, runs the connection handlers in the host context,
completes close handshakes, and speaks the connect sequence and state sync of
the Agents protocol (``.design/agents_wire_protocol.md`` §3, §5.2).
"""

import json
import logging
import secrets
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Any, cast, override
from urllib.parse import parse_qs, urlsplit

from workers import Response

from .. import _ffi
from ..core.naming import camel_case_to_kebab_case
from ..lifecycle.capability import LifecycleCapability
from ..lifecycle.types import WebSocket
from .connection import Connection
from .errors import DuplicateConnectionIdError
from .rpc import RpcDispatcher, is_rpc_request
from .types import (
    ConnectionContext,
    ConnectionDecision,
    ConnectionMeta,
    ConnectionTagger,
    RpcRequest,
    SyncedState,
    WebSocketHandlers,
)

if TYPE_CHECKING:
    from workers import Request

__all__ = ("WebSockets",)

_log = logging.getLogger("agents.websockets")

_IDENTITY = "cf_agent_identity"
_STATE = "cf_agent_state"
_STATE_ERROR = "cf_agent_state_error"

# Close codes the runtime makes up when no Close frame arrived; they can't be
# sent, and there's no peer left to answer.
_RESERVED_CLOSE_CODES = frozenset({1005, 1006, 1015})

_MAX_TAGS = 10
_MAX_TAG_LENGTH = 256


class WebSockets(LifecycleCapability):
    """Hibernatable WebSocket connections, with the Agents connect protocol.

    Accepts every upgrade (it's the catch-all), whether or not handlers are
    given. With ``state``, it pushes the current state after the identity
    frame on connect and applies the ``cf_agent_state`` frames clients send;
    broadcasting a change is the state owner's call (wire the `State`'s
    ``on_changed`` to `broadcast_state`).

    Parameters
    ----------
    handlers
        Connection hooks, run in the host context.
    protocol
        Whether new connections get the protocol frames (identity, then
        state): ``True`` for all, a decision per connection (``False`` marks
        it no-protocol: it never gets protocol frames, but works otherwise),
        or ``False`` for none, when the host sends them itself with
        `send_connect_frames` and applies state frames with
        `apply_state_frame` (``Agent`` does this).
    readonly
        Decides whether a new connection is readonly (its state changes are
        refused).
    state
        The state to sync to clients, normally the host's `State` capability.
    callables
        An object whose ``@callable`` methods clients may call with ``rpc``
        frames (``Agent`` passes itself).
    connection_tags
        Returns the tags to accept a new connection under (at most 9, plus
        the id, which is always the first tag).
    """

    claims = "catch-all"

    def __init__(
        self,
        *,
        handlers: WebSocketHandlers | None = None,
        protocol: bool | ConnectionDecision = True,
        readonly: ConnectionDecision | None = None,
        state: SyncedState | None = None,
        connection_tags: ConnectionTagger | None = None,
        callables: object | None = None,
    ) -> None:
        super().__init__("websockets")
        self._rpc = (
            RpcDispatcher(callables, self._emit) if callables is not None else None
        )
        self._handlers = handlers
        self._added: list[WebSocketHandlers] = []
        self._protocol = protocol
        self._readonly = readonly
        self._state = state
        self._connection_tags = connection_tags
        # Connections between their identity frame and their state frame:
        # reading state for the push can seed the initial value, whose
        # broadcast would otherwise reach them twice.
        self._connecting: set[Connection] = set()

    def use(self, handlers: WebSocketHandlers) -> None:
        """Add handlers for another component sharing these connections.

        They run before the configured handlers, and their ``on_message`` can
        claim a message by returning ``True``.
        """
        self._added.append(handlers)

    def _all_handlers(self) -> Sequence[WebSocketHandlers]:
        if self._handlers is None:
            return self._added
        return [*self._added, self._handlers]

    # Connections
    def get_connections(self, tag: str | None = None) -> Iterator[Connection]:
        """Yield the open connections, optionally only those with ``tag``."""
        for ws in self.lifecycle.websockets(tag):
            if ws.readyState != 1:
                continue
            connection = Connection._from_socket(ws)
            if connection is not None:
                yield connection

    def get_connection(self, id: str) -> Connection | None:
        """Return the open connection with ``id``, or ``None``.

        Raises
        ------
        DuplicateConnectionIdError
            If more than one open connection has that id.
        """
        matching = [c for c in self.get_connections(id) if c.id == id]
        if len(matching) > 1:
            raise DuplicateConnectionIdError(id)
        return matching[0] if matching else None

    def broadcast(
        self, message: str | bytes, *, exclude: Iterable[str | Connection] = ()
    ) -> None:
        """Send ``message`` to every open connection, except ``exclude``.

        An id excludes every connection with that id; a `Connection` excludes
        exactly that one.
        """
        excluded_ids = {item for item in exclude if isinstance(item, str)}
        excluded = [item for item in exclude if isinstance(item, Connection)]
        for connection in self.get_connections():
            if connection.id in excluded_ids or connection in excluded:
                continue
            _send(connection, message)

    # Lifecycle capability hooks
    @override
    async def on_websocket_upgrade(self, request: "Request") -> Response:
        """Accept an upgrade as a hibernatable connection."""
        client, server = _ffi.websocket_pair()
        query = parse_qs(urlsplit(request.url).query)
        # An empty `_pk` gets a generated id too: an empty tag is invalid.
        connection_id = (query.get("_pk") or [""])[0] or _new_connection_id()
        meta = ConnectionMeta(id=connection_id, tags=[connection_id], uri=request.url)
        # Written before accepting, so the tags hook can already use the
        # connection (including its state).
        _ffi.write_attachment(server, {"__pk": meta})
        connection = Connection(server, meta)
        context = ConnectionContext(request=request)

        if self._connection_tags is not None:
            tags = await self._decide(self._connection_tags, connection, context)
            meta["tags"] = _prepare_tags(connection_id, tags)
            attachment = _ffi.read_attachment(server)
            attachment["__pk"] = meta
            _ffi.write_attachment(server, attachment)
            connection = Connection(server, meta)
        self.lifecycle.accept_websocket(server, meta["tags"])

        await self._connect(connection, context)
        return Response(None, status=101, web_socket=client)

    @override
    async def on_websocket_message(self, ws: WebSocket, message: str | bytes) -> bool:
        """Handle a message on a connection this capability accepted."""
        connection = Connection._from_socket(ws)
        if connection is None:
            return False
        await self._message(connection, message)
        return True

    @override
    async def on_websocket_close(
        self, ws: WebSocket, code: int, reason: str, was_clean: bool
    ) -> bool:
        """Run the close handlers, then complete the close handshake."""
        connection = Connection._from_socket(ws)
        if connection is None:
            return False
        try:
            for handlers in self._all_handlers():
                if handlers.on_close is not None:
                    await self._in_host_context(
                        partial(handlers.on_close, connection, code, reason, was_clean),
                        connection,
                    )
        finally:
            _reciprocate_close(ws, code, reason)
        return True

    @override
    async def on_websocket_error(self, ws: WebSocket, error: BaseException) -> bool:
        """Run the error handlers."""
        connection = Connection._from_socket(ws)
        if connection is None:
            return False
        for handlers in self._all_handlers():
            if handlers.on_error is not None:
                await self._in_host_context(
                    partial(handlers.on_error, connection, error), connection
                )
        return True

    @override
    async def dispose(self) -> None:
        """Close every connection when the host is destroyed."""
        for connection in self.get_connections():
            with suppress(_ffi.JsException):  # closed meanwhile
                connection.close(1001, "Durable Object destroyed")

    # Connect and message dispatch
    async def _connect(
        self, connection: Connection, context: ConnectionContext
    ) -> None:
        # Flags first, so they're set before the client can respond.
        if self._readonly is not None and await self._decide(
            self._readonly, connection, context
        ):
            connection.readonly = True
        if self._protocol is True:
            self.send_connect_frames(connection)
        elif self._protocol is not False:
            if await self._decide(self._protocol, connection, context):
                self.send_connect_frames(connection)
            else:
                connection._set_protocol_enabled(False)
        for handlers in self._all_handlers():
            if handlers.on_connect is not None:
                await self._in_host_context(
                    partial(handlers.on_connect, connection, context),
                    connection,
                    context,
                )

    async def _message(self, connection: Connection, message: str | bytes) -> None:
        frame = _parse_frame(message)
        if frame is not None:
            if (
                self._state is not None
                and self._protocol is not False
                and _is_state_frame(frame)
            ):
                # In the host context, so the host's validator and change
                # hook see the sending connection.
                await self._in_host_context(
                    partial(self._apply_state_frame_async, connection, frame),
                    connection,
                )
                return
            if self._rpc is not None and is_rpc_request(frame):
                await self._in_host_context(
                    partial(self._rpc.answer, connection, cast(RpcRequest, frame)),
                    connection,
                )
                return
        for handlers in self._added:
            if handlers.on_message is not None:
                claimed = await self._in_host_context(
                    partial(handlers.on_message, connection, message), connection
                )
                if claimed is True:
                    return
        if self._handlers is not None and self._handlers.on_message is not None:
            await self._in_host_context(
                partial(self._handlers.on_message, connection, message), connection
            )

    def _emit(self, type: str, payload: Any) -> None:
        self.lifecycle.emit(type, payload)

    async def _apply_state_frame_async(
        self, connection: Connection, frame: dict[str, Any]
    ) -> None:
        self.apply_state_frame(connection, frame)

    async def _in_host_context[T](
        self,
        fn: Callable[[], Awaitable[T]],
        connection: Connection,
        context: ConnectionContext | None = None,
    ) -> T:
        return await self.lifecycle.run_in_host_context(
            fn,
            connection=connection,
            request=context.request if context is not None else None,
        )

    async def _decide[R](
        self,
        decision: Callable[[Connection, ConnectionContext], Awaitable[R]],
        connection: Connection,
        context: ConnectionContext,
    ) -> R:
        return await self._in_host_context(
            partial(decision, connection, context), connection, context
        )

    # Protocol frames
    def send_connect_frames(
        self,
        connection: Connection,
        *,
        name: str | None = None,
        agent: str | None = None,
    ) -> None:
        """Send the connect sequence: the identity frame, then the state.

        The identity carries ``stateFollows`` when state follows, so clients
        resolve ``ready`` only once it has arrived. Nothing is sent to a
        no-protocol connection. ``name`` and ``agent`` default as for
        `send_identity`.
        """
        if not connection.protocol_enabled:
            return
        self._connecting.add(connection)
        try:
            current = self._state.get() if self._state is not None else None
            # Serialized first: an identity promising state must never go
            # out without it.
            state_frame = (
                json.dumps({"type": _STATE, "state": current})
                if current is not None
                else None
            )
            identity = self._identity_frame(name, agent)
            if state_frame is not None:
                identity["stateFollows"] = True
            _send(connection, json.dumps(identity))
            if state_frame is not None:
                _send(connection, state_frame)
        finally:
            self._connecting.discard(connection)

    def send_identity(
        self,
        connection: Connection,
        *,
        name: str | None = None,
        agent: str | None = None,
    ) -> None:
        """Send the identity frame alone, unless the connection is no-protocol.

        Clients resolve ``ready`` on it, so a host that also pushes state on
        connect should use `send_connect_frames`. ``name`` defaults to the
        object's name and ``agent`` to the host class in kebab case; a host
        whose public identity differs (a facet) passes its own.
        """
        if connection.protocol_enabled:
            _send(connection, json.dumps(self._identity_frame(name, agent)))

    def _identity_frame(self, name: str | None, agent: str | None) -> dict[str, Any]:
        return {
            "type": _IDENTITY,
            "name": name if name is not None else self.lifecycle.name,
            "agent": agent
            if agent is not None
            else camel_case_to_kebab_case(self.lifecycle.class_name),
        }

    def send_state(self, connection: Connection) -> None:
        """Send the current state, unless there is none or it's no-protocol."""
        if self._state is None or not connection.protocol_enabled:
            return
        # Reading may seed the initial state; its broadcast must skip this
        # connection, which gets the value right here.
        self._connecting.add(connection)
        try:
            current = self._state.get()
        finally:
            self._connecting.discard(connection)
        if current is not None:
            _send(connection, json.dumps({"type": _STATE, "state": current}))

    def apply_state_frame(self, connection: Connection, frame: Any) -> bool:
        """Apply a parsed ``cf_agent_state`` frame from a client.

        A readonly connection is refused with ``cf_agent_state_error``; a
        change the validator rejects is logged in full and answered with a
        generic ``cf_agent_state_error``. Hosts that drive the protocol
        themselves call this inside the host context.

        Returns
        -------
        bool
            Whether the frame was a state frame (handled either way).
        """
        if self._state is None or not _is_state_frame(frame):
            return False
        if connection.readonly:
            _send_frame(
                connection, {"type": _STATE_ERROR, "error": "Connection is readonly"}
            )
            return True
        try:
            self._state.set(frame["state"], connection)
        except Exception:
            _log.exception("State update from connection %r rejected", connection.id)
            _send_frame(
                connection, {"type": _STATE_ERROR, "error": "State update rejected"}
            )
        return True

    def broadcast_state(self, exclude: Connection | None = None) -> None:
        """Push the current state to every protocol-enabled connection.

        ``exclude`` is the connection a change came from, which already has
        the value it sent. Wire a `State`'s ``on_changed`` to this.
        """
        if self._state is None:
            return
        current = self._state.get()
        if current is None:
            return
        text = json.dumps({"type": _STATE, "state": current})
        for connection in self.get_connections():
            if connection == exclude or connection in self._connecting:
                continue
            if connection.protocol_enabled:
                _send(connection, text)


def _new_connection_id() -> str:
    # 22 URL-safe characters, like upstream's nanoid().
    return secrets.token_urlsafe(16)


def _prepare_tags(connection_id: str, tags: list[str]) -> list[str]:
    """Return the tags to accept under: the id first, without duplicates.

    Raises
    ------
    ValueError
        If there are more than 10 tags (with the id), or a tag is empty,
        not a string, or longer than 256 characters (platform limits).
    """
    prepared = [connection_id, *(tag for tag in tags if tag != connection_id)]
    if len(prepared) > _MAX_TAGS:
        raise ValueError(
            f"A connection can have at most {_MAX_TAGS} tags, including its id"
        )
    for tag in prepared:
        if not isinstance(tag, str) or not tag:
            raise ValueError(f"Connection tags must be non-empty strings, not {tag!r}")
        if len(tag) > _MAX_TAG_LENGTH:
            raise ValueError(
                f"Connection tags must be at most {_MAX_TAG_LENGTH} characters"
            )
    return prepared


def _parse_frame(message: str | bytes) -> dict[str, Any] | None:
    """Return a text message as a JSON object, or ``None`` if it isn't one."""
    if not isinstance(message, str):
        return None
    try:
        frame = json.loads(message)
    except json.JSONDecodeError:
        return None
    return frame if isinstance(frame, dict) else None


def _is_state_frame(frame: Any) -> bool:
    return isinstance(frame, dict) and frame.get("type") == _STATE and "state" in frame


def _send(connection: Connection, text: str | bytes) -> None:
    """Send one frame, tolerating a client that disconnected meanwhile."""
    with suppress(_ffi.JsException):
        connection.send(text)


def _send_frame(connection: Connection, frame: dict[str, Any]) -> None:
    _send(connection, json.dumps(frame))


def _reciprocate_close(ws: WebSocket, code: int, reason: str) -> None:
    """Echo a client's Close frame, completing the handshake (best-effort)."""
    if code in _RESERVED_CLOSE_CODES:
        return
    with suppress(_ffi.JsException):  # already closed, or a code it can't send
        ws.close(code, reason)
