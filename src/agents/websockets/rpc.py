"""``@callable`` methods answered over ``rpc`` frames.

Port of upstream ``callable-decorator.ts``, ``StreamingResponse``, and the
``rpc`` handling in ``Agent.onMessage`` / ``WebSockets#answerRpc``
(``.design/agent_api.md`` §1.8, wire §5.4). Methods stream either by taking a
`StreamingResponse` first (``streaming=True``) or by being async generators.
"""

import inspect
import logging
from collections.abc import AsyncGenerator, Callable, Mapping
from contextlib import suppress
from typing import Any, overload

from .. import _ffi
from ..core.encoding import to_json
from .connection import Connection
from .types import CallableMetadata, RpcRequest

__all__ = (
    "RpcDispatcher",
    "StreamingResponse",
    "callable",
    "callable_methods",
    "is_rpc_request",
)

_log = logging.getLogger("agents.websockets")

_METADATA_ATTR = "__agents_callable__"


@overload
def callable[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def callable[F: Callable[..., Any]](
    *, description: str | None = None, streaming: bool = False
) -> Callable[[F], F]: ...
def callable(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    description: str | None = None,
    streaming: bool = False,
) -> Any:
    """Make an ``async`` method callable by clients over ``rpc`` frames.

    Works bare (``@callable``) or with options (``@callable(streaming=True)``).
    A ``streaming=True`` method gets a `StreamingResponse` before the client's
    arguments; an async generator streams one chunk per ``yield``. A
    subclass override must be decorated again to stay callable.

    Parameters
    ----------
    fn
        The method, when used bare.
    description
        What the method does (reported by ``get_callable_methods``).
    streaming
        Pass a `StreamingResponse` as the first argument.

    Raises
    ------
    TypeError
        If the method isn't ``async``, or is an async generator declared
        ``streaming=True``.
    """

    def decorate(method: Callable[..., Any]) -> Callable[..., Any]:
        name = getattr(method, "__qualname__", repr(method))
        generator = inspect.isasyncgenfunction(method)
        if not generator and not inspect.iscoroutinefunction(method):
            raise TypeError(f"Callable methods must be async: {name}")
        if generator and streaming:
            raise TypeError(
                f"{name} is an async generator, which streams "
                "already; drop streaming=True"
            )
        metadata = CallableMetadata(
            description=description, streaming=streaming or generator
        )
        setattr(method, _METADATA_ATTR, metadata)
        return method

    return decorate(fn) if fn is not None else decorate


def callable_metadata(method: object) -> CallableMetadata | None:
    """Return the metadata ``@callable`` recorded on ``method``, if any."""
    metadata = getattr(method, _METADATA_ATTR, None)
    return metadata if isinstance(metadata, CallableMetadata) else None


def callable_methods(target: object) -> dict[str, CallableMetadata]:
    """Return ``target``'s ``@callable`` methods by name.

    The nearest definition in the class hierarchy wins, so an undecorated
    override makes a method uncallable.
    """
    methods: dict[str, CallableMetadata] = {}
    seen: set[str] = set()
    for cls in type(target).__mro__:
        for name, value in vars(cls).items():
            if name in seen:
                continue
            seen.add(name)
            metadata = callable_metadata(value)
            if metadata is not None:
                methods[name] = metadata
    return methods


def is_rpc_request(frame: object) -> bool:
    """Return whether a parsed frame is a well-formed ``rpc`` request."""
    return (
        isinstance(frame, dict)
        and frame.get("type") == "rpc"
        and isinstance(frame.get("id"), str)
        and isinstance(frame.get("method"), str)
        and isinstance(frame.get("args"), list)
    )


class StreamingResponse:
    """Sends one streaming call's results: chunks, then an end or an error.

    Passed as the first argument to ``@callable(streaming=True)`` methods.
    Every method is a no-op returning ``False`` once the stream is closed.
    """

    __slots__ = ("_closed", "_connection", "_id")

    def __init__(self, connection: Connection, id: str) -> None:
        self._connection = connection
        self._id = id
        self._closed = False

    @property
    def is_closed(self) -> bool:
        """Whether `end` or `error` has been called."""
        return self._closed

    def send(self, chunk: Any) -> bool:
        """Send one chunk; return ``False`` if closed or the client has gone.

        Raises
        ------
        TypeError
            If ``chunk`` isn't JSON-serializable.
        """
        if self._closed:
            _log.warning("StreamingResponse.send() after the stream closed; dropped")
            return False
        frame = {"type": "rpc", "id": self._id, "success": True, "done": False}
        return _send(self._connection, to_json({**frame, "result": chunk}))

    def end(self, final: Any = None) -> bool:
        """Close the stream, with an optional final result.

        ``None`` means "no result": the client's call resolves with
        ``undefined`` (``.design/agent_api.md`` §1.8).
        """
        if self._closed:
            return False
        self._closed = True
        return _send(self._connection, _done_frame(self._id, final))

    def error(self, message: str) -> bool:
        """Close the stream with an error; the client's call rejects."""
        if self._closed:
            return False
        self._closed = True
        return _send(self._connection, _error_frame(self._id, message))


class RpcDispatcher:
    """Answers ``rpc`` frames by calling a target's ``@callable`` methods.

    The caller runs `answer` in the host context, with the calling
    connection.

    Parameters
    ----------
    target
        The object whose ``@callable`` methods clients may call.
    emit
        Publishes the ``rpc`` and ``rpc:error`` events.
    """

    __slots__ = ("_emit", "_methods", "_target")

    def __init__(self, target: object, emit: Callable[[str, Any], None]) -> None:
        self._target = target
        self._methods: Mapping[str, CallableMetadata] = callable_methods(target)
        self._emit = emit

    async def answer(self, connection: Connection, request: RpcRequest) -> None:
        """Call the requested method and send its result, chunks, or error."""
        name, id, args = request["method"], request["id"], request["args"]
        metadata = self._methods.get(name)
        if metadata is None:
            reason = (
                "is not callable" if hasattr(self._target, name) else "does not exist"
            )
            self._fail(connection, id, name, f"Method {name} {reason}")
            return
        method = getattr(self._target, name)
        if metadata.streaming:
            await self._stream(connection, id, name, method, args)
            return
        try:
            result = await method(*args)
        except Exception as error:
            _log.exception("RPC method %r failed", name)
            self._fail(connection, id, name, _message(error))
            return
        self._emit("rpc", {"method": name})
        try:
            text = _done_frame(id, result)
        except (TypeError, ValueError) as error:
            # A result JSON can't carry must still settle the client's call.
            text = _error_frame(id, f"Result is not JSON-serializable: {error}")
        _send(connection, text)

    async def _stream(
        self,
        connection: Connection,
        id: str,
        name: str,
        method: Callable[..., Any],
        args: list[Any],
    ) -> None:
        self._emit("rpc", {"method": name, "streaming": True})
        response = StreamingResponse(connection, id)
        try:
            if inspect.isasyncgenfunction(method):
                await _drain(method(*args), response)
            else:
                await method(response, *args)
        except Exception as error:
            _log.exception("Streaming RPC method %r failed", name)
            self._emit("rpc:error", {"method": name, "error": _message(error)})
            response.error(_message(error))
            return
        # A method that returns without closing would leave the client waiting.
        response.end()

    def _fail(self, connection: Connection, id: str, name: str, message: str) -> None:
        self._emit("rpc:error", {"method": name, "error": message})
        _send(connection, _error_frame(id, message))


async def _drain(chunks: AsyncGenerator[Any], response: StreamingResponse) -> None:
    """Send each yielded chunk; stop (closing the generator) if the client goes."""
    try:
        async for chunk in chunks:
            if not response.send(chunk):
                break
    finally:
        await chunks.aclose()


def _message(error: Exception) -> str:
    # Only the message crosses the wire; an empty one falls back to the type.
    return str(error) or type(error).__name__


def _done_frame(id: str, result: Any) -> str:
    frame: dict[str, Any] = {"type": "rpc", "id": id, "success": True, "done": True}
    if result is not None:
        frame["result"] = result
    return to_json(frame)


def _error_frame(id: str, message: str) -> str:
    return to_json({"type": "rpc", "id": id, "success": False, "error": message})


def _send(connection: Connection, text: str) -> bool:
    """Send a response; ``False`` if the client has gone (it's dropped)."""
    if not connection._is_open:
        return False
    with suppress(_ffi.JsException):
        connection.send(text)
        return True
    return False
