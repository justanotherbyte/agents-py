"""The boundary between Python and the Workers JavaScript runtime.

This is the only SDK module that imports ``js`` / ``pyodide``, so everything
else imports and unit-tests under CPython (tests replace this module). The
conversion rules are specified in ``.design/utilities.md`` §3.

Storage, SQL parameters, and hibernation attachments use structured clone,
so they go through `py_to_js` / `js_to_py`. Native DO RPC goes through the
Workers SDK's own converters, wrapped by `to_rpc` / `from_rpc`.
"""

import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Generator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from typing import Any

import js
from pyodide.ffi import (
    JsException,
    JsProxy,
    create_once_callable,
    create_proxy,
    jsnull,
    to_js,
)
from workers import Request, Response, python_from_rpc, python_to_rpc

__all__ = (
    "ConsoleHandler",
    "JsException",
    "JsProxy",
    "ProxyScope",
    "abort_without_alarm_retry",
    "block_concurrency_while",
    "call_rpc",
    "clone_request",
    "env_binding_names",
    "export_names",
    "facet_abort",
    "facet_delete",
    "facet_fetch",
    "facet_get",
    "from_rpc",
    "has_namespace",
    "js_to_py",
    "make_request",
    "namespace_stub",
    "proxies",
    "py_to_js",
    "read_attachment",
    "request_with",
    "send_message",
    "streaming_response",
    "to_rpc",
    "unwrap",
    "websocket_error_response",
    "websocket_pair",
    "websocket_rejection",
    "with_headers",
    "write_attachment",
)

_TYPED_ARRAY_TAGS = frozenset(
    {
        "[object ArrayBuffer]",
        "[object Uint8Array]",
        "[object Int8Array]",
        "[object Uint8ClampedArray]",
        "[object Uint16Array]",
        "[object Int16Array]",
        "[object Uint32Array]",
        "[object Int32Array]",
        "[object Float32Array]",
        "[object Float64Array]",
        "[object DataView]",
    }
)


def py_to_js(value: Any) -> Any:
    """Convert a Python value to a structured-clone JS value.

    Parameters
    ----------
    value
        ``None``, ``bool``, ``int``, ``float``, ``str``, ``bytes`` (or
        ``bytearray`` / ``memoryview``), a timezone-aware ``datetime``, or a
        ``list`` / ``dict`` (with ``str`` keys) of these.

    Returns
    -------
    Any
        The JS value: ``None`` becomes ``null``, a ``dict`` a plain object, a
        ``list`` an array, bytes a ``Uint8Array`` copy, a ``datetime`` a
        ``Date``.

    Raises
    ------
    TypeError
        For any other type (tuples, sets, objects), a non-``str`` dict key, or
        a naive ``datetime``.
    """
    return to_js(
        _prepare(value),
        dict_converter=js.Object.fromEntries,
        create_pyproxies=False,
    )


def _prepare(value: Any) -> Any:
    """Replace values `to_js` would get wrong, and reject unsupported types."""
    if value is None:
        return jsnull
    if isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value)
    if isinstance(value, datetime):
        return _js_date(value)
    if isinstance(value, list):
        return [_prepare(item) for item in value]
    if isinstance(value, dict):
        return {_require_str_key(key): _prepare(item) for key, item in value.items()}
    raise TypeError(f"{type(value).__name__} can't be stored as a JS value")


def _require_str_key(key: object) -> str:
    if not isinstance(key, str):
        raise TypeError(f"dict keys must be str, not {type(key).__name__}")
    return key


def _js_date(value: datetime) -> JsProxy:
    if value.tzinfo is None:
        raise TypeError("naive datetime; use a timezone-aware datetime")
    return js.Date.new(value.timestamp() * 1000)


def js_to_py(value: Any) -> Any:
    """Convert a structured-clone JS value to Python.

    Parameters
    ----------
    value
        A JS value, as returned by storage, SQL, or an attachment read.

    Returns
    -------
    Any
        ``None`` for ``null`` / ``undefined``; ``dict`` for a plain object;
        ``list`` for an array; ``bytes`` for an ``ArrayBuffer`` or typed array;
        a UTC ``datetime`` for a ``Date``; primitives unchanged.

    Raises
    ------
    TypeError
        For any other JS object (functions, maps, class instances).
    """
    if value is jsnull or value is None:
        return None
    if not isinstance(value, JsProxy):
        return value
    tag = str(js.Object.prototype.toString.call(value))
    if tag == "[object Array]":
        return [js_to_py(item) for item in value]
    if tag == "[object Object]":
        return {str(key): js_to_py(item) for key, item in js.Object.entries(value)}
    if tag in _TYPED_ARRAY_TAGS:
        return value.to_bytes()
    if tag == "[object Date]":
        return datetime.fromtimestamp(value.getTime() / 1000, UTC)
    raise TypeError(f"can't convert JS {tag} to a Python value")


def to_rpc(value: Any) -> Any:
    """Convert a value for native DO RPC (the Workers SDK's ``python_to_rpc``)."""
    return python_to_rpc(value)


def from_rpc(value: Any) -> Any:
    """Convert a native DO RPC result (the Workers SDK's ``python_from_rpc``)."""
    return python_from_rpc(value)


def unwrap(value: Any) -> Any:
    """Return the raw JS object behind a Workers SDK binding wrapper.

    The SDK wraps ``ctx.storage`` (and bindings in ``env``) to convert every
    call; hot paths use the raw object with `py_to_js` / `js_to_py` instead
    (``.design/utilities.md`` §3.8). Relies on the SDK's private ``_binding``
    attribute.
    """
    return getattr(value, "_binding", value)


class ProxyScope:
    """Proxies created for JS callbacks, destroyed together when the scope ends."""

    __slots__ = ("_proxies",)

    def __init__(self) -> None:
        self._proxies: list[JsProxy] = []

    def proxy(self, fn: Callable[..., Any]) -> JsProxy:
        """Wrap ``fn`` so JS can call it until the scope ends.

        Parameters
        ----------
        fn
            The Python callable to expose to JS.

        Returns
        -------
        JsProxy
            The proxy to pass to JS.
        """
        handle = create_proxy(fn)
        self._proxies.append(handle)
        return handle

    def rpc(self, fn: Callable[..., Any]) -> JsProxy:
        """Like `proxy`, for a function another object calls over RPC.

        Its arguments arrive as JS values and are converted to Python first;
        its result is converted back. ``fn`` must be synchronous.
        """

        def call(*args: Any) -> Any:
            return to_rpc(fn(*(from_rpc(arg) for arg in args)))

        return self.proxy(call)

    def _destroy(self) -> None:
        proxies, self._proxies = self._proxies, []
        for handle in proxies:
            handle.destroy()


@contextmanager
def proxies() -> Generator[ProxyScope]:
    """Create JS callback proxies that are destroyed when the block exits.

    Yields
    ------
    ProxyScope
        Use `ProxyScope.proxy` to wrap each callback.
    """
    scope = ProxyScope()
    try:
        yield scope
    finally:
        scope._destroy()


async def block_concurrency_while[T](ctx: Any, fn: Callable[[], Awaitable[T]]) -> T:
    """Run ``fn`` under ``ctx.blockConcurrencyWhile``, holding back other events.

    ``fn`` must not raise: an exception inside the callback leaves the object
    unusable (``.design/platform_verification.md`` §2.9), so callers catch
    inside ``fn`` and re-raise after this returns.
    """
    with proxies() as scope:
        return await ctx.blockConcurrencyWhile(scope.proxy(fn))


def abort_without_alarm_retry(ctx: Any, reason: str) -> None:
    """Reset the object on the next tick, without the platform retrying an alarm.

    The Workers SDK's ``ctx.abort(reason)`` takes no options, so this calls the
    raw JS ``abort(reason, {retryAlarm: false})`` through the SDK's private
    ``_ctx``; deferred so the current invocation settles first
    (``.design/platform_verification.md`` §7.5).
    """
    raw = getattr(ctx, "_ctx", ctx)
    options = py_to_js({"retryAlarm": False})
    js.setTimeout(create_once_callable(lambda: raw.abort(reason, options)), 0)


def websocket_error_response(message: str) -> Any:
    """Answer a failed WebSocket upgrade with a socket that reports the error.

    Browser devtools don't show an HTTP error body for an upgrade, so (as
    upstream) the error is sent as a frame and the socket closed with 1011.
    """
    client, server = js.WebSocketPair.new().object_values()
    server.accept()
    server.send(json.dumps({"error": message}))
    server.close(1011, "Uncaught exception during session setup")
    return Response(None, status=101, web_socket=client)


def websocket_pair() -> tuple[JsProxy, JsProxy]:
    """Create a ``WebSocketPair``; return its ``(client, server)`` ends."""
    client, server = js.WebSocketPair.new().object_values()
    return client, server


def read_attachment(ws: JsProxy) -> Any:
    """Return a socket's hibernation attachment as Python (``None`` if unset)."""
    return js_to_py(ws.deserializeAttachment())


def write_attachment(ws: JsProxy, value: Any) -> None:
    """Replace a socket's hibernation attachment (structured clone, 16 KiB max).

    Raises
    ------
    JsException
        If the serialized attachment exceeds the runtime's limit.
    """
    ws.serializeAttachment(py_to_js(value))


def send_message(ws: JsProxy, message: str | bytes) -> None:
    """Send a text or binary frame.

    Raises
    ------
    JsException
        If the socket is no longer open.
    """
    ws.send(message if isinstance(message, str) else py_to_js(message))


class ConsoleHandler(logging.Handler):
    """Write log records with the JS console method matching their level.

    Python's own output reaches Workers Logs at level ``error`` whatever the
    record's level; this keeps levels intact (``.design/observability.md``
    §6). Installed only on the SDK's ``agents`` logger.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Write ``record`` to ``console.error``/``warn``/``info``/``debug``."""
        message = self.format(record)
        if record.levelno >= logging.ERROR:
            js.console.error(message)
        elif record.levelno >= logging.WARNING:
            js.console.warn(message)
        elif record.levelno >= logging.INFO:
            js.console.info(message)
        else:
            js.console.debug(message)


def _install_console_handler() -> None:
    logger = logging.getLogger("agents")
    if not any(isinstance(handler, ConsoleHandler) for handler in logger.handlers):
        logger.addHandler(ConsoleHandler())


# This module only loads on Workers, so the handler is only installed there.
_install_console_handler()


def env_binding_names(env: Any) -> list[str]:
    """Return the binding names on a Worker's ``env`` (the SDK's wrapper)."""
    return [str(name) for name in js.Object.keys(env._env)]


def clone_request(request: Request) -> Request:
    """Return a copy of ``request`` whose body can be read independently."""
    return Request(request.js_object.clone())


def with_headers(response: Response, headers: dict[str, str]) -> Response:
    """Return ``response`` with ``headers`` set (fetched responses are immutable)."""
    copy = js.Response.new(response.js_object.body, response.js_object)
    for name, value in headers.items():
        copy.headers.set(name, value)
    return Response(copy)


def streaming_response(
    pieces: AsyncIterator[str], *, headers: dict[str, str]
) -> Response:
    """Return a response whose body is ``pieces``, pulled as the client reads.

    Each piece is UTF-8 encoded as it's sent, so the body is never held whole.
    The iterator is closed if the client goes away.
    """
    encoder = js.TextEncoder.new()
    callbacks: list[JsProxy] = []

    def release() -> None:
        while callbacks:
            callbacks.pop().destroy()

    async def pull(controller: Any) -> None:
        try:
            piece = await anext(pieces)
        except StopAsyncIteration:
            controller.close()
            release()
            return
        controller.enqueue(encoder.encode(piece))

    async def cancel(_reason: Any) -> None:
        release()
        await pieces.aclose()  # ty: ignore[unresolved-attribute]

    callbacks.extend((create_proxy(pull), create_proxy(cancel)))
    source = to_js(
        {"pull": callbacks[0], "cancel": callbacks[1]},
        dict_converter=js.Object.fromEntries,
    )
    return Response(js.ReadableStream.new(source), headers=headers)


# Facets (sub-agents; .design/subagents_engine.md §1)


def _raw_ctx(ctx: Any) -> Any:
    """Return the JS ``DurableObjectState`` behind the SDK's context wrapper."""
    return getattr(ctx, "_ctx", ctx)


def export_names(ctx: Any) -> list[str]:
    """Return the names the Worker module exports (``ctx.exports``)."""
    return [str(name) for name in js.Object.keys(_raw_ctx(ctx).exports)]


def has_namespace(ctx: Any, class_name: str) -> bool:
    """Return whether ``class_name`` is exported as a Durable Object namespace."""
    exported = getattr(_raw_ctx(ctx).exports, class_name, None)
    return exported is not None and hasattr(exported, "idFromName")


def namespace_stub(ctx: Any, class_name: str, name: str) -> Any:
    """Return a stub for ``class_name``'s instance ``name`` (via ``ctx.exports``)."""
    namespace = getattr(_raw_ctx(ctx).exports, class_name)
    return namespace.get(namespace.idFromName(name))


def facet_get(
    ctx: Any, key: str, class_name: str, root_class: str, identity: str
) -> Any:
    """Return the facet ``key`` (creating it on first use).

    Its id comes from the root's namespace, so the facet's ``ctx.id.name``
    is ``identity``.
    """
    raw = _raw_ctx(ctx)

    def getter() -> Any:
        namespace = getattr(raw.exports, root_class)
        return to_js(
            {
                "class": getattr(raw.exports, class_name),
                "id": namespace.idFromName(identity),
            },
            dict_converter=js.Object.fromEntries,
        )

    # The runtime may call the getter whenever the facet restarts, so the
    # proxy lives as long as the context.
    getters: dict[str, Any] = ctx.__dict__.setdefault("_agents_facet_getters", {})
    getters[key] = create_proxy(getter)
    return raw.facets.get(key, getters[key])


def facet_abort(ctx: Any, key: str, reason: Exception) -> None:
    """Stop facet ``key`` now; pending calls get ``reason``. Storage is kept."""
    _raw_ctx(ctx).facets.abort(key, js.Error.new(str(reason)))


def facet_delete(ctx: Any, key: str) -> None:
    """Stop facet ``key`` and wipe its storage (no-op if it doesn't exist)."""
    with suppress(JsException):  # thrown for a key that was never created
        _raw_ctx(ctx).facets.delete(key)


async def call_rpc(stub: Any, method: str, *args: Any) -> Any:
    """Call ``stub.method(*args)`` over native RPC, converting both ways.

    Arguments go through the SDK's RPC converter: a raw ``dict`` passed to a
    JS function would arrive as a Python proxy and fail to serialize.
    """
    result = await getattr(stub, method)(*(to_rpc(arg) for arg in args))
    return from_rpc(result)


async def facet_fetch(stub: Any, request: Request) -> Response:
    """Send ``request`` to a facet's ``fetch``."""
    return Response(await stub.fetch(request.js_object))


def request_with(
    request: Request, *, url: str | None = None, headers: dict[str, str] | None = None
) -> Request:
    """Return a copy of ``request`` with another URL and/or extra headers."""
    copy = js.Request.new(url if url is not None else request.url, request.js_object)
    for name, value in (headers or {}).items():
        copy.headers.set(name, value)
    return Request(copy)


def make_request(url: str, headers: list[list[str]] | None) -> Request:
    """Build a ``GET`` request (a forwarded connection's upgrade request)."""
    init = js.Object.new()
    init.headers = js.Headers.new(to_js(headers or []))
    return Request(js.Request.new(url, init))


def websocket_rejection(code: int, reason: str) -> Response:
    """Answer an upgrade with a socket that closes at once with ``code``.

    A browser can't read a failed handshake's response, so a refused
    ``/sub/`` connection gets a close frame instead.
    """
    client, server = js.WebSocketPair.new().object_values()
    server.accept()
    server.close(code, reason)
    return Response(None, status=101, web_socket=client)
