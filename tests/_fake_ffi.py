"""A pure-Python stand-in for ``agents._ffi`` (which needs the Workers runtime).

Values pass through unchanged; tests hand the SDK Python objects shaped like
the JS ones (e.g. a fake ``sql.exec`` returning a cursor of dicts).
"""

import copy
import json
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager
from typing import Any


class JsException(Exception):  # noqa: N818  (mirrors pyodide.ffi.JsException)
    """Stands in for ``pyodide.ffi.JsException``."""


JsProxy = object


class ProxyScope:
    def proxy(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn

    def rpc(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        async def call(*args: Any) -> Any:  # a JS RPC call returns a promise
            return _wire(fn(*(_wire(arg) for arg in args)))

        return call


@contextmanager
def proxies() -> Generator[ProxyScope]:
    yield ProxyScope()


def py_to_js(value: Any) -> Any:
    return value


def js_to_py(value: Any) -> Any:
    return value


def to_rpc(value: Any) -> Any:
    return value


def from_rpc(value: Any) -> Any:
    return value


def unwrap(value: Any) -> Any:
    return getattr(value, "_binding", value)


async def block_concurrency_while[T](ctx: Any, fn: Callable[[], Awaitable[T]]) -> T:
    return await ctx.blockConcurrencyWhile(fn)


aborts: list[str] = []


def abort_without_alarm_retry(ctx: Any, reason: str) -> None:
    aborts.append(reason)


def websocket_pair() -> tuple[Any, Any]:
    from fake_runtime import FakeWebSocket

    server = FakeWebSocket()
    return FakeWebSocket(peer=server), server


ATTACHMENT_LIMIT = 16_384


def read_attachment(ws: Any) -> Any:
    return copy.deepcopy(ws.deserializeAttachment())


def write_attachment(ws: Any, value: Any) -> None:
    if len(json.dumps(value)) > ATTACHMENT_LIMIT:
        raise JsException(
            "Error: A WebSocket 'attachment' cannot be larger than 16384 bytes."
        )
    ws.serializeAttachment(copy.deepcopy(value))


def send_message(ws: Any, message: str | bytes) -> None:
    ws.send(message)


def websocket_error_response(message: str) -> Any:
    from workers import Response

    return Response(message, status=101)


def env_binding_names(env: Any) -> list[str]:
    return list(vars(env))


def clone_request(request: Any) -> Any:
    return copy.copy(request)


def with_headers(response: Any, headers: dict[str, str]) -> Any:
    copied = copy.copy(response)
    copied.headers = type(response.headers)({**response.headers._values, **headers})
    return copied


# Facets: the fake runtime's FakeCtx carries `exports` and `facets`.


def streaming_response(pieces: Any, *, headers: dict[str, str]) -> Any:
    # The body stays the async iterator; tests read it with read_body().
    from workers import Response

    return Response(pieces, headers=headers)


def export_names(ctx: Any) -> list[str]:
    return list(ctx.exports)


def has_namespace(ctx: Any, class_name: str) -> bool:
    return hasattr(ctx.exports.get(class_name), "idFromName")


def namespace_stub(ctx: Any, class_name: str, name: str) -> Any:
    namespace = ctx.exports[class_name]
    return namespace.get(namespace.idFromName(name))


def facet_get(
    ctx: Any, key: str, class_name: str, root_class: str, identity: str
) -> Any:
    def getter() -> Any:
        namespace = ctx.exports[root_class]
        return {"class": ctx.exports[class_name], "id": namespace.idFromName(identity)}

    return ctx.facets.get(key, getter)


def facet_abort(ctx: Any, key: str, reason: Exception) -> None:
    ctx.facets.abort(key, reason)


def facet_delete(ctx: Any, key: str) -> None:
    ctx.facets.delete(key)


def _wire(value: Any) -> Any:
    """Copy data as RPC would (functions and objects pass by reference)."""
    if isinstance(value, dict):
        return {k: _wire(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_wire(v) for v in value]
    return value


async def call_rpc(stub: Any, method: str, *args: Any) -> Any:
    # A dict of functions crosses real RPC as a JS object with attributes.
    target = stub[method] if isinstance(stub, dict) else getattr(stub, method)
    return _wire(await target(*(_wire(arg) for arg in args)))


async def facet_fetch(stub: Any, request: Any) -> Any:
    return await stub.fetch(request)


def request_with(
    request: Any, *, url: str | None = None, headers: dict[str, str] | None = None
) -> Any:
    from workers import Request

    merged = {**request.headers._values, **(headers or {})}
    return Request(
        url if url is not None else request.url, method=request.method, headers=merged
    )


def make_request(url: str, headers: list[list[str]] | None) -> Any:
    from workers import Request

    return Request(url, headers=dict(headers or []))


rejections: list[tuple[int, str]] = []


def websocket_rejection(code: int, reason: str) -> Any:
    from workers import Response

    rejections.append((code, reason))
    return Response(f"rejected {code}", status=101)
