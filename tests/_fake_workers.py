"""A minimal stand-in for the Workers SDK (``workers``), which needs the runtime."""

from typing import Any


class Headers:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self._values = {k.lower(): v for k, v in (values or {}).items()}

    def get(self, name: str) -> str | None:
        return self._values.get(name.lower())

    def items(self) -> list[tuple[str, str]]:
        return list(self._values.items())


class Request:
    def __init__(
        self, url: str, *, method: str = "GET", headers: dict[str, str] | None = None
    ) -> None:
        self.url = url
        self.method = method
        self.headers = Headers(headers)


class Response:
    def __init__(
        self,
        body: Any = None,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        web_socket: Any = None,
    ) -> None:
        self.body = body
        self.status = status
        self.headers = Headers(headers)
        self.web_socket = web_socket


class DurableObject:
    def __init__(self, ctx: Any, env: Any) -> None:
        self.ctx = ctx
        self.env = env

    def __init_subclass__(cls, **_kwargs: Any) -> None:
        # Like the real SDK: wraps the class and doesn't chain to super().
        cls.wrapped_by_sdk = True


def python_to_rpc(value: Any) -> Any:
    return value


def python_from_rpc(value: Any) -> Any:
    return value
