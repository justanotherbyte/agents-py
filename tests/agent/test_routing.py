import asyncio
from http import HTTPMethod
from typing import Any

import pytest
from fake_runtime import FakeCtx
from workers import Request, Response

from agents import (
    Agent,
    AgentRoute,
    RoutingRetryEvent,
    RoutingRetryOptions,
    callable,
    get_agent_by_name,
    route_agent_request,
)


class ChatRoom(Agent):
    async def on_request(self, request: Request) -> Response:
        return Response(f"{self.name}:{request.url}")

    @callable
    async def ping(self) -> str:
        return "pong"


class TransientError(Exception):
    retryable = True


class OverloadedError(Exception):
    retryable = True
    overloaded = True


class Stub:
    def __init__(self, agent: Agent, failures: list[Exception]) -> None:
        self._agent = agent
        self._failures = failures

    async def fetch(self, request: Request) -> Any:
        if self._failures:
            raise self._failures.pop(0)
        return await self._agent.fetch(request)  # ty: ignore[unresolved-attribute]

    def __getattr__(self, name: str) -> Any:
        method = getattr(self._agent, name)

        async def call(*args: Any) -> Any:
            if self._failures:
                raise self._failures.pop(0)
            return await method(*args)

        return call


class Namespace:
    def __init__(self, cls: type[Agent]) -> None:
        self.cls = cls
        self.agents: dict[str, Agent] = {}
        self.failures: list[Exception] = []
        self.get_options: list[Any] = []

    def idFromName(self, name: str) -> str:  # noqa: N802
        return name

    def get(self, id: str, options: Any = None) -> Stub:
        self.get_options.append(options)
        if id not in self.agents:
            self.agents[id] = self.cls(FakeCtx(id), env=None)
        return Stub(self.agents[id], self.failures)


class Env:
    def __init__(self) -> None:
        self.ChatRoom = Namespace(ChatRoom)
        self.API_KEY = "not a binding"


def run[T](coro: Any) -> T:
    return asyncio.run(coro)


def test_routes_to_the_instance_named_in_the_path() -> None:
    env = Env()
    url = "https://x.dev/agents/chat-room/lobby/history"
    response = run(route_agent_request(Request(url), env))
    assert response is not None
    assert response.body == f"lobby:{url}"


def test_non_agent_paths_return_none_and_unknown_agents_400() -> None:
    env = Env()
    assert (
        run(route_agent_request(Request("https://x.dev/other/chat-room/a"), env))
        is None
    )
    assert (
        run(route_agent_request(Request("https://x.dev/agents/chat-room"), env)) is None
    )
    response = run(route_agent_request(Request("https://x.dev/agents/nope/a"), env))
    assert response is not None and response.status == 400


def test_custom_prefix() -> None:
    env = Env()
    request = Request("https://x.dev/api/v1/chat-room/a")
    response = run(route_agent_request(request, env, prefix="api/v1"))
    assert response is not None and response.body.startswith("a:")


def test_cors_preflight_and_headers() -> None:
    env = Env()
    preflight = Request("https://x.dev/agents/chat-room/a", method=HTTPMethod.OPTIONS)
    response = run(route_agent_request(preflight, env, cors=True))
    assert response is not None
    assert response.headers.get("Access-Control-Allow-Origin") == "*"
    response = run(
        route_agent_request(
            Request("https://x.dev/agents/chat-room/a"),
            env,
            cors={"Access-Control-Allow-Origin": "https://app.dev"},
        )
    )
    assert response is not None
    assert response.headers.get("Access-Control-Allow-Origin") == "https://app.dev"


def test_before_hooks_can_answer_or_replace_the_request() -> None:
    env = Env()
    seen: list[AgentRoute] = []

    async def deny(request: Request, route: AgentRoute) -> Response | None:
        seen.append(route)
        return Response("Forbidden", status=403)

    async def rewrite(request: Request, route: AgentRoute) -> Request:
        return Request(request.url + "?rewritten=1")

    url = "https://x.dev/agents/chat-room/a"
    denied = run(route_agent_request(Request(url), env, on_before_request=deny))
    assert denied is not None and denied.status == 403
    assert seen == [AgentRoute(class_name="ChatRoom", name="a")]
    rewritten = run(route_agent_request(Request(url), env, on_before_request=rewrite))
    assert rewritten is not None and rewritten.body.endswith("?rewritten=1")
    upgrade = Request(url, headers={"Upgrade": "websocket"})
    blocked = run(route_agent_request(upgrade, env, on_before_connect=deny))
    assert blocked is not None and blocked.status == 403


def test_transient_errors_are_retried_and_reported() -> None:
    env = Env()
    env.ChatRoom.failures = [TransientError("blip")]
    events: list[RoutingRetryEvent] = []

    async def on_retry(event: RoutingRetryEvent) -> None:
        events.append(event)

    options = RoutingRetryOptions(base_delay=0.001, max_delay=0.002, on_retry=on_retry)
    response = run(
        route_agent_request(
            Request("https://x.dev/agents/chat-room/a"), env, routing_retry=options
        )
    )
    assert response is not None and response.status == 200
    assert [(e.attempt, e.class_name, e.name) for e in events] == [(1, "ChatRoom", "a")]


def test_other_errors_and_disabled_retry_propagate() -> None:
    env = Env()
    env.ChatRoom.failures = [OverloadedError("busy")]
    with pytest.raises(OverloadedError):
        run(route_agent_request(Request("https://x.dev/agents/chat-room/a"), env))
    env.ChatRoom.failures = [TransientError("blip")]
    with pytest.raises(TransientError):
        run(
            route_agent_request(
                Request("https://x.dev/agents/chat-room/a"), env, routing_retry=False
            )
        )


def test_invalid_retry_options_are_rejected() -> None:
    with pytest.raises(ValueError, match="base_delay must be <= max_delay"):
        run(
            route_agent_request(
                Request("https://x.dev/agents/chat-room/a"),
                Env(),
                routing_retry=RoutingRetryOptions(base_delay=2, max_delay=1),
            )
        )


def test_get_agent_by_name_starts_the_agent_and_passes_placement() -> None:
    env = Env()
    stub = run(get_agent_by_name(env.ChatRoom, "lobby", location_hint="weur"))
    agent = env.ChatRoom.agents["lobby"]
    assert agent.lifecycle.is_started()
    assert env.ChatRoom.get_options == [{"locationHint": "weur"}]
    assert run(stub.ping()) == "pong"
