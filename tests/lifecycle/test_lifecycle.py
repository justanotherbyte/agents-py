import asyncio
from typing import Any

import pytest
from fake_runtime import Host
from workers import Request, Response

from agents.lifecycle import Lifecycle, LifecycleCapability, LifecycleEvent
from agents.lifecycle.host_context import current_host_context
from agents.lifecycle.types import HostContext


class Recorder(LifecycleCapability):
    """Records the hooks it receives into a shared log."""

    def __init__(self, capability_id: str, log: list[str]) -> None:
        super().__init__(capability_id)
        self.log = log

    async def on_start(self) -> None:
        self.log.append(f"{self.capability_id}:start")


class Router(LifecycleCapability):
    """Claims requests for one path."""

    def __init__(self, path: str) -> None:
        super().__init__(f"router{path}")
        self.path = path
        self.seen_context: object = "unset"

    async def on_request(self, request: Any) -> Any:
        self.seen_context = current_host_context()
        if request.url.endswith(self.path):
            return Response(f"router{self.path}")
        return None


class CatchAllRequests(LifecycleCapability):
    claims = "catch-all"

    def __init__(self, capability_id: str = "catch-all-http") -> None:
        super().__init__(capability_id)

    async def on_request(self, request: Any) -> Any:
        return Response("catch-all")


class CatchAllUpgrades(LifecycleCapability):
    claims = "catch-all"

    def __init__(self) -> None:
        super().__init__("catch-all-ws")

    async def on_websocket_upgrade(self, request: Any) -> Any:
        return Response(None, status=101)


def make_request(path: str = "/", *, upgrade: bool = False) -> Request:
    headers = {"Upgrade": "websocket"} if upgrade else {}
    return Request(f"https://example.com{path}", headers=headers)


# use()
def test_duplicate_capability_id_is_rejected() -> None:
    lifecycle = Lifecycle(Host())
    lifecycle.use(Recorder("same", []))
    with pytest.raises(RuntimeError, match="already installed"):
        lifecycle.use(Recorder("same", []))


def test_catch_alls_dispatch_last_whenever_installed() -> None:
    lifecycle = Lifecycle(Host())
    catch_all = CatchAllRequests()
    router = Router("/a")
    lifecycle.use(catch_all).use(router)
    assert lifecycle._capabilities == [router, catch_all]
    response = asyncio.run(lifecycle.fetch(make_request("/a")))
    assert response.body == "router/a"
    response = asyncio.run(lifecycle.fetch(make_request("/other")))
    assert response.body == "catch-all"


def test_second_catch_all_for_same_hook_is_rejected() -> None:
    lifecycle = Lifecycle(Host())
    lifecycle.use(CatchAllRequests("first"))
    with pytest.raises(RuntimeError, match="catch-all for on_request"):
        lifecycle.use(CatchAllRequests("second"))


def test_catch_alls_for_disjoint_hooks_coexist() -> None:
    lifecycle = Lifecycle(Host())
    lifecycle.use(CatchAllRequests()).use(CatchAllUpgrades())
    assert len(lifecycle._capabilities) == 2


def test_use_after_startup_is_rejected() -> None:
    lifecycle = Lifecycle(Host())
    asyncio.run(lifecycle.start())
    with pytest.raises(RuntimeError, match="before startup"):
        lifecycle.use(Recorder("late", []))


def test_capability_services_require_installation() -> None:
    with pytest.raises(RuntimeError, match=r"Lifecycle\.use\(\)"):
        _ = Recorder("loose", []).lifecycle


# Startup
class HostWithStart(Host):
    def __init__(self, log: list[str]) -> None:
        super().__init__()
        self.log = log
        self.context_in_start: HostContext | None = None

    async def on_start(self) -> None:
        self.context_in_start = current_host_context()
        self.log.append("host:start")


def test_startup_runs_capabilities_in_order_then_host_once() -> None:
    log: list[str] = []
    host = HostWithStart(log)
    lifecycle = Lifecycle(host).use(Recorder("a", log)).use(Recorder("b", log))

    async def scenario() -> None:
        await asyncio.gather(lifecycle.start(), lifecycle.start(), lifecycle.start())
        await lifecycle.start()

    asyncio.run(scenario())
    assert log == ["a:start", "b:start", "host:start"]
    assert lifecycle.is_started()
    assert host.ctx.blocked_calls == 1


def test_host_on_start_runs_in_host_context_and_capabilities_outside() -> None:
    log: list[str] = []
    host = HostWithStart(log)
    asyncio.run(Lifecycle(host).start())
    context = host.context_in_start
    assert isinstance(context, HostContext)
    assert context.host is host


class ReentrantHost(Host):
    def __init__(self) -> None:
        super().__init__()
        self.lifecycle = Lifecycle(self)
        self.reentered = False

    async def on_start(self) -> None:
        await self.lifecycle.start()  # must not wait on itself
        self.reentered = True


def test_start_from_inside_startup_returns_immediately() -> None:
    host = ReentrantHost()
    asyncio.run(asyncio.wait_for(host.lifecycle.start(), timeout=1))
    assert host.reentered


class FailingOnce(LifecycleCapability):
    def __init__(self) -> None:
        super().__init__("flaky")
        self.calls = 0

    async def on_start(self) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("first start fails")


def test_failed_startup_raises_and_the_next_call_retries() -> None:
    flaky = FailingOnce()
    lifecycle = Lifecycle(Host()).use(flaky)

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="first start fails"):
            await lifecycle.start()
        assert not lifecycle.is_started()
        await lifecycle.start()

    asyncio.run(scenario())
    assert flaky.calls == 2
    assert lifecycle.is_started()


def test_unnamed_object_fails_before_startup() -> None:
    log: list[str] = []
    lifecycle = Lifecycle(Host(name=None)).use(Recorder("a", log))
    with pytest.raises(RuntimeError, match="could not determine its Durable Object"):
        asyncio.run(lifecycle.start())
    assert log == []


# fetch
class HostWithRequest(Host):
    def __init__(self) -> None:
        super().__init__()
        self.context: HostContext | None = None

    async def on_request(self, request: Any) -> Any:
        self.context = current_host_context()
        return Response("host")


def test_unclaimed_request_goes_to_host_in_host_context() -> None:
    host = HostWithRequest()
    router = Router("/claimed")
    lifecycle = Lifecycle(host).use(router)
    request = make_request("/unclaimed")
    response = asyncio.run(lifecycle.fetch(request))
    assert response.body == "host"
    assert isinstance(host.context, HostContext)
    assert host.context.host is host
    assert host.context.request is request
    assert router.seen_context is None  # capability hooks run outside it


def test_unclaimed_request_without_host_handler_is_404() -> None:
    response = asyncio.run(Lifecycle(Host()).fetch(make_request()))
    assert response.status == 404


def test_upgrade_goes_to_capabilities_only() -> None:
    host = HostWithRequest()
    response = asyncio.run(Lifecycle(host).fetch(make_request(upgrade=True)))
    assert response.status == 404
    assert "WebSocket upgrades are not enabled" in response.body
    assert host.context is None
    lifecycle = Lifecycle(Host()).use(CatchAllUpgrades())
    response = asyncio.run(lifecycle.fetch(make_request(upgrade=True)))
    assert response.status == 101


class ExplodingHost(Host):
    async def on_request(self, request: Any) -> Any:
        raise RuntimeError("boom")


def test_request_error_becomes_500_without_leaking_details(
    caplog: pytest.LogCaptureFixture,
) -> None:
    response = asyncio.run(Lifecycle(ExplodingHost()).fetch(make_request()))
    assert response.status == 500
    assert "boom" not in response.body
    assert "boom" in caplog.text


def test_error_details_are_sent_only_when_enabled() -> None:
    lifecycle = Lifecycle(ExplodingHost(), expose_error_details=True)
    response = asyncio.run(lifecycle.fetch(make_request()))
    assert response.status == 500
    assert "RuntimeError: boom" in response.body
    assert "Traceback" in response.body


# Events
class Emitting(LifecycleCapability):
    def __init__(self) -> None:
        super().__init__("emitter")

    async def on_start(self) -> None:
        self.lifecycle.emit("thing:started", {"n": 1})


def test_events_emitted_during_startup_are_delivered_after_it() -> None:
    received: list[LifecycleEvent] = []
    lifecycle = Lifecycle(Host()).use(Emitting())
    lifecycle._set_event_sink(received.append)
    asyncio.run(lifecycle.start())
    assert received == [
        LifecycleEvent(source="emitter", type="thing:started", payload={"n": 1})
    ]


def test_failing_sink_never_fails_the_emitter(caplog: pytest.LogCaptureFixture) -> None:
    def broken_sink(event: LifecycleEvent) -> None:
        raise RuntimeError("sink down")

    lifecycle = Lifecycle(Host()).use(Emitting())
    lifecycle._set_event_sink(broken_sink)
    asyncio.run(lifecycle.start())
    assert "sink down" in caplog.text


# WebSockets
class SocketOwner(LifecycleCapability):
    def __init__(self) -> None:
        super().__init__("sockets")
        self.messages: list[str | bytes] = []
        self.errors: list[BaseException] = []

    async def on_websocket_message(self, ws: Any, message: str | bytes) -> bool:
        self.messages.append(message)
        return True

    async def on_websocket_error(self, ws: Any, error: BaseException) -> bool:
        self.errors.append(error)
        return True


class FakeSocket:
    def __init__(self, ready_state: int = 1) -> None:
        self.readyState = ready_state


def test_socket_messages_reach_the_owning_capability() -> None:
    owner = SocketOwner()
    lifecycle = Lifecycle(Host()).use(owner)
    asyncio.run(lifecycle.websocket_message(FakeSocket(), "hello"))
    assert owner.messages == ["hello"]


def test_benign_teardown_errors_on_closing_sockets_are_dropped() -> None:
    owner = SocketOwner()
    lifecycle = Lifecycle(Host()).use(owner)
    teardown = RuntimeError("Network connection lost.")
    asyncio.run(lifecycle.websocket_error(FakeSocket(ready_state=3), teardown))
    assert owner.errors == []
    asyncio.run(lifecycle.websocket_error(FakeSocket(ready_state=1), teardown))
    assert owner.errors == [teardown]
