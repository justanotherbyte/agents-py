import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from fake_runtime import Host, LocalTransport

from agents.core.types import RetryOptions
from agents.lifecycle import Lifecycle, LifecycleEvent, RouteAddress
from agents.lifecycle.host_context import current_host_context
from agents.lifecycle.types import HostContext
from agents.queue import Queue, QueueItem

FAST_RETRY = RetryOptions(max_attempts=2, base_delay=0.001, max_delay=0.002)


class Recorder:
    """Callbacks that record their calls (and the host context they ran in)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any, QueueItem]] = []
        self.contexts: list[object] = []
        self.failures_left = 0

    async def handle(self, payload: Any, item: QueueItem) -> None:
        self.contexts.append(current_host_context())
        self.calls.append(("handle", payload, item))

    async def other(self, payload: Any, item: QueueItem) -> None:
        self.calls.append(("other", payload, item))

    async def flaky(self, payload: Any, item: QueueItem) -> None:
        self.calls.append(("flaky", payload, item))
        if self.failures_left:
            self.failures_left -= 1
            raise RuntimeError("callback bug")


def setup(
    **queue_options: Any,
) -> tuple[Lifecycle, Queue, Recorder, list[LifecycleEvent]]:
    recorder = Recorder()
    queue = Queue(target=recorder, **queue_options)
    lifecycle = Lifecycle(Host()).use(queue)
    events: list[LifecycleEvent] = []
    lifecycle._set_event_sink(events.append)
    return lifecycle, queue, recorder, events


async def drain(lifecycle: Lifecycle) -> None:
    await asyncio.sleep(0.01)  # items pushed in one ms get strictly later times
    await lifecycle.alarm()


def event_types(events: list[LifecycleEvent]) -> list[str]:
    return [event.type for event in events]


# Pushing
def test_items_run_in_push_order_in_host_context_then_leave_the_queue() -> None:
    lifecycle, queue, recorder, events = setup()

    async def scenario() -> None:
        first = await queue.push(recorder.handle, {"n": 1})
        await queue.push("other", {"n": 2})
        await queue.push("handle", None)
        assert [item.callback for item in await queue.list()] == [
            "handle",
            "other",
            "handle",
        ]
        await drain(lifecycle)
        assert [(name, payload) for name, payload, _ in recorder.calls] == [
            ("handle", {"n": 1}),
            ("other", {"n": 2}),
            ("handle", None),
        ]
        assert recorder.calls[0][2] == first
        assert await queue.list() == []

    asyncio.run(scenario())
    context = recorder.contexts[0]
    assert isinstance(context, HostContext)
    assert context.host is lifecycle._host
    assert event_types(events) == ["queue:create"] * 3


def test_callbacks_dict_is_looked_up_before_target() -> None:
    seen: list[str] = []

    async def from_dict(payload: Any, item: QueueItem) -> None:
        seen.append("dict")

    lifecycle, queue, recorder, _ = setup(callbacks={"handle": from_dict})

    async def scenario() -> None:
        await queue.push("handle")
        await drain(lifecycle)

    asyncio.run(scenario())
    assert seen == ["dict"]
    assert recorder.calls == []


def test_unknown_callbacks_are_rejected_when_pushed() -> None:
    _, queue, _, _ = setup()
    with pytest.raises(ValueError, match="Unknown queue callback 'missing'"):
        asyncio.run(queue.push("missing"))
    with pytest.raises(ValueError, match="not a method of Recorder"):
        asyncio.run(queue.push(Recorder().handle))  # another object's method


def test_callable_without_target_is_rejected() -> None:
    queue = Queue(callbacks={"x": Recorder().handle})
    Lifecycle(Host()).use(queue)
    with pytest.raises(TypeError, match="registered name"):
        asyncio.run(queue.push(Recorder().handle))


def test_invalid_options_are_rejected() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        Queue(retry=RetryOptions(max_attempts=0))
    _, queue, recorder, _ = setup()
    with pytest.raises(ValueError, match="non-empty"):
        asyncio.run(queue.push(recorder.handle, id="  "))
    with pytest.raises(ValueError, match="base_delay"):
        asyncio.run(
            queue.push(recorder.handle, retry=RetryOptions(base_delay=5, max_delay=1))
        )


def test_stable_id_push_replaces_the_item_in_place() -> None:
    _, queue, recorder, _ = setup()

    async def scenario() -> None:
        first = await queue.push(recorder.handle, {"v": 1}, id="a")
        await queue.push(recorder.handle, {"v": 1}, id="b")
        replaced = await queue.push(recorder.other, {"v": 2}, id="a")
        items = await queue.list()
        assert [(item.id, item.callback, item.payload) for item in items] == [
            ("a", "other", {"v": 2}),
            ("b", "handle", {"v": 1}),
        ]
        assert replaced.created_at == first.created_at

    asyncio.run(scenario())


def test_a_fresh_instance_queues_after_the_stored_tail() -> None:
    lifecycle, queue, recorder, _ = setup()

    async def scenario() -> None:
        far = await queue.push(recorder.handle)
        lifecycle._queue.retime(far.id, 4_000_000_000_000)  # year 2096
        fresh = Queue(target=recorder)
        Lifecycle(lifecycle._host).use(fresh)
        later = await fresh.push(recorder.handle)
        stored = fresh.lifecycle.jobs.get(later.id)
        assert stored is not None
        assert int(stored.time.timestamp() * 1000) == 4_000_000_000_001

    asyncio.run(scenario())


# Inspecting and cancelling
def test_get_list_and_cancel() -> None:
    _, queue, recorder, _ = setup()

    async def scenario() -> None:
        a = await queue.push(recorder.handle, 1)
        b = await queue.push(recorder.other, 2)
        await queue.push(recorder.handle, 3)
        assert await queue.get(a.id) == a
        assert await queue.get("nope") is None
        assert [i.payload for i in await queue.list(recorder.handle)] == [1, 3]
        assert [i.payload for i in await queue.list("other")] == [2]
        assert await queue.cancel(b.id)
        assert not await queue.cancel(b.id)
        assert await queue.cancel_all("handle") == 2
        assert await queue.list() == []

    asyncio.run(scenario())


def test_items_of_other_capabilities_are_invisible() -> None:
    lifecycle, queue, _, _ = setup()

    async def scenario() -> None:
        await lifecycle.start()
        await lifecycle.jobs.push(fn="handle", time=datetime.now(UTC), id="host-job")
        assert await queue.get("host-job") is None
        assert await queue.list() == []

    asyncio.run(scenario())


# Failures
def test_failing_callback_is_retried_then_dropped_and_reported() -> None:
    reported: list[Exception] = []

    async def on_error(error: Exception) -> None:
        reported.append(error)

    lifecycle, queue, recorder, events = setup(retry=FAST_RETRY, on_error=on_error)
    recorder.failures_left = 5

    async def scenario() -> None:
        item = await queue.push(recorder.flaky, "x")
        await drain(lifecycle)
        assert await queue.get(item.id) is None

    asyncio.run(scenario())
    assert len(recorder.calls) == 2
    assert [str(error) for error in reported] == ["callback bug"]
    assert event_types(events) == ["queue:create", "queue:retry", "queue:error"]
    assert events[1].payload["maxAttempts"] == 2
    assert events[2].payload["attempts"] == 2


def test_a_retry_that_succeeds_runs_the_item_once_more_only() -> None:
    lifecycle, queue, recorder, events = setup(retry=FAST_RETRY)
    recorder.failures_left = 1

    async def scenario() -> None:
        await queue.push(recorder.flaky)
        await drain(lifecycle)

    asyncio.run(scenario())
    assert len(recorder.calls) == 2
    assert "queue:error" not in event_types(events)


def test_a_failing_on_error_hook_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def on_error(error: Exception) -> None:
        raise RuntimeError("observer broke")

    lifecycle, queue, recorder, _ = setup(
        retry=RetryOptions(max_attempts=1), on_error=on_error
    )
    recorder.failures_left = 1

    async def scenario() -> None:
        await queue.push(recorder.flaky)
        await drain(lifecycle)

    asyncio.run(scenario())
    assert "observer broke" in caplog.text


class FlakyPlatform(Recorder):
    async def flaky(self, payload: Any, item: QueueItem) -> None:
        raise RuntimeError("Network connection lost.")


def test_platform_failure_keeps_the_item() -> None:
    queue = Queue(target=FlakyPlatform(), retry=FAST_RETRY)
    lifecycle = Lifecycle(Host()).use(queue)

    async def scenario() -> None:
        item = await queue.push("flaky")
        with pytest.raises(RuntimeError, match="Network connection lost"):
            await drain(lifecycle)
        assert await queue.get(item.id) is not None

    asyncio.run(scenario())


# Facets: routed through the root's queue
def facet_setup() -> tuple[Lifecycle, Queue, Lifecycle, Queue, Recorder]:
    recorder = Recorder()
    root_queue = Queue(target=Recorder())
    root = Lifecycle(Host("root")).use(root_queue)
    facet_queue = Queue(target=recorder, retry=FAST_RETRY)
    facet = Lifecycle(Host("facet")).use(facet_queue)
    address = RouteAddress(key="child", data="child-data")

    root_transport = LocalTransport(None)
    root_transport.peers["child"] = facet
    facet_transport = LocalTransport(address)
    facet_transport.root = root
    root._set_route_transport(root_transport)
    facet._set_route_transport(facet_transport)
    return root, root_queue, facet, facet_queue, recorder


def test_facet_items_live_on_the_root_and_run_in_the_facet() -> None:
    root, root_queue, _, facet_queue, recorder = facet_setup()

    async def scenario() -> None:
        item = await facet_queue.push(recorder.handle, {"k": 1})
        assert await facet_queue.get(item.id) == item
        assert await root_queue.list() == []  # owner-scoped
        stored = root_queue.lifecycle.jobs.get(item.id)
        assert stored is not None
        assert stored.payload == {
            "payload": {"k": 1},
            "owner_path": "child-data",
            "owner_path_key": "child",
        }
        await drain(root)
        assert [(name, payload) for name, payload, _ in recorder.calls] == [
            ("handle", {"k": 1})
        ]
        assert await facet_queue.list() == []

    asyncio.run(scenario())


def test_facet_retries_its_own_items_and_cancels_through_the_root() -> None:
    root, _, _, facet_queue, recorder = facet_setup()
    recorder.failures_left = 1

    async def scenario() -> None:
        await facet_queue.push(recorder.flaky)
        await drain(root)
        assert len(recorder.calls) == 2
        a = await facet_queue.push(recorder.handle)
        await facet_queue.push(recorder.other)
        assert await facet_queue.cancel(a.id)
        assert await facet_queue.cancel_all() == 1
        assert await facet_queue.list() == []

    asyncio.run(scenario())


def test_unroutable_dispatch_yields_and_keeps_the_item() -> None:
    root, root_queue, _, facet_queue, recorder = facet_setup()

    async def scenario() -> None:
        item = await facet_queue.push(recorder.handle)
        root._route_transport.peers.clear()  # ty: ignore[unresolved-attribute]
        await drain(root)
        assert root_queue.lifecycle.jobs.get(item.id) is not None

    asyncio.run(scenario())


def test_cleanup_route_prefix_removes_a_facet_subtree() -> None:
    _, root_queue, _, facet_queue, recorder = facet_setup()

    async def scenario() -> None:
        await facet_queue.push(recorder.handle)
        await root_queue._cleanup_route_prefix("child")
        assert root_queue.lifecycle.jobs.list() == []

    asyncio.run(scenario())
