import asyncio

import pytest

from agents.core.events import Disposable, Emitter


def test_fire_calls_listeners_in_subscription_order():
    emitter = Emitter[int]()
    seen: list[tuple[str, int]] = []
    emitter.subscribe(lambda v: seen.append(("a", v)))
    emitter.subscribe(lambda v: seen.append(("b", v)))

    emitter.fire(1)

    assert seen == [("a", 1), ("b", 1)]


def test_same_callable_can_subscribe_twice():
    emitter = Emitter[int]()
    seen: list[int] = []
    emitter.subscribe(seen.append)
    emitter.subscribe(seen.append)

    emitter.fire(7)

    assert seen == [7, 7]


def test_raising_listener_is_logged_and_others_still_run(caplog):
    emitter = Emitter[int]()
    seen: list[int] = []

    def boom(_: int) -> None:
        raise ValueError("bad listener")

    emitter.subscribe(boom)
    emitter.subscribe(seen.append)

    emitter.fire(1)

    assert seen == [1]
    assert "Emitter listener failed" in caplog.text


def test_dispose_unsubscribes_and_is_idempotent():
    emitter = Emitter[int]()
    seen: list[int] = []
    handle = emitter.subscribe(seen.append)

    handle.dispose()
    handle.dispose()
    emitter.fire(1)

    assert seen == []


def test_listener_can_unsubscribe_while_firing():
    emitter = Emitter[int]()
    seen: list[int] = []
    handles: dict[str, Disposable] = {}

    def once(v: int) -> None:
        seen.append(v)
        handles["once"].dispose()

    handles["once"] = emitter.subscribe(once)
    emitter.fire(1)
    emitter.fire(2)

    assert seen == [1]


def test_fire_rejects_async_listener():
    emitter = Emitter[int]()

    async def listener(_: int) -> None: ...

    emitter.subscribe(listener)

    with pytest.raises(TypeError, match="fire_async"):
        emitter.fire(1)


def test_fire_async_awaits_listeners_in_order():
    emitter = Emitter[int]()
    seen: list[str] = []

    async def slow(_: int) -> None:
        await asyncio.sleep(0)
        seen.append("slow")

    emitter.subscribe(slow)
    emitter.subscribe(lambda _: seen.append("sync"))

    asyncio.run(emitter.fire_async(1))

    assert seen == ["slow", "sync"]


def test_fire_async_isolates_raising_async_listener(caplog):
    emitter = Emitter[int]()
    seen: list[int] = []

    async def boom(_: int) -> None:
        raise ValueError("bad listener")

    emitter.subscribe(boom)
    emitter.subscribe(seen.append)

    asyncio.run(emitter.fire_async(1))

    assert seen == [1]
    assert "Emitter listener failed" in caplog.text


def test_fire_async_does_not_swallow_cancellation():
    emitter = Emitter[int]()

    async def cancelled(_: int) -> None:
        raise asyncio.CancelledError

    emitter.subscribe(cancelled)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(emitter.fire_async(1))


def test_emitter_dispose_removes_all_listeners():
    emitter = Emitter[int]()
    seen: list[int] = []
    emitter.subscribe(seen.append)

    emitter.dispose()
    emitter.fire(1)

    assert seen == []
