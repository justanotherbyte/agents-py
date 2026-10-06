import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fake_runtime import FakeCtx, Host, LocalTransport

from agents import Agent, InvalidCronExpressionError, Schedule, get_current_agent
from agents.core.timing import epoch_ms
from agents.core.types import RetryOptions
from agents.lifecycle import Lifecycle, LifecycleEvent, RouteAddress
from agents.lifecycle.host_context import current_host_context
from agents.schedules import Scheduler

FAST_RETRY = RetryOptions(max_attempts=2, base_delay=0.001, max_delay=0.002)


class Recorder:
    def __init__(self) -> None:
        self.runs: list[tuple[str, Any, Schedule]] = []
        self.in_context: list[bool] = []
        self.failures_left = 0

    async def remind(self, payload: Any, schedule: Schedule) -> None:
        self.in_context.append(current_host_context() is not None)
        self.runs.append(("remind", payload, schedule))

    async def flaky(self, payload: Any, schedule: Schedule) -> None:
        self.runs.append(("flaky", payload, schedule))
        if self.failures_left:
            self.failures_left -= 1
            raise RuntimeError("callback bug")


def setup(
    **options: Any,
) -> tuple[Lifecycle, Scheduler, Recorder, list[LifecycleEvent]]:
    recorder = Recorder()
    scheduler = Scheduler(target=recorder, **options)
    lifecycle = Lifecycle(Host()).use(scheduler)
    events: list[LifecycleEvent] = []
    lifecycle._set_event_sink(events.append)
    return lifecycle, scheduler, recorder, events


def past(seconds: float = 1) -> datetime:
    return datetime.now(UTC) - timedelta(seconds=seconds)


def make_due(lifecycle: Lifecycle, schedule_id: str) -> None:
    lifecycle._queue.retime(schedule_id, epoch_ms(past()))


def types(events: list[LifecycleEvent]) -> list[str]:
    return [event.type for event in events]


# Creating schedules


def test_when_forms_produce_the_right_schedule() -> None:
    _, scheduler, recorder, _ = setup()

    async def scenario() -> list[Schedule]:
        at = datetime(2030, 1, 1, tzinfo=UTC)
        return [
            await scheduler.set(at, recorder.remind, {"a": 1}),
            await scheduler.set(90, "remind"),
            await scheduler.set(timedelta(minutes=2), recorder.remind),
            await scheduler.set("0 9 * * *", recorder.remind),
            await scheduler.every(timedelta(hours=1), recorder.remind),
        ]

    at, delayed, delayed_td, cron, every = asyncio.run(scenario())
    assert (at.type, at.time, at.payload) == (
        "scheduled",
        datetime(2030, 1, 1, tzinfo=UTC),
        {"a": 1},
    )
    assert (delayed.type, delayed.delay) == ("delayed", timedelta(seconds=90))
    assert delayed_td.delay == timedelta(minutes=2)
    assert (cron.type, cron.cron, cron.time.hour, cron.time.minute) == (
        "cron",
        "0 9 * * *",
        9,
        0,
    )
    assert (every.type, every.interval) == ("interval", timedelta(hours=1))
    assert timedelta(minutes=59) < every.time - datetime.now(UTC) <= timedelta(hours=1)


def test_invalid_input_is_rejected_where_given() -> None:
    _, scheduler, recorder, _ = setup()
    cases: list[tuple[Any, type[Exception]]] = [
        (lambda: scheduler.set(1, "missing"), ValueError),
        (lambda: scheduler.set(datetime(2030, 1, 1), recorder.remind), TypeError),
        (lambda: scheduler.set(True, recorder.remind), TypeError),
        (
            lambda: scheduler.set("not cron", recorder.remind),
            InvalidCronExpressionError,
        ),
        (lambda: scheduler.every(0, recorder.remind), ValueError),
        (lambda: scheduler.every(timedelta(days=31), recorder.remind), ValueError),
        (
            lambda: scheduler.set(
                1, recorder.remind, retry=RetryOptions(max_attempts=0)
            ),
            ValueError,
        ),
    ]
    for make, error in cases:
        with pytest.raises(error):
            asyncio.run(make())


def test_recurring_schedules_deduplicate_by_default_one_shots_on_request() -> None:
    _, scheduler, recorder, events = setup()

    async def scenario() -> None:
        first = await scheduler.set("0 9 * * *", recorder.remind, {"x": 1})
        again = await scheduler.set("0 9 * * *", recorder.remind, {"x": 1})
        assert again.id == first.id
        other_payload = await scheduler.set("0 9 * * *", recorder.remind, {"x": 2})
        assert other_payload.id != first.id
        forced = await scheduler.set(
            "0 9 * * *", recorder.remind, {"x": 1}, idempotent=False
        )
        assert forced.id != first.id
        every = await scheduler.every(60, recorder.remind)
        assert (await scheduler.every(60, recorder.remind)).id == every.id
        one = await scheduler.set(60, recorder.remind)
        assert (await scheduler.set(60, recorder.remind)).id != one.id
        kept = await scheduler.set(60, recorder.remind, {"k": 1}, idempotent=True)
        assert (
            await scheduler.set(60, recorder.remind, {"k": 1}, idempotent=True)
        ).id == kept.id

    asyncio.run(scenario())
    assert types(events).count("schedule:create") == 7


# Running schedules


def test_one_shots_run_in_host_context_then_disappear() -> None:
    lifecycle, scheduler, recorder, events = setup()

    async def scenario() -> None:
        created = await scheduler.set(past(), recorder.remind, {"n": 1})
        await lifecycle.alarm()
        assert await scheduler.get(created.id) is None

    asyncio.run(scenario())
    name, payload, schedule = recorder.runs[0]
    assert (name, payload, schedule.type) == ("remind", {"n": 1}, "scheduled")
    assert recorder.in_context == [True]
    assert "schedule:execute" in types(events)


def test_cron_and_interval_schedules_move_to_their_next_time() -> None:
    lifecycle, scheduler, recorder, _ = setup()

    async def scenario() -> tuple[Schedule | None, Schedule | None]:
        cron = await scheduler.set("0 9 * * *", recorder.remind)
        every = await scheduler.every(3600, recorder.remind)
        make_due(lifecycle, cron.id)
        make_due(lifecycle, every.id)
        await lifecycle.alarm()
        return await scheduler.get(cron.id), await scheduler.get(every.id)

    cron, every = asyncio.run(scenario())
    assert len(recorder.runs) == 2
    assert cron is not None and cron.time > datetime.now(UTC) and cron.time.hour == 9
    assert every is not None
    assert timedelta(minutes=59) < every.time - datetime.now(UTC) <= timedelta(hours=1)


def test_failures_are_retried_reported_and_recurring_ones_continue() -> None:
    reported: list[Exception] = []

    async def on_error(error: Exception) -> None:
        reported.append(error)

    lifecycle, scheduler, recorder, events = setup(retry=FAST_RETRY, on_error=on_error)
    recorder.failures_left = 10

    async def scenario() -> Schedule | None:
        every = await scheduler.every(3600, recorder.flaky)
        make_due(lifecycle, every.id)
        await lifecycle.alarm()
        return await scheduler.get(every.id)

    every = asyncio.run(scenario())
    assert len(recorder.runs) == 2
    assert [str(e) for e in reported] == ["callback bug"]
    assert types(events)[-3:] == [
        "schedule:execute",
        "schedule:retry",
        "schedule:error",
    ]
    assert every is not None and every.time > datetime.now(UTC)


def test_a_callback_gone_by_run_time_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    lifecycle, scheduler, recorder, _ = setup()

    async def scenario() -> None:
        await scheduler.set(past(), recorder.remind)
        scheduler._target = object()  # as if the method was renamed
        await lifecycle.alarm()

    asyncio.run(scenario())
    assert "not found" in caplog.text


class SchedulesOnStart(Scheduler):
    async def on_start(self) -> None:
        await super().on_start()
        await self.set(60, "remind")
        await self.set(60, "remind")


def test_one_shots_scheduled_during_startup_warn_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder = Recorder()
    lifecycle = Lifecycle(Host()).use(SchedulesOnStart(target=recorder))
    asyncio.run(lifecycle.start())
    assert caplog.text.count("during startup") == 1


# Reading and cancelling


def test_get_list_and_cancel() -> None:
    _, scheduler, recorder, events = setup()

    async def scenario() -> None:
        soon = await scheduler.set(60, recorder.remind)
        later = await scheduler.set(datetime(2031, 1, 1, tzinfo=UTC), recorder.remind)
        cron = await scheduler.set("0 9 * * *", recorder.remind)
        assert {s.id for s in await scheduler.list()} == {soon.id, later.id, cron.id}
        assert [s.id for s in await scheduler.list(type="cron")] == [cron.id]
        assert [s.id for s in await scheduler.list(id=soon.id)] == [soon.id]
        window = await scheduler.list(start=datetime(2030, 12, 1, tzinfo=UTC))
        assert [s.id for s in window] == [later.id]
        assert await scheduler.cancel(soon.id)
        assert not await scheduler.cancel(soon.id)

    asyncio.run(scenario())
    assert events[-1].type == "schedule:cancel"


# Facets


def facet_setup() -> tuple[Lifecycle, Scheduler, Scheduler, Recorder]:
    recorder = Recorder()
    root_scheduler = Scheduler(target=Recorder())
    root = Lifecycle(Host("root")).use(root_scheduler)
    facet_scheduler = Scheduler(target=recorder, retry=FAST_RETRY)
    facet = Lifecycle(Host("facet")).use(facet_scheduler)
    root_transport = LocalTransport(None)
    root_transport.peers["child"] = facet
    facet_transport = LocalTransport(RouteAddress(key="child", data="child-data"))
    facet_transport.root = root
    root._set_route_transport(root_transport)
    facet._set_route_transport(facet_transport)
    return root, root_scheduler, facet_scheduler, recorder


def test_facet_schedules_live_on_the_root_and_run_in_the_facet() -> None:
    root, root_scheduler, facet_scheduler, recorder = facet_setup()

    async def scenario() -> None:
        every = await facet_scheduler.every(3600, recorder.remind, {"k": 1})
        again = await facet_scheduler.every(3600, recorder.remind, {"k": 1})
        assert again.id == every.id
        assert [s.id for s in await facet_scheduler.list()] == [every.id]
        assert await root_scheduler.list() == []
        make_due(root, every.id)
        await root.alarm()
        assert [(name, payload) for name, payload, _ in recorder.runs] == [
            ("remind", {"k": 1})
        ]
        moved = await facet_scheduler.get(every.id)
        assert moved is not None and moved.time > datetime.now(UTC)
        assert await facet_scheduler.cancel(every.id)
        assert await facet_scheduler.list() == []

    asyncio.run(scenario())


# Agent


class Reminders(Agent):
    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.seen: list[Any] = []

    async def remind(self, payload: Any, schedule: Schedule) -> None:
        current = get_current_agent()
        self.seen.append((payload, current is not None and current.agent is self))


def test_agent_schedules_its_own_methods() -> None:
    agent = Reminders(FakeCtx("a"), env=None)

    async def scenario() -> None:
        created = await agent.schedule(past(), agent.remind, {"n": 1})
        assert await agent.get_schedule_by_id(created.id) is not None
        every = await agent.schedule_every(60, "remind")
        assert [s.id for s in await agent.list_schedules(type="interval")] == [every.id]
        await agent.alarm()  # ty: ignore[unresolved-attribute]
        assert await agent.cancel_schedule(every.id)

    asyncio.run(scenario())
    assert agent.seen == [({"n": 1}, True)]
    kv = agent.ctx.storage.kv
    assert kv["cf_agents:schedules_schema_version"] == 2
    assert kv["cf_agents:queue_schema_version"] == 1
