import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import _fake_ffi
import pytest
from fake_runtime import Host

from agents.core.types import RetryOptions
from agents.lifecycle import (
    JobContext,
    JobOutcome,
    Lifecycle,
    LifecycleCapability,
    MemoryLimitContext,
    Reschedule,
)

FAST_RETRY = RetryOptions(max_attempts=2, base_delay=0.001, max_delay=0.002)


def past(seconds: float = 1) -> datetime:
    return datetime.now(UTC) - timedelta(seconds=seconds)


def future(seconds: float = 60) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


class Worker(LifecycleCapability):
    """Runs jobs by calling a scripted behavior per job fn."""

    def __init__(self, capability_id: str = "worker") -> None:
        super().__init__(capability_id)
        self.runs: list[tuple[str, int]] = []
        self.behavior: dict[str, Any] = {}
        self.errors: list[tuple[str, Exception]] = []

    async def on_job(self, context: JobContext) -> JobOutcome:
        job = context.job
        self.runs.append((job.fn, context.attempt))
        behavior = self.behavior.get(job.fn)
        if isinstance(behavior, Exception):
            raise behavior
        if callable(behavior):
            return await behavior(job)
        return behavior

    async def on_job_error(self, context: JobContext, error: Exception) -> JobOutcome:
        self.errors.append((context.job.fn, error))
        return None


def setup(worker: Worker | None = None) -> tuple[Lifecycle, Worker, Host]:
    host = Host()
    worker = worker or Worker()
    lifecycle = Lifecycle(host).use(worker)
    return lifecycle, worker, host


# Queue and alarm arming
def test_push_arms_the_alarm_for_the_earliest_job() -> None:
    lifecycle, worker, host = setup()

    async def scenario() -> None:
        await lifecycle.start()
        later = future(120)
        sooner = future(60)
        await worker.lifecycle.jobs.push(fn="b", time=later)
        await worker.lifecycle.jobs.push(fn="a", time=sooner)
        assert host.ctx.storage.alarm == int(sooner.timestamp() * 1000)

    asyncio.run(scenario())


def test_cancelling_the_last_job_deletes_the_alarm() -> None:
    lifecycle, worker, host = setup()

    async def scenario() -> None:
        await lifecycle.start()
        job = await worker.lifecycle.jobs.push(fn="a", time=future())
        assert await worker.lifecycle.jobs.cancel(job.id)
        assert host.ctx.storage.alarm is None
        assert not await worker.lifecycle.jobs.cancel(job.id)

    asyncio.run(scenario())


def test_overdue_job_rearms_into_the_future() -> None:
    lifecycle, worker, host = setup()

    async def scenario() -> None:
        await lifecycle.start()
        await worker.lifecycle.jobs.push(fn="a", time=past(60))
        alarm = host.ctx.storage.alarm
        assert alarm is not None
        assert alarm > int(past(1).timestamp() * 1000)

    asyncio.run(scenario())


def test_job_ids_are_scoped_to_their_owner() -> None:
    other = Worker("other")
    lifecycle, worker, _ = setup()
    lifecycle.use(other)

    async def scenario() -> None:
        await lifecycle.start()
        await worker.lifecycle.jobs.push(fn="a", time=future(), id="shared")
        with pytest.raises(ValueError, match="already belongs to 'worker'"):
            await other.lifecycle.jobs.push(fn="b", time=future(), id="shared")
        assert other.lifecycle.jobs.get("shared") is None

    asyncio.run(scenario())


def test_job_round_trips_payload_retry_and_flags() -> None:
    lifecycle, worker, _ = setup()

    async def scenario() -> None:
        await lifecycle.start()
        when = future()
        job = await worker.lifecycle.jobs.push(
            fn="a",
            time=when,
            payload={"x": [1, None]},
            retry=RetryOptions(max_attempts=5, base_delay=0.25, max_delay=2),
            singleflight=True,
        )
        assert job.payload == {"x": [1, None]}
        assert job.retry == RetryOptions(max_attempts=5, base_delay=0.25, max_delay=2)
        assert job.singleflight
        assert abs((job.time - when).total_seconds()) < 0.001
        assert job.time.tzinfo is not None

    asyncio.run(scenario())


def test_naive_job_time_is_rejected() -> None:
    lifecycle, worker, _ = setup()
    asyncio.run(lifecycle.start())
    with pytest.raises(TypeError, match="naive"):
        asyncio.run(worker.lifecycle.jobs.push(fn="a", time=datetime(2030, 1, 1)))


# The alarm loop
def test_alarm_runs_due_jobs_in_order_and_completes_them() -> None:
    lifecycle, worker, _ = setup()

    async def scenario() -> None:
        await lifecycle.start()
        await worker.lifecycle.jobs.push(fn="second", time=past(1))
        await worker.lifecycle.jobs.push(fn="first", time=past(2))
        await worker.lifecycle.jobs.push(fn="later", time=future())
        await lifecycle.alarm()
        assert [fn for fn, _ in worker.runs] == ["first", "second"]
        assert [job.fn for job in worker.lifecycle.jobs.list()] == ["later"]

    asyncio.run(scenario())


def test_reschedule_and_yield_outcomes() -> None:
    lifecycle, worker, _ = setup()
    when = future(300)

    async def reschedule(_job: Any) -> JobOutcome:
        return Reschedule(at=when)

    worker.behavior = {"again": reschedule, "stay": "yield"}

    async def scenario() -> None:
        await lifecycle.start()
        moved = await worker.lifecycle.jobs.push(fn="again", time=past())
        stays = await worker.lifecycle.jobs.push(fn="stay", time=past())
        await lifecycle.alarm()
        job = worker.lifecycle.jobs.get(moved.id)
        assert job is not None
        assert abs((job.time - when).total_seconds()) < 0.001
        assert worker.lifecycle.jobs.get(stays.id) is not None

    asyncio.run(scenario())


def test_failing_job_is_retried_then_reported_to_on_job_error() -> None:
    lifecycle, worker, _ = setup()
    error = RuntimeError("app bug")
    worker.behavior = {"bad": error}

    async def scenario() -> None:
        await lifecycle.start()
        await worker.lifecycle.jobs.push(fn="bad", time=past(), retry=FAST_RETRY)
        await lifecycle.alarm()
        assert worker.runs == [("bad", 1), ("bad", 2)]
        assert worker.errors == [("bad", error)]
        assert worker.lifecycle.jobs.list() == []

    asyncio.run(scenario())


def test_platform_failure_preserves_the_job_and_reraises() -> None:
    lifecycle, worker, _ = setup()
    worker.behavior = {"blip": RuntimeError("Network connection lost.")}

    async def scenario() -> None:
        await lifecycle.start()
        job = await worker.lifecycle.jobs.push(fn="blip", time=past(), retry=FAST_RETRY)
        with pytest.raises(RuntimeError, match="Network connection lost"):
            await lifecycle.alarm()
        assert worker.lifecycle.jobs.get(job.id) is not None
        assert worker.errors == []

    asyncio.run(scenario())


def test_job_for_an_unknown_owner_is_dropped(caplog: pytest.LogCaptureFixture) -> None:
    lifecycle, _, _ = setup()

    async def scenario() -> None:
        await lifecycle.start()
        lifecycle._queue.push("ghost", fn="x", time=past())
        await lifecycle.alarm()
        assert lifecycle._queue.list("ghost") == []

    asyncio.run(scenario())
    assert "dropping it" in caplog.text


def test_same_id_push_during_dispatch_supersedes_the_outcome() -> None:
    lifecycle, worker, _ = setup()
    newer = future(600)

    async def repush(job: Any) -> JobOutcome:
        await worker.lifecycle.jobs.push(fn="loop", time=newer, id=job.id)
        return None  # would delete the job, but newer intent exists

    worker.behavior = {"loop": repush}

    async def scenario() -> None:
        await lifecycle.start()
        job = await worker.lifecycle.jobs.push(fn="loop", time=past(), id="j1")
        await lifecycle.alarm()
        stored = worker.lifecycle.jobs.get(job.id)
        assert stored is not None
        assert abs((stored.time - newer).total_seconds()) < 0.001

    asyncio.run(scenario())


def test_singleflight_job_still_running_is_skipped() -> None:
    lifecycle, worker, _ = setup()

    async def scenario() -> None:
        await lifecycle.start()
        job = await worker.lifecycle.jobs.push(
            fn="once", time=past(), singleflight=True
        )
        lifecycle._queue.mark_running(job.id, int(past().timestamp() * 1000))
        await lifecycle.alarm()
        assert worker.runs == []

    asyncio.run(scenario())


class HostWithJobs(Host):
    def __init__(self) -> None:
        super().__init__()
        self.handled: list[str] = []

    async def on_job(self, context: JobContext) -> JobOutcome:
        self.handled.append(context.job.fn)
        return None

    async def on_alarm(self) -> None:
        self.handled.append("on_alarm")


def test_host_jobs_and_on_alarm_run_after_due_jobs() -> None:
    host = HostWithJobs()
    lifecycle = Lifecycle(host)

    async def scenario() -> None:
        await lifecycle.start()
        await lifecycle.jobs.push(fn="host-job", time=past())
        await lifecycle.alarm()

    asyncio.run(scenario())
    assert host.handled == ["host-job", "on_alarm"]


# Memory-limit circuit breaker
class Policy(Worker):
    def __init__(self) -> None:
        super().__init__("policy")
        self.strikes: list[MemoryLimitContext] = []

    async def on_memory_limit(self, context: MemoryLimitContext) -> None:
        self.strikes.append(context)


def test_memory_limit_reset_backs_off_then_seals() -> None:
    _fake_ffi.aborts.clear()
    worker = Policy()
    lifecycle, _, host = setup(worker)
    lifecycle._driver._max_strikes = 2
    reset = RuntimeError("Durable Object's isolate exceeded its memory limit")
    worker.behavior = {"heavy": reset}

    async def scenario() -> None:
        await lifecycle.start()
        job = await worker.lifecycle.jobs.push(fn="heavy", time=past())
        await lifecycle.alarm()  # strike 1: swallowed, job backed off
        stored = worker.lifecycle.jobs.get(job.id)
        assert stored is not None
        assert stored.time > datetime.now(UTC)
        assert host.ctx.storage.kv["cf_agents:oom_alarm_strikes"] == 1
        assert worker.strikes[0].sealed is False
        assert worker.runs == [("heavy", 1)]  # never retried in-process

        # A fresh isolate (new driver state) hits the reset again: sealed.
        lifecycle._driver._strike = None
        lifecycle._driver._strike_recorded_this_isolate = False
        lifecycle._queue.retime(job.id, int(past().timestamp() * 1000))
        await lifecycle.alarm()
        assert worker.lifecycle.jobs.get(job.id) is None
        assert worker.strikes[-1].sealed is True
        assert "cf_agents:oom_alarm_strikes" not in host.ctx.storage.kv

    asyncio.run(scenario())
    assert len(_fake_ffi.aborts) == 2
    assert "sealed" in _fake_ffi.aborts[-1]


def test_clean_alarm_clears_previous_strikes() -> None:
    lifecycle, worker, host = setup()

    async def scenario() -> None:
        await lifecycle.start()
        host.ctx.storage.kv["cf_agents:oom_alarm_strikes"] = 2
        await worker.lifecycle.jobs.push(fn="ok", time=past())
        await lifecycle.alarm()
        assert "cf_agents:oom_alarm_strikes" not in host.ctx.storage.kv

    asyncio.run(scenario())


# Rearm during startup
class PushesOnStart(Worker):
    async def on_start(self) -> None:
        await self.lifecycle.jobs.push(fn="boot", time=future())


def test_rearm_requested_during_startup_happens_after_it() -> None:
    worker = PushesOnStart()
    lifecycle, _, host = setup(worker)
    asyncio.run(lifecycle.start())
    assert host.ctx.storage.alarm_history == [host.ctx.storage.alarm]
    assert host.ctx.storage.alarm is not None
