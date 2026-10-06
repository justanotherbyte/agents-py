import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fake_runtime import FakeCtx, FakeStorage

from agents import (
    Agent,
    AgentOptions,
    FiberCompleted,
    FiberConflictError,
    FiberContext,
    FiberInspection,
    FiberRecoveryContext,
    FiberRecoveryResult,
    LifecycleCapability,
    ObservabilityEvent,
)

# Fibers left hanging on purpose (a dead isolate's), kept referenced.
HUNG: list[asyncio.Future[Any]] = []


class Recorder:
    def __init__(self) -> None:
        self.events: list[ObservabilityEvent] = []

    def emit(self, event: ObservabilityEvent) -> None:
        self.events.append(event)

    def types(self, prefix: str = "fiber:") -> list[str]:
        return [e.type for e in self.events if e.type.startswith(prefix)]


class Worker(Agent):
    options = AgentOptions(keep_alive_interval=0.05)

    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.observability = Recorder()
        self.recovered: list[FiberRecoveryContext] = []
        self.after_start: list[bool] = []
        self.started = False
        self.result: FiberRecoveryResult | None = None
        self.fail_recovery = False
        self.release = asyncio.Event()

    async def on_start(self) -> None:
        self.started = True

    async def on_fiber_recovered(
        self, ctx: FiberRecoveryContext
    ) -> FiberRecoveryResult | None:
        self.recovered.append(ctx)
        self.after_start.append(self.started)
        if self.fail_recovery:
            raise RuntimeError("recovery hook failed")
        return self.result

    async def hang(self, ctx: FiberContext) -> None:
        ctx.stash({"step": 1})
        await self.release.wait()


def status(record: FiberInspection | None) -> str:
    assert record is not None
    return record.status


def make(storage: FakeStorage | None = None, cls: type[Worker] = Worker) -> Worker:
    return cls(FakeCtx("w", storage=storage), env=None)


def run_rows(agent: Agent) -> list[dict[str, Any]]:
    return list(agent.sql("SELECT id, name, snapshot FROM cf_agents_runs"))


def recorder(agent: Worker) -> Recorder:
    assert isinstance(agent.observability, Recorder)
    return agent.observability


async def crash(agent: Worker, start: Any) -> Worker:
    """Leave ``start``'s fiber hanging in ``agent``; return the next isolate."""
    HUNG.append(asyncio.ensure_future(start()))
    await asyncio.sleep(0.01)
    return make(agent.ctx.storage, type(agent))


# Plain fibers


def test_run_fiber_returns_and_checkpoints_while_running() -> None:
    agent = make()
    seen: list[Any] = []

    async def scenario() -> int:
        await agent.lifecycle.start()

        async def body(ctx: FiberContext) -> int:
            ctx.stash({"a": 1})
            agent.stash({"a": 2})  # the agent method reaches the same fiber
            seen.extend(run_rows(agent))
            return 7

        return await agent.run_fiber("work", body)

    assert asyncio.run(scenario()) == 7
    assert [(r["name"], json.loads(r["snapshot"])) for r in seen] == [
        ("work", {"a": 2})
    ]
    assert run_rows(agent) == []
    assert recorder(agent).types() == ["fiber:run:started", "fiber:run:completed"]


def test_run_fiber_errors_propagate_and_clean_up() -> None:
    agent = make()

    async def scenario() -> None:
        await agent.lifecycle.start()

        async def body(ctx: FiberContext) -> None:
            raise ValueError("boom")

        await agent.run_fiber("work", body)

    with pytest.raises(ValueError, match="boom"):
        asyncio.run(scenario())
    assert run_rows(agent) == []
    failed = recorder(agent).events[-1]
    assert failed.type == "fiber:run:failed" and failed.payload["error"] == "boom"


def test_stash_outside_a_fiber_is_an_error() -> None:
    with pytest.raises(RuntimeError, match="outside a fiber"):
        make().stash({})


# Keep-alive


def test_a_running_fiber_holds_a_keep_alive_heartbeat() -> None:
    agent = make()

    async def scenario() -> list[bool]:
        await agent.lifecycle.start()
        jobs = agent._keep_alive.lifecycle.jobs
        during: list[bool] = []

        async def body(ctx: FiberContext) -> None:
            during.append(jobs.get("keep-alive") is not None)
            await asyncio.sleep(0.07)
            await agent.lifecycle.alarm()  # the heartbeat reschedules itself
            during.append(jobs.get("keep-alive") is not None)

        await agent.run_fiber("work", body)
        await asyncio.sleep(0)
        return [*during, jobs.get("keep-alive") is None]

    assert asyncio.run(scenario()) == [True, True, True]


def test_keep_alive_while_releases_on_error() -> None:
    agent = make()

    async def scenario() -> bool:
        await agent.lifecycle.start()

        async def fail() -> None:
            raise KeyError("x")

        with pytest.raises(KeyError):
            await agent.keep_alive_while(fail)
        await asyncio.sleep(0)
        return agent._keep_alive.lifecycle.jobs.get("keep-alive") is None

    assert asyncio.run(scenario())


def test_a_stale_heartbeat_is_dropped_on_wake() -> None:
    agent = make()

    async def scenario() -> bool:
        await agent.lifecycle.start()
        await agent.keep_alive()  # never released: the isolate "dies"
        restarted = make(agent.ctx.storage)
        await restarted.lifecycle.start()
        return restarted._keep_alive.lifecycle.jobs.get("keep-alive") is None

    assert asyncio.run(scenario())


# Recovery of plain fibers


def test_an_interrupted_fiber_is_recovered_on_the_next_wake() -> None:
    agent = make()

    async def scenario() -> Worker:
        await agent.lifecycle.start()
        restarted = await crash(agent, lambda: agent.run_fiber("job", agent.hang))
        await restarted.lifecycle.start()
        return restarted

    restarted = asyncio.run(scenario())
    [ctx] = restarted.recovered
    assert restarted.after_start == [False]  # startup recovery precedes on_start
    assert (ctx.name, ctx.snapshot, ctx.status) == ("job", {"step": 1}, None)
    assert run_rows(restarted) == []
    assert recorder(restarted).types() == [
        "fiber:recovery:detected",
        "fiber:run:interrupted",
        "fiber:recovery:attempt",
        "fiber:recovery:handled",
    ]


def test_a_failing_hook_retries_with_backoff_until_max_age() -> None:
    class Aging(Worker):
        options = AgentOptions(
            keep_alive_interval=0.05, fiber_recovery_max_age=timedelta(seconds=0.3)
        )

    agent = make(cls=Aging)

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()
        restarted = await crash(agent, lambda: agent.run_fiber("job", agent.hang))
        restarted.fail_recovery = True
        await restarted.lifecycle.start()
        jobs = restarted._fibers.lifecycle.jobs
        assert len(run_rows(restarted)) == 1  # kept to retry
        assert restarted._fibers._no_progress_scans == 1
        await asyncio.sleep(0.11)  # the first retry waits 2 intervals
        await restarted.lifecycle.alarm()
        assert restarted._fibers._no_progress_scans == 2  # the next waits 4
        await asyncio.sleep(0.22)  # past max_age now
        await restarted.lifecycle.alarm()
        return [len(restarted.recovered), run_rows(restarted), jobs.get("housekeeping")]

    attempts, rows, job = asyncio.run(scenario())
    assert attempts == 3 and rows == [] and job is None


def test_a_settled_fiber_whose_delete_failed_is_dropped_without_the_hook() -> None:
    agent = make()

    async def scenario() -> Worker:
        await agent.lifecycle.start()
        agent.sql(
            "INSERT INTO cf_agents_runs (id, name, created_at, completed_at, outcome)"
            " VALUES ('f1', 'done', 1, 2, 'completed')"
        )
        restarted = make(agent.ctx.storage)
        await restarted.lifecycle.start()
        return restarted

    restarted = asyncio.run(scenario())
    assert restarted.recovered == [] and run_rows(restarted) == []


def test_the_internal_hook_runs_first_and_is_timed_out() -> None:
    class Framework(Worker):
        options = AgentOptions(fiber_recovery_hook_timeout=0.02)

        async def _handle_internal_fiber_recovery(
            self, ctx: FiberRecoveryContext
        ) -> bool:
            if ctx.name == "chat":
                return True
            await asyncio.sleep(1)
            return False

    agent = make(cls=Framework)

    async def scenario() -> Worker:
        await agent.lifecycle.start()
        HUNG.append(asyncio.ensure_future(agent.run_fiber("chat", agent.hang)))
        restarted = await crash(agent, lambda: agent.run_fiber("slow", agent.hang))
        await restarted.lifecycle.start()
        return restarted

    restarted = asyncio.run(scenario())
    assert restarted.recovered == []  # "chat" handled; "slow" timed out first
    failed = [
        e for e in recorder(restarted).events if e.type == "fiber:recovery:failed"
    ]
    assert len(failed) == 1 and "timed out" in failed[0].payload["error"]
    handled = [
        e for e in recorder(restarted).events if e.type == "fiber:recovery:handled"
    ]
    assert [e.payload["status"] for e in handled] == ["internal"]


# Managed fibers


def test_start_fiber_runs_in_the_background_and_dedupes() -> None:
    agent = make()

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()

        async def body(ctx: FiberContext) -> None:
            ctx.stash({"sent": True})

        first = await agent.start_fiber(
            "send", body, idempotency_key="k1", metadata={"to": "a"}
        )
        assert first.accepted and first.status == "pending"
        again = await agent.start_fiber("send", body, idempotency_key="k1")
        waited = await agent.start_fiber(
            "send", body, fiber_id=first.fiber_id, wait_for_completion=True
        )
        return [first, again, waited, await agent.inspect_fiber_by_key("k1")]

    first, again, waited, inspected = asyncio.run(scenario())
    assert not again.accepted and again.fiber_id == first.fiber_id
    assert not waited.accepted and waited.status == "completed"
    assert inspected.snapshot == {"sent": True} and inspected.metadata == {"to": "a"}
    assert inspected.started_at is not None and inspected.settled_at is not None


def test_start_fiber_rejects_blank_and_conflicting_ids() -> None:
    agent = make()

    async def noop(ctx: FiberContext) -> None:
        pass

    async def scenario() -> None:
        await agent.lifecycle.start()
        await agent.start_fiber("a", noop, fiber_id="f1", wait_for_completion=True)
        await agent.start_fiber(
            "b", noop, idempotency_key="k", wait_for_completion=True
        )
        with pytest.raises(FiberConflictError):
            await agent.start_fiber("a", noop, fiber_id="f1", idempotency_key="k")
        with pytest.raises(ValueError, match="blank"):
            await agent.start_fiber("a", noop, fiber_id=" ")

    asyncio.run(scenario())


def test_a_failed_managed_fiber_records_its_error() -> None:
    agent = make()

    async def scenario() -> Any:
        await agent.lifecycle.start()

        async def body(ctx: FiberContext) -> None:
            raise ValueError("nope")

        return await agent.start_fiber("x", body, wait_for_completion=True)

    result = asyncio.run(scenario())
    assert (result.status, result.error) == ("error", "nope")


def test_cancel_fiber_stops_the_body() -> None:
    agent = make()
    reached: list[str] = []

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()

        async def body(ctx: FiberContext) -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                reached.append("cancelled")
                raise

        started = await agent.start_fiber("long", body)
        await asyncio.sleep(0.01)
        cancelled = await agent.cancel_fiber(started.fiber_id, "user asked")
        again = await agent.cancel_fiber(started.fiber_id)
        await asyncio.sleep(0.01)
        return [cancelled, again, await agent.inspect_fiber(started.fiber_id)]

    cancelled, again, record = asyncio.run(scenario())
    assert cancelled and not again and reached == ["cancelled"]
    assert (record.status, record.error) == ("aborted", "user asked")
    assert run_rows(agent) == []


def test_an_interrupted_managed_fiber_is_settled_by_the_hook() -> None:
    agent = make()

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()
        restarted = await crash(
            agent, lambda: agent.start_fiber("m", agent.hang, idempotency_key="k")
        )
        restarted.result = FiberCompleted(snapshot={"done": True})
        await restarted.lifecycle.start()
        return [restarted.recovered[0], await restarted.inspect_fiber_by_key("k")]

    ctx, record = asyncio.run(scenario())
    assert (ctx.status, ctx.idempotency_key, ctx.snapshot) == (
        "interrupted",
        "k",
        {"step": 1},
    )
    assert (record.status, record.snapshot) == ("completed", {"done": True})


def test_resolve_fiber_settles_only_interrupted_records() -> None:
    agent = make()

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()
        restarted = await crash(
            agent, lambda: agent.start_fiber("m", agent.hang, fiber_id="f")
        )
        await restarted.lifecycle.start()
        interrupted = await restarted.inspect_fiber("f")
        resolved = await restarted.resolve_fiber("f", FiberCompleted())
        again = await restarted.resolve_fiber("f", FiberCompleted())
        return [
            status(interrupted),
            resolved,
            again,
            status(await restarted.inspect_fiber("f")),
        ]

    assert asyncio.run(scenario()) == ["interrupted", True, False, "completed"]


def test_a_record_that_never_started_is_interrupted_too() -> None:
    agent = make()

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()
        agent.sql(
            "INSERT INTO cf_agents_fibers (fiber_id, name, status, created_at)"
            " VALUES ('f', 'queued', 'pending', 1)"
        )
        restarted = make(agent.ctx.storage)
        await restarted.lifecycle.start()
        return [
            [c.name for c in restarted.recovered],
            status(await restarted.inspect_fiber("f")),
        ]

    assert asyncio.run(scenario()) == [["queued"], "interrupted"]


def test_waiting_on_a_fiber_left_by_a_dead_isolate_recovers_it() -> None:
    agent = make()

    async def scenario() -> Any:
        await agent.lifecycle.start()
        # A running fiber no isolate is executing (as after a crash mid-wake).
        agent.sql(
            "INSERT INTO cf_agents_fibers (fiber_id, name, status, created_at)"
            " VALUES ('f', 'm', 'running', 1)"
        )
        agent.sql(
            "INSERT INTO cf_agents_runs (id, name, created_at) VALUES ('f', 'm', 1)"
        )
        return await agent.start_fiber(
            "m", agent.hang, fiber_id="f", wait_for_completion=True
        )

    result = asyncio.run(scenario())
    assert (result.accepted, result.status) == (False, "interrupted")
    assert [c.id for c in agent.recovered] == ["f"]


def test_list_and_delete_fibers() -> None:
    agent = make()

    async def noop(ctx: FiberContext) -> None:
        pass

    async def fail(ctx: FiberContext) -> None:
        raise ValueError("x")

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()
        for i in range(3):
            await agent.start_fiber(
                "ok", noop, fiber_id=f"ok{i}", wait_for_completion=True
            )
        await agent.start_fiber("bad", fail, fiber_id="bad", wait_for_completion=True)
        agent.sql(
            "INSERT INTO cf_agents_fibers (fiber_id, name, status, created_at)"
            " VALUES ('int', 'x', 'interrupted', 1)"
        )
        listed = [
            f.fiber_id for f in await agent.list_fibers(status="completed", limit=2)
        ]
        errors = [
            f.fiber_id for f in await agent.list_fibers(status=["error"], name="bad")
        ]
        future = datetime.now(UTC) + timedelta(days=1)
        deleted = await agent.delete_fibers(settled_before=future)
        remaining = [f.fiber_id for f in await agent.list_fibers()]
        deleted_interrupted = await agent.delete_fibers(status="interrupted")
        nothing = await agent.delete_fibers(status="running")
        return [listed, errors, deleted, remaining, deleted_interrupted, nothing]

    listed, errors, deleted, remaining, deleted_interrupted, nothing = asyncio.run(
        scenario()
    )
    assert len(listed) == 2 and errors == ["bad"]
    assert deleted == 4 and remaining == ["int"]
    assert deleted_interrupted == 1 and nothing == 0


def test_recovery_waits_for_capabilities_a_subclass_installs() -> None:
    class Late(LifecycleCapability):
        def __init__(self) -> None:
            super().__init__("late")
            self.started = False

        async def on_start(self) -> None:
            self.started = True

    class WithLate(Worker):
        def __init__(self, ctx: Any, env: Any) -> None:
            super().__init__(ctx, env)
            self.late = self.use(Late())
            self.late_started: list[bool] = []

        async def on_fiber_recovered(
            self, ctx: FiberRecoveryContext
        ) -> FiberRecoveryResult | None:
            self.late_started.append(self.late.started)
            return await super().on_fiber_recovered(ctx)

    agent = make(cls=WithLate)

    async def scenario() -> Any:
        await agent.lifecycle.start()
        restarted = await crash(agent, lambda: agent.run_fiber("job", agent.hang))
        await restarted.lifecycle.start()
        return restarted

    restarted = asyncio.run(scenario())
    assert restarted.late_started == [True]
    assert restarted.after_start == [False]  # still before the user's on_start
