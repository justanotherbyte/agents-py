import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fake_runtime import FakeCtx, Host, LocalTransport

import agents.tasks.tasks as tasks_module
from agents import Agent, get_current_agent
from agents.core.timing import now_ms
from agents.lifecycle import Lifecycle, LifecycleEvent, MemoryLimitContext, RouteAddress
from agents.tasks import (
    CancelledRun,
    CompletedRun,
    FailedRun,
    NonRetryableError,
    PendingRun,
    StepRetries,
    Tasks,
    TaskSerializationError,
    TaskStep,
    TaskStepAttempt,
    WaitingRun,
    task,
)

FAST = StepRetries(limit=3, delay=0.01)


def setup(
    definitions: dict[str, Any], **options: Any
) -> tuple[Lifecycle, Tasks, list[LifecycleEvent]]:
    tasks = Tasks(definitions=definitions, **options)
    lifecycle = Lifecycle(Host()).use(tasks)
    events: list[LifecycleEvent] = []
    lifecycle._set_event_sink(events.append)
    return lifecycle, tasks, events


async def settle(tasks: Tasks) -> None:
    """Wait for warm starts and live attempts in this isolate to finish."""
    while tasks._background or tasks._active:
        pending = [*tasks._background, *(a.task for a in tasks._active.values())]
        await asyncio.gather(*pending, return_exceptions=True)


async def wake(lifecycle: Lifecycle, tasks: Tasks, after: float = 0.0) -> None:
    """Let deadlines pass, run the alarm, and wait for detached work."""
    await asyncio.sleep(after)
    await lifecycle.alarm()
    await settle(tasks)


def event_types(events: list[LifecycleEvent]) -> list[str]:
    return [event.type for event in events]


# Running


def test_a_run_completes_with_its_result_and_events() -> None:
    async def double(n: int, step: TaskStep) -> int:
        async def compute(_attempt: TaskStepAttempt) -> int:
            return n * 2

        return await step.do("compute", compute)

    _, tasks, events = setup({"double": double})

    async def scenario() -> Any:
        receipt = await tasks.run("double", 21)
        assert receipt.accepted and receipt.state == "pending"
        await settle(tasks)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, CompletedRun) and run.result == 42
    assert event_types(events) == [
        "task:accepted",
        "task:attempt:started",
        "task:step:started",
        "task:step:completed",
        "task:completed",
    ]
    assert events[0].payload["runId"] == run.run_id


def test_step_results_are_the_journaled_values_even_live() -> None:
    seen: list[Any] = []

    async def handler(_input: Any, step: TaskStep) -> None:
        async def point(_attempt: TaskStepAttempt) -> tuple[int, int]:
            return (1, 2)

        seen.append(await step.do("point", point))

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> None:
        await tasks.run("h")
        await settle(tasks)

    asyncio.run(scenario())
    assert seen == [[1, 2]]


def test_sleep_parks_the_run_and_replay_skips_completed_steps() -> None:
    calls: list[str] = []

    async def handler(_input: Any, step: TaskStep) -> str:
        async def first(_attempt: TaskStepAttempt) -> str:
            calls.append("first")
            return "a"

        async def second(_attempt: TaskStepAttempt) -> str:
            calls.append("second")
            return "b"

        a = await step.do("first", first)
        await step.sleep("pause", 0.05)
        return a + await step.do("second", second)

    lifecycle, tasks, _ = setup({"h": handler})

    async def scenario() -> tuple[Any, Any]:
        receipt = await tasks.run("h")
        await settle(tasks)
        parked = await tasks.get(receipt.run_id)
        await wake(lifecycle, tasks, after=0.08)
        return parked, await tasks.get(receipt.run_id)

    parked, done = asyncio.run(scenario())
    assert isinstance(parked, WaitingRun) and parked.reason == "sleep"
    assert parked.wake_at > datetime.now(UTC) - timedelta(seconds=1)
    assert isinstance(done, CompletedRun) and done.result == "ab"
    assert calls == ["first", "second"]


def test_failed_steps_retry_durably_then_succeed() -> None:
    attempts: list[int] = []

    async def handler(_input: Any, step: TaskStep) -> str:
        async def flaky(attempt: TaskStepAttempt) -> str:
            attempts.append(attempt.attempt)
            if attempt.attempt < 3:
                raise RuntimeError("try again")
            return "ok"

        return await step.do("flaky", flaky, retries=FAST)

    lifecycle, tasks, events = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        assert isinstance(await tasks.get(receipt.run_id), WaitingRun)
        await wake(lifecycle, tasks, after=0.03)
        await wake(lifecycle, tasks, after=0.05)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, CompletedRun) and run.result == "ok"
    assert attempts == [1, 2, 3]
    assert event_types(events).count("task:step:retry") == 2


def test_exhausted_retries_fail_the_run_and_notify_on_error() -> None:
    reported: list[Exception] = []

    async def on_error(error: Exception) -> None:
        reported.append(error)

    async def handler(_input: Any, step: TaskStep) -> None:
        async def broken(_attempt: TaskStepAttempt) -> None:
            raise ValueError("always")

        await step.do("broken", broken, retries=StepRetries(limit=1))

    _, tasks, _ = setup({"h": handler}, on_error=on_error)

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun)
    assert (run.error.name, run.error.message) == ("ValueError", "always")
    assert [str(e) for e in reported] == ["always"]


def test_non_retryable_errors_skip_retries() -> None:
    attempts: list[int] = []

    async def handler(_input: Any, step: TaskStep) -> None:
        async def refuse(attempt: TaskStepAttempt) -> None:
            attempts.append(attempt.attempt)
            raise NonRetryableError("card declined")

        await step.do("charge", refuse, retries=FAST)

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun) and run.error.name == "NonRetryableError"
    assert attempts == [1]


def test_a_step_that_ignores_cancellation_still_times_out() -> None:
    finished: list[bool] = []

    async def handler(_input: Any, step: TaskStep) -> None:
        async def stubborn(_attempt: TaskStepAttempt) -> None:
            with suppress(asyncio.CancelledError):  # swallows the timeout's cancel
                await asyncio.sleep(10)
            await asyncio.sleep(0.1)
            finished.append(True)

        await step.do("stuck", stubborn, retries=StepRetries(limit=1), timeout=0.02)

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await asyncio.wait_for(settle(tasks), timeout=1)
        run = await tasks.get(receipt.run_id)
        assert finished == []  # settled without waiting for the step to comply
        return run

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun) and run.error.name == "StepTimeoutError"


def test_signals_are_not_swallowed_by_except_exception() -> None:
    async def handler(_input: Any, step: TaskStep) -> str:
        try:
            await step.sleep("pause", 60)
        except Exception:
            return "swallowed"
        return "not reached on the first attempt"

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        return await tasks.get(receipt.run_id)

    assert isinstance(asyncio.run(scenario()), WaitingRun)


# Cancelling


def test_cancelling_a_parked_run_settles_it_now() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        await step.sleep("pause", 60)

    lifecycle, tasks, events = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        assert await tasks.cancel(receipt.run_id, "no longer needed")
        assert not await tasks.cancel(receipt.run_id)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, CancelledRun) and run.reason == "no longer needed"
    assert lifecycle.jobs.get(f"task:{run.run_id}") is None
    assert "task:cancelled" in event_types(events)


def test_cancelling_a_live_run_cancels_its_step() -> None:
    started = asyncio.Event()

    async def handler(_input: Any, step: TaskStep) -> None:
        async def slow(_attempt: TaskStepAttempt) -> None:
            started.set()
            await asyncio.sleep(10)

        await step.do("slow", slow)

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await started.wait()
        assert await tasks.cancel(receipt.run_id, "stop")
        await asyncio.wait_for(settle(tasks), timeout=1)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, CancelledRun) and run.reason == "stop"


# Acceptance


def test_idempotency_keys_join_the_existing_run() -> None:
    async def handler(_input: Any, _step: TaskStep) -> None: ...

    async def other(_input: Any, _step: TaskStep) -> None: ...

    _, tasks, _ = setup({"h": handler, "other": other})

    async def scenario() -> None:
        first = await tasks.run("h", 1, idempotency_key="k")
        again = await tasks.run("h", 1, idempotency_key="k")
        assert again.run_id == first.run_id and not again.accepted
        with pytest.raises(ValueError, match="belongs to definition"):
            await tasks.run("other", 1, idempotency_key="k")
        await settle(tasks)

    asyncio.run(scenario())


def test_invalid_runs_are_rejected() -> None:
    async def handler(_input: Any, _step: TaskStep) -> None: ...

    _, tasks, _ = setup({"h": handler})
    with pytest.raises(ValueError, match="Unknown task definition"):
        asyncio.run(tasks.run("missing"))
    with pytest.raises(TaskSerializationError):
        asyncio.run(tasks.run("h", {"when": datetime.now(UTC)}))  # ty: ignore[invalid-argument-type]


# Replay checks


def test_duplicate_step_names_fail_the_run() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        async def noop(_attempt: TaskStepAttempt) -> None: ...

        await step.do("same", noop)
        await step.do("same", noop)

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun) and run.error.name == "DuplicateTaskStepError"


def test_a_changed_step_layout_diverges() -> None:
    layout = {"kind": "do"}

    async def handler(_input: Any, step: TaskStep) -> None:
        async def noop(_attempt: TaskStepAttempt) -> None: ...

        if layout["kind"] == "do":
            await step.do("x", noop)
            await step.sleep("pause", 0.02)
        else:
            await step.sleep("x", 0.01)

    lifecycle, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        layout["kind"] = "sleep"  # a deploy changed the definition in flight
        await wake(lifecycle, tasks, after=0.04)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun) and run.error.name == "TaskReplayDivergedError"


def test_a_run_whose_definition_disappeared_fails() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        await step.sleep("pause", 0.01)

    lifecycle, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        renamed = Tasks(definitions={})  # a deploy removed "h"
        fresh = Lifecycle(lifecycle._host).use(renamed)
        await asyncio.sleep(0.02)
        await fresh.alarm()
        await settle(renamed)
        return await renamed.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun) and run.error.name == "MissingTaskDefinitionError"


# Recovery


def test_an_interrupted_attempt_replays_with_evidence() -> None:
    seen: list[Any] = []
    hang = asyncio.Event()

    async def handler(_input: Any, step: TaskStep) -> str:
        seen.append(step.interrupted)

        async def deliver(_attempt: TaskStepAttempt) -> str:
            if not hang.is_set():
                hang.set()
                await asyncio.sleep(10)  # the isolate "dies" here
            return "delivered"

        return await step.do("deliver", deliver)

    lifecycle, tasks, events = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await hang.wait()
        # Simulate the isolate dying: a fresh Tasks on the same storage.
        for attempt in tasks._active.values():
            attempt.task.cancel()
        fresh = Tasks(definitions={"h": handler})
        fresh_lifecycle = Lifecycle(lifecycle._host).use(fresh)
        fresh_events: list[LifecycleEvent] = []
        fresh_lifecycle._set_event_sink(fresh_events.append)
        await fresh_lifecycle.start()  # reconcile: the claimed run is due now
        await fresh_lifecycle.alarm()
        await settle(fresh)
        events.extend(fresh_events)
        return await fresh.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, CompletedRun) and run.result == "delivered"
    assert seen[0] is None
    assert seen[1] is not None and (seen[1].name, seen[1].attempt) == ("deliver", 1)
    assert "task:attempt:interrupted" in event_types(events)


def test_a_superseded_attempt_cannot_settle_the_run() -> None:
    proceed = asyncio.Event()
    started = asyncio.Event()

    async def handler(_input: Any, step: TaskStep) -> str:
        async def work(_attempt: TaskStepAttempt) -> str:
            started.set()
            await proceed.wait()
            return "stale"

        return await step.do("work", work)

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await started.wait()
        # Another isolate claims the run: a new generation.
        tasks._store.sql(
            "UPDATE cf_agents_task_runs SET generation = 'newer' WHERE run_id = ?",
            receipt.run_id,
        )
        proceed.set()
        await settle(tasks)
        return tasks._store.get_run(receipt.run_id)

    row = asyncio.run(scenario())
    assert (
        row is not None and row["state"] == "running" and row["generation"] == "newer"
    )


def test_memory_limit_strikes_back_off_then_seal() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        await step.sleep("pause", 60)

    lifecycle, tasks, _ = setup({"h": handler})

    async def scenario() -> tuple[Any, Any]:
        receipt = await tasks.run("h")
        await settle(tasks)
        job = lifecycle._queue.get("tasks", f"task:{receipt.run_id}")
        assert job is not None
        later = datetime.now(UTC) + timedelta(hours=1)
        await tasks.on_memory_limit(
            MemoryLimitContext(sealed=False, next_time=later, executing=job)
        )
        backed_off = tasks._store.get_run(receipt.run_id)
        await tasks.on_memory_limit(MemoryLimitContext(sealed=True, executing=job))
        return backed_off, await tasks.get(receipt.run_id)

    backed_off, sealed = asyncio.run(scenario())
    assert backed_off["next_at"] > now_ms() + 30 * 60 * 1000
    assert (
        isinstance(sealed, FailedRun) and sealed.error.name == "TaskMemoryLimitSealed"
    )


def test_a_long_attempt_detaches_from_the_alarm_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tasks_module, "_DISPATCH_BUDGET_S", 0.01)
    release = asyncio.Event()

    async def handler(_input: Any, step: TaskStep) -> str:
        async def long(_attempt: TaskStepAttempt) -> str:
            await release.wait()
            return "done"

        return await step.do("long", long)

    lifecycle, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks._enqueue("h", None)
        await asyncio.wait_for(lifecycle.alarm(), timeout=1)  # returns at the budget
        assert receipt.run_id in tasks._active
        release.set()
        await settle(tasks)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, CompletedRun) and run.result == "done"


# Status, retention, listing


def test_status_is_silent_while_replaying_old_ground() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        await step.status("phase 1")
        await step.sleep("pause", 0.02)
        await step.status("phase 2")
        await step.sleep("again", 60)

    lifecycle, tasks, _ = setup({"h": handler})
    written: list[str] = []

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        original = tasks._store.sql

        class Spy:
            def __call__(self, query: Any, *params: Any, **kw: Any) -> Any:
                if "status_message = ?" in query:
                    written.append(params[0])
                return original(query, *params, **kw)

        tasks._store.sql = Spy()  # ty: ignore[invalid-assignment]
        await wake(lifecycle, tasks, after=0.04)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, WaitingRun) and run.status_message == "phase 2"
    assert written == ["phase 2"]  # "phase 1" wasn't re-published on replay


def test_retain_false_deletes_the_run_and_delete_purges_old_ones() -> None:
    async def handler(_input: Any, _step: TaskStep) -> int:
        return 1

    _, tasks, events = setup({"h": handler})

    async def scenario() -> None:
        gone = await tasks.run("h", retain=False)
        kept = [await tasks.run("h") for _ in range(3)]
        pending = await tasks._enqueue("h", None)
        await settle(tasks)
        assert await tasks.get(gone.run_id) is None
        assert len(await tasks.list(status="completed")) == 3
        assert [r.run_id for r in await tasks.list(status=["pending"])] == [
            pending.run_id
        ]
        listed = await tasks.list(definition="h")
        assert len(listed) == 4
        assert sum(isinstance(run, PendingRun) for run in listed) == 1
        future = datetime.now(UTC) + timedelta(minutes=1)
        assert await tasks.delete(settled_before=future, limit=2) == 2
        assert await tasks.delete() == 1
        assert await tasks.get(kept[0].run_id) is None
        assert await tasks.get(pending.run_id) is not None

    asyncio.run(scenario())
    assert event_types(events).count("task:deleted") == 3


# @task on an agent


class Reports(Agent):
    @task
    async def build(self, input: dict[str, int], step: TaskStep) -> int:
        current = get_current_agent()
        assert current is not None and current.agent is self

        async def total(_attempt: TaskStepAttempt) -> int:
            return sum(input.values())

        return await step.do("total", total)

    @task
    async def other(self, input: None, step: TaskStep) -> None: ...


def test_task_methods_on_an_agent() -> None:
    agent = Reports(FakeCtx("r"), env=None)

    async def scenario() -> None:
        receipt = await agent.build.run({"a": 1, "b": 2})
        await settle(agent.tasks)
        run = await agent.build.get(receipt.run_id)
        assert isinstance(run, CompletedRun) and run.result == 3
        assert await agent.other.get(receipt.run_id) is None  # another definition's run
        assert not await agent.other.cancel(receipt.run_id)
        assert (await agent.tasks.run("build", {"c": 4})).accepted

    asyncio.run(scenario())


def test_task_requires_async_functions() -> None:
    with pytest.raises(TypeError, match="async"):

        class Bad(Agent):
            @task  # ty: ignore[invalid-argument-type]
            def sync(self, input: None, step: TaskStep) -> None: ...


def test_a_step_result_that_is_not_json_fails_the_run() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        async def when(_attempt: TaskStepAttempt) -> datetime:
            return datetime.now(UTC)

        await step.do("when", when, retries=FAST)

    _, tasks, _ = setup({"h": handler})

    async def scenario() -> Any:
        receipt = await tasks.run("h")
        await settle(tasks)
        return await tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun) and run.error.name == "TaskSerializationError"


# Facets: the run lives on the facet, its wake on the root


def facet_setup(handler: Any) -> tuple[Lifecycle, Tasks, Lifecycle, Tasks]:
    root_tasks = Tasks()
    root = Lifecycle(Host("root")).use(root_tasks)
    facet_tasks = Tasks(definitions={"h": handler})
    facet = Lifecycle(Host("facet")).use(facet_tasks)
    root_transport = LocalTransport(None)
    root_transport.peers["child"] = facet
    facet_transport = LocalTransport(RouteAddress(key="child", data="child-data"))
    facet_transport.root = root
    root._set_route_transport(root_transport)
    facet._set_route_transport(facet_transport)
    return root, root_tasks, facet, facet_tasks


def test_a_facet_run_sleeps_on_the_root_alarm_and_runs_on_the_facet() -> None:
    calls: list[str] = []

    async def handler(_input: Any, step: TaskStep) -> str:
        async def before(_attempt: TaskStepAttempt) -> str:
            calls.append("before")
            return "a"

        a = await step.do("before", before)
        await step.sleep("pause", 0.03)
        return a + "b"

    root, root_tasks, facet, facet_tasks = facet_setup(handler)

    async def scenario() -> tuple[Any, Any]:
        receipt = await facet_tasks.run("h")
        await settle(facet_tasks)
        mirror = root._queue.get("tasks", f"task:child:{receipt.run_id}")
        assert mirror is not None
        assert isinstance(mirror.payload, dict)
        assert mirror.payload["owner_path"] == "child-data"
        assert facet._queue.list("tasks") == []  # the facet has no alarm of its own
        parked = await facet_tasks.get(receipt.run_id)
        await asyncio.sleep(0.05)
        await root.alarm()
        await settle(facet_tasks)
        assert root._queue.get("tasks", f"task:child:{receipt.run_id}") is None
        return parked, await facet_tasks.get(receipt.run_id)

    parked, done = asyncio.run(scenario())
    assert isinstance(parked, WaitingRun)
    assert isinstance(done, CompletedRun) and done.result == "ab"
    assert calls == ["before"]
    assert root_tasks._active == {}


def test_an_unreachable_facet_leaves_its_run_due() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        await step.sleep("pause", 0.01)

    root, _, _, facet_tasks = facet_setup(handler)

    async def scenario() -> Any:
        receipt = await facet_tasks.run("h")
        await settle(facet_tasks)
        root._route_transport.peers.clear()  # ty: ignore[unresolved-attribute]
        await asyncio.sleep(0.02)
        await root.alarm()
        return root._queue.get("tasks", f"task:child:{receipt.run_id}")

    assert asyncio.run(scenario()) is not None


def test_memory_limit_strikes_reach_the_facet_and_its_host() -> None:
    strikes: list[MemoryLimitContext] = []

    async def handler(_input: Any, step: TaskStep) -> None:
        await step.sleep("pause", 60)

    root, root_tasks, _, facet_tasks = facet_setup(handler)

    async def hook(context: MemoryLimitContext) -> None:
        strikes.append(context)

    facet_tasks._set_routed_memory_limit_handler(hook)

    async def scenario() -> Any:
        receipt = await facet_tasks.run("h")
        await settle(facet_tasks)
        mirror = root._queue.get("tasks", f"task:child:{receipt.run_id}")
        await root_tasks.on_memory_limit(
            MemoryLimitContext(sealed=True, executing=mirror)
        )
        return await facet_tasks.get(receipt.run_id)

    run = asyncio.run(scenario())
    assert isinstance(run, FailedRun) and run.error.name == "TaskMemoryLimitSealed"
    assert [s.sealed for s in strikes] == [True]


def test_deleting_a_facet_cancels_its_mirrors_on_the_root() -> None:
    async def handler(_input: Any, step: TaskStep) -> None:
        await step.sleep("pause", 60)

    root, root_tasks, _, facet_tasks = facet_setup(handler)

    async def scenario() -> None:
        await facet_tasks.run("h")
        await settle(facet_tasks)
        assert len(root._queue.list("tasks")) == 1
        await root_tasks._cleanup_route_prefix("child")
        assert root._queue.list("tasks") == []

    asyncio.run(scenario())
