import asyncio
from typing import Any

from fake_runtime import FakeWorld

from agents import Agent, AgentOptions, FiberContext, FiberRecoveryContext

# Fibers left hanging on purpose (a dead isolate's), kept referenced.
HUNG: list[asyncio.Future[Any]] = []


class Root(Agent):
    options = AgentOptions(keep_alive_interval=0.05)


class Child(Agent):
    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.release = asyncio.Event()
        self.seen: dict[str, Any] = {}
        self.recovered: list[str] = []

    async def on_fiber_recovered(self, ctx: FiberRecoveryContext) -> None:
        self.recovered.append(ctx.name)

    async def hold(self) -> None:
        self.lease = await self.keep_alive()  # never released

    async def open_child(self) -> None:
        grandchild = await self.dynamic_agents.get(Child, "g")
        await grandchild.hold()

    async def work(self, root: Root) -> None:
        async def body(ctx: FiberContext) -> None:
            self.seen["index"] = list(
                root.sql("SELECT run_id FROM cf_agents_facet_runs")
            )
            self.seen["lease"] = root._keep_alive.active
            self.seen["heartbeat"] = root._keep_alive.lifecycle.jobs.get("keep-alive")
            self.seen["own_heartbeat"] = self._keep_alive.lifecycle.jobs.get(
                "keep-alive"
            )
            ctx.stash({"at": "middle"})
            await self.release.wait()

        await self.run_fiber("job", body)


def world() -> tuple[FakeWorld, Root]:
    w = FakeWorld()
    w.export(Root)
    w.export(Child, bound=False)
    return w, w.instance(Root, "r")


def child_of(root: Root) -> Child:
    return root.ctx.facets.live["Child\0c"]


def test_a_facet_fiber_is_indexed_and_kept_alive_on_the_root() -> None:
    _, root = world()

    async def scenario() -> Child:
        await root.lifecycle.start()
        await root.dynamic_agents.get(Child, "c")
        child = child_of(root)
        running = asyncio.ensure_future(child.work(root))
        await asyncio.sleep(0.01)
        child.release.set()
        await running
        await asyncio.sleep(0.01)
        return child

    child = asyncio.run(scenario())
    assert len(child.seen["index"]) == 1
    assert child.seen["lease"] and child.seen["heartbeat"] is not None
    assert child.seen["own_heartbeat"] is None  # the facet has no alarm
    assert list(root.sql("SELECT * FROM cf_agents_facet_runs")) == []
    assert not root._keep_alive.active


def test_root_housekeeping_recovers_an_idle_facets_fiber() -> None:
    _, root = world()

    async def scenario() -> Child:
        await root.lifecycle.start()
        await root.dynamic_agents.get(Child, "c")
        HUNG.append(asyncio.ensure_future(child_of(root).work(root)))
        await asyncio.sleep(0.01)
        # The facet's isolate dies; its storage and the root's index remain.
        root.dynamic_agents.abort(Child, "c")
        assert root._fibers.lifecycle.jobs.get("housekeeping") is not None
        await asyncio.sleep(0.06)
        await root.lifecycle.alarm()
        return child_of(root)

    revived = asyncio.run(scenario())
    assert revived.recovered == ["job"]
    assert list(root.sql("SELECT * FROM cf_agents_facet_runs")) == []
    assert root._fibers.lifecycle.jobs.get("housekeeping") is None


def test_deleting_a_facet_drops_its_index_entries() -> None:
    _, root = world()

    async def scenario() -> None:
        await root.lifecycle.start()
        await root.dynamic_agents.get(Child, "c")
        HUNG.append(asyncio.ensure_future(child_of(root).work(root)))
        await asyncio.sleep(0.01)
        await root.dynamic_agents.delete(Child, "c")

    asyncio.run(scenario())
    assert list(root.sql("SELECT * FROM cf_agents_facet_runs")) == []
    assert root._fibers.lifecycle.jobs.get("housekeeping") is None


def test_deleting_a_facet_drops_the_leases_it_held_on_the_root() -> None:
    _, root = world()

    async def scenario() -> list[Any]:
        await root.lifecycle.start()
        await root.dynamic_agents.get(Child, "c")
        await root.dynamic_agents.get(Child, "other")
        child = child_of(root)
        await child.hold()
        await child.open_child()  # a grandchild's lease, under the same subtree
        await root.ctx.facets.live["Child\0other"].hold()
        jobs = root._keep_alive.lifecycle.jobs
        held = root._keep_alive._refs
        await root.dynamic_agents.delete(Child, "c")
        after_one = (root._keep_alive._refs, jobs.get("keep-alive") is not None)
        await root.dynamic_agents.delete(Child, "other")
        await asyncio.sleep(0)
        return [held, after_one, root._keep_alive.active, jobs.get("keep-alive")]

    held, after_one, active, heartbeat = asyncio.run(scenario())
    assert held == 3
    assert after_one == (1, True)  # "other" still holds its lease
    assert not active and heartbeat is None


def test_a_late_release_from_a_deleted_facet_is_ignored() -> None:
    _, root = world()

    async def scenario() -> int:
        await root.lifecycle.start()
        await root.dynamic_agents.get(Child, "c")
        child = child_of(root)
        await child.hold()
        await root.dynamic_agents.delete(Child, "c")
        await root.keep_alive()  # the root's own lease
        child.lease.dispose()  # the deleted facet's release arrives late
        await asyncio.sleep(0.01)
        return root._keep_alive._refs

    assert asyncio.run(scenario()) == 1


def test_a_restarted_facet_drops_its_dead_isolates_leases() -> None:
    _, root = world()

    async def scenario() -> list[Any]:
        await root.lifecycle.start()
        await root.dynamic_agents.get(Child, "c")
        await root.dynamic_agents.get(Child, "other")
        await child_of(root).hold()
        await child_of(root).open_child()  # the grandchild's own lease
        await root.ctx.facets.live["Child\0other"].hold()
        root.dynamic_agents.abort(Child, "c")  # its isolate dies, lease held
        still_held = root._keep_alive._refs
        await root.dynamic_agents.get(Child, "c")  # a new isolate starts
        return [still_held, root._keep_alive._refs]

    still_held, after_restart = asyncio.run(scenario())
    # Until it wakes, the dead isolate's lease stays; then only it is dropped:
    # the sibling's and the grandchild's (another isolate) remain.
    assert (still_held, after_restart) == (3, 2)


def test_a_late_restart_notice_keeps_the_new_isolates_leases() -> None:
    _, root = world()

    async def scenario() -> int:
        await root.lifecycle.start()
        await root.dynamic_agents.get(Child, "c")
        child = child_of(root)
        await child.hold()
        # The same isolate's notice again (e.g. arriving after its lease).
        await child._keep_alive.on_start()
        return root._keep_alive._refs

    assert asyncio.run(scenario()) == 1
