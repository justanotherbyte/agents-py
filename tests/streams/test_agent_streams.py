import asyncio
from typing import Any

from fake_runtime import FakeCtx

from agents import Agent, FiberContext, FiberRecoveryContext, Streams

# The producing fiber is left hanging on purpose (a dead isolate's).
HUNG: list[asyncio.Future[Any]] = []


class Reporter(Agent):
    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        self.streams = self.use(Streams())
        self.recovered: list[Any] = []

    async def produce(self) -> None:
        async def body(ctx: FiberContext) -> None:
            writer = await self.streams.open("report")
            for i in range(3):
                writer.append(i)
                ctx.stash({"cursor": writer.cursor})
            await asyncio.Event().wait()  # the isolate dies here

        await self.run_fiber("report", body)

    async def on_fiber_recovered(self, ctx: FiberRecoveryContext) -> None:
        # Streams (installed by this subclass) has started by now.
        status = await self.streams.status("report")
        writer = await self.streams.open("report")
        writer.append("resumed")
        writer.close()
        self.recovered.append((ctx.snapshot, status.cursor if status else None))


def test_a_recovery_hook_can_resume_a_stream_after_a_crash() -> None:
    agent = Reporter(FakeCtx("r"), env=None)

    async def scenario() -> list[Any]:
        await agent.lifecycle.start()
        HUNG.append(asyncio.ensure_future(agent.produce()))
        await asyncio.sleep(0.01)
        restarted = Reporter(FakeCtx("r", storage=agent.ctx.storage), env=None)
        await restarted.lifecycle.start()
        chunks = [c.chunk async for c in restarted.streams.read("report")]
        return [restarted.recovered, chunks]

    recovered, chunks = asyncio.run(scenario())
    assert recovered == [({"cursor": 3}, 3)]
    assert chunks == [0, 1, 2, "resumed"]
