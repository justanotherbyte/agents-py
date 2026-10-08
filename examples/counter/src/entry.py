from typing import TypedDict

from workers import Request, Response, WorkerEntrypoint

from agents import Agent, callable, route_agent_request


class CounterState(TypedDict):
    count: int


class Counter(Agent[CounterState]):
    initial_state = CounterState(count=0)

    @callable
    async def increment(self, by: int = 1) -> int:
        assert self.state is not None  # initial_state sets it
        count = self.state["count"] + by
        self.set_state(CounterState(count=count))
        return count


class Default(WorkerEntrypoint):
    async def fetch(self, request: Request) -> Response:
        response = await route_agent_request(request, self.env)
        return response or Response("Not found", status=404)
