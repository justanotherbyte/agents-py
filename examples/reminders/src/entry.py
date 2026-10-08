from typing import TypedDict

from workers import Request, Response, WorkerEntrypoint

from agents import Agent, Schedule, callable, route_agent_request


class RemindersState(TypedDict):
    pending: list[str]
    done: list[str]


class Reminders(Agent[RemindersState]):
    initial_state = RemindersState(pending=[], done=[])

    @callable
    async def remind(self, text: str, seconds: float) -> None:
        await self.schedule(seconds, self.fire, text)
        state = self.state
        assert state is not None  # initial_state sets it
        self.set_state(
            RemindersState(pending=[*state["pending"], text], done=state["done"])
        )

    async def fire(self, text: str, schedule: Schedule) -> None:
        state = self.state
        assert state is not None
        pending = list(state["pending"])
        pending.remove(text)
        self.set_state(RemindersState(pending=pending, done=[*state["done"], text]))


class Default(WorkerEntrypoint):
    async def fetch(self, request: Request) -> Response:
        response = await route_agent_request(request, self.env)
        return response or Response("Not found", status=404)
