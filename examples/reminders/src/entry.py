from workers import Response, WorkerEntrypoint

from agents import Agent, callable, route_agent_request


class Reminders(Agent):
    initial_state = {"pending": [], "done": []}

    @callable
    async def remind(self, text, seconds):
        await self.schedule(seconds, self.fire, text)
        self.set_state({**self.state, "pending": [*self.state["pending"], text]})

    async def fire(self, text, schedule):
        pending = list(self.state["pending"])
        pending.remove(text)
        self.set_state({"pending": pending, "done": [*self.state["done"], text]})


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        response = await route_agent_request(request, self.env)
        return response or Response("Not found", status=404)
