from workers import Response, WorkerEntrypoint

from agents import Agent, callable, route_agent_request


class Counter(Agent):
    initial_state = {"count": 0}

    @callable
    async def increment(self, by=1):
        self.set_state({"count": self.state["count"] + by})
        return self.state["count"]


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        response = await route_agent_request(request, self.env)
        return response or Response("Not found", status=404)
