<h1 align="center">
<sub>
    <img src="https://raw.githubusercontent.com/justanotherbyte/agents-py/refs/heads/main/.github/agents.svg" height="36">
</sub>
&nbsp;
cf-agents
</h1>
<p align="center">
<sup>
Cloudflare Agents for Python Workers
</sup>
</p>


[![PyPI - Version](https://img.shields.io/pypi/v/cf-agents.svg)](https://pypi.org/project/cf-agents)
[![pypi](https://img.shields.io/pypi/pyversions/cf-agents.svg)](https://pypi.org/pypi/cf-agents)

> [!note]
> This is an unofficial SDK and not affiliated to Cloudflare. I worked on a port of the official [Agents SDK](https://agents.cloudflare.com) during my internship at Cloudflare. This project continues on my work there.
>
> This project is part of [Crosswind](https://crosswind.viswa.space).

# Features
- [x] Typed, `async`-first Python 3.12+
- [x] FFI Handling under the hood. Just write Python, we handle talking to the runtime.
- [x] Behavioural and API parity with the TypeScript [Agents SDK](https://github.com/cloudflare/agents).

# Installing

It's recommended you use `uv` - you'll need to use [pywrangler](https://developers.cloudflare.com/workers/languages/python/) to deploy to Cloudflare.

```shell
$ uvx --from workers-py pywrangler init
$ uv add cf-agents
```

# Quickstart

The `Agent` class gives you the core tools to build stateful, durable agents.

```python
from typing import TypedDict

from agents import Agent, Schedule, callable, route_agent_request
from workers import WorkerEntrypoint, Request, Response

from utils import search_menus_by_agent, choose_winners

MODEL = "@cf/zai-org/glm-4.7-flash"
SYSTEM_PROMPT = """
You help deliver results to a bunch of co-workers who are choosing lunch together.
The user is going to provide you with the options.
Your task is to make the choice sound exciting so people who voted for something
else feel validated.
"""


class Restaurant(TypedDict):
    cuisine: str
    name: str
    address: str


class Vote(TypedDict):
    username: str
    restaurant_name: str


class LunchState(TypedDict):
    office_address: str
    todays_votes: list[Vote]
    todays_ruling: str | None
    restaurants: list[Restaurant]


class LunchAgent(Agent[LunchState]):
    initial_state = LunchState(
        office_address="County Hall, London",
        todays_votes=[],
        todays_ruling=None,
        restaurants=[],
    )

    async def on_start(self) -> None:
        # Cron expressions, evaluated in UTC
        await self.schedule("30 23 * * 1-5", self.choose_lunch)
        await self.schedule("0 17 * * *", self.reset_lunch)

    @callable
    async def nominate_restaurant(self, restaurant_name: str) -> None:
        # A Workflow researches the restaurant, stores its menu in
        # Vectorize, then updates this agent's list of restaurants
        await self.env.RESTAURANT_RESEARCHER_WORKFLOW.create(
            {
                "params": {
                    "restaurantName": restaurant_name,
                    "agent": self.name,
                    "near": self.state["office_address"],
                }
            }
        )

    @callable
    async def search_restaurants(self, query: str) -> list[str]:
        # Vector search, filtered by metadata to this agent's menus
        results = await search_menus_by_agent(query, self.name)
        return [result["metadata"]["restaurantName"] for result in results]

    @callable
    async def vote(self, username: str, restaurant_name: str) -> None:
        state = self.state
        vote = Vote(username=username, restaurant_name=restaurant_name)
        votes = [*state["todays_votes"], vote]
        # Broadcasts the new state to every connected eater
        self.set_state({**state, "todays_votes": votes})

    async def reset_lunch(self, payload: None, schedule: Schedule) -> None:
        self.set_state({**self.state, "todays_votes": [], "todays_ruling": None})

    async def choose_lunch(self, payload: None, schedule: Schedule) -> None:
        winners = choose_winners(self.state["todays_votes"])
        result = await self.env.AI.run(
            MODEL,
            {
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": ", ".join(winners)},
                ]
            },
        )
        self.set_state({**self.state, "todays_ruling": result.response})


class Default(WorkerEntrypoint):
    async def fetch(self, request: Request) -> Response:
        response = await route_agent_request(request, self.env)
        return response or Response("Not Found", status=404)
```

It can get tedious implementing similar patterns for chat agents over and over again. See [AIChatAgent](https://crosswind.viswa.space/docs/agents/chat).

# Goals

`cf-agents` is a solo project right now. The scope is restricted, but feature-ful. Planned:

- [ ] MCP
- [ ] Channels
- [ ] Integration with [Agent Traces](https://developers.cloudflare.com/agents/runtime/operations/observability/tracing/)
- [ ] A harness/opinionated framework, similar to [Think](https://developers.cloudflare.com/agents/harnesses/think/)
- [ ] Integrations with popular Agent libraries in the Python eco-system (such as [LangChain](https://www.langchain.com/langchain) or [CrewAI](https://crewai.com/))

Some of these require the Cloudflare Python Workers runtime to improve. It's very easy right now to end up on the paid plan using Python Workers.


# Links

- [Documentation](https://crosswind.viswa.space/docs/agents/getting-started)
- [Crosswind](https://crosswind.viswa.space)
