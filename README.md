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
from agents import Agent, callable, route_agent_request
from workers import WorkerEntrypoint, Response

from utils import google_search

class LunchAgent(Agent):
    initial_state = {"votes": {}}

    @callable
    async def search_restaurants(self) -> list[str]:
        restaurants: list[str] = await google_search(
            "Korean BBQ places near County Hall"
        )
        return restaurants

    @callable
    async def vote(self, person: str, restaurant: str):
        votes = self.state["votes"]
        votes[person] = restaurant
        self.set_state(self.state)


class Default(WorkerEntrypoint):
    async def fetch(self, request) -> Response:
        response = await route_agent_request(request, self.env)
        return response or Response("Not Found", status=404)
```

It can get tedious implementing similar patterns for chat agents over and over again. See [AIChatAgent](https://crosswind.viswa.space/docs/agents/chat).

# Links

- [Documentation](https://crosswind.viswa.space/docs/agents/getting-started)
- [Crosswind](https://crosswind.viswa.space)
