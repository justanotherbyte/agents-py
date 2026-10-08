import json
from collections.abc import AsyncIterator

from workers import Request, Response, WorkerEntrypoint
from agents import AIChatAgent, ChatMessageOptions, TextPart, route_agent_request

MODEL = "@cf/meta/llama-3.3-70b-instruct-fp8-fast"


class Chat(AIChatAgent):
    async def on_chat_message(self, options: ChatMessageOptions) -> AsyncIterator[str]:
        messages = [
            {
                "role": m.role,
                "content": "".join(p.text for p in m.parts if isinstance(p, TextPart)),
            }
            for m in self.messages
        ]
        stream = await self.env.AI.run(MODEL, {"messages": messages, "stream": True})

        # Workers AI streams server-sent events: `data: {"response": "..."}` lines.
        buffer = b""
        async for chunk in stream:
            *lines, buffer = (buffer + chunk.to_bytes()).split(b"\n")
            for line in lines:
                if line.startswith(b"data: ") and line != b"data: [DONE]":
                    yield json.loads(line[6:]).get("response") or ""


class Default(WorkerEntrypoint):
    async def fetch(self, request: Request) -> Response:
        response = await route_agent_request(request, self.env)
        return response or Response("Not found", status=404)
