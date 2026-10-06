"""``self.dynamic_agents``: an agent's sub-agents (facets).

Port of upstream ``dynamic-agents/api.ts`` (``.design/agent_api.md`` §1.15).
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .stubs import AgentStub
from .types import SubAgentInfo

if TYPE_CHECKING:
    from .dynamic_agents import SubAgentsEngine

__all__ = ("DynamicAgents",)


class DynamicAgents:
    """An agent's sub-agents: children with their own isolate and SQLite.

    A sub-agent runs on the same machine as its parent, which supervises
    it. The class must be exported from the Worker module under its own
    name; it needs no binding or migration.

    Parameters
    ----------
    engine
        The agent's sub-agents engine.
    """

    __slots__ = ("_engine",)

    def __init__(self, engine: "SubAgentsEngine") -> None:
        self._engine = engine

    async def get(self, cls: type, name: str) -> AgentStub:
        """Return sub-agent ``cls``/``name``, creating it on first use.

        Its ``on_start`` runs the first time. Returns a stub: ``await
        stub.method(...)``.

        Raises
        ------
        ValueError
            If the class isn't exported from the Worker, is named so it
            kebab-cases to ``sub``, or the name contains NUL.
        """
        return AgentStub(await self._engine.resolve(cls.__name__, name))

    def abort(self, cls: type, name: str, reason: Exception | None = None) -> None:
        """Stop a sub-agent now; pending calls get ``reason``.

        It restarts on the next `get`, with its storage. Its own sub-agents
        stop too. The default reason is a `SubAgentAbortedError`.
        """
        self._engine.abort(cls.__name__, name, reason)

    async def delete(self, cls: type, name: str) -> None:
        """Delete a sub-agent and its storage, and its own sub-agents.

        Its clients are disconnected, and its routed schedules, queue
        items, and task runs are cancelled. Deleting a sub-agent that
        doesn't exist does nothing.
        """
        await self._engine.delete(cls.__name__, name)

    def has(self, cls: type | str, name: str) -> bool:
        """Return whether this agent created sub-agent ``cls``/``name``."""
        return self._engine.registry.has(_class_name(cls), name)

    def list(self, cls: type | str | None = None) -> Sequence[SubAgentInfo]:
        """Return the sub-agents this agent created, oldest first."""
        return self._engine.registry.list(_class_name(cls) if cls is not None else None)


def _class_name(cls: type | str) -> str:
    return cls if isinstance(cls, str) else cls.__name__
