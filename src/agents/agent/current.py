"""``get_current_agent()``: the agent the running code belongs to.

Port of upstream ``getCurrentAgent`` (``lifecycle/current-agent.ts``), read
from the Lifecycle host context (``.design/agent_api.md`` §1.16).
"""

from typing import TYPE_CHECKING, Any, cast

from ..lifecycle.host_context import current_host_context
from .agent import Agent
from .types import CurrentAgent

if TYPE_CHECKING:
    from ..websockets.connection import Connection

__all__ = ("get_current_agent",)


def get_current_agent() -> CurrentAgent[Agent[Any]] | None:
    """Return the agent the running code belongs to, or ``None`` outside one.

    Set around every hook, ``@callable`` method, queued callback, and public
    agent method (including ones reached through native RPC), and inherited
    by tasks they start. Lost in callbacks the JS runtime calls directly
    (``setTimeout``, event listeners): call an agent method from there.
    """
    current = current_host_context()
    if current is None or not isinstance(current.host, Agent):
        return None
    return CurrentAgent(
        agent=current.host,
        connection=cast("Connection | None", current.connection),
        request=current.request,
    )
