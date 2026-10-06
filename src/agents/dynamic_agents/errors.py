"""Exceptions for sub-agents."""

from ..core.errors import AgentsException

__all__ = ("SubAgentAbortedError",)


class SubAgentAbortedError(AgentsException):
    """A sub-agent was aborted (``dynamic_agents.abort``) while a call was pending.

    The default reason ``abort`` raises in pending callers.
    """
