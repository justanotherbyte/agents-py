"""Types for observability events."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

__all__ = ("Observability", "ObservabilityEvent")


@dataclass(slots=True, kw_only=True)
class ObservabilityEvent:
    """One structured event: what an agent did, with upstream's schema.

    Parameters
    ----------
    type
        Upstream's event name, e.g. ``"rpc"`` or ``"queue:error"``.
    agent
        The agent's class name.
    name
        The agent's instance name.
    payload
        The event's data, with upstream's (camelCase) keys.
    timestamp
        When the event happened (UTC); epoch milliseconds in JSON.
    """

    type: str
    agent: str
    name: str
    payload: dict[str, Any]
    timestamp: datetime


class Observability(Protocol):
    """Where an agent's events go (``Agent.observability``)."""

    def emit(self, event: ObservabilityEvent) -> None:
        """Record one event. Shouldn't raise; failures are logged and dropped."""
        ...
