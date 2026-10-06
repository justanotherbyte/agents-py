"""Structured events: the default sink and in-process subscriptions.

Port of upstream ``observability/`` (events only; tracing is out of scope).
The default sink writes each event as one JSON line to the
``agents.events`` logger at ``DEBUG``, and notifies `subscribe` listeners on
the event's channel (``.design/observability.md`` §5).
"""

import logging
from collections.abc import Callable

from ..core.encoding import to_json
from ..core.events import Disposable, Emitter
from .types import ObservabilityEvent

__all__ = ("LoggingObservability", "channel_for", "subscribe")

_log = logging.getLogger("agents.events")

_channels: dict[str, Emitter[ObservabilityEvent]] = {}

# (prefix, channel), checked in order; an exact name matches its prefix too.
_PREFIXES = (
    ("mcp:", "mcp"),
    ("workflow:", "workflow"),
    ("fiber:", "fiber"),
    ("task:", "task"),
    ("stream:", "stream"),
    ("transcript:", "transcript"),
    ("chat:transcript:", "transcript"),
    ("chat:", "chat"),
    ("agent_tool:", "agent_tool"),
    ("schedule:", "schedule"),
    ("queue:", "schedule"),
    ("message:", "message"),
    ("tool:", "message"),
    ("submission:", "message"),
    ("action:", "message"),
    ("rpc:", "rpc"),
    ("state:", "state"),
    ("email:", "email"),
    ("channel:", "channel"),
    ("notice:", "channel"),
)


def channel_for(event_type: str) -> str:
    """Return the channel an event type belongs to (upstream ``getChannel``).

    ``"queue:error"`` is on ``"schedule"``, ``"rpc"`` on ``"rpc"``; anything
    unmatched (``"connect"``, ``"destroy"``, ``"job:..."``) is ``"lifecycle"``.
    """
    if event_type == "rpc":
        return "rpc"
    for prefix, channel in _PREFIXES:
        if event_type.startswith(prefix):
            return channel
    return "lifecycle"


def subscribe(
    channel: str, listener: Callable[[ObservabilityEvent], object]
) -> Disposable:
    """Call ``listener`` with every event the default sink sees on ``channel``.

    In-process only, for tests and local debugging. Dispose the result to
    unsubscribe.
    """
    emitter = _channels.get(channel)
    if emitter is None:
        emitter = _channels[channel] = Emitter()
    return emitter.subscribe(listener)


class LoggingObservability:
    """The default sink: JSON lines on ``agents.events`` (``DEBUG``).

    Enable them with standard logging configuration, e.g.
    ``logging.getLogger("agents.events").setLevel(logging.DEBUG)``.
    """

    def emit(self, event: ObservabilityEvent) -> None:
        """Log ``event`` and notify the subscribers of its channel."""
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug(to_json(event))
        emitter = _channels.get(channel_for(event.type))
        if emitter is not None:
            emitter.fire(event)
