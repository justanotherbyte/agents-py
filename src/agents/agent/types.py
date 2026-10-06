"""Types for the Agent class."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, Literal

from typing_extensions import TypeVar

from ..core.types import Duration, RetryOptions
from ..dynamic_agents.types import AgentRoute

if TYPE_CHECKING:
    from workers import Request, Response

    from ..websockets.connection import Connection
    from .agent import Agent

__all__ = (
    "AgentOptions",
    "BeforeHook",
    "CurrentAgent",
    "RoutingRetry",
    "RoutingRetryEvent",
    "RoutingRetryOptions",
)

A = TypeVar("A", bound="Agent", default="Agent")


@dataclass(frozen=True, slots=True, kw_only=True)
class AgentOptions:
    """Per-class settings for an agent (``options = AgentOptions(...)``).

    Frozen, because one instance is shared by every agent of the class. A
    subclass's ``options`` replaces its parent's; extend them with
    ``dataclasses.replace(Parent.options, ...)``. Durations are a
    ``timedelta`` or a number of seconds.

    Parameters
    ----------
    send_identity_on_connect
        Send the ``cf_agent_identity`` frame (with the instance name) when a
        client connects.
    hung_schedule_timeout
        When a running interval schedule counts as hung and is reset.
    keep_alive_interval
        The ``keep_alive()`` heartbeat interval.
    retry
        Default retries for queued and scheduled callbacks.
    fiber_recovery_hook_timeout
        Timeout for framework fiber-recovery hooks.
    fiber_recovery_scan_deadline
        Soft deadline for one fiber-recovery scan.
    fiber_recovery_max_age
        When to give up on an interrupted fiber whose recovery keeps failing
        (``None`` keeps trying forever).
    max_alarm_memory_limit_strikes
        Alarm memory-limit resets tolerated before recovery work is sealed.
    expose_error_details
        Send an unhandled error's traceback to the client (local development
        only: tracebacks can reveal code and data).
    """

    send_identity_on_connect: bool = True
    hung_schedule_timeout: Duration = 30
    keep_alive_interval: Duration = 30
    retry: RetryOptions = field(default_factory=RetryOptions)
    fiber_recovery_hook_timeout: Duration = 10
    fiber_recovery_scan_deadline: Duration = 10
    fiber_recovery_max_age: Duration | None = timedelta(hours=24)
    max_alarm_memory_limit_strikes: int = 3
    expose_error_details: bool = False


@dataclass(slots=True, kw_only=True)
class CurrentAgent(Generic[A]):
    """The agent the running code belongs to, and what it's running for.

    Parameters
    ----------
    agent
        The agent.
    connection
        The client connection being served, if any.
    request
        The HTTP request (or WebSocket upgrade) being served, if any.
    """

    agent: A
    connection: "Connection | None" = None
    request: "Request | None" = None


type BeforeHook = Callable[
    ["Request", AgentRoute], Awaitable["Response | Request | None"]
]
"""Runs before a request is forwarded to its agent: return a ``Response`` to
answer it instead (e.g. 403), a ``Request`` to forward that instead, or
``None`` to forward it unchanged."""


@dataclass(slots=True, kw_only=True)
class RoutingRetryEvent:
    """Passed to ``RoutingRetryOptions.on_retry`` before each backoff."""

    error: Exception
    attempt: int
    max_attempts: int
    delay: timedelta
    name: str
    class_name: str | None
    """``None`` from ``get_agent_by_name``, as upstream."""


@dataclass(slots=True, kw_only=True)
class RoutingRetryOptions:
    """Retries for reaching an agent, on errors the platform marks transient.

    Parameters
    ----------
    max_attempts
        Total attempts, including the first.
    base_delay
        The backoff's base delay.
    max_delay
        The longest wait between attempts.
    on_retry
        Called before each backoff; a failure is logged and ignored.
    """

    max_attempts: int = 3
    base_delay: Duration = 0.1
    max_delay: Duration = 0.8
    on_retry: Callable[[RoutingRetryEvent], Awaitable[None]] | None = None


type RoutingRetry = RoutingRetryOptions | Literal[False] | None
"""Routing retry settings: ``None`` for the defaults, ``False`` to disable."""
