"""Types for the Lifecycle and the capabilities installed into it."""

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Protocol, TypedDict

from ..core.types import JSONValue, RetryOptions

if TYPE_CHECKING:
    from workers import Request

__all__ = (
    "AlarmScope",
    "Connection",
    "DriverStorage",
    "EventSink",
    "HostContext",
    "HostInvoker",
    "JobContext",
    "JobDispatch",
    "JobOutcome",
    "JobRow",
    "LifecycleEvent",
    "LifecycleJob",
    "LifecycleStatus",
    "MemoryLimitContext",
    "MemoryLimitStrike",
    "Reschedule",
    "RouteAddress",
    "RouteContext",
    "RouteEnvelope",
    "RouteTransport",
    "WebSocket",
)

type WebSocket = Any
"""The runtime's (JS) WebSocket object, as the hibernation API delivers it."""

type LifecycleStatus = Literal["zero", "starting", "started"]
"""``"zero"`` before startup, ``"starting"`` while it runs, ``"started"`` after."""


class Connection(Protocol):
    """The connection vocabulary Lifecycle refers to.

    The WebSockets capability implements it; Lifecycle only passes
    connections through (upstream ``lifecycle/types.ts``).
    """

    @property
    def id(self) -> str:
        """The connection id."""
        ...


@dataclass(slots=True, kw_only=True)
class LifecycleJob:
    """One durable job in the Lifecycle queue.

    Parameters
    ----------
    id
        Unique job id, stable across reschedules.
    capability
        The owning capability's id, or ``"host"``.
    fn
        The name the owner dispatches on (e.g. a callback name).
    time
        When the job is due.
    payload
        Owner-defined JSON payload.
    retry
        Dispatch retry policy, when the pusher gave one.
    singleflight
        Whether the job is skipped while a previous run is in flight.
    exclusive
        Whether the job suppresses ordinary alarm candidates while pending.
    recovery_loop
        Whether the alarm memory-limit circuit breaker governs this job.
    created_at
        When the job was first pushed.
    """

    id: str
    capability: str
    fn: str
    time: datetime
    payload: JSONValue = None
    retry: RetryOptions | None = None
    singleflight: bool = False
    exclusive: bool = False
    recovery_loop: bool = False
    created_at: datetime


class JobRow(TypedDict):
    """A raw ``cf_agents_jobs`` row (``.design/sql_schemas.md`` §2)."""

    id: str
    capability: str
    fn: str
    time: int
    payload: str | None
    retry_options: str | None
    singleflight: int
    hung_timeout_seconds: int | None
    exclusive: int
    recovery_loop: int
    running: int
    execution_started_at: int | None
    created_at: int


@dataclass(slots=True, kw_only=True)
class Reschedule:
    """A job outcome: run the job again at ``at``."""

    at: datetime


type JobOutcome = Reschedule | Literal["yield"] | None
"""What an owner returns after running a job.

``None`` completes (deletes) it, `Reschedule` suspends it until a later time,
and ``"yield"`` leaves it due so it runs again on the next wake. A same-id
push or reschedule made during dispatch wins over the returned outcome.
"""


@dataclass(slots=True, kw_only=True)
class JobContext:
    """The due job being dispatched, and the attempt number within this alarm."""

    job: LifecycleJob
    attempt: int


@dataclass(slots=True, kw_only=True)
class MemoryLimitContext:
    """A recorded alarm memory-limit strike (upstream #1825).

    Parameters
    ----------
    sealed
        Whether the strike budget ran out and recovery-loop work was purged.
    next_time
        The backoff wake armed for an unsealed strike.
    executing
        The job that was running when the strike landed, if any.
    purged_recovery_loop_jobs
        The recovery-loop jobs a sealing strike removed (snapshotted first).
    """

    sealed: bool
    next_time: datetime | None = None
    executing: LifecycleJob | None = None
    purged_recovery_loop_jobs: Sequence[LifecycleJob] | None = None


class RouteAddress(NamedTuple):
    """An opaque address a routing transport understands."""

    key: str
    """Stable equality and storage key."""
    data: str
    """The transport's serialized address."""


@dataclass(slots=True, kw_only=True)
class RouteContext:
    """A message routed to one capability from another Lifecycle."""

    source: RouteAddress | None
    payload: Any


@dataclass(slots=True, kw_only=True)
class RouteEnvelope:
    """A routed message between Lifecycles, addressed to a capability id."""

    capability: str
    source: RouteAddress | None
    payload: Any


@dataclass(slots=True, kw_only=True)
class LifecycleEvent:
    """One best-effort telemetry event published through Lifecycle."""

    source: str
    type: str
    payload: Any


type EventSink = Callable[[LifecycleEvent], Awaitable[None] | None]
"""Receives every Lifecycle event once startup has finished."""


@dataclass(slots=True, kw_only=True, frozen=True)
class HostContext:
    """The host an invocation runs for, and what it runs on behalf of.

    Frozen: one value is shared by every task that inherits the context.
    """

    host: object
    connection: Connection | None = None
    request: "Request | None" = None


class HostInvoker(Protocol):
    """Wraps user callbacks run through ``run_in_host_context`` (e.g. ``Agent``)."""

    async def __call__[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        connection: Connection | None,
        request: "Request | None",
    ) -> T:
        """Run ``fn`` inside the host's invocation boundary."""
        ...


class RouteTransport(Protocol):
    """Carries routed messages between Lifecycles (e.g. a facet and its root)."""

    @property
    def source(self) -> RouteAddress | None:
        """Return this Lifecycle's address, or ``None`` at the root."""
        ...

    async def to_root(self, envelope: RouteEnvelope) -> Any:
        """Deliver ``envelope`` to the root Lifecycle."""
        ...

    async def to(self, target: RouteAddress, envelope: RouteEnvelope) -> Any:
        """Deliver ``envelope`` to the Lifecycle at ``target``."""
        ...


@dataclass(slots=True, kw_only=True)
class JobDispatch:
    """The hooks one job owner exposes to the job driver."""

    on_job: Callable[[JobContext], Awaitable[JobOutcome]]
    on_job_error: Callable[[JobContext, Exception], Awaitable[JobOutcome]] | None = None


class DriverStorage(Protocol):
    """The slice of ``ctx.storage`` (the Workers SDK wrapper) the job driver uses."""

    async def get(self, key: str) -> Any:
        """Read a KV value."""
        ...

    async def put(self, key: str, value: Any) -> None:
        """Write a KV value."""
        ...

    async def delete(self, key: str) -> Any:
        """Delete a KV value."""
        ...

    async def setAlarm(self, time: int) -> None:  # noqa: N802  (JS name)
        """Set the physical alarm (epoch ms)."""
        ...

    async def sync(self) -> None:
        """Wait until pending writes are durable."""
        ...


@dataclass(slots=True, frozen=True)
class AlarmScope:
    """The job an async flow belongs to while an alarm drives it."""

    executing: JobRow | None


@dataclass(slots=True, kw_only=True, frozen=True)
class MemoryLimitStrike:
    """One recorded memory-limit strike, shared by every flow that saw it."""

    strikes: int
    limit: int
    sealed: bool
    next_time_ms: int | None
    purged_recovery_loop_jobs: Sequence[LifecycleJob] | None
