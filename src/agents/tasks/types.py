"""Types for the Tasks capability (``.design/tasks_engine.md``)."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Literal, Protocol, TypedDict

from ..core.types import Duration, JSONValue
from ..lifecycle.types import MemoryLimitContext

__all__ = (
    "Backoff",
    "CancelledRun",
    "CompletedRun",
    "DispatchMessage",
    "FailedRun",
    "MemoryLimitHandler",
    "MemoryLimitMessage",
    "PendingRun",
    "ResolvedStepPolicy",
    "RunningRun",
    "StepInterruption",
    "StepRetries",
    "SyncWakeMessage",
    "TaskError",
    "TaskErrorHandler",
    "TaskHandler",
    "TaskReceipt",
    "TaskRouteMessage",
    "TaskRun",
    "TaskRunRow",
    "TaskRunState",
    "TaskStep",
    "TaskStepAttempt",
    "TaskStepRow",
    "TaskWaitReason",
    "TaskWakePayload",
    "TerminalTaskState",
    "WaitingRun",
)

type TaskRunState = Literal[
    "pending", "running", "waiting", "completed", "failed", "cancelled"
]
"""``pending`` (accepted), ``running`` (an attempt is executing), ``waiting``
(parked on a sleep or retry deadline), then ``completed``, ``failed``, or
``cancelled``."""

type TerminalTaskState = Literal["completed", "failed", "cancelled"]
"""The states a run ends in."""

type TaskWaitReason = Literal["sleep", "retry"]
"""Why a waiting run is waiting."""

type Backoff = Literal["constant", "linear", "exponential"]
"""How a step's retry delay grows."""


@dataclass(slots=True, kw_only=True)
class StepRetries:
    """Retry policy for a step; unset fields use the Tasks defaults.

    Parameters
    ----------
    limit
        Total attempts, including the first (default 5).
    delay
        The delay before the first retry (default 1 s).
    backoff
        How the delay grows across retries (default exponential, capped at a
        day).
    """

    limit: int | None = None
    delay: Duration | None = None
    backoff: Backoff | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ResolvedStepPolicy:
    """A step's retry and timeout policy with every field resolved."""

    retry_limit: int
    retry_delay_ms: int
    backoff: Backoff
    timeout_ms: int


@dataclass(slots=True, kw_only=True)
class TaskStepAttempt:
    """What a ``step.do`` function receives on each attempt.

    Parameters
    ----------
    attempt
        The 1-based attempt number for this step.
    idempotency_key
        The same across every attempt and replay of this step: pass it to
        external services that deduplicate.
    """

    attempt: int
    idempotency_key: str


@dataclass(slots=True, kw_only=True, frozen=True)
class StepInterruption:
    """The step an interrupted attempt (a dead isolate) left mid-execution."""

    name: str
    attempt: int


class TaskStep(Protocol):
    """The step API a task handler receives.

    Named steps are the run's durable journal: ``do`` memoizes completed
    results, sleeps keep their first deadline, and both end the attempt
    instead of holding the invocation open. The handler replays from its
    first line on every attempt, so side effects belong inside ``do``.
    """

    @property
    def interrupted(self) -> StepInterruption | None:
        """The step a dead isolate left running, or ``None`` on a clean attempt."""
        ...

    async def do[T](
        self,
        name: str,
        fn: Callable[[TaskStepAttempt], Awaitable[T]],
        *,
        retries: StepRetries | None = None,
        timeout: Duration | None = None,
    ) -> T:
        """Run ``fn`` once as step ``name``; replays return its journaled result."""
        ...

    async def sleep(self, name: str, duration: Duration) -> None:
        """Sleep durably for ``duration``; the first recorded deadline wins."""
        ...

    async def sleep_until(self, name: str, when: datetime) -> None:
        """Sleep durably until ``when``."""
        ...

    async def status(self, message: str) -> None:
        """Record observable progress (replays stay silent over old ground)."""
        ...

    def idempotency_key(self, name: str) -> str:
        """Return the key ``do(name, ...)`` passes to its attempts."""
        ...


type TaskHandler = Callable[[Any, TaskStep], Awaitable[Any]]
"""A task definition, called as ``handler(input, step)``."""

type TaskErrorHandler = Callable[[Exception], Awaitable[None]]
"""Observes a run's terminal failure."""


@dataclass(slots=True, kw_only=True)
class TaskError:
    """What a failed run keeps of its error: the type name and message."""

    name: str
    message: str


@dataclass(slots=True, kw_only=True)
class TaskReceipt:
    """The result of starting a run.

    ``accepted`` is ``False`` when an existing run matched the ``run_id`` or
    ``idempotency_key`` (not an error).
    """

    run_id: str
    definition: str
    accepted: bool
    state: TaskRunState
    created_at: datetime


@dataclass(slots=True, kw_only=True)
class PendingRun:
    """A run accepted but not started yet."""

    state: ClassVar[Literal["pending"]] = "pending"
    run_id: str
    definition: str
    created_at: datetime
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True)
class RunningRun:
    """A run with an attempt executing."""

    state: ClassVar[Literal["running"]] = "running"
    run_id: str
    definition: str
    created_at: datetime
    attempt: int
    started_at: datetime
    status_message: str | None = None
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True)
class WaitingRun:
    """A run parked until a sleep or retry deadline."""

    state: ClassVar[Literal["waiting"]] = "waiting"
    run_id: str
    definition: str
    created_at: datetime
    reason: TaskWaitReason
    wake_at: datetime
    status_message: str | None = None
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True)
class CompletedRun[Out]:
    """A run that finished with ``result``."""

    state: ClassVar[Literal["completed"]] = "completed"
    run_id: str
    definition: str
    created_at: datetime
    result: Out
    settled_at: datetime
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True)
class FailedRun:
    """A run that failed."""

    state: ClassVar[Literal["failed"]] = "failed"
    run_id: str
    definition: str
    created_at: datetime
    error: TaskError
    settled_at: datetime
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True)
class CancelledRun:
    """A run that was cancelled."""

    state: ClassVar[Literal["cancelled"]] = "cancelled"
    run_id: str
    definition: str
    created_at: datetime
    settled_at: datetime
    reason: str | None = None
    metadata: dict[str, JSONValue] | None = None


type TaskRun[Out] = (
    PendingRun | RunningRun | WaitingRun | CompletedRun[Out] | FailedRun | CancelledRun
)
"""A snapshot of one run; narrow it with ``match`` or ``isinstance``."""


class TaskRunRow(TypedDict):
    """A raw ``cf_agents_task_runs`` row (``.design/sql_schemas.md`` §5)."""

    run_id: str
    definition: str
    input: str | None
    state: TaskRunState
    result: str | None
    error_name: str | None
    error_message: str | None
    status_message: str | None
    metadata: str | None
    idempotency_key: str | None
    retain: int
    attempt: int
    generation: str | None
    next_at: int | None
    wait_reason: TaskWaitReason | None
    cancel_requested: int
    cancel_reason: str | None
    created_at: int
    started_at: int | None
    updated_at: int
    settled_at: int | None


class TaskStepRow(TypedDict):
    """A raw ``cf_agents_task_steps`` row."""

    run_id: str
    step_name: str
    kind: Literal["do", "sleep"]
    state: Literal["running", "waiting", "completed", "failed"]
    result: str | None
    error_name: str | None
    error_message: str | None
    attempt: int
    next_at: int | None
    created_at: int
    started_at: int | None
    updated_at: int
    completed_at: int | None


class TaskWakePayload(TypedDict):
    """What a run's wake job carries.

    ``owner_path`` / ``owner_path_key`` are set on the root's mirror of a
    facet's run: the run and its journal live on that facet.
    """

    run_id: str
    owner_path: str | None
    owner_path_key: str | None


class SyncWakeMessage(TypedDict):
    """A facet mirroring a run's next wake (epoch ms; ``None`` cancels it)."""

    type: Literal["sync_wake"]
    run_id: str
    next_ms: int | None


class DispatchMessage(TypedDict):
    """The root asking a facet to run one due run; it answers with the next wake."""

    type: Literal["dispatch"]
    run_id: str


class MemoryLimitMessage(TypedDict):
    """The root forwarding a memory-limit strike on a facet run's mirror job."""

    type: Literal["memory_limit"]
    run_id: str
    sealed: bool
    next_ms: int | None


type TaskRouteMessage = SyncWakeMessage | DispatchMessage | MemoryLimitMessage
"""A Tasks message routed between a facet and the root."""

type MemoryLimitHandler = Callable[[MemoryLimitContext], Awaitable[None]]
"""The host's memory-limit hook, for strikes on its runs that the root saw."""
