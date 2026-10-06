"""Types for the Scheduler capability."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal, TypedDict

from ..core.types import JSONValue, RetryOptions

__all__ = (
    "ByIdMessage",
    "CronField",
    "DispatchMessage",
    "InsertMessage",
    "InsertResult",
    "ListMessage",
    "Schedule",
    "ScheduleCallback",
    "ScheduleErrorHandler",
    "ScheduleJobPayload",
    "ScheduleRouteMessage",
    "ScheduleTiming",
    "ScheduleType",
    "WireSchedule",
)

type ScheduleType = Literal["scheduled", "delayed", "cron", "interval"]
"""``"scheduled"`` runs once at a time, ``"delayed"`` once after a delay,
``"cron"`` on a cron expression, ``"interval"`` at a fixed interval."""


@dataclass(slots=True, kw_only=True)
class Schedule:
    """One persisted schedule.

    Parameters
    ----------
    id
        The schedule's id.
    callback
        The name of the callback it runs.
    payload
        The data passed to the callback.
    type
        How it runs (`ScheduleType`).
    time
        When it next runs.
    retry
        The retry policy given when it was created, if any.
    delay
        For ``"delayed"``: the delay it was created with.
    cron
        For ``"cron"``: the cron expression.
    interval
        For ``"interval"``: the time between runs.
    """

    id: str
    callback: str
    payload: JSONValue
    type: ScheduleType
    time: datetime
    retry: RetryOptions | None = None
    delay: timedelta | None = None
    cron: str | None = None
    interval: timedelta | None = None


type ScheduleCallback = Callable[[Any, Schedule], Awaitable[Any]]
"""A schedule callback, called as ``callback(payload, schedule)``."""

type ScheduleErrorHandler = Callable[[Exception], Awaitable[None]]
"""Observes a schedule's terminal failure (after its last attempt)."""


@dataclass(slots=True, kw_only=True, frozen=True)
class ScheduleTiming:
    """When and how a schedule runs: a `Schedule` minus identity and payload."""

    type: ScheduleType
    time_ms: int
    delay_seconds: float | None = None
    cron: str | None = None
    interval_seconds: float | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class CronField:
    """One cron field's allowed range and names."""

    minimum: int
    maximum: int
    aliases: Mapping[str, str] = field(default_factory=dict)


class ScheduleJobPayload(TypedDict):
    """What a schedule job carries besides its callback name (the job's ``fn``).

    ``retry`` is the caller's override as given (JSON text), for reading the
    schedule back; the job's own retry policy is resolved against the
    Scheduler's default. ``owner_path`` / ``owner_path_key`` are a facet's
    route address (``None`` on the root).
    """

    payload: JSONValue
    type: ScheduleType
    retry: str | None
    delay_seconds: float | None
    cron: str | None
    interval_seconds: float | None
    owner_path: str | None
    owner_path_key: str | None


class WireSchedule(TypedDict):
    """A `Schedule` as JSON, carried in routed messages."""

    id: str
    callback: str
    time_ms: int
    job: ScheduleJobPayload


class InsertResult(TypedDict):
    """A routed insert's result: the schedule, and whether it's new."""

    schedule: WireSchedule
    created: bool


class InsertMessage(TypedDict):
    """A facet creating a schedule on the root."""

    type: Literal["insert"]
    timing: dict[str, Any]
    callback: str
    payload: JSONValue
    retry: str | None
    idempotent: bool | None


class ByIdMessage(TypedDict):
    """A facet reading or cancelling one of its schedules."""

    type: Literal["get", "cancel"]
    id: str


class ListMessage(TypedDict):
    """A facet listing its schedules."""

    type: Literal["list"]
    id: str | None
    schedule_type: ScheduleType | None
    start_ms: int | None
    end_ms: int | None


class DispatchMessage(TypedDict):
    """The root handing a due schedule to the facet that owns it.

    ``retry`` is the job's resolved retry policy (JSON text).
    """

    type: Literal["dispatch"]
    schedule: WireSchedule
    retry: str | None


type ScheduleRouteMessage = InsertMessage | ByIdMessage | ListMessage | DispatchMessage
"""A Scheduler operation routed between Lifecycles."""
