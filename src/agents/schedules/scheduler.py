"""Persistent schedules: the Scheduler capability.

Port of upstream ``schedules/scheduler.ts``. Scheduler owns no storage: each
schedule is one Lifecycle job whose ``fn`` is the callback name and whose
payload carries the schedule's timing and the Lifecycle that owns it.
Lifecycle runs the alarm loop, the retries, and the physical alarm
(``.design/scheduling_queue_tasks_api.md``).
"""

import dataclasses
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from functools import partial
from typing import Any, cast, override

from ..core.methods import get_bound_method, method_name
from ..core.platform_errors import is_code_update_reset, is_platform_failure
from ..core.retry import run_with_retries, validate_retry_options
from ..core.timing import epoch_ms, from_epoch_ms, now_ms
from ..core.types import Duration, JSONValue, RetryOptions
from ..lifecycle.capability import LifecycleCapability
from ..lifecycle.job_queue import retry_from_json, retry_to_json
from ..lifecycle.types import (
    JobContext,
    JobOutcome,
    LifecycleJob,
    Reschedule,
    RouteAddress,
    RouteContext,
)
from .timing import is_recurring, next_cron_ms, parse_interval, parse_when
from .types import (
    DispatchMessage,
    InsertResult,
    Schedule,
    ScheduleCallback,
    ScheduleErrorHandler,
    ScheduleJobPayload,
    ScheduleRouteMessage,
    ScheduleTiming,
    ScheduleType,
    WireSchedule,
)

__all__ = ("Scheduler",)

_log = logging.getLogger("agents.schedules")

_SCHEMA_VERSION_KEY = "cf_agents:schedules_schema_version"
_SCHEMA_VERSION = 2


class Scheduler(LifecycleCapability):
    """Persistent schedules for a Lifecycle object: one-shot, cron, and interval.

    Each schedule is a Lifecycle job. When it comes due, its callback runs
    as ``callback(payload, schedule)``; a raising callback is retried per its
    retry policy, then reported (``schedule:error`` and ``on_error``).
    Recurring schedules then move to their next time; one-shots are deleted.

    Parameters
    ----------
    callbacks
        Callbacks by name; looked up first.
    target
        An object whose methods are callbacks too (e.g. the host itself).
    retry
        The default retry policy (3 attempts, 0.1 s base, 3 s max).
    hung_schedule_timeout
        When a still-running interval schedule counts as hung and may run
        again.
    on_error
        Observes a schedule's terminal failure. Runs as capability code,
        outside the host context.

    Raises
    ------
    TypeError, ValueError
        If ``retry`` is invalid.
    """

    def __init__(
        self,
        *,
        callbacks: Mapping[str, ScheduleCallback] | None = None,
        target: object | None = None,
        retry: RetryOptions | None = None,
        hung_schedule_timeout: Duration = 30,
        on_error: ScheduleErrorHandler | None = None,
    ) -> None:
        super().__init__("scheduler")
        self._callbacks = dict(callbacks or {})
        self._target = target
        self._retry = retry if retry is not None else RetryOptions()
        validate_retry_options(self._retry)
        self._hung_timeout = hung_schedule_timeout
        self._on_error = on_error
        self._warned_startup_callbacks: set[str] = set()

    # Scheduling

    async def set(
        self,
        when: datetime | timedelta | float | str,
        callback: str | Callable[..., Any],
        payload: JSONValue = None,
        *,
        retry: RetryOptions | None = None,
        idempotent: bool | None = None,
    ) -> Schedule:
        """Schedule ``callback(payload, schedule)``.

        Parameters
        ----------
        when
            A ``datetime`` runs it once then; a ``timedelta`` or number of
            seconds runs it once after that delay; a ``str`` runs it on that
            cron expression (UTC).
        callback
            A registered callback name, or a bound method of ``target``.
        payload
            JSON data passed to the callback.
        retry
            Overrides the Scheduler's default retry policy.
        idempotent
            Reuse a matching existing schedule instead of adding another.
            ``None`` means yes for cron, no for one-shots.

        Returns
        -------
        Schedule
            The new schedule, or the existing one it matched.

        Raises
        ------
        ValueError
            If the callback isn't registered, or ``when`` is invalid.
        TypeError
            If ``when`` has the wrong type.
        """
        await self.lifecycle.ready()
        name = self._require_callback(callback)
        timing = parse_when(when, now_ms())
        if retry is not None:
            validate_retry_options(retry)
        self._warn_if_scheduled_during_startup(timing, name, idempotent)
        return await self._create(timing, name, payload, retry, idempotent)

    async def every(
        self,
        interval: Duration,
        callback: str | Callable[..., Any],
        payload: JSONValue = None,
        *,
        retry: RetryOptions | None = None,
        idempotent: bool | None = None,
    ) -> Schedule:
        """Run ``callback(payload, schedule)`` every ``interval``.

        The first run is one interval from now. A run still in progress when
        the next is due is skipped, unless it has run longer than
        ``hung_schedule_timeout``.

        Parameters
        ----------
        interval
            The time between runs (at most 30 days).
        callback
            A registered callback name, or a bound method of ``target``.
        payload
            JSON data passed to the callback.
        retry
            Overrides the Scheduler's default retry policy.
        idempotent
            Reuse a matching existing schedule (default: yes).

        Raises
        ------
        ValueError
            If the callback isn't registered, or the interval is out of range.
        """
        await self.lifecycle.ready()
        name = self._require_callback(callback)
        timing = parse_interval(interval, now_ms())
        if retry is not None:
            validate_retry_options(retry)
        return await self._create(timing, name, payload, retry, idempotent)

    async def get(self, id: str) -> Schedule | None:
        """Return one schedule, or ``None``."""
        await self.lifecycle.ready()
        if self.lifecycle.routes.source is not None:
            wire = await self.lifecycle.routes.to_root({"type": "get", "id": id})
            return _schedule_from_wire(wire) if wire is not None else None
        return self._get_for_owner(None, id)

    async def list(
        self,
        *,
        id: str | None = None,
        type: ScheduleType | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Sequence[Schedule]:
        """Return schedules, optionally filtered by id, type, or next run time."""
        await self.lifecycle.ready()
        start_ms = epoch_ms(start) if start is not None else None
        end_ms = epoch_ms(end) if end is not None else None
        if self.lifecycle.routes.source is not None:
            wires = await self.lifecycle.routes.to_root(
                {
                    "type": "list",
                    "id": id,
                    "schedule_type": type,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }
            )
            return [_schedule_from_wire(wire) for wire in wires]
        return self._list_for_owner(None, id, type, start_ms, end_ms)

    async def cancel(self, id: str) -> bool:
        """Cancel one schedule; return whether it existed."""
        await self.lifecycle.ready()
        if self.lifecycle.routes.source is not None:
            callback = await self.lifecycle.routes.to_root({"type": "cancel", "id": id})
        else:
            callback = await self._cancel_for_owner(None, id)
        if callback is None:
            return False
        self.lifecycle.emit("schedule:cancel", {"callback": callback, "id": id})
        return True

    async def _create(
        self,
        timing: ScheduleTiming,
        callback: str,
        payload: JSONValue,
        retry: RetryOptions | None,
        idempotent: bool | None,
    ) -> Schedule:
        retry_json = retry_to_json(retry) if retry is not None else None
        if self.lifecycle.routes.source is not None:
            result: InsertResult = await self.lifecycle.routes.to_root(
                {
                    "type": "insert",
                    "timing": dataclasses.asdict(timing),
                    "callback": callback,
                    "payload": payload,
                    "retry": retry_json,
                    "idempotent": idempotent,
                }
            )
            schedule, created = (
                _schedule_from_wire(result["schedule"]),
                result["created"],
            )
        else:
            schedule, created = await self._insert(
                None, timing, callback, payload, retry_json, idempotent
            )
        if created:
            self.lifecycle.emit(
                "schedule:create", {"callback": schedule.callback, "id": schedule.id}
            )
        return schedule

    # Lifecycle capability hooks

    @override
    async def on_start(self) -> None:
        """Forget startup warnings, and record the storage version once."""
        self._warned_startup_callbacks.clear()
        storage = self.lifecycle.storage
        if await storage.get(_SCHEMA_VERSION_KEY) is None:
            await storage.put(_SCHEMA_VERSION_KEY, _SCHEMA_VERSION)

    @override
    async def on_job(self, context: JobContext) -> JobOutcome:
        """Run one due schedule handed over by the Lifecycle alarm loop."""
        job, attempt = context.job, context.attempt
        envelope = _envelope(job)
        if envelope is None:
            _log.error("Malformed schedule job %s; dropping it", job.id)
            return None

        if attempt == 1:
            self.lifecycle.emit("schedule:execute", {"callback": job.fn, "id": job.id})
        else:
            self.lifecycle.emit(
                "schedule:retry",
                {
                    "callback": job.fn,
                    "id": job.id,
                    "attempt": attempt,
                    "maxAttempts": (job.retry or self._retry).max_attempts,
                },
            )

        owner_path = envelope["owner_path"]
        if owner_path is not None:
            owner = RouteAddress(
                key=envelope["owner_path_key"] or owner_path, data=owner_path
            )
            if not await self._dispatch_to_owner(owner, job, envelope):
                return "yield"
            return _recurrence(envelope)

        handler = self._resolve(job.fn)
        if handler is None:
            _log.error("Schedule callback %r not found", job.fn)
            return _recurrence(envelope)
        schedule = _schedule(job.id, job.fn, epoch_ms(job.time), envelope)
        await self.lifecycle.run_in_host_context(
            partial(handler, envelope["payload"], schedule)
        )
        return _recurrence(envelope)

    @override
    async def on_job_error(self, context: JobContext, error: Exception) -> JobOutcome:
        """Report a schedule's terminal failure; a recurring one moves on."""
        job = context.job
        envelope = _envelope(job)
        if envelope is None:
            return None
        attempts = (job.retry or self._retry).max_attempts
        await self._report_failure(job.fn, job.id, error, attempts)
        return _recurrence(envelope)

    @override
    async def on_route(self, context: RouteContext) -> Any:
        """Handle a Scheduler operation routed from another Lifecycle."""
        message: ScheduleRouteMessage = context.payload
        owner = context.source
        match message["type"]:
            case "insert":
                schedule, created = await self._insert(
                    owner,
                    ScheduleTiming(**message["timing"]),
                    message["callback"],
                    message["payload"],
                    message["retry"],
                    message["idempotent"],
                )
                return InsertResult(
                    schedule=_schedule_to_wire(schedule), created=created
                )
            case "get":
                found = self._get_for_owner(owner, message["id"])
                return _schedule_to_wire(found) if found is not None else None
            case "list":
                schedules = self._list_for_owner(
                    owner,
                    message["id"],
                    message["schedule_type"],
                    message["start_ms"],
                    message["end_ms"],
                )
                return [_schedule_to_wire(schedule) for schedule in schedules]
            case "cancel":
                return await self._cancel_for_owner(owner, message["id"])
            case "dispatch":
                retry = message["retry"]
                await self._run_routed(
                    _schedule_from_wire(message["schedule"]),
                    message["schedule"]["job"],
                    retry_from_json(retry) if retry is not None else self._retry,
                )
                return True
        raise ValueError(f"Unknown routed Scheduler message {message!r}")

    # Dispatch

    async def _dispatch_to_owner(
        self, owner: RouteAddress, job: LifecycleJob, envelope: ScheduleJobPayload
    ) -> bool:
        """Hand a facet's schedule to the facet; return whether it was delivered.

        The facet retries the callback itself, so this is one attempt.
        """
        message: DispatchMessage = {
            "type": "dispatch",
            "schedule": WireSchedule(
                id=job.id, callback=job.fn, time_ms=epoch_ms(job.time), job=envelope
            ),
            "retry": retry_to_json(job.retry) if job.retry is not None else None,
        }
        try:
            await self.lifecycle.routes.to(owner, message)
        except Exception as error:
            if is_platform_failure(error):
                raise  # Lifecycle preserves the job and defers it
            _log.exception("Error dispatching schedule callback %r", job.fn)
            self.lifecycle.emit(
                "schedule:error",
                {"callback": job.fn, "id": job.id, "error": str(error), "attempts": 0},
            )
            await self._notify_on_error(error)
            return False  # left due: a later alarm retries the dispatch
        return True

    async def _run_routed(
        self, schedule: Schedule, envelope: ScheduleJobPayload, retry: RetryOptions
    ) -> None:
        """Run a routed schedule here, in its owning facet, with its own retries.

        A one-shot's platform failure re-raises, so the root keeps the job
        and its alarm retries it in a fresh invocation.
        """
        handler = self._resolve(schedule.callback)
        if handler is None:
            _log.error("Schedule callback %r not found", schedule.callback)
            return
        one_shot = not is_recurring(_timing(envelope))
        try:
            await run_with_retries(
                partial(self._attempt_routed, handler, schedule, retry),
                retry,
                should_retry=lambda error, _attempt: (
                    not (one_shot and is_code_update_reset(error))
                ),
            )
        except Exception as error:
            if one_shot and is_platform_failure(error):
                raise
            await self._report_failure(
                schedule.callback, schedule.id, error, retry.max_attempts
            )

    async def _attempt_routed(
        self,
        handler: ScheduleCallback,
        schedule: Schedule,
        retry: RetryOptions,
        attempt: int,
    ) -> None:
        if attempt > 1:
            self.lifecycle.emit(
                "schedule:retry",
                {
                    "callback": schedule.callback,
                    "id": schedule.id,
                    "attempt": attempt,
                    "maxAttempts": retry.max_attempts,
                },
            )
        await self.lifecycle.run_in_host_context(
            partial(handler, schedule.payload, schedule)
        )

    async def _report_failure(
        self, callback: str, id: str, error: Exception, attempts: int
    ) -> None:
        _log.error(
            "Schedule callback %r failed after %d attempts",
            callback,
            attempts,
            exc_info=error,
        )
        self.lifecycle.emit(
            "schedule:error",
            {"callback": callback, "id": id, "error": str(error), "attempts": attempts},
        )
        await self._notify_on_error(error)

    async def _notify_on_error(self, error: Exception) -> None:
        if self._on_error is None:
            return
        try:
            await self._on_error(error)
        except Exception:
            # The observer failing must not fail the Scheduler (upstream swallows).
            _log.exception("Scheduler on_error handler failed")

    # Storage

    async def _insert(
        self,
        owner: RouteAddress | None,
        timing: ScheduleTiming,
        callback: str,
        payload: JSONValue,
        retry_json: str | None,
        idempotent: bool | None,
    ) -> tuple[Schedule, bool]:
        """Push a schedule job, or reuse the one an idempotent call matches.

        Returns the schedule and whether it's new.
        """
        # Recurring schedules deduplicate unless told not to; one-shots only
        # when asked.
        if idempotent if idempotent is not None else is_recurring(timing):
            existing = self._find_matching(owner, timing, callback, payload)
            if existing is not None:
                # Re-arming recovers a lost alarm, as idempotent rescheduling
                # on startup has always guaranteed.
                await self.lifecycle.jobs.rearm()
                return existing, False

        envelope = ScheduleJobPayload(
            payload=payload,
            type=timing.type,
            retry=retry_json,
            delay_seconds=timing.delay_seconds,
            cron=timing.cron,
            interval_seconds=timing.interval_seconds,
            owner_path=owner.data if owner is not None else None,
            owner_path_key=owner.key if owner is not None else None,
        )
        job = await self.lifecycle.jobs.push(
            fn=callback,
            time=from_epoch_ms(timing.time_ms),
            payload=cast("dict[str, JSONValue]", envelope),
            retry=retry_from_json(retry_json)
            if retry_json is not None
            else self._retry,
            singleflight=timing.type == "interval",
            hung_timeout=self._hung_timeout,
        )
        return _schedule(job.id, callback, timing.time_ms, envelope), True

    def _find_matching(
        self,
        owner: RouteAddress | None,
        timing: ScheduleTiming,
        callback: str,
        payload: JSONValue,
    ) -> Schedule | None:
        key = owner.key if owner is not None else None
        payload_json = json.dumps(payload, sort_keys=True)
        for job, envelope in self._owned_jobs():
            if (
                envelope["type"] == timing.type
                and job.fn == callback
                and envelope["owner_path_key"] == key
                and json.dumps(envelope["payload"], sort_keys=True) == payload_json
                and envelope["cron"] == timing.cron
                and envelope["interval_seconds"] == timing.interval_seconds
            ):
                return _schedule(job.id, job.fn, epoch_ms(job.time), envelope)
        return None

    def _owned_jobs(self) -> Sequence[tuple[LifecycleJob, ScheduleJobPayload]]:
        owned = []
        for job in self.lifecycle.jobs.list():
            envelope = _envelope(job)
            if envelope is not None:
                owned.append((job, envelope))
        return owned

    def _get_for_owner(self, owner: RouteAddress | None, id: str) -> Schedule | None:
        job = self.lifecycle.jobs.get(id)
        if job is None:
            return None
        envelope = _envelope(job)
        if envelope is None or envelope["owner_path_key"] != _owner_key(owner):
            return None
        return _schedule(job.id, job.fn, epoch_ms(job.time), envelope)

    def _list_for_owner(
        self,
        owner: RouteAddress | None,
        id: str | None,
        schedule_type: ScheduleType | None,
        start_ms: int | None,
        end_ms: int | None,
    ) -> Sequence[Schedule]:
        key = _owner_key(owner)
        schedules = []
        for job, envelope in self._owned_jobs():
            time_ms = epoch_ms(job.time)
            if (
                envelope["owner_path_key"] != key
                or (id is not None and job.id != id)
                or (schedule_type is not None and envelope["type"] != schedule_type)
                or (start_ms is not None and time_ms < start_ms)
                or (end_ms is not None and time_ms > end_ms)
            ):
                continue
            schedules.append(_schedule(job.id, job.fn, time_ms, envelope))
        return schedules

    async def _cancel_for_owner(
        self, owner: RouteAddress | None, id: str
    ) -> str | None:
        """Cancel one schedule; return its callback name, or ``None`` if absent."""
        found = self._get_for_owner(owner, id)
        if found is None:
            return None
        await self.lifecycle.jobs.cancel(id)
        return found.callback

    async def _cleanup_route_prefix(self, prefix: str) -> None:
        """Remove the schedules owned by one routed Lifecycle subtree (internal)."""
        for job, envelope in self._owned_jobs():
            owner_path = envelope["owner_path"]
            if owner_path is None:
                continue
            key = envelope["owner_path_key"] or owner_path
            if key == prefix or key.startswith(f"{prefix}/"):
                self.lifecycle.emit(
                    "schedule:cancel", {"callback": job.fn, "id": job.id}
                )
                await self.lifecycle.jobs.cancel(job.id)

    # Callbacks

    def _require_callback(self, callback: str | Callable[..., Any]) -> str:
        if isinstance(callback, str):
            name = callback
        elif self._target is None:
            raise TypeError(
                "This Scheduler has no target to look methods up on; pass the "
                "callback's registered name"
            )
        else:
            name = method_name(self._target, callback)
        if self._resolve(name) is None:
            raise ValueError(
                f"Unknown schedule callback {name!r}: not registered on this Scheduler"
            )
        return name

    def _resolve(self, name: str) -> ScheduleCallback | None:
        """Return the callback registered as ``name`` (the dict, then ``target``)."""
        handler = self._callbacks.get(name)
        if handler is not None or self._target is None:
            return handler
        try:
            return get_bound_method(self._target, name)
        except (AttributeError, TypeError):
            return None

    def _warn_if_scheduled_during_startup(
        self, timing: ScheduleTiming, callback: str, idempotent: bool | None
    ) -> None:
        """Warn once per callback about a one-shot created on every wake.

        Passing ``idempotent`` either way opts out.
        """
        if (
            self.lifecycle.status() != "starting"
            or idempotent is not None
            or timing.type == "cron"
            or callback in self._warned_startup_callbacks
        ):
            return
        self._warned_startup_callbacks.add(callback)
        _log.warning(
            "Scheduling %r during startup (e.g. in on_start) without "
            "idempotent=True creates a new schedule on every wake, which can "
            "run it more than once. Pass idempotent=True to reuse one, or "
            "use schedule_every() for recurring work.",
            callback,
        )


def _owner_key(owner: RouteAddress | None) -> str | None:
    return owner.key if owner is not None else None


def _envelope(job: LifecycleJob) -> ScheduleJobPayload | None:
    """Return a schedule job's payload, or ``None`` if it's malformed."""
    raw = job.payload
    if not isinstance(raw, dict):
        return None
    kind = raw.get("type")
    if kind not in ("scheduled", "delayed", "cron", "interval"):
        return None
    return ScheduleJobPayload(
        payload=raw.get("payload"),
        type=cast(ScheduleType, kind),
        retry=_str(raw.get("retry")),
        delay_seconds=_number(raw.get("delay_seconds")),
        cron=_str(raw.get("cron")),
        interval_seconds=_number(raw.get("interval_seconds")),
        owner_path=_str(raw.get("owner_path")),
        owner_path_key=_str(raw.get("owner_path_key")),
    )


def _str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return value


def _timing(envelope: ScheduleJobPayload) -> ScheduleTiming:
    return ScheduleTiming(
        type=envelope["type"],
        time_ms=0,
        delay_seconds=envelope["delay_seconds"],
        cron=envelope["cron"],
        interval_seconds=envelope["interval_seconds"],
    )


def _recurrence(envelope: ScheduleJobPayload) -> JobOutcome:
    """Move a recurring schedule to its next time; complete a one-shot."""
    cron = envelope["cron"]
    if envelope["type"] == "cron" and cron is not None:
        return Reschedule(at=from_epoch_ms(next_cron_ms(cron, now_ms())))
    interval = envelope["interval_seconds"]
    if envelope["type"] == "interval" and interval is not None:
        return Reschedule(at=from_epoch_ms(now_ms() + round(interval * 1000)))
    return None


def _schedule(
    id: str, callback: str, time_ms: int, envelope: ScheduleJobPayload
) -> Schedule:
    retry = envelope["retry"]
    delay = envelope["delay_seconds"]
    interval = envelope["interval_seconds"]
    return Schedule(
        id=id,
        callback=callback,
        payload=envelope["payload"],
        type=envelope["type"],
        time=from_epoch_ms(time_ms),
        retry=retry_from_json(retry) if retry is not None else None,
        delay=timedelta(seconds=delay) if delay is not None else None,
        cron=envelope["cron"],
        interval=timedelta(seconds=interval) if interval is not None else None,
    )


def _schedule_to_wire(schedule: Schedule) -> WireSchedule:
    delay, interval = schedule.delay, schedule.interval
    return WireSchedule(
        id=schedule.id,
        callback=schedule.callback,
        time_ms=epoch_ms(schedule.time),
        job=ScheduleJobPayload(
            payload=schedule.payload,
            type=schedule.type,
            retry=retry_to_json(schedule.retry) if schedule.retry is not None else None,
            delay_seconds=delay.total_seconds() if delay is not None else None,
            cron=schedule.cron,
            interval_seconds=interval.total_seconds() if interval is not None else None,
            owner_path=None,
            owner_path_key=None,
        ),
    )


def _schedule_from_wire(wire: WireSchedule) -> Schedule:
    return _schedule(wire["id"], wire["callback"], wire["time_ms"], wire["job"])
