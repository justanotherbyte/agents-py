"""The alarm event loop that drives the job queue.

Port of upstream ``lifecycle/job-driver.ts``. When the physical alarm fires
the driver: arms a deadman alarm, drives due jobs in due order (single-flight
skip and hung recovery, per-job retries, platform-failure deferral, terminal
failure hooks), runs the host's alarm hook, and re-arms the alarm, all inside
the alarm memory-limit circuit breaker (upstream #1825).
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from datetime import datetime
from functools import partial
from typing import Any

from ..core.platform_errors import (
    is_code_update_reset,
    is_memory_limit_reset,
    is_platform_failure,
)
from ..core.retry import run_with_retries
from ..core.timing import from_epoch_ms, now_ms
from ..core.types import RetryOptions
from .errors import AttributedPlatformFailure
from .job_queue import JobQueue, hung_timeout_ms, is_hung, job_from_row
from .types import (
    AlarmScope,
    DriverStorage,
    JobContext,
    JobDispatch,
    JobOutcome,
    JobRow,
    LifecycleJob,
    MemoryLimitContext,
    MemoryLimitStrike,
)

__all__ = ("JobDriver",)

_log = logging.getLogger("agents.lifecycle")

DEFAULT_MAX_ALARM_MEMORY_LIMIT_STRIKES = 3
_OOM_ALARM_STRIKES_KEY = "cf_agents:oom_alarm_strikes"
_DEFAULT_JOB_RETRY = RetryOptions(max_attempts=3, base_delay=0.1, max_delay=3.0)
_JOB_BACKLOG_WARNING_THRESHOLD = 10
# Armed before driving jobs, so an isolate death mid-drive still wakes the
# object to resume its queue; the final re-arm overwrites it.
_DEADMAN_ALARM_DELAY_MS = 30_000


_alarm_scope: ContextVar[AlarmScope | None] = ContextVar(
    "agents_alarm_scope", default=None
)


class JobDriver:
    """Drives the job queue when the Durable Object's alarm fires.

    Parameters
    ----------
    queue
        The object's job queue.
    storage
        The object's ``ctx.storage`` (Workers SDK wrapper).
    disabled
        Returns ``True`` once the host is being destroyed; the driver then
        stops touching storage.
    resolve_dispatch
        Resolves a job's owner to its dispatch hooks, or ``None``.
    max_memory_limit_strikes
        Consecutive memory-limit resets tolerated before sealing.
    on_memory_limit
        Capability and host policy applied on each strike.
    emit
        Best-effort lifecycle telemetry.
    rearm
        Recomputes the physical alarm from queue state.
    reset
        Schedules an isolate reset that doesn't retry the current alarm.
    """

    def __init__(
        self,
        *,
        queue: JobQueue,
        storage: DriverStorage,
        disabled: Callable[[], bool],
        resolve_dispatch: Callable[[str], Awaitable[JobDispatch | None]],
        max_memory_limit_strikes: int,
        on_memory_limit: Callable[[MemoryLimitContext], Awaitable[None]],
        emit: Callable[[str, dict[str, Any]], None],
        rearm: Callable[[], Awaitable[None]],
        reset: Callable[[str], None],
    ) -> None:
        self._queue = queue
        self._storage = storage
        self._disabled = disabled
        self._resolve_dispatch = resolve_dispatch
        self._max_strikes = max_memory_limit_strikes
        self._on_memory_limit = on_memory_limit
        self._emit = emit
        self._rearm = rearm
        self._reset = reset
        self._alarms_in_flight = 0
        # Handed-off work still running; while non-empty a clean alarm must not
        # clear the strike counter (a handoff may yet report a reset).
        self._outstanding_work: set[asyncio.Future[Any]] = set()
        self._settle_tasks: set[asyncio.Task[None]] = set()
        # One reset is often seen by several flows; they share one strike.
        self._strike: asyncio.Future[MemoryLimitStrike] | None = None
        # Once a strike is recorded in this isolate nothing may clear the
        # counter; the reset that follows gives the next cycle a fresh driver.
        self._strike_recorded_this_isolate = False

    async def run_alarm(
        self,
        initialize: Callable[[], Awaitable[None]],
        run_host_alarm: Callable[[], Awaitable[None]],
    ) -> None:
        """Run one alarm invocation inside the memory-limit circuit breaker.

        A memory-limit reset is intercepted and handled here, where the heavy
        work has unwound; any other error re-raises so the platform's alarm
        retry still applies.
        """
        self._alarms_in_flight += 1
        clean = False
        try:
            try:
                await initialize()
                await self._drive_due_jobs()
                await _in_alarm_scope(None, run_host_alarm)
                clean = True
            except AttributedPlatformFailure as failure:
                if not is_memory_limit_reset(failure.cause):
                    raise failure.cause from None
                await self._handle_memory_limit_reset(failure.cause, failure.row)
                return
            except Exception as error:
                if not is_memory_limit_reset(error):
                    raise
                await self._handle_memory_limit_reset(error, None)
                return
        finally:
            self._alarms_in_flight -= 1
            if clean:
                await self._clear_strikes_when_quiescent()
        await self._rearm()

    def track_alarm_work(self, work: asyncio.Future[Any]) -> bool:
        """Keep work a job handed off inside this alarm's breaker domain.

        Returns
        -------
        bool
            ``True`` when called from an alarm-driven dispatch or the host's
            alarm hook (tracking the same work again is a no-op); ``False``
            otherwise, and nothing is tracked.
        """
        scope = _alarm_scope.get()
        if scope is None:
            return False
        if work in self._outstanding_work:
            return True
        self._outstanding_work.add(work)

        def settled(done: asyncio.Future[Any]) -> None:
            task = asyncio.ensure_future(self._settle_alarm_work(done, scope.executing))
            self._settle_tasks.add(task)
            task.add_done_callback(self._settle_tasks.discard)

        work.add_done_callback(settled)
        return True

    async def _settle_alarm_work(
        self, work: asyncio.Future[Any], executing: JobRow | None
    ) -> None:
        try:
            if self._disabled():
                return
            error = None if work.cancelled() else work.exception()
            if error is not None and is_memory_limit_reset(error):
                await self._handle_memory_limit_reset(error, executing)
                return
        finally:
            self._outstanding_work.discard(work)
        await self._clear_strikes_when_quiescent()

    async def _clear_strikes_when_quiescent(self) -> None:
        if self._alarms_in_flight > 0 or self._outstanding_work:
            return
        if self._strike_recorded_this_isolate:
            return
        self._strike = None
        await self._clear_memory_limit_strikes()

    async def _drive_due_jobs(self) -> None:
        now = now_ms()
        due = self._queue.due(now)
        if not due:
            return
        self._warn_backlog(due)
        if not self._disabled():
            await self._storage.setAlarm(now + _DEADMAN_ALARM_DELAY_MS)

        for stale in due:
            if self._disabled():
                return
            # An earlier dispatch may have pushed, rescheduled, or cancelled
            # this job; a row that is gone or no longer due belongs to that
            # newer intent.
            row = self._queue.due_row(stale["id"], now)
            if row is None:
                continue
            if row["singleflight"] == 1 and row["running"] == 1:
                if not is_hung(row, now):
                    _log.warning(
                        "Skipping job %s: previous execution still running", row["id"]
                    )
                    continue
                _log.warning(
                    "Forcing reset of hung job %s (started %ss ago)",
                    row["id"],
                    round((now - (row["execution_started_at"] or 0)) / 1000),
                )
            # Every dispatch is marked, so a same-id push made mid-dispatch
            # supersedes the outcome it returns (JobQueue.apply_outcome).
            self._queue.mark_running(row["id"], now)
            await self._drive_job(row)

    async def _drive_job(self, row: JobRow) -> None:
        job = job_from_row(row)
        dispatch = await self._resolve_dispatch(row["capability"])
        if dispatch is None:
            _log.error(
                "No installed capability or host handler for job %s (owner %r); "
                "dropping it",
                row["id"],
                row["capability"],
            )
            self._queue.delete(row["id"])
            return

        retry = job.retry or _DEFAULT_JOB_RETRY
        watchdog = asyncio.get_running_loop().call_later(
            hung_timeout_ms(row) / 1000, self._warn_slow_dispatch, row
        )
        outcome: JobOutcome = None
        try:
            outcome = await _in_alarm_scope(
                row, partial(self._dispatch_with_retries, dispatch, job, retry)
            )
        except Exception as error:
            if self._disabled():
                return
            if is_platform_failure(error):
                self._queue.clear_running(row["id"])
                _log.warning(
                    "Deferring job %s to a fresh invocation after a platform "
                    "failure; the job is preserved.",
                    row["id"],
                )
                raise AttributedPlatformFailure(row, error) from error
            outcome = await self._job_error_outcome(dispatch, job, retry, error)
        finally:
            watchdog.cancel()
        if self._disabled():
            return
        self._queue.apply_outcome(row["id"], outcome)

    async def _dispatch_with_retries(
        self, dispatch: JobDispatch, job: LifecycleJob, retry: RetryOptions
    ) -> JobOutcome:
        async def attempt(number: int) -> JobOutcome:
            return await dispatch.on_job(JobContext(job=job, attempt=number))

        return await run_with_retries(attempt, retry, should_retry=_retry_in_process)

    async def _job_error_outcome(
        self,
        dispatch: JobDispatch,
        job: LifecycleJob,
        retry: RetryOptions,
        error: Exception,
    ) -> JobOutcome:
        """Let the owner decide what a terminally failed job becomes."""
        if dispatch.on_job_error is None:
            return None
        context = JobContext(job=job, attempt=retry.max_attempts)
        try:
            return await dispatch.on_job_error(context, error)
        except Exception:
            # The job's own failure is already final; a failing hook completes it.
            _log.exception("Job failure hook raised for %s", job.id)
            return None

    def _warn_slow_dispatch(self, row: JobRow) -> None:
        threshold = hung_timeout_ms(row)
        _log.warning(
            "Job %s (%s/%s) has been dispatching for over %ss. Long dispatches "
            "starve every other job on this object; on_job must detach unbounded "
            "work and return.",
            row["id"],
            row["capability"],
            row["fn"],
            round(threshold / 1000),
        )
        self._emit(
            "job:slow_dispatch",
            {
                "capability": row["capability"],
                "fn": row["fn"],
                "id": row["id"],
                "thresholdMs": threshold,
            },
        )

    def _warn_backlog(self, due: Sequence[JobRow]) -> None:
        counts: dict[str, int] = {}
        for row in due:
            counts[row["capability"]] = counts.get(row["capability"], 0) + 1
        for owner, count in counts.items():
            if count < _JOB_BACKLOG_WARNING_THRESHOLD:
                continue
            _log.warning(
                "Processing %s due jobs for %r in a single alarm cycle. This "
                "usually means one-shot jobs are pushed repeatedly without a "
                "stable id.",
                count,
                owner,
            )
            self._emit("job:backlog_warning", {"capability": owner, "count": count})

    async def _clear_memory_limit_strikes(self) -> None:
        # Strikes count consecutive resets: a clean, quiescent alarm clears them.
        prior = await self._storage.get(_OOM_ALARM_STRIKES_KEY)
        if isinstance(prior, int) and prior > 0:
            await self._storage.delete(_OOM_ALARM_STRIKES_KEY)

    async def _handle_memory_limit_reset(
        self, error: BaseException, executing: JobRow | None
    ) -> None:
        """Break the platform's alarm-retry loop after a memory-limit reset.

        A durable counter tolerates a few consecutive strikes, backing off the
        executing job and every recovery-loop job, then seals: those jobs are
        purged. Each step is best-effort; even small writes can fail on a
        condemned isolate, and returning still stops the platform's retry.
        """
        first = self._strike is None
        self._strike_recorded_this_isolate = True
        if self._strike is None:
            self._strike = asyncio.ensure_future(self._record_strike(error))
        strike = await asyncio.shield(self._strike)

        await _best_effort(self._apply_strike_to_job, strike, executing)
        await _best_effort(
            self._on_memory_limit,
            MemoryLimitContext(
                sealed=strike.sealed,
                next_time=_maybe_datetime(strike.next_time_ms),
                executing=job_from_row(executing) if executing is not None else None,
                purged_recovery_loop_jobs=strike.purged_recovery_loop_jobs
                if first
                else None,
            ),
        )
        if not first:
            return
        await _best_effort(self._rearm)
        # Sync first: a reset discards unconfirmed writes, and the strike and
        # backoff must land. The backoff alarm owns the next wake.
        await _best_effort(self._storage.sync)
        self._reset(
            f"Alarm memory-limit strike {strike.strikes}/{strike.limit}"
            f"{' (sealed)' if strike.sealed else ''}; resetting isolate"
        )

    async def _apply_strike_to_job(
        self, strike: MemoryLimitStrike, row: JobRow | None
    ) -> None:
        if row is None:
            return
        if strike.sealed:
            self._queue.delete(row["id"])
        elif strike.next_time_ms is not None:
            self._queue.retime(row["id"], strike.next_time_ms)

    async def _record_strike(self, error: BaseException) -> MemoryLimitStrike:
        """Record one strike durably and apply the queue-wide policy once."""
        strikes = 1
        try:
            prior = await self._storage.get(_OOM_ALARM_STRIKES_KEY)
            strikes = (prior if isinstance(prior, int) else 0) + 1
            await self._storage.put(_OOM_ALARM_STRIKES_KEY, strikes)
        except Exception:
            _log.exception("Couldn't persist the memory-limit strike counter")

        limit = self._max_strikes
        sealed = strikes >= limit
        _log.error(
            "Alarm hit a Durable Object memory-limit reset (strike %s/%s%s). "
            "Breaking the platform alarm-retry loop: %s",
            strikes,
            limit,
            ", sealing recovery" if sealed else ", will retry with backoff",
            error,
        )
        next_time_ms = None if sealed else now_ms() + min(300, 30 * strikes) * 1000

        purged: Sequence[LifecycleJob] | None = None
        if sealed:
            # Snapshot before purging: policy hooks run after the rows are gone.
            purged = await _best_effort(self._snapshot_and_purge_recovery_loop)
            await _best_effort(self._storage.delete, _OOM_ALARM_STRIKES_KEY)
        elif next_time_ms is not None:
            await _best_effort(self._delay_recovery_loop, next_time_ms)

        self._emit(
            "alarm:memory_limit_reset",
            {"strikes": strikes, "limit": limit, "sealed": sealed, "error": str(error)},
        )
        return MemoryLimitStrike(
            strikes=strikes,
            limit=limit,
            sealed=sealed,
            next_time_ms=next_time_ms,
            purged_recovery_loop_jobs=purged,
        )

    async def _snapshot_and_purge_recovery_loop(self) -> Sequence[LifecycleJob]:
        jobs = tuple(self._queue.recovery_loop_jobs())
        self._queue.purge_recovery_loop_jobs()
        return jobs

    async def _delay_recovery_loop(self, time_ms: int) -> None:
        # Recovery-loop rows travel as a pack: a doomed loop's siblings would
        # re-trigger it on the next wake.
        self._queue.delay_recovery_loop_jobs(time_ms)


def _retry_in_process(error: Exception, _next_attempt: int) -> bool:
    # Retrying in-process is futile on a superseded isolate, and on a
    # memory-limit reset it can read half-claimed state as "nothing to do",
    # hiding the reset from the breaker. Defer both to the alarm boundary.
    return not is_code_update_reset(error) and not is_memory_limit_reset(error)


async def _in_alarm_scope[T](
    executing: JobRow | None, fn: Callable[[], Awaitable[T]]
) -> T:
    token = _alarm_scope.set(AlarmScope(executing))
    try:
        return await fn()
    finally:
        _alarm_scope.reset(token)


async def _best_effort[T, *Args](
    fn: Callable[[*Args], Awaitable[T]], *args: *Args
) -> T | None:
    # The breaker runs on a condemned isolate where even small writes can
    # fail; each step must not stop the next, and returning still halts the
    # platform's alarm retry.
    try:
        return await fn(*args)
    except Exception:
        _log.exception("Best-effort memory-limit step failed")
        return None


def _maybe_datetime(ms: int | None) -> datetime | None:
    return from_epoch_ms(ms) if ms is not None else None
