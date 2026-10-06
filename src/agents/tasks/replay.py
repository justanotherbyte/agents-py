"""Replay: the `TaskStep` one execution attempt receives.

Port of upstream ``tasks/replay.ts``. A completed step returns its journaled
result; the first unfinished step (the frontier) does real work. Sleeps and
retry waits end the attempt with `TaskSuspension`. Each step attempt runs as
its own asyncio task, so a timeout or cancellation settles the attempt even
if the step function ignores it (``.design/scheduling_queue_tasks_api.md``
§2.6).
"""

import asyncio
import math
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from ..core.platform_errors import (
    is_code_update_reset,
    is_memory_limit_reset,
    is_platform_failure,
)
from ..core.timing import epoch_ms, now_ms, to_milliseconds
from ..core.types import Duration
from .engine import TaskStepEngine
from .errors import (
    AttemptSupersededError,
    DuplicateTaskStepError,
    StepTimeoutError,
    TaskCancellation,
    TaskReplayDivergedError,
    TaskSerializationError,
    TaskSuspension,
    is_non_retryable,
)
from .serialization import deserialize_task_value, serialize_task_value
from .types import (
    ResolvedStepPolicy,
    StepInterruption,
    StepRetries,
    TaskStepAttempt,
    TaskStepRow,
)

__all__ = (
    "MAX_STEPS_PER_RUN",
    "MAX_STEP_NAME_LENGTH",
    "ReplayStep",
    "resolve_step_policy",
    "retry_delay_ms",
)

MAX_STEPS_PER_RUN = 10_000
MAX_STEP_NAME_LENGTH = 256
_MAX_RETRY_DELAY_MS = 24 * 60 * 60 * 1000


def retry_delay_ms(policy: ResolvedStepPolicy, failed_attempt: int) -> int:
    """Return the wait before the attempt after ``failed_attempt`` (capped at a day)."""
    base = policy.retry_delay_ms
    match policy.backoff:
        case "constant":
            delay = base
        case "linear":
            delay = base * failed_attempt
        case "exponential":
            delay = base * 2 ** (failed_attempt - 1)
    return min(delay, _MAX_RETRY_DELAY_MS)


def resolve_step_policy(
    defaults: ResolvedStepPolicy,
    retries: StepRetries | None,
    timeout: Duration | None,
) -> ResolvedStepPolicy:
    """Fill a step's unset retry and timeout fields from the defaults.

    Raises
    ------
    ValueError
        If ``retries.limit`` is below 1, a delay is negative, or the timeout
        isn't positive.
    """
    limit = (
        retries.limit if retries and retries.limit is not None else defaults.retry_limit
    )
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError(f"StepRetries.limit must be an int >= 1, not {limit!r}")
    delay_ms = defaults.retry_delay_ms
    if retries is not None and retries.delay is not None:
        delay_ms = _duration_ms(retries.delay, "StepRetries.delay", allow_zero=True)
    timeout_ms = (
        _duration_ms(timeout, "step timeout", allow_zero=False)
        if timeout is not None
        else defaults.timeout_ms
    )
    backoff = retries.backoff if retries and retries.backoff else defaults.backoff
    return ResolvedStepPolicy(
        retry_limit=limit,
        retry_delay_ms=delay_ms,
        backoff=backoff,
        timeout_ms=timeout_ms,
    )


def _duration_ms(duration: Duration, label: str, *, allow_zero: bool) -> int:
    ms = to_milliseconds(duration)
    if not math.isfinite(ms) or ms < 0 or (ms == 0 and not allow_zero):
        raise ValueError(
            f"{label} must be a {'non-negative' if allow_zero else 'positive'} duration"
        )
    return int(ms)


class ReplayStep:
    """The `TaskStep` for one execution attempt.

    The first attempt starts "live". A later attempt starts silent and goes
    live at the frontier (the first journal miss, or a step still waiting or
    running), so replayed ``status()`` calls don't re-publish old progress.

    Parameters
    ----------
    engine
        The attempt's journal operations.
    starts_live
        Whether ``status()`` is recorded from the start (the first attempt).
    interrupted
        The step a dead isolate left running, if any.
    """

    __slots__ = ("_current", "_engine", "_interrupted", "_live", "_used_names")

    def __init__(
        self,
        engine: TaskStepEngine,
        *,
        starts_live: bool,
        interrupted: StepInterruption | None,
    ) -> None:
        self._engine = engine
        self._live = starts_live
        self._interrupted = interrupted
        self._used_names: set[str] = set()
        self._current: asyncio.Future[Any] | None = None

    @property
    def interrupted(self) -> StepInterruption | None:
        """The step a dead isolate left running, or ``None`` on a clean attempt."""
        return self._interrupted

    def cancel_current(self) -> None:
        """Cancel the step attempt in progress, if any (run cancellation)."""
        if self._current is not None:
            self._current.cancel()

    async def do[T](
        self,
        name: str,
        fn: Callable[[TaskStepAttempt], Awaitable[T]],
        *,
        retries: StepRetries | None = None,
        timeout: Duration | None = None,
    ) -> T:
        """Run ``fn`` once as step ``name``; replays return its journaled result.

        Returns the journaled (decoded JSON) value even the first time, so
        live runs and replays see the same values.

        Raises
        ------
        DuplicateTaskStepError
            If this run already used ``name``.
        TaskReplayDivergedError
            If ``name`` was journaled as a sleep.
        TaskSerializationError
            If ``fn``'s result isn't plain JSON (the run fails).
        """
        policy = resolve_step_policy(self._engine.defaults, retries, timeout)
        self._enter_step(name)
        row = self._engine.read_step(name)
        if row is None:
            self._live = True
            if self._engine.count_steps() >= MAX_STEPS_PER_RUN:
                raise RuntimeError(
                    f"Run exceeded {MAX_STEPS_PER_RUN} steps; split the work "
                    "across several task runs"
                )
            self._engine.insert_do_step(name)
            return await self._execute_attempt(name, 1, policy, fn)
        if row["kind"] != "do":
            raise TaskReplayDivergedError(
                name, f"journaled as a {row['kind']} step but replayed as a do step"
            )
        match row["state"]:
            case "completed":
                return deserialize_task_value(row["result"])
            case "failed":
                # A failed step fails its run, so replay shouldn't get here.
                raise _restored_error(row)
            case "waiting":
                self._live = True
                wake_at = row["next_at"] if row["next_at"] is not None else now_ms()
                if now_ms() < wake_at:
                    raise TaskSuspension(wake_at, "retry")
                attempt = self._engine.claim_step_attempt(name)
                self._engine.emit("task:step:retry", {"step": name, "attempt": attempt})
                return await self._execute_attempt(name, attempt, policy, fn)
            case "running":
                # An attempt was interrupted mid-step: run it again.
                self._live = True
                attempt = self._engine.claim_step_attempt(name)
                return await self._execute_attempt(name, attempt, policy, fn)

    async def sleep(self, name: str, duration: Duration) -> None:
        """Sleep durably for ``duration``; the first recorded deadline wins."""
        delay_ms = _duration_ms(duration, "sleep duration", allow_zero=True)
        await self._sleep_at(name, lambda: now_ms() + delay_ms)

    async def sleep_until(self, name: str, when: datetime) -> None:
        """Sleep durably until ``when`` (timezone-aware)."""
        wake_at = epoch_ms(when)
        await self._sleep_at(name, lambda: wake_at)

    async def status(self, message: str) -> None:
        """Record observable progress (silent while replaying old ground)."""
        if self._live:
            self._engine.write_status(str(message))

    def idempotency_key(self, name: str) -> str:
        """Return the key ``do(name, ...)`` passes to its attempts."""
        return self._engine.idempotency_key(name)

    # Internals

    def _enter_step(self, name: str) -> None:
        """Check a step boundary: the name rules, duplicates, cancellation."""
        if not isinstance(name, str) or not name:
            raise ValueError("Step names must be non-empty strings")
        if len(name) > MAX_STEP_NAME_LENGTH:
            raise ValueError(
                f"Step names must be at most {MAX_STEP_NAME_LENGTH} characters"
            )
        if name.startswith("__cf"):
            raise ValueError('Step names must not use the reserved "__cf" prefix')
        if name in self._used_names:
            raise DuplicateTaskStepError(name)
        self._used_names.add(name)
        requested, reason = self._engine.cancellation_requested()
        if requested:
            raise TaskCancellation(reason)

    async def _sleep_at(self, name: str, wake_time: Callable[[], int]) -> None:
        self._enter_step(name)
        row = self._engine.read_step(name)
        if row is None:
            self._live = True
            wake_at = wake_time()
            if wake_at <= now_ms():
                self._engine.insert_completed_sleep(name)
                return
            self._engine.insert_sleep_step(name, wake_at)
            raise TaskSuspension(wake_at, "sleep")
        if row["kind"] != "sleep":
            raise TaskReplayDivergedError(
                name, f"journaled as a {row['kind']} step but replayed as a sleep"
            )
        if row["state"] == "completed":
            return
        self._live = True
        wake_at = row["next_at"] or 0
        if now_ms() < wake_at:
            raise TaskSuspension(wake_at, "sleep")
        self._engine.complete_step(name, None)

    async def _execute_attempt[T](
        self,
        name: str,
        attempt: int,
        policy: ResolvedStepPolicy,
        fn: Callable[[TaskStepAttempt], Awaitable[T]],
    ) -> T:
        """Run one attempt of a ``do`` step under its timeout and retry policy."""
        engine = self._engine
        engine.refresh_claim()
        engine.emit("task:step:started", {"step": name, "attempt": attempt})
        context = TaskStepAttempt(
            attempt=attempt, idempotency_key=engine.idempotency_key(name)
        )
        error: BaseException
        try:
            work = asyncio.ensure_future(fn(context))
        except Exception as raised:  # fn raised before returning an awaitable
            error = raised
        else:
            self._current = work
            try:
                done, _ = await asyncio.wait({work}, timeout=policy.timeout_ms / 1000)
            finally:
                self._current = None
                if not work.done():
                    work.cancel()  # don't wait for it to comply
            if not done:
                error = StepTimeoutError(name, attempt, policy.timeout_ms)
            elif work.cancelled():
                error = self._cancelled_attempt()
            elif (raised := work.exception()) is not None:
                error = raised
            else:
                return self._complete(name, attempt, work.result())
        raise self._classify(name, attempt, policy, error)

    def _cancelled_attempt(self) -> BaseException:
        """Explain a step attempt that was cancelled from outside it."""
        if not self._engine.is_current():
            return AttemptSupersededError(self._engine.run_id)
        requested, reason = self._engine.cancellation_requested()
        if requested:
            return TaskCancellation(reason)
        return asyncio.CancelledError()

    def _complete[T](self, name: str, attempt: int, result: T) -> T:
        try:
            text = serialize_task_value(
                result, f"result of step {name!r} in run {self._engine.run_id!r}"
            )
        except TaskSerializationError as error:
            self._engine.fail_step(name, type(error).__name__, str(error))
            raise
        self._engine.complete_step(name, text)
        self._engine.emit("task:step:completed", {"step": name, "attempt": attempt})
        # The journaled value, so live runs and replays see the same thing.
        return deserialize_task_value(text)

    def _classify(
        self, name: str, attempt: int, policy: ResolvedStepPolicy, error: BaseException
    ) -> BaseException:
        """Decide what a failed step attempt does; return what to raise."""
        if not isinstance(error, Exception):
            return error  # supersession, cancellation, or a real CancelledError
        requested, reason = self._engine.cancellation_requested()
        if requested:
            return TaskCancellation(reason)
        # A condemned isolate can't recover in-process: leave the step running
        # and let the run defer to a fresh isolate or the memory breaker.
        if is_code_update_reset(error) or is_memory_limit_reset(error):
            return error
        # Other platform failures use the step's retries, then defer the run
        # rather than fail it.
        if is_platform_failure(error) and attempt >= policy.retry_limit:
            return error
        if (
            is_non_retryable(error)
            or isinstance(error, TaskSerializationError)
            or attempt >= policy.retry_limit
        ):
            self._engine.fail_step(name, type(error).__name__, str(error))
            return error
        wake_at = now_ms() + retry_delay_ms(policy, attempt)
        self._engine.wait_step(name, wake_at)
        return TaskSuspension(wake_at, "retry")


def _restored_error(row: TaskStepRow) -> Exception:
    return RuntimeError(
        f"{row['error_name'] or 'Error'}: {row['error_message'] or 'Step failed'}"
    )
