"""Retrying an async operation with jittered exponential backoff.

Port of upstream ``retries.ts`` (``tryN``, ``jitterBackoff``,
``validateRetryOptions``) and ``Agent.retry`` (``.design/utilities.md`` §7).
Retries are in memory only; durable retries are schedules, queues, and Tasks.
"""

import asyncio
import inspect
import math
import random
from collections.abc import Awaitable, Callable
from datetime import timedelta

from .timing import to_seconds, to_timedelta
from .types import Duration, RetryOptions, ShouldRetry

__all__ = ("backoff_delay", "retry", "run_with_retries", "validate_retry_options")


async def retry[T](
    fn: Callable[[int], Awaitable[T]],
    *,
    max_attempts: int = 3,
    base_delay: Duration = 0.1,
    max_delay: Duration = 3.0,
    should_retry: ShouldRetry | None = None,
) -> T:
    """Call ``fn`` until it succeeds, waiting a random backoff between attempts.

    Parameters
    ----------
    fn
        Called once per attempt with the 1-based attempt number; must return
        a fresh awaitable each time (an ``async def`` function or a
        ``functools.partial`` of one, not a coroutine).
    max_attempts
        Total attempts, including the first.
    base_delay
        The backoff's base delay, a ``timedelta`` or seconds.
    max_delay
        The longest wait between attempts, a ``timedelta`` or seconds.
    should_retry
        Called with the error and the next attempt number; returning
        ``False`` stops retrying and re-raises that error. By default every
        ``Exception`` is retried.

    Returns
    -------
    T
        The first successful result.

    Raises
    ------
    Exception
        The last attempt's error, once attempts run out or ``should_retry``
        returns ``False``. Other ``BaseException`` subclasses, such as
        ``asyncio.CancelledError``, propagate immediately.
    TypeError
        If ``fn`` isn't callable, or an option has the wrong type.
    ValueError
        If an option is out of range.
    """
    options = RetryOptions(
        max_attempts=max_attempts, base_delay=base_delay, max_delay=max_delay
    )
    validate_retry_options(options)
    return await run_with_retries(fn, options, should_retry=should_retry)


def validate_retry_options(options: RetryOptions) -> None:
    """Check retry options eagerly, so bad values fail where they're given.

    Raises
    ------
    TypeError
        If ``max_attempts`` isn't an ``int``, or a delay isn't a duration.
    ValueError
        If ``max_attempts`` is below 1, a delay isn't positive and finite, or
        ``base_delay`` exceeds ``max_delay``.
    """
    attempts = options.max_attempts
    if isinstance(attempts, bool) or not isinstance(attempts, int):
        raise TypeError(f"max_attempts must be an int, not {type(attempts).__name__}")
    if attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    base = to_seconds(options.base_delay)
    maximum = to_seconds(options.max_delay)
    for name, seconds in (("base_delay", base), ("max_delay", maximum)):
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError(f"{name} must be positive and finite")
    if base > maximum:
        raise ValueError("base_delay must be <= max_delay")


async def run_with_retries[T](
    fn: Callable[[int], Awaitable[T]],
    options: RetryOptions,
    *,
    should_retry: ShouldRetry | None = None,
) -> T:
    """Run ``fn`` with already-validated retry options.

    The engine behind `retry`, also used by the SDK's own dispatch retries
    (upstream ``tryN``). See `retry` for the parameters and behavior.
    """
    if inspect.iscoroutine(fn) or not callable(fn):
        raise TypeError("fn must be a function returning an awaitable, not a coroutine")

    attempt = 1
    while True:
        try:
            return await fn(attempt)
        except Exception as error:
            next_attempt = attempt + 1
            if next_attempt > options.max_attempts or (
                should_retry is not None and not should_retry(error, next_attempt)
            ):
                raise
            await asyncio.sleep(backoff_delay(attempt, options).total_seconds())
            attempt = next_attempt


def backoff_delay(attempt: int, options: RetryOptions) -> timedelta:
    """Return a random wait before retrying after ``attempt`` ("full jitter").

    Parameters
    ----------
    attempt
        The 1-based attempt that just failed.
    options
        Supplies ``base_delay`` and ``max_delay``.

    Returns
    -------
    timedelta
        A uniformly random delay in ``[0, min(2 ** attempt * base, max))``.
    """
    upper = min(
        to_timedelta(options.base_delay) * 2**attempt,
        to_timedelta(options.max_delay),
    )
    return upper * random.random()
