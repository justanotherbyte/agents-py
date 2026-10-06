import asyncio
from datetime import timedelta

import pytest

from agents.core import RetryOptions, retry
from agents.core.retry import backoff_delay, validate_retry_options


class Flaky:
    """Fails ``failures`` times, then returns the attempt number."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.attempts: list[int] = []

    async def __call__(self, attempt: int) -> int:
        self.attempts.append(attempt)
        if len(self.attempts) <= self.failures:
            raise RuntimeError(f"fail {attempt}")
        return attempt


FAST_BASE = 0.001
FAST_MAX = 0.002


def test_succeeds_after_failures_with_attempt_numbers() -> None:
    fn = Flaky(failures=2)
    assert (
        asyncio.run(retry(fn, max_attempts=3, base_delay=FAST_BASE, max_delay=FAST_MAX))
        == 3
    )
    assert fn.attempts == [1, 2, 3]


def test_raises_last_error_when_attempts_run_out() -> None:
    fn = Flaky(failures=5)
    with pytest.raises(RuntimeError, match="fail 2"):
        asyncio.run(retry(fn, max_attempts=2, base_delay=FAST_BASE, max_delay=FAST_MAX))
    assert fn.attempts == [1, 2]


def test_should_retry_false_stops_immediately() -> None:
    fn = Flaky(failures=5)
    seen: list[tuple[str, int]] = []

    def should_retry(error: Exception, next_attempt: int) -> bool:
        seen.append((str(error), next_attempt))
        return False

    with pytest.raises(RuntimeError, match="fail 1"):
        asyncio.run(
            retry(
                fn,
                max_attempts=5,
                should_retry=should_retry,
                base_delay=FAST_BASE,
                max_delay=FAST_MAX,
            )
        )
    assert seen == [("fail 1", 2)]


def test_cancellation_is_not_retried() -> None:
    attempts: list[int] = []

    async def cancelled(attempt: int) -> None:
        attempts.append(attempt)
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            retry(cancelled, max_attempts=5, base_delay=FAST_BASE, max_delay=FAST_MAX)
        )
    assert attempts == [1]


def test_coroutine_instead_of_function_is_type_error() -> None:
    async def body(attempt: int) -> int:
        return attempt

    coroutine = body(1)
    with pytest.raises(TypeError, match="not a coroutine"):
        asyncio.run(retry(coroutine, base_delay=FAST_BASE, max_delay=FAST_MAX))  # ty: ignore[invalid-argument-type]
    coroutine.close()


@pytest.mark.parametrize(
    ("options", "error"),
    [
        (RetryOptions(max_attempts=0), ValueError),
        (RetryOptions(max_attempts=2.5), TypeError),  # ty: ignore[invalid-argument-type]
        (RetryOptions(max_attempts=True), TypeError),
        (RetryOptions(base_delay=0), ValueError),
        (RetryOptions(max_delay=-1), ValueError),
        (RetryOptions(base_delay=5, max_delay=1), ValueError),
        (RetryOptions(base_delay=float("inf")), ValueError),
    ],
)
def test_invalid_options(options: RetryOptions, error: type[Exception]) -> None:
    with pytest.raises(error):
        validate_retry_options(options)


def test_backoff_is_bounded_by_full_jitter_window() -> None:
    options = RetryOptions(base_delay=0.1, max_delay=3)
    for attempt, upper in [(1, 0.2), (2, 0.4), (10, 3.0)]:
        for _ in range(50):
            delay = backoff_delay(attempt, options)
            assert timedelta(0) <= delay < timedelta(seconds=upper)
