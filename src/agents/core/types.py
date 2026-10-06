"""Types shared across the SDK."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

__all__ = (
    "Duration",
    "JSONValue",
    "RetryOptions",
    "ShouldRetry",
    "SqlValue",
)

type JSONValue = (
    bool | int | float | str | list[JSONValue] | dict[str, JSONValue] | None
)
"""A value JSON can represent."""

type Duration = timedelta | float
"""A length of time: a ``timedelta``, or a number of seconds."""

type SqlValue = int | float | str | bytes | None
"""A value SQLite accepts as a parameter."""


type ShouldRetry = Callable[[Exception, int], bool]
"""Decides whether to retry: called with the error and the next attempt number."""


@dataclass(slots=True, kw_only=True)
class RetryOptions:
    """How many times to try an operation, and how long to wait between tries.

    The wait before each retry is random, up to
    ``min(2 ** attempt * base_delay, max_delay)`` ("full jitter").

    Parameters
    ----------
    max_attempts
        Total attempts, including the first.
    base_delay
        The backoff's base delay.
    max_delay
        The longest wait between attempts.
    """

    max_attempts: int = 3
    base_delay: Duration = 0.1
    max_delay: Duration = 3.0
