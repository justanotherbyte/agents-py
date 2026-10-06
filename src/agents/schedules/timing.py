"""When schedules run: parsing ``when`` and intervals, and recurrences.

Port of upstream ``schedules/schedule-timing.ts``.
"""

import math
from datetime import datetime, timedelta

from ..core.timing import epoch_ms, from_epoch_ms, to_seconds
from .cron import parse_cron
from .types import ScheduleTiming

__all__ = (
    "MAX_INTERVAL",
    "is_recurring",
    "next_cron_ms",
    "parse_interval",
    "parse_when",
)

MAX_INTERVAL = timedelta(days=30)
"""The longest allowed gap between interval runs."""


def parse_when(when: datetime | timedelta | float | str, now_ms: int) -> ScheduleTiming:
    """Turn a ``schedule()`` ``when`` into timing.

    A ``datetime`` runs once at that time, a ``timedelta`` or number of
    seconds once after that delay, and a ``str`` on that cron expression.

    Raises
    ------
    TypeError
        If ``when`` is none of those, or a naive ``datetime``.
    ValueError
        If a delay isn't finite, or the cron expression is invalid.
    """
    if isinstance(when, datetime):
        return ScheduleTiming(type="scheduled", time_ms=epoch_ms(when))
    if isinstance(when, str):
        return ScheduleTiming(
            type="cron", time_ms=next_cron_ms(when, now_ms), cron=when
        )
    if isinstance(when, timedelta) or (
        isinstance(when, int | float) and not isinstance(when, bool)
    ):
        seconds = to_seconds(when)
        if not math.isfinite(seconds):
            raise ValueError("A schedule delay must be finite")
        return ScheduleTiming(
            type="delayed",
            time_ms=now_ms + round(seconds * 1000),
            delay_seconds=seconds,
        )
    raise TypeError(
        "when must be a datetime, a timedelta or number of seconds, or a cron "
        f"string, not {type(when).__name__}"
    )


def parse_interval(interval: timedelta | float, now_ms: int) -> ScheduleTiming:
    """Turn a ``schedule_every()`` interval into timing.

    The first run is one interval from now.

    Raises
    ------
    ValueError
        If the interval isn't positive, or is longer than 30 days.
    """
    seconds = to_seconds(interval)
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("A schedule interval must be positive")
    if seconds > MAX_INTERVAL.total_seconds():
        raise ValueError("A schedule interval can't exceed 30 days")
    return ScheduleTiming(
        type="interval",
        time_ms=now_ms + round(seconds * 1000),
        interval_seconds=seconds,
    )


def next_cron_ms(cron: str, now_ms: int) -> int:
    """Return the next time ``cron`` matches after ``now_ms`` (epoch ms).

    Raises
    ------
    InvalidCronExpressionError
        If the expression is invalid or never matches.
    """
    return epoch_ms(parse_cron(cron).next_after(from_epoch_ms(now_ms)))


def is_recurring(timing: ScheduleTiming) -> bool:
    """Return whether the timing repeats (and so deduplicates by default)."""
    return timing.type in ("cron", "interval")
