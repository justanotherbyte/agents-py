"""Conversions between durations, timestamps, and epoch milliseconds.

The SDK's API takes durations as ``timedelta | float`` (seconds) and returns
timestamps as timezone-aware UTC ``datetime``; storage and the wire keep
upstream's epoch milliseconds (``.design/utilities.md`` §1).

Never call these at import time: in production ``Date.now()`` (and so
``time.time()``) is ``0`` while module code runs.
"""

import time
from datetime import UTC, datetime, timedelta

from .types import Duration

__all__ = (
    "epoch_ms",
    "from_epoch_ms",
    "now_ms",
    "to_milliseconds",
    "to_seconds",
    "to_timedelta",
)


def to_timedelta(duration: Duration) -> timedelta:
    """Return ``duration`` as a ``timedelta`` (a number is seconds)."""
    if isinstance(duration, timedelta):
        return duration
    return timedelta(seconds=_number(duration))


def to_seconds(duration: Duration) -> float:
    """Return ``duration`` in seconds."""
    if isinstance(duration, timedelta):
        return duration.total_seconds()
    return float(_number(duration))


def to_milliseconds(duration: Duration) -> float:
    """Return ``duration`` in milliseconds."""
    return to_seconds(duration) * 1000


def _number(duration: float) -> float:
    if isinstance(duration, bool) or not isinstance(duration, int | float):
        raise TypeError(
            f"a duration must be a timedelta or a number of seconds, "
            f"not {type(duration).__name__}"
        )
    return duration


def epoch_ms(moment: datetime) -> int:
    """Return a timezone-aware ``datetime`` as integer epoch milliseconds.

    Raises
    ------
    TypeError
        If ``moment`` is naive.
    """
    if moment.tzinfo is None:
        raise TypeError("naive datetime; use a timezone-aware datetime")
    return int(moment.timestamp() * 1000)


def from_epoch_ms(ms: float) -> datetime:
    """Return epoch milliseconds as a timezone-aware UTC ``datetime``."""
    return datetime.fromtimestamp(ms / 1000, UTC)


def now_ms() -> int:
    """Return the current time as integer epoch milliseconds."""
    return int(time.time() * 1000)
