from datetime import UTC, datetime, timedelta, timezone

import pytest

from agents.core.timing import (
    epoch_ms,
    from_epoch_ms,
    to_milliseconds,
    to_seconds,
    to_timedelta,
)


def test_numbers_are_seconds() -> None:
    assert to_timedelta(30) == timedelta(seconds=30)
    assert to_seconds(1.5) == 1.5
    assert to_milliseconds(2) == 2000


def test_timedelta_passes_through() -> None:
    assert to_timedelta(timedelta(minutes=1)) == timedelta(minutes=1)
    assert to_seconds(timedelta(milliseconds=250)) == 0.25


@pytest.mark.parametrize("bad", [True, "5", None])
def test_non_durations_raise_type_error(bad: object) -> None:
    with pytest.raises(TypeError):
        to_seconds(bad)  # ty: ignore[invalid-argument-type]


def test_epoch_ms_round_trip_is_utc() -> None:
    moment = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    assert from_epoch_ms(epoch_ms(moment)) == moment
    assert from_epoch_ms(0).tzinfo is UTC


def test_epoch_ms_accepts_any_timezone() -> None:
    plus_two = datetime(2026, 10, 5, 14, 0, tzinfo=timezone(timedelta(hours=2)))
    assert epoch_ms(plus_two) == epoch_ms(datetime(2026, 10, 5, 12, 0, tzinfo=UTC))


def test_epoch_ms_rejects_naive_datetime() -> None:
    with pytest.raises(TypeError, match="naive"):
        epoch_ms(datetime(2026, 10, 5))
