"""Cron expressions: parsing and the next matching time.

Port of the npm package ``cron-schedule`` 6.0 (MIT), which upstream uses
(``parseCronExpression(...).getNextDate(...)``), so expressions mean the same
in both SDKs. Linux cron syntax plus an optional leading seconds field: lists,
ranges, steps, month and weekday names, weekday ``7`` as Sunday, and the
``@yearly`` / ``@monthly`` / ``@weekly`` / ``@daily`` / ``@hourly`` /
``@minutely`` nicknames. When both day of month and weekday are restricted, a
day matching either counts. Times are UTC (the Workers runtime's local time).
"""

import calendar
import re
from datetime import UTC, datetime

from .errors import InvalidCronExpressionError
from .types import CronField

__all__ = ("CronExpression", "parse_cron")

_SECONDS = CronField(minimum=0, maximum=59)
_MINUTES = CronField(minimum=0, maximum=59)
_HOURS = CronField(minimum=0, maximum=23)
_DAYS = CronField(minimum=1, maximum=31)
_MONTHS = CronField(
    minimum=1,
    maximum=12,
    aliases={
        name: str(number)
        for number, name in enumerate(
            (
                "jan",
                "feb",
                "mar",
                "apr",
                "may",
                "jun",
                "jul",
                "aug",
                "sep",
                "oct",
                "nov",
                "dec",
            ),
            start=1,
        )
    },
)
_WEEKDAYS = CronField(
    minimum=0,
    maximum=7,
    aliases={
        name: str(number)
        for number, name in enumerate(
            ("mon", "tue", "wed", "thu", "fri", "sat", "sun"), start=1
        )
    },
)

_NICKNAMES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@hourly": "0 * * * *",
    "@minutely": "* * * * *",
}

# start-end or *, with an optional /step.
_RANGE = re.compile(r"^(([0-9a-zA-Z]+)-([0-9a-zA-Z]+)|\*)(/([0-9]+))?$")


class CronExpression:
    """A parsed cron expression; `next_after` finds the next matching time.

    Use `parse_cron` to create one.
    """

    __slots__ = ("_days", "_hours", "_minutes", "_months", "_seconds", "_weekdays")

    def __init__(
        self,
        *,
        seconds: set[int],
        minutes: set[int],
        hours: set[int],
        days: set[int],
        months: set[int],
        weekdays: set[int],
    ) -> None:
        self._seconds = sorted(seconds)
        self._minutes = sorted(minutes)
        self._hours = sorted(hours)
        self._days = sorted(days)
        self._months = sorted(months)  # 1-12
        self._weekdays = sorted(weekdays)  # 0-6, Sunday = 0

    def next_after(self, start: datetime) -> datetime:
        """Return the first matching time strictly after ``start`` (whole seconds).

        Raises
        ------
        TypeError
            If ``start`` is naive.
        InvalidCronExpressionError
            If nothing matches within five years (e.g. ``0 0 30 2 *``).
        """
        if start.tzinfo is None:
            raise TypeError("next_after() needs a timezone-aware datetime")
        start = start.astimezone(UTC)
        year = start.year
        start_index = next(
            (i for i, month in enumerate(self._months) if month >= start.month), None
        )
        if start_index is None:
            start_index = 0
            year += 1
        count = len(self._months)
        # Every month over five years covers a whole leap-year cycle.
        for offset in range(count * 5):
            candidate_year = year + (start_index + offset) // count
            month = self._months[(start_index + offset) % count]
            is_start_month = candidate_year == start.year and month == start.month
            day = self._allowed_day(
                candidate_year, month, start.day if is_start_month else 1
            )
            is_start_day = is_start_month and day == start.day
            if day is not None and is_start_day:
                time = self._allowed_time(start.hour, start.minute, start.second)
                if time is not None:
                    return datetime(candidate_year, month, day, *time, tzinfo=UTC)
                day = self._allowed_day(candidate_year, month, day + 1)
                is_start_day = False
            if day is not None and not is_start_day:
                return datetime(
                    candidate_year,
                    month,
                    day,
                    self._hours[0],
                    self._minutes[0],
                    self._seconds[0],
                    tzinfo=UTC,
                )
        raise InvalidCronExpressionError("No matching time within five years")

    def _allowed_time(
        self, hour: int, minute: int, second: int
    ) -> tuple[int, int, int] | None:
        """Return the first allowed time after ``hour:minute:second`` that day."""
        next_hour = _first_at_least(self._hours, hour)
        if next_hour is None:
            return None
        if next_hour != hour:
            return next_hour, self._minutes[0], self._seconds[0]
        next_minute = _first_at_least(self._minutes, minute)
        if next_minute == minute:
            next_second = _first_at_least(self._seconds, second + 1)
            if next_second is not None:
                return hour, minute, next_second
            next_minute = _first_at_least(self._minutes, minute + 1)
        if next_minute is not None:
            return hour, next_minute, self._seconds[0]
        next_hour = _first_at_least(self._hours, hour + 1)
        if next_hour is not None:
            return next_hour, self._minutes[0], self._seconds[0]
        return None

    def _allowed_day(self, year: int, month: int, start_day: int) -> int | None:
        """Return the first day from ``start_day`` matching the day or weekday."""
        days_in_month = calendar.monthrange(year, month)[1]
        days_restricted = len(self._days) != 31
        weekdays_restricted = len(self._weekdays) != 7
        if not days_restricted and not weekdays_restricted:
            return start_day if start_day <= days_in_month else None

        by_day = None
        if days_restricted:
            by_day = _first_at_least(self._days, start_day)
            if by_day is not None and by_day > days_in_month:
                by_day = None

        by_weekday = None
        if weekdays_restricted and start_day <= days_in_month:
            # Python's weekday() has Monday = 0; cron's has Sunday = 0.
            start_weekday = (calendar.weekday(year, month, start_day) + 1) % 7
            nearest = _first_at_least(self._weekdays, start_weekday)
            if nearest is None:
                nearest = self._weekdays[0]
            by_weekday = start_day + (nearest - start_weekday) % 7
            if by_weekday > days_in_month:
                by_weekday = None

        if by_day is not None and by_weekday is not None:
            return min(by_day, by_weekday)
        return by_day if by_day is not None else by_weekday


def parse_cron(expression: str) -> CronExpression:
    """Parse a cron expression (5 fields, or 6 with seconds first).

    Raises
    ------
    InvalidCronExpressionError
        If the expression isn't valid cron.
    """
    expression = _NICKNAMES.get(expression.lower(), expression)
    fields = expression.split()
    if len(fields) not in (5, 6):
        raise InvalidCronExpressionError(
            f"Invalid cron expression {expression!r}: expected 5 or 6 fields"
        )
    if len(fields) == 5:
        fields = ["0", *fields]
    seconds, minutes, hours, days, months, weekdays = fields
    return CronExpression(
        seconds=_parse_field(seconds, _SECONDS),
        minutes=_parse_field(minutes, _MINUTES),
        hours=_parse_field(hours, _HOURS),
        days=_parse_field(days, _DAYS),
        months=_parse_field(months, _MONTHS),
        weekdays={day % 7 for day in _parse_field(weekdays, _WEEKDAYS)},
    )


def _parse_field(text: str, field: CronField) -> set[int]:
    if text == "*":
        return set(range(field.minimum, field.maximum + 1))
    if "," in text:
        return {
            value for part in text.split(",") for value in _parse_field(part, field)
        }
    match = _RANGE.match(text)
    if match is None:
        return {_parse_value(text, text, field)}
    if match.group(1) == "*":
        start, end = field.minimum, field.maximum
    else:
        start = _parse_value(match.group(2), text, field)
        end = _parse_value(match.group(3), text, field)
    # Sunday parses as 7, but also starts a range as 0 (sun-sat); not sun-sun.
    if field is _WEEKDAYS and start == 7 and end != 7:
        start = 0
    if start > end:
        raise InvalidCronExpressionError(
            f"Failed to parse {text!r}: invalid range ({start} to {end})"
        )
    step = int(match.group(5)) if match.group(5) is not None else 1
    if step < 1:
        raise InvalidCronExpressionError(f"Failed to parse {text!r}: step must be >= 1")
    return set(range(start, end + 1, step))


def _parse_value(value: str, text: str, field: CronField) -> int:
    value = field.aliases.get(value.lower(), value)
    if not value.isdecimal():
        raise InvalidCronExpressionError(
            f"Failed to parse {text!r}: {value!r} isn't a number"
        )
    number = int(value)
    if not field.minimum <= number <= field.maximum:
        raise InvalidCronExpressionError(
            f"Failed to parse {text!r}: {number} is outside "
            f"{field.minimum}-{field.maximum}"
        )
    return number


def _first_at_least(values: list[int], minimum: int) -> int | None:
    return next((value for value in values if value >= minimum), None)
