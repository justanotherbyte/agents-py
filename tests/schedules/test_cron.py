import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from agents.schedules import InvalidCronExpressionError
from agents.schedules.cron import parse_cron

# [expression, start, next] triples produced by the npm package cron-schedule
# 6.0 (upstream's parser) with TZ=UTC.
ORACLE = json.loads((Path(__file__).parent / "cron_schedule_oracle.json").read_text())


def _parse(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


@pytest.mark.parametrize(("expression", "start", "expected"), ORACLE)
def test_matches_cron_schedule(expression: str, start: str, expected: str) -> None:
    assert parse_cron(expression).next_after(_parse(start)) == _parse(expected)


@pytest.mark.parametrize(
    "expression",
    [
        "* * * *",
        "* * * * * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "5-1 * * * *",
        "*/0 * * * *",
        "x * * * *",
        "5x * * * *",
    ],
)
def test_invalid_expressions(expression: str) -> None:
    with pytest.raises(InvalidCronExpressionError):
        parse_cron(expression)


def test_invalid_expressions_are_value_errors() -> None:
    with pytest.raises(ValueError):
        parse_cron("nope")


def test_an_impossible_date_never_matches() -> None:
    with pytest.raises(InvalidCronExpressionError, match="five years"):
        parse_cron("0 0 30 2 *").next_after(datetime(2026, 1, 1, tzinfo=UTC))


def test_naive_start_is_rejected() -> None:
    with pytest.raises(TypeError):
        parse_cron("* * * * *").next_after(datetime(2026, 1, 1))
