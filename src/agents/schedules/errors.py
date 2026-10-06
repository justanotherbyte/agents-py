"""Exceptions raised by the Scheduler."""

from ..core.errors import AgentsException

__all__ = ("InvalidCronExpressionError",)


class InvalidCronExpressionError(AgentsException, ValueError):
    """A cron expression couldn't be parsed, or never matches.

    Also a ``ValueError``: usually a mistake in code, but catchable when the
    expression comes from user or model input.
    """
