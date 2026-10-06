"""Exceptions raised inside the Lifecycle."""

from ..core.errors import AgentsException
from .types import JobRow

__all__ = ("AttributedPlatformFailure",)


class AttributedPlatformFailure(AgentsException):
    """Carries the job a platform failure escaped from, out to the alarm boundary.

    Internal: raised by the job driver and caught by its own ``run_alarm``.
    Attribution can't live on the driver, because overlapping alarm
    invocations dispatch jobs concurrently and would overwrite each other's
    row.

    Parameters
    ----------
    row
        The job row whose dispatch failed.
    cause
        The platform failure.
    """

    def __init__(self, row: JobRow, cause: Exception) -> None:
        super().__init__(str(cause))
        self.row = row
        self.cause = cause
