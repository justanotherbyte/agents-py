"""The base exception, and exceptions raised by ``core`` helpers.

Every exception the SDK raises on purpose subclasses `AgentsException`, so
``except AgentsException`` catches all of them. Misuse (a wrong argument type,
a call at the wrong time) raises built-in ``TypeError`` / ``ValueError`` /
``RuntimeError`` instead (``.design/utilities.md`` §1).
"""

__all__ = ("AgentsException", "SqlError")


class AgentsException(Exception):  # noqa: N818  (the decided public name)
    """Base class for every exception the SDK raises on purpose."""


class SqlError(AgentsException):
    """A SQL statement failed.

    Port of upstream ``sql-error.ts``.

    Parameters
    ----------
    query
        The statement that failed.
    message
        SQLite's error message.
    """

    def __init__(self, query: str, message: str) -> None:
        super().__init__(f"SQL query failed: {message}")
        self.query = query
