"""Exceptions raised by the WebSockets capability and connections."""

from ..core.errors import AgentsException

__all__ = ("ConnectionStateTooLargeError", "DuplicateConnectionIdError")


class DuplicateConnectionIdError(AgentsException):
    """More than one live connection has the id `get_connection` was asked for.

    Connection ids come from the client (``_pk``), so two sockets can share
    one; use ``get_connections(tag=id)`` to reach all of them.

    Parameters
    ----------
    id
        The shared connection id.
    """

    def __init__(self, id: str) -> None:
        super().__init__(
            f"More than one connection has id {id!r}; use get_connections(tag=id)"
        )
        self.id = id


class ConnectionStateTooLargeError(AgentsException):
    """A connection's state doesn't fit in its socket attachment (16 KiB).

    The limit is shared with the SDK's own connection metadata.
    """
