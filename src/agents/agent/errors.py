"""Exceptions raised by the Agent class."""

from ..core.errors import AgentsException

__all__ = ("ReadonlyConnectionError",)


class ReadonlyConnectionError(AgentsException):
    """``set_state`` was called while serving a readonly connection."""

    def __init__(self) -> None:
        super().__init__("Connection is readonly")
