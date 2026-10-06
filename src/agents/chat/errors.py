"""Errors raised by the chat layer."""

from ..core.errors import AgentsException

__all__ = ("ChatStreamError",)


class ChatStreamError(AgentsException):
    """A turn's stream reported an error (an ``Error`` chunk), ending the turn.

    ``on_error`` receives it for a turn that failed that way; ``error_text``
    is what the client was sent.
    """

    def __init__(self, error_text: str) -> None:
        super().__init__(error_text)
        self.error_text = error_text
