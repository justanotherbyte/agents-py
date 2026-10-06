"""Errors raised by the Streams capability."""

from ..core.errors import AgentsException

__all__ = ("StreamClosedError", "StreamNotFoundError", "StreamSerializationError")


class StreamClosedError(AgentsException):
    """The stream no longer accepts writes: it settled, or was deleted.

    Raised by ``open()`` on a terminal stream and by an append that reaches a
    stream settled or deleted after its writer was created.
    """

    def __init__(self, stream_id: str, detail: str) -> None:
        super().__init__(f"Stream {stream_id!r} is closed: {detail}")
        self.stream_id = stream_id


class StreamNotFoundError(AgentsException):
    """``read()`` targets a stream that was never opened (or was deleted).

    ``status()`` returns ``None`` instead, for existence checks.
    """

    def __init__(self, stream_id: str) -> None:
        super().__init__(
            f"Stream {stream_id!r} does not exist. Open it before reading, or "
            "use status() to check whether it exists."
        )
        self.stream_id = stream_id


class StreamSerializationError(AgentsException):
    """A chunk or metadata value isn't JSON, or is over the size limit."""

    def __init__(self, context: str, detail: str) -> None:
        super().__init__(f"Cannot serialize {context}: {detail}")
