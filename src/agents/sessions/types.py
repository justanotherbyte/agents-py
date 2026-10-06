"""Types for the Sessions capability (``.design/sessions_api.md`` §3)."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, NotRequired, TypedDict

from ..core.types import JSONValue

__all__ = (
    "AppendEvent",
    "AppendResult",
    "ClearEvent",
    "DeleteEvent",
    "RecentHistoryResult",
    "SessionChangeEvent",
    "SessionChangeListener",
    "SessionMessage",
    "SessionRowStat",
    "Source",
    "UpdateEvent",
)


class SessionMessage(TypedDict):
    """A message in AI SDK ``UIMessage`` wire form (JSON values throughout).

    Sessions reads ``parts`` only to sanitize them and estimate tokens, so
    part types it doesn't know are stored as they are.
    """

    id: str
    role: str
    parts: list[dict[str, JSONValue]]
    metadata: NotRequired[JSONValue]


type Source = Literal["server", "client"]
"""Who a write came from; ``"client"`` writes lose the reserved metadata keys."""


@dataclass(slots=True, kw_only=True, frozen=True)
class AppendResult:
    """The result of an append or upsert.

    ``inserted`` is ``False`` when the id already existed (nothing was
    written); ``message`` is the stored (sanitized) form.
    """

    inserted: bool
    message: SessionMessage


@dataclass(slots=True, kw_only=True, frozen=True)
class RecentHistoryResult:
    """The newest messages that fit a byte budget, oldest first.

    ``messages`` always includes the leaf. ``truncated`` means older
    messages were left out; ``total_content_bytes`` is the stored size of the
    whole path.
    """

    messages: list[SessionMessage]
    truncated: bool
    total_content_bytes: int


@dataclass(slots=True, kw_only=True, frozen=True)
class SessionRowStat:
    """One stored message's size and token estimate, without its content."""

    id: str
    role: str
    bytes: int
    """The message row plus its continuation rows, in UTF-8 bytes."""
    token_estimate: int
    """Stamped when the message was written."""


@dataclass(slots=True, kw_only=True, frozen=True)
class AppendEvent:
    """A message was appended (``inserted``) or its id already existed."""

    type: Literal["append"] = "append"
    session_id: str
    message: SessionMessage
    inserted: bool


@dataclass(slots=True, kw_only=True, frozen=True)
class UpdateEvent:
    """A stored message changed."""

    type: Literal["update"] = "update"
    session_id: str
    message: SessionMessage


@dataclass(slots=True, kw_only=True, frozen=True)
class DeleteEvent:
    """Messages were deleted (their children now follow their parent)."""

    type: Literal["delete"] = "delete"
    session_id: str
    message_ids: list[str]


@dataclass(slots=True, kw_only=True, frozen=True)
class ClearEvent:
    """Every message in the session was deleted."""

    type: Literal["clear"] = "clear"
    session_id: str


type SessionChangeEvent = AppendEvent | UpdateEvent | DeleteEvent | ClearEvent
"""What the change feed delivers, after the write has committed."""

type SessionChangeListener = Callable[[SessionChangeEvent], Awaitable[None] | None]
"""A change-feed listener, sync or async."""
