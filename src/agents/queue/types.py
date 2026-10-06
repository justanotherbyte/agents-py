"""Types for the Queue capability."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, TypedDict

from ..core.types import JSONValue, RetryOptions

__all__ = (
    "ByIdMessage",
    "CancelAllMessage",
    "DispatchMessage",
    "ListMessage",
    "PushMessage",
    "QueueCallback",
    "QueueErrorHandler",
    "QueueItem",
    "QueueJobPayload",
    "QueueRouteMessage",
    "WireQueueItem",
)


@dataclass(slots=True, kw_only=True)
class QueueItem:
    """One durable item waiting in a Queue.

    Parameters
    ----------
    id
        The item's id.
    callback
        The name of the callback the item runs.
    payload
        The data passed to the callback.
    created_at
        When the item was first pushed.
    retry
        The retry policy for the callback.
    """

    id: str
    callback: str
    payload: JSONValue
    created_at: datetime
    retry: RetryOptions | None = None


type QueueCallback = Callable[[Any, QueueItem], Awaitable[Any]]
"""A queue callback, called as ``callback(payload, item)``."""

type QueueErrorHandler = Callable[[Exception], Awaitable[None]]
"""Observes an item's terminal failure (after its last attempt)."""


class QueueJobPayload(TypedDict):
    """What a Queue job carries: the item payload and the Lifecycle owning it.

    ``owner_path`` / ``owner_path_key`` are a facet's route address (``None``
    for items pushed on the root itself).
    """

    payload: JSONValue
    owner_path: str | None
    owner_path_key: str | None


class WireQueueItem(TypedDict):
    """A `QueueItem` as JSON, carried in routed messages."""

    id: str
    callback: str
    payload: JSONValue
    created_at_ms: int
    retry: str | None
    """The retry policy as the job table stores it (JSON text)."""


class PushMessage(TypedDict):
    """A facet pushing an item onto the root's queue."""

    type: Literal["push"]
    callback: str
    payload: JSONValue
    id: str | None
    retry: str | None


class ByIdMessage(TypedDict):
    """A facet reading or cancelling one of its items."""

    type: Literal["get", "cancel"]
    id: str


class ListMessage(TypedDict):
    """A facet listing its items."""

    type: Literal["list"]
    callback: str | None


class CancelAllMessage(TypedDict):
    """A facet cancelling its items."""

    type: Literal["cancel_all"]
    callback: str | None


class DispatchMessage(TypedDict):
    """The root handing a due item to the facet that owns it."""

    type: Literal["dispatch"]
    item: WireQueueItem


type QueueRouteMessage = (
    PushMessage | ByIdMessage | ListMessage | CancelAllMessage | DispatchMessage
)
"""A Queue operation routed between Lifecycles."""
