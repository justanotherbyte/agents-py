"""Types for fibers and keep-alive (``.design/fibers_api.md`` §2.2)."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, TypedDict

from ..core.types import JSONValue

__all__ = (
    "FiberAborted",
    "FiberCompleted",
    "FiberContext",
    "FiberErrored",
    "FiberInspection",
    "FiberInterrupted",
    "FiberLedgerRow",
    "FiberRecoveryContext",
    "FiberRecoveryHandler",
    "FiberRecoveryResult",
    "FiberRouteMessage",
    "FiberRunRow",
    "FiberStatus",
    "InternalFiberRecoveryHandler",
    "KeepAliveRouteMessage",
    "StartFiberResult",
)

type FiberStatus = Literal[
    "pending", "running", "completed", "aborted", "interrupted", "error"
]
"""A managed fiber's status. ``pending`` and ``running`` are live; the rest
are terminal (``interrupted``: the isolate died mid-run and recovery
found it)."""


@dataclass(slots=True, kw_only=True)
class FiberContext:
    """What a fiber body receives.

    Parameters
    ----------
    id
        The fiber's id (the managed fiber's ``fiber_id``).
    stash
        Save a checkpoint (JSON-serializable), replacing the previous one.
        It's written before ``stash`` returns, and recovery receives it.
    """

    id: str
    stash: Callable[[Any], None]


@dataclass(slots=True, kw_only=True)
class FiberInspection:
    """A snapshot of one managed fiber's record."""

    fiber_id: str
    name: str
    status: FiberStatus
    created_at: datetime
    idempotency_key: str | None = None
    snapshot: Any = None
    error: str | None = None
    metadata: dict[str, JSONValue] | None = None
    started_at: datetime | None = None
    settled_at: datetime | None = None


@dataclass(slots=True, kw_only=True)
class StartFiberResult(FiberInspection):
    """The result of `start_fiber`.

    ``accepted`` is ``False`` when an existing fiber matched the
    ``fiber_id`` or ``idempotency_key`` (not an error).
    """

    accepted: bool


@dataclass(slots=True, kw_only=True)
class FiberRecoveryContext:
    """An interrupted fiber, as ``on_fiber_recovered`` sees it.

    Parameters
    ----------
    id
        The fiber's id.
    name
        The name it was started with.
    snapshot
        The last ``stash()``, or ``None``.
    created_at
        When the fiber started.
    recovery_reason
        Why it's being recovered (always ``"interrupted"``).
    status
        Managed fibers only: the record's status (``"interrupted"``).
    idempotency_key
        Managed fibers only.
    metadata
        Managed fibers only.
    """

    id: str
    name: str
    snapshot: Any
    created_at: datetime
    recovery_reason: Literal["interrupted"] = "interrupted"
    status: FiberStatus | None = None
    idempotency_key: str | None = None
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True)
class FiberCompleted:
    """Recovery result: mark the managed fiber ``completed``."""

    snapshot: Any = None
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True)
class FiberErrored:
    """Recovery result: mark the managed fiber ``error``."""

    error: str | None = None
    snapshot: Any = None


@dataclass(slots=True, kw_only=True)
class FiberAborted:
    """Recovery result: mark the managed fiber ``aborted``."""

    reason: str | None = None
    snapshot: Any = None


@dataclass(slots=True, kw_only=True)
class FiberInterrupted:
    """Recovery result: leave the managed fiber ``interrupted``, with a reason."""

    reason: str | None = None
    snapshot: Any = None


type FiberRecoveryResult = (
    FiberCompleted | FiberErrored | FiberAborted | FiberInterrupted
)
"""How a recovery hook settles a managed fiber; ``None`` leaves it ``interrupted``."""

type FiberRecoveryHandler = Callable[
    [FiberRecoveryContext], Awaitable[FiberRecoveryResult | None]
]
"""The user's recovery hook (``on_fiber_recovered``)."""

type InternalFiberRecoveryHandler = Callable[[FiberRecoveryContext], Awaitable[bool]]
"""A framework recovery hook that runs first; ``True`` means it handled the fiber."""


class FiberRunRow(TypedDict):
    """A raw ``cf_agents_runs`` row (``.design/sql_schemas.md`` §12.1)."""

    id: str
    name: str
    snapshot: str | None
    created_at: int
    completed_at: int | None
    outcome: Literal["completed", "error", "aborted"] | None
    error_message: str | None


class FiberLedgerRow(TypedDict):
    """A raw ``cf_agents_fibers`` row (``.design/sql_schemas.md`` §12.2)."""

    fiber_id: str
    idempotency_key: str | None
    name: str
    status: FiberStatus
    snapshot: str | None
    metadata_json: str | None
    error_message: str | None
    created_at: int
    started_at: int | None
    completed_at: int | None


class _AcquireMessage(TypedDict):
    type: Literal["acquire"]
    isolate: str


class _ReleaseMessage(TypedDict):
    type: Literal["release"]
    token: str


class _RestartedMessage(TypedDict):
    type: Literal["restarted"]
    isolate: str


type KeepAliveRouteMessage = _AcquireMessage | _ReleaseMessage | _RestartedMessage
"""A facet asking the root for (or returning) a keep-alive lease, or telling it
that the facet started in a new isolate (``isolate`` names the facet's)."""


class _RegisterRunMessage(TypedDict):
    type: Literal["register_run", "unregister_run"]
    run_id: str


class _CheckRunsMessage(TypedDict):
    type: Literal["check_runs"]


type FiberRouteMessage = _RegisterRunMessage | _CheckRunsMessage
"""A facet indexing its fiber on the root, or the root asking it to recover."""
