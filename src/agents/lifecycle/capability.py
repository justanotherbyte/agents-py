"""The base class every Lifecycle capability extends.

Port of upstream ``lifecycle/capability.ts`` and the hook contract in
``capability-runner.ts``. Upstream's two layers (the ``DurableObjectCapability``
interface and the ``LifecycleCapability`` base) are merged into one base class
with typed no-op hooks; a hook counts as implemented only when a subclass
overrides it (``.design/lifecycle_capabilities.md`` §9.1).
"""

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from .types import (
    JobContext,
    JobOutcome,
    MemoryLimitContext,
    RouteContext,
    WebSocket,
)

if TYPE_CHECKING:
    from workers import Request, Response

    from .services import LifecycleServices

__all__ = ("CATCH_ALL_HOOKS", "LifecycleCapability", "implements")

CATCH_ALL_HOOKS = ("on_request", "on_websocket_upgrade")
"""The hooks a catch-all capability can claim; at most one catch-all each."""


class LifecycleCapability:
    """A feature installed into a Durable Object's Lifecycle.

    Subclasses pass a stable, unique id to ``__init__`` and override the hooks
    they need; Lifecycle dispatches only to overridden hooks. Hooks run outside
    the host context: a capability calls user code through
    ``self.lifecycle.run_in_host_context``.

    Parameters
    ----------
    capability_id
        Identifies the capability: owns its jobs, receives routed messages,
        names its events. At most one capability per id in a Lifecycle.

    Raises
    ------
    ValueError
        If ``capability_id`` is blank.
    """

    claims: ClassVar[Literal["selective", "catch-all"]] = "selective"
    """``"catch-all"`` claims everything offered to the hooks it implements, so
    Lifecycle always dispatches to it last."""

    def __init__(self, capability_id: str) -> None:
        if not capability_id.strip():
            raise ValueError("Lifecycle capability ids must be non-empty")
        self.capability_id = capability_id
        self._services: LifecycleServices | None = None

    @property
    def lifecycle(self) -> "LifecycleServices":
        """The services Lifecycle grants this capability once installed.

        Raises
        ------
        RuntimeError
            If the capability hasn't been installed with ``Lifecycle.use``.
        """
        if self._services is None:
            raise RuntimeError(
                f"{type(self).__name__} must be installed with Lifecycle.use() "
                "before use"
            )
        return self._services

    async def on_start(self) -> None:
        """Initialize or recover state when the object wakes, before any work."""

    async def on_request(self, request: "Request") -> "Response | None":
        """Return a response to claim an HTTP request, or ``None`` to pass it on."""
        return None

    async def on_websocket_upgrade(self, request: "Request") -> "Response | None":
        """Return a response to claim a WebSocket upgrade and own its socket."""
        return None

    async def on_websocket_message(self, ws: WebSocket, message: str | bytes) -> bool:
        """Return ``True`` to consume a message on a socket this capability owns."""
        return False

    async def on_websocket_close(
        self, ws: WebSocket, code: int, reason: str, was_clean: bool
    ) -> bool:
        """Return ``True`` to consume a close on a socket this capability owns."""
        return False

    async def on_websocket_error(self, ws: WebSocket, error: BaseException) -> bool:
        """Return ``True`` to consume an error on a socket this capability owns."""
        return False

    async def on_job(self, context: JobContext) -> JobOutcome:
        """Run one due job this capability pushed.

        Must be bounded: the alarm loop awaits each job in turn, so long work
        should be started detached, with durable evidence, and ``on_job``
        should return.
        """
        return None

    async def on_job_error(self, context: JobContext, error: Exception) -> JobOutcome:
        """Decide what happens to a job whose retries ran out (default: complete)."""
        return None

    async def on_memory_limit(self, context: MemoryLimitContext) -> None:
        """Apply this capability's policy after an alarm memory-limit strike."""

    async def on_route(self, context: RouteContext) -> Any:
        """Handle a message routed to this capability from another Lifecycle."""
        raise NotImplementedError

    async def dispose(self) -> None:
        """Release in-memory resources when the host is destroyed."""


def implements(capability: LifecycleCapability, hook: str) -> bool:
    """Return whether ``capability``'s class overrides ``hook``.

    Lifecycle dispatches only to overridden hooks, and uses this for the
    catch-all uniqueness check.
    """
    return getattr(type(capability), hook) is not getattr(LifecycleCapability, hook)
