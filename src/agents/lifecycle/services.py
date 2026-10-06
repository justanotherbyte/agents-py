"""The services Lifecycle grants each installed capability (``self.lifecycle``).

Port of upstream ``LifecycleServices`` (``lifecycle/capability.ts``). A
capability talks to Lifecycle only through its hooks, these services, and
host-specific setters a composition root calls.
"""

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

from .. import _ffi
from ..core.sql import Sql
from .job_queue import LifecycleJobs
from .types import Connection, LifecycleStatus, RouteAddress, RouteEnvelope, WebSocket

if TYPE_CHECKING:
    from workers import Request

    from .lifecycle import Lifecycle

__all__ = ("LifecycleRoutes", "LifecycleServices")


class LifecycleRoutes:
    """Sending messages to capabilities in other Lifecycles (facets have no alarm).

    Parameters
    ----------
    lifecycle
        The owning Lifecycle.
    capability_id
        Messages are addressed to the capability with this id on the target.
    """

    __slots__ = ("_capability_id", "_lifecycle")

    def __init__(self, lifecycle: "Lifecycle", capability_id: str) -> None:
        self._lifecycle = lifecycle
        self._capability_id = capability_id

    @property
    def source(self) -> RouteAddress | None:
        """Return this Lifecycle's address, or ``None`` at the route root."""
        return self._lifecycle._route_source()

    def _envelope(self, payload: Any) -> RouteEnvelope:
        return RouteEnvelope(
            capability=self._capability_id, source=self.source, payload=payload
        )

    async def to_root(self, payload: Any) -> Any:
        """Send ``payload`` to this capability on the root Lifecycle."""
        return await self._lifecycle._route_to_root(self._envelope(payload))

    async def to(self, target: RouteAddress, payload: Any) -> Any:
        """Send ``payload`` to this capability on the Lifecycle at ``target``."""
        return await self._lifecycle._route_to(target, self._envelope(payload))


class LifecycleServices:
    """What a capability may use: identity, storage, sockets, jobs, events.

    Parameters
    ----------
    lifecycle
        The owning Lifecycle.
    capability_id
        The capability these services are scoped to.
    """

    __slots__ = ("_capability_id", "_lifecycle", "jobs", "routes")

    def __init__(self, lifecycle: "Lifecycle", capability_id: str) -> None:
        self._lifecycle = lifecycle
        self._capability_id = capability_id
        self.jobs: LifecycleJobs = lifecycle._jobs_for(capability_id)
        """This capability's view of the shared job queue."""
        self.routes = LifecycleRoutes(lifecycle, capability_id)
        """Messaging to the same capability in other Lifecycles."""

    @property
    def name(self) -> str:
        """The host Durable Object's name."""
        return self._lifecycle.name

    @property
    def class_name(self) -> str:
        """The host class's name."""
        return self._lifecycle.class_name

    @property
    def storage(self) -> Any:
        """The host's ``ctx.storage`` (the Workers SDK wrapper)."""
        return self._lifecycle._ctx.storage

    @property
    def sql(self) -> Sql:
        """Typed SQL over the host's database; each capability owns its tables."""
        return self._lifecycle.sql

    def accept_websocket(self, ws: WebSocket, tags: list[str]) -> None:
        """Accept ``ws`` into hibernation under ``tags``."""
        self._lifecycle._ctx.acceptWebSocket(ws, _ffi.py_to_js(tags))

    def websockets(self, tag: str | None = None) -> Sequence[WebSocket]:
        """Return every hibernated socket on the object, optionally by tag."""
        ctx = self._lifecycle._ctx
        sockets = ctx.getWebSockets() if tag is None else ctx.getWebSockets(tag)
        return list(sockets)

    async def ready(self) -> None:
        """Wait for startup, starting it if needed (returns at once inside it)."""
        await self._lifecycle._ensure_started()

    def status(self) -> LifecycleStatus:
        """Return the startup state."""
        return self._lifecycle._status

    def track_alarm_work(self, work: Any) -> bool:
        """Keep work handed off by a job inside the alarm's memory-limit breaker.

        Returns ``False`` (tracking nothing) outside an alarm invocation.
        """
        return self._lifecycle.track_alarm_work(work)

    async def run_in_host_context[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        connection: Connection | None = None,
        request: "Request | None" = None,
    ) -> T:
        """Run a user callback inside the host context.

        The one way a capability calls user code, so ``get_current_agent()``
        works there. Pass the connection or request it runs on behalf of.
        """
        return await self._lifecycle._run_in_host_boundary(
            fn, connection=connection, request=request
        )

    def emit(self, type: str, payload: Any) -> None:
        """Publish a best-effort telemetry event under this capability's id."""
        self._lifecycle._emit(self._capability_id, type, payload)
