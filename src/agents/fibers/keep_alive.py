"""Keep an object in memory while work runs: the KeepAlive capability.

Port of upstream ``keepAlive`` (``index.ts``) and the facet leases in
``dynamic-agents.ts`` (``.design/fibers_engine.md``). While any lease is held,
a ``keep-alive`` job stays in the Lifecycle queue, due every interval, so the
alarm keeps waking the object before idle eviction. A facet has no alarm: it
takes its lease from the root over the route transport, and the root drops a
facet's leases when it's deleted or starts in a new isolate (the old isolate
can't release them; ``.design/fibers_engine.md`` §6).
"""

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple, override

from ..core.events import Disposable
from ..core.timing import to_seconds
from ..core.types import Duration
from ..lifecycle.capability import LifecycleCapability
from ..lifecycle.types import JobContext, JobOutcome, Reschedule, RouteContext
from .types import KeepAliveRouteMessage

__all__ = ("KeepAlive",)

_log = logging.getLogger("agents.keep_alive")

_JOB_ID = "keep-alive"


class _FacetLease(NamedTuple):
    owner: str
    """The route key of the facet holding the lease."""
    isolate: str
    """The facet isolate that took it."""


class KeepAlive(LifecycleCapability):
    """Reference-counted heartbeats that keep the object from going idle.

    Parameters
    ----------
    interval
        How often the heartbeat alarm fires while a lease is held.
    """

    def __init__(self, *, interval: Duration = 30) -> None:
        super().__init__("keep-alive")
        self._interval = timedelta(seconds=to_seconds(interval))
        self._refs = 0
        # Leases the root holds for its facets (in memory, like upstream), by
        # token.
        self._facet_leases: dict[str, _FacetLease] = {}
        # On a facet: names this isolate in its leases, so the root can tell
        # them from a dead isolate's.
        self._isolate = secrets.token_urlsafe(9)
        self._background: set[asyncio.Task[Any]] = set()

    @property
    def active(self) -> bool:
        """Whether any lease is held here (facet leases included, on the root)."""
        return self._refs > 0

    async def acquire(self) -> Disposable:
        """Take a lease; dispose it when the work is done (idempotent)."""
        routes = self.lifecycle.routes
        if routes.source is not None:
            message: KeepAliveRouteMessage = {
                "type": "acquire",
                "isolate": self._isolate,
            }
            token: str = await routes.to_root(message)
            return Disposable(lambda: self._spawn(self._release_on_root(token)))
        await self._add_ref()
        return Disposable(self._drop_ref)

    async def keep_alive_while[T](self, fn: Callable[[], Awaitable[T]]) -> T:
        """Run ``fn`` holding a lease, releasing it however ``fn`` ends."""
        lease = await self.acquire()
        try:
            return await fn()
        finally:
            lease.dispose()

    @override
    async def on_start(self) -> None:
        """Drop leases left by a previous isolate: the work holding them died.

        Here that's a stale heartbeat; on a facet, the leases the root still
        holds for the facet's previous isolate.
        """
        routes = self.lifecycle.routes
        if routes.source is not None:
            message: KeepAliveRouteMessage = {
                "type": "restarted",
                "isolate": self._isolate,
            }
            await routes.to_root(message)
        if self._refs == 0 and self.lifecycle.jobs.get(_JOB_ID) is not None:
            await self.lifecycle.jobs.cancel(_JOB_ID)

    @override
    async def on_job(self, context: JobContext) -> JobOutcome:
        """Beat again while leases are held; otherwise let the job go."""
        if self._refs > 0:
            return Reschedule(at=self._next_beat())
        return None

    @override
    async def on_route(self, context: RouteContext) -> Any:
        """Hold or release leases on behalf of facets (root only)."""
        message: KeepAliveRouteMessage = context.payload
        match message["type"]:
            case "release":
                if self._facet_leases.pop(message["token"], None) is not None:
                    self._drop_ref()
                return True
            case "acquire" | "restarted":
                if context.source is None:
                    raise ValueError(
                        "A routed keep-alive message must come from a facet"
                    )
                owner = context.source.key
                if message["type"] == "restarted":
                    # Only this facet's own leases: its sub-agents run in
                    # isolates of their own.
                    self._drop_leases(
                        lambda lease: (
                            lease.owner == owner and lease.isolate != message["isolate"]
                        )
                    )
                    return True
                token = f"{owner}:{secrets.token_urlsafe(9)}"
                self._facet_leases[token] = _FacetLease(owner, message["isolate"])
                await self._add_ref()
                return token
        raise ValueError(f"Unknown routed keep-alive message {message!r}")

    @override
    async def dispose(self) -> None:
        """Forget every lease (the object is being destroyed)."""
        self._refs = 0
        self._facet_leases.clear()

    async def _cleanup_route_prefix(self, prefix: str) -> None:
        """Drop the leases of a deleted facet subtree (internal).

        Deleting a facet ends its isolate, so the work holding those leases
        can't release them itself (upstream keeps them; fibers_engine.md §6).
        """
        self._drop_leases(
            lambda lease: lease.owner == prefix or lease.owner.startswith(f"{prefix}/")
        )

    def _drop_leases(self, matches: Callable[[_FacetLease], bool]) -> None:
        dropped = [t for t, lease in self._facet_leases.items() if matches(lease)]
        for token in dropped:
            del self._facet_leases[token]
            self._drop_ref()

    async def _add_ref(self) -> None:
        self._refs += 1
        if self._refs == 1:
            await self.lifecycle.jobs.push(
                id=_JOB_ID, fn="keepAlive", time=self._next_beat()
            )

    def _drop_ref(self) -> None:
        self._refs = max(0, self._refs - 1)
        if self._refs == 0:
            # Disposal is synchronous; drop the heartbeat so a short lease
            # doesn't leave one armed.
            self._spawn(self._cancel_if_idle())

    async def _cancel_if_idle(self) -> None:
        # A lease taken since the release keeps the heartbeat.
        if self._refs == 0:
            await self.lifecycle.jobs.cancel(_JOB_ID)

    async def _release_on_root(self, token: str) -> None:
        message: KeepAliveRouteMessage = {"type": "release", "token": token}
        await self.lifecycle.routes.to_root(message)

    def _next_beat(self) -> datetime:
        return datetime.now(UTC) + self._interval

    def _spawn(self, work: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(work)
        self._background.add(task)
        task.add_done_callback(self._finished)

    def _finished(self, task: asyncio.Task[Any]) -> None:
        self._background.discard(task)
        if not task.cancelled() and task.exception() is not None:
            _log.error("Releasing a keep-alive lease failed", exc_info=task.exception())
