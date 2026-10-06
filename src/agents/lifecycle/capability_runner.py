"""Dispatching Lifecycle phases and events to installed capabilities.

Port of upstream ``lifecycle/capability-runner.ts``. Each loop visits only the
capabilities that override the hook (`implements`): startup reaches every
one, requests and upgrades stop at the first response, socket wakes stop at
the first ``True``, and jobs and routed messages go to one capability by id.
"""

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from .capability import LifecycleCapability, implements
from .types import MemoryLimitContext, RouteContext, WebSocket

if TYPE_CHECKING:
    from workers import Request, Response

__all__ = ("CapabilityRunner",)

_log = logging.getLogger("agents.lifecycle")


class CapabilityRunner:
    """Runs phases for an ordered list of capabilities.

    Parameters
    ----------
    capabilities
        Installed capabilities in dispatch order (catch-alls last). The list
        is read live, so capabilities installed before startup are included.
    """

    __slots__ = ("_capabilities", "_started")

    def __init__(self, capabilities: Sequence[LifecycleCapability]) -> None:
        self._capabilities = capabilities
        self._started = False

    def _implementing(self, hook: str) -> Sequence[LifecycleCapability]:
        return [cap for cap in self._capabilities if implements(cap, hook)]

    def _require_started(self, operation: str) -> None:
        if not self._started:
            raise RuntimeError(
                f"Cannot {operation} before the Durable Object lifecycle has started"
            )

    async def start(self) -> None:
        """Start every capability in order; a failure leaves the runner unstarted."""
        for capability in self._implementing("on_start"):
            await capability.on_start()
        self._started = True

    def reset(self) -> None:
        """Mark the runner unstarted again after a failed startup."""
        self._started = False

    async def request(self, request: "Request") -> "Response | None":
        """Offer an HTTP request; return the first capability's response."""
        self._require_started("handle a request")
        for capability in self._implementing("on_request"):
            response = await capability.on_request(request)
            if response is not None:
                return response
        return None

    async def websocket_upgrade(self, request: "Request") -> "Response | None":
        """Offer a WebSocket upgrade; the responding capability owns the socket."""
        self._require_started("handle a WebSocket upgrade")
        for capability in self._implementing("on_websocket_upgrade"):
            response = await capability.on_websocket_upgrade(request)
            if response is not None:
                return response
        return None

    async def websocket_message(self, ws: WebSocket, message: str | bytes) -> bool:
        """Offer a socket message; return whether a capability consumed it."""
        self._require_started("handle a WebSocket message")
        for capability in self._implementing("on_websocket_message"):
            if await capability.on_websocket_message(ws, message):
                return True
        return False

    async def websocket_close(
        self, ws: WebSocket, code: int, reason: str, was_clean: bool
    ) -> bool:
        """Offer a socket close; return whether a capability consumed it."""
        self._require_started("handle a WebSocket close")
        for capability in self._implementing("on_websocket_close"):
            if await capability.on_websocket_close(ws, code, reason, was_clean):
                return True
        return False

    async def websocket_error(self, ws: WebSocket, error: BaseException) -> bool:
        """Offer a socket error; return whether a capability consumed it."""
        self._require_started("handle a WebSocket error")
        for capability in self._implementing("on_websocket_error"):
            if await capability.on_websocket_error(ws, error):
                return True
        return False

    def find(self, capability_id: str) -> LifecycleCapability | None:
        """Return the installed capability with ``capability_id``, or ``None``."""
        self._require_started("dispatch capability work")
        return next(
            (c for c in self._capabilities if c.capability_id == capability_id), None
        )

    async def route(self, capability_id: str, context: RouteContext) -> Any:
        """Deliver a routed message to one capability.

        Raises
        ------
        RuntimeError
            If no installed capability with that id handles routed messages.
        """
        self._require_started("route a capability message")
        capability = self.find(capability_id)
        if capability is None or not implements(capability, "on_route"):
            raise RuntimeError(
                f"Lifecycle capability {capability_id!r} cannot receive routed messages"
            )
        return await capability.on_route(context)

    async def memory_limit(self, context: MemoryLimitContext) -> None:
        """Offer a memory-limit strike to every capability, best-effort.

        Not gated on startup: the strike may have come from startup itself.
        One capability's failure doesn't stop the next; the isolate is about
        to reset either way.
        """
        for capability in self._implementing("on_memory_limit"):
            try:
                await capability.on_memory_limit(context)
            except Exception:
                _log.exception(
                    "Capability %r memory-limit policy failed", capability.capability_id
                )

    async def dispose(self) -> None:
        """Dispose capabilities in reverse order; one failure doesn't stop the rest."""
        for capability in reversed(self._implementing("dispose")):
            try:
                await capability.dispose()
            except Exception:
                _log.exception(
                    "Capability %r disposal failed", capability.capability_id
                )
