"""The Lifecycle: a Durable Object's single entry point and dispatcher.

Port of upstream ``lifecycle/durable-object-lifecycle.ts``. It replaces the
runtime entry points (``fetch``, ``alarm``, ``webSocket*``), runs startup once
per wake, owns the job queue and the one physical alarm, and dispatches events
to installed capabilities (``.design/lifecycle_capabilities.md``).

The host delegates its entry points explicitly instead of having them patched
(``.design/lifecycle_capabilities.md`` §9)::

    class Room(DurableObject):
        def __init__(self, ctx, env):
            super().__init__(ctx, env)
            self.lifecycle = Lifecycle(self).use(Scheduler(target=self))

        async def fetch(self, request):
            return await self.lifecycle.fetch(request)

        async def alarm(self, *args):
            await self.lifecycle.alarm()
"""

import asyncio
import json
import logging
import re
import traceback
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from functools import partial
from typing import TYPE_CHECKING, Any

from workers import Response

from .. import _ffi
from ..core.sql import Sql
from ..core.timing import now_ms
from .capability import CATCH_ALL_HOOKS, LifecycleCapability, implements
from .capability_runner import CapabilityRunner
from .host_context import run_in_host_context, run_without_host_context
from .job_driver import DEFAULT_MAX_ALARM_MEMORY_LIMIT_STRIKES, JobDriver
from .job_queue import HOST_JOB_OWNER, JobQueue, LifecycleJobs
from .services import LifecycleServices
from .types import (
    Connection,
    EventSink,
    HostInvoker,
    JobContext,
    JobDispatch,
    JobOutcome,
    LifecycleEvent,
    LifecycleStatus,
    MemoryLimitContext,
    RouteAddress,
    RouteContext,
    RouteEnvelope,
    RouteTransport,
    WebSocket,
)

if TYPE_CHECKING:
    from workers import Request

__all__ = ("Lifecycle",)

_log = logging.getLogger("agents.lifecycle")
_events_log = logging.getLogger("agents.events")

_CLOSING = 2
_CLOSED = 3
_BENIGN_TEARDOWN = re.compile(
    r"Network connection lost|WebSocket peer disconnected", re.I
)

# Lifecycles whose startup the current async context is running inside, so a
# call back into the host from startup returns instead of waiting on itself.
_startup_scope: ContextVar[frozenset[int]] = ContextVar(
    "agents_startup_scope", default=frozenset()
)


class Lifecycle:
    """Coordinates one Durable Object's entry points, startup, jobs, and alarm.

    Parameters
    ----------
    host
        The Durable Object; Lifecycle reads ``host.ctx``, and host hooks run
        in its host context.
    hooks
        The object whose optional hooks Lifecycle calls: ``on_start``,
        ``on_request``, ``on_alarm``, ``on_job``, and
        ``_on_alarm_memory_limit`` (default: the host). ``Agent`` passes an
        object that applies its error handling around the user's hooks.
    max_alarm_memory_limit_strikes
        Consecutive alarm memory-limit resets tolerated before the circuit
        breaker seals recovery work.
    expose_error_details
        Send an unhandled error's traceback to the client in the 500
        response (or the upgrade's error frame). Off by default: tracebacks
        can reveal code, paths, and data. Meant for local development.
    """

    def __init__(
        self,
        host: Any,
        *,
        hooks: object | None = None,
        max_alarm_memory_limit_strikes: int = DEFAULT_MAX_ALARM_MEMORY_LIMIT_STRIKES,
        expose_error_details: bool = False,
    ) -> None:
        self._host = host
        self._hooks = hooks if hooks is not None else host
        self._expose_error_details = expose_error_details
        self._ctx = host.ctx
        self.class_name: str = type(host).__name__
        """The host class's name."""
        self.sql = Sql(self._ctx.storage)
        """Typed SQL over the host's database."""
        self._capabilities: list[LifecycleCapability] = []
        self._runner = CapabilityRunner(self._capabilities)
        self._queue = JobQueue(self.sql)
        self._status: LifecycleStatus = "zero"
        self._startup: asyncio.Task[None] | None = None
        self._capabilities_locked = False
        self._alarms_disabled = False
        self._rearm_lock = asyncio.Lock()
        self._rearm_requested_during_start = False
        self._pending_events: list[LifecycleEvent] = []
        self._background: set[asyncio.Task[None]] = set()
        self._name_override: str | None = None
        self._event_sink: EventSink | None = None
        self._host_invoker: HostInvoker | None = None
        self._route_transport: RouteTransport | None = None
        self._driver = JobDriver(
            queue=self._queue,
            storage=self._ctx.storage,
            disabled=lambda: self._alarms_disabled,
            resolve_dispatch=self._resolve_job_dispatch,
            max_memory_limit_strikes=max_alarm_memory_limit_strikes,
            on_memory_limit=self._on_memory_limit,
            emit=partial(self._emit, "lifecycle"),
            rearm=self.rearm_alarm,
            reset=partial(_ffi.abort_without_alarm_retry, self._ctx),
        )

    # Identity
    @property
    def name(self) -> str:
        """The name the Durable Object was addressed by (``ctx.id.name``).

        Raises
        ------
        RuntimeError
            If the object wasn't addressed by name (``idFromName`` /
            ``getByName``), so ``ctx.id.name`` is unset.
        """
        if self._name_override is not None:
            return self._name_override
        name = getattr(self._ctx.id, "name", None)
        if isinstance(name, str):
            return name
        raise RuntimeError(
            f"{self.class_name} could not determine its Durable Object name. "
            "Address it with idFromName() or getByName(); newUniqueId(), "
            "idFromString(), and names over 1,024 bytes don't expose ctx.id.name."
        )

    # Installing capabilities
    def use(self, capability: LifecycleCapability) -> "Lifecycle":
        """Install a capability before startup.

        Capabilities dispatch in installation order, except that catch-alls
        always come last. At most one catch-all per hook.

        Returns
        -------
        Lifecycle
            This Lifecycle, for chaining.

        Raises
        ------
        RuntimeError
            If startup has begun, a capability with the same id is installed,
            or a second catch-all claims the same hook.
        """
        if self._capabilities_locked:
            raise RuntimeError("Lifecycle capabilities must be added before startup")
        capability_id = capability.capability_id
        if any(c.capability_id == capability_id for c in self._capabilities):
            raise RuntimeError(
                f"Lifecycle capability {capability_id!r} is already installed"
            )
        if capability.claims == "catch-all":
            self._check_catch_all(capability)
            self._capabilities.append(capability)
        else:
            first_catch_all = next(
                (
                    i
                    for i, c in enumerate(self._capabilities)
                    if c.claims == "catch-all"
                ),
                len(self._capabilities),
            )
            self._capabilities.insert(first_catch_all, capability)
        capability._services = LifecycleServices(self, capability.capability_id)
        return self

    def _check_catch_all(self, capability: LifecycleCapability) -> None:
        for hook in CATCH_ALL_HOOKS:
            if not implements(capability, hook):
                continue
            rival = next(
                (
                    c
                    for c in self._capabilities
                    if c.claims == "catch-all" and implements(c, hook)
                ),
                None,
            )
            if rival is not None:
                raise RuntimeError(
                    f"Lifecycle already has a catch-all for {hook} "
                    f"({rival.capability_id!r}); a second one could never be reached"
                )

    # Startup
    async def start(self) -> None:
        """Start capabilities and the host, once per wake.

        Entry points call this automatically; native RPC methods that need
        startup call it themselves (RPC bypasses ``fetch``). Concurrent
        callers share one startup; a call from inside startup returns at once.
        """
        await self._ensure_started()

    def is_started(self) -> bool:
        """Return whether startup has finished."""
        return self._status == "started"

    async def _ensure_started(self) -> None:
        if self._status == "started":
            return
        if self._startup is None:
            task = asyncio.ensure_future(self._run_startup())
            self._startup = task
            task.add_done_callback(self._forget_startup)
            await asyncio.shield(task)
            return
        if id(self) in _startup_scope.get():
            return
        await asyncio.shield(self._startup)

    def _forget_startup(self, task: asyncio.Task[None]) -> None:
        if self._startup is task:
            self._startup = None

    async def _run_startup(self) -> None:
        self.name  # noqa: B018  (fail before host startup if the object is unnamed)
        self._capabilities_locked = True
        failure: list[BaseException] = []
        await _ffi.block_concurrency_while(
            self._ctx, partial(self._startup_body, failure)
        )
        # Re-raised outside blockConcurrencyWhile: raising inside it would
        # leave the object unusable, and a later event must be able to retry.
        if failure:
            self._rearm_requested_during_start = False
            self._pending_events.clear()
            raise failure[0]
        self._deliver_pending_events()
        if self._rearm_requested_during_start:
            self._rearm_requested_during_start = False
            await self.rearm_alarm()

    async def _startup_body(self, failure: list[BaseException]) -> None:
        token = _startup_scope.set(_startup_scope.get() | {id(self)})
        self._status = "starting"
        try:
            await run_without_host_context(self._runner.start)
            on_start = getattr(self._hooks, "on_start", None)
            if on_start is not None:
                await run_in_host_context(self._host, on_start)
            self._status = "started"
        except BaseException as error:  # re-raised by _run_startup
            self._status = "zero"
            self._runner.reset()
            failure.append(error)
        finally:
            _startup_scope.reset(token)

    # Entry points
    async def fetch(self, request: "Request") -> Response:
        """Handle an HTTP request or WebSocket upgrade for the object.

        A plain request goes to the capabilities, then the host's
        ``on_request``, then 404. An upgrade goes to the capabilities only.
        """
        upgrade = is_upgrade_request(request)
        try:
            await self._ensure_started()
            if upgrade:
                return await self._fetch_upgrade(request)
            return await self._fetch_request(request)
        except Exception:
            _log.exception("Error in %s fetch", self._describe())
            detail = traceback.format_exc() if self._expose_error_details else None
            if upgrade:
                return _ffi.websocket_error_response(detail or "Internal error")
            return Response(detail or "Internal Server Error", status=500)

    async def _fetch_request(self, request: "Request") -> Response:
        response = await run_without_host_context(
            partial(self._runner.request, request)
        )
        if response is not None:
            return response
        on_request = getattr(self._hooks, "on_request", None)
        if on_request is None:
            return Response("Not implemented", status=404)
        return await run_in_host_context(
            self._host, partial(on_request, request), request=request
        )

    async def _fetch_upgrade(self, request: "Request") -> Response:
        response = await run_without_host_context(
            partial(self._runner.websocket_upgrade, request)
        )
        if response is not None:
            return response
        return Response(
            "WebSocket upgrades are not enabled on this Durable Object. Install a "
            "capability that claims them (e.g. WebSockets).",
            status=404,
        )

    async def websocket_message(self, ws: WebSocket, message: Any) -> None:
        """Dispatch a hibernated socket's message to the capability that owns it."""
        text_or_bytes = message if isinstance(message, str) else _ffi.js_to_py(message)
        try:
            await self._ensure_started()
            await run_without_host_context(
                partial(self._runner.websocket_message, ws, text_or_bytes)
            )
        except Exception:
            _log.exception("Error in %s webSocketMessage", self._describe())

    async def websocket_close(
        self, ws: WebSocket, code: int, reason: str, was_clean: bool
    ) -> None:
        """Dispatch a hibernated socket's close to the capability that owns it."""
        try:
            await self._ensure_started()
            await run_without_host_context(
                partial(self._runner.websocket_close, ws, code, reason, was_clean)
            )
        except Exception:
            _log.exception("Error in %s webSocketClose", self._describe())

    async def websocket_error(self, ws: WebSocket, error: BaseException) -> None:
        """Dispatch a hibernated socket's error to the capability that owns it.

        Transport teardown errors on a socket that is already closing are the
        connection going away, not application errors, and are dropped.
        """
        if _is_benign_teardown(ws, error):
            return
        try:
            await self._ensure_started()
            await run_without_host_context(
                partial(self._runner.websocket_error, ws, error)
            )
        except Exception:
            _log.exception("Error in %s webSocketError", self._describe())

    async def alarm(self) -> None:
        """Run one alarm: due jobs, then the host's ``on_alarm``, then re-arm."""
        await self._driver.run_alarm(self._ensure_started, self._run_host_alarm)

    async def _run_host_alarm(self) -> None:
        on_alarm = getattr(self._hooks, "on_alarm", None)
        if on_alarm is not None:
            await run_in_host_context(self._host, on_alarm)

    def _describe(self) -> str:
        name = getattr(self._ctx.id, "name", None) or "<unnamed>"
        return f"{self.class_name}:{name}"

    # Jobs and the alarm
    @property
    def jobs(self) -> LifecycleJobs:
        """The host's own view of the job queue; jobs go to ``host.on_job``."""
        return self._jobs_for(HOST_JOB_OWNER)

    def _jobs_for(self, owner: str) -> LifecycleJobs:
        return LifecycleJobs(self._queue, owner, self.rearm_alarm)

    async def rearm_alarm(self) -> None:
        """Set the physical alarm from queue state (deferred while starting).

        Serialized, so an earlier computation can't overwrite a later one.
        """
        if self._alarms_disabled:
            return
        if self._status == "starting":
            self._rearm_requested_during_start = True
            return
        async with self._rearm_lock:
            if self._alarms_disabled:
                return
            time_ms = self._queue.next_alarm_time(now_ms())
            if time_ms is None:
                await self._ctx.storage.deleteAlarm()
            else:
                await self._ctx.storage.setAlarm(time_ms)

    def track_alarm_work(self, work: Any) -> bool:
        """Keep work a job handed off inside the current alarm's breaker domain."""
        return self._driver.track_alarm_work(work)

    async def _resolve_job_dispatch(self, owner: str) -> JobDispatch | None:
        # Host jobs run in the host context; capability jobs outside it. There
        # is no host on_job_error: a failed host job completes, and the host
        # re-derives its jobs from durable state.
        if owner == HOST_JOB_OWNER:
            on_job = getattr(self._hooks, "on_job", None)
            if on_job is None:
                return None
            return JobDispatch(on_job=partial(self._run_host_job, on_job))
        capability = self._runner.find(owner)
        if capability is None or not implements(capability, "on_job"):
            return None
        on_job_error = (
            partial(_outside_host_context, capability.on_job_error)
            if implements(capability, "on_job_error")
            else None
        )
        return JobDispatch(
            on_job=partial(_outside_host_context, capability.on_job),
            on_job_error=on_job_error,
        )

    async def _run_host_job(
        self, on_job: Callable[[JobContext], Awaitable[JobOutcome]], context: JobContext
    ) -> JobOutcome:
        return await run_in_host_context(self._host, partial(on_job, context))

    async def _on_memory_limit(self, context: MemoryLimitContext) -> None:
        # Capabilities first (each best-effort), then the host, so a failed
        # capability policy can't silence the host's.
        await self._runner.memory_limit(context)
        hook = getattr(self._hooks, "_on_alarm_memory_limit", None)
        if hook is not None:
            await run_in_host_context(self._host, partial(hook, context))

    # Teardown
    async def dispose(self) -> None:
        """Dispose capabilities in reverse installation order."""
        await run_without_host_context(self._runner.dispose)

    async def disable_alarms(self) -> None:
        """Permanently stop alarms and delete the physical alarm (host teardown)."""
        self._alarms_disabled = True
        async with self._rearm_lock:
            await self._ctx.storage.deleteAlarm()

    # Host context, events, routing
    async def _run_in_host_boundary[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        connection: Connection | None,
        request: "Request | None",
    ) -> T:
        if self._host_invoker is not None:
            return await self._host_invoker(fn, connection=connection, request=request)
        return await run_in_host_context(
            self._host, fn, connection=connection, request=request
        )

    def _emit(self, source: str, type: str, payload: Any) -> None:
        if not source.strip() or not type.strip():
            raise ValueError("Lifecycle events require a non-empty source and type")
        event = LifecycleEvent(source=source, type=type, payload=payload)
        if self._status != "started":
            self._pending_events.append(event)
            return
        self._publish(event)

    def _deliver_pending_events(self) -> None:
        events, self._pending_events = self._pending_events, []
        for event in events:
            self._publish(event)

    def _publish(self, event: LifecycleEvent) -> None:
        # Telemetry never fails the work that emitted it.
        sink = self._event_sink
        if sink is None:
            self._log_event(event)
            return
        try:
            pending = sink(event)
        except Exception:
            _log.exception(
                "Lifecycle event sink failed for %s:%s", event.source, event.type
            )
            return
        if pending is not None:
            task = asyncio.ensure_future(self._await_sink(pending, event))
            self._background.add(task)
            task.add_done_callback(self._background.discard)

    async def _await_sink(
        self, pending: Awaitable[None], event: LifecycleEvent
    ) -> None:
        try:
            await pending
        except Exception:
            _log.exception(
                "Lifecycle event sink failed for %s:%s", event.source, event.type
            )

    def _log_event(self, event: LifecycleEvent) -> None:
        if not _events_log.isEnabledFor(logging.DEBUG):
            return
        _events_log.debug(
            json.dumps(
                {
                    "type": event.type,
                    "source": event.source,
                    "agent": self.class_name,
                    "name": self.name,
                    "payload": event.payload,
                    "timestamp": now_ms(),
                },
                default=str,
            )
        )

    def _route_source(self) -> RouteAddress | None:
        transport = self._route_transport
        return transport.source if transport is not None else None

    async def _route_to_root(self, envelope: RouteEnvelope) -> Any:
        if self._route_transport is None:
            return await self.route(envelope)
        return await self._route_transport.to_root(envelope)

    async def _route_to(self, target: RouteAddress, envelope: RouteEnvelope) -> Any:
        if self._route_transport is None:
            raise RuntimeError("Lifecycle has no transport for routed capabilities")
        return await self._route_transport.to(target, envelope)

    async def route(self, envelope: RouteEnvelope) -> Any:
        """Deliver a routed message to the addressed capability (internal)."""
        await self._ensure_started()
        context = RouteContext(source=envelope.source, payload=envelope.payload)
        return await run_without_host_context(
            partial(self._runner.route, envelope.capability, context)
        )

    # Composition-root setters (used by Agent)
    def _set_event_sink(self, sink: EventSink) -> None:
        self._event_sink = sink

    def _set_host_invoker(self, invoker: HostInvoker) -> None:
        self._host_invoker = invoker

    def _set_route_transport(self, transport: RouteTransport) -> None:
        self._route_transport = transport

    def _set_name(self, name: str) -> None:
        # Facets report their logical sub-agent name (agent_api.md §1.5).
        self._name_override = name


def is_upgrade_request(request: "Request") -> bool:
    """Return whether ``request`` is a WebSocket upgrade."""
    upgrade = request.headers.get("Upgrade")
    return upgrade is not None and upgrade.lower() == "websocket"


def _is_benign_teardown(ws: WebSocket, error: BaseException) -> bool:
    # Port of upstream transport-errors.ts isBenignTeardownError.
    if getattr(ws, "readyState", None) not in (_CLOSING, _CLOSED):
        return False
    if getattr(error, "retryable", False) is True:
        return True
    return bool(_BENIGN_TEARDOWN.search(str(error)))


async def _outside_host_context[*Args, T](
    hook: Callable[[*Args], Awaitable[T]], *args: *Args
) -> T:
    return await run_without_host_context(partial(hook, *args))
