"""The Agent class: a Durable Object built from Lifecycle capabilities.

Port of upstream ``index.ts`` (``Agent``). ``Agent`` installs sub-agents,
Scheduler, Queue, State, WebSockets, and Tasks on its Lifecycle, drives the
connect sequence, answers ``@callable`` methods, forwards ``/sub/`` requests
and sockets to sub-agents, and applies its error policy around the user's
hooks (``.design/agent_api.md``, ``.design/subagents_engine.md``).
"""

import asyncio
import functools
import inspect
import json
import logging
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from datetime import UTC, datetime, timedelta
from functools import partial
from types import FunctionType
from typing import TYPE_CHECKING, Any, ClassVar, Generic, cast

from typing_extensions import TypeVar
from workers import DurableObject, Response

from .. import _ffi
from ..core.encoding import to_json
from ..core.events import Disposable
from ..core.naming import camel_case_to_kebab_case
from ..core.sql import Sql
from ..core.types import Duration, JSONValue, RetryOptions
from ..dynamic_agents.api import DynamicAgents
from ..dynamic_agents.connections import (
    OUTER_URL_FLAG,
    VirtualConnection,
    apply_forwarded_state,
)
from ..dynamic_agents.dynamic_agents import (
    OUTER_URL_HEADER,
    SubAgentsEngine,
    as_message,
)
from ..dynamic_agents.paths import parse_sub_agent_path, path_from_json
from ..dynamic_agents.stubs import AgentStub, PathStub
from ..dynamic_agents.types import (
    AgentPathStep,
    AgentRoute,
    ForwardedConnection,
    WireEnvelope,
    WireRouteAddress,
)
from ..fibers.fibers import Fibers, with_fiber_stash
from ..fibers.fibers import stash as stash_in_fiber
from ..fibers.keep_alive import KeepAlive
from ..fibers.types import (
    FiberContext,
    FiberInspection,
    FiberRecoveryContext,
    FiberRecoveryResult,
    FiberStatus,
    StartFiberResult,
)
from ..lifecycle.capability import LifecycleCapability
from ..lifecycle.host_context import (
    call_in_host_context,
    current_host_context,
    run_in_host_context,
)
from ..lifecycle.lifecycle import Lifecycle
from ..lifecycle.types import LifecycleEvent, MemoryLimitContext
from ..observability.observability import LoggingObservability
from ..observability.types import Observability, ObservabilityEvent
from ..queue.queue import Queue
from ..queue.types import QueueItem
from ..schedules.scheduler import Scheduler
from ..schedules.types import Schedule, ScheduleType
from ..state.state import State
from ..state.types import StateSource
from ..tasks.tasks import Tasks
from ..websockets.connection import Connection
from ..websockets.rpc import RpcDispatcher, callable_methods, is_rpc_request
from ..websockets.types import (
    CallableMetadata,
    ConnectionContext,
    RpcRequest,
    WebSocketHandlers,
)
from ..websockets.websockets import WebSockets, _parse_frame, _prepare_tags, _send
from .errors import ReadonlyConnectionError
from .types import AgentOptions

if TYPE_CHECKING:
    from workers import Request

__all__ = ("Agent",)

_log = logging.getLogger("agents.agent")

S = TypeVar("S", bound=Mapping[str, Any], default=dict[str, Any])

_DESTROY_PENDING_KEY = "cf_agents_destroy_pending"
# MCP is out of scope, but clients expect this frame on connect (wire §3).
_MCP_SERVERS_FRAME = to_json(
    {
        "type": "cf_agent_mcp_servers",
        "mcp": {"servers": {}, "tools": [], "prompts": [], "resources": []},
    }
)


# Generic comes first: the Workers SDK's DurableObject.__init_subclass__
# doesn't chain to super(), so with Generic second its __init_subclass__
# (which sets __parameters__) never runs and Agent[...] fails. Generic's
# chains on to the SDK's, which still wraps every subclass.
class Agent(Generic[S], DurableObject):  # noqa: UP046  (see above; 3.12 has no default)
    """A Durable Object with state, client connections, RPC, and a queue.

    Subclass it, override the ``on_*`` hooks you need, and mark methods
    clients may call with ``@callable``. Extra capabilities are installed in
    ``__init__`` with `use`.

    Class attributes
    ----------------
    initial_state
        The state seeded the first time nothing is stored (each agent gets
        its own copy).
    options
        Per-class settings (`AgentOptions`).
    observability
        Where events go (default: JSON lines on ``agents.events`` at
        ``DEBUG``); ``None`` disables them.
    """

    _sdk_names: ClassVar[frozenset[str]]

    initial_state: S | None = None
    options: AgentOptions = AgentOptions()
    observability: Observability | None = LoggingObservability()

    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        options = type(self).options
        self._destroyed = False
        self._background: set[asyncio.Task[None]] = set()
        self._lifecycle = Lifecycle(
            self,
            hooks=_AgentHooks(self),
            max_alarm_memory_limit_strikes=options.max_alarm_memory_limit_strikes,
            expose_error_details=options.expose_error_details,
        )
        self._lifecycle._set_event_sink(self._forward_event)
        # Installed first: a facet's identity is restored before the other
        # capabilities start, since they route through it.
        self._sub_agents = SubAgentsEngine(self)
        self._lifecycle._set_route_transport(self._sub_agents.transport)
        self._dynamic_agents = DynamicAgents(self._sub_agents)
        self._rpc = RpcDispatcher(self, self._emit)
        self._state: State[S] = _AgentState(
            self,
            validate_state_change=self._validate_state_change,
            on_changed=self._state_changed,
        )
        handlers = _AgentConnectionHandlers(self)
        self._websockets = WebSockets(
            handlers=WebSocketHandlers(
                on_connect=handlers.on_connect,
                on_message=handlers.on_message,
                on_close=handlers.on_close,
                on_error=handlers.on_error,
            ),
            # Agent sends the connect frames and applies state frames itself.
            protocol=False,
            state=self._state,
            connection_tags=self.get_connection_tags,
        )
        self._scheduler = Scheduler(
            target=self,
            retry=options.retry,
            hung_schedule_timeout=options.hung_schedule_timeout,
            on_error=self._callback_failed,
        )
        self._queue = Queue(
            target=self, retry=options.retry, on_error=self._callback_failed
        )
        self.tasks = Tasks(target=self, on_error=self._callback_failed)
        """Durable, replayable background work (``@task`` methods and more)."""
        # A facet's own Lifecycle never sees the root's alarm; this is how its
        # memory-limit hook hears about strikes on its task runs.
        self.tasks._set_routed_memory_limit_handler(self._routed_memory_limit)
        self._keep_alive = KeepAlive(interval=options.keep_alive_interval)
        self._fibers = Fibers(
            self._keep_alive,
            on_recovered=self.on_fiber_recovered,
            internal_recovery=self._handle_internal_fiber_recovery,
            hook_timeout=options.fiber_recovery_hook_timeout,
            scan_deadline=options.fiber_recovery_scan_deadline,
            max_age=options.fiber_recovery_max_age,
            housekeeping_interval=options.keep_alive_interval,
        )
        for capability in (
            self._sub_agents,
            self._scheduler,
            self._queue,
            self._state,
            self._websockets,
            self.tasks,
            self._keep_alive,
            self._fibers,
        ):
            self._lifecycle.use(capability)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Wrap a subclass's public methods so they run as the current agent."""
        super().__init_subclass__(**kwargs)
        if cls.__module__.partition(".")[0] == __name__.partition(".")[0]:
            # An SDK subclass (AIChatAgent): its names are SDK hooks as well.
            cls._sdk_names = cls._sdk_names | frozenset(vars(cls))
            return
        for name, value in list(vars(cls).items()):
            if name.startswith("_") or name in cls._sdk_names:
                continue
            if isinstance(value, FunctionType):
                setattr(cls, name, _in_agent_context(value))

    # Identity

    @property
    def name(self) -> str:
        """The instance name (``idFromName`` / ``getByName``)."""
        return self._lifecycle.name

    @property
    def parent_path(self) -> Sequence[AgentPathStep]:
        """The agents above this one, root first (empty on a top-level agent)."""
        return tuple(self._sub_agents.parent_path)

    @property
    def self_path(self) -> Sequence[AgentPathStep]:
        """The path from the root to this agent, root first."""
        return tuple(self._sub_agents.self_path)

    @property
    def dynamic_agents(self) -> DynamicAgents:
        """The agent's sub-agents (facets): ``get``, ``abort``, ``delete``, ..."""
        return self._dynamic_agents

    async def parent_agent(self, cls: type) -> Any:
        """Return a stub for this sub-agent's direct parent.

        Raises
        ------
        RuntimeError
            If this agent isn't a sub-agent.
        TypeError
            If ``cls`` isn't the parent's class.
        """
        parent_path = self._sub_agents.parent_path
        if not parent_path:
            raise RuntimeError("parent_agent() is only available on a sub-agent")
        parent = parent_path[-1]
        if cls.__name__ != parent.class_name:
            raise TypeError(
                f"This sub-agent's parent is a {parent.class_name}, "
                f"not a {cls.__name__}"
            )
        if len(parent_path) == 1:
            return AgentStub(
                _ffi.namespace_stub(self.ctx, parent.class_name, parent.name)
            )
        root = parent_path[0]
        return PathStub(
            _ffi.namespace_stub(self.ctx, root.class_name, root.name), parent_path
        )

    @property
    def lifecycle(self) -> Lifecycle:
        """The Lifecycle the agent's capabilities are installed on."""
        return self._lifecycle

    @property
    def session_affinity(self) -> str:
        """A stable, unique key for this instance (e.g. for Workers AI)."""
        return str(self.ctx.id)

    @property
    def sql(self) -> Sql:
        """Typed SQL over the agent's database."""
        return self._lifecycle.sql

    def use[C: LifecycleCapability](self, capability: C) -> C:
        """Install a capability (before startup) and return it."""
        self._lifecycle.use(capability)
        return capability

    # Hooks (override these)

    async def on_start(self) -> None:
        """Run once per wake, after capabilities start and before any event."""

    async def on_request(self, request: "Request") -> Response:
        """Handle an HTTP request no capability claimed (default: 404)."""
        return Response("Not implemented", status=404)

    async def on_connect(self, connection: Connection, ctx: ConnectionContext) -> None:
        """Handle a new connection (after its protocol frames were sent)."""

    async def on_message(self, connection: Connection, message: str | bytes) -> None:
        """Handle a frame the SDK didn't consume."""

    async def on_close(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None:
        """Handle a client closing its connection."""

    async def on_error(self, connection: Connection | None, error: Exception) -> None:
        """Handle an error from a hook or a connection (default: re-raise).

        ``connection`` is set only for a connection's own error event.
        Returning normally handles the error: a failed request gets a 500, a
        failed connect or message leaves the connection open. Some errors
        are reported here without being handled (``on_start``,
        ``on_state_changed``, queued callbacks); raising then changes
        nothing.
        """
        raise error

    async def on_before_sub_agent(
        self, request: "Request", child: AgentRoute
    ) -> "Request | Response | None":
        """Gate a request to a sub-agent (``/sub/{class}/{name}/...``).

        Return a ``Response`` to answer it instead (e.g. 404), a ``Request``
        to forward that instead (its headers and body; the sub-agent's path
        stays the tail), or ``None`` to forward it. Default: allow.
        """
        return None

    async def on_alarm(self) -> None:
        """Run on every alarm, after due jobs (rarely needed: use the queue)."""

    async def on_fiber_recovered(
        self, ctx: FiberRecoveryContext
    ) -> FiberRecoveryResult | None:
        """Recover a fiber whose isolate died mid-run (default: log a warning).

        Runs on the next wake, before ``on_start``. ``ctx.snapshot`` is the
        fiber's last ``stash()``. For a managed fiber, return a result to
        settle its record; ``None`` leaves it ``interrupted``. Raising keeps a
        plain fiber for a later retry (until ``fiber_recovery_max_age``) and
        marks a managed one ``error``.
        """
        _log.warning(
            "Fiber %r (%s) was interrupted; override on_fiber_recovered to recover it",
            ctx.name,
            ctx.id,
        )
        return None

    async def get_connection_tags(
        self, connection: Connection, ctx: ConnectionContext
    ) -> list[str]:
        """Return the tags to accept a new connection under (at most 9)."""
        return []

    async def should_connection_be_readonly(
        self, connection: Connection, ctx: ConnectionContext
    ) -> bool:
        """Decide whether a new connection may change the state."""
        return False

    async def should_send_protocol_messages(
        self, connection: Connection, ctx: ConnectionContext
    ) -> bool:
        """Decide whether a new connection gets protocol frames at all."""
        return True

    def validate_state_change(self, next_state: S, source: StateSource) -> None:
        """Raise to reject a state change (synchronous; default: accept)."""

    async def on_state_changed(self, state: S, source: StateSource) -> None:
        """Run after a state change is saved and broadcast."""

    # State

    @property
    def state(self) -> S | None:
        """The current state (treat it as read-only; change it with `set_state`)."""
        return self._state.get()

    def set_state(self, state: S) -> None:
        """Replace the state: validate, save, broadcast, then ``on_state_changed``.

        Raises
        ------
        ReadonlyConnectionError
            If called while serving a readonly connection.
        Exception
            Whatever ``validate_state_change`` raises, rejecting the change.
        """
        current = current_host_context()
        if (
            current is not None
            and current.host is self
            and isinstance(current.connection, Connection)
            and current.connection.readonly
        ):
            raise ReadonlyConnectionError
        self._state.set(state, "server")

    def _validate_state_change(self, next_state: S, source: StateSource) -> None:
        self.validate_state_change(next_state, source)

    def _state_changed(self, state: S, source: StateSource) -> None:
        self._broadcast_state(source if isinstance(source, Connection) else None)
        self._emit("state:update", {})
        connection = source if isinstance(source, Connection) else None
        self._spawn(
            run_in_host_context(
                self,
                partial(self._run_on_state_changed, state, source),
                connection=connection,
            )
        )

    def _broadcast_state(self, exclude: Connection | None) -> None:
        current = self._state.get()
        if current is None:
            return
        text = to_json({"type": "cf_agent_state", "state": current})
        connecting = self._websockets._connecting
        for connection in self.get_connections():
            if connection == exclude or connection in connecting:
                continue
            if connection.protocol_enabled:
                _send(connection, text)

    async def _run_on_state_changed(self, state: S, source: StateSource) -> None:
        try:
            await self.on_state_changed(state, source)
        except Exception as error:
            _log.exception("on_state_changed failed")
            await self._report_error(None, error)

    # Connections

    def get_connections(self, tag: str | None = None) -> Iterator[Connection]:
        """Yield the open connections, optionally only those with ``tag``."""
        if self._sub_agents.is_facet:
            # A sub-agent's clients are sockets the root owns, mirrored here;
            # touching the root's sockets from a facet isn't allowed.
            yield from self._sub_agents.virtual_connections(tag)
            return
        for connection in self._websockets.get_connections(tag):
            if not self._sub_agents.is_child_targeted(connection):
                yield connection

    def get_connection(self, id: str) -> Connection | None:
        """Return the open connection with ``id``, or ``None``.

        Raises
        ------
        DuplicateConnectionIdError
            If more than one open connection has that id.
        """
        if self._sub_agents.is_facet:
            return next(
                (c for c in self._sub_agents.virtual_connections() if c.id == id), None
            )
        connection = self._websockets.get_connection(id)
        if connection is None or self._sub_agents.is_child_targeted(connection):
            return None
        return connection

    def broadcast(
        self, message: str | bytes, *, exclude: Iterable[str | Connection] = ()
    ) -> None:
        """Send ``message`` to every open connection, except ``exclude``.

        An id excludes every connection with that id; a `Connection`
        excludes exactly that one.
        """
        excluded = list(exclude)
        if self._sub_agents.is_facet:
            without = [item if isinstance(item, str) else item.id for item in excluded]
            self._spawn(
                self._sub_agents.broadcast_to_path(
                    self._sub_agents.self_path, message, without
                )
            )
            return
        excluded_ids = {item for item in excluded if isinstance(item, str)}
        for connection in self.get_connections():
            if connection.id in excluded_ids or connection in excluded:
                continue
            _send(connection, message)

    # RPC

    def get_callable_methods(self) -> dict[str, CallableMetadata]:
        """Return the ``@callable`` methods clients may call, by name."""
        return callable_methods(self)

    # Schedules

    async def schedule(
        self,
        when: datetime | timedelta | float | str,
        callback: str | Callable[..., Any],
        payload: JSONValue = None,
        *,
        retry: RetryOptions | None = None,
        idempotent: bool | None = None,
    ) -> Schedule:
        """Schedule ``callback(payload, schedule)``.

        Parameters
        ----------
        when
            A ``datetime`` runs it once then; a ``timedelta`` or number of
            seconds once after that delay; a ``str`` on that cron expression
            (UTC).
        callback
            A method of this agent, or its name.
        payload
            JSON data passed to the callback.
        retry
            Overrides ``options.retry`` for this schedule.
        idempotent
            Reuse a matching existing schedule instead of adding another
            (default: yes for cron, no for one-shots).
        """
        return await self._scheduler.set(
            when, callback, payload, retry=retry, idempotent=idempotent
        )

    async def schedule_every(
        self,
        interval: Duration,
        callback: str | Callable[..., Any],
        payload: JSONValue = None,
        *,
        retry: RetryOptions | None = None,
        idempotent: bool | None = None,
    ) -> Schedule:
        """Run ``callback(payload, schedule)`` every ``interval`` (at most 30 days).

        A run still in progress when the next is due is skipped, unless it's
        older than ``options.hung_schedule_timeout``.
        """
        return await self._scheduler.every(
            interval, callback, payload, retry=retry, idempotent=idempotent
        )

    async def get_schedule_by_id(self, id: str) -> Schedule | None:
        """Return one schedule, or ``None``."""
        return await self._scheduler.get(id)

    async def list_schedules(
        self,
        *,
        id: str | None = None,
        type: ScheduleType | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Sequence[Schedule]:
        """Return schedules, optionally filtered by id, type, or next run time."""
        return await self._scheduler.list(id=id, type=type, start=start, end=end)

    async def cancel_schedule(self, id: str) -> bool:
        """Cancel one schedule; return whether it existed."""
        return await self._scheduler.cancel(id)

    # Queue

    async def queue(
        self,
        callback: str | Callable[..., Any],
        payload: JSONValue = None,
        *,
        retry: RetryOptions | None = None,
        id: str | None = None,
    ) -> str:
        """Queue ``callback(payload, item)`` to run soon, in order; return the id.

        Parameters
        ----------
        callback
            A method of this agent, or its name.
        payload
            JSON data passed to the callback.
        retry
            Overrides ``options.retry`` for this item.
        id
            A stable id; pushing it again replaces the pending item.
        """
        item = await self._queue.push(callback, payload, id=id, retry=retry)
        return item.id

    async def dequeue(self, id: str) -> bool:
        """Cancel one queued item; return whether it existed."""
        return await self._queue.cancel(id)

    async def dequeue_all(self) -> int:
        """Cancel every queued item; return how many."""
        return await self._queue.cancel_all()

    async def dequeue_all_by_callback(self, callback: str | Callable[..., Any]) -> int:
        """Cancel every queued item for ``callback``; return how many."""
        return await self._queue.cancel_all(callback)

    async def get_queue(self, id: str) -> QueueItem | None:
        """Return one queued item, or ``None``."""
        return await self._queue.get(id)

    async def queue_items(
        self, callback: str | Callable[..., Any] | None = None
    ) -> Sequence[QueueItem]:
        """Return the queued items in order, optionally for one callback."""
        return await self._queue.list(callback)

    async def _routed_memory_limit(self, context: MemoryLimitContext) -> None:
        hook = getattr(self, "_on_alarm_memory_limit", None)
        if hook is not None:
            await hook(context)

    async def _callback_failed(self, error: Exception) -> None:
        # A scheduled or queued callback, or a task run, failed for good.
        await run_in_host_context(self, partial(self._report_error, None, error))

    # Fibers

    async def run_fiber[T](
        self, name: str, fn: Callable[[FiberContext], Awaitable[T]]
    ) -> T:
        """Run ``fn`` as a fiber: if the isolate dies, it's recovered on wake.

        ``fn`` gets a `FiberContext`; checkpoint with ``ctx.stash(data)`` (or
        `stash`), and ``on_fiber_recovered`` receives the last one. The
        agent stays in memory while it runs. Errors propagate; there are no
        retries.
        """
        return await self._fibers.run(name, fn)

    def stash(self, data: Any) -> None:
        """Checkpoint the current fiber, replacing its previous snapshot.

        ``data`` must be JSON-serializable; it's saved before this returns.

        Raises
        ------
        RuntimeError
            If called outside a fiber.
        """
        stash_in_fiber(data)

    async def start_fiber(
        self,
        name: str,
        fn: Callable[[FiberContext], Awaitable[None]],
        *,
        fiber_id: str | None = None,
        idempotency_key: str | None = None,
        metadata: dict[str, JSONValue] | None = None,
        wait_for_completion: bool = False,
    ) -> StartFiberResult:
        """Start a managed fiber: a fiber with a record you can inspect.

        A ``fiber_id`` or ``idempotency_key`` that matches an existing fiber
        returns it with ``accepted=False`` instead of starting another. The
        body runs in the background (its return value is discarded); with
        ``wait_for_completion``, this returns once the fiber has finished.

        Raises
        ------
        ValueError
            If ``fiber_id`` or ``idempotency_key`` is blank.
        FiberConflictError
            If they name different existing fibers.
        FiberNotFoundError
            If the fiber is deleted while waiting for it.
        """
        return await self._fibers.start(
            name,
            fn,
            fiber_id=fiber_id,
            idempotency_key=idempotency_key,
            metadata=metadata,
            wait_for_completion=wait_for_completion,
        )

    async def inspect_fiber(self, fiber_id: str) -> FiberInspection | None:
        """Return a managed fiber's record, or ``None``."""
        return self._fibers.inspect(fiber_id)

    async def inspect_fiber_by_key(
        self, idempotency_key: str
    ) -> FiberInspection | None:
        """Return the managed fiber with ``idempotency_key``, or ``None``."""
        return self._fibers.inspect_by_key(idempotency_key)

    async def list_fibers(
        self,
        *,
        status: FiberStatus | Sequence[FiberStatus] | None = None,
        name: str | None = None,
        limit: int | None = None,
    ) -> Sequence[FiberInspection]:
        """Return managed fibers, newest first (default 50, at most 100)."""
        return self._fibers.list(status=status, name=name, limit=limit)

    async def cancel_fiber(self, fiber_id: str, reason: str | None = None) -> bool:
        """Abort a live managed fiber; ``False`` if it's unknown or finished.

        Its record turns ``aborted``; a body running here gets
        ``CancelledError`` at its next ``await``.
        """
        return self._fibers.cancel(fiber_id, reason)

    async def cancel_fiber_by_key(
        self, idempotency_key: str, reason: str | None = None
    ) -> bool:
        """`cancel_fiber` the managed fiber with ``idempotency_key``."""
        return self._fibers.cancel_by_key(idempotency_key, reason)

    async def resolve_fiber(self, fiber_id: str, result: FiberRecoveryResult) -> bool:
        """Settle an ``interrupted`` managed fiber; ``False`` for any other."""
        return self._fibers.resolve(fiber_id, result)

    async def delete_fibers(
        self,
        *,
        status: FiberStatus | Sequence[FiberStatus] | None = None,
        settled_before: datetime | None = None,
        limit: int | None = None,
    ) -> int:
        """Delete finished managed-fiber records and return how many.

        By default ``completed``, ``error``, and ``aborted`` ones (at most
        100); ``interrupted`` records are deleted only when asked for.
        """
        return self._fibers.delete(
            status=status, settled_before=settled_before, limit=limit
        )

    async def _run_fiber_with_stash_wrapper[T](
        self,
        name: str,
        fn: Callable[[FiberContext], Awaitable[T]],
        wrap: Callable[[Any], Any],
    ) -> T:
        # For chat turns: the snapshot wraps whatever the turn stashes.
        return await self._fibers.run_with_stash_wrapper(name, fn, wrap)

    async def _with_fiber_stash[T](
        self, context: FiberContext, fn: Callable[[], Awaitable[T]]
    ) -> T:
        return await with_fiber_stash(context, fn)

    async def _handle_internal_fiber_recovery(self, ctx: FiberRecoveryContext) -> bool:
        # Framework recovery (chat turns) overrides this; True means handled.
        return False

    # Keep-alive

    async def keep_alive(self) -> Disposable:
        """Keep the agent in memory until the returned handle is disposed.

        The alarm fires every ``keep_alive_interval`` meanwhile (on a
        sub-agent, the root's alarm). Prefer `keep_alive_while`.
        """
        return await self._keep_alive.acquire()

    async def keep_alive_while[T](self, fn: Callable[[], Awaitable[T]]) -> T:
        """Run ``fn`` keeping the agent in memory, however ``fn`` ends."""
        return await self._keep_alive.keep_alive_while(fn)

    # Teardown

    async def destroy(self) -> None:
        """Delete the agent: its storage, alarm, and connections.

        The object resets right after this returns; don't use it again. A
        teardown cut short is finished on the next wake. On a sub-agent, the
        root deletes it, which may abort this call (fire-and-forget).
        """
        if self._sub_agents.is_facet:
            self._emit("destroy", {})
            await self._sub_agents.destroy_self()
            return
        await self.ctx.storage.put(_DESTROY_PENDING_KEY, True)
        await self._lifecycle.disable_alarms()
        await self._lifecycle.dispose()
        await self.ctx.storage.deleteAll()
        self._destroyed = True
        # Reset on the next tick (ctx.abort can't be caught), without the
        # platform retrying an alarm whose constructor would recreate tables.
        _ffi.abort_without_alarm_retry(self.ctx, "destroyed")
        self._emit("destroy", {})

    # Runtime entry points (attached under the runtime's names below)

    async def __fetch(self, request: "Request") -> Response:
        match = parse_sub_agent_path(request.url, _ffi.export_names(self.ctx))
        if match is None:
            return await self._lifecycle.fetch(request)
        try:
            await self._lifecycle.start()
            return await self._sub_agents.route_request(request, match)
        except Exception:
            _log.exception("Error routing a sub-agent request")
            return Response("Internal Server Error", status=500)

    async def __alarm(self, *_args: Any) -> None:
        # A pending destroy pre-empts everything, including startup.
        if await self.ctx.storage.get(_DESTROY_PENDING_KEY):
            await self.destroy()
            return
        await self._lifecycle.alarm()

    async def __websocket_message(self, ws: Any, message: Any) -> None:
        await self._lifecycle.websocket_message(ws, message)

    async def __websocket_close(
        self, ws: Any, code: int, reason: str, was_clean: bool
    ) -> None:
        await self._lifecycle.websocket_close(ws, code, reason, was_clean)

    async def __websocket_error(self, ws: Any, error: Any) -> None:
        await self._lifecycle.websocket_error(ws, error)

    async def __ensure_initialized(self) -> None:
        # Native RPC bypasses fetch; get_agent_by_name calls this first.
        await self._lifecycle.start()

    # Connection handling (the root's sockets and forwarded ones)

    async def _connect_sequence(
        self, connection: Connection, ctx: ConnectionContext
    ) -> None:
        websockets = self._websockets
        # Flags first, so they're set before the client can respond.
        if await self.should_connection_be_readonly(connection, ctx):
            connection.readonly = True
        if await self.should_send_protocol_messages(connection, ctx):
            if type(self).options.send_identity_on_connect:
                websockets.send_connect_frames(
                    connection,
                    name=self.name,
                    agent=camel_case_to_kebab_case(type(self).__name__),
                )
            else:
                websockets.send_state(connection)
            _send(connection, _MCP_SERVERS_FRAME)
        else:
            connection._set_protocol_enabled(False)
        self._emit("connect", {"connectionId": connection.id})
        await self._after_connect_frames(connection)
        try:
            await self.on_connect(connection, ctx)
        except Exception as error:
            await self.on_error(None, error)

    async def _connect_virtual(
        self, connection: Connection, request: "Request"
    ) -> None:
        # A forwarded connection: this sub-agent picks its own tags.
        ctx = ConnectionContext(request=request)
        tags = await self.get_connection_tags(connection, ctx)
        cast(VirtualConnection, connection)._set_tags(
            _prepare_tags(connection.id, tags)
        )
        await self._connect_sequence(connection, ctx)

    async def _message_locally(
        self, connection: Connection, message: str | bytes
    ) -> None:
        frame = _parse_frame(message)
        if frame is not None:
            if self._websockets.apply_state_frame(connection, frame):
                return
            if is_rpc_request(frame):
                await self._rpc.answer(connection, cast(RpcRequest, frame))
                return
        try:
            await self.on_message(connection, message)
        except Exception as error:
            await self.on_error(None, error)

    async def _close_locally(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None:
        self._emit(
            "disconnect",
            {"connectionId": connection.id, "code": code, "reason": reason},
        )
        await self.on_close(connection, code, reason, was_clean)

    async def _after_connect_frames(self, connection: Connection) -> None:
        """Send an SDK subclass's own connect frames, before ``on_connect``."""

    async def _cleanup_route_prefix(self, prefix: str) -> None:
        await self._scheduler._cleanup_route_prefix(prefix)
        await self._queue._cleanup_route_prefix(prefix)
        await self.tasks._cleanup_route_prefix(prefix)
        await self._fibers._cleanup_route_prefix(prefix)
        await self._keep_alive._cleanup_route_prefix(prefix)

    # Sub-agent RPC entry points (called by parents, children, and the root)

    async def _cf_init_as_facet(
        self, name: str, parent_path: str, identity: str
    ) -> None:
        await self._sub_agents.init_as_facet(name, parent_path, identity)

    async def _cf_route_lifecycle(
        self, target: WireRouteAddress | None, envelope: WireEnvelope
    ) -> Any:
        return await self._sub_agents.route_lifecycle(target, envelope)

    async def _cf_cleanup_facet_prefix(self, path: str) -> None:
        await self._lifecycle.start()
        await self._sub_agents.cleanup_prefix(path_from_json(path))

    async def _cf_destroy_descendant_facet(self, path: str) -> None:
        await self._lifecycle.start()
        await self._sub_agents.destroy_descendant(path_from_json(path))

    async def _cf_handle_sub_agent_websocket_connect(
        self, bridge: Any, meta: ForwardedConnection
    ) -> None:
        await self._sub_agents.handle_connect(bridge, meta)

    async def _cf_handle_sub_agent_websocket_message(
        self, bridge: Any, meta: ForwardedConnection, message: str | bytes
    ) -> None:
        await self._sub_agents.handle_message(bridge, meta, message)

    async def _cf_handle_sub_agent_websocket_close(
        self,
        bridge: Any,
        meta: ForwardedConnection,
        code: int,
        reason: str,
        was_clean: bool,
    ) -> None:
        await self._sub_agents.handle_close(bridge, meta, code, reason, was_clean)

    async def _cf_send_to_sub_agent_connection(
        self, connection_id: str, message: str | bytes
    ) -> None:
        connection = self._sub_agents.root_connection(connection_id)
        if connection is not None:
            _send(connection, as_message(message))

    async def _cf_close_sub_agent_connection(
        self, connection_id: str, code: int | None, reason: str | None
    ) -> None:
        connection = self._sub_agents.root_connection(connection_id)
        if connection is not None:
            connection.close(code, reason)

    async def _cf_set_sub_agent_connection_state(
        self, connection_id: str, state: Any, flags: dict[str, Any]
    ) -> None:
        connection = self._sub_agents.root_connection(connection_id)
        if connection is not None:
            apply_forwarded_state(connection, state, dict(flags or {}))

    async def _cf_broadcast_to_sub_agent(
        self, path: str, message: str | bytes, without: list[str]
    ) -> None:
        await self._sub_agents.broadcast_to_path(
            path_from_json(path), message, without or []
        )

    async def _cf_sub_agent_connection_metas(
        self, path: str
    ) -> list[ForwardedConnection]:
        return self._sub_agents.connection_metas(path_from_json(path))

    async def _cf_close_sub_agent_connections_for_prefix(
        self, path: str, code: int, reason: str
    ) -> None:
        self._sub_agents.close_connections_under(path_from_json(path), code, reason)

    async def _cf_invoke_sub_agent(
        self, class_name: str, name: str, method: str, args: list[Any]
    ) -> Any:
        return await self._sub_agents.invoke(class_name, name, method, list(args or []))

    async def _cf_invoke_sub_agent_path(
        self, path: str, method: str, args: list[Any]
    ) -> Any:
        return await self._sub_agents.invoke_path(
            path_from_json(path), method, list(args or [])
        )

    # Internals

    async def _report_error(
        self, connection: Connection | None, error: Exception
    ) -> None:
        """Tell ``on_error`` about an error it can't handle; never raises."""
        try:
            await self.on_error(connection, error)
        except Exception as raised:
            if raised is not error:
                _log.exception("on_error failed")

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        self._publish(event_type, payload)

    def _forward_event(self, event: LifecycleEvent) -> None:
        self._publish(event.type, event.payload)

    def _publish(self, event_type: str, payload: Any) -> None:
        # Telemetry never fails the work that emitted it.
        sink = self.observability
        if sink is None:
            return
        event = ObservabilityEvent(
            type=event_type,
            agent=self._lifecycle.class_name,
            name=self.name,
            payload=payload,
            timestamp=datetime.now(UTC),
        )
        try:
            sink.emit(event)
        except Exception:
            _log.exception("Observability emit failed for %s", event_type)

    def _spawn(self, work: Any) -> None:
        task = asyncio.ensure_future(work)
        self._background.add(task)
        task.add_done_callback(self._background.discard)


class _AgentState(State[Any]):
    """State whose initial value is a fresh copy of the agent's ``initial_state``.

    Read when first needed, so an ``initial_state`` set in ``__init__`` counts.
    """

    def __init__(self, agent: Agent[Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._agent = agent

    def _initial_value(self) -> Any:
        initial = self._agent.initial_state
        return json.loads(json.dumps(initial)) if initial is not None else None


class _AgentHooks:
    """The host hooks Lifecycle calls, with Agent's error policy around them."""

    __slots__ = ("_agent",)

    def __init__(self, agent: Agent[Any]) -> None:
        self._agent = agent

    async def on_start(self) -> None:
        agent = self._agent
        try:
            # Every capability has started (including any a subclass
            # installed), and the user's on_start hasn't: upstream's order.
            await agent._fibers.recover_on_wake()
            await agent.on_start()
        except Exception as error:
            # Startup can't be half-done: on_error is told, then it re-raises.
            await agent._report_error(None, error)
            raise

    async def on_request(self, request: "Request") -> Response:
        agent = self._agent
        try:
            return await agent.on_request(request)
        except Exception as error:
            await agent.on_error(None, error)
            return Response("Internal Server Error", status=500)

    async def on_alarm(self) -> None:
        if not self._agent._destroyed:
            await self._agent.on_alarm()


class _AgentConnectionHandlers:
    """Agent's handlers for the root's sockets: forward, or handle locally."""

    __slots__ = ("_agent",)

    def __init__(self, agent: Agent[Any]) -> None:
        self._agent = agent

    async def on_connect(self, connection: Connection, ctx: ConnectionContext) -> None:
        agent = self._agent
        outer = ctx.request.headers.get(OUTER_URL_HEADER)
        if outer:
            connection._set_flag(OUTER_URL_FLAG, outer)
        # The request was gated in fetch; forward without gating again.
        if await agent._sub_agents.forward_connect(connection, ctx.request, gate=False):
            return
        await agent._connect_sequence(connection, ctx)

    async def on_message(self, connection: Connection, message: str | bytes) -> None:
        agent = self._agent
        if await agent._sub_agents.forward_message(connection, message):
            return
        await agent._message_locally(connection, message)

    async def on_close(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None:
        agent = self._agent
        if await agent._sub_agents.forward_close(connection, code, reason, was_clean):
            return
        await agent._close_locally(connection, code, reason, was_clean)

    async def on_error(self, connection: Connection, error: BaseException) -> None:
        if isinstance(error, Exception):
            await self._agent.on_error(connection, error)


def _in_agent_context(method: FunctionType) -> Callable[..., Any]:
    """Wrap a public agent method so it runs as the current agent.

    Methods reached through native RPC bypass every SDK hook; this makes
    ``get_current_agent()`` work there. Inside an existing context for the
    same agent the method runs as-is, keeping its connection and request.
    """
    if inspect.isasyncgenfunction(method):

        @functools.wraps(method)
        async def generator_wrapper(
            self: Agent[Any], *args: Any, **kwargs: Any
        ) -> AsyncGenerator[Any]:
            chunks = method(self, *args, **kwargs)
            if _is_current(self):
                async for chunk in chunks:
                    yield chunk
                return
            try:
                while True:
                    try:
                        chunk = await run_in_host_context(self, chunks.__anext__)
                    except StopAsyncIteration:
                        return
                    yield chunk
            finally:
                await chunks.aclose()

        return generator_wrapper

    if inspect.iscoroutinefunction(method):

        @functools.wraps(method)
        async def async_wrapper(self: Agent[Any], *args: Any, **kwargs: Any) -> Any:
            if _is_current(self):
                return await method(self, *args, **kwargs)
            return await run_in_host_context(
                self, partial(method, self, *args, **kwargs)
            )

        return async_wrapper

    @functools.wraps(method)
    def sync_wrapper(self: Agent[Any], *args: Any, **kwargs: Any) -> Any:
        if _is_current(self):
            return method(self, *args, **kwargs)
        return call_in_host_context(self, partial(method, self, *args, **kwargs))

    return sync_wrapper


def _is_current(agent: Agent[Any]) -> bool:
    current = current_host_context()
    return current is not None and current.host is agent


# Names Agent (and SDK subclasses) define aren't auto-wrapped: their hooks
# already run in the context the SDK sets, with their connection and request.
Agent._sdk_names = frozenset(dir(Agent))

# The runtime calls a Durable Object's JS method names. They're attached to
# the class after its body, so autocomplete and type checkers don't list them
# while workerd's introspection (dir(cls)) still finds them.
for _js_name, _impl in {
    "fetch": Agent._Agent__fetch,  # ty: ignore[unresolved-attribute]
    "alarm": Agent._Agent__alarm,  # ty: ignore[unresolved-attribute]
    "webSocketMessage": Agent._Agent__websocket_message,  # ty: ignore[unresolved-attribute]
    "webSocketClose": Agent._Agent__websocket_close,  # ty: ignore[unresolved-attribute]
    "webSocketError": Agent._Agent__websocket_error,  # ty: ignore[unresolved-attribute]
    "__unsafe_ensureInitialized": Agent._Agent__ensure_initialized,  # ty: ignore[unresolved-attribute]
}.items():
    setattr(Agent, _js_name, _impl)
