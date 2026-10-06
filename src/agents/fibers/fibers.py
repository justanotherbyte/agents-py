"""Durable execution with checkpoints and a recovery hook: the Fibers capability.

Port of upstream's "legacy fibers" engine (``index.ts``: ``runFiber``,
``startFiber`` and the managed-fiber ledger, ``_checkRunFibers``) and the
facet-run index in ``dynamic-agents.ts`` (``.design/fibers_api.md`` §4,
``.design/fibers_engine.md``).

A running fiber has a ``cf_agents_runs`` row; a row still there on a later
wake, and not running in this isolate, means the isolate died mid-fiber, and
the recovery scan hands its last checkpoint to the recovery hook. Managed
fibers also keep a ``cf_agents_fibers`` record that outlives the run. A facet
has no alarm, so the root indexes its fibers and calls back into it to
recover them.
"""

import asyncio
import json
import logging
import secrets
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from datetime import datetime, timedelta
from functools import partial
from typing import Any, cast, override

from ..core.sql import SqlError
from ..core.timing import epoch_ms, from_epoch_ms, now_ms, to_seconds
from ..core.types import Duration, JSONValue
from ..lifecycle.capability import LifecycleCapability
from ..lifecycle.types import (
    JobContext,
    JobOutcome,
    Reschedule,
    RouteAddress,
    RouteContext,
)
from .errors import FiberConflictError, FiberNotFoundError
from .keep_alive import KeepAlive
from .store import LIVE, TERMINAL, FiberStore, inspection_from_row
from .types import (
    FiberAborted,
    FiberCompleted,
    FiberContext,
    FiberErrored,
    FiberInspection,
    FiberInterrupted,
    FiberLedgerRow,
    FiberRecoveryContext,
    FiberRecoveryHandler,
    FiberRecoveryResult,
    FiberRouteMessage,
    FiberStatus,
    InternalFiberRecoveryHandler,
    StartFiberResult,
)

__all__ = ("Fibers", "stash", "with_fiber_stash")

_log = logging.getLogger("agents.fibers")

_SCHEMA_VERSION_KEY = "cf_agents:fibers_schema_version"
_SCHEMA_VERSION = 1
_HOUSEKEEPING_JOB = "housekeeping"
# A scan that makes no progress backs the housekeeping wake off, so a hook
# that always raises can't wake the object every interval forever.
_MAX_BACKOFF_MS = 5 * 60_000
_MAX_BACKOFF_EXPONENT = 20
_DEFAULT_DELETE_STATUSES: tuple[FiberStatus, ...] = ("completed", "aborted", "error")

type _Outcome = tuple[FiberStatus, str | None]

_current: ContextVar[FiberContext | None] = ContextVar("agents_fiber", default=None)


def stash(data: Any) -> None:
    """Checkpoint the current fiber (replacing its previous snapshot).

    Raises
    ------
    RuntimeError
        If called outside a fiber.
    """
    context = _current.get()
    if context is None:
        raise RuntimeError("stash() called outside a fiber")
    context.stash(data)


async def with_fiber_stash[T](
    context: FiberContext, fn: Callable[[], Awaitable[T]]
) -> T:
    """Run ``fn`` with ``stash()`` writing through ``context`` (for chat turns)."""
    token = _current.set(context)
    try:
        return await fn()
    finally:
        _current.reset(token)


class Fibers(LifecycleCapability):
    """Fibers and managed fibers, with recovery of interrupted ones.

    Parameters
    ----------
    keep_alive
        The lease every running fiber holds, so the object stays in memory.
    on_recovered
        The recovery hook, run in the host context; for a managed fiber, a
        returned result settles its record.
    internal_recovery
        A framework hook that runs first, under ``hook_timeout``; returning
        ``True`` skips ``on_recovered``.
    hook_timeout
        The time limit for ``internal_recovery`` (0 or less: none).
    scan_deadline
        A soft limit on one recovery scan; the rest waits for the next.
    max_age
        How long recovery retries a plain fiber whose hook keeps failing
        (``None``: forever).
    housekeeping_interval
        How often the root checks pending recovery and its facets' fibers.
    """

    def __init__(
        self,
        keep_alive: KeepAlive,
        *,
        on_recovered: FiberRecoveryHandler,
        internal_recovery: InternalFiberRecoveryHandler | None = None,
        hook_timeout: Duration = 10,
        scan_deadline: Duration = 10,
        max_age: Duration | None = timedelta(hours=24),
        housekeeping_interval: Duration = 30,
    ) -> None:
        super().__init__("fibers")
        self._keep_alive = keep_alive
        self._on_recovered = on_recovered
        self._internal_recovery = internal_recovery
        self._hook_timeout = to_seconds(hook_timeout)
        self._scan_deadline_ms = to_seconds(scan_deadline) * 1000
        self._max_age_ms = to_seconds(max_age) * 1000 if max_age is not None else None
        self._interval_ms = int(to_seconds(housekeeping_interval) * 1000)
        self._store_instance: FiberStore | None = None
        # Fibers running in this isolate; their rows aren't interrupted.
        self._active: set[str] = set()
        self._executions: dict[str, asyncio.Task[None]] = {}
        self._waiters: dict[str, list[asyncio.Future[None]]] = {}
        self._scanning = False
        self._no_progress_scans = 0

    @property
    def _store(self) -> FiberStore:
        if self._store_instance is None:
            self._store_instance = FiberStore(self.lifecycle.sql)
        return self._store_instance

    # Plain fibers

    async def run[T](self, name: str, fn: Callable[[FiberContext], Awaitable[T]]) -> T:
        """Run ``fn`` as a fiber and return its result (errors propagate)."""
        return await self._run(_new_id(), name, fn)

    async def run_with_stash_wrapper[T](
        self,
        name: str,
        fn: Callable[[FiberContext], Awaitable[T]],
        wrap: Callable[[Any], Any],
    ) -> T:
        """Run a fiber whose snapshots are ``wrap(data)``, starting at ``wrap(None)``.

        For chat turns, whose recovery snapshot wraps the user's.
        """
        return await self._run(_new_id(), name, fn, wrap_stash=wrap)

    # Managed fibers

    async def start(
        self,
        name: str,
        fn: Callable[[FiberContext], Awaitable[None]],
        *,
        fiber_id: str | None = None,
        idempotency_key: str | None = None,
        metadata: dict[str, JSONValue] | None = None,
        wait_for_completion: bool = False,
    ) -> StartFiberResult:
        """Accept a managed fiber, or return the existing one it matches.

        Raises
        ------
        ValueError
            If ``fiber_id`` or ``idempotency_key`` is blank.
        FiberConflictError
            If they name different existing fibers.
        FiberNotFoundError
            If the fiber is deleted while waiting for it.
        """
        if fiber_id is not None and not fiber_id.strip():
            raise ValueError("fiber_id must not be blank")
        if idempotency_key is not None and not idempotency_key.strip():
            raise ValueError("idempotency_key must not be blank")
        store = self._store
        id = fiber_id if fiber_id is not None else _new_id()
        by_id = store.get(id)
        by_key = store.get_by_key(idempotency_key) if idempotency_key else None
        if (
            by_key is not None
            and by_key["fiber_id"] != id
            and (by_id is not None or fiber_id is not None)
        ):
            raise FiberConflictError(id, by_key["idempotency_key"] or "")
        existing = by_id or by_key
        if existing is not None:
            if wait_for_completion and existing["status"] in LIVE:
                return _started(await self._wait(existing["fiber_id"]), accepted=False)
            return _started(inspection_from_row(existing), accepted=False)

        store.insert_pending(id, name, idempotency_key, metadata)
        row = store.get(id)
        assert row is not None
        execution = asyncio.ensure_future(self._execute(id, name, fn))
        self._executions[id] = execution
        execution.add_done_callback(partial(self._execution_done, id))
        if wait_for_completion:
            return _started(await self._wait(id), accepted=True)
        return _started(inspection_from_row(row), accepted=True)

    def inspect(self, fiber_id: str) -> FiberInspection | None:
        """Return a managed fiber's record, or ``None``."""
        row = self._store.get(fiber_id)
        return inspection_from_row(row) if row is not None else None

    def inspect_by_key(self, idempotency_key: str) -> FiberInspection | None:
        """Return the managed fiber with ``idempotency_key``, or ``None``."""
        row = self._store.get_by_key(idempotency_key)
        return inspection_from_row(row) if row is not None else None

    def list(
        self,
        *,
        status: FiberStatus | Sequence[FiberStatus] | None = None,
        name: str | None = None,
        limit: int | None = None,
    ) -> Sequence[FiberInspection]:
        """Return managed fibers, newest first (default 50, at most 100)."""
        bounded = min(max(limit if limit is not None else 50, 1), 100)
        rows = self._store.list(_statuses(status), name, bounded)
        return [inspection_from_row(row) for row in rows]

    def cancel(self, fiber_id: str, reason: str | None = None) -> bool:
        """Abort a live managed fiber; ``False`` if it's unknown or finished.

        The record turns ``aborted`` and waiters are released; a fiber running
        here gets ``CancelledError`` at its next ``await``.
        """
        row = self._store.get(fiber_id)
        if row is None or row["status"] not in LIVE:
            return False
        self._store.settle(fiber_id, "aborted", reason, when=LIVE)
        execution = self._executions.get(fiber_id)
        if execution is not None:
            execution.cancel(reason)
        self._notify(fiber_id)
        return True

    def cancel_by_key(self, idempotency_key: str, reason: str | None = None) -> bool:
        """`cancel` the managed fiber with ``idempotency_key``."""
        row = self._store.get_by_key(idempotency_key)
        return self.cancel(row["fiber_id"], reason) if row is not None else False

    def resolve(self, fiber_id: str, result: FiberRecoveryResult) -> bool:
        """Settle an ``interrupted`` managed fiber; ``False`` for any other."""
        row = self._store.get(fiber_id)
        if row is None or row["status"] != "interrupted":
            return False
        self._apply_result(fiber_id, result)
        return True

    def delete(
        self,
        *,
        status: FiberStatus | Sequence[FiberStatus] | None = None,
        settled_before: datetime | None = None,
        limit: int | None = None,
    ) -> int:
        """Delete finished records (default: completed, error, aborted).

        ``interrupted`` records are kept unless asked for. Returns how many
        were deleted (default at most 100, at most 500).
        """
        wanted = _statuses(status) or _DEFAULT_DELETE_STATUSES
        terminal = [value for value in wanted if value in TERMINAL]
        if not terminal:
            return 0
        bounded = min(max(limit if limit is not None else 100, 1), 500)
        before = epoch_ms(settled_before) if settled_before is not None else None
        return self._store.delete_settled(terminal, before, bounded)

    # Recovery

    async def recover(self) -> None:
        """Recover interrupted fibers: hand each one to the recovery hook.

        One scan runs at a time; a second call while one runs returns at once.
        """
        if self._scanning:
            return
        self._scanning = True
        started = now_ms()
        progress = False
        store = self._store
        try:
            for row in store.runs():
                id = row["id"]
                if self._past_deadline(started):
                    self._emit_deadline(id, row["name"], started, managed=None)
                    break
                if id in self._active:
                    continue
                managed = store.get(id)
                if row["completed_at"] is not None and (
                    managed is None or row["outcome"] is not None
                ):
                    # The body finished and only its cleanup failed. A record
                    # still live means its own settle failed too.
                    if managed is not None and row["outcome"] is not None:
                        store.settle(
                            id,
                            row["outcome"],
                            row["error_message"],
                            when=LIVE,
                            completed_at=row["completed_at"],
                        )
                    store.delete_run(id)
                    progress = True
                    self._notify(id)
                    continue
                if managed is not None and managed["status"] in TERMINAL:
                    store.delete_run(id)
                    progress = True
                    self._notify(id)
                    continue

                context = FiberRecoveryContext(
                    id=id,
                    name=row["name"],
                    snapshot=_snapshot(row["snapshot"]),
                    created_at=from_epoch_ms(row["created_at"]),
                )
                if managed is not None:
                    store.interrupt(id, row["snapshot"])
                    _add_ledger_fields(context, managed)
                self._emit_detected(context, managed is not None)
                recovered = await self._run_hook(context, managed)
                too_old = (
                    self._max_age_ms is not None
                    and now_ms() - row["created_at"] > self._max_age_ms
                )
                if recovered or managed is not None or too_old:
                    if not recovered and managed is None:
                        self._emit(
                            "fiber:recovery:skipped",
                            {
                                "fiberId": id,
                                "fiberName": row["name"],
                                "reason": "max_age_exceeded",
                                "elapsedMs": now_ms() - row["created_at"],
                            },
                        )
                    store.delete_run(id)
                    progress = True
                if managed is not None:
                    self._notify(id)

            for record in store.orphaned_records():
                id = record["fiber_id"]
                if self._past_deadline(started):
                    self._emit_deadline(id, record["name"], started, managed=True)
                    break
                if id in self._active:
                    continue
                store.interrupt(id, None)
                context = FiberRecoveryContext(
                    id=id,
                    name=record["name"],
                    snapshot=_snapshot(record["snapshot"]),
                    created_at=from_epoch_ms(record["created_at"]),
                )
                _add_ledger_fields(context, record)
                self._emit_detected(context, True)
                await self._run_hook(context, record)
                # Settled this pass whatever the hook did.
                progress = True
                self._notify(id)
        finally:
            self._scanning = False
            if progress:
                self._no_progress_scans = 0
            elif self._has_pending_recovery():
                self._no_progress_scans += 1
            else:
                self._no_progress_scans = 0

    async def recover_on_wake(self) -> None:
        """Recover what a dead isolate left, then arm housekeeping.

        The host calls this once per wake, after every capability has started
        and before its own startup code (``Agent`` does it before
        ``on_start``, as upstream), so the recovery hook can use any
        capability.
        """
        await self.recover()
        await self._sync_housekeeping()

    # Lifecycle hooks

    @override
    async def on_start(self) -> None:
        """Create the tables once.

        Recovery waits for `recover_on_wake`: the hook may use capabilities
        installed after this one, which haven't started yet.
        """
        storage = self.lifecycle.storage
        if (await storage.get(_SCHEMA_VERSION_KEY) or 0) < _SCHEMA_VERSION:
            self._store.ensure_tables()
            await storage.put(_SCHEMA_VERSION_KEY, _SCHEMA_VERSION)

    @override
    async def on_job(self, context: JobContext) -> JobOutcome:
        """Housekeeping: recover here, then visit facets with indexed fibers."""
        await self.recover()
        await self._check_facets()
        at = self._next_housekeeping()
        return Reschedule(at=at) if at is not None else None

    @override
    async def on_route(self, context: RouteContext) -> Any:
        """Index a facet's fiber (root), or recover on the root's request (facet)."""
        message: FiberRouteMessage = context.payload
        match message["type"]:
            case "register_run" | "unregister_run":
                owner = context.source
                if owner is None:
                    raise ValueError(
                        "A routed fiber registration must come from a facet"
                    )
                if message["type"] == "register_run":
                    self._store.register_facet_run(
                        owner.key, owner.data, message["run_id"]
                    )
                else:
                    self._store.unregister_facet_run(owner.key, message["run_id"])
                await self._sync_housekeeping()
                return True
            case "check_runs":
                await self.recover()
                return self._store.count_runs()
        raise ValueError(f"Unknown routed fiber message {message!r}")

    async def _cleanup_route_prefix(self, prefix: str) -> None:
        """Forget the root's index entries for a deleted facet subtree (internal)."""
        self._store.forget_facets_under(prefix)
        await self._sync_housekeeping()

    # Running

    async def _run[T](
        self,
        id: str,
        name: str,
        fn: Callable[[FiberContext], Awaitable[T]],
        *,
        managed: bool = False,
        wrap_stash: Callable[[Any], Any] | None = None,
        on_settled: Callable[[_Outcome], None] | None = None,
    ) -> T:
        store = self._store
        store.insert_run(id, name)
        started = now_ms()
        self._emit(
            "fiber:run:started", {"fiberId": id, "fiberName": name, "managed": managed}
        )
        self._active.add(id)

        def write(data: Any) -> None:
            store.write_snapshot(id, json.dumps(data), managed=managed)

        def fiber_stash(data: Any) -> None:
            write(wrap_stash(data) if wrap_stash is not None else data)

        routes = self.lifecycle.routes
        registered = False
        lease = None
        outcome: _Outcome | None = None
        try:
            if wrap_stash is not None:
                write(wrap_stash(None))
            if routes.source is not None:
                # The facet's row stays here; the root's index brings recovery
                # back to it while it's idle.
                message: FiberRouteMessage = {"type": "register_run", "run_id": id}
                await routes.to_root(message)
                registered = True
            lease = await self._keep_alive.acquire()
            context = FiberContext(id=id, stash=fiber_stash)
            try:
                result = await with_fiber_stash(context, partial(fn, context))
            except (Exception, asyncio.CancelledError) as error:
                status: FiberStatus = (
                    "aborted" if isinstance(error, asyncio.CancelledError) else "error"
                )
                outcome = (status, _message(error))
                if on_settled is not None:
                    on_settled(outcome)
                self._emit(
                    "fiber:run:failed",
                    {
                        "fiberId": id,
                        "fiberName": name,
                        "managed": managed,
                        "error": outcome[1],
                        "elapsedMs": now_ms() - started,
                    },
                )
                raise
            outcome = ("completed", None)
            if on_settled is not None:
                on_settled(outcome)
            self._emit(
                "fiber:run:completed",
                {
                    "fiberId": id,
                    "fiberName": name,
                    "managed": managed,
                    "elapsedMs": now_ms() - started,
                },
            )
            return result
        finally:
            self._active.discard(id)
            deleted = await self._finalize(id, name, outcome)
            if lease is not None:
                lease.dispose()
            # The root's entry is what brings recovery to an idle facet, so it
            # stays until the row is gone.
            if registered and deleted:
                await self._unregister(id)

    async def _unregister(self, id: str) -> None:
        message: FiberRouteMessage = {"type": "unregister_run", "run_id": id}
        try:
            await self.lifecycle.routes.to_root(message)
        except Exception:
            # Raising here would hide the fiber's own result. The root's
            # housekeeping drops the stale entry once it finds no row.
            _log.exception("Unregistering facet fiber %s from the root failed", id)

    async def _finalize(self, id: str, name: str, outcome: _Outcome | None) -> bool:
        # Two writes: the outcome first, so a scan can tell "finished but the
        # delete failed" (drop silently) from "interrupted" (recover).
        store = self._store
        if outcome is not None:
            try:
                store.record_outcome(id, outcome[0], outcome[1])
            except SqlError:
                _log.exception(
                    "Recording the outcome of fiber %r (%s) failed", name, id
                )
        try:
            store.delete_run(id)
        except SqlError:
            _log.exception(
                "Finalizing fiber %r (%s) failed; its run row is left for recovery",
                name,
                id,
            )
            await self._sync_housekeeping()
            return False
        return True

    async def _execute(
        self, fiber_id: str, name: str, fn: Callable[[FiberContext], Awaitable[None]]
    ) -> None:
        if not self._store.mark_running(fiber_id):
            return
        settled = False

        def on_settled(outcome: _Outcome) -> None:
            nonlocal settled
            settled = True
            self._settle_execution(fiber_id, outcome)

        try:
            await self._run(fiber_id, name, fn, managed=True, on_settled=on_settled)
        except asyncio.CancelledError as error:
            if not settled:
                self._settle_execution(fiber_id, ("aborted", _message(error)))
            raise
        except Exception as error:
            if not settled:
                self._settle_execution(fiber_id, ("error", _message(error)))
            # The record holds the outcome; log it so it isn't lost entirely.
            _log.exception("Managed fiber %r (%s) failed", name, fiber_id)

    def _execution_done(self, fiber_id: str, execution: asyncio.Task[None]) -> None:
        if self._executions.get(fiber_id) is execution:
            del self._executions[fiber_id]

    def _settle_execution(self, fiber_id: str, outcome: _Outcome) -> None:
        status, error = outcome
        self._store.settle(fiber_id, status, error, when=("running",))
        self._notify(fiber_id)

    def _apply_result(self, fiber_id: str, result: FiberRecoveryResult) -> None:
        snapshot = json.dumps(result.snapshot) if result.snapshot is not None else None
        match result:
            case FiberCompleted():
                self._store.settle(
                    fiber_id,
                    "completed",
                    None,
                    when=("interrupted",),
                    snapshot=snapshot,
                    metadata=result.metadata,
                )
            case FiberErrored():
                self._store.settle(
                    fiber_id,
                    "error",
                    result.error,
                    when=("interrupted",),
                    snapshot=snapshot,
                )
            case FiberAborted():
                self._store.settle(
                    fiber_id,
                    "aborted",
                    result.reason,
                    when=("interrupted",),
                    snapshot=snapshot,
                )
            case FiberInterrupted():
                self._store.settle(
                    fiber_id,
                    "interrupted",
                    result.reason,
                    when=("interrupted",),
                    snapshot=snapshot,
                )
        self._notify(fiber_id)

    # Waiting

    async def _wait(self, fiber_id: str) -> FiberInspection:
        row = self._store.get(fiber_id)
        if row is not None and row["status"] in LIVE:
            if fiber_id not in self._executions:
                # Left by a dead isolate: recovery settles it.
                await self.recover()
                await self._sync_housekeeping()
            await self._terminal(fiber_id)
            row = self._store.get(fiber_id)
        if row is None:
            raise FiberNotFoundError(fiber_id)
        return inspection_from_row(row)

    async def _terminal(self, fiber_id: str) -> None:
        row = self._store.get(fiber_id)
        if row is None or row["status"] not in LIVE:
            return
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(fiber_id, []).append(waiter)
        await waiter

    def _notify(self, fiber_id: str) -> None:
        row = self._store.get(fiber_id)
        if row is not None and row["status"] in LIVE:
            return
        for waiter in self._waiters.pop(fiber_id, []):
            if not waiter.done():
                waiter.set_result(None)

    # Recovery hooks

    async def _run_hook(
        self, context: FiberRecoveryContext, managed: FiberLedgerRow | None
    ) -> bool:
        started = now_ms()
        is_managed = managed is not None
        self._emit("fiber:recovery:attempt", _recovery_payload(context, is_managed))
        try:
            handled = False
            if self._internal_recovery is not None:
                handled = await self._internal_hook(context, self._internal_recovery)
            if not handled:
                result = await self.lifecycle.run_in_host_context(
                    partial(self._on_recovered, context)
                )
                if is_managed and result is not None:
                    self._apply_result(context.id, result)
        except Exception as error:
            _log.exception(
                "Fiber recovery failed for %r (%s)", context.name, context.id
            )
            if is_managed:
                self._store.settle(
                    context.id, "error", _message(error), when=("interrupted",)
                )
                self._notify(context.id)
            self._emit(
                "fiber:recovery:failed",
                {
                    **_recovery_payload(context, is_managed, started),
                    "error": _message(error),
                    "reason": "handler_error",
                },
            )
            return False
        self._emit(
            "fiber:recovery:handled",
            {
                **_recovery_payload(context, is_managed, started),
                "status": "internal"
                if handled
                else "managed"
                if is_managed
                else "user",
            },
        )
        return True

    async def _internal_hook(
        self, context: FiberRecoveryContext, hook: InternalFiberRecoveryHandler
    ) -> bool:
        call = partial(self.lifecycle.run_in_host_context, partial(hook, context))
        if self._hook_timeout <= 0:
            return await call()
        try:
            async with asyncio.timeout(self._hook_timeout) as scope:
                return await call()
        except TimeoutError:
            if not scope.expired():
                raise
            raise TimeoutError(
                f"Fiber recovery hook timed out after {self._hook_timeout:g}s "
                f"for {context.name!r} ({context.id})"
            ) from None

    # Housekeeping (the root owns the alarm)

    def _has_pending_recovery(self) -> bool:
        if any(id not in self._active for id in self._store.run_ids()):
            return True
        return any(
            record["fiber_id"] not in self._active
            for record in self._store.orphaned_records()
        )

    def _next_housekeeping(self) -> datetime | None:
        now = now_ms()
        times: list[int] = []
        if self._has_pending_recovery():
            exponent = min(self._no_progress_scans, _MAX_BACKOFF_EXPONENT)
            times.append(now + min(_MAX_BACKOFF_MS, self._interval_ms * 2**exponent))
        if self._store.has_facet_runs():
            times.append(now + self._interval_ms)
        return from_epoch_ms(min(times)) if times else None

    async def _sync_housekeeping(self) -> None:
        jobs = self.lifecycle.jobs
        at = (
            None
            if self.lifecycle.routes.source is not None
            else self._next_housekeeping()
        )
        if at is not None:
            await jobs.push(id=_HOUSEKEEPING_JOB, fn="housekeeping", time=at)
        elif jobs.get(_HOUSEKEEPING_JOB) is not None:
            # Also drops one left on a facet, which has no alarm.
            await jobs.cancel(_HOUSEKEEPING_JOB)

    async def _check_facets(self) -> None:
        if self.lifecycle.routes.source is not None:
            return
        for key, data in self._store.facet_owners():
            message: FiberRouteMessage = {"type": "check_runs"}
            try:
                remaining = await self.lifecycle.routes.to(
                    RouteAddress(key=key, data=data), message
                )
            except Exception:
                # One failing facet mustn't hold up the others; its entry
                # stays, so the next housekeeping retries it.
                _log.exception("Fiber recovery check failed for facet %s", key)
                continue
            # 0 rows left, or False: the facet was deleted (and cleaned up).
            if not remaining:
                self._store.forget_facet(key)

    # Events

    def _past_deadline(self, started: int) -> bool:
        return (
            self._scan_deadline_ms > 0 and now_ms() - started > self._scan_deadline_ms
        )

    def _emit_deadline(
        self, id: str, name: str, started: int, *, managed: bool | None
    ) -> None:
        payload: dict[str, Any] = {
            "fiberId": id,
            "fiberName": name,
            "reason": "scan_deadline_exceeded",
            "elapsedMs": now_ms() - started,
        }
        if managed is not None:
            payload["managed"] = managed
        self._emit("fiber:recovery:skipped", payload)

    def _emit_detected(self, context: FiberRecoveryContext, managed: bool) -> None:
        elapsed = now_ms() - epoch_ms(context.created_at)
        self._emit(
            "fiber:recovery:detected",
            {**_recovery_payload(context, managed), "elapsedMs": elapsed},
        )
        self._emit(
            "fiber:run:interrupted",
            {
                "fiberId": context.id,
                "fiberName": context.name,
                "managed": managed,
                "recoveryReason": context.recovery_reason,
                "elapsedMs": elapsed,
            },
        )

    def _emit(self, type: str, payload: dict[str, Any]) -> None:
        self.lifecycle.emit(type, payload)


def _new_id() -> str:
    return secrets.token_urlsafe(16)


def _message(error: BaseException) -> str:
    return str(error) or type(error).__name__


def _snapshot(text: str | None) -> Any:
    return json.loads(text) if text else None


def _statuses(
    status: FiberStatus | Sequence[FiberStatus] | None,
) -> tuple[FiberStatus, ...] | None:
    if status is None:
        return None
    if isinstance(status, str):
        return (cast(FiberStatus, status),)
    return tuple(status)


def _add_ledger_fields(context: FiberRecoveryContext, record: FiberLedgerRow) -> None:
    context.status = "interrupted"
    context.idempotency_key = record["idempotency_key"]
    metadata = json.loads(record["metadata_json"]) if record["metadata_json"] else None
    context.metadata = metadata if isinstance(metadata, dict) else None


def _recovery_payload(
    context: FiberRecoveryContext, managed: bool, started: int | None = None
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "fiberId": context.id,
        "fiberName": context.name,
        "managed": managed,
        "recoveryReason": context.recovery_reason,
    }
    if started is not None:
        payload["elapsedMs"] = now_ms() - started
    return payload


def _started(inspection: FiberInspection, *, accepted: bool) -> StartFiberResult:
    return StartFiberResult(
        fiber_id=inspection.fiber_id,
        name=inspection.name,
        status=inspection.status,
        created_at=inspection.created_at,
        idempotency_key=inspection.idempotency_key,
        snapshot=inspection.snapshot,
        error=inspection.error,
        metadata=inspection.metadata,
        started_at=inspection.started_at,
        settled_at=inspection.settled_at,
        accepted=accepted,
    )
