"""Durable background work: the Queue capability.

Port of upstream ``queue/queue.ts``. Queue owns no storage: each pushed item
becomes a Lifecycle job due immediately, whose ``fn`` is the callback name and
whose payload carries the item's payload and the Lifecycle that owns it.
Lifecycle runs the alarm loop, the retries, and the physical alarm
(``.design/scheduling_queue_tasks_api.md``).
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Any, override

from ..core.methods import get_bound_method, method_name
from ..core.platform_errors import is_code_update_reset, is_platform_failure
from ..core.retry import run_with_retries, validate_retry_options
from ..core.timing import epoch_ms, from_epoch_ms, now_ms
from ..core.types import JSONValue, RetryOptions
from ..lifecycle.capability import LifecycleCapability
from ..lifecycle.job_queue import retry_from_json, retry_to_json
from ..lifecycle.types import (
    JobContext,
    JobOutcome,
    LifecycleJob,
    RouteAddress,
    RouteContext,
)
from .types import (
    DispatchMessage,
    QueueCallback,
    QueueErrorHandler,
    QueueItem,
    QueueJobPayload,
    QueueRouteMessage,
    WireQueueItem,
)

__all__ = ("Queue",)

_log = logging.getLogger("agents.queue")

_SCHEMA_VERSION_KEY = "cf_agents:queue_schema_version"
_SCHEMA_VERSION = 1


class Queue(LifecycleCapability):
    """Durable background work for a Lifecycle object.

    Each pushed item becomes a Lifecycle job due immediately. Items run one at
    a time in push order when the alarm fires; a raising callback is retried
    per its retry policy, then dropped after a ``queue:error`` event and the
    ``on_error`` hook. Items survive the object leaving memory, so callbacks
    should be idempotent.

    Parameters
    ----------
    callbacks
        Callbacks by name; looked up first.
    target
        An object whose methods are callbacks too (e.g. the host itself);
        looked up after ``callbacks``.
    retry
        The default retry policy (3 attempts, 0.1 s base, 3 s max).
    on_error
        Observes an item's terminal failure. Runs as capability code, outside
        the host context.

    Raises
    ------
    TypeError, ValueError
        If ``retry`` is invalid.
    """

    def __init__(
        self,
        *,
        callbacks: Mapping[str, QueueCallback] | None = None,
        target: object | None = None,
        retry: RetryOptions | None = None,
        on_error: QueueErrorHandler | None = None,
    ) -> None:
        super().__init__("queue")
        self._callbacks = dict(callbacks or {})
        self._target = target
        self._retry = retry if retry is not None else RetryOptions()
        validate_retry_options(self._retry)
        self._on_error = on_error
        # The last due time handed out. The driver runs due jobs in time
        # order, so strictly increasing times keep push order within one ms.
        self._last_time_ms: int | None = None

    # Queue API

    async def push(
        self,
        callback: str | Callable[..., Any],
        payload: JSONValue = None,
        *,
        id: str | None = None,
        retry: RetryOptions | None = None,
    ) -> QueueItem:
        """Push one item; it runs in push order once this call returns.

        Parameters
        ----------
        callback
            A registered callback name, or a bound method of ``target``.
            Called as ``callback(payload, item)``.
        payload
            JSON data passed to the callback.
        id
            A stable id. Pushing an existing id replaces that item in place
            (keeping its position) and supersedes a run of it still in
            flight. Generated when omitted.
        retry
            Overrides the Queue's default retry policy for this item.

        Returns
        -------
        QueueItem
            The stored item.

        Raises
        ------
        ValueError
            If the callback isn't registered, or ``id`` is blank.
        TypeError
            If ``callback`` is a callable and the Queue has no ``target``.
        """
        await self.lifecycle.ready()
        name = self._callback_name(callback)
        if self._resolve(name) is None:
            raise ValueError(
                f"Unknown queue callback {name!r}: not registered on this Queue"
            )
        if retry is not None:
            validate_retry_options(retry)
        if id is not None and not id.strip():
            raise ValueError("Queue item ids must be non-empty")

        if self.lifecycle.routes.source is not None:
            wire: WireQueueItem = await self.lifecycle.routes.to_root(
                {
                    "type": "push",
                    "callback": name,
                    "payload": payload,
                    "id": id,
                    "retry": retry_to_json(retry) if retry is not None else None,
                }
            )
            item = _item_from_wire(wire)
        else:
            item = await self._insert(None, name, payload, id=id, retry=retry)
        self.lifecycle.emit("queue:create", {"callback": name, "id": item.id})
        return item

    async def get(self, id: str) -> QueueItem | None:
        """Return one pending item, or ``None``."""
        await self.lifecycle.ready()
        if self.lifecycle.routes.source is not None:
            wire = await self.lifecycle.routes.to_root({"type": "get", "id": id})
            return _item_from_wire(wire) if wire is not None else None
        return self._get_for_owner(None, id)

    async def list(
        self, callback: str | Callable[..., Any] | None = None
    ) -> Sequence[QueueItem]:
        """Return pending items in push order, optionally for one callback."""
        await self.lifecycle.ready()
        name = self._callback_name(callback) if callback is not None else None
        if self.lifecycle.routes.source is not None:
            wires = await self.lifecycle.routes.to_root(
                {"type": "list", "callback": name}
            )
            return [_item_from_wire(wire) for wire in wires]
        return self._list_for_owner(None, name)

    async def cancel(self, id: str) -> bool:
        """Cancel one pending item; return whether it existed."""
        await self.lifecycle.ready()
        if self.lifecycle.routes.source is not None:
            return await self.lifecycle.routes.to_root({"type": "cancel", "id": id})
        return await self._cancel_for_owner(None, id)

    async def cancel_all(self, callback: str | Callable[..., Any] | None = None) -> int:
        """Cancel every pending item, or every item for one callback.

        Returns
        -------
        int
            How many items were cancelled.
        """
        await self.lifecycle.ready()
        name = self._callback_name(callback) if callback is not None else None
        if self.lifecycle.routes.source is not None:
            return await self.lifecycle.routes.to_root(
                {"type": "cancel_all", "callback": name}
            )
        return await self._cancel_all_for_owner(None, name)

    # Lifecycle capability hooks

    @override
    async def on_start(self) -> None:
        """Record the storage version once (``.design/sql_schemas.md`` §4)."""
        storage = self.lifecycle.storage
        if await storage.get(_SCHEMA_VERSION_KEY) is None:
            await storage.put(_SCHEMA_VERSION_KEY, _SCHEMA_VERSION)

    @override
    async def on_job(self, context: JobContext) -> JobOutcome:
        """Run one due item handed over by the Lifecycle alarm loop."""
        job, attempt = context.job, context.attempt
        envelope = _envelope(job)
        if envelope is None:
            _log.error("Malformed queue item %s; dropping it", job.id)
            return None
        item = _item(job, envelope)

        if attempt > 1:
            self.lifecycle.emit(
                "queue:retry",
                {
                    "callback": job.fn,
                    "id": job.id,
                    "attempt": attempt,
                    "maxAttempts": self._retry_for(item).max_attempts,
                },
            )

        owner_path = envelope["owner_path"]
        if owner_path is not None:
            owner = RouteAddress(
                key=envelope["owner_path_key"] or owner_path, data=owner_path
            )
            return await self._dispatch_to_owner(owner, item)

        handler = self._resolve(job.fn)
        if handler is None:
            _log.error("Queue callback %r not found; dropping item %s", job.fn, job.id)
            return None
        await self.lifecycle.run_in_host_context(partial(handler, item.payload, item))
        return None

    @override
    async def on_job_error(self, context: JobContext, error: Exception) -> JobOutcome:
        """Report an item's terminal failure; the item is dropped."""
        job = context.job
        attempts = (job.retry or self._retry).max_attempts
        await self._report_failure(job.fn, job.id, error, attempts)
        return None

    @override
    async def on_route(self, context: RouteContext) -> Any:
        """Handle a Queue operation routed from another Lifecycle."""
        message: QueueRouteMessage = context.payload
        owner = context.source
        match message["type"]:
            case "push":
                retry = message["retry"]
                item = await self._insert(
                    owner,
                    message["callback"],
                    message["payload"],
                    id=message["id"],
                    retry=retry_from_json(retry) if retry is not None else None,
                )
                return _item_to_wire(item)
            case "get":
                found = self._get_for_owner(owner, message["id"])
                return _item_to_wire(found) if found is not None else None
            case "list":
                items = self._list_for_owner(owner, message["callback"])
                return [_item_to_wire(item) for item in items]
            case "cancel":
                return await self._cancel_for_owner(owner, message["id"])
            case "cancel_all":
                return await self._cancel_all_for_owner(owner, message["callback"])
            case "dispatch":
                await self._run_routed(_item_from_wire(message["item"]))
                return True
        raise ValueError(f"Unknown routed Queue message {message!r}")

    # Dispatch
    async def _dispatch_to_owner(
        self, owner: RouteAddress, item: QueueItem
    ) -> JobOutcome:
        # A facet's item runs inside the facet, which retries it itself, so
        # this is a single routed attempt.
        message: DispatchMessage = {"type": "dispatch", "item": _item_to_wire(item)}
        try:
            await self.lifecycle.routes.to(owner, message)
        except Exception as error:
            if is_platform_failure(error):
                raise  # Lifecycle preserves the item and defers it
            _log.exception("Error dispatching queue callback %r", item.callback)
            self.lifecycle.emit(
                "queue:error",
                {
                    "callback": item.callback,
                    "id": item.id,
                    "error": str(error),
                    "attempts": 0,
                },
            )
            await self._notify_on_error(error)
            # Leave the item due: a later alarm retries the dispatch.
            return "yield"
        return None

    async def _run_routed(self, item: QueueItem) -> None:
        """Run a routed item here, in its owning facet, with its own retries.

        Platform failures re-raise, so the root keeps the item and its alarm
        retries it in a fresh invocation.
        """
        handler = self._resolve(item.callback)
        if handler is None:
            _log.error(
                "Queue callback %r not found; dropping item %s", item.callback, item.id
            )
            return
        retry = self._retry_for(item)
        try:
            await run_with_retries(
                partial(self._attempt_routed, handler, item, retry),
                retry,
                should_retry=_retry_in_process,
            )
        except Exception as error:
            if is_platform_failure(error):
                raise
            await self._report_failure(
                item.callback, item.id, error, retry.max_attempts
            )

    async def _attempt_routed(
        self, handler: QueueCallback, item: QueueItem, retry: RetryOptions, attempt: int
    ) -> None:
        if attempt > 1:
            self.lifecycle.emit(
                "queue:retry",
                {
                    "callback": item.callback,
                    "id": item.id,
                    "attempt": attempt,
                    "maxAttempts": retry.max_attempts,
                },
            )
        await self.lifecycle.run_in_host_context(partial(handler, item.payload, item))

    async def _report_failure(
        self, callback: str, id: str, error: Exception, attempts: int
    ) -> None:
        _log.error(
            "Queue callback %r failed after %d attempts",
            callback,
            attempts,
            exc_info=error,
        )
        self.lifecycle.emit(
            "queue:error",
            {"callback": callback, "id": id, "error": str(error), "attempts": attempts},
        )
        await self._notify_on_error(error)

    async def _notify_on_error(self, error: Exception) -> None:
        if self._on_error is None:
            return
        try:
            await self._on_error(error)
        except Exception:
            # The observer failing must not fail the queue (upstream swallows).
            _log.exception("Queue on_error handler failed")

    # Storage

    async def _insert(
        self,
        owner: RouteAddress | None,
        callback: str,
        payload: JSONValue,
        *,
        id: str | None,
        retry: RetryOptions | None,
    ) -> QueueItem:
        # A stable-id push replaces the item in place, keeping its due time
        # and so its position in the queue.
        existing = self.lifecycle.jobs.get(id) if id is not None else None
        if existing is not None and self._get_for_owner(owner, existing.id) is not None:
            time = existing.time
        else:
            time = from_epoch_ms(self._next_time_ms())
        envelope: dict[str, JSONValue] = {
            "payload": payload,
            "owner_path": owner.data if owner is not None else None,
            "owner_path_key": owner.key if owner is not None else None,
        }
        job = await self.lifecycle.jobs.push(
            fn=callback,
            time=time,
            payload=envelope,
            id=id,
            retry=retry or self._retry,
        )
        return QueueItem(
            id=job.id,
            callback=job.fn,
            payload=payload,
            created_at=job.created_at,
            retry=job.retry,
        )

    def _next_time_ms(self) -> int:
        """Return the next strictly increasing due time, never before now."""
        if self._last_time_ms is None:
            # Seed from the stored tail, so a fresh instance never sorts a new
            # item ahead of ones an earlier instance queued.
            self._last_time_ms = max(
                (epoch_ms(job.time) for job, _ in self._owned_jobs()), default=0
            )
        time = max(now_ms(), self._last_time_ms + 1)
        self._last_time_ms = time
        return time

    def _owned_jobs(self) -> Sequence[tuple[LifecycleJob, QueueJobPayload]]:
        """Return every queue job with a valid envelope, in push order."""
        owned = []
        for job in self.lifecycle.jobs.list():
            envelope = _envelope(job)
            if envelope is not None:
                owned.append((job, envelope))
        return owned

    def _get_for_owner(self, owner: RouteAddress | None, id: str) -> QueueItem | None:
        job = self.lifecycle.jobs.get(id)
        if job is None:
            return None
        envelope = _envelope(job)
        if envelope is None or envelope["owner_path_key"] != _owner_key(owner):
            return None
        return _item(job, envelope)

    def _list_for_owner(
        self, owner: RouteAddress | None, callback: str | None
    ) -> Sequence[QueueItem]:
        key = _owner_key(owner)
        return [
            _item(job, envelope)
            for job, envelope in self._owned_jobs()
            if envelope["owner_path_key"] == key
            and (callback is None or job.fn == callback)
        ]

    async def _cancel_for_owner(self, owner: RouteAddress | None, id: str) -> bool:
        if self._get_for_owner(owner, id) is None:
            return False
        return await self.lifecycle.jobs.cancel(id)

    async def _cancel_all_for_owner(
        self, owner: RouteAddress | None, callback: str | None
    ) -> int:
        cancelled = 0
        for item in self._list_for_owner(owner, callback):
            if await self.lifecycle.jobs.cancel(item.id):
                cancelled += 1
        return cancelled

    async def _cleanup_route_prefix(self, prefix: str) -> None:
        """Remove the items owned by one routed Lifecycle subtree (internal).

        Called when a facet (and the facets under it) is deleted.
        """
        for job, envelope in self._owned_jobs():
            owner_path = envelope["owner_path"]
            if owner_path is None:
                continue
            key = envelope["owner_path_key"] or owner_path
            if key == prefix or key.startswith(f"{prefix}/"):
                await self.lifecycle.jobs.cancel(job.id)

    # Callbacks

    def _callback_name(self, callback: str | Callable[..., Any]) -> str:
        if isinstance(callback, str):
            return callback
        if self._target is None:
            raise TypeError(
                "This Queue has no target to look methods up on; pass the "
                "callback's registered name"
            )
        return method_name(self._target, callback)

    def _resolve(self, name: str) -> QueueCallback | None:
        """Return the callback registered as ``name`` (the dict, then ``target``)."""
        handler = self._callbacks.get(name)
        if handler is not None or self._target is None:
            return handler
        try:
            return get_bound_method(self._target, name)
        except (AttributeError, TypeError):
            return None

    def _retry_for(self, item: QueueItem) -> RetryOptions:
        return item.retry or self._retry


def _owner_key(owner: RouteAddress | None) -> str | None:
    return owner.key if owner is not None else None


def _envelope(job: LifecycleJob) -> QueueJobPayload | None:
    """Return the queue envelope a job carries, or ``None`` if it's malformed."""
    raw = job.payload
    if not isinstance(raw, dict) or "owner_path" not in raw:
        return None
    owner_path = raw["owner_path"]
    owner_path_key = raw.get("owner_path_key")
    return QueueJobPayload(
        payload=raw.get("payload"),
        owner_path=owner_path if isinstance(owner_path, str) else None,
        owner_path_key=owner_path_key if isinstance(owner_path_key, str) else None,
    )


def _item(job: LifecycleJob, envelope: QueueJobPayload) -> QueueItem:
    return QueueItem(
        id=job.id,
        callback=job.fn,
        payload=envelope["payload"],
        created_at=job.created_at,
        retry=job.retry,
    )


def _item_to_wire(item: QueueItem) -> WireQueueItem:
    return {
        "id": item.id,
        "callback": item.callback,
        "payload": item.payload,
        "created_at_ms": epoch_ms(item.created_at),
        "retry": retry_to_json(item.retry) if item.retry is not None else None,
    }


def _item_from_wire(wire: WireQueueItem) -> QueueItem:
    retry = wire["retry"]
    return QueueItem(
        id=wire["id"],
        callback=wire["callback"],
        payload=wire["payload"],
        created_at=from_epoch_ms(wire["created_at_ms"]),
        retry=retry_from_json(retry) if retry is not None else None,
    )


def _retry_in_process(error: Exception, _next_attempt: int) -> bool:
    # A code-update reset means this isolate is being replaced; retrying here
    # is pointless (upstream `shouldRetry: !isDurableObjectCodeUpdateReset`).
    return not is_code_update_reset(error)
