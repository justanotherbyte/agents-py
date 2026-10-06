"""Durable, replayable background work: the Tasks capability.

Port of upstream ``tasks/tasks.ts`` (``.design/tasks_engine.md``). Tasks owns
the run and step tables, the definitions, run acceptance, generation-fenced
claiming, and due-run processing. A run's ``next_at`` is the source of truth
for when it wakes; it's mirrored as one Lifecycle job (``task:<run_id>``).
Handlers replay from their first line on every attempt; completed steps
return their journaled results.
"""

import asyncio
import logging
import secrets
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, Literal, override

from ..core.platform_errors import is_platform_failure
from ..core.timing import epoch_ms, from_epoch_ms, now_ms
from ..core.types import Duration, JSONValue, RetryOptions
from ..lifecycle.capability import LifecycleCapability
from ..lifecycle.types import (
    JobContext,
    JobOutcome,
    LifecycleJob,
    MemoryLimitContext,
    Reschedule,
    RouteAddress,
    RouteContext,
)
from .decorator import TaskHandle, task
from .engine import TaskStepEngine
from .errors import (
    AttemptSupersededError,
    MissingTaskDefinitionError,
    TaskCancellation,
    TaskSuspension,
)
from .registry import register_target
from .replay import ReplayStep, resolve_step_policy
from .serialization import deserialize_task_value, serialize_task_value
from .store import TaskStore, row_to_run
from .types import (
    DispatchMessage,
    MemoryLimitHandler,
    MemoryLimitMessage,
    ResolvedStepPolicy,
    StepInterruption,
    StepRetries,
    SyncWakeMessage,
    TaskErrorHandler,
    TaskHandler,
    TaskReceipt,
    TaskRouteMessage,
    TaskRun,
    TaskRunRow,
    TaskRunState,
    TaskWakePayload,
    TerminalTaskState,
)

__all__ = ("Tasks",)

_log = logging.getLogger("agents.tasks")

_SCHEMA_VERSION_KEY = "cf_agents:tasks_schema_version"
_SCHEMA_VERSION = 1

_DEFAULT_POLICY = ResolvedStepPolicy(
    retry_limit=5, retry_delay_ms=1000, backoff="exponential", timeout_ms=5 * 60 * 1000
)
# Added to the step timeout to form the claim deadline: the durable backstop
# that wakes the object when a claimed attempt's isolate disappears.
_CLAIM_SLACK_MS = 30_000
# How long one queue-driven attempt may hold the serial alarm loop before it
# detaches (correctness never depends on the inline await).
_DISPATCH_BUDGET_S = 5.0
_WAKE_JOB_PREFIX = "task:"
_WAKE_JOB_FN = "wake"
# One dispatch attempt: a platform failure keeps the job and the platform
# retries the alarm, while the run's claim deadline stays the durable wake.
_WAKE_JOB_RETRY = RetryOptions(max_attempts=1)
_MAX_DEFINITION_NAME_LENGTH = 256
_LIVE = ("pending", "waiting", "running")
_TERMINAL: tuple[TerminalTaskState, ...] = ("completed", "failed", "cancelled")

type _StartMode = Literal["warm", "queued", "attached"]


@dataclass(slots=True, kw_only=True)
class _ActiveAttempt:
    generation: str
    step: ReplayStep
    task: "asyncio.Task[None]"


class Tasks(LifecycleCapability):
    """Durable, replayable background work for a Lifecycle object.

    Each run of a definition survives the object leaving memory: completed
    steps return their journaled results, sleeps keep their deadlines, and
    an interrupted attempt replays from the first unfinished step.

    Parameters
    ----------
    definitions
        Task definitions by name, each called as ``handler(input, step)``.
    target
        An object whose ``@task`` methods are definitions too (named by the
        method); ``definitions`` wins on a clash.
    step_retries
        Default step retries (5 attempts, 1 s, exponential).
    step_timeout
        Default timeout of one step attempt (5 minutes).
    on_error
        Observes a run's terminal failure, in the host context.
    """

    def __init__(
        self,
        *,
        definitions: Mapping[str, TaskHandler] | None = None,
        target: object | None = None,
        step_retries: StepRetries | None = None,
        step_timeout: Duration | None = None,
        on_error: TaskErrorHandler | None = None,
    ) -> None:
        super().__init__("tasks")
        self._defaults = resolve_step_policy(
            _DEFAULT_POLICY, step_retries, step_timeout
        )
        self._definitions: dict[str, TaskHandler] = {}
        if target is not None:
            self._definitions.update(_collect_task_methods(target))
            register_target(target, self)
        self._definitions.update(definitions or {})
        self._registered: dict[str, TaskHandler] = {}
        self._on_error = on_error
        self._active: dict[str, _ActiveAttempt] = {}
        self._background: set[asyncio.Task[None]] = set()
        self._store_instance: TaskStore | None = None
        self._routed_memory_limit_handler: MemoryLimitHandler | None = None

    @property
    def _store(self) -> TaskStore:
        if self._store_instance is None:
            self._store_instance = TaskStore(self.lifecycle.sql)
        return self._store_instance

    @property
    def _claim_timeout_ms(self) -> int:
        return self._defaults.timeout_ms + _CLAIM_SLACK_MS

    # Definitions

    def _resolve(self, name: str) -> TaskHandler | None:
        return self._definitions.get(name) or self._registered.get(name)

    def _check_definition(self, name: str) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("Task definition names must be non-empty strings")
        if len(name) > _MAX_DEFINITION_NAME_LENGTH:
            raise ValueError(
                f"Task definition names must be at most "
                f"{_MAX_DEFINITION_NAME_LENGTH} characters"
            )
        if name.startswith("__cf"):
            raise ValueError(
                'Task definition names must not use the reserved "__cf" prefix'
            )
        if self._resolve(name) is None:
            raise ValueError(
                f"Unknown task definition {name!r}: not declared on this Tasks"
            )

    def _register(self, name: str, handler: TaskHandler) -> None:
        """Register a reserved (``__cf``-prefixed) framework definition (internal).

        Call once per name from the host's constructor, so every wake
        registers the same definitions.
        """
        if not name.startswith("__cf"):
            raise ValueError(f'_register() needs a "__cf"-prefixed name, not {name!r}')
        if name in self._definitions or name in self._registered:
            raise ValueError(f"Task definition {name!r} is already registered")
        self._registered[name] = handler

    # Starting runs

    async def run(
        self,
        definition: str,
        input: JSONValue = None,
        *,
        idempotency_key: str | None = None,
        run_id: str | None = None,
        metadata: dict[str, JSONValue] | None = None,
        retain: bool = True,
    ) -> TaskReceipt:
        """Durably accept a run and start it; return without waiting for it.

        The same ``idempotency_key`` (or ``run_id``) joins the existing run
        (``accepted=False``) instead of starting another.

        Parameters
        ----------
        definition
            The definition's name.
        input
            JSON passed to the handler.
        idempotency_key
            Deduplicates repeated starts onto one run.
        run_id
            A run id to use (generated when omitted).
        metadata
            JSON kept with the run.
        retain
            Keep the run's record after it settles (default ``True``).

        Raises
        ------
        ValueError
            If the definition isn't registered, or an id or key conflicts.
        TaskSerializationError
            If ``input`` or ``metadata`` isn't plain JSON, or is too big.
        """
        self._check_definition(definition)
        return await self._accept(
            definition, input, idempotency_key, run_id, metadata, retain, "warm"
        )

    async def _run_attached(
        self, definition: str, input: JSONValue, **options: Any
    ) -> TaskReceipt:
        """Accept a run and drive its first attempt in the caller (internal).

        Reserved names are allowed. Returns when the attempt reaches a durable
        boundary.
        """
        receipt = await self._accept_reserved(definition, input, "attached", **options)
        if receipt.accepted:
            await self._execute_run(receipt.run_id)
        return receipt

    async def _enqueue(
        self, definition: str, input: JSONValue, **options: Any
    ) -> TaskReceipt:
        """Accept a run and leave its first attempt to the alarm (internal).

        Reserved names are allowed. The alarm runs it inside the memory-limit
        breaker.
        """
        return await self._accept_reserved(definition, input, "queued", **options)

    async def _accept_reserved(
        self, definition: str, input: JSONValue, mode: _StartMode, **options: Any
    ) -> TaskReceipt:
        if self._resolve(definition) is None:
            raise ValueError(f"Unknown task definition {definition!r}")
        return await self._accept(
            definition,
            input,
            options.get("idempotency_key"),
            options.get("run_id"),
            options.get("metadata"),
            options.get("retain", True),
            mode,
        )

    async def _accept(
        self,
        definition: str,
        input: JSONValue,
        idempotency_key: str | None,
        run_id: str | None,
        metadata: dict[str, JSONValue] | None,
        retain: bool,
        mode: _StartMode,
    ) -> TaskReceipt:
        await self.lifecycle.ready()
        if run_id is not None and not run_id:
            raise ValueError("run_id must be non-empty when given")
        if idempotency_key is not None and not idempotency_key:
            raise ValueError("idempotency_key must be non-empty when given")
        input_json = serialize_task_value(input, f"input for task {definition!r}")
        metadata_json = serialize_task_value(
            metadata, f"metadata for task {definition!r}"
        )

        existing = (self._store.get_run(run_id) if run_id is not None else None) or (
            self._store.get_run_by_key(idempotency_key)
            if idempotency_key is not None
            else None
        )
        if existing is not None:
            return await self._join(existing, definition, idempotency_key, run_id)

        run_id = run_id if run_id is not None else f"task_{secrets.token_urlsafe(16)}"
        now = now_ms()
        self._store.sql(
            """INSERT INTO cf_agents_task_runs
                 (run_id, definition, input, state, metadata, idempotency_key,
                  retain, attempt, next_at, cancel_requested, created_at,
                  updated_at)
               VALUES (?, ?, ?, 'pending', ?, ?, ?, 0, ?, 0, ?, ?)""",
            run_id,
            definition,
            input_json,
            metadata_json,
            idempotency_key,
            retain,
            now,
            now,
            now,
        )
        await self._sync_wake(run_id)
        self._emit(
            "task:accepted",
            {"runId": run_id, "definition": definition, "accepted": True},
        )
        # Warm start: begin now when past startup; the durable deadline above
        # is authoritative either way.
        if mode == "warm" and self.lifecycle.status() != "starting":
            self._spawn(self._execute_run(run_id))
        return TaskReceipt(
            run_id=run_id,
            definition=definition,
            accepted=True,
            state="pending",
            created_at=from_epoch_ms(now),
        )

    async def _join(
        self,
        existing: TaskRunRow,
        definition: str,
        idempotency_key: str | None,
        run_id: str | None,
    ) -> TaskReceipt:
        if existing["definition"] != definition:
            via = "run id" if run_id is not None else "idempotency key"
            raise ValueError(
                f"Task run {existing['run_id']!r} belongs to definition "
                f"{existing['definition']!r}; refusing to reuse its {via} for "
                f"{definition!r}"
            )
        # The key decides deduplication: a run found by key joins even if a
        # different (unused) run id was asked for. The reverse is a conflict.
        if (
            idempotency_key is not None
            and existing["idempotency_key"] != idempotency_key
        ):
            raise ValueError(
                f"Task run {existing['run_id']!r} has idempotency key "
                f"{existing['idempotency_key']!r}; refusing to join it with "
                f"{idempotency_key!r}"
            )
        # A previous accept may have failed after inserting the row; repair
        # its wake rather than report a run nothing will wake.
        await self._sync_wake(existing["run_id"])
        return TaskReceipt(
            run_id=existing["run_id"],
            definition=definition,
            accepted=False,
            state=existing["state"],
            created_at=from_epoch_ms(existing["created_at"]),
        )

    # Lifecycle capability hooks

    @override
    async def on_start(self) -> None:
        """Create the tables once, then make every live run's wake sane."""
        storage = self.lifecycle.storage
        if (await storage.get(_SCHEMA_VERSION_KEY) or 0) < _SCHEMA_VERSION:
            self._store.ensure_tables()
            await storage.put(_SCHEMA_VERSION_KEY, _SCHEMA_VERSION)
        self._reconcile()
        await self._sync_all_wakes()

    @override
    async def on_job(self, context: JobContext) -> JobOutcome:
        """Drive one due run handed over by the Lifecycle alarm loop.

        On the root, a mirror of a facet's run is dispatched to that facet.
        """
        wake = _wake_payload(context.job)
        owner = _owner(wake)
        if owner is not None:
            return await self._dispatch_to_owner(owner, wake["run_id"])
        return await self._dispatch_run(wake["run_id"])

    @override
    async def on_memory_limit(self, context: MemoryLimitContext) -> None:
        """Contain a run whose wake hit the alarm memory-limit breaker.

        The claim is stripped and the deadline pushed to the backoff, so the
        run keeps its state (a replay still sees ``step.interrupted``) and
        startup won't make it due again early; a sealed strike fails it.
        """
        job = context.executing
        if job is None or job.capability != self.capability_id:
            return
        wake = _wake_payload(job)
        next_ms = epoch_ms(context.next_time) if context.next_time is not None else None
        owner = _owner(wake)
        if owner is None:
            await self._apply_memory_limit(wake["run_id"], context.sealed, next_ms)
            return
        # The run and its claim live on the facet: apply the policy there.
        # A non-sealed strike matters too, or the facet's next startup would
        # read the stale claim as an interruption and make it due now.
        message: MemoryLimitMessage = {
            "type": "memory_limit",
            "run_id": wake["run_id"],
            "sealed": context.sealed,
            "next_ms": next_ms,
        }
        try:
            await self.lifecycle.routes.to(owner, message)
        except Exception:
            _log.exception(
                "Failed to route memory-limit policy for task run %r", wake["run_id"]
            )

    async def _apply_memory_limit(
        self, run_id: str, sealed: bool, next_ms: int | None
    ) -> None:
        """Apply the memory-limit policy to a run whose storage is here."""
        if sealed:
            await self._settle_failed(
                run_id,
                None,
                "TaskMemoryLimitSealed",
                "Sealed by the alarm memory-limit circuit breaker after "
                "consecutive Durable Object memory-limit resets.",
            )
            return
        if next_ms is None:
            return
        self._store.sql(
            """UPDATE cf_agents_task_runs
               SET generation = NULL,
                   next_at = CASE WHEN next_at IS NULL OR next_at < ?
                                  THEN ? ELSE next_at END,
                   updated_at = ?
               WHERE run_id = ? AND state IN ('pending', 'waiting', 'running')""",
            next_ms,
            next_ms,
            now_ms(),
            run_id,
        )
        await self._sync_wake(run_id)

    # Dispatch and wakes

    async def _dispatch_run(self, run_id: str) -> JobOutcome:
        """Drive one due run to its next durable boundary.

        Holds the serial alarm loop for at most the dispatch budget.
        """
        active = self._active.get(run_id)
        if active is not None:
            # A live attempt in this isolate: push the backstop forward so the
            # due job doesn't hot-loop the alarm while it works.
            self._refresh_claim(run_id)
            self.lifecycle.track_alarm_work(active.task)
            return self._wake_outcome(run_id)
        attempt = asyncio.ensure_future(self._execute_run(run_id))
        done, _ = await asyncio.wait({attempt}, timeout=_DISPATCH_BUDGET_S)
        if not done:
            # Keep running, inside this alarm's memory-limit breaker; the
            # claim backstop is the durable wake.
            live = self._active.get(run_id)
            self.lifecycle.track_alarm_work(live.task if live is not None else attempt)
            return self._wake_outcome(run_id)
        attempt.result()  # a platform failure re-enters the driver's deferral
        return self._wake_outcome(run_id)

    def _refresh_claim(self, run_id: str) -> None:
        now = now_ms()
        self._store.sql(
            """UPDATE cf_agents_task_runs SET next_at = ?, updated_at = ?
               WHERE run_id = ? AND state = 'running'""",
            now + self._claim_timeout_ms,
            now,
            run_id,
        )

    async def _dispatch_to_owner(self, owner: RouteAddress, run_id: str) -> JobOutcome:
        """Run a facet's due run on that facet (the root side of dispatch).

        The facet awaits its attempt in full, so the root races its own
        await against the dispatch budget; past it, the call keeps running,
        tracked inside this alarm's memory-limit breaker. A memory-limit
        reset on the facet reaches the root as the platform's own error, so
        it's attributed here like a local attempt's.
        """
        message: DispatchMessage = {"type": "dispatch", "run_id": run_id}
        call = asyncio.ensure_future(self.lifecycle.routes.to(owner, message))
        done, _ = await asyncio.wait({call}, timeout=_DISPATCH_BUDGET_S)
        if not done:
            # The facet's own sync_wake, sent when the attempt settles,
            # supersedes whatever this returns.
            self.lifecycle.track_alarm_work(call)
            return None
        try:
            next_ms = call.result()
        except Exception as error:
            if is_platform_failure(error):
                raise
            _log.exception("Error dispatching routed task run %r", run_id)
            return "yield"  # stay due: a later alarm retries the dispatch
        return Reschedule(at=from_epoch_ms(next_ms)) if next_ms is not None else None

    async def _dispatch_routed_run(self, run_id: str) -> int | None:
        """Run one due run here, on the facet that owns it; return its next wake.

        No budget here: the root races its await of this call instead.
        """
        if run_id in self._active:
            self._refresh_claim(run_id)
        else:
            await self._execute_run(run_id)
        return self._store.next_at(run_id)

    @override
    async def on_route(self, context: RouteContext) -> Any:
        """Handle a Tasks message routed between a facet and the root."""
        message: TaskRouteMessage = context.payload
        match message["type"]:
            case "sync_wake":
                owner = context.source
                if owner is None:
                    raise ValueError("A routed sync_wake must come from a facet")
                return await self._mirror(
                    f"{_WAKE_JOB_PREFIX}{owner.key}:{message['run_id']}",
                    message["run_id"],
                    message["next_ms"],
                    owner,
                )
            case "dispatch":
                return await self._dispatch_routed_run(message["run_id"])
            case "memory_limit":
                await self._apply_memory_limit(
                    message["run_id"], message["sealed"], message["next_ms"]
                )
                # The facet's own Lifecycle never sees the root's alarm, so
                # this is how its host hears about the strike.
                if self._routed_memory_limit_handler is not None:
                    next_ms = message["next_ms"]
                    context_for_host = MemoryLimitContext(
                        sealed=message["sealed"],
                        next_time=from_epoch_ms(next_ms)
                        if next_ms is not None
                        else None,
                    )
                    await self.lifecycle.run_in_host_context(
                        partial(self._routed_memory_limit_handler, context_for_host)
                    )
                return True
        raise ValueError(f"Unknown routed Tasks message {message!r}")

    def _set_routed_memory_limit_handler(self, handler: MemoryLimitHandler) -> None:
        """Bridge memory-limit strikes on this facet's runs to the host (internal)."""
        self._routed_memory_limit_handler = handler

    async def _cleanup_route_prefix(self, prefix: str) -> None:
        """Cancel the root's mirrors of runs owned by a deleted facet subtree.

        The runs themselves are deleted with the facets' storage (internal).
        """
        for job in self.lifecycle.jobs.list():
            owner = _owner(_wake_payload(job))
            if owner is not None and (
                owner.key == prefix or owner.key.startswith(f"{prefix}/")
            ):
                await self.lifecycle.jobs.cancel(job.id)

    def _wake_outcome(self, run_id: str) -> JobOutcome:
        """Return the wake job's outcome from the run row's deadline."""
        next_ms = self._store.next_at(run_id)
        return Reschedule(at=from_epoch_ms(next_ms)) if next_ms is not None else None

    async def _sync_wake(self, run_id: str) -> bool:
        """Mirror a run's deadline into the job queue (cancel it once settled).

        Returns
        -------
        bool
            ``False`` when the queue already had exactly this wake.
        """
        next_ms = self._store.next_at(run_id)
        if self.lifecycle.routes.source is not None:
            # The run stays here; only its wake goes to the root, which owns
            # the alarm.
            message: SyncWakeMessage = {
                "type": "sync_wake",
                "run_id": run_id,
                "next_ms": next_ms,
            }
            return await self.lifecycle.routes.to_root(message)
        return await self._mirror(f"{_WAKE_JOB_PREFIX}{run_id}", run_id, next_ms, None)

    async def _mirror(
        self,
        job_id: str,
        run_id: str,
        next_ms: int | None,
        owner: RouteAddress | None,
    ) -> bool:
        """Push or cancel one wake job; ``False`` if it already matched."""
        if next_ms is None:
            await self.lifecycle.jobs.cancel(job_id)
            return True
        existing = self.lifecycle.jobs.get(job_id)
        if (
            existing is not None
            and existing.fn == _WAKE_JOB_FN
            and epoch_ms(existing.time) == next_ms
            and existing.retry is not None
            and existing.retry.max_attempts == _WAKE_JOB_RETRY.max_attempts
        ):
            return False
        await self.lifecycle.jobs.push(
            id=job_id,
            fn=_WAKE_JOB_FN,
            time=from_epoch_ms(next_ms),
            payload={
                "run_id": run_id,
                "owner_path": owner.data if owner is not None else None,
                "owner_path_key": owner.key if owner is not None else None,
            },
            retry=_WAKE_JOB_RETRY,
        )
        return True

    async def _sync_all_wakes(self) -> None:
        run_ids = self._store.live_runs_with_deadlines()
        pushed = False
        for run_id in run_ids:
            pushed = await self._sync_wake(run_id) or pushed
        # Pushes re-arm the alarm; a reconcile that wrote nothing must
        # recover a lost alarm itself.
        if run_ids and not pushed:
            await self.lifecycle.jobs.rearm()

    def _reconcile(self) -> None:
        """Make interrupted attempts and deadline-less runs due now (fresh start)."""
        now = now_ms()
        # No attempt survives an isolate, so every claimed row was interrupted.
        self._store.sql(
            """UPDATE cf_agents_task_runs SET next_at = ?, updated_at = ?
               WHERE state = 'running' AND generation IS NOT NULL""",
            now,
            now,
        )
        self._store.sql(
            """UPDATE cf_agents_task_runs SET next_at = ?, updated_at = ?
               WHERE state IN ('pending', 'waiting') AND next_at IS NULL""",
            now,
            now,
        )

    # Execution

    async def _execute_run(self, run_id: str) -> None:
        """Claim one due run and drive an attempt to its next durable boundary."""
        if run_id in self._active:
            return
        row = self._store.get_run(run_id)
        if row is None or row["state"] in _TERMINAL:
            return
        now = now_ms()
        if row["cancel_requested"] == 1:
            await self._settle_cancelled(run_id, None, row["cancel_reason"])
            return
        if row["next_at"] is not None and row["next_at"] > now:
            return
        handler = self._resolve(row["definition"])
        if handler is None:
            error = MissingTaskDefinitionError(row["definition"])
            _log.error("%s", error)
            await self._settle_failed(run_id, None, type(error).__name__, str(error))
            await self._observe_error(error)
            return

        # An unclean interruption: the previous attempt's isolate is gone. The
        # claim below replays the handler; the interrupted step is evidence.
        interrupted = (
            self._interrupted_step(run_id) if row["state"] == "running" else None
        )
        if row["state"] == "running":
            self._emit(
                "task:attempt:interrupted",
                {
                    "runId": run_id,
                    "definition": row["definition"],
                    "attempt": row["attempt"],
                    "step": interrupted.name if interrupted is not None else None,
                },
            )
        generation = secrets.token_urlsafe(16)
        attempt = row["attempt"] + 1
        self._store.sql(
            """UPDATE cf_agents_task_runs
               SET state = 'running', attempt = ?, generation = ?,
                   started_at = coalesce(started_at, ?), next_at = ?,
                   wait_reason = NULL, updated_at = ?
               WHERE run_id = ? AND state IN ('pending', 'waiting', 'running')""",
            attempt,
            generation,
            now,
            now + self._claim_timeout_ms,
            now,
            run_id,
        )
        await self._sync_wake(run_id)
        self._emit(
            "task:attempt:started",
            {"runId": run_id, "definition": row["definition"], "attempt": attempt},
        )
        step = ReplayStep(
            self._engine(run_id, row["definition"], generation, now),
            starts_live=attempt == 1,
            interrupted=interrupted,
        )
        attempt_task = asyncio.ensure_future(
            self._run_attempt(row, handler, generation, step)
        )
        self._active[run_id] = _ActiveAttempt(
            generation=generation, step=step, task=attempt_task
        )
        attempt_task.add_done_callback(partial(self._attempt_done, run_id, generation))
        await attempt_task

    def _attempt_done(self, run_id: str, generation: str, _task: object) -> None:
        current = self._active.get(run_id)
        if current is not None and current.generation == generation:
            del self._active[run_id]

    async def _run_attempt(
        self, row: TaskRunRow, handler: TaskHandler, generation: str, step: ReplayStep
    ) -> None:
        """Run one claimed attempt and record its outcome (generation-fenced)."""
        run_id = row["run_id"]
        task_input = deserialize_task_value(row["input"])
        try:
            output = await self.lifecycle.run_in_host_context(
                partial(handler, task_input, step)
            )
            result_json = serialize_task_value(
                output, f"result of task {row['definition']!r}"
            )
        except BaseException as thrown:  # signals are BaseExceptions too
            if isinstance(thrown, asyncio.CancelledError) or not isinstance(
                thrown,
                Exception | TaskSuspension | TaskCancellation | AttemptSupersededError,
            ):
                raise
            await self._settle_thrown(row, generation, thrown)
            return
        now = now_ms()
        settled = self._store.sql(
            """UPDATE cf_agents_task_runs
               SET state = 'completed', result = ?, generation = NULL, next_at = NULL,
                   settled_at = ?, updated_at = ?
               WHERE run_id = ? AND generation = ? AND state = 'running'
               RETURNING retain""",
            result_json,
            now,
            now,
            run_id,
            generation,
        )
        if settled:
            self._emit(
                "task:completed", {"runId": run_id, "definition": row["definition"]}
            )
            await self._finish_settlement(run_id, retain=bool(settled[0]["retain"]))

    async def _settle_thrown(
        self, row: TaskRunRow, generation: str, thrown: BaseException
    ) -> None:
        """Record an attempt that didn't complete."""
        run_id = row["run_id"]
        if isinstance(thrown, AttemptSupersededError):
            return  # a newer attempt owns the run
        if isinstance(thrown, Exception) and is_platform_failure(thrown):
            # Not an application outcome: the run mustn't settle. The claim
            # backstop is the durable wake; the next invocation replays it.
            raise thrown
        if isinstance(thrown, TaskCancellation):
            await self._settle_cancelled(run_id, generation, thrown.reason)
            return
        if isinstance(thrown, TaskSuspension):
            current = self._store.get_run(run_id)
            if current is not None and current["cancel_requested"] == 1:
                await self._settle_cancelled(
                    run_id, generation, current["cancel_reason"]
                )
                return
            parked = self._store.sql(
                """UPDATE cf_agents_task_runs
                   SET state = 'waiting', wait_reason = ?, next_at = ?,
                       generation = NULL, updated_at = ?
                   WHERE run_id = ? AND generation = ? AND state = 'running'
                   RETURNING run_id""",
                thrown.reason,
                thrown.wake_at_ms,
                now_ms(),
                run_id,
                generation,
            )
            if parked:
                self._emit(
                    "task:waiting",
                    {
                        "runId": run_id,
                        "definition": row["definition"],
                        "reason": thrown.reason,
                        "wakeAt": thrown.wake_at_ms,
                    },
                )
                await self._sync_wake(run_id)
            return
        assert isinstance(thrown, Exception)
        if await self._settle_failed(
            run_id, generation, type(thrown).__name__, str(thrown)
        ):
            _log.error(
                "Task run %r (definition %r) failed",
                run_id,
                row["definition"],
                exc_info=thrown,
            )
        await self._observe_error(thrown)

    async def _observe_error(self, error: Exception) -> None:
        if self._on_error is None:
            return
        try:
            await self.lifecycle.run_in_host_context(partial(self._on_error, error))
        except Exception:
            # The observer failing must not fail Tasks (upstream swallows).
            _log.exception("Tasks on_error handler failed")

    def _engine(
        self, run_id: str, definition: str, generation: str, claimed_at_ms: int
    ) -> TaskStepEngine:
        def emit(type: str, payload: dict[str, Any]) -> None:
            self._emit(type, {"runId": run_id, "definition": definition, **payload})

        return TaskStepEngine(
            store=self._store,
            run_id=run_id,
            generation=generation,
            claim_timeout_ms=self._claim_timeout_ms,
            claimed_at_ms=claimed_at_ms,
            claim_refresh_after_ms=_CLAIM_SLACK_MS // 2,
            defaults=self._defaults,
            emit=emit,
        )

    def _interrupted_step(self, run_id: str) -> StepInterruption | None:
        rows = self._store.sql(
            """SELECT step_name, attempt FROM cf_agents_task_steps
               WHERE run_id = ? AND state = 'running'
               ORDER BY started_at DESC LIMIT 1""",
            run_id,
        )
        if not rows:
            return None
        return StepInterruption(name=rows[0]["step_name"], attempt=rows[0]["attempt"])

    # Settlement

    async def _settle_cancelled(
        self, run_id: str, generation: str | None, reason: str | None
    ) -> None:
        now = now_ms()
        if generation is not None:
            rows = self._store.sql(
                """UPDATE cf_agents_task_runs
                   SET state = 'cancelled', cancel_requested = 1, cancel_reason = ?,
                       generation = NULL, next_at = NULL, settled_at = ?, updated_at = ?
                   WHERE run_id = ? AND generation = ? AND state = 'running'
                   RETURNING definition, retain""",
                reason,
                now,
                now,
                run_id,
                generation,
            )
        else:
            rows = self._store.sql(
                """UPDATE cf_agents_task_runs
                   SET state = 'cancelled', cancel_requested = 1, cancel_reason = ?,
                       generation = NULL, next_at = NULL, settled_at = ?, updated_at = ?
                   WHERE run_id = ? AND state IN ('pending', 'waiting', 'running')
                   RETURNING definition, retain""",
                reason,
                now,
                now,
                run_id,
            )
        if rows:
            self._emit(
                "task:cancelled",
                {
                    "runId": run_id,
                    "definition": rows[0]["definition"],
                    "reason": reason,
                },
            )
            await self._finish_settlement(run_id, retain=bool(rows[0]["retain"]))

    async def _settle_failed(
        self, run_id: str, generation: str | None, error_name: str, message: str
    ) -> bool:
        now = now_ms()
        if generation is not None:
            rows = self._store.sql(
                """UPDATE cf_agents_task_runs
                   SET state = 'failed', error_name = ?, error_message = ?,
                       generation = NULL, next_at = NULL, settled_at = ?, updated_at = ?
                   WHERE run_id = ? AND generation = ? AND state = 'running'
                   RETURNING definition, retain""",
                error_name,
                message,
                now,
                now,
                run_id,
                generation,
            )
        else:
            rows = self._store.sql(
                """UPDATE cf_agents_task_runs
                   SET state = 'failed', error_name = ?, error_message = ?,
                       generation = NULL, next_at = NULL, settled_at = ?, updated_at = ?
                   WHERE run_id = ? AND state IN ('pending', 'waiting', 'running')
                   RETURNING definition, retain""",
                error_name,
                message,
                now,
                now,
                run_id,
            )
        if not rows:
            return False
        self._emit(
            "task:failed",
            {"runId": run_id, "definition": rows[0]["definition"], "error": error_name},
        )
        await self._finish_settlement(run_id, retain=bool(rows[0]["retain"]))
        return True

    async def _finish_settlement(self, run_id: str, *, retain: bool) -> None:
        if not retain:
            self._store.delete_run(run_id)
        await self._sync_wake(run_id)

    # Inspection and control

    def handle(self, definition: str) -> TaskHandle[Any, Any]:
        """Return a handle scoped to one definition's runs.

        Raises
        ------
        ValueError
            If the definition isn't registered.
        """
        self._check_definition(definition)
        return TaskHandle(self, definition)

    async def get(self, run_id: str) -> TaskRun[Any] | None:
        """Return one run by id, or ``None``."""
        return await self._snapshot(run_id, None)

    async def get_by_idempotency_key(self, idempotency_key: str) -> TaskRun[Any] | None:
        """Return the run started with ``idempotency_key``, or ``None``."""
        await self.lifecycle.ready()
        row = self._store.get_run_by_key(idempotency_key)
        return row_to_run(row) if row is not None else None

    async def list(
        self,
        *,
        definition: str | None = None,
        status: TaskRunState | Iterable[TaskRunState] | None = None,
        limit: int = 100,
    ) -> Sequence[TaskRun[Any]]:
        """Return runs, newest first, optionally by definition and state."""
        await self.lifecycle.ready()
        rows = self._store.list_runs(definition, _states(status), limit)
        return [row_to_run(row) for row in rows]

    async def cancel(self, run_id: str, reason: str | None = None) -> bool:
        """Cancel a run; return whether a live run accepted the request.

        A parked run settles at once; a running attempt is cancelled and
        settles at its next step boundary.
        """
        await self.lifecycle.ready()
        row = self._store.get_run(run_id)
        if row is None or row["state"] in _TERMINAL:
            return False
        active = self._active.get(run_id)
        if active is None:
            await self._settle_cancelled(run_id, None, reason)
            return True
        now = now_ms()
        self._store.sql(
            """UPDATE cf_agents_task_runs
               SET cancel_requested = 1, cancel_reason = ?, next_at = ?, updated_at = ?
               WHERE run_id = ?""",
            reason,
            now,
            now,
            run_id,
        )
        active.step.cancel_current()
        await self._sync_wake(run_id)
        return True

    async def delete(
        self,
        *,
        status: TerminalTaskState | Iterable[TerminalTaskState] | None = None,
        settled_before: datetime | None = None,
        limit: int = 100,
    ) -> int:
        """Delete settled runs and their journals, oldest first; return how many."""
        await self.lifecycle.ready()
        states = _states(status) or _TERMINAL
        before = epoch_ms(settled_before) if settled_before is not None else None
        rows = self._store.settled_runs(states, before, limit)
        for row in rows:
            self._store.delete_run(row["run_id"])
            self._emit(
                "task:deleted",
                {"runId": row["run_id"], "definition": row["definition"]},
            )
        return len(rows)

    async def _snapshot(
        self, run_id: str, definition: str | None
    ) -> TaskRun[Any] | None:
        await self.lifecycle.ready()
        row = self._store.get_run(run_id)
        if row is None or (definition is not None and row["definition"] != definition):
            return None
        return row_to_run(row)

    # Plumbing

    def _emit(self, type: str, payload: dict[str, Any]) -> None:
        self.lifecycle.emit(type, payload)

    def _spawn(self, work: Any) -> None:
        task = asyncio.ensure_future(work)
        self._background.add(task)
        task.add_done_callback(self._spawned_done)

    def _spawned_done(self, task: "asyncio.Task[None]") -> None:
        self._background.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            # A warm start's platform failure: the claim backstop retries it.
            _log.warning("Task warm start ended early: %r", error)


def _wake_payload(job: LifecycleJob) -> TaskWakePayload:
    """Return a wake job's payload (the run id from the job id if malformed)."""
    raw = job.payload if isinstance(job.payload, dict) else {}
    run_id = raw.get("run_id")
    owner_path = raw.get("owner_path")
    owner_path_key = raw.get("owner_path_key")
    return TaskWakePayload(
        run_id=run_id
        if isinstance(run_id, str)
        else job.id.removeprefix(_WAKE_JOB_PREFIX),
        owner_path=owner_path if isinstance(owner_path, str) else None,
        owner_path_key=owner_path_key if isinstance(owner_path_key, str) else None,
    )


def _owner(wake: TaskWakePayload) -> RouteAddress | None:
    """Return the facet that owns a mirrored run, or ``None`` for a local run."""
    owner_path = wake["owner_path"]
    if owner_path is None:
        return None
    return RouteAddress(key=wake["owner_path_key"] or owner_path, data=owner_path)


def _states(status: Any) -> Sequence[Any]:
    if status is None:
        return ()
    if isinstance(status, str):
        return (status,)
    return tuple(status)


def _collect_task_methods(target: object) -> dict[str, TaskHandler]:
    """Return ``target``'s ``@task`` methods, bound, by name (nearest wins)."""
    found: dict[str, TaskHandler] = {}
    seen: set[str] = set()
    for cls in type(target).__mro__:
        for name, value in vars(cls).items():
            if name in seen:
                continue
            seen.add(name)
            if isinstance(value, task):
                found[value.name] = partial(value.fn, target)
    return found
