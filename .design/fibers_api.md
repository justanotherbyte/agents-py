# Fibers (Python)

Durable execution through registered closures, checkpoints, and a recovery
hook. Upstream: the "legacy fibers" engine in `../agents/packages/agents/src/index.ts`
(`runFiber` ~4083, `startFiber` ~4107, recovery `_checkRunFibers` ~4486) and
`docs/agents/durable-execution.md`.

Related: [scope.md](./scope.md) §2.7, [sql_schemas.md](./sql_schemas.md) §11–12,
[lifecycle_capabilities.md](./lifecycle_capabilities.md) §5 (host jobs),
[scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) (Tasks).

---

## 1. Why it's in scope

- **Upstream calls it legacy, but it is not deprecated.** Deprecation is
  deferred "until the facet migration lands" (`design/rfc-fibers.md:193`).
- **Chat turns hosted in facets run on fibers, not Tasks.** The Tasks
  capability doesn't accept runs on routed sub-agents yet
  (`ai-chat/src/index.ts:893`). With `AIChatAgent` and facets both in scope,
  the engine is required.
- **The public API (plain and managed fibers) is in scope too (decided).**

### Fibers vs Tasks

| | Fibers | Tasks |
| --- | --- | --- |
| After a crash | The closure is gone; `on_fiber_recovered(ctx)` gets the last checkpoint, and **the user writes the recovery** | The handler is **re-run from the top**; completed steps return their saved results |
| Checkpoints | `stash(data)`: one snapshot that replaces the previous one | One saved result per named `step.do` |
| Retries | None automatically | Per step, with backoff |

---

## 2. API

### 2.1 Methods on `Agent`

```python
class Agent:
    # Plain fibers
    async def run_fiber(
        self, name: str, fn: Callable[[FiberContext], Awaitable[T]]
    ) -> T: ...
    def stash(
        self, data: Any
    ) -> None: ...  # the current fiber, via a ContextVar; raises outside one

    # Managed fibers
    async def start_fiber(
        self,
        name: str,
        fn: Callable[[FiberContext], Awaitable[None]],
        *,
        fiber_id: str | None = None,
        idempotency_key: str | None = None,
        metadata: dict[str, Any] | None = None,
        wait_for_completion: bool = False,
    ) -> StartFiberResult: ...
    async def inspect_fiber(self, fiber_id: str) -> FiberInspection | None: ...
    async def inspect_fiber_by_key(
        self, idempotency_key: str
    ) -> FiberInspection | None: ...
    async def list_fibers(
        self,
        *,
        status: FiberStatus | Sequence[FiberStatus] | None = None,
        name: str | None = None,
        limit: int | None = None,
    ) -> Sequence[FiberInspection]: ...  # read-only result
    async def cancel_fiber(self, fiber_id: str, reason: str | None = None) -> bool: ...
    async def cancel_fiber_by_key(
        self, idempotency_key: str, reason: str | None = None
    ) -> bool: ...
    async def resolve_fiber(
        self, fiber_id: str, result: FiberRecoveryResult
    ) -> bool: ...
    async def delete_fibers(
        self,
        *,
        status: FiberStatus | Sequence[FiberStatus] | None = None,
        settled_before: datetime | None = None,
        limit: int | None = None,
    ) -> int: ...

    # Recovery hook (override)
    async def on_fiber_recovered(
        self, ctx: FiberRecoveryContext
    ) -> FiberRecoveryResult | None: ...

    # Keep-alive
    async def keep_alive(self) -> Disposable: ...
    async def keep_alive_while(self, fn: Callable[[], Awaitable[T]]) -> T: ...
```

All callbacks are `async def` (§2.3 of the scheduling doc).

### 2.2 Types

Dataclasses, per the SDK convention ([utilities.md](./utilities.md) §5).

```python
FiberStatus = Literal[
    "pending", "running", "completed", "aborted", "interrupted", "error"
]


@dataclass(slots=True, kw_only=True)
class FiberContext:
    id: str
    stash: Callable[[Any], None]


@dataclass(slots=True, kw_only=True)
class FiberInspection:
    fiber_id: str
    name: str
    status: FiberStatus
    created_at: datetime  # timezone-aware UTC
    idempotency_key: str | None = None
    snapshot: Any = None
    error: str | None = None
    metadata: dict[str, Any] | None = None
    started_at: datetime | None = None
    settled_at: datetime | None = None


@dataclass(slots=True, kw_only=True)
class StartFiberResult(FiberInspection):
    accepted: bool  # False: an existing fiber matched fiber_id / idempotency_key


@dataclass(slots=True, kw_only=True)
class FiberRecoveryContext:
    id: str
    name: str
    snapshot: Any  # the last stash(), or None
    created_at: datetime
    recovery_reason: Literal["interrupted"] = "interrupted"
    # managed fibers only
    status: FiberStatus | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] | None = None


# Recovery results: one class per status
@dataclass(slots=True, kw_only=True)
class FiberCompleted:
    snapshot: Any = None
    metadata: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class FiberErrored:
    error: str | None = None
    snapshot: Any = None


@dataclass(slots=True, kw_only=True)
class FiberAborted:
    reason: str | None = None
    snapshot: Any = None


@dataclass(slots=True, kw_only=True)
class FiberInterrupted:
    reason: str | None = None
    snapshot: Any = None


FiberRecoveryResult = FiberCompleted | FiberErrored | FiberAborted | FiberInterrupted
```

### 2.3 Agent options

Fields of `AgentOptions` ([agent_api.md](./agent_api.md) §1.7); durations are
`timedelta | float` seconds.

| Option | Default | Upstream |
| --- | --- | --- |
| `keep_alive_interval` | `30` s | `keepAliveIntervalMs` |
| `fiber_recovery_max_age` | 24 h (`None` = keep forever) | `fiberRecoveryMaxAgeMs` (`0` = keep forever) |
| `fiber_recovery_scan_deadline` | `10` s | `fiberRecoveryScanDeadlineMs` |
| `fiber_recovery_hook_timeout` | `10` s (framework hooks) | `fiberRecoveryHookTimeoutMs` |

### 2.4 Example

```python
class ResearchAgent(Agent):
    async def research(self, topic: str) -> str:
        async def body(ctx: FiberContext) -> str:
            sources = await search(topic)
            ctx.stash({"topic": topic, "sources": sources})
            return await summarize(sources)

        return await self.run_fiber("research", body)

    async def handle_webhook(self, event: dict) -> StartFiberResult:
        async def send(ctx: FiberContext) -> None:
            ctx.stash({"event_id": event["id"], "target": event["target"]})
            await send_reply(event["target"])

        return await self.start_fiber(
            "send-reply",
            send,
            idempotency_key=f"webhook:{event['id']}",
            wait_for_completion=True,
        )

    async def on_fiber_recovered(
        self, ctx: FiberRecoveryContext
    ) -> FiberRecoveryResult | None:
        if ctx.name == "send-reply" and ctx.snapshot:
            await send_recovery_message(ctx.snapshot["target"])
            return FiberCompleted()
        return None
```

---

## 3. Decisions (differences from upstream)

1. **Cancellation cancels the asyncio task.** `cancel_fiber` marks the record
   `aborted` and calls `.cancel()` on the fiber's task if it is running in this
   isolate, so the code gets `CancelledError` at its next `await` and actually
   stops. Upstream is cooperative (`ctx.signal`), and a callback that ignores
   it keeps running. This is consistent with the Tasks decision
   ([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md) §2.6).
   - **Consequence:** `FiberContext` has **no `signal`**.
   - **A body that swallows `CancelledError`:** the record is already
     `aborted` and waiters are released, as upstream; the code just isn't
     stopped.
   - **Verified ([platform_verification.md](./platform_verification.md) §2.8):** whether an in-flight request stops
     depends on the client: `pyfetch` / `workers.fetch` abort while waiting
     for headers; raw `js.fetch`, httpx, and chunk-by-chunk body reads complete
     in the background. Documented, no SDK mechanism (§6 there).
2. **`FiberContext.snapshot` is removed.** Upstream documents it as always
   `null` during a run; snapshots only arrive in `FiberRecoveryContext`.
3. **Recovery results are one dataclass per status**
   (`FiberCompleted(snapshot=…)`), instead of upstream's
   `{ status: "completed", … }` objects.
4. **`keep_alive()` returns a `Disposable`** rather than a bare function, so it
   fits an `AsyncExitStack` ([core_disposable_store.md](./core_disposable_store.md)).
5. **`this.stash()`'s AsyncLocalStorage** becomes a `contextvars.ContextVar`.

---

## 4. How it works

### 4.1 `run_fiber`

```
run_fiber(name, fn)
  ├─ INSERT cf_agents_runs (id, name, snapshot=NULL, created_at)
  ├─ add id to the in-memory set of running fibers
  ├─ (facet) register (self_path, id) in the root's cf_agents_facet_runs
  ├─ keep_alive()                                  (reference-counted)
  ├─ run fn(ctx) as a task, with the stash ContextVar set
  │    └─ stash(d) → synchronous UPDATE cf_agents_runs.snapshot
  │                  (and cf_agents_fibers.snapshot for managed fibers)
  ├─ when the body ends: UPDATE completed_at, outcome ('completed'|'error'|'aborted'), error_message
  ├─ DELETE the run row
  ├─ remove from the running set; release keep_alive
  ├─ (facet) unregister from the root, only if the row was deleted
  └─ return the result / re-raise
```

- **The row exists only while the fiber runs.** A row left over after a wake,
  not in the running set, means the process died mid-fiber.
- **Finishing takes two writes.** It records `outcome` **before** deleting the
  row, so a scan can tell "the body finished but the delete failed" (delete
  silently) from "interrupted" (recover).
- **`stash` writes synchronously**, so the snapshot is saved before it returns,
  and it **replaces** the previous snapshot rather than merging. Data must be
  JSON-serializable.
- **Errors propagate** to the caller. There are no automatic retries.

### 4.2 `keep_alive`

A reference count. While it's above 0, the `KeepAlive` capability keeps its
`keep-alive` job in the Lifecycle job queue, due every `keep_alive_interval`
(upstream: the host job `cf:keep-alive`; see
[fibers_engine.md](./fibers_engine.md)).
That prevents idle eviction (~70–140 s). When the count drops to 0, the job is
removed. In a facet, the lease is acquired from the root (facets have no
alarm).

### 4.3 Recovery scan

**When it runs:** on wake (from startup) and from the `Fibers` capability's
`housekeeping` job (upstream: the host job `cf:housekeeping`), which wakes an
agent that has no clients connected.

For each `cf_agents_runs` row that isn't in the running set:
1. **`completed_at` is set:** delete the row, with no hook. For a managed fiber
   whose record is still `pending`/`running`, first copy the recorded outcome
   into the record.
2. **Its managed record is already terminal** (e.g. `aborted`): delete the row.
3. **Otherwise:** build a `FiberRecoveryContext`. For a managed fiber, mark the
   record `interrupted` and add `idempotency_key`, `metadata`, and `status`.
   Then:
   - call the framework hook first (`_handle_internal_fiber_recovery`, with a
     timeout; chat turns are handled here), and if it doesn't handle the fiber,
     call `on_fiber_recovered(ctx)`;
   - for a managed fiber, a returned `FiberRecoveryResult` sets its final
     status; returning `None` leaves it `interrupted`.
4. **Delete the row** if the hook succeeded, if the fiber is managed (its
   record holds the outcome), or if the row is older than
   `fiber_recovery_max_age` (emitting `fiber:recovery:skipped`,
   `max_age_exceeded`). Otherwise keep it for a later scan.

Then **records with no run row**: managed records stuck at `pending` /
`running` with no `cf_agents_runs` row are marked `interrupted` and go through
the hook.

**Limits:**
- the scan stops after `fiber_recovery_scan_deadline`;
- a scan that makes no progress while work is still pending backs off the
  housekeeping wake exponentially, capped at 5 minutes, so a hook that always
  raises can't wake the DO continuously;
- only one scan runs at a time.

### 4.4 Managed fibers

**The record** (`cf_agents_fibers`) outlives the run:

```
pending ──▶ running ──▶ completed | error | aborted
                  └──▶ interrupted
```

- **`start_fiber`:**
  - look up an existing record by `fiber_id` and by `idempotency_key`; if they
    match different fibers, raise;
  - **if a record exists:** return it with `accepted=False`. With
    `wait_for_completion`, wait for it to finish first: if it's running in this
    isolate, wait on the in-memory notification; otherwise run a recovery scan,
    then wait;
  - **otherwise:** insert the record as `pending`, then start the body in the
    background (`pending` → `running`, run on the §4.1 engine with
    `managed=True`, and write the final status when it ends). Return the
    `pending` record, or the finished one with `wait_for_completion`.
  - A blank `fiber_id` or `idempotency_key` raises.
- **The body's return value is discarded:** `start_fiber` confirms acceptance;
  results come from inspecting the record.
- **`cancel_fiber`:** only for non-terminal records. Sets `aborted` with
  `error = reason`, cancels the task (§3, item 1), notifies waiters, and returns
  `True`. Returns `False` for unknown or already-terminal records.
- **`resolve_fiber`:** only for **`interrupted`** records. Applies the result
  and returns `True`; otherwise returns `False`.
- **`delete_fibers`:** defaults to deleting `completed`, `error`, and `aborted`
  records. **`interrupted` records are kept unless requested explicitly.**
  `settled_before` filters on `completed_at`.
- **Waiting** uses in-memory notifications per fiber, released whenever a record
  reaches a terminal status.

### 4.5 Facets

- **The run row, snapshots, and `on_fiber_recovered` stay in the facet.** The
  facet is `self` in the hook.
- **The root holds what needs an alarm:** the keep-alive lease, and an index
  entry in `cf_agents_facet_runs` that root housekeeping uses
  (`_check_facet_run_fibers`) to call back into an idle facet and run its
  recovery scan.
- **A stale index entry** (whose run row is already gone) is removed when root
  housekeeping visits the facet.

---

## 5. Storage

`cf_agents_runs` (running fibers), `cf_agents_fibers` (managed-fiber records),
and `cf_agents_facet_runs` (root-side facet index). The DDL is in
[sql_schemas.md](./sql_schemas.md) §11.2 and §12.

---

## 6. Resolved items

- **JS `fetch` cancellation:** verified (depends on the client,
  [platform_verification.md](./platform_verification.md) §2.8); decided: document it ([platform_verification.md](./platform_verification.md) §6).
- ~~Observability events~~: **decided:** `fiber:run:*` and `fiber:recovery:*`
  are emitted with upstream's names and payloads
  ([observability.md](./observability.md)).
