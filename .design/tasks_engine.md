# Tasks engine (design pass, step 7)

The engine behind `@task` / `step.do`: how runs and steps are stored,
claimed, replayed, retried, recovered, cancelled, and woken. The **API** was
designed earlier ([scheduling_queue_tasks_api.md](./scheduling_queue_tasks_api.md)
§2.3–2.8, §2.11); this doc is the internals [scope.md](./scope.md) left open.

Upstream: `packages/agents/src/tasks/` (v0.25: `tasks.ts` 1483 lines,
`replay.ts`, `engine-port.ts`, `store.ts`, `types.ts`, `errors.ts`,
`serialization.ts`, `duration.ts`), `docs/agents/tasks.md`, and its users
`chat/turn-task.ts`, `chat/recovery-task.ts`. Schema:
[sql_schemas.md](./sql_schemas.md) §5.

Status: **decided** (2026-10-05). §3 records the four questions and their answers.

---

## 1. How upstream's engine works

**Model: replay, not resumption.** Each *execution attempt* calls the
handler from its first line. `step.do(name, fn)` looks `name` up in the
run's step journal (`cf_agents_task_steps`): a completed step returns its
journaled result without running `fn`; the first unfinished step is the
"frontier", where real work happens. Sleeps and retry waits **end the
attempt** (the handler unwinds) and park the run until a durable deadline;
the next attempt replays to the same point and continues.

**Run states:** `pending` (accepted) → `running` (an attempt holds the
claim) → `waiting` (parked: `sleep` or `retry`) → … → `completed` /
`failed` / `cancelled`.

**Claims and fencing.** Starting an attempt writes a fresh random
**generation** into the run row, with `next_at` = now + step timeout + 30 s
slack (the *claim backstop*: if the isolate dies, that deadline wakes the
object and the run is reclaimed). Every later write by that attempt is
**fenced** (`WHERE generation = ?`); a superseded attempt's writes affect
nothing and it unwinds with `AttemptSupersededError`. The claim is refreshed
as steps run (throttled to once per 15 s).

**Wakes.** The run row's `next_at` is the source of truth. It's mirrored as
**one Lifecycle job** (`id = "task:" + run_id`, `fn = "wake"`, one dispatch
attempt) via `#syncWake`, called after every deadline or state change; a
terminal run cancels its job. The job's `on_job` dispatches the run.

**Dispatch** (`#dispatchRun`): claim and run an attempt, but only hold the
serial alarm loop for **5 s** (`DISPATCH_BUDGET_MS`); a longer attempt
detaches, is kept inside the alarm's memory-limit breaker with
`track_alarm_work`, and keeps running. Correctness never depends on the
await: the claim backstop is the durable wake.

**Warm start.** `run()` inserts the row, mirrors the wake, emits
`task:accepted`, and (unless the object is still starting) starts the first
attempt **immediately, detached**, in the caller's invocation.

**Step attempts** (`ReplayStep.#executeAttempt`): refresh the claim, emit
`task:step:started`, run `fn` raced against the step timeout and the run's
abort signal. On error:
- superseded → unwind; cancellation requested → settle cancelled;
- a code-update or memory-limit reset → rethrow, leaving the step `running`
  (the run defers to a fresh isolate / the breaker);
- other platform failures → normal retries, but past the last attempt
  rethrow (defer, don't fail);
- `NonRetryableError`, a serialization error, or the last attempt → the
  step and the run fail;
- otherwise → the step waits `retry` until `now + backoff` and the attempt
  ends (**retries are durable**: the run parks, another attempt replays).

**Replay checks:** a step name used twice in one attempt →
`DuplicateTaskStepError`; a journaled name replayed as the other kind (do vs
sleep) → `TaskReplayDivergedError`; a run whose definition is no longer
registered → fails with `MissingTaskDefinitionError` (never deleted or run
against another handler). Limits: 10,000 steps per run, 256-character names,
the `__cf` prefix reserved, 1 MiB per serialized value.

**`step.status(message)`** persists observable progress; a replayed attempt
starts **silent** and becomes "live" at the frontier, so old progress isn't
re-published. **`step.interrupted`** is `{name, attempt}` of the step a dead
isolate left `running` (`null` on a clean attempt), so handlers can check
before redoing irreversible work.

**Startup** (`on_start`): create the tables (version-gated), **reconcile**
(every `running` row with a generation is an interrupted attempt → due now;
non-terminal rows without `next_at` → due now), then mirror every wake.

**Cancellation:** a parked run settles `cancelled` at once; a live attempt
gets `cancel_requested = 1` and its signal aborted, and settles at its next
step boundary.

**Memory-limit breaker** (`on_memory_limit`): for the run whose wake struck,
strip its claim and push `next_at` to the backoff time (state kept, so the
replay still sees `step.interrupted`); when sealed, fail it with
`TaskMemoryLimitSealed`.

**Routed runs (facets):** the run row and journal stay on the facet; only
the wake is mirrored to the root (job `task:<owner_key>:<run_id>`). The
root's `on_job` routes `dispatch` back to the facet; memory-limit strikes
are forwarded too. (Upstream's user doc still says routed runs aren't
supported; the code now supports them.)

**Framework entry points** used by chat (step 12): `register(name, fn)` for
reserved `__cf…` definitions; `__DO_NOT_USE_WILL_BREAK__runAttached` (accept
and drive the first attempt in the caller, awaited); `…enqueue` (accept, but
leave the first attempt to the alarm, so it runs under the breaker); a routed
memory-limit handler bridge to the host's `onAlarmMemoryLimit`.

---

## 2. Python design

### 2.1 A straight port (no choice involved)

Everything in §1 is ported as-is: the replay model, states, generation
fencing, the claim backstop and its 30 s slack and 15 s refresh throttle, the
wake mirror (`task:` ids, one dispatch attempt, skip identical re-pushes),
the 5 s dispatch budget with `track_alarm_work`, warm start, startup
reconcile, the step error classification, durable retries, the replay checks
and limits, the status live gate, `step.interrupted`, cooperative
cancellation, the memory-limit policy, routed (facet) runs (§3 Q3), and the
framework entry points. Schema as in [sql_schemas.md](./sql_schemas.md) §5, with
`cf_agents:tasks_schema_version = 1`. Events as upstream (names and
camelCase payload keys, [observability.md](./observability.md) §3).

**Defaults (upstream's):** step retries 5 attempts, 1 s base delay,
exponential backoff (capped at one day), step timeout 5 minutes; `list()`
and `delete()` default `limit=100`; `retain=True`.

### 2.2 Where Python differs (proposed, for review)

1. **Control-flow signals are `BaseException`s, not `Exception`s.** The
   engine ends an attempt by raising through user code: `TaskSuspension`
   (sleep / retry wait), `TaskCancellation`, `AttemptSupersededError`.
   Upstream makes the first two non-`Error` values "so a step callback's
   `catch (error)` … is less likely to swallow it". The Python equivalent is
   subclassing `BaseException`, like `asyncio.CancelledError`, so a user's
   `except Exception:` can't swallow a sleep or a cancellation. All three are
   internal (not exported). Consistent with "only `Exception`s reach
   `on_error`" ([agent_api.md](./agent_api.md) §1.10).
2. **Abort signals become task cancellation** (already decided, §2.6 of the
   API doc): each step attempt is its own `asyncio.Task`, awaited with
   `asyncio.wait(timeout=...)`; timeout, run cancellation, or supersession
   call `.cancel()` on it and settle the attempt without waiting for it to
   comply. The run's live attempt is tracked as `ActiveAttempt(generation,
   task, current_step_task)`.
3. **One wire format for values: plain JSON.** Inputs, step results,
   metadata, and results go through `json.dumps` (not the SDK's
   dataclass/`datetime` encoder) and must be at most 1 MiB, or
   `TaskSerializationError`. A step that returns a dataclass or `datetime`
   would come back as a `dict` / number on replay, so Python refuses it up
   front rather than letting the first run and the replay see different
   types.
4. **`step.do` returns the journaled value even the first time** (see Q2):
   the result is serialized, stored, and the *decoded* copy is returned, so a
   handler sees identical values live and on replay. Upstream returns the
   original object live and the decoded one on replay.
5. **`None` is "no result".** Upstream stores `undefined` as SQL `NULL` and
   `null` as JSON `"null"`; Python has one `None`, stored as `NULL` (same rule
   as RPC results and state).
6. **Durations follow the SDK rule** (`timedelta` or seconds): `step.sleep(name,
   duration)`, `step.sleep_until(name, datetime)`, `StepRetries(delay=...)`,
   `timeout=...`, and the `Tasks(step_retries=..., step_timeout=...)`
   defaults. Upstream's millisecond numbers and `"10 seconds"` strings
   aren't ported (decided, API doc §2.7).
7. **Timestamps in snapshots are aware UTC `datetime`s** (`created_at`,
   `started_at`, `wake_at`, `settled_at`), from the epoch-ms columns.
8. **Definitions:** `Tasks(definitions={...}, target=obj)`, the dict first,
   then `@task` methods collected from `type(target).__mro__` (decided, API
   doc §2.11). `@task` handles are typed from the method:
   `async def research(self, input: In, step: TaskStep) -> Out` gives a
   `TaskHandle[In, Out]` whose `get()` returns a run typed with `Out`.
9. **Framework entry points are private methods** (`_register`,
   `_run_attached`, `_enqueue`, `_set_routed_memory_limit_handler`), used by
   `Agent` / `AIChatAgent`, instead of upstream's module-level setters and
   `__DO_NOT_USE_WILL_BREAK__` names. `Agent` passes `target=self`, so no
   definition resolver is needed.
10. **The step-engine port is a plain class** (`TaskStepEngine` over
    `TaskStore`), not upstream's object-of-closures; same operations.

### 2.3 Module layout (`src/agents/tasks/`)

| Module | Holds | Upstream |
| --- | --- | --- |
| `types.py` | `TaskStep` (Protocol), `TaskStepAttempt`, `StepRetries`, `TaskReceipt`, the run snapshot(s), `TaskRunState`, `TaskWaitReason`, `TaskError`, row `TypedDict`s, wake payload, route messages, `ResolvedStepPolicy` | `types.ts`, `options.ts` |
| `errors.py` | `NonRetryableError`, `DuplicateTaskStepError`, `TaskReplayDivergedError`, `MissingTaskDefinitionError`, `TaskSerializationError`, `StepTimeoutError`; internal `TaskSuspension`, `TaskCancellation`, `AttemptSupersededError` | `errors.ts`, `replay.ts` |
| `serialization.py` | `serialize_task_value` / `deserialize_task_value` (JSON, 1 MiB) | `serialization.ts` |
| `store.py` | `TaskStore`: DDL, row access, fenced writes, row → snapshot | `store.ts` |
| `engine.py` | `TaskStepEngine`: journal reads/writes for one claimed attempt | `engine-port.ts` |
| `replay.py` | `ReplayStep` (implements `TaskStep`), retry backoff, policy resolution | `replay.ts` |
| `tasks.py` | `Tasks` capability: acceptance, claiming, dispatch, settlement, reconcile, wakes, routing, control | `tasks.ts` |
| `decorator.py` | `@task` descriptor and `TaskHandle` | (Python only) |

`duration.ts` isn't needed (Python durations, §2.2 item 6).

### 2.4 `Agent` integration

`Agent` installs `Tasks(target=self, on_error=...)` (upstream installs it on
every agent). `self.tasks` is public, as upstream. `on_error` is notified of
terminal run failures, inside the host context, as for schedules and queues
([agent_api.md](./agent_api.md) §1.10).

---

## 3. Questions to decide

**Q1. Run snapshots: one class per state, or one class with optional
fields?** **Decided: (a) one dataclass per state.** Upstream's `TaskRunSnapshot` is a union discriminated by `state`
(only `completed` has `result`, only `failed` has `error`, only `waiting` has
`reason` / `wake_at`, …).
- (a) **One dataclass per state** (`PendingRun`, `RunningRun`, `WaitingRun`,
  `CompletedRun[Out]`, `FailedRun`, `CancelledRun`), with
  `type TaskRun[Out] = PendingRun | … | CancelledRun`. `match run:` /
  `isinstance` narrows, and `result` exists only where it's meaningful. Same
  choice as the chat tool parts ([chat_models.md](./chat_models.md)).
  **Recommended.**
- (b) **One `TaskRun` dataclass** with `state` and every field optional
  (`result`, `error`, `wake_at`, …). Simpler to construct; callers check
  `state` and then trust fields to be set.

**Q2. What `step.do` returns on the first execution.** **Decided: (a) the
journaled (decoded) value.** Why not upstream: TypeScript's `TaskValue` type
makes step results plain JSON at compile time, so the live object and its
round-tripped copy match; Python enforces nothing at runtime, and ordinary
values serialize without error but change on replay (a tuple becomes a
list and is no longer hashable; `{1: "a"}` comes back as `{"1": "a"}`). Those
bugs would only surface on the rare replay path (after a sleep, retry, or
restart). Returning the decoded copy makes every attempt see the same
values; it costs one `json.loads` of the string just journaled. Also
considered: (c) return the original but raise `TaskSerializationError` when
`json.loads(text) != value` (upstream's behavior, strict); rejected as less
forgiving.
- (a) **The journaled (decoded) value**, so live and replayed runs see
  identical values (§2.2 item 4). Costs one `json.loads` per completed step.
  **Recommended.**
- (b) **The callback's original object**, as upstream; a handler can then
  behave differently live and on replay (e.g. a tuple live, a list on
  replay).

**Q3. Routed (facet) runs: implement now, or in step 8?** **Decided
(revised 2026-10-05): port them.** First decided as "refuse on facets until
step 8", because upstream's routed path is unpublished (its docs list "no
runs on routed sub-agents"; its own `AIChatAgent` and Think keep facet turns
on fibers) and can't be verified on real facets before step 8. Revised
because neither is a reason to leave out a useful feature: without it a
sub-agent can't use `@task` at all while the same agent works as a root,
upstream's code is complete, and if upstream changes it we adapt. Ported now
with the in-process transport tests (as Queue and Scheduler's facet paths);
**step 8 verifies it on real facets.** Queue and
Scheduler already carry their facet paths, tested with an in-process
transport.
- (a) **Now**, with the same in-process test transport. **Recommended**, for
  consistency, and so step 8 only wires the transport.
- (b) In step 8 with the rest of sub-agents.

**Q4. The run listing/deleting API shape.** **Decided: (a) keyword
arguments**: `list(*, definition=None, status=None, limit=100) ->
Sequence[TaskRun]` (newest first) and `delete(*, status=None,
settled_before=None, limit=100) -> int` (oldest settled first), where
`status` is one state or an iterable of states, `delete`'s is typed to the
terminal states, and `settled_before` is an aware `datetime`. Same rule as
`list_schedules` and `Queue.list` (API doc §2.9); TypeScript's option bags
only stand in for keyword arguments. Upstream: `list({definition,
status, limit})`, `delete({status, settledBefore, limit})`.
- (a) **Keyword arguments**: `list(definition=None, status=None, limit=100)`,
  `delete(status=None, settled_before=None, limit=100)`, where `status` is a
  state or a sequence of states. **Recommended** (the rule from API doc §2.9).
- (b) Options dataclasses.

---

## 4. Verification plan

- **Unit tests** (fake runtime): the replay model (journal hits, the
  frontier, duplicates, divergence), durable sleeps and retries, timeouts and
  task cancellation of a callback that swallows `CancelledError`, generation
  fencing against a superseded attempt, the claim backstop and startup
  reconcile, warm start versus queued versus attached, cancellation of parked
  and live runs, retention, the memory-limit policy, routed runs (Q3),
  `@task` handles, and events.
- **On `workerd`:** a run with a step, a 2-second durable sleep across a real
  alarm, a retried step, cancellation of a live run, and **an interrupted
  run**: an attempt whose isolate is reset mid-step (`ctx.abort`), replayed by
  the claim backstop with `step.interrupted` set.

---

## 5. Implementation notes (step 7, 2026-10-05)

Implemented in `src/agents/tasks/` as laid out in §2.3, plus `registry.py`
(the target → `Tasks` map behind `@task` handles, separate so `@task` and
`Tasks` don't import each other).

1. **Fenced writes use `UPDATE … RETURNING`** to learn whether the fence
   let them through (upstream reads `cursor.rowsWritten`, which `Sql`
   doesn't expose). Verified on `workerd`'s SQLite.
2. **`StepRetries` fields default to `None`, meaning "use the Tasks
   default"**, including `backoff` (the API doc's earlier snippet defaulted it
   to `"exponential"`, which would have overridden a capability default).
   Capability defaults: `Tasks(step_retries=..., step_timeout=...)`.
3. **Filtering by state uses `json_each(?)`** with a JSON array parameter,
   so `list` / `delete` queries stay literal SQL.
4. **`asyncio.CancelledError` from outside still propagates:** only the
   engine's own signals and `Exception`s are settled; a step attempt
   cancelled by `Tasks.cancel` becomes `TaskCancellation`, and one whose run
   was taken over becomes `AttemptSupersededError`.
5. **`Agent` installs `Tasks(target=self, on_error=...)`** as the public
   `self.tasks`, after WebSockets (upstream's order).

**Verified on `workerd`** (`verify/results/tasks_client.py`): a run that
parks on a 2-second sleep (status "sleeping") and completes after a real
alarm, with the step before the sleep run only once and a failing step
retried durably; cancelling a run mid-step (the step never finished); and
**a real isolate crash mid-step** (`ctx.abort`): the next request's startup
found the run still claimed, made it due, and the replay saw
`step.interrupted = "boom"`, re-ran the step, and completed.

**Platform fact found on the way:** `ctx.abort` discards writes not yet
flushed in the same turn (Durable Object write coalescing), so a crash in
the same turn as `run()` loses the run entirely, as it would upstream. The
crash test calls `await ctx.storage.sync()` first so the claim and journal
are durable before the crash, which is the case the engine handles.

**Routed runs (ported after the Q3 revision):** as upstream, the run and its
journal stay on the facet; `_sync_wake` sends `sync_wake` to the root, which
keeps one mirror job `task:<owner_key>:<run_id>`; the root's `on_job`
dispatches it back (`dispatch`, raced against the 5 s budget; past it the
call is tracked by the alarm's breaker; a non-platform failure leaves the
mirror due); memory-limit strikes on a mirror are forwarded
(`memory_limit`), applied on the facet, and passed to the facet host's
`_on_alarm_memory_limit` (`Agent` wires the bridge); `_cleanup_route_prefix`
cancels a deleted facet subtree's mirrors. **Two adaptations:** the facet
answers `dispatch` with its next wake in epoch ms (a `Reschedule` holding a
`datetime` can't cross a real RPC transport), and `memory_limit` carries
`sealed` / `next_ms` instead of the whole context (whose job record is the
root's). Tested with the in-process transport; verified on real facets in
step 8.
