# Fibers engine (design pass, step 9)

The machinery behind `run_fiber`, `stash`, `start_fiber` and the rest of the
managed-fiber API, `keep_alive`, and recovery of interrupted fibers, in
top-level agents and in facets. The **API** is in
[fibers_api.md](./fibers_api.md) (§2, decided); its §4 describes the
mechanics. This doc is how they're built in Python.

Upstream: the "legacy fibers" section of `packages/agents/src/index.ts`
(`keepAlive` ~3455, the managed-fiber ledger ~3524–4060, `_runFiberInternal`
~4250, `_checkRunFibers` ~4486, `_syncHostJobs` / `onJob` ~4809–4960) and the
facet half in `dynamic-agents/dynamic-agents.ts` (`acquireKeepAlive`,
`registerRun`, `checkRunFibers`, `checkRunFibersAtPath`, `unregisterRun`).

Status: **decided and implemented** (2026-10-05). §4 records the question
and answer; §5 records how the implementation turned out; §6 records a fix for an
upstream bug in facet keep-alive leases.

---

## 1. How upstream works

All of it lives on `Agent`; nothing is a capability.

- **Run rows.** `_runFiberInternal` inserts a `cf_agents_runs` row, adds the
  id to an in-memory set, takes a keep-alive ref, and runs the body under an
  `AsyncLocalStorage` holding `stash`. On the way out it records
  `completed_at` / `outcome` first, then deletes the row (two writes, so a scan
  can tell "settled but the delete failed" from "interrupted").
- **Managed fibers.** `cf_agents_fibers` is a ledger that outlives the run
  (`pending → running → completed | error | aborted`, or `interrupted` by
  recovery). `startFiber` inserts `pending` and runs `_executeManagedFiber` in
  the background; in-memory waiters back `waitForCompletion`. `interrupted`
  counts as terminal.
- **Keep-alive.** `_keepAliveRefs` is a counter. While it's above zero, the
  host job `cf:keep-alive` is kept in the Lifecycle queue, due every
  `keepAliveIntervalMs`, and its `onJob` reschedules itself.
- **Housekeeping.** The host job `cf:housekeeping` exists while any recovery
  is pending (orphan run rows, ledger rows stuck in `pending`/`running`, or
  facet-run index rows). Every alarm runs the scan (`_onAlarmHousekeeping`
  wraps `onAlarm`), then `_syncHostJobs` re-derives both host jobs. A scan
  that makes no progress backs the wake off exponentially (capped at 5
  minutes).
- **Recovery scan** (`_checkRunFibers`): runs at startup (before the user's
  `onStart`), from housekeeping, and before waiting on a fiber left by a dead
  process. One at a time, with a soft deadline. The internal hook
  (`_handleInternalFiberRecovery`, used by chat) is timed out; the user's
  `onFiberRecovered` isn't.
- **Facets.** A facet has no alarm, so:
  - `keepAlive` on a facet calls `_cf_acquireFacetKeepAlive` on the root,
    which holds a token and a ref for it;
  - `runFiber` on a facet calls `_cf_registerFacetRun` so the root's
    `cf_agents_facet_runs` index lists it, and `_cf_unregisterFacetRun` once
    the row is gone;
  - root housekeeping calls `_cf_checkRunFibersForFacet(path)` once per
    owner path. That call walks down the tree, runs the facet's scan, and
    returns how many run rows remain; at zero the root drops the index rows.

---

## 2. Python design

### 2.1 A straight port (no choice involved)

- The two tables (`cf_agents_runs`, `cf_agents_fibers`) and the root index
  (`cf_agents_facet_runs`) with the DDL in [sql_schemas.md](./sql_schemas.md)
  §11.2 and §12, behind a schema-version stamp like the other capabilities.
- The run-row protocol (insert, synchronous `stash` writes, outcome then
  delete), the ledger state machine, `interrupted` as terminal, the scan's
  order and rules, the scan deadline, the no-progress backoff, the max-age
  give-up, and the event names and payloads (`fiber:run:*`,
  `fiber:recovery:*`).
- The scan runs at startup before the user's `on_start` (so the hook can run
  before user code, as upstream), from housekeeping, and before
  `wait_for_completion` waits on a fiber left by a dead process.
- The chat hooks, kept private for step 12: `_run_fiber_with_stash_wrapper`,
  `_with_fiber_stash`, `_handle_internal_fiber_recovery` (timed out by
  `fiber_recovery_hook_timeout`).
- List limits as upstream: `list_fibers` defaults to 50 (clamped 1–100),
  `delete_fibers` to 100 (clamped 1–500).

### 2.2 Where Python differs (already decided in fibers_api.md §3)

Cancellation cancels the asyncio task (so `FiberContext` has no `signal`), no
`FiberContext.snapshot`, one dataclass per recovery result, `keep_alive()`
returns a `Disposable`, and a `ContextVar` replaces `AsyncLocalStorage`.

### 2.3 Smaller choices made here

1. **Errors.** Per the dedicated-exception rule:
   - `FiberConflictError` when `fiber_id` and `idempotency_key` name
     different fibers;
   - `FiberNotFoundError` when a fiber is deleted while `start_fiber` waits
     on it;
   - built-ins for misuse: `ValueError` for a blank `fiber_id` or
     `idempotency_key`, `RuntimeError` for `stash()` outside a fiber.
2. **Read-only results return `Sequence`.** `list_fibers` returns
   `Sequence[FiberInspection]` (fibers_api.md §2.1 said `list`).
3. **The default `on_fiber_recovered` logs a warning** (upstream:
   `console.warn`) and returns `None`.
4. **The facet index uses the routing key.** `cf_agents_facet_runs` stores
   the facet's route address (`key`, `data`) as `owner_path_key` /
   `owner_path`, the same pair Tasks mirrors carry, so the root never
   parses paths.

---

## 3. Module layout (`src/agents/fibers/`)

| Module | Holds | Upstream |
| --- | --- | --- |
| `types.py` | `FiberStatus`, `FiberContext`, `FiberInspection`, `StartFiberResult`, `FiberRecoveryContext`, the four recovery results, row `TypedDict`s, routed message types | `index.ts` types |
| `errors.py` | `FiberConflictError`, `FiberNotFoundError` | (Python) |
| `store.py` | SQL for the three tables | `index.ts` |
| `keep_alive.py` | `KeepAlive` capability: the ref count, its job, facet leases held on the root | `keepAlive`, `acquireKeepAlive` |
| `fibers.py` | `Fibers` capability: run rows, the ledger, waiters, the scan, housekeeping, the facet index | `index.ts`, `dynamic-agents.ts` |

`Agent` gets the public methods from fibers_api.md §2.1, delegating to the
capabilities.

---

## 4. Question to decide

**Q1. Where the engine lives.** **Decided: (a) two capabilities.**

- (a) **Two capabilities, `KeepAlive` and `Fibers`.** **Recommended.** They
  own their jobs (`keep-alive`, `housekeeping`) instead of host jobs.
  Facet traffic (keep-alive leases, run registration, the root's
  check-this-facet call) goes over the Lifecycle route transport as routed
  messages (`on_route`), the way Tasks already routes facet runs. `Agent`
  keeps only thin public methods.
  - Same structure as State, Queue, Scheduler, and Tasks; `agent.py` stays
    readable.
  - No new `_cf_*` entry points: the transport already reaches the root and
    walks down to any facet.
  - `keep_alive` becomes usable by other capabilities (chat in step 12)
    without going through `Agent`.
- (b) **Upstream's shape.** The engine as methods on `Agent`, host jobs
  `cf:keep-alive` / `cf:housekeeping` in `_AgentHooks.on_job`, and the five
  facet RPCs (`_cf_acquire_facet_keep_alive`, `_cf_release_facet_keep_alive`,
  `_cf_register_facet_run`, `_cf_unregister_facet_run`,
  `_cf_check_run_fibers_for_facet`).
  - Line-for-line with upstream, but adds roughly 1,000 lines to `agent.py`
    and a second facet-messaging path beside the route transport.

Either way, behavior and storage are the same.

---

## 5. Implementation notes (step 9, 2026-10-05)

Done: `src/agents/fibers/` and the fiber and keep-alive methods on `Agent`;
438 tests in total at the time (22 new, in `tests/fibers/`; 440 with §6). Checked on `workerd`
(`verify/results/fibers_client.py`):
- `run_fiber` with checkpoints;
- `start_fiber` idempotency and `wait_for_completion`;
- the `keep-alive` job existing only while a fiber runs;
- `cancel_fiber` stopping a sleeping body (`aborted`, with the reason);
- **a real isolate crash** (`ctx.abort`) mid-fiber: on the next request,
  the new isolate recovered both the plain and the managed fiber before
  `on_start`, with their last snapshots, and the hook's `FiberCompleted`
  settled the managed record;
- **a facet's isolate dying mid-fiber**: the root's index entry and its
  `housekeeping` job brought recovery back to the idle facet on the root's
  alarm, and the entry was dropped.

How it turned out, beyond §2:

1. **The startup scan runs from the host's start hook.** `Fibers.on_start`
   only creates the tables. The scan and arming housekeeping are
   `Fibers.recover_on_wake()`, which `Agent`'s start hook calls after every
   capability has started (including any a subclass installs) and before the
   user's `on_start`: upstream's order. *Fixed 2026-10-06*: the first version
   scanned in `Fibers.on_start`, so a capability installed in a subclass's
   `__init__` hadn't started when the hook ran (found in the Streams design
   pass, [streams_engine.md](./streams_engine.md) §4; regression test
   `test_recovery_waits_for_capabilities_a_subclass_installs`; the `workerd`
   crash run was repeated and recovery still precedes `on_start`).
2. **Housekeeping runs from its own job, not every alarm.** Upstream wraps
   `onAlarm` to scan on every alarm, then re-derives its host jobs. Here
   the `housekeeping` job runs the scan and the facet visits, and returns
   its next time (`Reschedule`) or completes. It exists exactly when
   upstream's would (pending recovery, or indexed facet fibers), so the
   wakes are the same; alarms for other work don't also scan.
3. **One `wrap` for the chat stash wrapper.** Upstream's
   `_runFiberWithStashWrapper` takes `initialSnapshot` and `wrapStash`, and
   chat always passes `initialSnapshot: wrap(null)`.
   `_run_fiber_with_stash_wrapper(name, fn, wrap)` writes `wrap(None)` first,
   so no "not given" sentinel is needed.
4. **Cancellation outcomes.** A body that ends with `CancelledError` records
   `aborted`, for plain fibers too (upstream: only managed fibers, through
   their abort signal). `cancel_fiber(id, reason)` passes the reason to
   `Task.cancel`, so the body sees it on the `CancelledError`.
5. **A failed managed fiber is logged.** Upstream records the error and
   emits `fiber:run:failed` but logs nothing. Here the traceback goes to the
   `agents.fibers` logger too, since nothing else would ever see it.
6. **Error messages fall back to the class name** (`str(error) or
   type(error).__name__`), so an empty `CancelledError` records `CancelledError`.
7. **Snapshots aren't re-validated.** Upstream treats a snapshot that isn't
   valid JSON as `null`; here every snapshot is written with `json.dumps`,
   so they're parsed directly.
8. **Facet cleanup starts the root first.** `SubAgentsEngine.cleanup_prefix`
   calls `lifecycle.start()` before the capabilities' `_cleanup_route_prefix`,
   so their tables exist (upstream's `_cf_cleanupFacetPrefix` initializes
   first too). Found by a sub-agent test that deleted a facet from a root
   that was never started.
9. **Facet list queries filter with `json_each`.** Status filters are bound
   as a JSON array (`status IN (SELECT value FROM json_each(?))`), so SQL
   stays a literal string; results match upstream's per-status queries.

---

## 6. Facet keep-alive leases left by a dead facet (fixed; differs from upstream)

**The upstream bug.** The root keeps a facet's lease token in memory
(`#facetKeepAliveTokens`, `dynamic-agents.ts:130`), and only the facet's own
`_cf_releaseFacetKeepAlive` removes it (`:344`). `delete` → `cleanupPrefix`
cancels the subtree's schedules, task wakes, and queue items and drops its
fiber-index rows, but never its leases. Deleting a facet ends its isolate, so
the work holding the lease (a fiber's `finally`, a `keep_alive_while`) never
releases it. The root then heartbeats every `keep_alive_interval` for as long
as its own isolate lives, and since the heartbeat itself prevents idle
eviction, that's indefinitely.

**Verified on `workerd`** (`verify/results/lease_client.py`, before the
fix: `lease_report_before_fix.json`). A facet holding a lease, either directly
or through a running fiber, was deleted. Five seconds later (more than two
2-second heartbeats), the root still had its `keep-alive` job and an active
lease. In the fiber case the `housekeeping` job and index row were gone, but
the lease remained.

**The fix.** The root records the route key of the facet holding each token
(`KeepAlive._facet_leases`). Deleting a facet runs `_cleanup_route_prefix`,
which drops every lease held by the facet or its descendants, and with the
last lease, the heartbeat. A release that arrives late from the deleted
facet finds no token and does nothing. After the fix, the same `workerd`
run left the root with no jobs and no active lease
(`lease_report_after_fix.json`).

**Drop on restart (decided 2026-10-06, implemented).** A facet that dies
without being deleted keeps its storage and registry entry, so deletion never
runs. This covers an aborted facet (`dynamic_agents.abort`) and one whose
isolate crashes (§5's facet crash run was this case). The root can't tell from
a lease that the isolate behind it is gone; it can tell when the facet starts
again, since a new isolate holds no leases yet.
- Each facet isolate gets a random id (`KeepAlive._isolate`), sent with every
  lease it takes.
- On startup, a facet's `KeepAlive.on_start` sends `restarted` with its
  id. The root drops the leases under that facet's exact route key from any
  other isolate. Its sub-agents' leases stay: they run in isolates of their
  own, and drop theirs when they restart.
- Matching on the isolate id means a notice that arrives late, or twice,
  can't drop a lease the new isolate already took.
- Chosen over lease expiry (holders renew every interval; the root drops
  stale ones), which would also cover facets that never wake but costs an RPC
  per lease per interval.

**Verified on `workerd`** (`verify/results/lease_abort_client.py`,
`lease_abort_report.json`), after aborting a facet's isolate:
- a bare `keep_alive()` lease was still held 5 s later (nothing had woken
  the facet), then dropped once a request woke it;
- a fiber's lease was dropped within 5 s with no request at all: the root's
  housekeeping woke the facet to recover the fiber, and its restart dropped
  the lease.

**Remaining limitation (accepted, 2026-10-06).** A bare facet lease (not a
fiber's) whose facet dies and never wakes again stays until the root's own
isolate ends. Fibers don't have this problem, since recovery always wakes
their facet. Accepted as is for now; lease expiry (above) is the fix if it
ever matters.
