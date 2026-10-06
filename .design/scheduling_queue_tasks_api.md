# Python API: schedules, queues, tasks

The user-facing API for the three "run code later" capabilities.
Upstream background:
- [agents-subpackages.md](./agents-subpackages.md) §4 (`schedules/`, `queue/`, `tasks/`)
- [lifecycle_capabilities.md](./lifecycle_capabilities.md) §5 (the job queue they share)

Naming: every API is snake_case. A full naming pass over the inspect and
cancel APIs is deferred (§3); names there are working names.

---

## 1. Shape

```python
class MyAgent(Agent):
    async def on_start(self):
        await self.schedule_every(timedelta(seconds=30), self.tick)

    async def tick(self, _payload, _schedule): ...

    async def remind(self, data: dict, _schedule): ...

    async def process_upload(self, data: dict, _item): ...

    @task
    async def research(self, input: dict, step: TaskStep) -> str:
        async def search_step(_attempt):
            return await search(input["topic"])

        sources = await step.do("search", search_step)
        await step.sleep("cooldown", timedelta(minutes=10))
        return await step.do(
            "summarize", partial(summarize_step, sources), retries=StepRetries(limit=3)
        )

    async def on_request(self, req):
        await self.schedule(
            60, self.remind, {"text": "stretch"}, retry=RetryOptions(max_attempts=5)
        )
        await self.queue(self.process_upload, {"key": "uploads/a.png"}, id="upload-a")
        await self.research.run({"topic": "DO hibernation"}, idempotency_key="r-42")
```

Upstream mapping:

| Python | TypeScript |
| --- | --- |
| `await self.schedule(when, callback, payload, *, retry, idempotent)` | `this.schedule(when, "callback", payload, { retry, idempotent })` |
| `await self.schedule_every(interval, callback, payload, *, retry)` | `this.scheduleEvery(seconds, "callback", payload, { retry })` |
| `await self.queue(callback, payload, *, retry, id)` | `this.queue("callback", payload, { retry, id })` |
| `@task` + `await self.research.run(input, *, ...)` | `taskDefinitions = { research }` + `this.tasks.run("research", input, {...})` |
| `await self.tasks.run("research", input, *, ...)` | `this.tasks.run("research", input, {...})` |
| `await step.do(name, fn, *, retries, timeout)` | `step.do(name, { retries, timeout }, fn)` |

---

## 2. Decided

### 2.1 Callback calling convention: always two arguments

Schedule callbacks are called as `method(payload, schedule)`, and queue
callbacks as `method(payload, item)`, every time. This matches upstream
(`index.ts:1935`, `:1960`). With no payload, `payload` is `None`.

```python
async def remind(self, data, schedule): ...
async def tick(self, _payload, _schedule): ...  # unused → underscore
```

**Why:** it's explicit and the same everywhere. Unused parameters get `_`.

**Background, and the alternatives that were rejected.** JavaScript drops
extra arguments and fills missing ones with `undefined`, so upstream callbacks
can declare `(payload)`, `(payload, schedule)`, or `()`, and the upstream docs
use both one- and two-argument forms. Python requires the arguments to match
the method's parameters, so one of these had to be chosen:
- **(a) always two arguments**: chosen;
- (b) payload only: rejected, because callbacks would lose access to the
  schedule/item metadata;
- (c) inspect the signature and pass as many arguments as the method accepts:
  rejected as implicit.

**Consequence:** the callback's signature is not validated, matching upstream.
A callback declared with the wrong number of parameters raises `TypeError`
when the job fires. It is then logged and retried like any other error raised
by a callback.

### 2.2 Callbacks: `str | Callable`

```python
await self.schedule(
    60, self.remind, data
)  # preferred: IDE-checked, safe to rename at the call site
await self.schedule(60, "remind", data)  # also accepted, for parity with upstream
```

- A callable must be a **bound method of `self`**. Lambdas, free functions,
  and other objects' methods can't be stored as a name and looked up again
  after a wake.
- The stored value is the **name** (`method.__name__` for callables). When the
  job fires, the name is resolved with `getattr(self, name)`, as upstream does
  (`index.ts:1928`).
- **The name is checked when scheduling**, the same as upstream
  `#validateSchedule` (`scheduler.ts:673`): the name must resolve to a method,
  otherwise an "unknown callback" error is raised.
- **Mangled methods get no special handling.** If a method is name-mangled,
  callers pass the mangled name themselves (`"_MyAgent__remind"`). Methods are
  mangled to discourage outside access, and the SDK shouldn't make them easier
  to reach. Passing `self.__remind` stores `"__remind"`, which fails the normal
  existence check when scheduling, so the mistake surfaces at the call site.
- **Renames get partial protection.** A method reference makes the call site
  safe to rename, but rows that are already stored hold the old name.
  Renaming a method strands its pending jobs, as upstream.
- Optional: use `ParamSpec`/generics to tie the payload's type to the
  callback's parameter type (upstream's equivalent is
  `SchedulerPayload<Handler>`). The `str` form remains untyped.
- The same `str | Callable` rule applies wherever a callback is named in the
  inspect and cancel APIs (§2.9).
- **Implemented once, in two shared utilities** (see
  [utilities.md](./utilities.md) §2): `method_name(target, callback)` turns
  `str | Callable` into the stored name when scheduling, and
  `get_bound_method(target, name)` turns the name back into a method when the
  job fires.

### 2.3 Every callback is `async def`

Schedule callbacks, queue callbacks, task functions, and step functions must
all be async (return an awaitable). The SDK always awaits the result, so there
is a single code path with no "await it if it's awaitable" branch.

- **Schedule and queue callbacks are not checked.** A plain `def` returns
  `None`, and `await None` raises `TypeError: object NoneType can't be used in
  'await' expression` when the job fires. That message is clear enough on its
  own.
- **`@task` checks once, when the class is defined.** If the decorated function
  isn't a coroutine function (`inspect.iscoroutinefunction`), it raises
  `TypeError("Task functions must be async")`. This costs nothing at runtime.
- **Step functions are not checked** (§2.5). They only need to return an
  awaitable, so `functools.partial` and lambdas that return a coroutine are
  valid.

**Why async rather than also allowing sync:** most callbacks need `await` anyway
(DO storage, `fetch`, scheduling, and anything else that goes through the JS
bridge is async in Python Workers), and requiring it keeps the SDK to one code
path and matches handlers like `on_request` and `on_connect`. (A sync callback
wouldn't block the event loop any *more* than an async one without awaits;
blocking isn't the reason.)

### 2.4 Tasks: `@task` decorator that returns a handle

```python
class MyAgent(Agent):
    @task
    async def research(self, input: dict, step: TaskStep) -> str: ...


await self.research.run(input, idempotency_key="r-42")  # handle bound to this instance
await self.research.get(run_id)
await self.research.cancel(run_id)
await self.tasks.run("research", input)  # by name, still supported
```

- `@task` is a **descriptor**. When the class is defined it registers the
  definition (named by `__name__`) and checks that the function is async
  (§2.3). Accessing it on an instance returns a handle bound to `self.tasks`,
  the equivalent of upstream `tasks.handle(name)`: `run`, `get`,
  `get_by_idempotency_key`, `cancel`.
- Decorators run when the class is defined, so every wake registers the same
  definitions. That is the property upstream needs for in-flight runs to
  resume (see `setTaskDefinitionResolver`).
- Handler signature: `(self, input, step)`. `input` and the return value must
  be JSON-serializable (upstream `TaskJson`).

### 2.5 `step.do(name, fn, *, retries=None, timeout=None)`

```python
fn: Callable[[TaskStepAttempt], Awaitable[T]]
```

`fn` is called **once per attempt** with the attempt context, and the
awaitable it returns is awaited:

```python
@dataclass(slots=True, kw_only=True)
class TaskStepAttempt:
    attempt: int  # 1-based retry count for this step
    idempotency_key: str  # identical across retries and replays of this step
```

```python
async def charge(attempt: TaskStepAttempt) -> dict:
    return await stripe_charge(amount, idempotency_key=attempt.idempotency_key)


receipt = await step.do("charge", charge)
summary = await step.do(
    "summarize", partial(summarize_step, sources), retries=StepRetries(limit=3)
)
```

- **Any callable returning an awaitable is accepted:** `async def`,
  `functools.partial` of an async function, or a lambda that returns a
  coroutine. **Docs and examples use `async def` (or `partial`) and never
  lambdas.** The lambda-returning-a-coroutine pattern is allowed but not
  promoted.
- **`fn` must be a callable, not a coroutine.** `step.do("x", search(topic))`
  would create the coroutine once: it couldn't be retried, and on replay it
  would never be awaited ("coroutine was never awaited"). That is why a step
  takes a function that produces a fresh awaitable for each attempt.
- **The attempt is always passed**, consistent with §2.1. Unused attempts get
  `_`.
- **Why the attempt exists:** `idempotency_key` makes external side effects safe
  on replay. If the isolate dies after Stripe charged the card but before the
  result was saved, the replay calls Stripe again with the same key, and
  Stripe returns the original charge instead of charging twice. Upstream also
  offers `step.idempotency_key(name)` outside the callback.
- **There is no `signal`.** See §2.6.

### 2.6 Step timeout and cancellation: asyncio cancellation instead of `AbortSignal`

**What upstream's `signal` does** (`tasks/replay.ts:385`): each attempt gets an
`AbortController` that aborts when **(1)** the step's timeout expires, **(2)**
the run is cancelled, or **(3)** the attempt is superseded by a newer
generation. JavaScript cannot forcibly stop a running async function, so
upstream:
- passes `signal` to the callback, which can forward it to `fetch()` or check
  `signal.aborted` in loops to *cooperatively* stop;
- races the callback against the signal (`#raceTimeout`). The attempt settles
  as soon as the signal aborts, even if the callback ignores it. In that case
  the callback keeps running in the background, its late result is
  discarded, and generation fencing rejects any late writes.

**Python equivalent:** cancel the attempt's task. On timeout, run
cancellation, or supersession, the SDK cancels it, and the step function
receives `CancelledError` at its next `await`. This is **stronger** than
upstream: the function actually stops instead of continuing in the
background.

**Implementation note: plain `asyncio.timeout` is not quite the same.** If a step
function catches and swallows `CancelledError`, `asyncio.timeout` (and
`asyncio.wait_for`, which uses it on Python 3.12+) never sees it, so the
attempt would continue and not settle. To keep upstream's guarantee that a
callback can't wedge its attempt, run each attempt as its own task:

```python
attempt_task = asyncio.create_task(fn(attempt))
done, _ = await asyncio.wait({attempt_task}, timeout=timeout_s)
if not done:
    attempt_task.cancel()  # request cancellation; don't wait for it to comply
    raise StepTimeoutError(...)  # the attempt settles now, as upstream's race does
```

Run cancellation and supersession call `attempt_task.cancel()` the same way.
Late writes from a task that ignores cancellation are rejected by generation
fencing, as upstream.

**Verified ([platform_verification.md](./platform_verification.md) §2.8): cancelling the task stops the Python code,
but whether an in-flight request stops depends on the client.** `pyfetch` /
`workers.fetch` abort while waiting for headers; raw `js.fetch`, **httpx**,
and chunk-by-chunk body reads keep running to completion in the background
(result discarded), the same as upstream when a callback ignores `signal`.
The attempt still settles correctly either way. **Decided: document it, no
SDK mechanism for now** ([platform_verification.md](./platform_verification.md) §6).

### 2.7 Options are keyword-only

Upstream passes optional settings as a trailing options object. Python uses
keyword-only parameters (after a bare `*`), so they can only be passed by
name, and options can be added or reordered without breaking callers:

```python
async def schedule(
    self,
    when,
    callback,
    payload=None,
    *,
    retry: RetryOptions | None = None,
    idempotent: bool | None = None,
) -> Schedule: ...
```

| Call | Keyword options | Upstream |
| --- | --- | --- |
| `schedule`, `schedule_every` | `retry`, `idempotent` (`None` = default: on for cron/interval, off for one-shot) | `ScheduleOptions` |
| `queue` | `retry`, `id` (replaces an existing item with that id) | `QueuePushOptions` |
| `tasks.run` / `handle.run` | `idempotency_key`, `run_id`, `metadata`, `retain` | `TaskRunOptions` |
| `step.do` | `retries`, `timeout` | `TaskStepConfig` |

`retries` takes a `StepRetries` dataclass, upstream's `TaskStepConfig.retries`
(`tasks/types.ts:106`) in Python form:

```python
@dataclass(slots=True, kw_only=True)
class StepRetries:
    limit: int | None = None  # total attempts, including the first
    delay: Duration | None = None  # before the first retry (seconds or timedelta)
    backoff: Literal["constant", "linear", "exponential"] | None = None
    # None = the Tasks default (5 attempts, 1 s, exponential); tasks_engine.md §5
```

`RetryOptions` is a dataclass (`slots=True, kw_only=True`; [utilities.md](./utilities.md) §5) with upstream's defaults:
`max_attempts=3`, `base_delay=0.1`, `max_delay=3` (durations are
`timedelta | float` seconds, [utilities.md](./utilities.md) §1; upstream's
`baseDelayMs` / `maxDelayMs` are milliseconds). Using a dataclass rather than
a dict means a misspelled field fails immediately.

**Durations across these APIs** follow the same rule: `step.do(...,
timeout=…)`, step retry delays, `step.sleep(name, duration)`, and
`schedule_every(interval)` take `timedelta | float` seconds. Upstream Tasks
durations are milliseconds or strings like `"10 seconds"`; neither is
ported.

### 2.8 Return types match upstream

| Call | Returns |
| --- | --- |
| `schedule`, `schedule_every` | `Schedule` (dataclass) |
| `queue` | item id, `str` |
| `tasks.run` / `handle.run` | `TaskReceipt` (dataclass: `run_id`, `definition`, `accepted`, `state`, `created_at`) |

### 2.9 Inspect and cancel APIs

- **Deprecated upstream functions are not ported.** Upstream's synchronous
  `getSchedule(id)` and `getSchedules(criteria)` are `@deprecated` ("cannot
  cross Durable Object boundaries and throws inside sub-agents"). Only their
  async replacements (`getScheduleById`, `listSchedules`) are ported.
- **Functions that filter by callback accept `str | Callable`**, the same rule as
  §2.2: `dequeueAllByCallback`, `Queue.list({callback})`.
- **Filter criteria are keyword arguments.** `ScheduleCriteria`
  (`{id?, type?, timeRange?: {start?, end?}}`) becomes
  `list_schedules(id=..., type="cron", start=datetime(...), end=...)`.
- Final snake_case names: deferred naming pass (§3).

### 2.10 `schedule` and `schedule_every` accept `timedelta`

- `schedule(when: datetime | timedelta | int | float | str, callback, payload=None, *, ...)`:
  a `datetime` is an absolute time, a `timedelta` or number of seconds is a
  delay, and a `str` is a cron expression.
- `schedule_every(interval: timedelta | int | float, callback, payload=None, *, ...)`.

Upstream takes seconds (or a `Date` / cron string) only.

### 2.10.1 `queue_items` replaces `getQueues` (decided)

Upstream's `getQueues(key, value)` is `list()` plus a strict-equality filter on
one top-level payload field (`index.ts:3175`), with no internal callers. Python
exposes `queue_items(callback=None) -> list[QueueItem]` (wrapping `Queue.list`;
`callback` accepts `str | Callable`, §2.9), and callers filter with a
comprehension:

```python
items = [i for i in await self.queue_items() if i.payload.get("group") == "a"]
```

### 2.11 Capabilities can be installed on plain Durable Objects, with method lookup and `@task`

`Scheduler`, `Queue`, and `Tasks` are public, standalone capabilities, as
upstream. They can be installed on a plain DO without `Agent`, and **method
lookup and `@task` work there too**. They are not `Agent`-only.

**Why upstream makes them `Agent`-only, and why Python doesn't need to.**
Upstream's Lifecycle design says capabilities don't get the whole host
"through an implicit host interface" (`rfc-durable-object-lifecycle.md:81`):
a `Scheduler` may not reach into whatever object it's installed on. So
`Agent`, as the composition root, supplies method lookup through private
setters (`setSchedulerCallbackResolver(this.scheduler, name => this[name])`,
`index.ts:1928`; `setQueueCallbackResolver`, `:1952`), and plain DOs never get
it. Python keeps that rule by having the host **pass itself to the constructor
explicitly**. That is explicit, not implicit, access, the same as passing a
dict:

```python
class Room(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.scheduler = Scheduler(target=self)  # look up callbacks as methods on self
        self.queue = Queue(target=self)
        self.tasks = Tasks(target=self)  # collect @task methods from type(self)
        self.lifecycle = (
            Lifecycle(self).use(self.scheduler).use(self.queue).use(self.tasks)
        )

    async def fetch(self, request):
        return await self.lifecycle.fetch(request)

    async def alarm(self, *args):  # jobs only fire if the alarm reaches Lifecycle
        return await self.lifecycle.alarm()

    async def remind(self, payload, schedule): ...

    @task
    async def research(self, input, step): ...


# A dict works too, alone or combined with a target:
Scheduler(callbacks={"remind": self._remind})
Scheduler(target=self, callbacks={"old_remind": self.remind})
```

- **Two keyword parameters: `callbacks` (a dict) and `target` (an object)**
  (decided, §4.4). `Tasks` names its dict `definitions`:

  ```python
  Scheduler(*, callbacks: Mapping[str, Callable[..., Awaitable[Any]]] | None = None, target: object | None = None, ...)
  Queue(*,     callbacks: Mapping[str, Callable[..., Awaitable[Any]]] | None = None, target: object | None = None, ...)
  Tasks(*,     definitions: Mapping[str, Callable[..., Awaitable[Any]]] | None = None, target: object | None = None, ...)
  ```

  - **Lookup order:** the dict first, then methods on `target`. Either can be
    used alone, or both together (e.g. keep jobs stored under an old name
    working after a rename).
  - **The dict** is looked up by key, inside the capability.
  - **`target`** is looked up with the shared utilities `method_name` /
    `get_bound_method` ([utilities.md](./utilities.md) §2), so every rule from
    §2.2 applies, with "bound method of `self`" meaning "bound method of
    `target`".
- **`Tasks(target=obj)` collects the `@task` descriptors** by walking
  `type(obj).__mro__` into a name → handler table. The decorator does the
  filtering, so `Tasks` needs nothing extra. Entries in `definitions` take
  precedence over collected ones with the same name.
- **How the `@task` handle finds its `Tasks` instance.**
  `self.research.run(...)` has to locate the `Tasks` capability, and a plain DO
  may store it under any attribute name. `Tasks(target=obj)` records itself in
  a module-level `WeakKeyDictionary` keyed by `obj`, and the descriptor's
  `__get__` looks it up there. If there is no entry, it raises an error saying
  no `Tasks` capability was created with `target=self`.
- **`Agent` uses the same public mechanism**: `Scheduler(target=self)`,
  `Queue(target=self)`, `Tasks(target=self)`. The port needs **no private
  resolver setters** (`setSchedulerCallbackResolver`,
  `setQueueCallbackResolver`).

---

### 2.12 No sentinel for these APIs

Every option here treats `None` as "use the default", so none needs a
"not passed" sentinel (the SDK defines none, [utilities.md](./utilities.md) §4).

### 2.13 `Queue` implementation decisions (decided, step 3)

Implemented in `src/agents/queue/` (port of upstream `queue/queue.ts`).

```python
queue = Queue(callbacks=None, target=None, retry=None, on_error=None)
item = await queue.push(callback, payload=None, *, id=None, retry=None)  # QueueItem
await queue.get(id)                # QueueItem | None
await queue.list(callback=None)    # list[QueueItem], push order
await queue.cancel(id)             # bool
await queue.cancel_all(callback=None)  # int
```

1. **`Queue.push` returns the `QueueItem`**, as upstream; `Agent.queue`
   returns its id (§2.8).
2. **`QueueItem`** is a dataclass: `id`, `callback`, `payload`,
   `created_at: datetime`, `retry: RetryOptions | None`.
3. **A callable callback needs `target`.** A dict-only Queue takes names; a
   callable there raises `TypeError` pointing at the registered name. Names
   are checked when pushed (`ValueError` "Unknown queue callback").
4. **The default `retry` is validated in the constructor.** Upstream only
   resolves it there and lets an invalid default fail each dispatch, to avoid
   "bricking" the object; in Python it's a programming error that should
   fail loudly at deploy, not as a stream of `queue:error`s.
5. **A failing `on_error` hook is logged** (`agents.queue`, with the
   traceback); upstream swallows it silently. It still never fails the queue.
6. **No `cf_agents_queues` migration**: Python never created that table
   ([sql_schemas.md](./sql_schemas.md)).
7. **Routed (facet) messages are Python-to-Python**, so they use Python
   names (`"cancel_all"`) and a JSON item (`created_at_ms`, `retry` as the
   job table's JSON text), typed as a `TypedDict` union in `queue/types.py`.
   The job envelope (`payload`, `owner_path`, `owner_path_key`) matches
   upstream.
8. **Events keep upstream's payload keys** (`maxAttempts`), per
   [observability.md](./observability.md).
9. **`_cleanup_route_prefix(prefix)`** (upstream
   `__DO_NOT_USE_WILL_BREAK__cleanupRoutePrefix`) is internal, for `Agent`'s
   sub-agent deletion.

Verified on `workerd` (verify `LifecycleHost`, `/lc/<name>/q-push` and
`/q-state`): push order, an in-alarm retry with `queue:retry`, and a
stable-id replace, drained ~120 ms after the push.

### 2.14 `Scheduler` implementation decisions (decided, step 6)

Implemented in `src/agents/schedules/` (port of upstream `schedules/`).

```python
scheduler = Scheduler(callbacks=None, target=None, retry=None,
                      hung_schedule_timeout=30, on_error=None)
await scheduler.set(when, callback, payload=None, *, retry=None, idempotent=None)  # Schedule
await scheduler.every(interval, callback, payload=None, *, retry=None, idempotent=None)
await scheduler.get(id)                                    # Schedule | None
await scheduler.list(id=None, type=None, start=None, end=None)  # Sequence[Schedule]
await scheduler.cancel(id)                                 # bool

# Agent
await self.schedule(when, callback, payload, *, retry, idempotent)
await self.schedule_every(interval, callback, payload, *, retry, idempotent)
await self.get_schedule_by_id(id)
await self.list_schedules(id=..., type=..., start=..., end=...)
await self.cancel_schedule(id)
```

1. **Cron: a port of `cron-schedule` 6.0** (decided; `schedules/cron.py`),
   upstream's parser, so expressions mean the same in both SDKs: 5 fields,
   or 6 with **seconds first**; lists, ranges, steps, month and weekday
   names, weekday `7` = Sunday, the `@daily`-style nicknames, and the
   "either day of month or weekday" rule. UTC (the Workers runtime's local
   time). Checked against the real JS library: 162 expression/start pairs
   (`tests/schedules/cron_schedule_oracle.json`). **Stricter than upstream
   in one way:** values must be plain numbers or names (JS `parseInt`
   accepts `"5x"` as 5). Rejected: depending on `croniter` (its 6-field
   form puts seconds last; a dependency in every Worker) and a 5-field-only
   parser.
2. **`InvalidCronExpressionError`** is both an `AgentsException` and a
   `ValueError`: usually a mistake in code, but catchable when the
   expression comes from user or model input.
3. **`Schedule` is one dataclass** with `type`, `time: datetime`, and the
   type-specific `delay` / `interval` (`timedelta`) and `cron`, instead of
   upstream's four-way union; `retry` is the override given at creation.
4. **Times keep millisecond precision** (decided). Upstream floors to whole
   seconds because its public `Schedule.time` is Unix seconds (from the
   legacy table); a side effect is that a delayed schedule can fire up to a
   second early (`schedule(1)` at `…:00.800` stores `…:01`). Python's `time`
   is a `datetime`, so it fires on time (seen on `workerd`: created
   `…20.180`, fired `…20.194`). Cron times are whole seconds either way.
5. **Idempotent matching compares payloads as sorted-key JSON**, so key
   order doesn't matter (upstream compares `JSON.stringify` output).
6. **Facet inserts send the parsed timing**, not `when` (decided): the
   facet parses `when` with its own clock and the root stores it (upstream
   re-parses on the root). Errors (bad cron, wrong type, naive `datetime`)
   are raised in the caller with their real types instead of coming back
   across RPC as a generic error, and the timing is plain JSON, so
   `datetime` / `timedelta` need no wire encoding. A facet shares its root's
   machine, so the clocks agree; only the routing delay differs.
7. **The default `retry` is validated in the constructor**, as for `Queue`
   (§2.13 item 4).
8. **Not ported:** the legacy `cf_agents_schedules` migration
   ([sql_schemas.md](./sql_schemas.md) §4), the deprecated synchronous
   `getSchedule` / `getSchedules` (§2.9), and the internal `recoveryLoop`
   schedule option (decided). In upstream 0.25 nothing passes
   `recoveryLoop` to the Scheduler: the option is `@internal`, marked "do
   not use it for new work" and slated for removal once routed chat
   recovery moves to Tasks; its only live uses are flagging migrated legacy
   chat-recovery rows (`_chatRecoveryContinue` / `_chatRecoveryRetry`) and
   re-flagging them on a dedup hit, neither of which applies to Python.
   `AIChatAgent` and Think only mention it in comments. The pi harness sets
   it on a Lifecycle job directly, which Python already supports
   (`LifecycleJobs.push(..., recovery_loop=True)`). **Revisit in step 12** if
   chat recovery on facets turns out to need a schedule-level flag.
9. **Schema-version stamps** (`cf_agents:schedules_schema_version` = 2,
   `cf_agents:queue_schema_version` = 1) are written once, on first start
   ([sql_schemas.md](./sql_schemas.md) §4); `Queue` gained the stamp it had
   missed in step 3.
10. **Agent method names** (`get_schedule_by_id`, `cancel_schedule`, …)
    are pending the naming pass (§3).

Verified on `workerd` with real alarms: a 1-second one-shot, a
`*/2 * * * * *` cron (fired on each even second, within ~15 ms), and a
3-second interval, with the one-shot deleted and the alarm re-armed for the
next schedule.

---

## 3. Deferred

- **`@task(name=...)`.** A stored name that stays the same after the method is
  renamed. Every run stores its definition's name, so renaming the method
  strands in-flight runs. Assume no `name` keyword for now; the default is
  `__name__`.
- **Full snake_case naming** of the inspect and cancel APIs: a naming pass,
  not written yet.

---

## 4. Open

1. ~~`getQueues`~~: decided, see §2.10.1.
2. ~~`timedelta` for `schedule()`~~: decided, see §2.10.
3. **JS `fetch` cancellation on Python Workers** (§2.6): verified, it isn't
   aborted ([platform_verification.md](./platform_verification.md) §2.8); decided: document it ([platform_verification.md](./platform_verification.md) §6).
4. ~~Constructor parameter shape~~: **decided: two parameters**, `callbacks`
   (`definitions` for `Tasks`) and `target`, combinable (§2.11). Rejected: a
   single `callbacks: Mapping | object` parameter, because `Mapping[...] |
   object` simplifies to `object` for a type checker (no type errors for a
   wrong argument) and couldn't combine a dict with a target.
