# Code semantics and project structure

How SDK code is written and organized, decided before implementation starts.
Related: [utilities.md](./utilities.md) §1 (SDK-wide principles: exceptions,
durations, timestamps), §5 (record types); [scope.md](./scope.md) §5 (build
order).

---

## 1. Style (decided)

- **PEP 8.** Enforced by `ruff format` and `ruff check` (§6).
- **Python 3.12 syntax** (decided): the code must run on Python 3.12 as
  well as 3.13 (the version current Python Workers run,
  [platform_verification.md](./platform_verification.md): 3.13.2).
  `requires-python = ">=3.12"`, ruff `target-version = "py312"`, and ty
  checks against 3.12, so 3.13-only syntax is caught.
  - **Allowed (3.12):** PEP 695 generics without defaults (`class Emitter[T]`,
    `def use[C: …]`), `type` aliases, `typing.override`, `X | None`,
    `collections.abc` for ABCs.
  - **Not allowed: PEP 696 default syntax** (`class Agent[State = dict[...]]`
    is 3.13-only). A generic class that needs a default type parameter uses
    `typing_extensions.TypeVar(..., default=...)` with `Generic[...]`
    instead (checked with pyright, ty, and mypy on 3.12: an un-parameterized
    `class MyAgent(Agent)` gets the default):
    ```python
    from typing_extensions import TypeVar

    State = TypeVar("State", bound=Mapping[str, Any], default=dict[str, Any])


    class Agent(DurableObject, Generic[State]): ...
    ```
    `typing_extensions` is therefore a runtime dependency (pure Python).
- **`websocket` is one word** in Python names (`on_websocket_message`,
  `accept_websocket`), not `web_socket`; JS / runtime names keep their own
  spelling.
- **Naming:** `snake_case` functions, methods, variables, modules;
  `PascalCase` classes; `UPPER_SNAKE` constants; a leading `_` for anything
  not public. No shadowing builtins, except the decided `callable` decorator
  ([agent_api.md](./agent_api.md) §1.1).
- **Comments say why, not what.** A ported piece cites its upstream source
  by file and function (`# Port of upstream job-driver.ts runAlarm`), not by
  line number, which rots.
- **Section comments are plain** (decided): a group of related methods is
  headed `# Connections`, not `# ── Connections ──────`.
- **Read-only results return `collections.abc.Sequence[...]`** (decided),
  not `list` or `tuple[X, ...]`: query results, snapshots, lookups
  (`Sql(...)`, `Queue.list`, `LifecycleJobs.list`, `Connection.tags`,
  `MemoryLimitContext.purged_recovery_loop_jobs`). It tells callers the
  result is for reading, and leaves the SDK free to return a tuple or a
  cached value. The implementation may still build a tuple where a shared
  snapshot must not change. A `list` stays where the caller owns the result
  as data (`_prepare_tags`, whose list is stored in the attachment);
  fixed-shape tuples (`(client, server)`) stay tuples. This also avoids a
  pitfall: in a class with a `list` method, `list[...]` in a later
  annotation names the method (`TypeError: 'function' object is not
  subscriptable` at import). Rejected for that case: `builtins.list[...]` and
  `typing.List[...]` (a deprecated alias ruff flags).

## 2. Project structure (decided: mirror upstream's folders)

Each folder is one well-scoped area, matching upstream
`packages/agents/src/` where one exists, so the two codebases are easy to
compare ([agents-subpackages.md](./agents-subpackages.md)).

```
src/agents/
├── __init__.py          # the public API: Agent, callable, route_agent_request, get_agent_by_name,
│                        #   get_current_agent, retry, AgentsException, … (explicit __all__)
├── py.typed
├── _ffi.py              # the only module importing js / pyodide (utilities.md §3)
├── core/                # upstream core/ + shared helpers: Emitter/Disposable,
│                        #   method lookup, typed SQL, retry, JSON encoding, base exceptions
├── lifecycle/           # upstream lifecycle/: Lifecycle, LifecycleCapability, job queue,
│                        #   job driver, current-agent context
├── state/               # upstream state/
├── websockets/          # upstream websockets/: Connection, the connect sequence, RPC frames,
│                        #   @callable, StreamingResponse (rpc.py; agent_api.md §1.19)
├── schedules/           # upstream schedules/: Scheduler
├── queue/               # upstream queue/: Queue
├── tasks/               # upstream tasks/: Tasks, @task, TaskStep, replay
├── streams/             # upstream streams/
├── sessions/            # upstream sessions/ (phase-1 subset)
├── dynamic_agents/      # upstream dynamic-agents/ + sub-routing.ts: facets, sub-agent routing
├── fibers/              # upstream's fiber engine (lives in index.ts upstream): run_fiber,
│                        #   managed fibers, recovery
├── observability/       # upstream observability/ (events only; no tracing)
├── agent/               # upstream index.ts, agent-routing.ts: Agent, AgentOptions,
│                        #   get_current_agent, routing
├── chat/                # upstream chat/: chunks, messages/parts, message builder,
│                        #   resumable stream, turn queue, chat wire frames
└── ai_chat/             # upstream @cloudflare/ai-chat: AIChatAgent
```

- **Folder names** follow upstream's, in `snake_case` (`dynamic-agents` →
  `dynamic_agents`, `@cloudflare/ai-chat` → `ai_chat`). Upstream code that
  lives in one big file gets its own folder here when it's a separate concern
  (`fibers/`, `agent/`).
- **Inside each folder:**
  - `__init__.py`: the folder's public names, re-exported with an explicit
    `__all__`; no logic. Internal helpers are imported from their own module
    (`from ..core.methods import get_bound_method`), not listed in `__all__`.
  - **`__all__` is always a tuple** (decided), in every module.
  - **`types.py`** (decided): **all** the folder's dataclasses, `TypedDict`s,
    `NamedTuple`s, `Enum`s, `Literal` / `type` aliases, and `Protocol`s,
    including internal ones (decided: no type definitions in implementation
    modules). Internal types are simply left out of `__all__`. It imports
    only from the standard library and other folders' `types.py`, never from
    implementation modules, so types can be shared without import cycles.
  - **`errors.py`** (decided): the folder's exception classes, all
    subclassing `AgentsException` (defined in `core/errors.py`). Like
    upstream's `tasks/errors.ts`.
  - implementation modules, one concern each (`lifecycle/job_queue.py`,
    `lifecycle/job_driver.py`, …), mirroring upstream's file split.
- **Dependency direction** (lower layers never import higher ones):
  `core` → `lifecycle` → capabilities (`state`, `websockets`, `schedules`,
  `queue`, `tasks`, `streams`, `sessions`, `dynamic_agents`, `fibers`,
  `observability`) → `agent` → `chat` → `ai_chat`. Capabilities don't import
  each other (upstream's rule: "neither capability imports the other").
- **The top-level `agents/__init__.py` wildcard-imports each subpackage and
  has a static `__all__`** (decided):
  ```python
  from .core import *
  from .lifecycle import *

  __all__ = ("AgentsException", "Duration", ..., "Lifecycle", ...)
  ```
  `__all__` is a literal tuple, never computed (computing it is unsupported
  by tools and bad practice), and it repeats every subpackage's public
  names; `tests/test_public_api.py` checks that the two stay in step. ruff's
  `F403` / `F405` are ignored for that one file.
- **Public import paths** mirror upstream's subpath exports:
  `from agents import Agent`, `from agents.streams import Streams`,
  `from agents.chat import chunks`, `from agents.ai_chat import AIChatAgent`.
  Anything under a `_`-prefixed name, or not in an `__all__`, is internal.
- **Tests** live in `tests/`, mirroring the package (`tests/core/`,
  `tests/lifecycle/`, …), run under CPython with `pytest`. `tests/conftest.py`
  registers `tests/_fake_ffi.py` as `agents._ffi` (values pass through
  unchanged), so SDK modules import without Pyodide.
  - **The Workers SDK itself (`workers`) also imports `js`** at import time
    (`workers/blob.py`), so it can't be imported under CPython either. Modules
    that import `workers` (the `Agent` base class, `Request` / `Response`,
    routing) need the same treatment in tests: a stand-in module registered
    in `conftest.py`.
  - **The real `_ffi.py` is checked on the runtime** through the verification
    Worker (`verify/`, probe `sdk_ffi`), since CPython tests use the
    stand-in.

## 3. Typing (decided: well-typed)

- **Every function and method is fully annotated**, parameters and return,
  including private ones. `py.typed` ships with the package.
- **Checked with `ty`** (§6), with zero diagnostics as the bar. `js` and
  `pyodide` can't be resolved outside the runtime, so they're listed in
  `allowed-unresolved-imports`; every other unresolved import is still an
  error (checked: `ty` 0.0.84).
- **`workers` (the Workers SDK) resolves**: `workers-runtime-sdk` is on PyPI,
  so it's a dev dependency for type checking and tests (the runtime provides
  it on Workers).
- **`Any` only where decided**: untyped stubs (`get_agent_by_name`), JS
  values inside `_ffi.py`, and JSON payloads typed as `JSONValue`. Not as a
  shortcut.
- **Precise types over loose ones:** `Literal` for closed string sets,
  `TypedDict` / dataclasses for structured data, generics where a value's
  type flows through (`use[C](…) -> C`), `Protocol` for structural
  interfaces (`Observability`).
- **`typing.override`** on methods overriding a base, inside the SDK (users
  aren't required to, [lifecycle_capabilities.md](./lifecycle_capabilities.md) §9.1).

## 4. Scope of functions, methods, and classes (decided)

- **One job per function.** If it needs "and" to describe, split it. As a
  smell test, a function over ~40 lines or with more than ~4 levels of
  nesting gets a second look.
- **Plain functions unless there's state to hold** (utilities §1); a class
  that is only a namespace for functions is a module instead.
- **Options are keyword-only** (scheduling doc §2.7); no boolean flag that
  switches a function between two behaviors: make two functions.
- **Small public surfaces:** a class exposes what callers need; helpers are
  `_private`. Composition over deep inheritance (capabilities are the one
  deliberate base class).
- **Errors** follow utilities §1: dedicated `AgentsException` subclasses for
  catchable conditions, built-ins for misuse, no blanket `try`/`except`, only
  `Exception` caught where catching is right (never swallow
  `CancelledError`).
- **`@contextmanager` functions are annotated `-> Generator[T]`** (decided),
  not `-> Iterator[T]`, which is deprecated for them.
- **Async rules:** callbacks are `async def`; never block the event loop; keep
  a strong reference to every background task; release FFI proxies
  (`_ffi.proxies()`).

## 5. Documentation in code (decided)

- **Docstrings on every public module, class, function, and method**, in
  **NumPy style** (decided): a one-line summary, then sections where they
  help (`Parameters`, `Returns`, `Yields`, `Raises`, `Notes`, `Examples`),
  each underlined with dashes. Details only when behavior isn't obvious from
  the signature; types live in annotations, not repeated in the docstring.
  ```python
  def retry[T](fn: Callable[[int], Awaitable[T]], *, max_attempts: int = 3) -> T:
      """Call ``fn`` until it succeeds, with jittered exponential backoff.

      Parameters
      ----------
      fn
          Called once per attempt with the 1-based attempt number.
      max_attempts
          Total attempts, including the first.

      Returns
      -------
      T
          The first successful result.

      Raises
      ------
      Exception
          The last attempt's error, once attempts run out.
      """
  ```
- Private helpers get a docstring or comment only when the *why* isn't
  obvious.

## 6. Tooling: ruff and ty (decided)

Run as code is written, before considering any change done:

```bash
uv run ruff format            # format
uv run ruff check --fix       # lint (and autofix what's safe)
uv run ty check               # type-check
uv run pytest                 # tests
```

Configuration in `pyproject.toml` (proposed values in §7):

```toml
[dependency-groups]
dev = ["pytest", "ruff", "ty", "workers-runtime-sdk"]

[tool.ruff]
line-length = 88
target-version = "py312"
extend-exclude = ["verify", ".design"]   # not SDK code; ruff would also format
                                         # the Python blocks inside Markdown

[tool.ruff.lint]
select = [
  "E", "W", "F",      # pycodestyle, pyflakes
  "I",                # import sorting
  "N",                # PEP 8 naming
  "UP",               # modern syntax for py312
  "B",                # bugbear
  "SIM", "C4", "PIE", # simplifications
  "RUF",              # ruff-specific
  "ANN",              # annotations everywhere
  "D",                # docstrings (pydocstyle)
]
ignore = ["ANN401"]   # Any is allowed where decided (§3)

[tool.ruff.lint.pydocstyle]
convention = "numpy"

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["D", "ANN"]

[tool.ty.environment]
python-version = "3.12"

[tool.ty.analysis]
allowed-unresolved-imports = ["js", "pyodide", "pyodide.**"]
```

- Not selected: ruff's `A` rules, whose `A005` flags modules named like a
  standard-library module, which every `types.py` would be (harmless with
  absolute imports: `import types` inside the package still gets the
  standard library).

## 7. Decisions

1. **Line length: 88** (decided; ruff's default, within PEP 8's allowance for
   a team-agreed limit up to 99).
2. **Exceptions in a per-folder `errors.py`** (decided), with the
   `AgentsException` base in `core/errors.py`.
3. **Relative imports are allowed** (decided): `from .types import Job`
   inside a folder, `from ..lifecycle import Lifecycle` across folders, or
   absolute (`from agents.lifecycle import …`); not enforced either way.
4. **NumPy-style docstrings** on all public API (decided), enforced by
   ruff's `D` rules with `convention = "numpy"`.
5. **The folder layout in §2** (decided), including `agent/` for the `Agent`
   class and `core/` as the base layer (§8).
6. **Python 3.12 syntax** (decided, §1).

## 8. Decided: the `Agent` class lives in `agent/`, not `core/`

The question: put upstream's `index.ts` (the `Agent` class, `AgentOptions`,
`@callable`, routing) in `core/` instead of `agent/`.

- **Upstream's `core/` is the base layer:** two small internal files
  (`Emitter`, `Disposable`, base64 redaction) that `Agent` and the MCP client
  build on ([core_disposable_store.md](./core_disposable_store.md) §1).
  In the layout here, `core/` plays the same role and is the **bottom** of
  the dependency order: `core` → `lifecycle` → capabilities → `agent` → …
- **`Agent` is the top of that order:** it imports Lifecycle and every
  capability. Moving it into `core/` would make `core/` both the lowest layer
  (what everything imports) and the highest (what imports everything), so
  the base helpers would need a new home (`utils/`, `_internal/`) to avoid
  import cycles, and `core/` would stop meaning what it means upstream.
- **Users never see the folder name:** they write `from agents import Agent`
  (§2), so the choice only affects contributors and comparing with upstream.

**Decided:** `core/` stays the base layer (matching upstream) and `agent/`
holds the `Agent` class.
