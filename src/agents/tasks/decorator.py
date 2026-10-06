"""``@task``: task definitions as methods, and the handles they return.

Python's form of upstream's ``taskDefinitions`` map and ``tasks.handle()``
(``.design/scheduling_queue_tasks_api.md`` §2.4, §2.11). The decorator
marks a method; ``Tasks(target=obj)`` collects it; accessing it on ``obj``
returns a `TaskHandle` bound to that capability.
"""

import inspect
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Self, overload

from .registry import tasks_for
from .types import TaskReceipt, TaskRun, TaskStep

if TYPE_CHECKING:
    from ..core.types import JSONValue
    from .tasks import Tasks

__all__ = ("TaskHandle", "task")


class task[In, Out]:  # noqa: N801  (a decorator, named like one)
    """Make an ``async`` method a task definition, named after the method.

    ``self.<method>`` then gives a `TaskHandle` for starting and inspecting
    its runs. The handler is called as ``method(self, input, step)`` and
    replays from its first line on every attempt.

    Raises
    ------
    TypeError
        If the function isn't ``async`` (checked when the class is defined).

    Examples
    --------
    >>> class Reports(Agent):
    ...     @task
    ...     async def build(self, input: dict, step: TaskStep) -> str: ...
    ...
    ...     async def on_request(self, request):
    ...         receipt = await self.build.run({"id": 1})
    """

    __slots__ = ("fn", "name")

    def __init__(self, fn: Callable[[Any, In, TaskStep], Awaitable[Out]]) -> None:
        name = getattr(fn, "__name__", None)
        if not isinstance(name, str) or not inspect.iscoroutinefunction(fn):
            raise TypeError(f"Task functions must be async functions: {fn!r}")
        self.fn = fn
        self.name = name

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Self: ...
    @overload
    def __get__(
        self, instance: object, owner: type | None = None
    ) -> "TaskHandle[In, Out]": ...
    def __get__(
        self, instance: object | None, owner: type | None = None
    ) -> "Self | TaskHandle[In, Out]":
        """Return the handle bound to ``instance`` (the descriptor, on the class).

        Raises
        ------
        RuntimeError
            If no Tasks capability was created with ``target=instance``.
        """
        if instance is None:
            return self
        tasks = tasks_for(instance)
        if tasks is None:
            raise RuntimeError(
                f"No Tasks capability was created with target="
                f"{type(instance).__name__} instance; create Tasks(target=self) "
                f"to use @task {self.name!r}"
            )
        return TaskHandle(tasks, self.name)


class TaskHandle[In, Out]:
    """Starts and inspects the runs of one definition (sees only its runs).

    Parameters
    ----------
    tasks
        The owning capability.
    name
        The definition's name.
    """

    __slots__ = ("_tasks", "name")

    def __init__(self, tasks: "Tasks", name: str) -> None:
        self._tasks = tasks
        self.name = name

    async def run(
        self,
        input: In,
        *,
        idempotency_key: str | None = None,
        run_id: str | None = None,
        metadata: "dict[str, JSONValue] | None" = None,
        retain: bool = True,
    ) -> TaskReceipt:
        """Durably accept a run and start it (see ``Tasks.run``)."""
        return await self._tasks.run(
            self.name,
            input,  # ty: ignore[invalid-argument-type]
            idempotency_key=idempotency_key,
            run_id=run_id,
            metadata=metadata,
            retain=retain,
        )

    async def get(self, run_id: str) -> TaskRun[Out] | None:
        """Return one run of this definition, or ``None``."""
        return await self._tasks._snapshot(run_id, self.name)

    async def get_by_idempotency_key(self, idempotency_key: str) -> TaskRun[Out] | None:
        """Return this definition's run with ``idempotency_key``, or ``None``."""
        run = await self._tasks.get_by_idempotency_key(idempotency_key)
        return run if run is not None and run.definition == self.name else None

    async def cancel(self, run_id: str, reason: str | None = None) -> bool:
        """Cancel a run of this definition; ``False`` for another definition's run."""
        run = await self._tasks._snapshot(run_id, self.name)
        if run is None:
            return False
        return await self._tasks.cancel(run_id, reason)
