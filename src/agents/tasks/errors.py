"""Exceptions and control-flow signals for the Tasks capability.

The signals (`TaskSuspension`, `TaskCancellation`, `AttemptSupersededError`)
end an execution attempt by unwinding through user code. They subclass
``BaseException``, like ``asyncio.CancelledError``, so a handler's
``except Exception:`` can't swallow a sleep or a cancellation
(``.design/tasks_engine.md`` §2.2 item 1).
"""

from ..core.errors import AgentsException

__all__ = (
    "AttemptSupersededError",
    "DuplicateTaskStepError",
    "MissingTaskDefinitionError",
    "NonRetryableError",
    "StepTimeoutError",
    "TaskCancellation",
    "TaskReplayDivergedError",
    "TaskSerializationError",
    "TaskSuspension",
    "is_non_retryable",
)


class NonRetryableError(AgentsException):
    """Raise from a step function to fail the run now, skipping retries.

    Errors of any class named ``NonRetryableError`` count too, as upstream.
    """


def is_non_retryable(error: BaseException) -> bool:
    """Return whether ``error`` should skip a step's remaining retries."""
    return isinstance(error, NonRetryableError) or type(error).__name__ == (
        "NonRetryableError"
    )


class DuplicateTaskStepError(AgentsException):
    """A run used the same step name twice; step names are journal keys.

    Parameters
    ----------
    step_name
        The repeated name.
    """

    def __init__(self, step_name: str) -> None:
        super().__init__(
            f"Step name {step_name!r} was already used in this run. Step names "
            f"are durable journal keys; suffix loop steps with a stable index, "
            f"e.g. {f'{step_name}:0'!r}."
        )
        self.step_name = step_name


class TaskReplayDivergedError(AgentsException):
    """A replay found a journal this handler can't have written.

    Change an in-flight definition's steps by versioning its name instead.

    Parameters
    ----------
    step_name
        Where the replay diverged.
    detail
        What differed.
    """

    def __init__(self, step_name: str, detail: str) -> None:
        super().__init__(
            f"Replay diverged from the journal at step {step_name!r}: {detail}. "
            "Version the definition (e.g. a new name) instead of changing the "
            "steps of in-flight runs."
        )
        self.step_name = step_name


class MissingTaskDefinitionError(AgentsException):
    """A run's definition is no longer registered (removed or renamed).

    The run fails; it's never deleted or run against a different handler.

    Parameters
    ----------
    definition
        The definition name the run was started with.
    """

    def __init__(self, definition: str) -> None:
        super().__init__(
            f"No task definition named {definition!r} is registered. A "
            "deployment removed or renamed it while this run was active; "
            "register it again to let the run finish."
        )
        self.definition = definition


class TaskSerializationError(AgentsException, TypeError):
    """A task input, step result, metadata, or result isn't JSON, or is too big.

    Parameters
    ----------
    context
        What was being serialized.
    detail
        Why it failed.
    """

    def __init__(self, context: str, detail: str) -> None:
        super().__init__(f"Cannot serialize {context}: {detail}")


class StepTimeoutError(AgentsException, TimeoutError):
    """A step attempt ran longer than its timeout (it's retried like any error).

    Parameters
    ----------
    step_name
        The step.
    attempt
        The attempt that timed out.
    timeout_ms
        The timeout, in milliseconds.
    """

    def __init__(self, step_name: str, attempt: int, timeout_ms: int) -> None:
        super().__init__(
            f"Step {step_name!r} attempt {attempt} timed out after "
            f"{timeout_ms / 1000:g} s"
        )
        self.step_name = step_name


class TaskSuspension(BaseException):
    """Ends an attempt while the run waits for a durable deadline (internal).

    Parameters
    ----------
    wake_at_ms
        When the run should wake (epoch ms).
    reason
        ``"sleep"`` or ``"retry"``.
    """

    def __init__(self, wake_at_ms: int, reason: str) -> None:
        super().__init__(f"suspended until {wake_at_ms} ({reason})")
        self.wake_at_ms = wake_at_ms
        self.reason = reason


class TaskCancellation(BaseException):
    """Ends an attempt whose run was cancelled (internal).

    Parameters
    ----------
    reason
        The cancellation reason, if given.
    """

    def __init__(self, reason: str | None) -> None:
        super().__init__(reason or "cancelled")
        self.reason = reason


class AttemptSupersededError(BaseException):
    """Ends an attempt that a newer attempt has replaced (internal).

    Every write the stale attempt might still make is fenced by its
    generation, so unwinding is all that's left to do.

    Parameters
    ----------
    run_id
        The run.
    """

    def __init__(self, run_id: str) -> None:
        super().__init__(
            f"Task attempt superseded: run {run_id!r} is no longer claimed by "
            "this attempt"
        )
