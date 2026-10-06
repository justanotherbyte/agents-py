"""Errors raised by the fiber API."""

from ..core.errors import AgentsException

__all__ = ("FiberConflictError", "FiberNotFoundError")


class FiberConflictError(AgentsException):
    """``fiber_id`` and ``idempotency_key`` name different existing fibers."""

    def __init__(self, fiber_id: str, idempotency_key: str) -> None:
        super().__init__(
            f"fiber_id {fiber_id!r} and idempotency_key {idempotency_key!r} "
            "refer to different fibers"
        )
        self.fiber_id = fiber_id
        self.idempotency_key = idempotency_key


class FiberNotFoundError(AgentsException):
    """A fiber was deleted while `start_fiber` waited for it to finish."""

    def __init__(self, fiber_id: str) -> None:
        super().__init__(f"Fiber {fiber_id!r} no longer exists")
        self.fiber_id = fiber_id
