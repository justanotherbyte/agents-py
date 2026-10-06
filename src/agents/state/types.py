"""Types for the State capability."""

from collections.abc import Callable
from typing import Literal

from ..lifecycle.types import Connection

__all__ = ("StateChangeHandler", "StateSource", "StateValidator")

type StateSource = Connection | Literal["server"]
"""Where a change came from: ``"server"`` for host code, or the connection a
client's change arrived on."""

type StateValidator[T] = Callable[[T, StateSource], None]
"""Gates a change before it's saved; raise to reject it. Synchronous."""

type StateChangeHandler[T] = Callable[[T, StateSource], None]
"""Runs after a change is saved. Synchronous; failures are logged."""
