"""What a send does while a turn is busy (upstream ``chat/submit-concurrency.ts``).

The controller only decides and keeps counts; the agent applies the
decision. Times are the event loop's clock, in seconds.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from .types import Debounce, MessageConcurrency

__all__ = ("SubmitConcurrencyController", "SubmitDecision")

_IDLE_POLL_SECONDS = 0.005


@dataclass(slots=True, frozen=True)
class SubmitDecision:
    """Whether a send runs, and how it waits.

    ``submit_sequence`` is set for ``latest``, ``merge``, and debounce: a
    send whose sequence is no longer the newest has been superseded.
    ``debounce_until`` is the loop time a debounced send waits for.
    """

    action: Literal["execute", "drop"]
    strategy: MessageConcurrency | None = None
    submit_sequence: int | None = None
    debounce_until: float | None = None


class SubmitConcurrencyController:
    """Admission state for overlapping user sends."""

    __slots__ = (
        "_debounce_waiters",
        "_latest_overlapping",
        "_pending_enqueues",
        "_reset_epoch",
        "_submit_sequence",
    )

    def __init__(self) -> None:
        self._submit_sequence = 0
        self._latest_overlapping = 0
        self._pending_enqueues = 0
        self._reset_epoch = 0
        self._debounce_waiters: set[asyncio.Future[None]] = set()

    @property
    def pending_enqueue_count(self) -> int:
        """Sends admitted but not yet in the turn queue."""
        return self._pending_enqueues

    def decide(
        self,
        concurrency: MessageConcurrency,
        *,
        is_submit_message: bool,
        queued_turns: int,
    ) -> SubmitDecision:
        """Decide for a send arriving with ``queued_turns`` running or queued.

        Only user sends (not regenerations) that overlap other work are
        subject to the policy.
        """
        if not is_submit_message or queued_turns + self._pending_enqueues == 0:
            return SubmitDecision("execute")
        if concurrency == "drop":
            return SubmitDecision("drop", concurrency)
        if concurrency == "queue":
            return SubmitDecision("execute", concurrency)
        self._submit_sequence += 1
        sequence = self._latest_overlapping = self._submit_sequence
        if isinstance(concurrency, Debounce):
            until = asyncio.get_running_loop().time() + concurrency.seconds
            return SubmitDecision("execute", concurrency, sequence, until)
        return SubmitDecision("execute", concurrency, sequence)

    def begin_enqueue(self) -> Callable[[], None]:
        """Count a send between admission and the turn queue.

        Returns its idempotent release, which must be called once the send
        reaches the queue or is abandoned. A release from before the last
        `reset` does nothing.
        """
        self._pending_enqueues += 1
        epoch = self._reset_epoch
        released = False

        def release() -> None:
            nonlocal released
            if released:
                return
            released = True
            if self._reset_epoch == epoch:
                self._pending_enqueues = max(0, self._pending_enqueues - 1)

        return release

    def is_superseded(self, submit_sequence: int | None) -> bool:
        """Whether a newer overlapping send arrived after this one."""
        return (
            submit_sequence is not None and submit_sequence < self._latest_overlapping
        )

    async def wait_until(self, deadline: float) -> None:
        """Sleep until loop time ``deadline`` (or until `cancel_active_debounce`)."""
        loop = asyncio.get_running_loop()
        remaining = deadline - loop.time()
        if remaining <= 0:
            return
        waiter: asyncio.Future[None] = loop.create_future()
        handle = loop.call_later(remaining, _resolve, waiter)
        self._debounce_waiters.add(waiter)
        try:
            await waiter
        finally:
            handle.cancel()
            self._debounce_waiters.discard(waiter)

    def cancel_active_debounce(self) -> None:
        """End every debounce wait now."""
        for waiter in list(self._debounce_waiters):
            _resolve(waiter)

    def reset(self) -> None:
        """Forget in-flight sends and end debounce waits (a chat clear)."""
        self._reset_epoch += 1
        self._pending_enqueues = 0
        self.cancel_active_debounce()

    async def wait_for_idle(
        self, wait_for_queue_idle: Callable[[], Awaitable[None]]
    ) -> None:
        """Wait until the turn queue is idle and no send is between the two."""
        while True:
            await wait_for_queue_idle()
            if self._pending_enqueues == 0:
                return
            await asyncio.sleep(_IDLE_POLL_SECONDS)


def _resolve(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)
