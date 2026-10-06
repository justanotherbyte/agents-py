"""One chat turn at a time (upstream ``chat/turn-queue.ts``).

Turns run in the order they were queued. A generation counter, advanced by
`TurnQueue.reset` (a chat clear), turns everything queued before it stale:
a stale turn's function isn't called.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

__all__ = ("TurnQueue", "TurnResult")


@dataclass(slots=True, frozen=True)
class TurnResult[T]:
    """``completed`` with the function's return value, or ``stale``."""

    status: Literal["completed", "stale"]
    value: T | None = None


class TurnQueue:
    """A serial queue for chat turns, with generation-based invalidation."""

    __slots__ = ("_active_request_id", "_counts", "_generation", "_tail")

    def __init__(self) -> None:
        self._tail: asyncio.Future[None] | None = None
        self._generation = 0
        self._active_request_id: str | None = None
        self._counts: dict[int, int] = {}

    @property
    def generation(self) -> int:
        """The current generation."""
        return self._generation

    @property
    def active_request_id(self) -> str | None:
        """The request id of the running turn, if any."""
        return self._active_request_id

    @property
    def is_active(self) -> bool:
        """Whether a turn is running."""
        return self._active_request_id is not None

    async def enqueue[T](
        self,
        request_id: str,
        fn: Callable[[], Awaitable[T]],
        *,
        generation: int | None = None,
    ) -> TurnResult[T]:
        """Run ``fn`` once every turn queued before it has finished.

        ``generation`` defaults to the current one; if the queue has moved
        past it by the time this turn's start comes, ``fn`` isn't called and
        the result is ``stale``. Cancelling the caller while it waits leaves
        the order of the turns behind it intact.
        """
        captured = self._generation if generation is None else generation
        self._counts[captured] = self._counts.get(captured, 0) + 1
        previous = self._tail
        release: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._tail = release
        try:
            if previous is not None:
                # Shielded: cancelling this caller mustn't cancel the turn
                # ahead of it.
                await asyncio.shield(previous)
        except asyncio.CancelledError:
            self._decrement(captured)
            if previous is not None:
                previous.add_done_callback(lambda _: _resolve(release))
            raise
        if self._generation != captured:
            self._decrement(captured)
            _resolve(release)
            return TurnResult("stale")
        self._active_request_id = request_id
        try:
            return TurnResult("completed", await fn())
        finally:
            self._active_request_id = None
            self._decrement(captured)
            _resolve(release)

    def reset(self) -> None:
        """Advance the generation: turns queued before now become stale."""
        self._generation += 1

    async def wait_for_idle(self) -> None:
        """Wait until nothing is running or queued."""
        # The tail is the last turn queued, so once it's done all are; a turn
        # queued while waiting becomes the new tail.
        while (tail := self._tail) is not None and not tail.done():
            await asyncio.shield(tail)

    def queued_count(self, generation: int | None = None) -> int:
        """Count running plus queued turns of ``generation`` (default: current)."""
        return self._counts.get(
            self._generation if generation is None else generation, 0
        )

    def _decrement(self, generation: int) -> None:
        count = self._counts.get(generation, 1) - 1
        if count <= 0:
            self._counts.pop(generation, None)
        else:
            self._counts[generation] = count


def _resolve(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)
