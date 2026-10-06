"""Settings and results of ``AIChatAgent`` (upstream ``chat/lifecycle.ts``)."""

import math
from dataclasses import dataclass
from typing import Literal

from ..agent.types import AgentOptions
from .messages import UIMessage

__all__ = (
    "AIChatAgentOptions",
    "ChatResponseResult",
    "Debounce",
    "MessageConcurrency",
    "SaveMessagesResult",
)


@dataclass(slots=True, frozen=True)
class Debounce:
    """Run only the newest of overlapping sends, once none came for ``seconds``.

    Raises
    ------
    ValueError
        If ``seconds`` is negative or not finite.
    """

    seconds: float = 0.75

    def __post_init__(self) -> None:  # noqa: D105
        if not math.isfinite(self.seconds) or self.seconds < 0:
            raise ValueError(f"Debounce seconds must be >= 0, not {self.seconds!r}")


type MessageConcurrency = Literal["queue", "latest", "merge", "drop"] | Debounce
"""What a user's send does while another turn is running or queued.

- ``"queue"``: run every send, in order.
- ``"latest"``: only the newest overlapping send runs; the others still save
  their user messages.
- ``"merge"``: overlapping sends' user messages become one user message,
  answered once.
- ``"drop"``: overlapping sends are rejected (the client rolls them back).
- ``Debounce(seconds)``: like ``"latest"``, after a quiet window.

Regenerations and programmatic turns (`save_messages`) always queue.
"""


@dataclass(frozen=True, slots=True, kw_only=True)
class AIChatAgentOptions(AgentOptions):
    """Per-class settings for a chat agent (``options = AIChatAgentOptions(...)``).

    Extends `AgentOptions`; see it for the agent-wide settings.

    Parameters
    ----------
    message_concurrency
        What a send does while another turn is running (`MessageConcurrency`).
    max_persisted_messages
        Keep at most this many stored messages, deleting the oldest after
        each save (``None``: no limit). Storage only: what the model sees is
        up to ``on_chat_message``.
    hydration_byte_budget
        How many bytes of the newest stored messages ``self.messages`` holds
        after a wake (``None``: all of them). The full history stays stored
        and is what ``get-messages`` serves.
    """

    message_concurrency: MessageConcurrency = "queue"
    max_persisted_messages: int | None = None
    hydration_byte_budget: int | None = 32 * 1024 * 1024


@dataclass(slots=True, kw_only=True, frozen=True)
class SaveMessagesResult:
    """How a turn started by ``save_messages`` or ``continue_last_turn`` ended.

    ``skipped``: the chat was cleared before it ran (or, for
    ``continue_last_turn``, there was no assistant message). ``error``
    carries the error's message.
    """

    request_id: str
    status: Literal["completed", "error", "skipped", "aborted"]
    error: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ChatResponseResult:
    """A finished turn, as ``on_chat_response`` receives it.

    Parameters
    ----------
    message
        The assistant message the turn produced (as saved).
    request_id
        The turn's request id.
    continuation
        Whether the turn continued the previous assistant message.
    status
        How it ended.
    error
        The error's message, when ``status`` is ``error``.
    """

    message: UIMessage
    request_id: str
    continuation: bool
    status: Literal["completed", "error", "aborted"]
    error: str | None = None
