"""The chat wire protocol: frame types and parsing (wire protocol §7).

Port of upstream ``chat/protocol.ts``, ``chat/wire-types.ts``,
``chat/parse-protocol.ts``, and the frame helpers of ``chat/connection.ts``
and ``chat/origin-message-ids.ts``.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Literal

from .. import _ffi
from ..core.types import JSONValue
from ..websockets.connection import Connection
from ._json import dumps

__all__ = (
    "CHAT_CLEAR",
    "CHAT_MESSAGES",
    "CHAT_RECOVERING",
    "CHAT_REQUEST_CANCEL",
    "MESSAGE_UPDATED",
    "RESUME_NONE_CONTINUATION_OWNED",
    "RESUME_NONE_IDLE",
    "STREAM_PENDING",
    "STREAM_RESUME_ACK",
    "STREAM_RESUME_NONE",
    "STREAM_RESUME_REQUEST",
    "STREAM_RESUMING",
    "TOOL_APPROVAL",
    "TOOL_RESULT",
    "USE_CHAT_REQUEST",
    "USE_CHAT_RESPONSE",
    "Cancel",
    "ChatProtocolEvent",
    "ChatRequest",
    "ChatTurnOutcome",
    "Clear",
    "Messages",
    "StreamResumeAck",
    "StreamResumeRequest",
    "ToolApproval",
    "ToolResult",
    "origin_message_ids",
    "parse_protocol_message",
    "send_if_open",
    "with_origin_message_ids",
)

CHAT_MESSAGES: Final = "cf_agent_chat_messages"
USE_CHAT_REQUEST: Final = "cf_agent_use_chat_request"
USE_CHAT_RESPONSE: Final = "cf_agent_use_chat_response"
CHAT_CLEAR: Final = "cf_agent_chat_clear"
CHAT_REQUEST_CANCEL: Final = "cf_agent_chat_request_cancel"
STREAM_RESUMING: Final = "cf_agent_stream_resuming"
STREAM_RESUME_ACK: Final = "cf_agent_stream_resume_ack"
STREAM_RESUME_REQUEST: Final = "cf_agent_stream_resume_request"
STREAM_RESUME_NONE: Final = "cf_agent_stream_resume_none"
STREAM_PENDING: Final = "cf_agent_stream_pending"
TOOL_RESULT: Final = "cf_agent_tool_result"
TOOL_APPROVAL: Final = "cf_agent_tool_approval"
MESSAGE_UPDATED: Final = "cf_agent_message_updated"
CHAT_RECOVERING: Final = "cf_agent_chat_recovering"

RESUME_NONE_IDLE: Final = "idle"
"""No active, pending, or terminal stream exists (the only proof of idleness)."""
RESUME_NONE_CONTINUATION_OWNED: Final = "continuation-owned"
"""An active tool continuation belongs to another live connection."""

type ChatTurnOutcome = Literal["completed", "error", "aborted", "skipped", "recovering"]
"""How a chat request ended, sent on its terminal frame.

``skipped``: the turn never ran (a newer send superseded it, or the
concurrency policy dropped it). ``recovering``: this request stopped, but
recovery continues the turn under a new request with the same
``messageIds``.
"""


# Incoming frames


@dataclass(slots=True, frozen=True)
class ChatRequest:
    """``cf_agent_use_chat_request``: start a turn (``init`` is a ``RequestInit``)."""

    id: str
    init: dict[str, Any]


@dataclass(slots=True, frozen=True)
class Clear:
    """``cf_agent_chat_clear``: clear the history."""


@dataclass(slots=True, frozen=True)
class Cancel:
    """``cf_agent_chat_request_cancel``: stop a running turn."""

    id: str


@dataclass(slots=True, frozen=True)
class ToolResult:
    """``cf_agent_tool_result``: the result of a tool the browser ran."""

    tool_call_id: str
    tool_name: str
    output: JSONValue
    state: str | None = None
    error_text: str | None = None
    auto_continue: bool | None = None
    client_tools: Sequence[dict[str, Any]] | None = None


@dataclass(slots=True, frozen=True)
class ToolApproval:
    """``cf_agent_tool_approval``: the user approved or denied a tool call."""

    tool_call_id: str
    approved: bool
    auto_continue: bool | None = None


@dataclass(slots=True, frozen=True)
class StreamResumeRequest:
    """``cf_agent_stream_resume_request``: ask whether there is a stream to resume."""

    probe_id: str | None = None


@dataclass(slots=True, frozen=True)
class StreamResumeAck:
    """``cf_agent_stream_resume_ack``: send the replay of ``id``."""

    id: str


@dataclass(slots=True, frozen=True)
class Messages:
    """``cf_agent_chat_messages`` from a client: its transcript."""

    messages: Sequence[Any] = field(default_factory=tuple)


type ChatProtocolEvent = (
    ChatRequest
    | Clear
    | Cancel
    | ToolResult
    | ToolApproval
    | StreamResumeRequest
    | StreamResumeAck
    | Messages
)


def parse_protocol_message(raw: str) -> ChatProtocolEvent | None:
    """Return the chat event a text frame carries, or ``None``.

    ``None`` means the frame isn't JSON or isn't a chat frame, and belongs to
    the agent's ``on_message``. Field types aren't checked, as upstream.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    match data.get("type"):
        case "cf_agent_use_chat_request":
            init = data.get("init")
            return ChatRequest(id=data.get("id"), init=init if init is not None else {})
        case "cf_agent_chat_clear":
            return Clear()
        case "cf_agent_chat_request_cancel":
            return Cancel(id=data.get("id"))
        case "cf_agent_tool_result":
            return ToolResult(
                tool_call_id=data.get("toolCallId"),
                tool_name=data.get("toolName") or "",
                output=data.get("output"),
                state=data.get("state"),
                error_text=data.get("errorText"),
                auto_continue=data.get("autoContinue"),
                client_tools=data.get("clientTools"),
            )
        case "cf_agent_tool_approval":
            return ToolApproval(
                tool_call_id=data.get("toolCallId"),
                approved=data.get("approved"),
                auto_continue=data.get("autoContinue"),
            )
        case "cf_agent_stream_resume_request":
            probe = data.get("probeId")
            return StreamResumeRequest(
                probe_id=probe if isinstance(probe, str) else None
            )
        case "cf_agent_stream_resume_ack":
            return StreamResumeAck(id=data.get("id"))
        case "cf_agent_chat_messages":
            messages = data.get("messages")
            return Messages(messages=messages if messages is not None else ())
    return None


# Outgoing frames


def send_if_open(connection: Connection, frame: dict[str, Any] | str) -> bool:
    """Send a frame; return ``False`` if the socket had already closed."""
    try:
        connection.send(frame if isinstance(frame, str) else dumps(frame))
    except _ffi.JsException:
        return False
    return True


def origin_message_ids(messages: Any) -> list[str] | None:
    """Return the ids of the trailing run of user messages in a request.

    Terminal frames echo them as ``messageIds``, so a client settles exactly
    the sends a completion, error, or cancellation belongs to.
    """
    if not isinstance(messages, list):
        return None
    ids: list[str] = []
    for message in reversed(messages):
        if not isinstance(message, dict) or message.get("role") != "user":
            break
        message_id = message.get("id")
        if isinstance(message_id, str) and message_id:
            ids.insert(0, message_id)
    return ids or None


def with_origin_message_ids(
    frame: dict[str, Any], message_ids: Sequence[str] | None
) -> dict[str, Any]:
    """Return ``frame`` with ``messageIds`` added if it's terminal and lacks them."""
    if (
        not message_ids
        or "messageIds" in frame
        or not (frame.get("done") or frame.get("error"))
    ):
        return frame
    return {**frame, "messageIds": list(message_ids)}
