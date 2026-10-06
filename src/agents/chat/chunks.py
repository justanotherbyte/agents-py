"""The chunks ``on_chat_message`` yields: AI SDK v6 ``UIMessageChunk``s.

``.design/chat_models.md`` §2. Fields match ``ai@6.0.300`` in snake_case;
the codec (`agents.chat.codec`) writes the camelCase wire form. Use them
through the module::

    from agents.chat import chunks

    yield chunks.TextDelta(id="t1", delta="Hello")
"""

from dataclasses import dataclass
from typing import ClassVar, Literal

from ..core.records import wire
from ..core.types import JSONValue

__all__ = (
    "Abort",
    "Data",
    "Error",
    "File",
    "Finish",
    "FinishReason",
    "FinishStep",
    "MessageMetadata",
    "ProviderMetadata",
    "ReasoningDelta",
    "ReasoningEnd",
    "ReasoningStart",
    "SourceDocument",
    "SourceUrl",
    "Start",
    "StartStep",
    "TextDelta",
    "TextEnd",
    "TextStart",
    "ToolApprovalRequest",
    "ToolInputAvailable",
    "ToolInputDelta",
    "ToolInputError",
    "ToolInputStart",
    "ToolOutputAvailable",
    "ToolOutputDenied",
    "ToolOutputError",
    "UIMessageChunk",
)

type ProviderMetadata = dict[str, dict[str, JSONValue]]
"""Provider name → provider-specific JSON."""

type FinishReason = Literal[
    "stop", "length", "content-filter", "tool-calls", "error", "other"
]


# Text


@dataclass(slots=True, kw_only=True)
class TextStart:
    """A text part begins."""

    type: ClassVar[str] = "text-start"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class TextDelta:
    """More text for the part with ``id``."""

    type: ClassVar[str] = "text-delta"
    id: str
    delta: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class TextEnd:
    """The text part with ``id`` is complete."""

    type: ClassVar[str] = "text-end"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


# Reasoning


@dataclass(slots=True, kw_only=True)
class ReasoningStart:
    """A reasoning part begins."""

    type: ClassVar[str] = "reasoning-start"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class ReasoningDelta:
    """More reasoning text."""

    type: ClassVar[str] = "reasoning-delta"
    id: str
    delta: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class ReasoningEnd:
    """The reasoning part is complete."""

    type: ClassVar[str] = "reasoning-end"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


# Tool input


@dataclass(slots=True, kw_only=True)
class ToolInputStart:
    """A tool call begins; its input streams next."""

    type: ClassVar[str] = "tool-input-start"
    tool_call_id: str = wire("toolCallId")
    tool_name: str = wire("toolName")
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    title: str | None = None


@dataclass(slots=True, kw_only=True)
class ToolInputDelta:
    """A raw slice of the tool call's JSON input."""

    type: ClassVar[str] = "tool-input-delta"
    tool_call_id: str = wire("toolCallId")
    input_text_delta: str = wire("inputTextDelta")


@dataclass(slots=True, kw_only=True)
class ToolInputAvailable:
    """The tool call's complete input."""

    type: ClassVar[str] = "tool-input-available"
    tool_call_id: str = wire("toolCallId")
    tool_name: str = wire("toolName")
    input: JSONValue
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    title: str | None = None


@dataclass(slots=True, kw_only=True)
class ToolInputError:
    """The tool call's input was invalid."""

    type: ClassVar[str] = "tool-input-error"
    tool_call_id: str = wire("toolCallId")
    tool_name: str = wire("toolName")
    input: JSONValue
    error_text: str = wire("errorText")
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    title: str | None = None


# Tool approval


@dataclass(slots=True, kw_only=True)
class ToolApprovalRequest:
    """The tool call needs the user's approval before it runs."""

    type: ClassVar[str] = "tool-approval-request"
    approval_id: str = wire("approvalId")
    tool_call_id: str = wire("toolCallId")
    approval_descriptor: JSONValue = wire("approvalDescriptor", None)
    input_schema_input: JSONValue = wire("inputSchemaInput", None)
    signature: str | None = None


# Tool output


@dataclass(slots=True, kw_only=True)
class ToolOutputAvailable:
    """The tool call's result."""

    type: ClassVar[str] = "tool-output-available"
    tool_call_id: str = wire("toolCallId")
    output: JSONValue
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    preliminary: bool | None = None
    """``True``: a later chunk may replace this output."""


@dataclass(slots=True, kw_only=True)
class ToolOutputError:
    """The tool call failed."""

    type: ClassVar[str] = "tool-output-error"
    tool_call_id: str = wire("toolCallId")
    error_text: str = wire("errorText")
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None


@dataclass(slots=True, kw_only=True)
class ToolOutputDenied:
    """The user denied the tool call."""

    type: ClassVar[str] = "tool-output-denied"
    tool_call_id: str = wire("toolCallId")


# Sources and files


@dataclass(slots=True, kw_only=True)
class SourceUrl:
    """A web source the reply cites."""

    type: ClassVar[str] = "source-url"
    source_id: str = wire("sourceId")
    url: str
    title: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class SourceDocument:
    """A document source the reply cites."""

    type: ClassVar[str] = "source-document"
    source_id: str = wire("sourceId")
    media_type: str = wire("mediaType")
    title: str
    filename: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class File:
    """A file in the reply (``url`` may be a ``data:`` URL)."""

    type: ClassVar[str] = "file"
    url: str
    media_type: str = wire("mediaType")
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


# Developer data


@dataclass(slots=True, kw_only=True)
class Data:
    """App data in the reply; its wire type is ``data-{name}``.

    A later chunk with the same ``name`` and ``id`` replaces the part.
    ``transient`` data reaches clients but isn't saved.
    """

    name: str
    data: JSONValue
    id: str | None = None
    transient: bool = False


# Steps and the message


@dataclass(slots=True, kw_only=True)
class StartStep:
    """A model step begins (a ``step-start`` part)."""

    type: ClassVar[str] = "start-step"


@dataclass(slots=True, kw_only=True)
class FinishStep:
    """A model step ends."""

    type: ClassVar[str] = "finish-step"


@dataclass(slots=True, kw_only=True)
class Start:
    """The message begins (the SDK sends one if the stream doesn't)."""

    type: ClassVar[str] = "start"
    message_id: str | None = wire("messageId", None)
    message_metadata: JSONValue = wire("messageMetadata", None)


@dataclass(slots=True, kw_only=True)
class Finish:
    """The message is complete (the SDK sends one if the stream doesn't)."""

    type: ClassVar[str] = "finish"
    finish_reason: FinishReason | None = wire("finishReason", None)
    message_metadata: JSONValue = wire("messageMetadata", None)


@dataclass(slots=True, kw_only=True)
class MessageMetadata:
    """Metadata merged into the message mid-stream."""

    type: ClassVar[str] = "message-metadata"
    message_metadata: JSONValue = wire("messageMetadata")


@dataclass(slots=True, kw_only=True)
class Error:
    """The reply failed: ends the turn, and the client shows ``error_text``."""

    type: ClassVar[str] = "error"
    error_text: str = wire("errorText")


@dataclass(slots=True, kw_only=True)
class Abort:
    """The stream was aborted."""

    type: ClassVar[str] = "abort"
    reason: str | None = None


type UIMessageChunk = (
    TextStart
    | TextDelta
    | TextEnd
    | ReasoningStart
    | ReasoningDelta
    | ReasoningEnd
    | ToolInputStart
    | ToolInputDelta
    | ToolInputAvailable
    | ToolInputError
    | ToolApprovalRequest
    | ToolOutputAvailable
    | ToolOutputError
    | ToolOutputDenied
    | SourceUrl
    | SourceDocument
    | File
    | Data
    | StartStep
    | FinishStep
    | Start
    | Finish
    | MessageMetadata
    | Error
    | Abort
)
"""One chunk of a streamed reply."""
