"""Chat messages and their parts: AI SDK v6 ``UIMessage`` as dataclasses.

``.design/chat_models.md`` §5 and §6. These are what ``self.messages`` returns
and what hooks receive; the SDK stores and sends their wire form (see
`agents.chat.codec`). Tool parts have one class per state, so a type checker
knows ``output`` exists only once the tool has finished.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import ClassVar, Literal

from ..core.records import wire
from ..core.types import JSONValue
from .chunks import ProviderMetadata

__all__ = (
    "AnyToolPart",
    "ChatMessageOptions",
    "ClientToolSchema",
    "DataPart",
    "FilePart",
    "ReasoningPart",
    "SourceDocumentPart",
    "SourceUrlPart",
    "StepStartPart",
    "TextPart",
    "ToolApproval",
    "ToolApprovalRequestedPart",
    "ToolApprovalRespondedPart",
    "ToolInputAvailablePart",
    "ToolInputStreamingPart",
    "ToolOutputAvailablePart",
    "ToolOutputDeniedPart",
    "ToolOutputErrorPart",
    "ToolPart",
    "ToolState",
    "UIMessage",
    "UIMessagePart",
    "UnknownPart",
)


@dataclass(slots=True, kw_only=True)
class TextPart:
    """Text in a message."""

    type: ClassVar[str] = "text"
    text: str
    state: Literal["streaming", "done"] | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class ReasoningPart:
    """The model's reasoning."""

    type: ClassVar[str] = "reasoning"
    text: str
    id: str | None = None
    state: Literal["streaming", "done"] | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class FilePart:
    """A file (``url`` may be a ``data:`` URL)."""

    type: ClassVar[str] = "file"
    url: str
    media_type: str = wire("mediaType")
    filename: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class SourceUrlPart:
    """A cited web source."""

    type: ClassVar[str] = "source-url"
    source_id: str = wire("sourceId")
    url: str
    title: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class SourceDocumentPart:
    """A cited document."""

    type: ClassVar[str] = "source-document"
    source_id: str = wire("sourceId")
    media_type: str = wire("mediaType")
    title: str
    filename: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class StepStartPart:
    """The start of a model step."""

    type: ClassVar[str] = "step-start"


@dataclass(slots=True, kw_only=True)
class DataPart:
    """App data; its wire type is ``data-{name}``."""

    name: str
    data: JSONValue
    id: str | None = None


type ToolState = Literal[
    "input-streaming",
    "input-available",
    "approval-requested",
    "approval-responded",
    "output-available",
    "output-error",
    "output-denied",
]


@dataclass(slots=True, kw_only=True)
class ToolApproval:
    """A tool call's approval: ``approved`` is ``None`` until answered."""

    id: str
    approved: bool | None = None
    reason: str | None = None
    descriptor: JSONValue = None
    signature: str | None = None
    input_schema_input: JSONValue = wire("inputSchemaInput", None)


@dataclass(slots=True, kw_only=True)
class ToolPart:
    """Fields every tool part has; ``isinstance(part, ToolPart)`` means "a tool".

    The wire type is ``tool-{tool_name}``, or ``dynamic-tool`` when
    ``dynamic``; each subclass is one state.
    """

    state: ClassVar[str]
    tool_name: str = wire("toolName")
    tool_call_id: str = wire("toolCallId")
    dynamic: bool = False
    title: str | None = None
    provider_executed: bool | None = wire("providerExecuted", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    call_provider_metadata: ProviderMetadata | None = wire("callProviderMetadata", None)


@dataclass(slots=True, kw_only=True)
class ToolInputStreamingPart(ToolPart):
    """The tool call's input is still streaming."""

    state: ClassVar[str] = "input-streaming"
    input: JSONValue = None
    """The partial input parsed so far."""
    raw_input: str | None = wire("rawInput", None)


@dataclass(slots=True, kw_only=True)
class ToolInputAvailablePart(ToolPart):
    """The tool call's input is complete; it hasn't run yet."""

    state: ClassVar[str] = "input-available"
    input: JSONValue


@dataclass(slots=True, kw_only=True)
class ToolApprovalRequestedPart(ToolPart):
    """The tool call waits for the user's approval."""

    state: ClassVar[str] = "approval-requested"
    input: JSONValue
    approval: ToolApproval


@dataclass(slots=True, kw_only=True)
class ToolApprovalRespondedPart(ToolPart):
    """The user approved or denied the tool call; it hasn't finished."""

    state: ClassVar[str] = "approval-responded"
    input: JSONValue
    approval: ToolApproval


@dataclass(slots=True, kw_only=True)
class ToolOutputAvailablePart(ToolPart):
    """The tool call finished with ``output``."""

    state: ClassVar[str] = "output-available"
    input: JSONValue
    output: JSONValue
    result_provider_metadata: ProviderMetadata | None = wire(
        "resultProviderMetadata", None
    )
    preliminary: bool | None = None
    approval: ToolApproval | None = None


@dataclass(slots=True, kw_only=True)
class ToolOutputErrorPart(ToolPart):
    """The tool call failed."""

    state: ClassVar[str] = "output-error"
    input: JSONValue = None
    raw_input: JSONValue = wire("rawInput", None)
    error_text: str = wire("errorText")
    result_provider_metadata: ProviderMetadata | None = wire(
        "resultProviderMetadata", None
    )
    approval: ToolApproval | None = None


@dataclass(slots=True, kw_only=True)
class ToolOutputDeniedPart(ToolPart):
    """The user denied the tool call."""

    state: ClassVar[str] = "output-denied"
    input: JSONValue
    approval: ToolApproval


type AnyToolPart = (
    ToolInputStreamingPart
    | ToolInputAvailablePart
    | ToolApprovalRequestedPart
    | ToolApprovalRespondedPart
    | ToolOutputAvailablePart
    | ToolOutputErrorPart
    | ToolOutputDeniedPart
)
"""Every tool state, for an exhaustive ``match``."""


@dataclass(slots=True, kw_only=True)
class UnknownPart:
    """A part the models don't know, kept exactly as stored.

    A newer AI SDK's part type, or a known one missing a required field.
    It's saved back unchanged.
    """

    fields: dict[str, JSONValue]

    @property
    def type(self) -> str | None:
        """The part's wire type, if it has one."""
        value = self.fields.get("type")
        return value if isinstance(value, str) else None


type UIMessagePart = (
    TextPart
    | ReasoningPart
    | AnyToolPart
    | FilePart
    | SourceUrlPart
    | SourceDocumentPart
    | StepStartPart
    | DataPart
    | UnknownPart
)
"""One part of a message."""


@dataclass(slots=True, kw_only=True)
class UIMessage:
    """A chat message (the AI SDK's ``UIMessage``)."""

    id: str
    role: Literal["system", "user", "assistant"]
    parts: list[UIMessagePart] = field(default_factory=list)
    metadata: JSONValue = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ClientToolSchema:
    """A tool the browser runs, as its client sent it (JSON Schema parameters)."""

    name: str
    description: str | None = None
    parameters: dict[str, JSONValue] | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ChatMessageOptions:
    """What ``on_chat_message`` gets about the turn it answers.

    Parameters
    ----------
    request_id
        The turn's request id.
    client_tools
        Tools the browser runs; a call to one is sent back to the client.
    body
        Extra fields from the client's request body.
    continuation
        ``True`` when the turn continues the last assistant message rather
        than answering a new user message (after a client tool result or
        approval, `continue_last_turn`, or recovery).
    """

    request_id: str
    client_tools: Sequence[ClientToolSchema] = ()
    body: dict[str, JSONValue] | None = None
    continuation: bool = False
