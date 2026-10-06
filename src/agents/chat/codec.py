"""Converting chat models to and from their wire form (``.design/chat_engine.md`` §4.2).

The SDK's chat machinery works on wire-form dictionaries (camelCase, as
stored and sent); the typed chunks and messages are the public surface,
converted here. Optional fields that are ``None`` (or a ``False`` flag) are
left out; required fields are always written. A part that doesn't decode
cleanly becomes `UnknownPart` and is written back exactly as it was.
"""

import dataclasses
from collections.abc import Mapping
from functools import cache
from typing import Any, NamedTuple, cast

from ..core.types import JSONValue
from .chunks import Data, UIMessageChunk
from .messages import (
    AnyToolPart,
    DataPart,
    FilePart,
    ReasoningPart,
    SourceDocumentPart,
    SourceUrlPart,
    StepStartPart,
    TextPart,
    ToolApproval,
    ToolApprovalRequestedPart,
    ToolApprovalRespondedPart,
    ToolInputAvailablePart,
    ToolInputStreamingPart,
    ToolOutputAvailablePart,
    ToolOutputDeniedPart,
    ToolOutputErrorPart,
    ToolPart,
    UIMessage,
    UIMessagePart,
    UnknownPart,
)

__all__ = (
    "WireMessage",
    "chunk_to_wire",
    "message_from_wire",
    "message_to_wire",
    "part_from_wire",
    "part_to_wire",
)

type WireMessage = dict[str, Any]
"""A message in wire form: what Sessions stores and clients send."""

_PARTS: dict[str, type] = {
    cls.type: cls
    for cls in (
        TextPart,
        ReasoningPart,
        FilePart,
        SourceUrlPart,
        SourceDocumentPart,
        StepStartPart,
    )
}
_TOOL_STATES: dict[str, type[ToolPart]] = {
    cls.state: cls
    for cls in (
        ToolInputStreamingPart,
        ToolInputAvailablePart,
        ToolApprovalRequestedPart,
        ToolApprovalRespondedPart,
        ToolOutputAvailablePart,
        ToolOutputErrorPart,
        ToolOutputDeniedPart,
    )
}
# A tool part's name and dynamic flag are folded into its wire `type`.
_TOOL_TYPE_FIELDS = frozenset({"tool_name", "dynamic"})


class _Field(NamedTuple):
    name: str
    wire: str
    required: bool
    default: Any
    nested: bool


@cache
def _fields(cls: Any) -> tuple[_Field, ...]:
    out = []
    for f in dataclasses.fields(cls):
        required = (
            f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING
        )
        out.append(
            _Field(
                name=f.name,
                wire=f.metadata.get("wire", f.name),
                required=required,
                default=None if required else f.default,
                nested=f.name == "approval",
            )
        )
    return tuple(out)


# Encoding


def chunk_to_wire(chunk: UIMessageChunk) -> dict[str, Any]:
    """Return a chunk's wire form (``type`` first, as the AI SDK writes it)."""
    if isinstance(chunk, Data):
        out: dict[str, Any] = {"type": f"data-{chunk.name}"}
        if chunk.id is not None:
            out["id"] = chunk.id
        out["data"] = chunk.data
        if chunk.transient:
            out["transient"] = True
        return out
    return {"type": chunk.type, **_record_to_wire(chunk)}


def part_to_wire(part: UIMessagePart) -> dict[str, Any]:
    """Return a part's wire form."""
    match part:
        case UnknownPart():
            return dict(part.fields)
        case DataPart():
            out: dict[str, Any] = {"type": f"data-{part.name}"}
            if part.id is not None:
                out["id"] = part.id
            out["data"] = part.data
            return out
        case ToolPart():
            kind = "dynamic-tool" if part.dynamic else f"tool-{part.tool_name}"
            return {
                "type": kind,
                "toolCallId": part.tool_call_id,
                "toolName": part.tool_name,
                "state": part.state,
                **_record_to_wire(part, skip=_TOOL_TYPE_FIELDS | {"tool_call_id"}),
            }
        case _:
            return {"type": part.type, **_record_to_wire(part)}


def message_to_wire(message: UIMessage) -> WireMessage:
    """Return a message's wire form."""
    out: WireMessage = {
        "id": message.id,
        "role": message.role,
        "parts": [part_to_wire(part) for part in message.parts],
    }
    if message.metadata is not None:
        out["metadata"] = message.metadata
    return out


def _record_to_wire(record: Any, skip: frozenset[str] = frozenset()) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for f in _fields(type(record)):
        if f.name in skip:
            continue
        value = getattr(record, f.name)
        if not f.required and value is f.default:
            continue  # optional and unset: left out on the wire
        if f.nested and value is not None:
            value = _record_to_wire(value)
        out[f.wire] = value
    return out


# Decoding


def message_from_wire(message: Mapping[str, Any]) -> UIMessage:
    """Return the typed form of a wire message (its parts never fail to decode).

    Raises
    ------
    KeyError
        If the message has no ``id``, ``role``, or ``parts``.
    """
    return UIMessage(
        id=message["id"],
        role=message["role"],
        parts=[part_from_wire(part) for part in message["parts"]],
        metadata=message.get("metadata"),
    )


def part_from_wire(part: Mapping[str, Any]) -> UIMessagePart:
    """Return the typed form of a wire part, or `UnknownPart` if it doesn't fit."""
    try:
        return _decode_part(part)
    except (KeyError, TypeError, ValueError):
        return _unknown(part)


def _decode_part(part: Mapping[str, Any]) -> UIMessagePart:
    kind = part.get("type")
    if not isinstance(kind, str):
        return _unknown(part)
    if kind.startswith("data-"):
        return DataPart(name=kind[len("data-") :], data=part["data"], id=part.get("id"))
    if kind == "dynamic-tool" or kind.startswith("tool-"):
        cls = _TOOL_STATES.get(part.get("state", ""))
        if cls is None:
            return _unknown(part)
        dynamic = kind == "dynamic-tool"
        tool_name = part["toolName"] if dynamic else kind[len("tool-") :]
        fields = _record_from_wire(cls, part, skip=_TOOL_TYPE_FIELDS)
        return cast(AnyToolPart, cls(tool_name=tool_name, dynamic=dynamic, **fields))
    cls = _PARTS.get(kind)
    if cls is None:
        return _unknown(part)
    return cls(**_record_from_wire(cls, part))


def _unknown(part: Mapping[str, Any]) -> UnknownPart:
    return UnknownPart(fields=cast(dict[str, JSONValue], dict(part)))


def _record_from_wire(
    cls: type, wire: Mapping[str, Any], skip: frozenset[str] = frozenset()
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    for f in _fields(cls):
        if f.name in skip:
            continue
        if f.wire not in wire:
            if f.required:
                raise KeyError(f.wire)
            continue
        value: JSONValue | ToolApproval = wire[f.wire]
        if f.nested and value is not None:
            if not isinstance(value, Mapping):
                raise TypeError("approval must be an object")
            value = ToolApproval(**_record_from_wire(ToolApproval, value))
        kwargs[f.name] = value
    return kwargs
