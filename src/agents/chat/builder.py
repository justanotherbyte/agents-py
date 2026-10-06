"""Folding stream chunks into a message's parts (upstream ``chat/message-builder.ts``).

Works on wire-form dictionaries (``.design/chat_engine.md`` §4.2), mutating a
message's ``parts`` list in place as upstream does. An absent key plays the
part of JavaScript's ``undefined``: stored JSON never has one.

The rules that matter: text and reasoning go ``streaming`` → ``done``; tool
parts only move forward (providers may resend earlier tool calls in a
continuation); a tool call's raw input text is kept aside, so a partial JSON
string is never saved; and tool input is always a JSON object.
"""

import json
from collections.abc import Iterable
from typing import Any

__all__ = (
    "BuilderScratch",
    "apply_chunk_to_parts",
    "apply_late_tool_input",
    "is_late_tool_input_chunk",
    "is_replay_chunk",
    "late_tool_input_forward_chunks",
    "normalize_tool_input",
    "partial_stream_text",
)

type Part = dict[str, Any]
type Chunk = dict[str, Any]

_APPROVAL_STATES = frozenset({"approval-requested", "approval-responded"})
_SETTLED = frozenset({"output-available", "output-error", "output-denied"})


class BuilderScratch:
    """Per-part state upstream keeps in module ``WeakMap``s.

    A tool call's raw input text, and whether its input is only provisional
    (from a delta, replaceable by a late ``tool-input-available``). Keyed by
    the part's identity; each entry holds the part, so its id can't be reused.
    """

    __slots__ = ("_provisional", "_raw_input")

    def __init__(self) -> None:
        self._raw_input: dict[int, tuple[Part, str]] = {}
        self._provisional: dict[int, Part] = {}

    def raw_input(self, part: Part) -> str | None:
        """Return the raw input text collected for ``part``."""
        entry = self._raw_input.get(id(part))
        return entry[1] if entry is not None else None

    def add_raw_input(self, part: Part, text: str) -> None:
        """Append to ``part``'s raw input text."""
        self._raw_input[id(part)] = (part, (self.raw_input(part) or "") + text)

    def drop_raw_input(self, part: Part) -> None:
        """Forget ``part``'s raw input text."""
        self._raw_input.pop(id(part), None)

    def is_provisional(self, part: Part) -> bool:
        """Return whether ``part``'s input came from a delta."""
        return id(part) in self._provisional

    def mark_provisional(self, part: Part, provisional: bool) -> None:
        """Mark (or unmark) ``part``'s input as provisional."""
        if provisional:
            self._provisional[id(part)] = part
        else:
            self._provisional.pop(id(part), None)


def normalize_tool_input(raw: Any) -> tuple[Any, bool]:
    """Return ``raw`` as a JSON object, and whether it changed.

    An object stays; a string holding a JSON object is parsed; anything else
    (``None``, an array, unparseable text) becomes ``{}``. Some providers
    reject tool input that isn't an object.
    """
    if isinstance(raw, dict):
        return raw, False
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            return parsed, True
    return {}, True


def is_late_tool_input_chunk(parts: list[Part], chunk: Chunk) -> bool:
    """Return whether ``chunk`` is tool input arriving after its approval request."""
    if chunk.get("type") != "tool-input-available":
        return False
    existing = _find_tool_part(parts, chunk.get("toolCallId"))
    return existing is not None and existing.get("state") in _APPROVAL_STATES


def apply_late_tool_input(
    parts: list[Part], chunk: Chunk, scratch: BuilderScratch | None = None
) -> bool:
    """Fill a tool part's input without leaving its approval state.

    Only an absent or provisional input is replaced. Returns whether it was.
    """
    scratch = scratch if scratch is not None else BuilderScratch()
    if not is_late_tool_input_chunk(parts, chunk):
        return False
    part = _find_tool_part(parts, chunk.get("toolCallId"))
    assert part is not None
    if "input" in part and not scratch.is_provisional(part):
        return False
    part["input"] = normalize_tool_input(chunk.get("input"))[0]
    scratch.mark_provisional(part, False)
    _copy_call_fields(part, chunk)
    return True


def late_tool_input_forward_chunks(
    parts: list[Part], chunk: Chunk, approval_request: Chunk | None = None
) -> list[Chunk]:
    """Return the chunks to send for late tool input on a pending approval.

    The input chunk (with the part's normalized input), then the approval
    request again, so a client that applied the input still sees the request.
    """
    if chunk.get("type") != "tool-input-available":
        return []
    part = _find_tool_part(parts, chunk.get("toolCallId"))
    approval = part.get("approval") if part is not None else None
    if (
        part is None
        or part.get("state") != "approval-requested"
        or not isinstance(approval, dict)
        or not approval.get("id")
    ):
        return []
    request = approval_request
    if request is None:
        request = {
            "type": "tool-approval-request",
            "approvalId": approval["id"],
            "toolCallId": chunk.get("toolCallId"),
        }
        if "descriptor" in approval:
            request["approvalDescriptor"] = approval["descriptor"]
    return [{**chunk, "input": part.get("input")}, request]


def apply_chunk_to_parts(
    parts: list[Part], chunk: Chunk, scratch: BuilderScratch | None = None
) -> bool:
    """Apply one wire chunk to ``parts``; return whether the chunk type was handled.

    ``start``, ``finish``, ``message-metadata``, ``finish-step``, and
    ``error`` aren't parts and return ``False`` (the accumulator handles
    them).
    """
    scratch = scratch if scratch is not None else BuilderScratch()
    match chunk.get("type"):
        case "text-start":
            parts.append({"type": "text", "text": "", "state": "streaming"})
        case "text-delta":
            last = _find_last(parts, "text")
            if last is not None:
                last["text"] += _or(chunk.get("delta"), "")
            else:
                parts.append(
                    {
                        "type": "text",
                        "text": _or(chunk.get("delta"), ""),
                        "state": "streaming",
                    }
                )
        case "text-end":
            last = _find_last(parts, "text")
            if last is not None and "state" in last:
                last["state"] = "done"
        case "reasoning-start":
            parts.append({"type": "reasoning", "text": "", "state": "streaming"})
        case "reasoning-delta":
            last = _find_last(parts, "reasoning")
            if last is not None:
                last["text"] += _or(chunk.get("delta"), "")
                _merge_provider_metadata(last, chunk.get("providerMetadata"))
            else:
                part: Part = {
                    "type": "reasoning",
                    "text": _or(chunk.get("delta"), ""),
                    "state": "streaming",
                }
                if chunk.get("providerMetadata") is not None:
                    part["providerMetadata"] = chunk["providerMetadata"]
                parts.append(part)
        case "reasoning-end":
            last = _find_last(parts, "reasoning")
            if last is not None and "state" in last:
                last["state"] = "done"
                _merge_provider_metadata(last, chunk.get("providerMetadata"))
        case "file":
            parts.append(_pick({"type": "file"}, chunk, "mediaType", "url"))
        case "source-url":
            parts.append(
                _pick(
                    {"type": "source-url"},
                    chunk,
                    "sourceId",
                    "url",
                    "title",
                    "providerMetadata",
                )
            )
        case "source-document":
            parts.append(
                _pick(
                    {"type": "source-document"},
                    chunk,
                    "sourceId",
                    "mediaType",
                    "title",
                    "filename",
                    "providerMetadata",
                )
            )
        case "tool-input-start":
            if _find_tool_part(parts, chunk.get("toolCallId")) is None:
                part = _new_tool_part(chunk, "input-streaming")
                _copy_call_fields(part, chunk)
                parts.append(part)
        case "tool-input-delta":
            part = _find_tool_part(parts, chunk.get("toolCallId"))
            if part is not None and part.get("state") == "input-streaming":
                delta = chunk.get("inputTextDelta")
                if isinstance(delta, str):
                    scratch.add_raw_input(part, delta)
                if "input" in chunk:
                    part["input"] = chunk["input"]
                    scratch.mark_provisional(part, True)
        case "tool-input-available":
            existing = _find_tool_part(parts, chunk.get("toolCallId"))
            if existing is None:
                part = _new_tool_part(chunk, "input-available")
                part["input"] = normalize_tool_input(chunk.get("input"))[0]
                _copy_call_fields(part, chunk)
                parts.append(part)
            elif existing.get("state") == "input-streaming":
                existing["state"] = "input-available"
                existing["input"] = normalize_tool_input(chunk.get("input"))[0]
                _copy_call_fields(existing, chunk)
                scratch.drop_raw_input(existing)
                scratch.mark_provisional(existing, False)
            else:
                apply_late_tool_input(parts, chunk, scratch)
        case "tool-input-error":
            existing = _find_tool_part(parts, chunk.get("toolCallId"))
            if existing is None:
                part = _new_tool_part(chunk, "output-error")
                part["input"] = normalize_tool_input(chunk.get("input"))[0]
                _set(part, "errorText", chunk, "errorText")
                _copy_call_fields(part, chunk, title=False)
                parts.append(part)
            elif existing.get("state") not in _SETTLED:
                existing["state"] = "output-error"
                _set(existing, "errorText", chunk, "errorText")
                existing["input"] = normalize_tool_input(chunk.get("input"))[0]
                _copy_call_fields(existing, chunk, title=False)
        case "tool-approval-request":
            part = _find_tool_part(parts, chunk.get("toolCallId"))
            if part is not None and part.get("state") not in (
                _SETTLED | {"approval-responded"}
            ):
                raw = scratch.raw_input(part)
                if "input" not in part and raw is not None:
                    part["input"] = normalize_tool_input(raw)[0]
                    scratch.mark_provisional(part, True)
                scratch.drop_raw_input(part)
                part["state"] = "approval-requested"
                approval: Part = {}
                _set(approval, "id", chunk, "approvalId")
                if "approvalDescriptor" in chunk:
                    approval["descriptor"] = chunk["approvalDescriptor"]
                part["approval"] = approval
        case "tool-output-denied":
            part = _find_tool_part(parts, chunk.get("toolCallId"))
            if part is not None and part.get("state") not in (
                _SETTLED | {"approval-responded"}
            ):
                part["state"] = "output-denied"
        case "tool-output-available":
            part = _find_tool_part(parts, chunk.get("toolCallId"))
            if part is not None:
                part["state"] = "output-available"
                _set(part, "output", chunk, "output")
                if "preliminary" in chunk:
                    part["preliminary"] = chunk["preliminary"]
        case "tool-output-error":
            part = _find_tool_part(parts, chunk.get("toolCallId"))
            if part is not None:
                part["state"] = "output-error"
                _set(part, "errorText", chunk, "errorText")
        case "step-start" | "start-step":
            parts.append({"type": "step-start"})
        case str(kind) if kind.startswith("data-"):
            if chunk.get("transient"):
                return True
            if chunk.get("id") is not None:
                existing = _find_data_part(parts, kind, chunk["id"])
                if existing is not None:
                    _set(existing, "data", chunk, "data")
                    return True
            part = {"type": kind}
            if chunk.get("id") is not None:
                part["id"] = chunk["id"]
            _set(part, "data", chunk, "data")
            parts.append(part)
        case _:
            return False
    return True


def is_replay_chunk(parts: list[Part], chunk: Chunk) -> bool:
    """Return whether ``chunk`` would move a tool part backwards (a resend)."""
    kind = chunk.get("type")
    tool_call_id = chunk.get("toolCallId")
    if kind in ("tool-output-denied", "tool-approval-request"):
        if not tool_call_id:
            return False
        existing = _find_tool_part(parts, tool_call_id)
        return existing is not None and existing.get("state") in (
            _SETTLED | {"approval-responded"}
        )
    if kind not in ("tool-input-start", "tool-input-delta", "tool-input-available"):
        return False
    if not tool_call_id:
        return False
    existing = _find_tool_part(parts, tool_call_id)
    if existing is None:
        return False
    if kind == "tool-input-start":
        return True
    return existing.get("state") != "input-streaming"


def partial_stream_text(bodies: Iterable[str]) -> tuple[str, list[Part]]:
    """Rebuild parts from stored chunk bodies; return their text and the parts.

    Bodies that aren't JSON are skipped.
    """
    parts: list[Part] = []
    scratch = BuilderScratch()
    for body in bodies:
        try:
            chunk = json.loads(body)
        except ValueError:
            continue
        if isinstance(chunk, dict):
            apply_chunk_to_parts(parts, chunk, scratch)
    text = "".join(
        part["text"]
        for part in parts
        if part.get("type") == "text" and isinstance(part.get("text"), str)
    )
    return text, parts


# Helpers


def _or(value: Any, default: Any) -> Any:
    # JavaScript's `value ?? default`.
    return default if value is None else value


def _set(target: Part, key: str, chunk: Chunk, source: str) -> None:
    # Copy a field the chunk has (an absent one stays absent, like undefined).
    if source in chunk:
        target[key] = chunk[source]


def _pick(target: Part, chunk: Chunk, *keys: str) -> Part:
    for key in keys:
        _set(target, key, chunk, key)
    return target


def _new_tool_part(chunk: Chunk, state: str) -> Part:
    part: Part = {"type": f"tool-{chunk.get('toolName')}"}
    _set(part, "toolCallId", chunk, "toolCallId")
    _set(part, "toolName", chunk, "toolName")
    part["state"] = state
    return part


def _copy_call_fields(part: Part, chunk: Chunk, *, title: bool = True) -> None:
    # The call's metadata, when the chunk carries it (`!= null` upstream).
    if chunk.get("providerExecuted") is not None:
        part["providerExecuted"] = chunk["providerExecuted"]
    if chunk.get("providerMetadata") is not None:
        part["callProviderMetadata"] = chunk["providerMetadata"]
    if title and chunk.get("title") is not None:
        part["title"] = chunk["title"]


def _merge_provider_metadata(part: Part, metadata: Any) -> None:
    if metadata is None:
        return
    current = part.get("providerMetadata")
    part["providerMetadata"] = {
        **(current if isinstance(current, dict) else {}),
        **metadata,
    }


def _find_last(parts: list[Part], kind: str) -> Part | None:
    for part in reversed(parts):
        if part.get("type") == kind:
            return part
    return None


def _find_tool_part(parts: list[Part], tool_call_id: Any) -> Part | None:
    if not tool_call_id:
        return None
    for part in reversed(parts):
        if "toolCallId" in part and part["toolCallId"] == tool_call_id:
            return part
    return None


def _find_data_part(parts: list[Part], kind: str, id: Any) -> Part | None:
    for part in reversed(parts):
        if part.get("type") == kind and "id" in part and part["id"] == id:
            return part
    return None
