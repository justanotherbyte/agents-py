"""Aligning client-sent messages with stored ones.

Port of upstream ``chat/message-reconciler.ts``.

A client submits its whole transcript, and its copies of assistant messages
can be stale: built under a temporary id, or still showing a tool call the
server has since settled. Before saving, three passes run in order:

1. give an incoming assistant message its stored id (exact id, then the same
   tool calls, then the same content);
2. merge server-known tool outputs into incoming copies still pending;
3. drop stale duplicate assistant copies echoed in the same submit.

Works on wire-form dictionaries; inputs aren't mutated.
"""

from collections.abc import Callable, Sequence
from typing import Any

from ._json import dumps, stable_dumps

__all__ = (
    "assistant_content_key",
    "reconcile_messages",
    "reconcile_orphan_partial",
    "resolve_tool_merge_id",
)

type Message = dict[str, Any]
type Part = dict[str, Any]
type Sanitize = Callable[[Message], Message]

_RESOLVED = ("output-available", "output-error", "output-denied")
_PENDING = ("input-available", "approval-requested", "approval-responded")


def reconcile_messages(
    incoming: Sequence[Message],
    server_messages: Sequence[Message],
    sanitize_for_content_key: Sanitize | None = None,
) -> list[Message]:
    """Return ``incoming`` aligned with ``server_messages`` (the three passes)."""
    with_ids = _reconcile_assistant_ids(
        incoming, server_messages, sanitize_for_content_key
    )
    merged = _merge_server_tool_outputs(with_ids, server_messages)
    return _drop_stale_tool_copies(merged, server_messages, sanitize_for_content_key)


def resolve_tool_merge_id(
    message: Message, server_messages: Sequence[Message]
) -> Message:
    """Give an assistant message the id of the stored message sharing a tool call."""
    if message.get("role") != "assistant":
        return message
    for part in message["parts"]:
        tool_call_id = part.get("toolCallId")
        if tool_call_id:
            existing = _find_by_tool_call(server_messages, tool_call_id)
            if existing is not None and existing["id"] != message["id"]:
                return {**message, "id": existing["id"]}
    return message


def reconcile_orphan_partial(existing: Message, incoming: Message) -> Message:
    """Merge a rebuilt partial into the stored message, keeping existing tool calls."""
    existing_ids = {p["toolCallId"] for p in existing["parts"] if "toolCallId" in p}
    new_parts = [
        p
        for p in incoming["parts"]
        if not ("toolCallId" in p and p["toolCallId"] in existing_ids)
    ]
    merged: Message = {**incoming, "parts": [*existing["parts"], *new_parts]}
    if existing.get("metadata"):
        merged["metadata"] = (
            {**existing["metadata"], **incoming["metadata"]}
            if incoming.get("metadata")
            else existing["metadata"]
        )
    return merged


def assistant_content_key(
    message: Message, sanitize: Sanitize | None = None
) -> str | None:
    """Return the JSON of an assistant message's parts (``None`` for others)."""
    if message.get("role") != "assistant":
        return None
    sanitized = sanitize(message) if sanitize is not None else message
    return dumps(sanitized["parts"])


def _drop_stale_tool_copies(
    reconciled: list[Message],
    server_messages: Sequence[Message],
    sanitize: Sanitize | None,
) -> list[Message]:
    reconciled_ids = {m["id"] for m in reconciled}
    server_ids: set[str] = set()
    settled_on_claimed: dict[str, list[Part]] = {}
    for message in server_messages:
        server_ids.add(message["id"])
        if message.get("role") != "assistant" or message["id"] not in reconciled_ids:
            continue
        for part in message["parts"]:
            if _is_resolved(part):
                settled_on_claimed.setdefault(part["toolCallId"], []).append(part)
    if not settled_on_claimed:
        return reconciled

    last = len(reconciled) - 1

    def keep(index: int, message: Message) -> bool:
        if index == last:
            return True
        if message.get("role") != "assistant" or message["id"] in server_ids:
            return True
        comparable = sanitize(message) if sanitize is not None else message
        has_tool_part = False
        for part in comparable["parts"]:
            if part.get("type") == "step-start":
                continue
            tool_call_id = part.get("toolCallId")
            if not isinstance(tool_call_id, str) or not (
                _is_pending(part) or part.get("state") == "input-streaming"
            ):
                return True
            settled = settled_on_claimed.get(tool_call_id)
            if not settled or not any(_same_tool_call(c, part) for c in settled):
                return True
            has_tool_part = True
        return not has_tool_part

    kept = [m for i, m in enumerate(reconciled) if keep(i, m)]
    return reconciled if len(kept) == len(reconciled) else kept


def _merge_server_tool_outputs(
    incoming: list[Message], server_messages: Sequence[Message]
) -> list[Message]:
    resolved_by_message: dict[str, dict[str, Part]] = {}
    for message in server_messages:
        if message.get("role") != "assistant":
            continue
        resolved = {p["toolCallId"]: p for p in message["parts"] if _is_resolved(p)}
        if resolved:
            resolved_by_message[message["id"]] = resolved
    if not resolved_by_message:
        return incoming

    def merge(message: Message) -> Message:
        if message.get("role") != "assistant":
            return message
        own = resolved_by_message.get(message["id"])
        if own is None:
            return message
        changed = False
        parts = []
        for part in message["parts"]:
            server = own.get(part["toolCallId"]) if _is_pending(part) else None
            if server is None:
                parts.append(part)
                continue
            changed = True
            merged = {**part, "state": server["state"]}
            if server["state"] == "output-available" and "output" in server:
                merged["output"] = server["output"]
            elif server["state"] == "output-error" and "errorText" in server:
                merged["errorText"] = server["errorText"]
            elif server["state"] == "output-denied" and "approval" in server:
                merged["approval"] = server["approval"]
            parts.append(merged)
        return {**message, "parts": parts} if changed else message

    return [merge(m) for m in incoming]


def _reconcile_assistant_ids(
    incoming: Sequence[Message],
    server_messages: Sequence[Message],
    sanitize: Sanitize | None,
) -> list[Message]:
    if not server_messages:
        return list(incoming)
    claimed: set[int] = set()
    exact: set[int] = set()
    for i, message in enumerate(incoming):
        for j, server in enumerate(server_messages):
            if j not in claimed and server["id"] == message["id"]:
                claimed.add(j)
                exact.add(i)
                break

    def reconcile(i: int, message: Message) -> Message:
        if i in exact or message.get("role") != "assistant":
            return message
        tool_parts = _tool_parts_by_call(
            sanitize(message) if sanitize is not None else message
        )
        if tool_parts:
            for j, server in enumerate(server_messages):
                if j in claimed:
                    continue
                if server.get("role") == "assistant" and _carries_same_tool_calls(
                    server, tool_parts
                ):
                    claimed.add(j)
                    return {**message, "id": server["id"]}
            return message
        key = assistant_content_key(message, sanitize)
        if key is None:
            return message
        for j, server in enumerate(server_messages):
            if j in claimed:
                continue
            if server.get("role") != "assistant" or _has_tool_call(server):
                continue
            if assistant_content_key(server, sanitize) == key:
                claimed.add(j)
                return {**message, "id": server["id"]}
        return message

    return [reconcile(i, m) for i, m in enumerate(incoming)]


def _has_tool_call(message: Message) -> bool:
    return any("toolCallId" in part for part in message["parts"])


def _is_resolved(part: Part) -> bool:
    return "toolCallId" in part and part.get("state") in _RESOLVED


def _is_pending(part: Part) -> bool:
    return "toolCallId" in part and part.get("state") in _PENDING


def _carries_same_tool_calls(server: Message, incoming_parts: dict[str, Part]) -> bool:
    shared = False
    for part in server["parts"]:
        tool_call_id = part.get("toolCallId")
        if not isinstance(tool_call_id, str):
            continue
        incoming = incoming_parts.get(tool_call_id)
        if incoming is None:
            continue
        if not _same_tool_call(part, incoming):
            return False
        shared = True
    return shared


def _same_tool_call(a: Part, b: Part) -> bool:
    return (
        a.get("type") == b.get("type")
        and (
            a.get("toolName") is None
            or b.get("toolName") is None
            or a.get("toolName") == b.get("toolName")
        )
        and _input_key(a) == _input_key(b)
    )


def _input_key(part: Part) -> str | None:
    # An absent input (JavaScript's undefined) differs from null.
    return stable_dumps(part["input"]) if "input" in part else None


def _tool_parts_by_call(message: Message) -> dict[str, Part]:
    parts: dict[str, Part] = {}
    for part in message["parts"]:
        tool_call_id = part.get("toolCallId")
        if isinstance(tool_call_id, str) and tool_call_id not in parts:
            parts[tool_call_id] = part
    return parts


def _find_by_tool_call(
    messages: Sequence[Message], tool_call_id: str
) -> Message | None:
    for message in messages:
        if message.get("role") != "assistant":
            continue
        if any(p.get("toolCallId") == tool_call_id for p in message["parts"]):
            return message
    return None
