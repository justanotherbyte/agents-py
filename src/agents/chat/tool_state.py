"""Applying tool results and approvals to tool parts (upstream ``chat/tool-state.ts``).

Each update names a tool call, the states it may apply in, and how to change
the part. Works on wire-form dictionaries and returns new ones (the input
isn't mutated).
"""

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

__all__ = (
    "ToolPartUpdate",
    "apply_tool_update",
    "client_resolvable_tool_names",
    "cross_message_tool_result_update",
    "has_incomplete_tool_batch",
    "part_awaits_client_interaction",
    "paused_execution_update",
    "tool_approval_update",
    "tool_part_name",
    "tool_result_update",
)

type Part = dict[str, Any]

_SETTLED = ("output-available", "output-error", "output-denied")
_ALL_STATES = (
    "input-streaming",
    "input-available",
    "approval-requested",
    "approval-responded",
    *_SETTLED,
)


@dataclass(slots=True, kw_only=True, frozen=True)
class ToolPartUpdate:
    """A change to the tool part for ``tool_call_id``, if it's in ``match_states``."""

    tool_call_id: str
    match_states: tuple[str, ...]
    apply: Callable[[Part], Part]


def apply_tool_update(
    parts: Sequence[Part], update: ToolPartUpdate
) -> tuple[list[Part], int] | None:
    """Apply ``update`` to the first matching part.

    Returns the new parts and the updated part's index, or ``None``.
    """
    for index, part in enumerate(parts):
        if (
            part.get("toolCallId") == update.tool_call_id
            and "toolCallId" in part
            and "state" in part
            and part["state"] in update.match_states
        ):
            updated = list(parts)
            updated[index] = update.apply(part)
            return updated, index
    return None


def tool_result_update(
    tool_call_id: str,
    output: Any,
    override_state: Literal["output-error"] | None = None,
    error_text: str | None = None,
) -> ToolPartUpdate:
    """Return the update for a client tool's result (or its error)."""

    def apply(part: Part) -> Part:
        if override_state == "output-error":
            return {
                **part,
                "state": "output-error",
                "errorText": error_text
                if error_text is not None
                else "Tool execution denied by user",
            }
        return {
            **part,
            "state": "output-available",
            "output": output,
            "preliminary": False,
        }

    return ToolPartUpdate(
        tool_call_id=tool_call_id,
        match_states=("input-available", "approval-requested", "approval-responded"),
        apply=apply,
    )


def cross_message_tool_result_update(
    tool_call_id: str,
    update_type: Literal["output-available", "output-error"],
    output: Any = None,
    error_text: str | None = None,
    preliminary: bool | None = None,
) -> ToolPartUpdate:
    """Return the update for a result to a call in an earlier message.

    Settled parts stay as they are.
    """

    def apply(part: Part) -> Part:
        if part.get("state") in _SETTLED:
            return part
        if update_type == "output-error":
            return {
                **part,
                "state": "output-error",
                "errorText": error_text
                if error_text is not None
                else "Tool execution failed",
            }
        return {
            **part,
            "state": "output-available",
            "output": output,
            "preliminary": preliminary if preliminary is not None else False,
        }

    return ToolPartUpdate(
        tool_call_id=tool_call_id, match_states=_ALL_STATES, apply=apply
    )


def paused_execution_update(
    tool_call_id: str, execution_id: str, output: Any
) -> ToolPartUpdate:
    """Replace a paused execution's placeholder output with its final output."""

    def apply(part: Part) -> Part:
        current = part.get("output")
        if (
            not isinstance(current, dict)
            or current.get("status") != "paused"
            or current.get("executionId") != execution_id
        ):
            return part
        return {**part, "output": output, "preliminary": False}

    return ToolPartUpdate(
        tool_call_id=tool_call_id, match_states=("output-available",), apply=apply
    )


def tool_approval_update(tool_call_id: str, approved: bool) -> ToolPartUpdate:
    """Return the update for the user's answer to an approval request."""

    def apply(part: Part) -> Part:
        approval = part.get("approval")
        approval = approval if isinstance(approval, dict) else None
        approval_id = (
            approval["id"]
            if approval is not None and isinstance(approval.get("id"), str)
            else tool_call_id
        )
        return {
            **part,
            "state": "approval-responded" if approved else "output-denied",
            "approval": {**(approval or {}), "id": approval_id, "approved": approved},
        }

    return ToolPartUpdate(
        tool_call_id=tool_call_id,
        match_states=("input-available", "approval-requested"),
        apply=apply,
    )


def tool_part_name(part: Part) -> str | None:
    """Return a tool part's tool name (``None`` for other parts)."""
    kind = part.get("type")
    if not isinstance(kind, str):
        return None
    if kind == "dynamic-tool":
        name = part.get("toolName")
        return name if isinstance(name, str) else None
    if kind.startswith("tool-"):
        return kind[len("tool-") :]
    return None


def part_awaits_client_interaction(
    part: Any, client_resolvable: frozenset[str] | set[str]
) -> bool:
    """Return whether ``part`` waits on the client.

    That is an approval request, or a client tool's call awaiting its result.
    """
    if not isinstance(part, dict) or "state" not in part:
        return False
    state = part["state"]
    if state == "approval-requested":
        return True
    if state != "input-available":
        return False
    name = tool_part_name(part)
    return name is not None and name in client_resolvable


def client_resolvable_tool_names(tools: Iterable[Any] | None) -> frozenset[str]:
    """Return the names of the client's tools (dicts or objects with ``name``)."""
    names = set()
    for tool in tools or ():
        name = (
            tool.get("name") if isinstance(tool, dict) else getattr(tool, "name", None)
        )
        if name:
            names.add(name)
    return frozenset(names)


def has_incomplete_tool_batch(messages: Sequence[dict[str, Any]]) -> bool:
    """Return whether the last assistant message has pending and settled tool calls."""
    leaf = next((m for m in reversed(messages) if m.get("role") == "assistant"), None)
    if leaf is None:
        return False
    pending = settled = False
    for part in leaf.get("parts", ()):
        state = part.get("state")
        kind = part.get("type")
        if state in ("input-available", "approval-requested"):
            pending = True
        elif (
            isinstance(kind, str)
            and (kind.startswith("tool-") or kind == "dynamic-tool")
            and state in (*_SETTLED, "approval-responded")
        ):
            settled = True
        if pending and settled:
            return True
    return False
