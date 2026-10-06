"""Settling tool calls an interrupted stream left open.

Port of upstream ``chat/repair-transcript.ts``.

A stream cut off mid-tool-call leaves a ``tool-*`` part with no result, and
the next model call would fail on it. Before a recovered turn calls the model
again, each unsettled call is handed to ``repair_part`` (which settles it,
e.g. as an error), and malformed tool input is normalized. Works on
wire-form dictionaries; inputs aren't mutated.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from .builder import normalize_tool_input

__all__ = (
    "RepairResult",
    "repair_interrupted_tool_parts",
    "tool_part_has_settled_result",
)

type Message = dict[str, Any]
type Part = dict[str, Any]


@dataclass(slots=True, kw_only=True, frozen=True)
class RepairResult:
    """The repaired messages, and what changed."""

    messages: list[Message]
    removed_tool_calls: int
    normalized_inputs: int
    tool_call_ids: list[str]


def tool_part_has_settled_result(part: Part) -> bool:
    """Return whether a tool part has a result (an output, or a settled state)."""
    if "output" in part or "result" in part:
        return True
    return part.get("state") in ("output-available", "output-error", "output-denied")


def repair_interrupted_tool_parts(
    messages: Sequence[Message],
    *,
    repair_part: Callable[[Part], Part],
    is_settled: Callable[[Part], bool] = tool_part_has_settled_result,
    normalize_input: Callable[[Any], tuple[Any, bool]] = normalize_tool_input,
    should_repair: Callable[[Part], bool] | None = None,
    repair_approval_responded: bool = False,
) -> RepairResult:
    """Settle unsettled tool calls with ``repair_part``; normalize tool input.

    Parameters
    ----------
    repair_part
        Turns an unsettled tool part (input already normalized) into a
        settled one.
    is_settled
        Whether a tool part has its result.
    should_repair
        Leave a part as is when this returns ``False``.
    repair_approval_responded
        Also settle approved calls left ``approval-responded`` (except in the
        last message, whose turn may still run them); denied ones become
        ``output-denied`` either way when this is set.
    """
    removed = 0
    normalized_count = 0
    tool_call_ids: list[str] = []
    repaired: list[Message] = []
    for index, message in enumerate(messages):
        is_last = index == len(messages) - 1
        parts: list[Part] = []
        changed = False
        for part in message["parts"]:
            tool_call_id = part.get("toolCallId")
            kind = part.get("type")
            is_tool = (
                isinstance(kind, str)
                and (kind.startswith("tool-") or kind == "dynamic-tool")
                and isinstance(tool_call_id, str)
                and tool_call_id
            )
            if not is_tool:
                parts.append(part)
                continue
            assert isinstance(tool_call_id, str)
            if not is_settled(part):
                if part.get("state") == "approval-responded":
                    if not repair_approval_responded or is_last:
                        parts.append(part)
                        continue
                    approval = part.get("approval")
                    if isinstance(approval, dict) and approval.get("approved") is False:
                        value, was_changed = normalize_input(part.get("input"))
                        parts.append({**part, "input": value, "state": "output-denied"})
                        normalized_count += was_changed
                        removed += 1
                        changed = True
                        tool_call_ids.append(tool_call_id)
                        continue
                if should_repair is not None and not should_repair(part):
                    parts.append(part)
                    continue
                value, was_changed = normalize_input(part.get("input"))
                parts.append(repair_part({**part, "input": value}))
                normalized_count += was_changed
                removed += 1
                changed = True
                tool_call_ids.append(tool_call_id)
                continue
            value, was_changed = normalize_input(part.get("input"))
            if was_changed:
                parts.append({**part, "input": value})
                normalized_count += 1
                changed = True
                continue
            parts.append(part)
        repaired.append({**message, "parts": parts} if changed else message)
    return RepairResult(
        messages=repaired,
        removed_tool_calls=removed,
        normalized_inputs=normalized_count,
        tool_call_ids=tool_call_ids,
    )
