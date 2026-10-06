"""The chat port against upstream's own code: same inputs, same JSON out.

oracle.json comes from running upstream's TypeScript modules over cases.json
(see oracle/make_cases.py and oracle/generate.mjs).
"""

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from agents.chat.accumulator import ChunkResult, StreamAccumulator
from agents.chat.builder import BuilderScratch, apply_chunk_to_parts, is_replay_chunk
from agents.chat.reconciler import (
    reconcile_messages,
    reconcile_orphan_partial,
    resolve_tool_merge_id,
)
from agents.chat.repair import repair_interrupted_tool_parts
from agents.chat.tool_state import (
    apply_tool_update,
    cross_message_tool_result_update,
    has_incomplete_tool_batch,
    part_awaits_client_interaction,
    paused_execution_update,
    tool_approval_update,
    tool_result_update,
)

HERE = Path(__file__).parent / "oracle"
CASES = json.loads((HERE / "cases.json").read_text())
ORACLE = json.loads((HERE / "oracle.json").read_text())


def plain(value: Any) -> Any:
    return json.loads(json.dumps(value))


def ids(section: str) -> list[str]:
    return [case["name"] for case in CASES[section]]


def case(section: str, name: str) -> dict[str, Any]:
    return copy.deepcopy(next(c for c in CASES[section] if c["name"] == name))


@pytest.mark.parametrize("name", ids("builder"))
def test_builder(name: str) -> None:
    c = case("builder", name)
    parts = c["parts"]
    scratch = BuilderScratch()
    replay, handled = [], []
    for chunk in c["chunks"]:
        replay.append(is_replay_chunk(parts, chunk))
        handled.append(apply_chunk_to_parts(parts, chunk, scratch))
    assert (
        plain({"parts": parts, "replay": replay, "handled": handled})
        == ORACLE["builder"][name]
    )


ACTION_FIELDS = {
    "message_id": "messageId",
    "metadata": "metadata",
    "finish_reason": "finishReason",
    "tool_call_id": "toolCallId",
    "update_type": "updateType",
    "output": "output",
    "error_text": "errorText",
    "preliminary": "preliminary",
    "error": "error",
}


def result_json(result: ChunkResult) -> dict[str, Any]:
    out: dict[str, Any] = {"handled": result.handled}
    if result.action is not None:
        action = {"type": result.action.type}
        for field, key in ACTION_FIELDS.items():
            value = getattr(result.action, field)
            if value is not None:
                action[key] = value
        out["action"] = action
    return out


@pytest.mark.parametrize("name", ids("accumulator"))
def test_accumulator(name: str) -> None:
    c = case("accumulator", name)
    options = c["options"]
    acc = StreamAccumulator(
        message_id=options["messageId"],
        continuation=options.get("continuation", False),
        existing_parts=options.get("existingParts"),
        existing_metadata=options.get("existingMetadata"),
    )
    results = [result_json(acc.apply_chunk(chunk)) for chunk in c["chunks"]]
    out: dict[str, Any] = {"results": results, "message": acc.to_message()}
    if "mergeInto" in c:
        out["merged"] = acc.merge_into(c["mergeInto"])
    assert plain(out) == ORACLE["accumulator"][name]


@pytest.mark.parametrize("name", ids("reconciler"))
def test_reconciler(name: str) -> None:
    c = case("reconciler", name)
    orphan = c.get("orphan")
    out = {
        "reconciled": reconcile_messages(copy.deepcopy(c["incoming"]), c["server"]),
        "toolMergeIds": [
            resolve_tool_merge_id(m, c["server"])["id"] for m in c["incoming"]
        ],
        "orphan": reconcile_orphan_partial(orphan["existing"], orphan["incoming"])
        if orphan
        else None,
    }
    assert plain(out) == ORACLE["reconciler"][name]


UPDATES: dict[str, Any] = {
    "result": lambda u: tool_result_update(
        u["toolCallId"], u.get("output"), u.get("overrideState"), u.get("errorText")
    ),
    "crossMessage": lambda u: cross_message_tool_result_update(
        u["toolCallId"],
        u["updateType"],
        u.get("output"),
        u.get("errorText"),
        u.get("preliminary"),
    ),
    "approval": lambda u: tool_approval_update(u["toolCallId"], u["approved"]),
    "paused": lambda u: paused_execution_update(
        u["toolCallId"], u["executionId"], u["output"]
    ),
}


@pytest.mark.parametrize("name", ids("toolState"))
def test_tool_state(name: str) -> None:
    c = case("toolState", name)
    update = UPDATES[c["update"]["kind"]](c["update"])
    applied = apply_tool_update(copy.deepcopy(c["parts"]), update)
    out = {
        "applied": {"parts": applied[0], "index": applied[1]} if applied else None,
        "awaits": [
            part_awaits_client_interaction(p, set(c.get("clientTools", [])))
            for p in c["parts"]
        ],
        "incompleteBatch": has_incomplete_tool_batch(
            [{"role": "assistant", "parts": c["parts"]}]
        ),
    }
    assert plain(out) == ORACLE["toolState"][name]


@pytest.mark.parametrize("name", ids("repair"))
def test_repair(name: str) -> None:
    c = case("repair", name)
    result = repair_interrupted_tool_parts(
        c["messages"],
        repair_part=lambda part: {
            **part,
            "state": "output-error",
            "errorText": "interrupted",
        },
        repair_approval_responded=c.get("repairApprovalResponded", False),
    )
    out = {
        "messages": result.messages,
        "removedToolCalls": result.removed_tool_calls,
        "normalizedInputs": result.normalized_inputs,
        "toolCallIds": result.tool_call_ids,
    }
    assert plain(out) == ORACLE["repair"][name]
