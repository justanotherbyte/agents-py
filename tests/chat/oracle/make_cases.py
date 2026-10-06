"""Write cases.json: inputs upstream's chat modules and the Python port both run.

    python tests/chat/oracle/make_cases.py
    node tests/chat/oracle/generate.mjs ../agents/packages/agents/src/chat

The cases follow upstream's own test scenarios (``agents/src/chat/__tests__``);
expected outputs come only from running upstream's code (oracle.json).
"""

import json
from pathlib import Path
from typing import Any


def text(t: str, id: str = "t") -> list[dict[str, Any]]:
    return [
        {"type": "text-start", "id": id},
        {"type": "text-delta", "id": id, "delta": t},
        {"type": "text-end", "id": id},
    ]


def tool(call: str = "c1", name: str = "search", **extra: Any) -> dict[str, Any]:
    return {"toolCallId": call, "toolName": name, **extra}


def part(
    state: str, call: str = "c1", name: str = "search", **extra: Any
) -> dict[str, Any]:
    return {
        "type": f"tool-{name}",
        "toolCallId": call,
        "toolName": name,
        "state": state,
        **extra,
    }


def msg(id: str, role: str, *parts: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"id": id, "role": role, "parts": list(parts), **extra}


BUILDER = {
    "text": text("Hello") + text(" again", "t2"),
    "text_delta_without_start": [{"type": "text-delta", "id": "t", "delta": "x"}],
    "text_end_without_part": [{"type": "text-end", "id": "t"}],
    "empty_delta": [
        {"type": "text-start", "id": "t"},
        {"type": "text-delta", "id": "t"},
    ],
    "reasoning_metadata": [
        {"type": "reasoning-start", "id": "r"},
        {
            "type": "reasoning-delta",
            "id": "r",
            "delta": "think",
            "providerMetadata": {"a": {"x": 1}},
        },
        {"type": "reasoning-end", "id": "r", "providerMetadata": {"b": {"sig": "s"}}},
    ],
    "reasoning_delta_without_start": [
        {
            "type": "reasoning-delta",
            "id": "r",
            "delta": "x",
            "providerMetadata": {"a": {}},
        }
    ],
    "reasoning_end_no_metadata": [
        {"type": "reasoning-start", "id": "r"},
        {"type": "reasoning-end", "id": "r"},
    ],
    "file_and_sources": [
        {"type": "file", "url": "data:image/png;base64,AA", "mediaType": "image/png"},
        {"type": "source-url", "sourceId": "s1", "url": "https://x.dev", "title": "X"},
        {"type": "source-url", "sourceId": "s2", "url": "https://y.dev"},
        {
            "type": "source-document",
            "sourceId": "d1",
            "mediaType": "application/pdf",
            "title": "Doc",
            "filename": "d.pdf",
        },
    ],
    "steps": [{"type": "start-step"}, {"type": "step-start"}, {"type": "finish-step"}],
    "data_parts": [
        {"type": "data-status", "id": "s", "data": {"n": 1}},
        {"type": "data-status", "id": "s", "data": {"n": 2}},
        {"type": "data-status", "data": {"n": 3}},
        {"type": "data-status", "data": {"n": 4}},
        {"type": "data-temp", "data": 1, "transient": True},
        {"type": "data-other", "id": "s", "data": "other"},
    ],
    "tool_lifecycle": [
        {"type": "tool-input-start", **tool(providerExecuted=True, title="Search")},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": '{"q":'},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": '"x"}'},
        {
            "type": "tool-input-available",
            **tool(input={"q": "x"}, providerMetadata={"p": {}}),
        },
        {
            "type": "tool-output-available",
            "toolCallId": "c1",
            "output": {"r": [1]},
            "preliminary": True,
        },
        {"type": "tool-output-available", "toolCallId": "c1", "output": {"r": [1, 2]}},
    ],
    "tool_streaming_to_available": [
        {"type": "tool-input-start", **tool(title="T")},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": '{"q":1}'},
        {
            "type": "tool-input-available",
            **tool(input={"q": 1}, providerExecuted=False),
        },
    ],
    "tool_streaming_only": [
        {"type": "tool-input-start", **tool()},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": '{"q":'},
    ],
    "tool_available_without_start": [
        {"type": "tool-input-available", **tool(input=None)},
        {"type": "tool-input-available", **tool("c2", input='{"a":1}')},
        {"type": "tool-input-available", **tool("c3", input=[1, 2])},
        {"type": "tool-input-available", **tool("c4", input="not json")},
    ],
    "tool_input_error": [
        {"type": "tool-input-start", **tool()},
        {"type": "tool-input-error", **tool(input="{bad", errorText="invalid input")},
        {"type": "tool-input-error", **tool("c2", input={}, errorText="fresh")},
    ],
    "tool_input_error_after_output": [
        {"type": "tool-input-available", **tool(input={})},
        {"type": "tool-output-available", "toolCallId": "c1", "output": 1},
        {"type": "tool-input-error", **tool(input={}, errorText="late")},
    ],
    "tool_output_error_and_denied": [
        {"type": "tool-input-available", **tool(input={})},
        {"type": "tool-output-error", "toolCallId": "c1", "errorText": "boom"},
        {"type": "tool-input-available", **tool("c2", input={})},
        {"type": "tool-output-denied", "toolCallId": "c2"},
        {"type": "tool-output-denied", "toolCallId": "c2"},
        {"type": "tool-output-available", "toolCallId": "missing", "output": 1},
    ],
    "replayed_tool_chunks": [
        {"type": "tool-input-start", **tool()},
        {"type": "tool-input-available", **tool(input={"q": 1})},
        {"type": "tool-input-start", **tool()},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": "x"},
        {"type": "tool-input-available", **tool(input={"q": 2})},
        {"type": "tool-output-available", "toolCallId": "c1", "output": 1},
        {"type": "tool-output-denied", "toolCallId": "c1"},
        {"type": "tool-approval-request", "approvalId": "a", "toolCallId": "c1"},
    ],
    "approval_normal_order": [
        {"type": "tool-input-start", **tool()},
        {"type": "tool-input-available", **tool(input={"q": 1})},
        {"type": "tool-approval-request", "approvalId": "a1", "toolCallId": "c1"},
    ],
    "approval_input_from_deltas": [
        {"type": "tool-input-start", **tool()},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": '{"q":'},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": "1}"},
        {
            "type": "tool-approval-request",
            "approvalId": "a1",
            "toolCallId": "c1",
            "approvalDescriptor": {"why": "x"},
        },
    ],
    "approval_late_input": [
        {"type": "tool-input-start", **tool()},
        {"type": "tool-approval-request", "approvalId": "a1", "toolCallId": "c1"},
        {"type": "tool-input-available", **tool(input={"q": 9}, title="Late title")},
    ],
    "approval_canonical_replaces_truncated_deltas": [
        {"type": "tool-input-start", **tool()},
        {"type": "tool-input-delta", "toolCallId": "c1", "inputTextDelta": '{"q":'},
        {"type": "tool-approval-request", "approvalId": "a1", "toolCallId": "c1"},
        {"type": "tool-input-available", **tool(input={"q": "full"})},
    ],
    "approval_complete_input_never_replaced": [
        {"type": "tool-input-available", **tool(input={"q": 1})},
        {"type": "tool-approval-request", "approvalId": "a1", "toolCallId": "c1"},
        {"type": "tool-input-available", **tool(input={"q": 2})},
    ],
    "older_emitter_parsed_delta_input": [
        {"type": "tool-input-start", **tool()},
        {
            "type": "tool-input-delta",
            "toolCallId": "c1",
            "inputTextDelta": "{",
            "input": {"q": "part"},
        },
        {"type": "tool-approval-request", "approvalId": "a1", "toolCallId": "c1"},
        {"type": "tool-input-available", **tool(input={"q": "whole"})},
    ],
    "approval_after_response_ignored": [
        {"type": "tool-input-available", **tool(input={})},
        {"type": "tool-approval-request", "approvalId": "a1", "toolCallId": "c1"},
    ],
    "unknown_chunks": [
        {"type": "start", "messageId": "m"},
        {"type": "finish"},
        {"type": "message-metadata", "messageMetadata": {}},
        {"type": "error", "errorText": "x"},
        {"type": "custom-v7", "kind": "x"},
    ],
}

# Parts that exist before the chunks apply.
BUILDER_PARTS = {
    "approval_after_response_ignored": [
        part("approval-responded", input={}, approval={"id": "a0", "approved": True})
    ],
}

ACCUMULATOR = [
    {
        "name": "start_finish_metadata",
        "options": {"messageId": "m1"},
        "chunks": [
            {"type": "start", "messageId": "provider-id", "messageMetadata": {"a": 1}},
            *text("hi"),
            {"type": "message-metadata", "messageMetadata": {"b": 2}},
            {"type": "finish-step"},
            {"type": "finish", "finishReason": "stop", "messageMetadata": {"c": 3}},
        ],
        "mergeInto": [msg("u1", "user", {"type": "text", "text": "q"})],
    },
    {
        "name": "start_without_fields",
        "options": {"messageId": "m1"},
        "chunks": [{"type": "start"}, {"type": "finish"}],
    },
    {
        "name": "continuation_keeps_id_and_merges",
        "options": {
            "messageId": "new",
            "continuation": True,
            "existingParts": [{"type": "text", "text": "a", "state": "done"}],
            "existingMetadata": {"x": 1},
        },
        "chunks": [{"type": "start", "messageId": "ignored"}, *text("b")],
        "mergeInto": [
            msg("u1", "user"),
            msg("a1", "assistant", {"type": "text", "text": "a", "state": "done"}),
        ],
    },
    {
        "name": "continuation_replays_pending_onto_last_assistant",
        "options": {"messageId": "temp", "continuation": True},
        "chunks": [
            *text("more"),
            {"type": "message-metadata", "messageMetadata": {"y": 2}},
        ],
        "mergeInto": [
            msg("u1", "user"),
            msg(
                "a1",
                "assistant",
                {"type": "text", "text": "first", "state": "done"},
                metadata={"x": 1},
            ),
            msg("u2", "user"),
        ],
    },
    {
        "name": "continuation_appends_without_assistant",
        "options": {"messageId": "temp", "continuation": True},
        "chunks": text("x"),
        "mergeInto": [msg("u1", "user")],
    },
    {
        "name": "replace_by_id",
        "options": {"messageId": "a1"},
        "chunks": text("new"),
        "mergeInto": [
            msg("a1", "assistant", {"type": "text", "text": "old"}),
            msg("u2", "user"),
        ],
    },
    {
        "name": "tool_actions",
        "options": {"messageId": "m1"},
        "chunks": [
            {"type": "tool-input-available", **tool(input={})},
            {"type": "tool-approval-request", "approvalId": "a", "toolCallId": "c1"},
            {
                "type": "tool-approval-request",
                "approvalId": "b",
                "toolCallId": "nowhere",
            },
            {
                "type": "tool-output-available",
                "toolCallId": "earlier",
                "output": 5,
                "preliminary": True,
            },
            {"type": "tool-output-error", "toolCallId": "earlier2", "errorText": "bad"},
            {"type": "tool-output-available", "toolCallId": "c1", "output": 1},
        ],
    },
    {
        "name": "errors",
        "options": {"messageId": "m1"},
        "chunks": [{"type": "error", "errorText": "boom"}, {"type": "error"}],
    },
]

STALE_PENDING = part("input-available", input={"q": 1})
SETTLED = part("output-available", input={"q": 1}, output="r")

RECONCILER = [
    {
        "name": "merges_server_output_into_pending",
        "incoming": [msg("u1", "user"), msg("a1", "assistant", STALE_PENDING)],
        "server": [msg("u1", "user"), msg("a1", "assistant", SETTLED)],
    },
    {
        "name": "merges_error_and_denial",
        "incoming": [
            msg(
                "a1",
                "assistant",
                part("approval-requested", "c1", input={}),
                part("input-available", "c2", input={}),
                part("output-error", "c3", input={}, errorText="client"),
            )
        ],
        "server": [
            msg(
                "a1",
                "assistant",
                part("output-error", "c1", input={}, errorText="e"),
                part(
                    "output-denied",
                    "c2",
                    input={},
                    approval={"id": "x", "approved": False},
                ),
                part("output-available", "c3", input={}, output=1),
            )
        ],
    },
    {
        "name": "adopts_server_id_by_content",
        "incoming": [
            msg("u1", "user"),
            msg("temp", "assistant", {"type": "text", "text": "hi"}),
        ],
        "server": [
            msg("u1", "user"),
            msg("a1", "assistant", {"type": "text", "text": "hi"}),
        ],
    },
    {
        "name": "identical_contents_map_one_to_one",
        "incoming": [
            msg("t1", "assistant", {"type": "text", "text": "same"}),
            msg("t2", "assistant", {"type": "text", "text": "same"}),
        ],
        "server": [
            msg("a1", "assistant", {"type": "text", "text": "same"}),
            msg("a2", "assistant", {"type": "text", "text": "same"}),
        ],
    },
    {
        "name": "adopts_server_id_by_tool_call",
        "incoming": [msg("temp", "assistant", STALE_PENDING)],
        "server": [msg("a1", "assistant", SETTLED)],
    },
    {
        "name": "reordered_input_keys_are_the_same_call",
        "incoming": [
            msg("temp", "assistant", part("input-available", input={"b": 2, "a": 1}))
        ],
        "server": [
            msg(
                "a1",
                "assistant",
                part("output-available", input={"a": 1, "b": 2}, output=0),
            )
        ],
    },
    {
        "name": "different_tool_same_call_id_not_claimed",
        "incoming": [
            msg(
                "temp",
                "assistant",
                part("input-available", name="other", input={"q": 1}),
            )
        ],
        "server": [msg("a1", "assistant", SETTLED)],
    },
    {
        "name": "drops_stale_echoed_copy",
        "incoming": [
            msg("u1", "user"),
            msg("copy", "assistant", STALE_PENDING),
            msg("a1", "assistant", SETTLED),
            msg("u2", "user"),
        ],
        "server": [msg("u1", "user"), msg("a1", "assistant", SETTLED)],
    },
    {
        "name": "keeps_copy_with_other_content",
        "incoming": [
            msg("copy", "assistant", STALE_PENDING, {"type": "text", "text": "extra"}),
            msg("a1", "assistant", SETTLED),
            msg("u2", "user"),
        ],
        "server": [msg("a1", "assistant", SETTLED)],
    },
    {
        "name": "no_server_messages",
        "incoming": [msg("u1", "user"), msg("a1", "assistant", STALE_PENDING)],
        "server": [],
        "orphan": {
            "existing": msg("a1", "assistant", SETTLED, metadata={"a": 1, "b": 1}),
            "incoming": msg(
                "a1",
                "assistant",
                STALE_PENDING,
                part("input-available", "c2", input={}),
                metadata={"b": 2},
            ),
        },
    },
    {
        "name": "orphan_without_incoming_metadata",
        "incoming": [msg("u1", "user")],
        "server": [msg("u1", "user")],
        "orphan": {
            "existing": msg(
                "a1", "assistant", {"type": "text", "text": "a"}, metadata={"a": 1}
            ),
            "incoming": msg("a1", "assistant", {"type": "text", "text": "b"}),
        },
    },
]

TOOL_STATE = [
    {
        "name": "result",
        "parts": [part("input-available", input={})],
        "update": {"kind": "result", "toolCallId": "c1", "output": {"ok": True}},
        "clientTools": ["search"],
    },
    {
        "name": "result_error_default",
        "parts": [part("approval-requested", input={}, approval={"id": "a"})],
        "update": {
            "kind": "result",
            "toolCallId": "c1",
            "output": None,
            "overrideState": "output-error",
        },
    },
    {
        "name": "result_wrong_state",
        "parts": [part("output-available", input={}, output=1)],
        "update": {"kind": "result", "toolCallId": "c1", "output": 2},
    },
    {
        "name": "result_no_match",
        "parts": [part("input-available", input={})],
        "update": {"kind": "result", "toolCallId": "nope", "output": 2},
    },
    {
        "name": "cross_message_settled_stays",
        "parts": [part("output-error", input={}, errorText="e")],
        "update": {
            "kind": "crossMessage",
            "toolCallId": "c1",
            "updateType": "output-available",
            "output": 1,
        },
    },
    {
        "name": "cross_message_preliminary",
        "parts": [part("input-streaming")],
        "update": {
            "kind": "crossMessage",
            "toolCallId": "c1",
            "updateType": "output-available",
            "output": 1,
            "preliminary": True,
        },
    },
    {
        "name": "cross_message_error_default",
        "parts": [
            part("approval-responded", input={}, approval={"id": "a", "approved": True})
        ],
        "update": {
            "kind": "crossMessage",
            "toolCallId": "c1",
            "updateType": "output-error",
        },
    },
    {
        "name": "approval_yes",
        "parts": [part("approval-requested", input={}, approval={"id": "a1"})],
        "update": {"kind": "approval", "toolCallId": "c1", "approved": True},
    },
    {
        "name": "approval_no_synthesized_id",
        "parts": [part("input-available", input={})],
        "update": {"kind": "approval", "toolCallId": "c1", "approved": False},
    },
    {
        "name": "paused_replaced",
        "parts": [
            part(
                "output-available",
                input={},
                output={"status": "paused", "executionId": "e1"},
            )
        ],
        "update": {
            "kind": "paused",
            "toolCallId": "c1",
            "executionId": "e1",
            "output": {"done": 1},
        },
    },
    {
        "name": "paused_other_execution",
        "parts": [
            part(
                "output-available",
                input={},
                output={"status": "paused", "executionId": "e2"},
            )
        ],
        "update": {
            "kind": "paused",
            "toolCallId": "c1",
            "executionId": "e1",
            "output": 1,
        },
    },
    {
        "name": "batch_incomplete",
        "parts": [
            part("output-available", "c1", input={}, output=1),
            part("input-available", "c2", input={}),
            {
                "type": "dynamic-tool",
                "toolCallId": "c3",
                "toolName": "dyn",
                "state": "input-available",
                "input": {},
            },
        ],
        "update": {"kind": "result", "toolCallId": "c2", "output": 2},
        "clientTools": ["dyn"],
    },
]

REPAIR = [
    {
        "name": "interrupted_and_settled",
        "messages": [
            msg(
                "a1",
                "assistant",
                part("input-available", "c1", input="bad"),
                part("output-available", "c2", input=[1], output=1),
                {"type": "text", "text": "x"},
                {
                    "type": "dynamic-tool",
                    "toolCallId": "c3",
                    "toolName": "d",
                    "state": "input-streaming",
                },
            ),
        ],
    },
    {
        "name": "approvals_option_off",
        "messages": [
            msg(
                "a1",
                "assistant",
                part(
                    "approval-responded",
                    input={},
                    approval={"id": "a", "approved": True},
                ),
            ),
            msg("u2", "user"),
        ],
    },
    {
        "name": "approvals_option_on",
        "repairApprovalResponded": True,
        "messages": [
            msg(
                "a1",
                "assistant",
                part(
                    "approval-responded",
                    "c1",
                    input={},
                    approval={"id": "a", "approved": True},
                ),
                part(
                    "approval-responded",
                    "c2",
                    input=None,
                    approval={"id": "b", "approved": False},
                ),
            ),
            msg("u2", "user"),
            msg(
                "a3",
                "assistant",
                part(
                    "approval-responded",
                    "c3",
                    input={},
                    approval={"id": "c", "approved": True},
                ),
            ),
        ],
    },
    {
        "name": "result_field_counts_as_settled",
        "messages": [
            msg("a1", "assistant", {**part("input-available", input={}), "result": 1}),
        ],
    },
]


def main() -> None:
    cases = {
        "builder": [
            {"name": name, "chunks": chunks, "parts": BUILDER_PARTS.get(name, [])}
            for name, chunks in BUILDER.items()
        ],
        "accumulator": ACCUMULATOR,
        "reconciler": RECONCILER,
        "toolState": TOOL_STATE,
        "repair": REPAIR,
    }
    path = Path(__file__).with_name("cases.json")
    path.write_text(json.dumps(cases, indent=1) + "\n")


if __name__ == "__main__":
    main()
