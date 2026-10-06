from typing import Any

import pytest

from agents.chat import (
    DataPart,
    FilePart,
    ReasoningPart,
    SourceDocumentPart,
    SourceUrlPart,
    StepStartPart,
    TextPart,
    ToolApproval,
    ToolApprovalRequestedPart,
    ToolInputStreamingPart,
    ToolOutputAvailablePart,
    ToolOutputDeniedPart,
    ToolOutputErrorPart,
    ToolPart,
    UIMessage,
    UnknownPart,
    chunks,
)
from agents.chat.accumulator import StreamAccumulator
from agents.chat.codec import (
    chunk_to_wire,
    message_from_wire,
    message_to_wire,
    part_from_wire,
)
from agents.chat.persistence import truncate_provider_tool_payloads


def test_chunks_are_written_in_wire_form() -> None:
    assert chunk_to_wire(chunks.TextDelta(id="t", delta="hi")) == {
        "type": "text-delta",
        "id": "t",
        "delta": "hi",
    }
    assert chunk_to_wire(
        chunks.ToolInputAvailable(
            tool_call_id="c", tool_name="search", input=None, dynamic=True
        )
    ) == {
        "type": "tool-input-available",
        "toolCallId": "c",
        "toolName": "search",
        "input": None,
        "dynamic": True,
    }
    assert chunk_to_wire(chunks.ToolOutputAvailable(tool_call_id="c", output=None)) == {
        "type": "tool-output-available",
        "toolCallId": "c",
        "output": None,  # required: written even when None
    }
    assert chunk_to_wire(chunks.Data(name="status", data={"n": 1}, transient=True)) == {
        "type": "data-status",
        "data": {"n": 1},
        "transient": True,
    }
    assert chunk_to_wire(chunks.Data(name="s", data=None, id="x")) == {
        "type": "data-s",
        "id": "x",
        "data": None,
    }
    assert chunk_to_wire(chunks.Finish(finish_reason="stop")) == {
        "type": "finish",
        "finishReason": "stop",
    }
    assert chunk_to_wire(chunks.Start()) == {"type": "start"}


PARTS: list[Any] = [
    TextPart(text="hi", state="done", provider_metadata={"p": {"x": 1}}),
    ReasoningPart(text="r", id="r1"),
    FilePart(url="data:,x", media_type="text/plain", filename="f.txt"),
    SourceUrlPart(source_id="s", url="https://x.dev"),
    SourceDocumentPart(source_id="d", media_type="application/pdf", title="T"),
    StepStartPart(),
    DataPart(name="status", data=[1, 2], id="s1"),
    ToolInputStreamingPart(tool_name="search", tool_call_id="c1"),
    ToolApprovalRequestedPart(
        tool_name="search", tool_call_id="c2", input={}, approval=ToolApproval(id="a")
    ),
    ToolOutputAvailablePart(
        tool_name="dyn", tool_call_id="c3", dynamic=True, input={"q": 1}, output=None
    ),
    ToolOutputErrorPart(tool_name="search", tool_call_id="c4", error_text="boom"),
    ToolOutputDeniedPart(
        tool_name="search",
        tool_call_id="c5",
        input={},
        approval=ToolApproval(id="b", approved=False, reason="no"),
    ),
    UnknownPart(fields={"type": "reasoning-file", "url": "x", "extra": [1]}),
]


@pytest.mark.parametrize("part", PARTS, ids=lambda p: type(p).__name__)
def test_parts_round_trip(part: Any) -> None:
    message = UIMessage(id="m", role="assistant", parts=[part])
    wire = message_to_wire(message)
    assert message_from_wire(wire) == message
    assert message_to_wire(message_from_wire(wire)) == wire


def test_tool_parts_carry_their_name_and_state_on_the_wire() -> None:
    wire = message_to_wire(
        UIMessage(id="m", role="assistant", parts=[PARTS[9], PARTS[8]])
    )
    dynamic, static = wire["parts"]
    assert (dynamic["type"], dynamic["toolName"], dynamic["state"]) == (
        "dynamic-tool",
        "dyn",
        "output-available",
    )
    assert dynamic["output"] is None and "dynamic" not in dynamic
    assert (static["type"], static["toolName"], static["approval"]) == (
        "tool-search",
        "search",
        {"id": "a"},
    )


@pytest.mark.parametrize(
    "wire",
    [
        {"type": "reasoning-file", "mediaType": "image/png", "url": "x"},
        {"type": "text"},  # missing its required text
        {"type": "tool-search", "toolCallId": "c", "state": "some-new-state"},
        {
            "type": "tool-search",
            "toolCallId": "c",
            "state": "output-denied",
            "input": {},
            "approval": 1,
        },
        {"no": "type"},
    ],
)
def test_parts_that_dont_decode_are_kept_raw(wire: dict[str, Any]) -> None:
    part = part_from_wire(wire)
    assert isinstance(part, UnknownPart) and part.fields == wire
    assert message_to_wire(UIMessage(id="m", role="user", parts=[part]))["parts"] == [
        wire
    ]


def test_unknown_fields_on_known_parts_are_dropped() -> None:
    part = part_from_wire({"type": "text", "text": "x", "futureField": 1})
    assert part == TextPart(text="x")


def test_typed_chunks_build_the_message_the_codec_reads() -> None:
    stream = [
        chunks.Start(message_id="m1"),
        chunks.TextStart(id="t"),
        chunks.TextDelta(id="t", delta="hello"),
        chunks.TextEnd(id="t"),
        chunks.ToolInputStart(tool_call_id="c", tool_name="search"),
        chunks.ToolInputAvailable(tool_call_id="c", tool_name="search", input={"q": 1}),
        chunks.ToolOutputAvailable(tool_call_id="c", output=[1]),
        chunks.Data(name="progress", data=50, transient=True),
        chunks.Finish(),
    ]
    acc = StreamAccumulator(message_id="temp")
    for chunk in stream:
        acc.apply_chunk(chunk_to_wire(chunk))
    message = message_from_wire(acc.to_message())
    assert message.id == "m1"
    text, tool = message.parts
    assert text == TextPart(text="hello", state="done")
    assert isinstance(tool, ToolPart) and isinstance(tool, ToolOutputAvailablePart)
    assert (tool.tool_name, tool.input, tool.output) == ("search", {"q": 1}, [1])


def test_provider_tool_payloads_are_truncated_for_storage() -> None:
    big = "x" * 600
    part = {
        "type": "tool-code_execution",
        "providerExecuted": True,
        "input": {"code": big, "encrypted_state": big},
        "output": [big, 1],
    }
    truncated = truncate_provider_tool_payloads(part)
    code = truncated["input"]["code"]
    assert len(code) == 500 and code.endswith("[truncated, original length: 600]")
    assert truncated["input"]["encrypted_state"] == big  # opaque: kept whole
    assert truncated["output"][0].endswith("600]") and truncated["output"][1] == 1
    search = {**part, "type": "tool-web_search"}
    assert truncate_provider_tool_payloads(search) is search
    client = {**part, "providerExecuted": False}
    assert truncate_provider_tool_payloads(client) is client
