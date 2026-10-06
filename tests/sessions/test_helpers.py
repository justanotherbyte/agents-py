from agents.sessions.chunking import split_content
from agents.sessions.sanitize import byte_length, sanitize_message
from agents.sessions.tokens import (
    estimate_row_tokens,
    estimate_string_tokens,
    estimated_data_url_bytes,
)
from agents.sessions.types import SessionMessage


def message(*parts: dict) -> SessionMessage:
    return {"id": "m", "role": "assistant", "parts": list(parts)}


def test_split_content_cuts_between_characters() -> None:
    text = "ab" + "🌍" * 3 + "é" * 4  # 2 + 12 + 8 bytes
    for budget in (1, 3, 4, 5, 7, 22, 100):
        slices = split_content(text, budget)
        assert "".join(slices) == text
        assert all(byte_length(s) <= max(budget, 4) for s in slices)
    assert split_content("") == [""]
    assert split_content("🌍", 2) == ["🌍"]  # wider than the budget: alone


def test_sanitize_strips_openai_ephemera_and_empty_reasoning() -> None:
    sanitized = sanitize_message(
        message(
            {
                "type": "text",
                "text": "hi",
                "providerMetadata": {"openai": {"itemId": "x", "keep": 1}},
            },
            {
                "type": "tool-search",
                "callProviderMetadata": {
                    "openai": {"itemId": "y"},
                    "other": {"a": 1},
                },
            },
            {"type": "text", "text": "x", "providerMetadata": {"openai": {}}},
            {"type": "reasoning", "text": "  "},
            {
                "type": "reasoning",
                "text": "",
                "providerMetadata": {"anthropic": {"s": 1}},
            },
            {
                "type": "reasoning",
                "text": "",
                "providerMetadata": {"openai": {"reasoningEncryptedContent": "z"}},
            },
        )
    )
    assert sanitized["parts"] == [
        {"type": "text", "text": "hi", "providerMetadata": {"openai": {"keep": 1}}},
        {"type": "tool-search", "callProviderMetadata": {"other": {"a": 1}}},
        {"type": "text", "text": "x"},
        {"type": "reasoning", "text": "", "providerMetadata": {"anthropic": {"s": 1}}},
    ]


def test_token_estimates() -> None:
    assert estimate_string_tokens("") == 0
    assert estimate_string_tokens("abcdefgh") == 2  # characters / 4
    assert estimate_string_tokens("a b c d e f") == 8  # 6 words * 1.3
    assert estimated_data_url_bytes("data:image/png;base64,AAAA") == 3
    assert estimated_data_url_bytes("data:text/plain,hello") == 5
    image = message(
        {"type": "file", "mediaType": "image/png", "url": "data:image/png;base64,AA"}
    )
    assert estimate_row_tokens(image) == 4 + 1600
    tool = message({"type": "tool-x", "input": {"q": "abcd"}, "output": "efgh"})
    # '{"q":"abcd"}' is 12 characters (3); "efgh" is 1 word * 1.3 (2).
    assert estimate_row_tokens(tool) == 4 + 3 + 2
