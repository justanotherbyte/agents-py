"""Token estimates stamped on stored messages (upstream ``sessions/tokens.ts``).

A heuristic: a real tokenizer would cost about 100 MB of heap. Nothing acts
on it in phase 1 (compaction is deferred); it's reported by
``get_history_row_stats``.
"""

import json
import math
from typing import Any

from .types import SessionMessage

__all__ = (
    "CHARS_PER_TOKEN",
    "IMAGE_ATTACHMENT_TOKENS",
    "MAX_ATTACHMENT_TOKENS",
    "TOKENS_PER_MESSAGE",
    "WORDS_TOKEN_MULTIPLIER",
    "estimate_attachment_tokens",
    "estimate_message_tokens",
    "estimate_row_tokens",
    "estimate_string_tokens",
    "estimated_data_url_bytes",
)

CHARS_PER_TOKEN = 4
WORDS_TOKEN_MULTIPLIER = 1.3
TOKENS_PER_MESSAGE = 4
IMAGE_ATTACHMENT_TOKENS = 1_600
MAX_ATTACHMENT_TOKENS = 20_000


def estimate_string_tokens(text: str) -> int:
    """Estimate ``text`` as the larger of characters / 4 and words * 1.3."""
    if not text:
        return 0
    chars = len(text) / CHARS_PER_TOKEN
    words = len(text.split()) * WORDS_TOKEN_MULTIPLIER
    return math.ceil(max(chars, words))


def estimate_message_tokens(messages: list[SessionMessage]) -> int:
    """Estimate messages: a fixed charge each, plus their parts' text."""
    tokens = 0
    for message in messages:
        tokens += TOKENS_PER_MESSAGE
        for part in message["parts"]:
            kind = str(part.get("type", ""))
            if kind in ("text", "reasoning"):
                tokens += _estimate_value(_first(part, "text", "reasoning"))
            elif kind.startswith("tool-") or kind == "dynamic-tool":
                tokens += _estimate_value(part.get("input"))
                tokens += _estimate_value(_first(part, "output", "result"))
            elif part.get("text") is not None:
                tokens += _estimate_value(part["text"])
            elif part.get("result") is not None:
                tokens += _estimate_value(part["result"])
    return tokens


def estimate_attachment_tokens(media_type: str, size: int) -> int:
    """Estimate an attachment: images flat, other files by size (capped)."""
    if media_type.startswith("image/"):
        return IMAGE_ATTACHMENT_TOKENS
    return min(math.ceil(size / 4), MAX_ATTACHMENT_TOKENS)


def estimated_data_url_bytes(url: str) -> int:
    """Return the decoded size of a ``data:`` URL's payload (0 otherwise)."""
    comma = url.find(",")
    if not url.startswith("data:") or comma < 0:
        return 0
    header, payload = url[len("data:") : comma], url[comma + 1 :]
    return len(payload) * 3 // 4 if header.endswith(";base64") else len(payload)


def estimate_row_tokens(message: SessionMessage) -> int:
    """Return the estimate stamped on a stored message (inline ``data:`` files too)."""
    tokens = estimate_message_tokens([message])
    for part in message["parts"]:
        url = part.get("url")
        if (
            part.get("type") == "file"
            and isinstance(url, str)
            and url.startswith("data:")
        ):
            media_type = part.get("mediaType")
            tokens += estimate_attachment_tokens(
                media_type
                if isinstance(media_type, str)
                else "application/octet-stream",
                estimated_data_url_bytes(url),
            )
    return tokens


def _first(part: dict[str, Any], *keys: str) -> Any:
    # JS `a ?? b`: the first that's present and not null.
    for key in keys:
        if part.get(key) is not None:
            return part[key]
    return None


def _estimate_value(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return estimate_string_tokens(value)
    # Measured as JSON.stringify would write it: compact, non-ASCII kept.
    text = json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str)
    return estimate_string_tokens(text)
