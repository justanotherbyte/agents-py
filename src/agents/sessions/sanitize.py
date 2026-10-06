"""Message sanitization before storage (upstream ``sessions/sanitize.ts``).

The chat layer uses these too (upstream ``chat/sanitize.ts`` imports them).
"""

from typing import Any

from .types import SessionMessage

__all__ = ("byte_length", "sanitize_message")

_METADATA_KEYS = ("providerMetadata", "callProviderMetadata")
# OpenAI's per-response item ids and encrypted reasoning: ephemeral, and
# invalid if replayed in a later request.
_EPHEMERAL_OPENAI_KEYS = ("itemId", "reasoningEncryptedContent")


def byte_length(text: str) -> int:
    """Return the UTF-8 length of ``text`` (a lone surrogate counts 3 bytes)."""
    return len(text.encode("utf-8", "surrogatepass"))


def sanitize_message(message: SessionMessage) -> SessionMessage:
    """Return ``message`` ready to store.

    Removes OpenAI's ephemeral ``itemId`` and ``reasoningEncryptedContent``
    from ``providerMetadata.openai`` and ``callProviderMetadata.openai``
    (dropping keys left empty), and drops reasoning parts with no text and
    no remaining provider metadata. The input isn't modified.
    """
    parts = []
    for part in message["parts"]:
        sanitized = part
        for key in _METADATA_KEYS:
            metadata = sanitized.get(key)
            if isinstance(metadata, dict) and "openai" in metadata:
                sanitized = _strip_openai_metadata(sanitized, key)
        if sanitized.get("type") == "reasoning" and _is_empty_reasoning(sanitized):
            continue
        parts.append(sanitized)
    return {**message, "parts": parts}


def _is_empty_reasoning(part: dict[str, Any]) -> bool:
    text = part.get("text")
    if isinstance(text, str) and text.strip():
        return False
    metadata = part.get("providerMetadata")
    return not (isinstance(metadata, dict) and metadata)


def _strip_openai_metadata(part: dict[str, Any], key: str) -> dict[str, Any]:
    metadata = part[key]
    openai = metadata["openai"]
    if not isinstance(openai, dict):
        return part
    rest_openai = {k: v for k, v in openai.items() if k not in _EPHEMERAL_OPENAI_KEYS}
    rest_metadata = {k: v for k, v in metadata.items() if k != "openai"}
    if rest_openai:
        replacement: dict[str, Any] | None = {**rest_metadata, "openai": rest_openai}
    elif rest_metadata:
        replacement = rest_metadata
    else:
        replacement = None
    rest = {k: v for k, v in part.items() if k != key}
    return {**rest, key: replacement} if replacement is not None else rest
