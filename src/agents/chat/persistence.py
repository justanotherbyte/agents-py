"""Chat's write-side hygiene on top of Sessions' (upstream ``AIChatAgent``).

Provider-executed tool parts (code execution, text editors, …) can carry
large inputs and outputs that only the provider needs; their strings over
500 characters are cut with a marker before saving. Web search and fetch
results are kept whole, as are strings under keys starting with
``encrypted`` (opaque provider state that must replay byte for byte).
"""

from typing import Any

__all__ = ("PROVIDER_TOOL_MAX_STRING_LENGTH", "truncate_provider_tool_payloads")

PROVIDER_TOOL_MAX_STRING_LENGTH = 500
_OPAQUE_KEY_PREFIX = "encrypted"
_PRESERVED_TOOLS = frozenset({"web_search", "web_fetch"})

type Part = dict[str, Any]


def truncate_provider_tool_payloads(part: Part) -> Part:
    """Return ``part`` with a provider-executed tool's long strings truncated."""
    if not part.get("providerExecuted") or _tool_name(part) in _PRESERVED_TOOLS:
        return part
    result = dict(part)
    if "input" in result:
        result["input"] = _truncate(result["input"])
    if "output" in result:
        result["output"] = _truncate(result["output"])
    return result


def _truncate(value: Any, opaque: bool = False) -> Any:
    if isinstance(value, str):
        if opaque or len(value) <= PROVIDER_TOOL_MAX_STRING_LENGTH:
            return value
        marker = f"… [truncated, original length: {len(value)}]"
        return value[: max(0, PROVIDER_TOOL_MAX_STRING_LENGTH - len(marker))] + marker
    if isinstance(value, list):
        return [_truncate(item, opaque) for item in value]
    if isinstance(value, dict):
        return {
            key: _truncate(item, opaque or key.startswith(_OPAQUE_KEY_PREFIX))
            for key, item in value.items()
        }
    return value


def _tool_name(part: Part) -> str | None:
    name = part.get("toolName")
    if isinstance(name, str):
        return name
    kind = part.get("type")
    if isinstance(kind, str) and kind.startswith("tool-"):
        return kind[len("tool-") :]
    return None
