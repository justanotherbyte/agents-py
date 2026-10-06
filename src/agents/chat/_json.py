"""JSON text as ``JSON.stringify`` writes it, for chat's comparisons."""

import json
from typing import Any

__all__ = ("dumps", "stable_dumps")


def dumps(value: Any) -> str:
    """Return compact JSON keeping non-ASCII characters (``JSON.stringify``)."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def stable_dumps(value: Any) -> str:
    """Return `dumps` with object keys sorted (upstream's ``stableStringify``)."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True)
