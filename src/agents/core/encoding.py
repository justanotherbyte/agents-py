"""The SDK's JSON encoding for frames, RPC results, and events.

``json.dumps`` plus two conversions (``.design/agent_api.md`` §1.8):
dataclasses become objects (SDK records under their camelCase wire names,
omitting optional fields that are ``None``), and timezone-aware
``datetime``s become epoch milliseconds, as upstream's records use.
"""

import dataclasses
import json
from datetime import datetime
from typing import Any

from .timing import epoch_ms

__all__ = ("to_json",)


def to_json(value: Any) -> str:
    """Serialize ``value`` as compact JSON.

    Raises
    ------
    TypeError
        If ``value`` contains something JSON can't represent (including a
        naive ``datetime``).
    ValueError
        If ``value`` contains a circular reference.
    """
    return json.dumps(value, default=_encode, separators=(",", ":"))


def _encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return epoch_ms(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _dataclass_fields(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _dataclass_fields(record: Any) -> dict[str, Any]:
    """Return a dataclass's fields, under their wire names where declared.

    A field with a wire name (``field(metadata={"wire": ...})``) that is
    optional (defaults to ``None``) is left out when ``None``: the wire
    format omits optional fields rather than sending ``null``.
    """
    out: dict[str, Any] = {}
    for field in dataclasses.fields(record):
        value = getattr(record, field.name)
        wire_name = field.metadata.get("wire")
        if wire_name is None:
            out[field.name] = value
        elif value is not None or field.default is not None:
            out[wire_name] = value
    return out
