"""Task values as JSON text (``.design/tasks_engine.md`` §2.2 items 3-5).

Inputs, step results, metadata, and results are stored as JSON in SQLite.
``None`` is stored as SQL ``NULL``: "no value".
"""

import json
from typing import Any

from .errors import TaskSerializationError

__all__ = ("MAX_SERIALIZED_BYTES", "deserialize_task_value", "serialize_task_value")

MAX_SERIALIZED_BYTES = 1_048_576
"""The largest serialized value (1 MiB)."""


def serialize_task_value(value: Any, context: str) -> str | None:
    """Return ``value`` as JSON text, or ``None`` for ``None``.

    Parameters
    ----------
    value
        The value to store.
    context
        What it is, for error messages (e.g. ``result of step 'fetch'``).

    Raises
    ------
    TaskSerializationError
        If ``value`` isn't plain JSON (dataclasses, ``datetime``, sets, and
        NaN aren't), or its JSON exceeds 1 MiB.
    """
    if value is None:
        return None
    try:
        text = json.dumps(value, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise TaskSerializationError(context, str(error)) from error
    size = len(text.encode())
    if size > MAX_SERIALIZED_BYTES:
        raise TaskSerializationError(
            context, f"{size} bytes exceeds the {MAX_SERIALIZED_BYTES}-byte limit"
        )
    return text


def deserialize_task_value(stored: str | None) -> Any:
    """Return a value stored by `serialize_task_value` (``NULL`` is ``None``)."""
    return json.loads(stored) if stored is not None else None
