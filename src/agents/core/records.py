"""Declaring record fields that have a different name on the wire.

``.design/utilities.md`` §5: SDK records are dataclasses with snake_case
attributes; a field whose wire (JSON) name differs is declared with `wire`,
and codecs read the name from the field's metadata.
"""

from dataclasses import MISSING, field
from typing import Any

__all__ = ("wire",)


def wire(name: str, default: Any = MISSING) -> Any:
    """Declare a field whose wire name is ``name``.

    Without ``default`` the field stays required; ``wire(name, None)`` makes
    it optional.
    """
    return field(default=default, metadata={"wire": name})
