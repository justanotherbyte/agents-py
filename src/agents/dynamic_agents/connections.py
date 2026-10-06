"""Virtual connections: a facet's view of client sockets the root owns.

Port of the virtual-connection half of upstream ``dynamic-agents.ts``. The
root keeps every native socket; a facet sees a `VirtualConnection` with the
same API, whose operations go back through the live event's bridge or, after
that event has returned, through the root over RPC
(``.design/subagents_engine.md`` §2, §3.2 items 3-5).
"""

from typing import TYPE_CHECKING, Any

from ..websockets.connection import Connection
from ..websockets.types import Attachment
from .types import ForwardedConnection

if TYPE_CHECKING:
    from .dynamic_agents import SubAgentsEngine

__all__ = (
    "DELETED_FLAG",
    "OUTER_URL_FLAG",
    "ROOT_ONLY_FLAGS",
    "TAGS_FLAG",
    "VirtualConnection",
)

OUTER_URL_FLAG = "sub_agent_outer_url"
"""On a root socket: the client's full URL, so events can be forwarded."""

DELETED_FLAG = "sub_agent_deleted"
"""On a root socket: its sub-agent was deleted; stale frames are dropped."""

TAGS_FLAG = "sub_agent_tags"
"""On a root socket: the tags the sub-agent's ``get_connection_tags`` chose."""

ROOT_ONLY_FLAGS = frozenset({OUTER_URL_FLAG, DELETED_FLAG})
"""Flags the root keeps for itself and never forwards to a sub-agent."""


class VirtualConnection(Connection):
    """A client connection whose socket lives on the root agent.

    Reads come from the metadata forwarded with each event; writes
    (``send``, ``close``, ``set_state``, flags) are queued, in order, to the
    root. Equal when the ids are equal (there's no local socket).
    """

    __slots__ = ("_engine", "_meta")

    def __init__(self, engine: "SubAgentsEngine", meta: ForwardedConnection) -> None:
        super().__init__(
            None, {"id": meta["id"], "uri": meta["uri"], "tags": list(meta["tags"])}
        )
        self._engine = engine
        self._meta = meta

    def _update(self, meta: ForwardedConnection) -> None:
        self._meta = meta
        self._uri = meta["uri"]
        self._tags = tuple(meta["tags"])

    def _set_tags(self, tags: list[str]) -> None:
        self._tags = tuple(tags)
        self._meta["tags"] = list(tags)

    @property
    def _is_open(self) -> bool:
        return True

    def _attachment(self) -> Attachment:
        attachment: Attachment = {
            "__pk": {"id": self._id, "uri": self._uri, "tags": list(self._tags)},
            "__user": self._meta["state"],
        }
        if self._meta["flags"]:
            attachment["__flags"] = dict(self._meta["flags"])
        return attachment

    def _write(self, attachment: Attachment) -> None:
        state = attachment.get("__user")
        flags = dict(attachment.get("__flags", {}))
        self._meta["state"] = state
        self._meta["flags"] = flags
        self._engine.route_operation(self._id, "set_state", state, flags)

    def send(self, message: str | bytes) -> None:
        """Send a frame to the client (through the root, in order)."""
        self._engine.route_operation(self._id, "send", message)

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Close the client's socket (through the root, in order)."""
        self._engine.route_operation(self._id, "close", code, reason)

    def __eq__(self, other: object) -> bool:
        """Return whether ``other`` is the same client connection."""
        return isinstance(other, VirtualConnection) and other._id == self._id

    def __hash__(self) -> int:
        """Hash the connection id."""
        return hash(self._id)


def forwarded_state(connection: Connection) -> tuple[Any, dict[str, Any]]:
    """Return what a sub-agent sees of a connection: its state and flags."""
    attachment = connection._attachment()
    flags = {
        key: value
        for key, value in attachment.get("__flags", {}).items()
        if key not in ROOT_ONLY_FLAGS
    }
    return attachment.get("__user"), flags


def apply_forwarded_state(
    connection: Connection, state: Any, flags: dict[str, Any]
) -> None:
    """Store a sub-agent's state and flags on a connection, keeping root-only flags."""
    attachment = connection._attachment()
    kept = {
        key: value
        for key, value in attachment.get("__flags", {}).items()
        if key in ROOT_ONLY_FLAGS
    }
    attachment["__user"] = state
    merged = {**flags, **kept}
    if merged:
        attachment["__flags"] = merged
    else:
        attachment.pop("__flags", None)
    connection._write(attachment)
