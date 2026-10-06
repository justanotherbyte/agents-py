"""The server side of the stream resume handshake (wire protocol §7.4).

Port of upstream ``chat/resume-handshake.ts`` and ``chat/pre-stream-turns.ts``.
A reconnecting client asks whether a turn is in flight; the server offers
the active stream (``resuming``), tells it to keep waiting for a turn that
hasn't started streaming (``pending``), replays a terminal error it missed,
or says there's nothing (``resume_none``). The frames match upstream's
frozen fixture byte for byte.

Not yet ported (12c): the branches for a tool continuation turn.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from ..websockets.connection import Connection
from .protocol import (
    RESUME_NONE_IDLE,
    STREAM_PENDING,
    STREAM_RESUME_NONE,
    STREAM_RESUMING,
    USE_CHAT_RESPONSE,
    send_if_open,
)
from .resumable_stream import ResumableStream
from .terminal import TerminalRecord

__all__ = ("PreStreamTurns", "ResumeHandshake")


class PreStreamTurns:
    """Accepted turns that haven't started streaming, and clients waiting on them.

    Between a send being accepted and its first chunk (queued, debouncing,
    or in ``on_chat_message``'s setup), a reconnecting client is parked
    here and told to keep waiting. Parked clients are offered the stream
    when it starts, or released once every accepted turn has settled without
    one. In memory only: a turn in that window keeps the object awake.
    """

    __slots__ = ("_accepted", "_latest_request_id", "awaiting")

    def __init__(self) -> None:
        self._accepted: set[str] = set()
        self._latest_request_id: str | None = None
        self.awaiting: dict[str, Connection] = {}
        """Parked connections, by id."""

    def begin(self, request_id: str) -> None:
        """Mark an accepted turn as in flight."""
        self._accepted.add(request_id)
        self._latest_request_id = request_id

    def settle(self, request_id: str) -> bool:
        """Mark an accepted turn as settled; return whether none remain."""
        self._accepted.discard(request_id)
        if self._accepted:
            return False
        self._latest_request_id = None
        return True

    def has_in_flight(self) -> bool:
        """Whether an accepted turn hasn't settled."""
        return bool(self._accepted)

    def park(self, connection: Connection, probe_id: str | None = None) -> bool:
        """Park a connection and send it ``stream_pending``, if a turn is in flight.

        Parked connections still get live broadcasts until they're offered
        the stream.
        """
        if not self._accepted:
            return False
        self.awaiting[connection.id] = connection
        send_if_open(connection, _pending_frame(self._latest_request_id, probe_id))
        return True

    def release(self, connection_id: str) -> None:
        """Forget one connection (it closed)."""
        self.awaiting.pop(connection_id, None)

    def flush_on_stream_start(self, notify: Callable[[Connection], None]) -> None:
        """Hand every parked connection to ``notify`` (a stream started)."""
        awaiting, self.awaiting = self.awaiting, {}
        for connection in awaiting.values():
            notify(connection)

    def release_awaiting(self) -> None:
        """Send ``resume_none`` to every parked connection and forget them."""
        awaiting, self.awaiting = self.awaiting, {}
        for connection in awaiting.values():
            send_if_open(connection, {"type": STREAM_RESUME_NONE})

    def reset(self) -> None:
        """Forget everything, without sending frames (a chat clear)."""
        self._accepted.clear()
        self.awaiting.clear()
        self._latest_request_id = None


class ResumeHandshake:
    """Answers a client's resume request and ACK.

    Parameters
    ----------
    stream
        The agent's resumable streams.
    pre_stream
        Its accepted turns that haven't started streaming.
    pending_resume
        Ids of connections offered the stream but not yet ACKed; they're
        left out of live broadcasts until their replay. Shared with the
        agent's broadcast path.
    pending_terminal
        Read the stored terminal record.
    persist_orphaned_stream
        Save the partial message of a stream nothing produces any more.
    holds_terminal_frames
        Whether the agent still holds a request's terminal frames (its
        message is being saved; they follow live).
    """

    __slots__ = (
        "_holds_terminal_frames",
        "_pending_resume",
        "_pending_terminal",
        "_persist_orphaned_stream",
        "_pre_stream",
        "_stream",
    )

    def __init__(
        self,
        *,
        stream: ResumableStream,
        pre_stream: PreStreamTurns,
        pending_resume: set[str],
        pending_terminal: Callable[[], Awaitable[TerminalRecord | None]],
        persist_orphaned_stream: Callable[[str], Awaitable[None]],
        holds_terminal_frames: Callable[[str], bool],
    ) -> None:
        self._stream = stream
        self._pre_stream = pre_stream
        self._pending_resume = pending_resume
        self._pending_terminal = pending_terminal
        self._persist_orphaned_stream = persist_orphaned_stream
        self._holds_terminal_frames = holds_terminal_frames

    def notify_stream_resuming(
        self, connection: Connection, probe_id: str | None = None
    ) -> None:
        """Offer the active stream to a connection (``stream_resuming``).

        A connection may be offered it twice (on connect and in reply to its
        request); that's intended, and the client de-duplicates its ACK.
        """
        request_id = self._stream.active_request_id
        if request_id is None:
            return
        if send_if_open(connection, _resuming_frame(request_id, probe_id)):
            self._pending_resume.add(connection.id)

    async def handle_resume_request(
        self, connection: Connection, probe_id: str | None = None
    ) -> None:
        """Answer ``cf_agent_stream_resume_request``."""
        if self._stream.has_active_stream():
            self.notify_stream_resuming(connection, probe_id)
            return
        # A turn that failed while no client was connected: its terminal
        # frame follows the client's ACK.
        if await self._replay_terminal_on_resume(connection, probe_id):
            return
        # A turn that hasn't started streaming: offered the stream when it
        # starts, or released.
        if self._pre_stream.park(connection, probe_id):
            return
        # The only reply that proves the agent is idle.
        frame: dict[str, Any] = {"type": STREAM_RESUME_NONE, "reason": RESUME_NONE_IDLE}
        if probe_id:
            frame["probeId"] = probe_id
        send_if_open(connection, frame)

    async def handle_resume_ack(self, connection: Connection, request_id: str) -> None:
        """Answer ``cf_agent_stream_resume_ack``: replay ``request_id``."""
        stream = self._stream
        self._pending_resume.discard(connection.id)
        if stream.has_active_stream():
            if stream.active_request_id != request_id:
                return  # an ACK for another request
            orphaned = stream.replay_chunks(connection, request_id)
            if orphaned is not None:
                await self._persist_orphaned_stream(orphaned)
            return
        if self._holds_terminal_frames(request_id):
            stream.replay_closed_stream_chunks(connection, request_id)
            return
        if await self._replay_terminal_on_ack(connection, request_id):
            return
        if not stream.replay_completed_chunks_by_request_id(connection, request_id):
            frame: dict[str, Any] = {
                "body": "",
                "done": True,
                "id": request_id,
                "type": USE_CHAT_RESPONSE,
                "replay": True,
            }
            message_ids = stream.get_origin_message_ids(request_id)
            if message_ids:
                frame["messageIds"] = list(message_ids)
            outcome = stream.get_outcome(request_id)
            if outcome:
                frame["outcome"] = outcome
            send_if_open(connection, frame)

    async def _replay_terminal_on_resume(
        self, connection: Connection, probe_id: str | None
    ) -> bool:
        # A bare terminal frame is dropped by the client unless it arrives on
        # a resumed stream, so offer the stream and send it after the ACK.
        pending = await self._pending_terminal()
        if pending is None:
            return False
        send_if_open(connection, _resuming_frame(pending.request_id, probe_id))
        return True

    async def _replay_terminal_on_ack(
        self, connection: Connection, request_id: str
    ) -> bool:
        # The record is kept, so every reconnecting tab learns the outcome.
        pending = await self._pending_terminal()
        if pending is None or pending.request_id != request_id:
            return False
        if not self._stream.replay_errored_chunks_by_request_id(connection, request_id):
            return True  # closed mid-replay: the next reconnect retries it all
        frame: dict[str, Any] = {
            "body": pending.body,
            "done": True,
            "error": True,
            "id": request_id,
            "type": USE_CHAT_RESPONSE,
        }
        message_ids = pending.message_ids or self._stream.get_origin_message_ids(
            request_id
        )
        if message_ids:
            frame["messageIds"] = list(message_ids)
        send_if_open(connection, frame)
        return True


def _resuming_frame(request_id: str, probe_id: str | None) -> dict[str, Any]:
    frame: dict[str, Any] = {"type": STREAM_RESUMING, "id": request_id}
    if probe_id:
        frame["probeId"] = probe_id
    return frame


def _pending_frame(request_id: str | None, probe_id: str | None) -> dict[str, Any]:
    frame: dict[str, Any] = {"type": STREAM_PENDING}
    if request_id:
        frame["id"] = request_id
    if probe_id:
        frame["probeId"] = probe_id
    return frame
