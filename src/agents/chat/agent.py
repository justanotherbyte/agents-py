"""``AIChatAgent``: an agent that runs chat turns (upstream ``ai-chat/src/index.ts``).

The transcript is stored in Sessions and mirrored, as wire-form dicts, into
memory (``self.messages`` decodes them on demand). A turn runs
``on_chat_message`` and streams what it yields to every client as
``cf_agent_use_chat_response`` frames, storing each chunk in a resumable
stream so a client that reconnects mid-turn is sent the replay; the
finished message is saved in the same transaction that settles the stream
(``.design/chat_engine.md`` §5).

Turns run one at a time from a queue. Each turn's consumption of
``on_chat_message`` runs in its own task, which a cancel (the client's stop,
`AIChatAgent.abort_request`, or cancelling the code awaiting
`AIChatAgent.save_messages`) cancels; the turn then ends as ``aborted``.

Not yet ported: client tools and approvals (12c); running turns as Tasks,
recovery, and the stall watchdog (12d).
"""

import asyncio
import copy
import inspect
import json
import logging
import secrets
import typing
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, cast, overload, override

from typing_extensions import TypeVar
from workers import Response

from .. import _ffi
from ..agent.agent import Agent
from ..core.timing import now_ms
from ..core.types import JSONValue
from ..lifecycle.capability import LifecycleCapability
from ..sessions.sessions import Sessions
from ..sessions.types import SessionMessage
from ..websockets.connection import Connection
from ._json import dumps, stable_dumps
from .accumulator import StreamAccumulator
from .builder import (
    BuilderScratch,
    apply_chunk_to_parts,
    apply_late_tool_input,
    is_replay_chunk,
    late_tool_input_forward_chunks,
)
from .chunks import UIMessageChunk
from .codec import (
    WireMessage,
    chunk_to_wire,
    message_from_wire,
    message_to_wire,
    part_from_wire,
    part_to_wire,
)
from .concurrency import SubmitConcurrencyController, SubmitDecision
from .errors import ChatStreamError
from .handshake import PreStreamTurns, ResumeHandshake
from .messages import (
    AnyToolPart,
    ChatMessageOptions,
    ClientToolSchema,
    UIMessage,
    UIMessagePart,
)
from .persistence import truncate_provider_tool_payloads
from .protocol import (
    CHAT_CLEAR,
    CHAT_MESSAGES,
    USE_CHAT_RESPONSE,
    Cancel,
    ChatRequest,
    ChatTurnOutcome,
    Clear,
    Messages,
    StreamResumeAck,
    StreamResumeRequest,
    origin_message_ids,
    parse_protocol_message,
    send_if_open,
    with_origin_message_ids,
)
from .reconciler import reconcile_messages, reconcile_orphan_partial
from .repair import repair_interrupted_tool_parts
from .resumable_stream import ResumableStream, create_chat_streams
from .terminal import TerminalRecord, clear_terminal, pending_terminal, record_terminal
from .tool_state import client_resolvable_tool_names, part_awaits_client_interaction
from .turn_queue import TurnQueue
from .types import (
    AIChatAgentOptions,
    ChatResponseResult,
    SaveMessagesResult,
)

if TYPE_CHECKING:
    from workers import Request

__all__ = ("AIChatAgent",)

_log = logging.getLogger("agents.chat")

S = TypeVar("S", bound=typing.Mapping[str, Any], default=dict[str, Any])

_CHUNK_CLASSES = typing.get_args(UIMessageChunk.__value__)
_FLUSH_AT_ONCE = frozenset(
    {"tool-output-available", "tool-output-error", "tool-output-denied"}
)
_VALID_ROLES = frozenset({"user", "assistant", "system"})
_SAVE_FAILED = "Failed to save the response."


@dataclass(slots=True)
class _ReplyResult:
    status: Literal["completed", "error", "aborted"]
    error: str | None = None
    finish_reason: str | None = None


@dataclass(slots=True, frozen=True)
class _Cutover:
    stream_id: str
    message_id: str


@dataclass(slots=True)
class _Turn:
    """One turn's consumption state."""

    request_id: str
    stream_id: str
    message: WireMessage
    continuation: bool
    scratch: BuilderScratch = field(default_factory=BuilderScratch)
    mode: Literal["chunks", "text"] | None = None
    finished: bool = False
    text_resumed: bool = False
    reasoning_resumed: bool = False
    finish_reason: str | None = None
    failure: Exception | None = None
    # (kind, id) of the text, reasoning, and tool-input parts the stream has
    # started (and not ended): a delta or end must follow its start.
    open_parts: set[tuple[str, str]] = field(default_factory=set)


class AIChatAgent(Agent[S]):
    """An agent that answers chat messages, streaming its replies to clients.

    Override `on_chat_message` to produce each reply. The transcript is
    ``self.messages``; change it with `persist_messages`, `save_messages`,
    and `delete_messages`. Settings go in ``options`` (`AIChatAgentOptions`).

    Attributes
    ----------
    sessions
        The Sessions the transcript is stored in (the default session).
    streams
        The Streams each turn's output is stored in.
    """

    options: AIChatAgentOptions = AIChatAgentOptions()

    def __init__(self, ctx: Any, env: Any) -> None:
        super().__init__(ctx, env)
        if not isinstance(type(self).options, AIChatAgentOptions):
            raise TypeError(
                f"{type(self).__name__}.options must be an AIChatAgentOptions"
            )
        self.sessions = self.use(Sessions())
        self.streams = self.use(create_chat_streams())
        self._session = self.sessions.session()
        # The transcript as stored (wire form), kept in step by the change
        # feed; typed views are decoded from it on demand and cached by dict.
        self._wire_messages: list[WireMessage] = []
        self._decoded: dict[int, tuple[WireMessage, UIMessage]] = {}
        self._session.mirror(
            get=lambda: self._wire_messages, set=self._replace_wire_messages
        )
        self._resumable: ResumableStream | None = None
        self._handshake_instance: ResumeHandshake | None = None
        self._turn_queue = TurnQueue()
        self._submit = SubmitConcurrencyController()
        self._pre_stream = PreStreamTurns()
        # Connections offered the stream that haven't ACKed: left out of
        # live broadcasts until their replay.
        self._pending_resume: set[str] = set()
        self._streaming_message: WireMessage | None = None
        self._pending_cutover: _Cutover | None = None
        self._held_terminal_frames: dict[
            str, list[tuple[dict[str, Any], tuple[str | Connection, ...]]]
        ] = {}
        self._request_origin_ids: dict[str, list[str]] = {}
        self._merge_start_by_epoch: dict[int, int] = {}
        self._pending_responses: list[ChatResponseResult] = []
        self._inside_response_hook = False
        # Request ids whose turn is running, and the tasks consuming
        # on_chat_message (cancelled to stop a turn).
        self._running_turns: set[str] = set()
        self._consuming: dict[str, asyncio.Task[Any]] = {}
        self._abort_requested: set[str] = set()
        self._last_client_tools: list[dict[str, Any]] | None = None
        self._last_body: dict[str, Any] | None = None
        self.use(_ChatHost(self))

    # The transcript

    @property
    def messages(self) -> Sequence[UIMessage]:
        """The conversation, oldest first (read-only).

        After a wake it holds the newest stored messages that fit
        ``options.hydration_byte_budget``. Don't mutate the messages: change
        the transcript with `persist_messages`, `save_messages`, or
        `delete_messages`.
        """
        wire = self._wire_messages
        if len(self._decoded) > 2 * len(wire) + 64:
            live = {id(message) for message in wire}
            self._decoded = {k: v for k, v in self._decoded.items() if k in live}
        return _MessagesView(tuple(wire), self._decoded)

    def _replace_wire_messages(self, messages: list[WireMessage]) -> None:
        self._wire_messages = messages

    # Hooks (override these)

    def on_chat_message(
        self, options: ChatMessageOptions
    ) -> AsyncIterator[UIMessageChunk] | AsyncIterator[str]:
        """Produce the reply to the conversation in ``self.messages``.

        Write it as an async generator yielding chunks (`agents.chat.chunks`)
        or plain ``str`` text, not both. The SDK sends ``Start`` and
        ``Finish`` if the stream doesn't. A delta or end chunk must follow
        its start (``TextStart``, ``ReasoningStart``, ``ToolInputStart`` with
        the same id), or the turn fails with ``TypeError``: clients reject
        it. An ``Error`` chunk or an exception ends the turn as failed. A
        cancelled turn gets ``CancelledError`` at its next ``await``. An
        ``async def`` returning an async iterator also works.
        """
        raise NotImplementedError(
            f"{type(self).__name__} received a chat message: override "
            "on_chat_message to answer it"
        )

    async def on_chat_response(self, result: ChatResponseResult) -> None:
        """Run after a turn's message is saved (any outcome but skipped).

        The turn queue is free by then, so this may call `save_messages`;
        turns started from here don't call it again.
        """

    def sanitize_message_for_persistence(self, message: UIMessage) -> UIMessage:
        """Return ``message`` as it should be stored (default: unchanged).

        Runs after the SDK's own cleanup, on every message saved.
        """
        return message

    def repair_interrupted_tool_part(self, part: AnyToolPart) -> UIMessagePart:
        """Return what replaces a tool call left without a result.

        Before each turn, a tool call that never settled (its turn was
        interrupted) is replaced, so the model isn't sent a call without a
        result. Default: an ``output-error`` part saying it was interrupted.
        Calls still waiting on the client are left alone.
        """
        return part_from_wire(_default_repair(part_to_wire(part)))

    # Writing the transcript

    async def persist_messages(
        self, messages: Sequence[UIMessage], *, exclude: Iterable[str | Connection] = ()
    ) -> None:
        """Store ``messages`` as the transcript, without starting a turn.

        ``messages`` is the intended transcript (typically
        ``[*self.messages, new]``). It's reconciled with what's stored, only
        changed messages are written, and stored messages left out aren't
        deleted. Every client but ``exclude`` is sent the result. May be
        overridden (call ``super()``).
        """
        await self._persist_wire(
            [message_to_wire(message) for message in messages], exclude=exclude
        )

    async def save_messages(
        self,
        messages: Sequence[UIMessage]
        | Callable[[list[UIMessage]], Awaitable[Sequence[UIMessage]]],
    ) -> SaveMessagesResult:
        """Store messages, then run a turn to answer them.

        Waits for any running turn. ``messages`` may be an ``async def``
        given the current messages when the turn starts. The turn uses the
        last request's client tools and body. Cancelling the caller stops
        the turn (its partial reply is still saved) and raises
        ``CancelledError``.
        """
        request_id = _new_id()
        client_tools, body = self._last_client_tools, self._last_body
        epoch = self._turn_queue.generation
        result = SaveMessagesResult(request_id=request_id, status="completed")

        async def turn() -> None:
            nonlocal result
            resolved = (
                messages
                if isinstance(messages, Sequence)
                else await messages(list(self.messages))
            )
            if self._turn_queue.generation != epoch:
                result = SaveMessagesResult(request_id=request_id, status="skipped")
                return
            await self._save([message_to_wire(m) for m in resolved])
            if self._turn_queue.generation != epoch:
                result = SaveMessagesResult(request_id=request_id, status="skipped")
                return
            reply = await self._programmatic_turn(
                request_id, client_tools, body, continuation=False
            )
            result = SaveMessagesResult(
                request_id=request_id, status=reply.status, error=reply.error
            )

        await self._run_exclusive_turn(request_id, turn, epoch=epoch)
        if self._turn_queue.generation != epoch and result.status == "completed":
            return SaveMessagesResult(request_id=request_id, status="skipped")
        return result

    async def continue_last_turn(
        self, *, body: dict[str, JSONValue] | None = None
    ) -> SaveMessagesResult:
        """Run a turn that continues the last assistant message.

        ``on_chat_message`` gets ``continuation=True`` and its output is
        appended to that message. ``body`` replaces the last request's body
        for this turn. ``skipped`` if there's no assistant message.
        Cancelling the caller stops the turn, as for `save_messages`.
        """
        if self._last_assistant() is None:
            return SaveMessagesResult(request_id="", status="skipped")
        request_id = _new_id()
        client_tools = self._last_client_tools
        resolved_body = body if body is not None else self._last_body
        epoch = self._turn_queue.generation
        result = SaveMessagesResult(request_id=request_id, status="completed")

        async def turn() -> None:
            nonlocal result
            if self._turn_queue.generation != epoch:
                result = SaveMessagesResult(request_id=request_id, status="skipped")
                return
            reply = await self._programmatic_turn(
                request_id, client_tools, resolved_body, continuation=True
            )
            result = SaveMessagesResult(
                request_id=request_id, status=reply.status, error=reply.error
            )

        await self._run_exclusive_turn(request_id, turn, epoch=epoch)
        if self._turn_queue.generation != epoch and result.status == "completed":
            return SaveMessagesResult(request_id=request_id, status="skipped")
        return result

    async def delete_messages(
        self, message_ids: Iterable[str], *, exclude: Iterable[str | Connection] = ()
    ) -> None:
        """Delete messages by id and send clients the result (unknown ids are ignored).

        Doesn't wait for a running turn; deleting the message a turn is
        still streaming has no lasting effect (stop the turn first).
        """
        ids = list(message_ids)
        if ids:
            await self._session.delete_messages(ids)
        self._broadcast_chat(
            {"messages": list(self._wire_messages), "type": CHAT_MESSAGES}, exclude
        )

    # Stopping turns

    def abort_request(self, request_id: str) -> None:
        """Stop a running turn, as a client's stop would (no-op if not running).

        The turn ends as ``aborted``; its partial reply is saved.
        """
        task = self._consuming.pop(request_id, None)
        if task is not None:
            task.cancel()
        elif request_id in self._running_turns:
            self._abort_requested.add(request_id)

    def abort_all_requests(self) -> None:
        """Stop every running turn (queued turns still run)."""
        for request_id in list(self._running_turns | self._consuming.keys()):
            self.abort_request(request_id)

    def reset_turn_state(self) -> None:
        """Stop the running turn and drop queued ones (done on a chat clear)."""
        self._merge_start_by_epoch.pop(self._turn_queue.generation, None)
        self._turn_queue.reset()
        self.abort_all_requests()
        self._submit.reset()
        self._pre_stream.release_awaiting()
        self._pre_stream.reset()
        self._pending_responses.clear()

    # Agent integration

    @override
    async def _after_connect_frames(self, connection: Connection) -> None:
        if self._stream.has_active_stream():
            self._handshake.notify_stream_resuming(connection)
        else:
            # A turn accepted but not yet streaming: told to keep waiting.
            self._pre_stream.park(connection)

    @override
    async def _message_locally(
        self, connection: Connection, message: str | bytes
    ) -> None:
        event = parse_protocol_message(message) if isinstance(message, str) else None
        if event is None or (
            isinstance(event, ChatRequest) and event.init.get("method") != "POST"
        ):
            await super()._message_locally(connection, message)
            return
        try:
            match event:
                case ChatRequest():
                    await self._on_chat_request(connection, event)
                case Clear():
                    await self._on_clear(connection)
                case Messages(messages=messages):
                    await self._save(list(messages), exclude=[connection.id])
                case Cancel(id=request_id):
                    self.abort_request(request_id)
                    self._emit("message:cancel", {"requestId": request_id})
                case StreamResumeRequest(probe_id=probe_id):
                    await self._handshake.handle_resume_request(connection, probe_id)
                case StreamResumeAck(id=request_id):
                    await self._handshake.handle_resume_ack(connection, request_id)
                case _:  # tool results and approvals (12c)
                    await super()._message_locally(connection, message)
        except Exception as error:
            await self.on_error(None, error)

    @override
    async def _close_locally(
        self, connection: Connection, code: int, reason: str, was_clean: bool
    ) -> None:
        self._pending_resume.discard(connection.id)
        self._pre_stream.release(connection.id)
        await super()._close_locally(connection, code, reason, was_clean)

    # Frames

    async def _on_chat_request(
        self, connection: Connection, event: ChatRequest
    ) -> None:
        raw = event.init.get("body")
        if not raw:
            _log.warning("Ignoring a chat request with an empty body")
            return
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            _log.warning("Ignoring a chat request whose body isn't JSON")
            return
        if not isinstance(parsed, dict) or not isinstance(parsed.get("messages"), list):
            _log.warning("Ignoring a chat request without messages")
            return
        messages: list[WireMessage] = parsed.pop("messages")
        client_tools = parsed.pop("clientTools", None) or None
        trigger = parsed.pop("trigger", None)
        body = parsed or None
        request_id = event.id
        epoch = self._turn_queue.generation
        origin = origin_message_ids(messages)
        if origin:
            self._request_origin_ids[request_id] = origin
        decision = self._submit_decision(trigger != "regenerate-message")
        if decision.action == "drop":
            # The client rolls its optimistic send back to this transcript.
            send_if_open(
                connection,
                {"messages": self._messages_for_client(), "type": CHAT_MESSAGES},
            )
            self._complete_skipped(connection, request_id)
            self._request_origin_ids.pop(request_id, None)
            return
        # A new turn supersedes a stored terminal error.
        await clear_terminal(self.ctx.storage)
        self._pre_stream.begin(request_id)
        try:
            release = self._submit.begin_enqueue()
            try:
                # Saved and sent to the other tabs before queueing, so
                # overlapping sends see the whole transcript.
                self._broadcast_chat(
                    {"messages": messages, "type": CHAT_MESSAGES}, [connection.id]
                )
                await self._save(
                    messages, exclude=[connection.id], delete_stale_rows=True
                )
                if decision.strategy == "merge":
                    await self._merge_queued_user_messages(epoch)
            finally:
                release()

            async def turn() -> None:
                if not await self._still_admitted(decision, epoch):
                    self._complete_skipped(connection, request_id)
                    return
                self._set_request_context(client_tools, body)
                self._emit("message:request", {})
                try:
                    await self._chat_turn(
                        request_id,
                        continuation=False,
                        client_tools=client_tools,
                        body=body,
                        exclude=[connection.id],
                    )
                finally:
                    self._settle_pre_stream(request_id)

            await self._run_exclusive_turn(
                request_id,
                turn,
                epoch=epoch,
                on_stale=lambda: self._complete_skipped(connection, request_id),
            )
        finally:
            # Settled however the turn ended (idempotent).
            self._settle_pre_stream(request_id)
            self._request_origin_ids.pop(request_id, None)

    async def _still_admitted(self, decision: SubmitDecision, epoch: int) -> bool:
        # Re-checked at the head of the queue: a newer send may have
        # superseded this one (latest, merge, debounce) or a clear dropped it.
        if self._submit.is_superseded(decision.submit_sequence):
            return False
        if decision.debounce_until is not None:
            await self._submit.wait_until(decision.debounce_until)
            if self._turn_queue.generation != epoch or self._submit.is_superseded(
                decision.submit_sequence
            ):
                return False
        if decision.strategy == "merge":
            # More overlapping sends may have been saved while this waited.
            await self._merge_queued_user_messages(epoch)
            if self._turn_queue.generation != epoch or self._submit.is_superseded(
                decision.submit_sequence
            ):
                return False
        return True

    async def _on_clear(self, connection: Connection) -> None:
        self.reset_turn_state()
        await self._session.clear_messages()
        await clear_terminal(self.ctx.storage)
        self._stream.clear_all()
        self._pending_resume.clear()
        self._last_client_tools = None
        self._last_body = None
        self._persist_request_context()
        self._broadcast_chat({"type": CHAT_CLEAR}, [connection.id])
        self._emit("message:clear", {})

    def _complete_skipped(self, connection: Connection, request_id: str) -> None:
        send_if_open(
            connection,
            self._with_origin_ids(
                {
                    "body": "",
                    "done": True,
                    "id": request_id,
                    "type": USE_CHAT_RESPONSE,
                    "outcome": "skipped",
                }
            ),
        )
        # A newer turn was admitted (or the queue moved on): parked clients
        # stay parked for it.
        self._settle_pre_stream(request_id, release_parked=False)

    def _settle_pre_stream(
        self, request_id: str, *, release_parked: bool = True
    ) -> None:
        idle = self._pre_stream.settle(request_id)
        if release_parked and idle and not self._stream.has_active_stream():
            self._pre_stream.release_awaiting()

    def _submit_decision(self, is_submit_message: bool) -> SubmitDecision:
        decision = self._submit.decide(
            type(self).options.message_concurrency,
            is_submit_message=is_submit_message,
            queued_turns=self._turn_queue.queued_count(),
        )
        if decision.strategy == "merge":
            self._merge_start_by_epoch.setdefault(
                self._turn_queue.generation, len(self._wire_messages)
            )
        return decision

    async def _merge_queued_user_messages(self, epoch: int) -> None:
        start = self._merge_start_by_epoch.get(epoch)
        if start is None:
            return
        messages = self._wire_messages
        end = start
        while end < len(messages) and messages[end].get("role") == "user":
            end += 1
        if end == start and start < len(messages):
            _log.warning(
                "merge: expected user messages at index %d but found role=%r; "
                "skipping merge",
                start,
                messages[start].get("role"),
            )
        if end - start < 2:
            return
        merged = _merge_user_messages(messages[start:end])
        await self._save(
            [*messages[:start], merged, *messages[end:]], delete_stale_rows=True
        )

    # Turns

    async def _run_exclusive_turn(
        self,
        request_id: str,
        fn: Callable[[], Awaitable[None]],
        *,
        epoch: int,
        on_stale: Callable[[], None] | None = None,
    ) -> None:
        try:
            result = await self._turn_queue.enqueue(request_id, fn, generation=epoch)
        finally:
            if self._turn_queue.queued_count(epoch) == 0:
                self._merge_start_by_epoch.pop(epoch, None)
            if self._pending_responses and not self._inside_response_hook:
                self._inside_response_hook = True
                try:
                    await self.keep_alive_while(self._drain_chat_responses)
                finally:
                    self._inside_response_hook = False
        if result.status == "stale" and on_stale is not None:
            on_stale()

    async def _drain_chat_responses(self) -> None:
        storage = self.ctx.storage
        while self._pending_responses:
            result = self._pending_responses.pop(0)
            if result.status == "error":
                # A client that missed it learns of it when it reconnects.
                await record_terminal(
                    storage,
                    TerminalRecord(
                        request_id=result.request_id,
                        body=result.error or "The assistant encountered an error.",
                        message_ids=self._origin_ids_for(result.request_id),
                    ),
                )
            else:
                await clear_terminal(storage)
            try:
                await self.on_chat_response(result)
            except Exception as error:
                _log.exception("on_chat_response failed")
                await self._report_error(None, error)

    async def _programmatic_turn(
        self,
        request_id: str,
        client_tools: list[dict[str, Any]] | None,
        body: dict[str, Any] | None,
        *,
        continuation: bool,
    ) -> _ReplyResult:
        self._set_request_context(client_tools, body)
        return await self._chat_turn(
            request_id,
            continuation=continuation,
            client_tools=client_tools,
            body=body,
            exclude=(),
        )

    async def _chat_turn(
        self,
        request_id: str,
        *,
        continuation: bool,
        client_tools: list[dict[str, Any]] | None,
        body: dict[str, Any] | None,
        exclude: Sequence[str | Connection],
    ) -> _ReplyResult:
        """Run one turn: on_chat_message's reply, streamed and saved.

        The reply runs in its own task. Cancelling the caller stops it,
        waits for it to settle, and re-raises.
        """
        options = ChatMessageOptions(
            request_id=request_id,
            client_tools=tuple(
                _client_tool_schema(tool) for tool in client_tools or ()
            ),
            body=body,
            continuation=continuation,
        )
        self._running_turns.add(request_id)
        task = asyncio.ensure_future(
            self.keep_alive_while(partial(self._reply, options, tuple(exclude)))
        )
        cancelled = False
        try:
            while not task.done():
                try:
                    await asyncio.wait({task})
                except asyncio.CancelledError:
                    cancelled = True
                    self.abort_request(request_id)
        finally:
            self._running_turns.discard(request_id)
            self._abort_requested.discard(request_id)
        if cancelled:
            raise asyncio.CancelledError
        try:
            return task.result()
        except Exception as error:
            # The turn machinery itself failed (e.g. saving the message).
            await self._report_error(None, error)
            return _ReplyResult("error", str(error))

    async def _reply(
        self, options: ChatMessageOptions, exclude: tuple[str | Connection, ...]
    ) -> _ReplyResult:
        request_id = options.request_id
        continuation = options.continuation
        try:
            await self._repair_interrupted_tools(continuation=continuation)
        except Exception as error:
            # Failed before streaming: the client still gets the error.
            self._broadcast_chat(
                _terminal_error_frame(request_id, str(error), continuation)
            )
            self._emit("message:error", {"error": str(error)})
            await self._report_error(None, error)
            return _ReplyResult("error", str(error))
        message = self._new_assistant_message(continuation)
        turn = _Turn(
            request_id=request_id,
            stream_id=self._start_stream(request_id, message["id"], continuation),
            message=message,
            continuation=continuation,
        )
        self._streaming_message = message
        self._held_terminal_frames[request_id] = []
        persisted = False
        try:
            try:
                result = await self._consume(turn, options)
            finally:
                self._streaming_message = None
            if result.status != "error":
                self._emit("message:response", {})
            stream = self._stream
            # The cutover: the save that carries this message settles the
            # stream and deletes its rows in the same transaction.
            self._pending_cutover = (
                _Cutover(turn.stream_id, message["id"])
                if stream.pending_cutover_id == turn.stream_id
                else None
            )
            if message["parts"]:
                await self._save(
                    _with_message(self._wire_messages, message), exclude=exclude
                )
            # Nothing to save, or a save that didn't reach the cutover.
            self._pending_cutover = None
            stream.finalize_pending()
            persisted = True
            self._release_terminal_frames(request_id)
            self._pending_responses.append(
                ChatResponseResult(
                    message=message_from_wire(message),
                    request_id=request_id,
                    continuation=continuation,
                    status=result.status,
                    error=result.error,
                )
            )
            if turn.failure is not None:
                await self._report_error(None, turn.failure)
            return result
        finally:
            self._pending_cutover = None
            self._stream.finalize_pending()
            self._release_terminal_frames(
                request_id, None if persisted else _SAVE_FAILED
            )

    async def _consume(self, turn: _Turn, options: ChatMessageOptions) -> _ReplyResult:
        """Stream on_chat_message's output: the turn's one cancellable stretch."""
        request_id = turn.request_id
        current = asyncio.current_task()
        assert current is not None
        if request_id in self._abort_requested:
            return self._end_aborted(turn)
        self._consuming[request_id] = current
        source: Any = None
        try:
            source = self.on_chat_message(options)
            if inspect.isawaitable(source):
                source = await source
            async for item in source:
                ended = self._handle_item(turn, item)
                if ended is not None:
                    return ended
            self._consuming.pop(request_id, None)
            if turn.mode == "text":
                self._apply_and_send(turn, {"type": "text-end", "id": request_id})
            if turn.mode is not None and not turn.finished:
                self._apply_and_send(turn, {"type": "finish"})
            self._finish_stream(turn.stream_id)
            self._broadcast_chat(_done_frame(request_id, turn.continuation))
            return _ReplyResult("completed", finish_reason=turn.finish_reason)
        except asyncio.CancelledError:
            # Only abort_request cancels this task (a cancelled caller is
            # turned into one): the turn ends as aborted.
            self._consuming.pop(request_id, None)
            current.uncancel()
            return self._end_aborted(turn)
        except Exception as error:
            self._consuming.pop(request_id, None)
            turn.failure = error
            self._mark_stream_error(turn.stream_id)
            self._broadcast_chat(
                _terminal_error_frame(request_id, str(error), turn.continuation)
            )
            self._emit("message:error", {"error": str(error)})
            return _ReplyResult("error", str(error))
        finally:
            self._consuming.pop(request_id, None)
            if inspect.isasyncgen(source):
                await source.aclose()

    def _end_aborted(self, turn: _Turn) -> _ReplyResult:
        if turn.mode == "text":
            self._apply_and_send(turn, {"type": "text-end", "id": turn.request_id})
        self._finish_stream(turn.stream_id, "aborted")
        self._broadcast_chat(_done_frame(turn.request_id, turn.continuation, "aborted"))
        return _ReplyResult("aborted")

    def _handle_item(self, turn: _Turn, item: Any) -> _ReplyResult | None:
        if isinstance(item, str):
            if turn.mode == "chunks":
                raise TypeError("on_chat_message yielded a str after chunks")
            if turn.mode is None:
                turn.mode = "text"
                self._apply_and_send(turn, {"type": "start"})
                self._apply_and_send(
                    turn, {"type": "text-start", "id": turn.request_id}
                )
            if item:
                return self._apply_and_send(
                    turn, {"type": "text-delta", "id": turn.request_id, "delta": item}
                )
            return None
        if not isinstance(item, _CHUNK_CLASSES):
            raise TypeError(
                "on_chat_message must yield chunks (agents.chat.chunks) or str, "
                f"not {type(item).__name__}"
            )
        if turn.mode == "text":
            raise TypeError("on_chat_message yielded a chunk after str text")
        data = chunk_to_wire(cast(UIMessageChunk, item))
        if turn.mode is None:
            turn.mode = "chunks"
            if data["type"] != "start":
                self._apply_and_send(turn, {"type": "start"})
        elif data["type"] == "start":
            raise TypeError("a Start chunk must come first")
        if data["type"] == "finish":
            turn.finished = True
        _check_order(turn, data, type(item).__name__)
        return self._apply_and_send(turn, data)

    def _apply_and_send(self, turn: _Turn, data: dict[str, Any]) -> _ReplyResult | None:
        """Apply one wire chunk to the message, then store and broadcast it.

        Port of upstream ``_streamSSEReply``'s loop body. Returns the turn's
        result when the chunk ends it (an ``error`` chunk).
        """
        message = turn.message
        parts: list[dict[str, Any]] = message["parts"]
        kind = data.get("type")
        skip_apply = False
        if turn.continuation:
            # The first text-start / reasoning-start merges into a part the
            # interrupted message was still streaming.
            if not turn.text_resumed and kind == "text-start":
                last_text = next(
                    (p for p in reversed(parts) if p.get("type") == "text"), None
                )
                if last_text is not None and last_text.get("state") == "streaming":
                    turn.text_resumed = True
                    return None
            if not turn.reasoning_resumed and kind == "reasoning-start":
                last = next(
                    (
                        p
                        for p in reversed(parts)
                        if p.get("type") in ("text", "reasoning")
                    ),
                    None,
                )
                turn.reasoning_resumed = (
                    last is not None and last.get("state") == "streaming"
                )
                # Still sent: the client needs reasoning-start before deltas.
                skip_apply = turn.reasoning_resumed
        if is_replay_chunk(parts, data):
            # A provider resending an earlier tool call would move the
            # client's part backwards: dropped, except late input for a
            # pending approval, sent with the approval request again.
            apply_late_tool_input(parts, data, turn.scratch)
            for forwarded in late_tool_input_forward_chunks(parts, data):
                self._send_chunk(turn, forwarded)
            return None
        handled = skip_apply or apply_chunk_to_parts(parts, data, turn.scratch)
        if not handled:
            match kind:
                case "start":
                    if data.get("messageId") is not None and not turn.continuation:
                        message["id"] = data["messageId"]
                    _merge_metadata(message, data.get("messageMetadata"))
                case "finish" | "message-metadata":
                    _merge_metadata(message, data.get("messageMetadata"))
                case "error":
                    return self._end_with_error_chunk(turn, data)
        event = data
        if kind == "start":
            if turn.continuation:
                event = {k: v for k, v in data.items() if k != "messageId"}
            elif data.get("messageId") is None:
                # The client builds the live message under the id it's saved as.
                event = {**data, "messageId": message["id"]}
        elif kind == "finish" and "finishReason" in data:
            turn.finish_reason = data["finishReason"]
            event = {k: v for k, v in data.items() if k != "finishReason"}
            event.update(
                type="finish", messageMetadata={"finishReason": turn.finish_reason}
            )
        self._send_chunk(turn, event)
        return None

    def _end_with_error_chunk(self, turn: _Turn, data: dict[str, Any]) -> _ReplyResult:
        error_text = data.get("errorText")
        if error_text is None:
            error_text = dumps({"type": "error"})
        frame: dict[str, Any] = {
            "error": True,
            "body": error_text,
            "done": False,
            "id": turn.request_id,
            "type": USE_CHAT_RESPONSE,
        }
        if turn.continuation:
            frame["continuation"] = True
        self._broadcast_chat(frame)
        self._mark_stream_error(turn.stream_id)
        self._emit("message:error", {"error": error_text})
        self._broadcast_chat(_done_frame(turn.request_id, turn.continuation, "error"))
        turn.failure = ChatStreamError(error_text)
        return _ReplyResult("error", error_text)

    def _send_chunk(self, turn: _Turn, event: dict[str, Any]) -> None:
        body = dumps(event)
        stream = self._stream
        seq = stream.store_chunk(turn.stream_id, body)
        if event.get("type") in _FLUSH_AT_ONCE:
            # A settled tool result is a finished side effect: stored now,
            # not with the rest of its segment.
            stream.flush_buffer()
        frame: dict[str, Any] = {
            "body": body,
            "done": False,
            "id": turn.request_id,
            "type": USE_CHAT_RESPONSE,
        }
        if seq is not None:
            frame["seq"] = seq
        if turn.continuation:
            frame["continuation"] = True
        self._broadcast_chat(frame)

    # Streams

    @property
    def _stream(self) -> ResumableStream:
        if self._resumable is None:
            raise RuntimeError("AIChatAgent used before it started")
        return self._resumable

    @property
    def _handshake(self) -> ResumeHandshake:
        if self._handshake_instance is None:
            self._handshake_instance = ResumeHandshake(
                stream=self._stream,
                pre_stream=self._pre_stream,
                pending_resume=self._pending_resume,
                pending_terminal=partial(pending_terminal, self.ctx.storage),
                persist_orphaned_stream=self._persist_orphaned_stream,
                holds_terminal_frames=self._held_terminal_frames.__contains__,
            )
        return self._handshake_instance

    def _start_stream(
        self, request_id: str, message_id: str, continuation: bool
    ) -> str:
        stream_id = self._stream.start(
            request_id,
            message_id=message_id,
            continuation=continuation,
            origin_message_ids=self._request_origin_ids.get(request_id),
        )
        # Clients that reconnected before the first chunk are offered it now.
        self._pre_stream.flush_on_stream_start(self._handshake.notify_stream_resuming)
        return stream_id

    def _finish_stream(
        self, stream_id: str, outcome: ChatTurnOutcome | None = None
    ) -> None:
        self._stream.finish(stream_id, outcome)
        self._pending_resume.clear()

    def _mark_stream_error(self, stream_id: str) -> None:
        self._stream.mark_error(stream_id)
        self._pending_resume.clear()

    async def _persist_orphaned_stream(self, stream_id: str) -> None:
        """Save the partial message of a stream a dead isolate left behind."""
        bodies = self._stream.stream_chunks(stream_id)
        if not bodies:
            return
        fallback_id = _assistant_id()
        accumulator = StreamAccumulator(message_id=fallback_id)
        for body in bodies:
            try:
                chunk = json.loads(body)
            except json.JSONDecodeError:
                continue
            accumulator.apply_chunk(chunk)
        if not accumulator.parts:
            return
        message = accumulator.to_message()
        if message["id"] == fallback_id:
            # The id the stream was producing; for streams that didn't
            # record one, the last assistant message.
            stored_id = self._stream.get_stream_message_id(stream_id)
            last = self._last_assistant()
            if stored_id is not None:
                message["id"] = stored_id
            elif last is not None:
                message["id"] = last["id"]
        existing = next(
            (m for m in self._wire_messages if m["id"] == message["id"]), None
        )
        if existing is not None:
            message = reconcile_orphan_partial(existing, message)
        await self._save(_with_message(self._wire_messages, message))

    # Saving

    @property
    def _persist_overridden(self) -> bool:
        return type(self).persist_messages is not AIChatAgent.persist_messages

    async def _save(
        self,
        messages: list[WireMessage],
        *,
        exclude: Iterable[str | Connection] = (),
        delete_stale_rows: bool = False,
    ) -> None:
        """Persist wire messages: through an override of persist_messages if any."""
        if not self._persist_overridden:
            await self._persist_wire(
                messages, exclude=exclude, delete_stale_rows=delete_stale_rows
            )
            return
        prior = list(self._wire_messages)
        await self.persist_messages(
            [message_from_wire(message) for message in messages], exclude=exclude
        )
        if delete_stale_rows:
            await self._delete_stale_rows(
                reconcile_messages(messages, prior, self._sanitize), prior
            )

    async def _persist_wire(
        self,
        messages: list[WireMessage],
        *,
        exclude: Iterable[str | Connection] = (),
        delete_stale_rows: bool = False,
    ) -> None:
        # A snapshot: the mirrored list changes as the writes land.
        prior = list(self._wire_messages)
        prior_by_id = {message["id"]: message for message in prior}
        merged = reconcile_messages(messages, prior, self._sanitize)
        to_write: list[WireMessage] = []
        for message in merged:
            resolved = self._sanitize(message)
            stored = prior_by_id.get(resolved["id"])
            # Compared in canonical form, so key order alone isn't a change.
            if stored is not None and (
                stored is resolved or stable_dumps(stored) == stable_dumps(resolved)
            ):
                continue
            to_write.append(resolved)
        cutover = self._pending_cutover
        if cutover is not None and any(m["id"] == cutover.message_id for m in merged):
            self._pending_cutover = None
            afters: list[Callable[[], Awaitable[None]]] = []

            def commit() -> None:
                for message in to_write:
                    afters.append(
                        self._session._upsert_sync(cast(SessionMessage, message))[1]
                    )

            try:
                self._stream.cutover(cutover.stream_id, commit)
            except BaseException:
                # Rolled back: forget what Sessions' caches counted.
                self._session._abandon()
                raise
            for after in afters:
                await after()
        else:
            for message in to_write:
                await self._session.upsert_message(cast(SessionMessage, message))
        if delete_stale_rows:
            await self._delete_stale_rows(merged, prior)
        limit = type(self).options.max_persisted_messages
        if limit is not None:
            # Counts stored rows, not just the hydrated window.
            stats = await self._session.get_history_row_stats()
            excess = len(stats) - limit
            if excess > 0:
                await self._session.delete_messages([row.id for row in stats[:excess]])
        self._broadcast_chat({"messages": merged, "type": CHAT_MESSAGES}, exclude)

    async def _delete_stale_rows(
        self, merged: Sequence[WireMessage], prior: Sequence[WireMessage]
    ) -> None:
        # A regenerate sends a strict subset of the stored transcript: the
        # stored messages it left out (the replaced reply) are deleted.
        server_ids = {message["id"] for message in prior}
        if not all(message["id"] in server_ids for message in merged):
            return
        keep = {message["id"] for message in merged}
        stale = [m["id"] for m in self._wire_messages if m["id"] not in keep]
        if stale:
            await self._session.delete_messages(stale)

    def _sanitize(self, message: WireMessage) -> WireMessage:
        sanitized = {
            **message,
            "parts": [truncate_provider_tool_payloads(p) for p in message["parts"]],
        }
        if (
            type(self).sanitize_message_for_persistence
            is AIChatAgent.sanitize_message_for_persistence
        ):
            return sanitized
        return message_to_wire(
            self.sanitize_message_for_persistence(message_from_wire(sanitized))
        )

    async def _repair_interrupted_tools(self, *, continuation: bool) -> None:
        # Settle tool calls an interrupted turn left without a result, so the
        # model isn't sent a call it never got an answer to.
        client_resolvable = client_resolvable_tool_names(self._last_client_tools)
        repaired = repair_interrupted_tool_parts(
            self._wire_messages,
            repair_part=self._repair_part,
            should_repair=lambda p: (
                not part_awaits_client_interaction(p, client_resolvable)
            ),
            repair_approval_responded=not continuation,
        )
        if repaired.removed_tool_calls or repaired.normalized_inputs:
            await self._save(repaired.messages)

    def _repair_part(self, part: dict[str, Any]) -> dict[str, Any]:
        if (
            type(self).repair_interrupted_tool_part
            is AIChatAgent.repair_interrupted_tool_part
        ):
            return _default_repair(part)
        typed = part_from_wire(part)
        return part_to_wire(self.repair_interrupted_tool_part(cast(AnyToolPart, typed)))

    # Broadcasting

    def _broadcast_chat(
        self, frame: dict[str, Any], exclude: Iterable[str | Connection] = ()
    ) -> None:
        frame = self._with_origin_ids(frame)
        excluded = tuple(exclude)
        if frame.get("type") == USE_CHAT_RESPONSE and (
            frame.get("done") or frame.get("error")
        ):
            # Held until the turn's message is saved: a client that saw the
            # turn end would otherwise have a later send replaced by it.
            held = self._held_terminal_frames.get(frame["id"])
            if held is not None:
                held.append((frame, excluded))
                return
        self.broadcast(dumps(frame), exclude=(*excluded, *self._pending_resume))

    def _release_terminal_frames(
        self, request_id: str, error_text: str | None = None
    ) -> None:
        held = self._held_terminal_frames.pop(request_id, None)
        if held is None:
            return
        # A save that failed turns the outcome into an error, unless the
        # turn already reported one.
        failed = error_text is not None and not any(
            frame.get("error") for frame, _ in held
        )
        for frame, excluded in held:
            if failed:
                frame = {**frame, "body": error_text, "error": True}
                if frame.get("done"):
                    frame["outcome"] = "error"
            self._broadcast_chat(frame, excluded)

    def _with_origin_ids(self, frame: dict[str, Any]) -> dict[str, Any]:
        if frame.get("type") != USE_CHAT_RESPONSE or not (
            frame.get("done") or frame.get("error")
        ):
            return frame
        return with_origin_message_ids(frame, self._origin_ids_for(frame["id"]))

    def _origin_ids_for(self, request_id: str) -> Sequence[str] | None:
        ids = self._request_origin_ids.get(request_id)
        return (
            ids if ids is not None else self._stream.get_origin_message_ids(request_id)
        )

    def _messages_for_client(self) -> list[WireMessage]:
        # The transcript, with the message being streamed in its place.
        streaming = self._streaming_message
        if streaming is None or not streaming["parts"]:
            return list(self._wire_messages)
        return _with_message(self._wire_messages, streaming)

    # Request context (survives hibernation for programmatic turns)

    def _set_request_context(
        self, client_tools: list[dict[str, Any]] | None, body: dict[str, Any] | None
    ) -> None:
        self._last_client_tools = client_tools or None
        self._last_body = body or None
        self._persist_request_context()

    def _persist_request_context(self) -> None:
        for key, value in (
            ("lastBody", self._last_body),
            ("lastClientTools", self._last_client_tools),
        ):
            if value:
                self.sql(
                    "INSERT OR REPLACE INTO cf_ai_chat_request_context (key, value)"
                    " VALUES (?, ?)",
                    key,
                    dumps(value),
                )
            else:
                self.sql("DELETE FROM cf_ai_chat_request_context WHERE key = ?", key)

    def _restore_request_context(self) -> None:
        for row in self.sql("SELECT key, value FROM cf_ai_chat_request_context"):
            try:
                value = json.loads(row["value"])
            except json.JSONDecodeError:
                continue  # a corrupt row is overwritten by the next request
            if row["key"] == "lastBody":
                self._last_body = value
            elif row["key"] == "lastClientTools":
                self._last_client_tools = value

    # Helpers

    def _last_assistant(self) -> WireMessage | None:
        return next(
            (m for m in reversed(self._wire_messages) if m.get("role") == "assistant"),
            None,
        )

    def _new_assistant_message(self, continuation: bool) -> WireMessage:
        if continuation:
            last = self._last_assistant()
            if last is not None:
                return copy.deepcopy(last)
        return {"id": _assistant_id(), "role": "assistant", "parts": []}

    async def _hydrate(self) -> None:
        budget = type(self).options.hydration_byte_budget
        stored = (
            (await self._session.get_recent_history(budget)).messages
            if budget is not None and budget > 0
            else await self._session.get_history()
        )
        messages: list[WireMessage] = []
        for message in stored:
            if not message["id"] or message["role"] not in _VALID_ROLES:
                _log.warning("Skipping invalid stored message %r", message.get("id"))
                continue
            messages.append(cast(WireMessage, message))
        self._wire_messages = messages

    async def _messages_json(self) -> AsyncIterator[str]:
        # The whole stored transcript as one JSON array, a batch at a time.
        yield "["
        first = True
        async for batch in self._session.history_batches():
            if batch:
                text = ",".join(dumps(message) for message in batch)
                yield text if first else "," + text
                first = False
        yield "]"


class _ChatHost(LifecycleCapability):
    """Chat's startup and its ``get-messages`` route."""

    def __init__(self, agent: AIChatAgent[Any]) -> None:
        super().__init__("chat")
        self._agent = agent

    @override
    async def on_start(self) -> None:
        """Restore the request context and active stream, then load the transcript."""
        agent = self._agent
        self.lifecycle.sql(
            "CREATE TABLE IF NOT EXISTS cf_ai_chat_request_context"
            " (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        agent._restore_request_context()
        if agent._resumable is not None:
            agent._resumable.dispose()
        agent._resumable = ResumableStream(agent.streams)
        agent._handshake_instance = None
        await agent._hydrate()

    @override
    async def on_request(self, request: "Request") -> Response | None:
        """Serve ``…/get-messages``: the stored transcript, streamed as JSON."""
        path = request.url.split("?", 1)[0].split("#", 1)[0]
        if path.rsplit("/", 1)[-1] != "get-messages":
            return None
        return _ffi.streaming_response(
            self._agent._messages_json(),
            headers={"content-type": "application/json"},
        )


class _MessagesView(Sequence[UIMessage]):
    """``self.messages``: typed messages decoded on access, cached per dict."""

    __slots__ = ("_cache", "_wire")

    def __init__(
        self,
        wire: tuple[WireMessage, ...],
        cache: dict[int, tuple[WireMessage, UIMessage]],
    ) -> None:
        self._wire = wire
        self._cache = cache

    def __len__(self) -> int:
        return len(self._wire)

    @overload
    def __getitem__(self, index: int) -> UIMessage: ...
    @overload
    def __getitem__(self, index: slice) -> Sequence[UIMessage]: ...
    def __getitem__(self, index: int | slice) -> UIMessage | Sequence[UIMessage]:
        if isinstance(index, slice):
            return [self._decode(message) for message in self._wire[index]]
        return self._decode(self._wire[index])

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self)!r})"

    def _decode(self, wire: WireMessage) -> UIMessage:
        entry = self._cache.get(id(wire))
        if entry is not None and entry[0] is wire:
            return entry[1]
        typed = message_from_wire(wire)
        self._cache[id(wire)] = (wire, typed)
        return typed


# Module helpers


def _new_id() -> str:
    return secrets.token_urlsafe(16)


def _assistant_id() -> str:
    return f"assistant_{now_ms()}_{secrets.token_hex(5)[:9]}"


def _client_tool_schema(tool: Any) -> ClientToolSchema:
    return ClientToolSchema(
        name=tool.get("name", ""),
        description=tool.get("description"),
        parameters=tool.get("parameters"),
    )


def _default_repair(part: dict[str, Any]) -> dict[str, Any]:
    error_text = (
        "The tool call was approved but did not run before the next turn started."
        if part.get("state") == "approval-responded"
        else "The tool call was interrupted before a result was recorded."
    )
    return {**part, "state": "output-error", "errorText": error_text}


def _with_message(
    messages: Sequence[WireMessage], message: WireMessage
) -> list[WireMessage]:
    """Return ``messages`` with ``message`` replacing its id's entry, or appended."""
    updated = list(messages)
    for index, existing in enumerate(updated):
        if existing["id"] == message["id"]:
            updated[index] = message
            return updated
    updated.append(message)
    return updated


def _merge_metadata(message: WireMessage, metadata: Any) -> None:
    if metadata is None:
        return
    current = message.get("metadata")
    message["metadata"] = (
        {**current, **metadata}
        if isinstance(current, dict) and isinstance(metadata, dict)
        else metadata
    )


def _merge_user_messages(messages: Sequence[WireMessage]) -> WireMessage:
    # Overlapping sends under "merge": one user message, texts joined by a
    # blank line, other parts kept in order; the last message's id and
    # metadata.
    parts = list(messages[0]["parts"])
    for message in messages[1:]:
        _append_text(parts, "\n\n")
        for part in message["parts"]:
            if part.get("type") == "text":
                _append_text(parts, part.get("text", ""))
            else:
                parts.append(part)
    return {**messages[-1], "parts": parts}


def _append_text(parts: list[dict[str, Any]], text: str) -> None:
    if not text:
        return
    if parts and parts[-1].get("type") == "text":
        parts[-1] = {**parts[-1], "text": parts[-1]["text"] + text}
    else:
        parts.append({"type": "text", "text": text})


# The AI SDK client applies a delta or end only to a part its start opened.
# Starts: chunk type -> (part kind, id field).
_OPENED_BY = {
    "text-start": ("text", "id"),
    "reasoning-start": ("reasoning", "id"),
    "tool-input-start": ("tool-input", "toolCallId"),
}
# Deltas and ends: chunk type -> (part kind, id field, the start's class,
# whether it closes the part).
_NEEDS_START = {
    "text-delta": ("text", "id", "TextStart", False),
    "text-end": ("text", "id", "TextStart", True),
    "reasoning-delta": ("reasoning", "id", "ReasoningStart", False),
    "reasoning-end": ("reasoning", "id", "ReasoningStart", True),
    "tool-input-delta": ("tool-input", "toolCallId", "ToolInputStart", False),
}


def _check_order(turn: _Turn, data: dict[str, Any], chunk_name: str) -> None:
    """Raise if a delta or end chunk has no open start in this stream.

    The AI SDK client rejects such a chunk and drops the whole reply, so the
    turn fails instead, saying what was wrong.
    """
    kind = data["type"]
    if kind in _OPENED_BY:
        part, id_field = _OPENED_BY[kind]
        turn.open_parts.add((part, data[id_field]))
        return
    if kind not in _NEEDS_START:
        return
    part, id_field, start, ends = _NEEDS_START[kind]
    key = (part, data[id_field])
    if key not in turn.open_parts:
        raise TypeError(
            f"on_chat_message yielded {chunk_name}({id_field}={key[1]!r}) "
            f"without a {start} for it first"
        )
    if ends:
        turn.open_parts.discard(key)


def _done_frame(
    request_id: str, continuation: bool, outcome: ChatTurnOutcome | None = None
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "body": "",
        "done": True,
        "id": request_id,
        "type": USE_CHAT_RESPONSE,
    }
    if outcome is not None:
        frame["outcome"] = outcome
    if continuation:
        frame["continuation"] = True
    return frame


def _terminal_error_frame(
    request_id: str, error: str, continuation: bool
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "body": error,
        "done": True,
        "error": True,
        "id": request_id,
        "type": USE_CHAT_RESPONSE,
    }
    if continuation:
        frame["continuation"] = True
    return frame
