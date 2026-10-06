# `AIChatAgent` API

The user-facing API of `AIChatAgent` beyond what's decided elsewhere:
`on_chat_message`, chunks, parts, and `ChatMessageOptions` are in
[chat_models.md](./chat_models.md); storage is in
[sessions_api.md](./sessions_api.md); `self.messages` is a read-only property
([sessions_api.md](./sessions_api.md) §8).

Upstream: `../agents/packages/ai-chat/src/index.ts`,
`../agents/packages/agents/src/chat/lifecycle.ts`,
`docs/agents/chat-agents.md` ("`persistMessages` and `saveMessages`").

---

## 1. Writing messages (decided)

`self.messages` is read-only, so these methods are how server code changes the
transcript: from a schedule callback, a webhook, a `@callable`, or another
agent.

### 1.1 Upstream

**`persistMessages(messages, excludeBroadcastIds = [])`** (`index.ts`,
public, async): store messages **without** starting a turn.
- `messages` is the intended transcript (typically `[...this.messages, m]`).
- It's **reconciled** against the stored transcript (`chat/message-reconciler.ts`):
  assistant ids matched (exact id, then same tool call, then content), and
  server-known tool outputs merged into incoming copies that still show stale
  tool states.
- Each message is sanitized; **only messages that changed are written**
  (compared with the in-memory copy, to avoid a row read per message).
- **Upsert only:** a stored message missing from `messages` is **not**
  deleted. (Deletion of omitted rows exists only behind an internal option
  used for regeneration.)
- Retention: trims to `maxPersistedMessages` (counting stored rows).
- Broadcasts `cf_agent_chat_messages` with the reconciled list to every client
  except the connection ids in `excludeBroadcastIds` (the sender, when a
  client's own submit is being persisted).
- **Overridable:** a subclass can override it and call `super`. If a turn has
  just finished, the write lands in the same transaction as the stream cutover
  ([streams_api.md](./streams_api.md) §2.2), because the pending cutover is
  tracked on the instance, not passed as an argument.

**`saveMessages(messages | (current) => messages, { signal }?)`** (public,
async): store messages **and** run `onChatMessage` for a new response.
- **Queued:** waits for any active turn to finish, so programmatic turns never
  overlap a streaming one.
- **Functional form:** a function receives the latest `this.messages` when the
  turn actually starts (and may be async), so several queued calls don't build
  on a stale baseline: `saveMessages((messages) => [...messages, synthetic])`.
- Persists through `persistMessages`, then runs a turn with the last request's
  client tools and body.
- Returns `{ requestId, status, error? }`: `status` is `"completed"`,
  `"error"` (the stream reported an error; `error` holds its message),
  `"skipped"` (the chat was cleared while it waited), or `"aborted"` (the
  external `signal` fired; partial chunks are still saved).
- `signal`: an `AbortSignal` that cancels the turn as a client's
  `cf_agent_chat_request_cancel` would, without knowing the generated request
  id.

**`continueLastTurn(body?, { signal }?)`** (**protected**, async): run a turn
that **continues the last assistant message** instead of answering a new user
message (`continuation: true` in `ChatMessageOptions`), the same mechanism as
tool auto-continuation. Returns `{ requestId: "", status: "skipped" }` if
there's no assistant message.

### 1.2 Python API (decided)

```python
@dataclass(slots=True, kw_only=True)
class SaveMessagesResult:
    request_id: str
    status: Literal["completed", "error", "skipped", "aborted"]
    error: str | None = None


class AIChatAgent(Agent):
    async def persist_messages(
        self,
        messages: Sequence[UIMessage],
        *,
        exclude: Iterable[str | Connection] = (),
    ) -> None: ...

    async def save_messages(
        self,
        messages: Sequence[UIMessage]
        | Callable[[list[UIMessage]], Awaitable[Sequence[UIMessage]]],
    ) -> SaveMessagesResult: ...

    async def continue_last_turn(
        self, *, body: dict[str, JSONValue] | None = None
    ) -> SaveMessagesResult: ...

    async def delete_messages(
        self,
        message_ids: Iterable[str],
        *,
        exclude: Iterable[str | Connection] = (),
    ) -> None: ...
```

```python
class Support(AIChatAgent):
    async def on_start(self):
        await self.schedule("0 9 * * *", self.daily_check_in)

    async def daily_check_in(self, _payload, _schedule):
        async def add_prompt(messages: list[UIMessage]) -> list[UIMessage]:
            prompt = UIMessage(
                id=new_id(), role="user", parts=[TextPart(text="Any updates today?")]
            )
            return [*messages, prompt]

        result = await self.save_messages(
            add_prompt
        )  # stores it and runs on_chat_message
        if result.status == "error":
            log.warning("check-in failed: %s", result.error)

    @callable
    async def add_note(self, text: str) -> None:  # stored and broadcast, no model call
        note = UIMessage(id=new_id(), role="assistant", parts=[TextPart(text=text)])
        await self.persist_messages([*self.messages, note])
```

- **Same semantics as upstream**, including reconciliation, changed-only
  writes, upsert-only (omitted messages aren't deleted), retention, the
  broadcast, queueing behind the active turn, and the functional form resolved
  when the turn starts.
- **`exclude`** instead of `excludeBroadcastIds`, accepting ids or
  `Connection`s, like `broadcast(exclude=...)`
  ([agent_api.md](./agent_api.md) §1.9).
- **The functional form is `async def` only** (decided; the SDK-wide
  async-callback rule), receiving a copy of the current messages. Docs use a
  named `async def`, never a lambda.
- **No `signal`** (as everywhere else): cancelling the task that awaits
  `save_messages` / `continue_last_turn` cancels the turn, exactly like a
  client's stop. The awaiting code gets `CancelledError` (normal asyncio), so
  `status="aborted"` is what the caller gets when the turn is stopped some
  other way: by a client's `cf_agent_chat_request_cancel`, or by upstream's
  protected `abortRequest(requestId)` (in Python: public `abort_request`,
  [chat_engine.md](./chat_engine.md) §5.2).
- **`continue_last_turn` is public** (Python has no `protected`); documented
  as meant for the agent's own code. `body` replaces the last request's body
  for this turn, as upstream.
- **`persist_messages` stays overridable** (call `super().persist_messages(...)`),
  and the cutover rides on the instance as upstream, so an override that calls
  `super` with only the messages still saves atomically.

- **`delete_messages(message_ids)`** (decided; Python addition, not public
  upstream): deletes those messages through Sessions and broadcasts the
  updated `cf_agent_chat_messages` (except to `exclude`), so server code can
  remove specific messages (moderation, a failed tool exchange).
  - Separate from `persist_messages`, which stays upsert-only: leaving a
    message out of `persist_messages` never deletes it.
  - Unknown ids are ignored. Not queued behind the active turn, like
    `persist_messages`.
  - Deleting the assistant message of a turn that is still streaming has no
    lasting effect (the turn saves it when it finishes); stop the turn first.
  - Upstream deletes only internally: regeneration (`_deleteStaleRows`) and
    retention (`_deleteMessagesByIds`). Sessions'
    `Session.delete_messages` ([sessions_api.md](./sessions_api.md) §3) does
    the work.

**Decided:**
1. The functional form of `save_messages` is **`async def` only**.
2. **Explicit deletion is a separate `delete_messages` method** (above), not a
   flag on `persist_messages`.

---

## 2. A failed chat turn and `on_error` (decided)

### 2.1 Upstream

Two kinds of failure, handled differently (`ai-chat/src/index.ts`):

| Failure | Where it's caught | What the client and app see | `onError`? |
| --- | --- | --- | --- |
| **The response stream fails** midway (an `error` chunk, or the stream throws while being read) | Inside `_reply` (`:7735`, `:7262`) | `cf_agent_use_chat_response` with `error: true` and the message; stream marked errored; `message:error` event; partial assistant message saved; `onChatResponse` with `status: "error"`; `saveMessages` → `status: "error"` | **No** |
| **`onChatMessage` throws** before returning a `Response` (e.g. the model call fails up front), or the turn machinery fails | `_tryCatchChat` (`:2622`) | Turn settled; a continuation sends an error frame (`_reportContinuationFailure`, `:3515`) | **Yes**, then re-thrown: out of the turn, and for a client request into `Agent`'s own `onMessage` wrapper, which calls `onError` **a second time** and re-throws again (the runtime logs it as uncaught) |

### 2.2 What "handled" could mean

`on_error`'s general rule ([agent_api.md](./agent_api.md) §1.10): returning
normally means the error is handled and stops propagating. For a chat turn,
propagating only means "the exception keeps going up the stack" (an uncaught
error logged for the invocation). The things a user actually sees — the error
frame, the saved partial message, `on_chat_response(status="error")`,
`SaveMessagesResult(status="error")` — are the turn's terminal state and
happen either way. So there's nothing for a handler to rescue: the turn has
already failed.

### 2.3 Decided

- **Python can't split the two upstream cases.** `on_chat_message` is an
  async generator: a failure before the first chunk and a failure midway both
  surface as an exception from iterating it, caught by the SDK's consumption
  loop.
- **So every failed turn is handled the same way:** the SDK ends the turn as
  failed (error frame with `str(exc)`, partial message saved, stream marked
  errored, `message:error`, `on_chat_response` with `status="error"`,
  `save_messages` → `status="error"`), then calls **`on_error(None, exc)`
  once**, as **notification only** (like failed schedules, queue items, and
  tasks): whether it returns or raises changes nothing, and anything it raises
  is logged. The exception doesn't propagate further.
- **Called once**, not twice as upstream's nested wrappers do.
- **Cancellation isn't a failure:** a stopped turn ends as aborted, and
  `CancelledError` never reaches `on_error` (§1.10's `Exception`-only rule).
- Difference from upstream: stream failures also reach `on_error` (upstream
  only reports them through the frame, events, and `onChatResponse`).

### 2.4 Client compatibility (checked against the JS client)

`on_error` itself sends nothing to clients, so the only client-visible
question is which frames a failed turn produces:
- **Mid-stream failure:** the same frames as upstream (chunks so far, then
  `cf_agent_use_chat_response` with `error: true`, `done: true`; the partial
  message saved and broadcast in `cf_agent_chat_messages`).
- **Failure before the first chunk:** Python sends the same error frame.
  **Upstream sends the requesting client nothing** in this case (it only
  settles its pre-stream bookkeeping, releasing reconnecting clients with
  `cf_agent_stream_resume_none`), and the JS transport has no timeout for a
  normal request, so the browser waits until the socket closes.
- **The JS client already handles the frame:** `WsChatTransport`
  (`agents/src/chat/ws-chat-transport.ts:459`) errors the request's stream on
  any `cf_agent_use_chat_response` with `error: true` for that request id,
  whether or not chunks came first, so `useAgentChat` shows the error. No new
  frame type or field is introduced, so this is compatible, and it fixes the
  hang.
- The error frame's `body` is `str(exc)`, as upstream sends the error message:
  exception messages reach the browser, which the docs should mention.
