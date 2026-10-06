# Chat models: chunks, parts, messages

The typed objects `AIChatAgent` users produce (`UIMessageChunk`) and read
(`UIMessage`, `UIMessagePart` via `self.messages`), and the rules the SDK
applies to the stream `on_chat_message` returns.

Related: [scope.md](./scope.md) §2.4, [sessions_api.md](./sessions_api.md),
[agents_wire_protocol.md](./agents_wire_protocol.md) §7,
[utilities.md](./utilities.md) §5 (dataclass convention, `wire()` helper).

**Source of truth for shapes:** the published `ai` package. Field lists below
were read from `ai@6.0.300` (`dist/index.d.ts`, `UIMessageChunk` at line
~2023), with `ai@7.0.127` for comparison.

---

## 1. Decisions so far

| Decision | Status |
| --- | --- |
| `on_chat_message` returns `AsyncIterator[UIMessageChunk]` or `AsyncIterator[str]` | Decided ([scope.md](./scope.md) §2.4.1) |
| Provider-agnostic: no adapters, users yield chunks themselves | Decided |
| Chunks, parts, and messages are `@dataclass(slots=True, kw_only=True)` | Decided ([utilities.md](./utilities.md) §5) |
| Match the **AI SDK v6** chunk set in phase 1; v7 additions later | Decided (§2.4) |
| The SDK guarantees exactly one `Start` and one `Finish` per turn | Decided (§3) |
| Chunk class names: short names in an `agents.chat.chunks` module | Decided (§2.1) |
| `UIMessage` / `UIMessagePart` dataclasses | Decided (§5): tool parts one class per state, no `created_at` |
| `ChatMessageOptions` | Decided (§6): upstream's fields incl. `continuation`, no `abort_signal` |

---

## 2. `UIMessageChunk`

### 2.1 Naming (decided)

Short names inside `agents.chat.chunks`, meant to be used through the module:

```python
from agents.chat import chunks

yield chunks.TextDelta(id="t1", delta="Hello")
yield chunks.Error(error_text="model failed")
```

Namespaced like this, generic names (`Error`, `File`, `Start`) read clearly and
don't clash with the part classes (`TextPart`, `FilePart`). The alternative is
a `Chunk` suffix on every class (`TextDeltaChunk`), which is unambiguous
anywhere but verbose.

### 2.2 Classes (v6 set)

Fields match `ai@6.0.300` exactly, in snake_case. Each class follows the
dataclass convention: the wire `type` is a `ClassVar`, and camelCase wire names
go through `wire()` ([utilities.md](./utilities.md) §5; `wire(name)` is
required, `wire(name, None)` is optional).

```python
# agents/chat/chunks.py
JSONValue = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
ProviderMetadata = dict[
    str, dict[str, JSONValue]
]  # provider name → provider-specific JSON
FinishReason = Literal[
    "stop", "length", "content-filter", "tool-calls", "error", "other"
]


# ── Text ──
@dataclass(slots=True, kw_only=True)
class TextStart:
    type: ClassVar[str] = "text-start"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class TextDelta:
    type: ClassVar[str] = "text-delta"
    id: str
    delta: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class TextEnd:
    type: ClassVar[str] = "text-end"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


# ── Reasoning ──
@dataclass(slots=True, kw_only=True)
class ReasoningStart:
    type: ClassVar[str] = "reasoning-start"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class ReasoningDelta:
    type: ClassVar[str] = "reasoning-delta"
    id: str
    delta: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class ReasoningEnd:
    type: ClassVar[str] = "reasoning-end"
    id: str
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


# ── Tool input ──
@dataclass(slots=True, kw_only=True)
class ToolInputStart:
    type: ClassVar[str] = "tool-input-start"
    tool_call_id: str = wire("toolCallId")
    tool_name: str = wire("toolName")
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    title: str | None = None


@dataclass(slots=True, kw_only=True)
class ToolInputDelta:
    type: ClassVar[str] = "tool-input-delta"
    tool_call_id: str = wire("toolCallId")
    input_text_delta: str = wire(
        "inputTextDelta"
    )  # a raw slice of the tool's JSON input


@dataclass(slots=True, kw_only=True)
class ToolInputAvailable:
    type: ClassVar[str] = "tool-input-available"
    tool_call_id: str = wire("toolCallId")
    tool_name: str = wire("toolName")
    input: JSONValue
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    title: str | None = None


@dataclass(slots=True, kw_only=True)
class ToolInputError:
    type: ClassVar[str] = "tool-input-error"
    tool_call_id: str = wire("toolCallId")
    tool_name: str = wire("toolName")
    input: JSONValue
    error_text: str = wire("errorText")
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    title: str | None = None


# ── Tool approval ──
@dataclass(slots=True, kw_only=True)
class ToolApprovalRequest:
    type: ClassVar[str] = "tool-approval-request"
    approval_id: str = wire("approvalId")
    tool_call_id: str = wire("toolCallId")
    approval_descriptor: JSONValue = wire("approvalDescriptor", None)
    input_schema_input: JSONValue = wire("inputSchemaInput", None)
    signature: str | None = None


# ── Tool output ──
@dataclass(slots=True, kw_only=True)
class ToolOutputAvailable:
    type: ClassVar[str] = "tool-output-available"
    tool_call_id: str = wire("toolCallId")
    output: JSONValue
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None
    preliminary: bool | None = None  # True: a later chunk may replace this output


@dataclass(slots=True, kw_only=True)
class ToolOutputError:
    type: ClassVar[str] = "tool-output-error"
    tool_call_id: str = wire("toolCallId")
    error_text: str = wire("errorText")
    provider_executed: bool | None = wire("providerExecuted", None)
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    dynamic: bool | None = None


@dataclass(slots=True, kw_only=True)
class ToolOutputDenied:
    type: ClassVar[str] = "tool-output-denied"
    tool_call_id: str = wire("toolCallId")


# ── Sources and files ──
@dataclass(slots=True, kw_only=True)
class SourceUrl:
    type: ClassVar[str] = "source-url"
    source_id: str = wire("sourceId")
    url: str
    title: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class SourceDocument:
    type: ClassVar[str] = "source-document"
    source_id: str = wire("sourceId")
    media_type: str = wire("mediaType")
    title: str
    filename: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class File:
    type: ClassVar[str] = "file"
    url: str  # a URL or a data: URL
    media_type: str = wire("mediaType")
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


# ── Developer data ──
@dataclass(slots=True, kw_only=True)
class Data:
    name: str  # wire type is f"data-{name}"
    data: JSONValue
    id: str | None = None  # same name + id updates the existing part
    transient: bool = False  # True: sent to clients, not saved


# ── Steps and message lifecycle ──
@dataclass(slots=True, kw_only=True)
class StartStep:
    type: ClassVar[str] = "start-step"


@dataclass(slots=True, kw_only=True)
class FinishStep:
    type: ClassVar[str] = "finish-step"


@dataclass(slots=True, kw_only=True)
class Start:
    type: ClassVar[str] = "start"
    message_id: str | None = wire("messageId", None)
    message_metadata: JSONValue = wire("messageMetadata", None)


@dataclass(slots=True, kw_only=True)
class Finish:
    type: ClassVar[str] = "finish"
    finish_reason: FinishReason | None = wire("finishReason", None)
    message_metadata: JSONValue = wire("messageMetadata", None)


@dataclass(slots=True, kw_only=True)
class MessageMetadata:
    type: ClassVar[str] = "message-metadata"
    message_metadata: JSONValue = wire("messageMetadata")  # required


@dataclass(slots=True, kw_only=True)
class Error:
    type: ClassVar[str] = "error"
    error_text: str = wire("errorText")


@dataclass(slots=True, kw_only=True)
class Abort:
    type: ClassVar[str] = "abort"
    reason: str | None = None


UIMessageChunk = (
    TextStart
    | TextDelta
    | TextEnd
    | ReasoningStart
    | ReasoningDelta
    | ReasoningEnd
    | ToolInputStart
    | ToolInputDelta
    | ToolInputAvailable
    | ToolInputError
    | ToolApprovalRequest
    | ToolOutputAvailable
    | ToolOutputError
    | ToolOutputDenied
    | SourceUrl
    | SourceDocument
    | File
    | Data
    | StartStep
    | FinishStep
    | Start
    | Finish
    | MessageMetadata
    | Error
    | Abort
)
```

### 2.3 Notes

- **`Data` is the one class without a `type` `ClassVar`**: its wire type is
  computed (`f"data-{name}"`). Generic typed data, like the AI SDK's
  `DataUIMessageChunk<DATA_TYPES>`, could come later through a type parameter
  (`class Data[T]`, valid on 3.12).
- **`input`, `output`, and metadata are `JSONValue`.** That's the Python
  equivalent of the AI SDK's `unknown`, constrained to what serializes to JSON.
- **A tool's `input` is forced to a JSON object when the message is built**
  (`normalize_tool_input`, ported from `message-builder.ts`): `None`, `""`,
  arrays, and unparseable strings become `{}`, because Anthropic rejects any
  `tool_use.input` that isn't an object.

### 2.4 Version: v6 now, v7 later (decided)

Upstream's server (`chat/message-builder.ts`) handles exactly the v6 set;
v7-only chunks fall through unhandled. Clients may be on AI SDK v6 or v7
(`agents` accepts `ai ^6 || ^7`). Phase 1 matches the v6 set.

**v7 additions, deferred:** `custom` (`kind`), `reasoning-file`, `reset-step`,
`tool-approval-response`, and extra fields on `tool-approval-request`
(`reason`, `isAutomatic`).

---

## 3. Stream rules: what the SDK does with `on_chat_message`'s output

### 3.1 The SDK owns `Start` and `Finish` (decided)

**What upstream does:** `AIChatAgent` doesn't generate them. The **AI SDK**
emits `start` / `finish` from `streamText(...).toUIMessageStreamResponse()`,
and `AIChatAgent` only rewrites them (`ai-chat/src/index.ts:7303`). Its
plain-text path sends neither (`index.ts:7389`).

**Python:** users write chunks by hand, so the SDK guarantees **exactly one
`Start` first and one `Finish` last**, while still letting users supply them
to set fields:

| User's stream | What the SDK sends |
| --- | --- |
| No `Start` | Its own `Start(message_id=<server message id>)` before the first chunk; no `message_id` on a continuation |
| `Start` as the first chunk | That chunk, rewritten as upstream does (§3.3) |
| `Start` later in the stream | `TypeError` |
| No `Finish` | Its own `Finish()` after the iterator ends normally |
| `Finish(finish_reason=…)` | That chunk, converted on the wire (§3.3) |
| A delta or end without its start (`TextDelta` / `TextEnd` before a `TextStart` with that id; likewise `Reasoning*`, and `ToolInputDelta` before `ToolInputStart`) | `TypeError`: the turn fails (decided 2026-10-06; [chat_engine.md](./chat_engine.md) §5.6). The AI SDK client rejects such a chunk and drops the whole reply, so it's never passed through, and never patched silently |
| The iterator raises (before or after the first chunk) | The error terminal frame (`error: true`, `done: true`, body `str(exc)`); no `Finish`; then `on_error(None, exc)`, notification only ([ai_chat_agent_api.md](./ai_chat_agent_api.md) §2) |

`MessageMetadata` chunks pass through for attaching metadata mid-stream.

User code only deals with content:

```python
async def on_chat_message(
    self, options: ChatMessageOptions
) -> AsyncIterator[chunks.UIMessageChunk]:
    yield chunks.TextStart(id="t1")
    async for piece in my_model_stream():
        yield chunks.TextDelta(id="t1", delta=piece)
    yield chunks.TextEnd(id="t1")
```

### 3.2 `AsyncIterator[str]`: the plain-text path (decided)

A `str` iterator is wrapped as upstream's `_sendPlaintextReply` does: one text
part, `text-start` → a `text-delta` per string → `text-end`. Python also
applies §3.1, so plain-text turns get `Start` (with the server message id)
and `Finish`. That **fixes upstream's gap**: upstream's plain-text path sends no
`start`, so the client builds the live message under its own id and briefly
renders the turn twice.

- **The stream type is decided by the first item.** A later item of the other
  type (a chunk in a `str` stream, or vice versa) raises `TypeError`.
- **On a continuation,** if the last text part is still streaming, deltas are
  appended to it and no new `text-start` is sent (upstream behavior).

### 3.3 Wire rewrites (as upstream, `index.ts:7303`)

- **`start` on a new turn:** if `message_id` is missing, stamp the server's
  assistant message id, so the client builds the live message under the id
  the server saves. A provider-supplied `message_id` is honored and becomes the
  message's id.
- **`start` on a continuation:** remove `messageId`, so the client appends to
  the existing assistant message.
- **`finish` with `finish_reason`:** sent as
  `{"type": "finish", "messageMetadata": {"finishReason": …}}` (upstream
  `#677`), and recorded as the turn's finish reason. Without a reason, it's
  sent as is.
- **Every chunk** is serialized (snake_case → camelCase, optional `None`
  fields omitted), stored in the resumable stream, applied to the assistant
  message by the message builder, and broadcast as `cf_agent_use_chat_response`
  ([agents_wire_protocol.md](./agents_wire_protocol.md) §7.2).

The end of a turn is signaled by the `cf_agent_use_chat_response` frame with
`done: true`, not by the `finish` chunk.

---

## 4. Message builder (port of `chat/message-builder.ts`)

Folds chunks into the assistant message's parts. Needed to save the finished
message, to apply tool results and approvals mid-stream, to rebuild a partial
reply from stored chunks after a crash, and to filter chunks that would regress
client state (`is_replay_chunk`). Port close to line for line. The key rules:

- text and reasoning parts go `streaming` → `done`; a delta with no start
  creates the part (needed when resuming mid-stream);
- tool parts go `input-streaming` → `input-available` →
  (`approval-requested` → `approval-responded`) → `output-available` |
  `output-error` | `output-denied`, and **only move forward** (some providers
  resend earlier tool calls in continuation streams);
- raw tool-input JSON text is collected separately from the part, so a partial
  string is never saved;
- `normalize_tool_input` (§2.3);
- late tool input after an approval request fills the input without leaving
  the approval state, and re-sends the approval request;
- `Data` chunks: `transient` ones aren't saved; the same `name` and `id` updates
  the existing part;
- `start-step` → a `step-start` part.

---

## 5. `UIMessage` and `UIMessagePart` (decided)

The types `self.messages` returns and Sessions stores. Shapes from `ai@6.0.300`
(`UIMessage`, `UIMessagePart`, `UIToolInvocation`).

```python
# agents/chat/messages.py


@dataclass(slots=True, kw_only=True)
class UIMessage:
    id: str
    role: Literal["system", "user", "assistant"]
    parts: list[UIMessagePart] = field(default_factory=list)
    metadata: JSONValue = None


@dataclass(slots=True, kw_only=True)
class TextPart:
    type: ClassVar[str] = "text"
    text: str
    state: Literal["streaming", "done"] | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class ReasoningPart:
    type: ClassVar[str] = "reasoning"
    text: str
    id: str | None = None
    state: Literal["streaming", "done"] | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class FilePart:
    type: ClassVar[str] = "file"
    url: str
    media_type: str = wire("mediaType")
    filename: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class SourceUrlPart:
    type: ClassVar[str] = "source-url"
    source_id: str = wire("sourceId")
    url: str
    title: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class SourceDocumentPart:
    type: ClassVar[str] = "source-document"
    source_id: str = wire("sourceId")
    media_type: str = wire("mediaType")
    title: str
    filename: str | None = None
    provider_metadata: ProviderMetadata | None = wire("providerMetadata", None)


@dataclass(slots=True, kw_only=True)
class StepStartPart:
    type: ClassVar[str] = "step-start"


@dataclass(slots=True, kw_only=True)
class DataPart:
    name: str  # wire type is f"data-{name}"
    data: JSONValue
    id: str | None = None


ToolState = Literal[
    "input-streaming",
    "input-available",
    "approval-requested",
    "approval-responded",
    "output-available",
    "output-error",
    "output-denied",
]


@dataclass(slots=True, kw_only=True)
class ToolApproval:
    id: str
    approved: bool | None = None  # None while approval-requested
    reason: str | None = None
    descriptor: JSONValue = None
    signature: str | None = None
    input_schema_input: JSONValue = wire("inputSchemaInput", None)


# ── Tool parts: one class per state (decided, §7) ──
# Wire type: f"tool-{tool_name}", or "dynamic-tool" when dynamic (then
# `toolName` is a field). The wire `state` is a ClassVar per subclass.


@dataclass(slots=True, kw_only=True)
class ToolPart:  # base: fields every state has; use for isinstance
    state: ClassVar[str]
    tool_name: str = wire("toolName")
    tool_call_id: str = wire("toolCallId")
    dynamic: bool = False
    title: str | None = None
    provider_executed: bool | None = wire("providerExecuted", None)
    tool_metadata: dict[str, JSONValue] | None = wire("toolMetadata", None)
    call_provider_metadata: ProviderMetadata | None = wire("callProviderMetadata", None)


@dataclass(slots=True, kw_only=True)
class ToolInputStreamingPart(ToolPart):
    state: ClassVar[str] = "input-streaming"
    input: JSONValue = None  # partial input parsed so far
    raw_input: str | None = wire("rawInput", None)


@dataclass(slots=True, kw_only=True)
class ToolInputAvailablePart(ToolPart):
    state: ClassVar[str] = "input-available"
    input: JSONValue


@dataclass(slots=True, kw_only=True)
class ToolApprovalRequestedPart(ToolPart):
    state: ClassVar[str] = "approval-requested"
    input: JSONValue
    approval: ToolApproval  # approved is None


@dataclass(slots=True, kw_only=True)
class ToolApprovalRespondedPart(ToolPart):
    state: ClassVar[str] = "approval-responded"
    input: JSONValue
    approval: ToolApproval  # approved is True or False


@dataclass(slots=True, kw_only=True)
class ToolOutputAvailablePart(ToolPart):
    state: ClassVar[str] = "output-available"
    input: JSONValue
    output: JSONValue
    result_provider_metadata: ProviderMetadata | None = wire(
        "resultProviderMetadata", None
    )
    preliminary: bool | None = None
    approval: ToolApproval | None = None  # approved is True when present


@dataclass(slots=True, kw_only=True)
class ToolOutputErrorPart(ToolPart):
    state: ClassVar[str] = "output-error"
    input: JSONValue = None  # may be missing if input never parsed
    raw_input: JSONValue = wire("rawInput", None)
    error_text: str = wire("errorText")
    result_provider_metadata: ProviderMetadata | None = wire(
        "resultProviderMetadata", None
    )
    approval: ToolApproval | None = None


@dataclass(slots=True, kw_only=True)
class ToolOutputDeniedPart(ToolPart):
    state: ClassVar[str] = "output-denied"
    input: JSONValue
    approval: ToolApproval  # approved is False


type AnyToolPart = (
    ToolInputStreamingPart
    | ToolInputAvailablePart
    | ToolApprovalRequestedPart
    | ToolApprovalRespondedPart
    | ToolOutputAvailablePart
    | ToolOutputErrorPart
    | ToolOutputDeniedPart
)

UIMessagePart = (
    TextPart
    | ReasoningPart
    | AnyToolPart
    | FilePart
    | SourceUrlPart
    | SourceDocumentPart
    | StepStartPart
    | DataPart
)
```

**Notes:**
- **Tool parts: one class per state** (decided; §7 item 2). Fields per
  state follow `ai@6.0.300`'s `UIToolInvocation` union exactly, so type
  checkers know e.g. `output` only exists on `ToolOutputAvailablePart`.
  - **`ToolPart` is the shared base** (`isinstance(part, ToolPart)` = "any tool
    part"); `AnyToolPart` is the union, for exhaustive `match`.
  - **A `Part` suffix**, like `TextPart`, because the chunk classes already use
    `ToolInputAvailable`, `ToolOutputAvailable`, `ToolOutputError`,
    `ToolOutputDenied` (as `chunks.…`), and both get imported together.
  - **A state change replaces the part** with an instance of the next state's
    class. (The SDK's own machinery works on wire-form dictionaries, decided
    2026-10-06, [chat_engine.md](./chat_engine.md) §4.4 Q1; the typed parts
    are what `self.messages` and the hooks hand to user code.)
  - **Parts the models don't know** (a newer AI SDK's, or malformed) decode to
    `UnknownPart`, which encodes back exactly (decided 2026-10-06,
    [chat_engine.md](./chat_engine.md) §4.4 Q2).
  - Deserialization picks the class from the wire `state`.
  - One `ToolApproval` class for all states; `approved` is `None` while
    requested.
- **`toolName` on the wire.** Static tool parts carry the name only in their
  `type`; upstream's message builder also stores a `toolName` field on them.
  Serialization writes both, matching what upstream stores.
- **`DataPart.name`** mirrors `chunks.Data.name`.
- **No `created_at`** on `UIMessage` (decided, §7 item 3).

---

## 6. `ChatMessageOptions` (decided)

Upstream `OnChatMessageOptions` (`ai-chat/src/index.ts:359`): `requestId`,
`abortSignal?`, `clientTools?`, `body?`, `continuation?`.

```python
@dataclass(slots=True, kw_only=True)
class ChatMessageOptions:
    request_id: str
    client_tools: list[ClientToolSchema] = field(
        default_factory=list
    )  # schemas sent by the browser
    body: dict[str, JSONValue] | None = None  # extra request body fields
    continuation: bool = False  # see below


@dataclass(slots=True, kw_only=True)
class ClientToolSchema:
    name: str
    description: str | None = None
    parameters: dict[str, JSONValue] | None = None  # JSON Schema
```

- **`continuation`** (upstream `continuation?: boolean`, missing from the
  first draft): `True` when the turn continues the previous assistant message
  instead of answering a new user message (auto-continue after a client tool
  result or approval, `continue_last_turn`, recovery). Upstream's use: adjust
  the system prompt, pick a model, skip expensive context assembly.
- **No `abort_signal`.** Cancelling a turn (`cf_agent_chat_request_cancel`)
  cancels the task running `on_chat_message`, so the user's code gets
  `CancelledError` at its next `await`. This is the same asyncio approach as
  Tasks and fibers. **To document:** an in-flight model request made with
  httpx (or a provider SDK built on it) is *not* aborted and keeps streaming
  until it finishes ([platform_verification.md](./platform_verification.md)
  §2.8, §6).
- **Upstream's first parameter, `onFinish`** (an AI SDK
  `GenerateTextOnFinishCallback` meant to be passed to `streamText`), has no
  Python equivalent and is dropped. Upstream's `onChatResponse` hook covers
  "after the turn is saved".

---

## 7. Formerly open items (all decided)

1. **Chunk class naming (decided, §2.1):** short names in `agents.chat.chunks`
   (`chunks.TextDelta`, `chunks.Error`), used through the module.
2. **Tool parts: one class per state (decided, §5).** `ToolPart` base plus
   `ToolInputStreamingPart` … `ToolOutputDeniedPart`, and the `AnyToolPart`
   union.
3. **`UIMessage.created_at`** (§5). Upstream: AI SDK v6's `UIMessage` has no
   `createdAt`. Sessions' `SessionMessage` allows an optional `createdAt` on
   *input*, but stores the time in the row's `created_at` column and doesn't
   put it back on messages it returns (only compactions get `createdAt` on
   read, `sessions/core.ts:1381`). **Decided: no `created_at` on `UIMessage`**
   (as the AI SDK); the timestamp stays in storage. Apps that want one put it
   in `metadata`.
4. **`ChatMessageOptions`** (§6): upstream's fields are `request_id`,
   `client_tools`, `body`, `continuation` (added; the draft missed it), with
   `abort_signal` replaced by task cancellation. **Decided: nothing beyond
   upstream.**
