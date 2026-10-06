# SQL schemas (phase 1)

Every SQLite table and Durable Object KV key the phase-1 Python SDK needs
(including sub-agents/facets and the legacy fiber engine),
copied from upstream (`../agents/packages/agents` v0.25.0 and
`../agents/packages/ai-chat`). Scope follows [scope.md](./scope.md).

**Table names and columns match upstream exactly.** Migrating a DO between the
TypeScript and Python SDKs is **not supported** ([scope.md](./scope.md) §4.5);
keeping the names only makes upstream code and docs easy to compare.

---

## 1. Summary

| Owner | Tables | KV keys |
| --- | --- | --- |
| Lifecycle (job queue + driver) | `cf_agents_jobs` | `cf_agents:oom_alarm_strikes` |
| State | `cf_agents_state` | `cf_agents:state_schema_version` |
| Scheduler | — (uses `cf_agents_jobs`) | `cf_agents:schedules_schema_version` |
| Queue | — (uses `cf_agents_jobs`) | `cf_agents:queue_schema_version` |
| Tasks | `cf_agents_task_runs`, `cf_agents_task_steps` | `cf_agents:tasks_schema_version` |
| Streams | `cf_agents_streams`, `cf_agents_stream_blocks` | `cf_agents:streams_schema_version` |
| Sessions | `cf_agents_session_messages`, `cf_agents_session_message_chunks` | `cf_agents:sessions_schema_version` |
| WebSockets | — (connection data lives in socket attachments) | — |
| `Agent` | — | `cf_agents_destroy_pending` |
| Sub-agents / facets | `cf_agents_sub_agents`, `cf_agents_facet_runs` | — |
| Fibers | `cf_agents_runs`, `cf_agents_fibers` | — |
| Shared chat layer | `cf_agents_chat_progress` | `cf:chat-recovery:progress`, `cf:chat:recovering`, `cf:chat:last-terminal` |
| `AIChatAgent` | `cf_ai_chat_request_context` | — |

Fourteen tables in total.

**Conventions** (upstream):
- **`WITHOUT ROWID` wherever possible.** A DO SQLite row write costs about 1000×
  a read, and an ordinary rowid table maintains a hidden unique index for its
  `PRIMARY KEY`, which is one extra billed row on every insert and delete.
  `WITHOUT ROWID` makes the primary key the table itself.
- **Very few secondary indexes**, for the same reason: each index is charged on
  every write. The few that exist are justified in the notes below.
- **Each owner creates its own tables** with `CREATE … IF NOT EXISTS`, in its
  `on_start` hook (or lazily, before first use), and **gates migrations on its
  own KV schema-version key**. No central schema.
- **Timestamps are integer epoch milliseconds** unless noted.
  `cf_agents_jobs.created_at` defaults to `unixepoch()`, which is seconds.
  User-facing records convert them to timezone-aware UTC `datetime`s at the
  storage boundary ([utilities.md](./utilities.md) §1).
- **Durations stored in JSON** (e.g. `retry_options`) keep upstream's
  millisecond fields (`baseDelayMs`, `maxDelayMs`); the Python API's
  `timedelta | float` seconds are converted on write.
- **Access path:** SDK internals use raw JS storage on hot paths
  ([utilities.md](./utilities.md) §3.8).

---

## 2. Lifecycle: `cf_agents_jobs`

The single job queue that every capability pushes into, and from which the
single physical alarm is armed ([lifecycle_capabilities.md](./lifecycle_capabilities.md) §5).
Upstream: `lifecycle/job-queue.ts:205`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_jobs (
  id TEXT PRIMARY KEY NOT NULL,
  capability TEXT NOT NULL,            -- owning capability id, or "host"
  fn TEXT NOT NULL,                    -- name the owner dispatches on (e.g. a callback name)
  time INTEGER NOT NULL,               -- due time, epoch ms
  payload TEXT,                        -- JSON
  retry_options TEXT,                  -- JSON RetryOptions
  singleflight INTEGER NOT NULL DEFAULT 0,
  hung_timeout_seconds INTEGER,
  exclusive INTEGER NOT NULL DEFAULT 0,
  recovery_loop INTEGER NOT NULL DEFAULT 0,
  running INTEGER NOT NULL DEFAULT 0,
  execution_started_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (unixepoch())
) WITHOUT ROWID;
```

- **Job ids belong to their owner.** A push with an existing id replaces only
  the pusher's own job; an id held by another capability is an error.
- **`recovery_loop`** is upstream scaffolding for chat recovery on routed
  sub-agents, marked for removal (`schedules/types.ts`). Python can leave it
  unset but keeps the column.
- **KV `cf_agents:oom_alarm_strikes`:** the count used by the alarm's
  memory-limit circuit breaker (`job-driver.ts:36`).

---

## 3. State: `cf_agents_state`

Upstream: `state/index.ts:115`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_state (
  id TEXT PRIMARY KEY NOT NULL,
  state TEXT                            -- JSON
);
```

- **There is only one row**, `id = 'cf_state_row_id'`. Whether that row exists
  is the signal that state was ever set, so falsy states (`null`, `0`, `false`,
  `""`) read back correctly.
- An ordinary rowid table upstream, not `WITHOUT ROWID`. Keep it the same.
- Upstream's v1 migration deletes a legacy `cf_state_was_changed` row. Python
  has no legacy rows, so it only needs to stamp
  `cf_agents:state_schema_version`.

---

## 4. Scheduler and Queue: no tables

Both store each schedule or queue item as a row in `cf_agents_jobs`. Upstream
migrates their old tables (`cf_agents_schedules`, `cf_agents_queues`) into jobs
on startup. **Python creates neither old table and runs neither migration.** It
only stamps `cf_agents:schedules_schema_version` / `cf_agents:queue_schema_version`
at the current version, for parity.

---

## 5. Tasks: `cf_agents_task_runs`, `cf_agents_task_steps`

Upstream: `tasks/store.ts:97`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_task_runs (
  run_id TEXT PRIMARY KEY,
  definition TEXT NOT NULL,
  input TEXT,                           -- JSON
  state TEXT NOT NULL CHECK (state IN (
    'pending', 'running', 'waiting',
    'completed', 'failed', 'cancelled'
  )),
  result TEXT,                          -- JSON
  error_name TEXT,
  error_message TEXT,
  status_message TEXT,                  -- step.status(...)
  metadata TEXT,                        -- JSON
  idempotency_key TEXT UNIQUE,
  retain INTEGER NOT NULL DEFAULT 1,
  attempt INTEGER NOT NULL DEFAULT 0,
  generation TEXT,                      -- claim fencing token
  next_at INTEGER,                      -- next wake (sleep / retry / claim deadline)
  wait_reason TEXT,                     -- 'sleep' | 'retry'
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  cancel_reason TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  updated_at INTEGER NOT NULL,
  settled_at INTEGER
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS cf_agents_task_runs_definition
  ON cf_agents_task_runs (definition, created_at);

CREATE TABLE IF NOT EXISTS cf_agents_task_steps (
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('do', 'sleep')),
  state TEXT NOT NULL CHECK (state IN (
    'running', 'waiting', 'completed', 'failed'
  )),
  result TEXT,                          -- JSON, the journaled step result
  error_name TEXT,
  error_message TEXT,
  attempt INTEGER NOT NULL DEFAULT 0,
  next_at INTEGER,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER,
  PRIMARY KEY (run_id, step_name)
) WITHOUT ROWID;
```

- **Index notes (upstream):**
  - `cf_agents_task_runs_definition` exists because list-by-definition reads
    grow with retained runs, and `definition` and `created_at` never change
    after insert.
  - There is deliberately **no** `(state, next_at)` index: every claim,
    refresh, and settle rewrites `next_at`, so that index would add a billed
    row to the most frequent writes.
- **`idempotency_key UNIQUE`** creates an implicit index. It is what makes run
  acceptance idempotent.
- **A run's wake is mirrored as one job** in `cf_agents_jobs` (id prefix
  `task:`).

---

## 6. Streams: `cf_agents_streams`, `cf_agents_stream_blocks`

A durable, ordered chunk log per stream. Chat's resumable streams are built on
it (`chat/resumable-stream.ts:81`). Upstream: `streams/streams.ts:1045`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_streams (
  stream_id TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN (
    'streaming', 'completed', 'errored'
  )),
  tag TEXT,                             -- indexed lookup key (chat: the request id)
  metadata TEXT,                        -- JSON (chat: messageId, continuation, seqBase, cfChat)
  error_message TEXT,
  chunk_count INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  closed_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_cf_agents_streams_tag
  ON cf_agents_streams(tag, created_at);

CREATE TABLE IF NOT EXISTS cf_agents_stream_blocks (
  stream_id TEXT NOT NULL,
  block INTEGER NOT NULL,               -- 0, 1, 2, … per stream
  seq_from INTEGER NOT NULL,            -- first chunk seq in this block
  seq_to INTEGER NOT NULL,              -- one past the last chunk seq (half-open)
  body TEXT NOT NULL,                   -- chunk JSON texts joined by ','
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (stream_id, block)
) WITHOUT ROWID;
```

- **`cf_agents_streams` is deliberately a rowid table.** Its rowid breaks ties
  in insertion order for "newest first" when streams share a `created_at`
  millisecond (stream ids are random). It costs one billed row per stream
  opened, which is once per turn, never per chunk.
- **Chunks are packed into blocks.** An append either `UPDATE`s the open block
  (`body = body || ',' || :chunk_json`, `seq_to = seq_to + 1`) or, once the body
  would exceed `BLOCK_MAX_CHARS` (256 × 1024 characters), `INSERT`s the next
  block. Either way, one billed row per chunk. Cleanup deletes a handful of
  block rows instead of thousands of chunk rows.
- **Reading a block:** parse `'[' || body || ']'` as a JSON array. Chunk `seq`
  for element *i* is `seq_from + i`.
- **Chunk size limit:** `DEFAULT_MAX_CHUNK_BYTES` = 1 MiB (chat passes its own
  `CHAT_STREAM_MAX_CHUNK_BYTES`).
- Upstream also folds in chunks from an older layout (`#foldLegacyChunks`).
  Python has none.

---

## 7. Sessions: `cf_agents_session_messages`, `cf_agents_session_message_chunks`

Upstream: `sessions/core.ts:147`. Phase-1 scope is in
[sessions_api.md](./sessions_api.md).

```sql
CREATE TABLE IF NOT EXISTS cf_agents_session_messages (
  session_id TEXT NOT NULL,             -- '' for the default session
  id TEXT NOT NULL,                     -- message id
  seq INTEGER NOT NULL,                 -- ordering; the active leaf is max(seq)
  parent_id TEXT,                       -- tree (linear in phase 1)
  type TEXT NOT NULL DEFAULT 'message', -- row kind
  role TEXT NOT NULL,
  content TEXT NOT NULL,                -- slice 0 of the message JSON
  content_chunks INTEGER NOT NULL DEFAULT 0,
  token_estimate INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  content_hash TEXT,                    -- digest of the full JSON, for no-op update detection
  PRIMARY KEY (session_id, id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cf_agents_session_message_chunks (
  session_id TEXT NOT NULL,
  id TEXT NOT NULL,
  idx INTEGER NOT NULL,                 -- 1..content_chunks
  content TEXT NOT NULL,                -- slice idx of the message JSON
  PRIMARY KEY (session_id, id, idx)
) WITHOUT ROWID;
```

- **`content_hash`** lets an update find out whether anything changed without
  reassembling the stored JSON (`core.ts:1174`: equal digest means
  `"unchanged"`). Upstream adds it with `ALTER TABLE` for older objects; Python
  creates it from the start.
- **The row budget** is 1.5 MiB (`1536 * 1024`) of UTF-8 per slice
  ([sessions_api.md](./sessions_api.md) §4.3).
- **Not created in phase 1:** `cf_agents_session_compactions`,
  `cf_agents_session_config`, the `cf_agents_session_attachment_*` tables, and
  the `cf_agents_session_fts` virtual table (all deferred; see
  [sessions_api.md](./sessions_api.md) §7).

---

## 8. Shared chat layer: `cf_agents_chat_progress` and KV keys

Upstream: `chat/resumable-stream.ts:300`, `chat/recovery-incident.ts:125`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_chat_progress (
  key TEXT PRIMARY KEY,                 -- single row: 'chat'
  retired INTEGER NOT NULL,             -- stream segments of deleted streams + explicit credits
  legacy INTEGER NOT NULL DEFAULT 0     -- pre-derivation KV counter, folded in once
) WITHOUT ROWID;
```

- **Purpose:** a resumable-stream progress counter. It's derived from the
  segments in the live streams plus `retired + legacy`, so deleting streams
  never makes the counter go backwards.
- **`legacy`:** Python has no legacy counter, so it's always 0. Keep the column
  for parity.

**KV keys** used by chat recovery (recovery runs through Tasks, which is in
scope):

| Key | Value |
| --- | --- |
| `cf:chat-recovery:progress` | Integer recovery progress counter |
| `cf:chat:recovering` | The "recovering" record (drives `cf_agent_chat_recovering`) |
| `cf:chat:last-terminal` | The last terminal result of a turn that ended while no client was connected (replayed through the resume handshake, wire §7.4). Python stores it as JSON text (`{requestId, body, messageIds?}`) |

---

## 9. `AIChatAgent`: `cf_ai_chat_request_context`

Upstream: `ai-chat/src/index.ts:1100`.

```sql
CREATE TABLE IF NOT EXISTS cf_ai_chat_request_context (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL                   -- JSON
);
```

- A key/value store for request context that has to survive hibernation.
  Keys: `lastBody` (custom body fields from the last chat request) and
  `lastClientTools` (client tool schemas from the last request). Auto-continued
  turns reuse them (`index.ts:2265`–`2304`).
- An ordinary rowid table upstream.

---

## 10. `Agent` KV key

| Key | Value |
| --- | --- |
| `cf_agents_destroy_pending` | Written at the start of `destroy()` (`index.ts:8414`) so that an interrupted destroy is finished on the next wake |

Upstream `Agent._ensureSchema` (`index.ts:1598`, schema version 14 under KV
`cf_agents:schema_version`) creates `Agent`'s own tables. In phase 1, the ones
needed are the fiber tables and the facet-run index (§11, §12). Python keeps a
`cf_agents:schema_version` key to gate them.

---

## 11. Sub-agents / facets

### 11.1 `cf_agents_sub_agents`

The parent's registry of its child agents. Upstream: `dynamic-agents/registry.ts:56`.
Created lazily on first sub-agent access, not by the schema gate.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_sub_agents (
  class TEXT NOT NULL,                  -- child class name
  name TEXT NOT NULL,                   -- child instance name
  created_at INTEGER NOT NULL,
  identity_version TEXT,
  identity_name TEXT,
  PRIMARY KEY (class, name)
);
```

Upstream adds `identity_version` / `identity_name` with `ALTER TABLE` for older
objects; Python creates them from the start.

### 11.2 `cf_agents_facet_runs`

The **root's** index of fibers running in descendant facets. The fiber's
authoritative row stays in the facet's own `cf_agents_runs`; this index only
tells the root (which owns the alarm) which idle facets need recovery checks.
Upstream: `index.ts:1664`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_facet_runs (
  owner_path TEXT NOT NULL,             -- serialized path to the facet
  owner_path_key TEXT NOT NULL,         -- stable key for that path
  run_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (owner_path_key, run_id)
);

CREATE INDEX IF NOT EXISTS idx_facet_runs_owner_path_key
  ON cf_agents_facet_runs(owner_path_key);
```

---

## 12. Legacy fiber engine

Needed because facet-hosted chat turns run on fibers ([scope.md](./scope.md) §2.7).

### 12.1 `cf_agents_runs` (needed)

The fiber engine behind `runFiber` / `stash()`. A row is inserted when a fiber
starts, updated by `stash()` checkpoints, and deleted on completion; rows
still present after a wake are interrupted fibers to recover. Upstream:
`index.ts:1648`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_runs (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,                   -- e.g. the chat fiber name + ':' + request id
  snapshot TEXT,                        -- JSON, the last stash() checkpoint
  created_at INTEGER NOT NULL,
  completed_at INTEGER,
  outcome TEXT,
  error_message TEXT
);
```

### 12.2 `cf_agents_fibers` (needed: the public managed-fiber API is in scope)

The ledger behind `startFiber` / `inspectFiber` / `cancelFiber` /
`resolveFiber` / `deleteFibers` (idempotent acceptance, inspection,
cancellation, and cleanup of finished fibers). Upstream: `index.ts:1681`.

```sql
CREATE TABLE IF NOT EXISTS cf_agents_fibers (
  fiber_id TEXT PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  snapshot TEXT,
  metadata_json TEXT,
  error_message TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  completed_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_fibers_status_created
  ON cf_agents_fibers(status, created_at, fiber_id);
CREATE INDEX IF NOT EXISTS idx_fibers_name_status_created
  ON cf_agents_fibers(name, status, created_at, fiber_id);
CREATE INDEX IF NOT EXISTS idx_fibers_status_completed
  ON cf_agents_fibers(status, completed_at, created_at);
```

---

## 13. Upstream tables not created in phase 1

| Table(s) | Owner | Why not |
| --- | --- | --- |
| `cf_agents_mcp_servers` | MCP client | MCP out of scope. `cf_agent_mcp_servers` is sent as an empty state. |
| `cf_agents_workflows` | Workflows | Out of scope |
| `cf_agent_tool_runs`, `cf_ai_chat_agent_tool_runs`, `cf_ai_chat_agent_tool_milestones` | Agents as tools | Out of scope |
| `cf_agents_session_compactions`, `cf_agents_session_config`, `cf_agents_session_attachment_meta` / `_chunks` / `_refs`, `cf_agents_session_fts` | Sessions | Deferred ([sessions_api.md](./sessions_api.md) §7) |
| `cf_agents_schedules`, `cf_agents_queues` | Scheduler / Queue (legacy) | Migrated into jobs upstream; Python never creates them |
| `cf_agents_context_blocks`, `cf_agents_search_fts` | `agents/context` | Out of scope |
| `cf_voice_messages` | Voice | Out of scope |
| routed-agents catalog | `RoutedAgents` | Out of scope |
| `chat_sdk_state_*`, channels identity tables | chat-sdk, channels | Out of scope |
| `cf_ai_chat_agent_messages`, `assistant_messages`, … | Legacy chat storage | No legacy data in Python |
