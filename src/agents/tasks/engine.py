"""The step journal operations of one claimed execution attempt.

Port of upstream ``tasks/engine-port.ts`` (a class instead of an object of
closures). `ReplayStep` drives it; every mutation first checks that the
attempt still holds the run's claim (its generation), and run-row writes are
fenced on it.
"""

from collections.abc import Callable
from typing import Any

from ..core.timing import now_ms
from .errors import AttemptSupersededError
from .store import TaskStore
from .types import ResolvedStepPolicy, TaskStepRow

__all__ = ("TaskStepEngine",)


class TaskStepEngine:
    """Journal reads and writes for one attempt of one run.

    Parameters
    ----------
    store
        The Tasks store.
    run_id
        The run.
    generation
        The attempt's claim token.
    claim_timeout_ms
        How far ahead each claim refresh moves the run's deadline.
    claimed_at_ms
        When the claim was written.
    claim_refresh_after_ms
        The least time between claim refreshes.
    defaults
        The step policy used where a step leaves fields unset.
    emit
        Publishes an event, with the run id and definition added.
    """

    __slots__ = (
        "_claim_refresh_after_ms",
        "_claim_timeout_ms",
        "_emit",
        "_last_claim_ms",
        "_last_status",
        "_store",
        "defaults",
        "generation",
        "run_id",
    )

    def __init__(
        self,
        *,
        store: TaskStore,
        run_id: str,
        generation: str,
        claim_timeout_ms: int,
        claimed_at_ms: int,
        claim_refresh_after_ms: int,
        defaults: ResolvedStepPolicy,
        emit: Callable[[str, dict[str, Any]], None],
    ) -> None:
        self._store = store
        self.run_id = run_id
        self.generation = generation
        self._claim_timeout_ms = claim_timeout_ms
        self._claim_refresh_after_ms = claim_refresh_after_ms
        self._last_claim_ms = claimed_at_ms
        self._last_status: str | None = None
        self.defaults = defaults
        self._emit = emit

    def emit(self, type: str, payload: dict[str, Any]) -> None:
        """Publish a ``task:*`` event for this run."""
        self._emit(type, payload)

    def idempotency_key(self, name: str) -> str:
        """Return step ``name``'s stable deduplication key."""
        return f"{self.run_id}:{name}"

    def is_current(self) -> bool:
        """Return whether this attempt still holds the run's claim."""
        rows = self._store.sql(
            "SELECT generation FROM cf_agents_task_runs WHERE run_id = ?", self.run_id
        )
        return bool(rows) and rows[0]["generation"] == self.generation

    def _assert_current(self) -> None:
        if not self.is_current():
            raise AttemptSupersededError(self.run_id)

    # Journal

    def read_step(self, name: str) -> TaskStepRow | None:
        """Return the journal row of step ``name``, or ``None``."""
        rows = self._store.sql(
            "SELECT * FROM cf_agents_task_steps WHERE run_id = ? AND step_name = ?",
            self.run_id,
            name,
            row=TaskStepRow,
        )
        return rows[0] if rows else None

    def count_steps(self) -> int:
        """Return how many steps the run has journaled."""
        rows = self._store.sql(
            "SELECT COUNT(*) AS count FROM cf_agents_task_steps WHERE run_id = ?",
            self.run_id,
        )
        return rows[0]["count"]

    def insert_do_step(self, name: str) -> None:
        """Journal a new ``do`` step, running its first attempt."""
        self._assert_current()
        now = now_ms()
        self._store.sql(
            """INSERT INTO cf_agents_task_steps
                 (run_id, step_name, kind, state, attempt, created_at, started_at,
                  updated_at)
               VALUES (?, ?, 'do', 'running', 1, ?, ?, ?)""",
            self.run_id,
            name,
            now,
            now,
            now,
        )

    def insert_sleep_step(self, name: str, wake_at_ms: int) -> None:
        """Journal a new sleep, waiting until ``wake_at_ms``."""
        self._assert_current()
        now = now_ms()
        self._store.sql(
            """INSERT INTO cf_agents_task_steps
                 (run_id, step_name, kind, state, attempt, next_at, created_at,
                  updated_at)
               VALUES (?, ?, 'sleep', 'waiting', 0, ?, ?, ?)""",
            self.run_id,
            name,
            wake_at_ms,
            now,
            now,
        )

    def insert_completed_sleep(self, name: str) -> None:
        """Journal a sleep whose deadline has already passed, born completed."""
        self._assert_current()
        now = now_ms()
        self._store.sql(
            """INSERT INTO cf_agents_task_steps
                 (run_id, step_name, kind, state, attempt, created_at, completed_at,
                  updated_at)
               VALUES (?, ?, 'sleep', 'completed', 0, ?, ?, ?)""",
            self.run_id,
            name,
            now,
            now,
            now,
        )

    def claim_step_attempt(self, name: str) -> int:
        """Start the next attempt of an existing step; return its number."""
        self._assert_current()
        now = now_ms()
        rows = self._store.sql(
            """UPDATE cf_agents_task_steps
               SET state = 'running', attempt = attempt + 1, next_at = NULL,
                   started_at = ?, updated_at = ?
               WHERE run_id = ? AND step_name = ?
               RETURNING attempt""",
            now,
            now,
            self.run_id,
            name,
        )
        return rows[0]["attempt"] if rows else 1

    def complete_step(self, name: str, result_json: str | None) -> None:
        """Record a step's (already serialized) result."""
        self._assert_current()
        now = now_ms()
        self._store.sql(
            """UPDATE cf_agents_task_steps
               SET state = 'completed', result = ?, next_at = NULL,
                   completed_at = ?, updated_at = ?
               WHERE run_id = ? AND step_name = ?""",
            result_json,
            now,
            now,
            self.run_id,
            name,
        )

    def fail_step(self, name: str, error_name: str, message: str) -> None:
        """Record a step's terminal failure."""
        self._assert_current()
        self._store.sql(
            """UPDATE cf_agents_task_steps
               SET state = 'failed', error_name = ?, error_message = ?,
                   next_at = NULL, updated_at = ?
               WHERE run_id = ? AND step_name = ?""",
            error_name,
            message,
            now_ms(),
            self.run_id,
            name,
        )

    def wait_step(self, name: str, wake_at_ms: int) -> None:
        """Park a step until its retry at ``wake_at_ms``."""
        self._assert_current()
        self._store.sql(
            """UPDATE cf_agents_task_steps
               SET state = 'waiting', next_at = ?, updated_at = ?
               WHERE run_id = ? AND step_name = ?""",
            wake_at_ms,
            now_ms(),
            self.run_id,
            name,
        )

    # The run row

    def refresh_claim(self) -> None:
        """Move the run's claim deadline forward, at most every refresh window."""
        now = now_ms()
        if now - self._last_claim_ms < self._claim_refresh_after_ms:
            return
        rows = self._store.sql(
            """UPDATE cf_agents_task_runs SET next_at = ?, updated_at = ?
               WHERE run_id = ? AND generation = ? AND state = 'running'
               RETURNING run_id""",
            now + self._claim_timeout_ms,
            now,
            self.run_id,
            self.generation,
        )
        if rows:
            self._last_claim_ms = now

    def write_status(self, message: str) -> None:
        """Record the run's status message (skipped when unchanged)."""
        if message == self._last_status:
            return
        rows = self._store.sql(
            """UPDATE cf_agents_task_runs SET status_message = ?, updated_at = ?
               WHERE run_id = ? AND generation = ? AND state = 'running'
               RETURNING run_id""",
            message,
            now_ms(),
            self.run_id,
            self.generation,
        )
        if rows:
            self._last_status = message

    def cancellation_requested(self) -> tuple[bool, str | None]:
        """Return whether the run was asked to cancel, and the reason."""
        row = self._store.get_run(self.run_id)
        if row is None or row["cancel_requested"] != 1:
            return False, None
        return True, row["cancel_reason"]
