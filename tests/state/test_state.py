import asyncio
from typing import Any

import pytest
from fake_runtime import Host

from agents.lifecycle import Lifecycle
from agents.state import State, StateSource


def install(state: State[Any], host: Host | None = None) -> State[Any]:
    Lifecycle(host or Host()).use(state)
    return state


def test_initial_state_is_seeded_once_and_persisted() -> None:
    host = Host()
    changes: list[tuple[Any, StateSource]] = []
    state = install(
        State(
            initial_state={"count": 0},
            on_changed=lambda value, source: changes.append((value, source)),
        ),
        host,
    )
    assert state.get() == {"count": 0}
    assert state.get() == {"count": 0}
    assert changes == [({"count": 0}, "server")]

    fresh = install(State(initial_state={"count": 99}), host)
    assert fresh.get() == {"count": 0}  # stored value wins over the initial one


def test_no_initial_state_means_none() -> None:
    assert install(State()).get() is None


def test_falsy_states_read_back() -> None:
    host = Host()
    state = install(State(initial_state=5), host)
    state.set(0)
    assert install(State(initial_state=5), host).get() == 0


def test_a_rejected_change_is_not_saved() -> None:
    def no_negatives(value: dict[str, int], source: StateSource) -> None:
        if value["count"] < 0:
            raise ValueError("negative")

    state = install(
        State(initial_state={"count": 1}, validate_state_change=no_negatives)
    )
    with pytest.raises(ValueError, match="negative"):
        state.set({"count": -1})
    assert state.get() == {"count": 1}


def test_unserializable_state_is_never_cached() -> None:
    state = install(State(initial_state={"count": 1}))
    with pytest.raises(TypeError):
        state.set({"count": object()})
    assert state.get() == {"count": 1}


def test_the_cache_is_a_copy_of_what_the_caller_passed() -> None:
    state = install(State())
    value = {"items": [1]}
    state.set(value)
    value["items"].append(2)
    assert state.get() == {"items": [1]}


def test_corrupt_row_falls_back_to_initial_state_or_is_cleared(
    caplog: pytest.LogCaptureFixture,
) -> None:
    host = Host()
    install(State(initial_state=1), host).get()
    host.ctx.storage.sql.exec("UPDATE cf_agents_state SET state = '{bad'")
    assert install(State(initial_state={"reset": True}), host).get() == {"reset": True}

    host.ctx.storage.sql.exec("UPDATE cf_agents_state SET state = '{bad'")
    assert install(State(), host).get() is None
    rows = host.ctx.storage.sql.exec("SELECT * FROM cf_agents_state").toArray()
    assert rows == []
    assert "not valid JSON" in caplog.text


def test_a_failing_on_changed_hook_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    def broken(value: Any, source: StateSource) -> None:
        raise RuntimeError("observer broke")

    state = install(State(on_changed=broken))
    state.set({"a": 1})
    assert state.get() == {"a": 1}
    assert "observer broke" in caplog.text


def test_startup_stamps_the_schema_version() -> None:
    host = Host()
    lifecycle = Lifecycle(host).use(State())
    asyncio.run(lifecycle.start())
    assert host.ctx.storage.kv["cf_agents:state_schema_version"] == 1
