import agents
from agents import (
    agent,
    chat,
    core,
    dynamic_agents,
    fibers,
    lifecycle,
    observability,
    queue,
    schedules,
    sessions,
    state,
    streams,
    tasks,
    websockets,
)

SUBPACKAGES = (
    agent,
    chat,
    core,
    dynamic_agents,
    fibers,
    lifecycle,
    observability,
    queue,
    schedules,
    sessions,
    state,
    streams,
    tasks,
    websockets,
)


def test_top_level_all_lists_every_subpackage_public_name() -> None:
    expected = {name for package in SUBPACKAGES for name in package.__all__}
    assert set(agents.__all__) == expected
    assert len(agents.__all__) == len(expected)  # no duplicates


def test_top_level_names_are_the_subpackage_objects() -> None:
    for package in SUBPACKAGES:
        for name in package.__all__:
            assert getattr(agents, name) is getattr(package, name)


def test_all_is_a_tuple() -> None:
    assert isinstance(agents.__all__, tuple)
    for package in SUBPACKAGES:
        assert isinstance(package.__all__, tuple)
