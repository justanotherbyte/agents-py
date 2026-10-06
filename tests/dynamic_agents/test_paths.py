import pytest

from agents import AgentPathStep, build_agent_path, build_agent_url
from agents.dynamic_agents.paths import (
    identity_name,
    parse_sub_agent_path,
    path_from_json,
    path_key,
    path_to_json,
)

INBOX = AgentPathStep(class_name="Inbox", name="alice")
CHAT = AgentPathStep(class_name="ChatRoom", name="room 1")


def test_parse_finds_the_first_hop_and_the_rest() -> None:
    match = parse_sub_agent_path(
        "https://x.dev/agents/inbox/alice/sub/chat-room/room%201/sub/note/n/hi?q=1",
        known_classes=["Inbox", "ChatRoom", "Note"],
    )
    assert match is not None
    assert (match.child_class, match.child_name, match.remaining_path) == (
        "ChatRoom",
        "room 1",
        "/sub/note/n/hi",
    )


def test_parse_without_a_match() -> None:
    assert parse_sub_agent_path("https://x.dev/agents/inbox/alice") is None
    assert parse_sub_agent_path("https://x.dev/agents/inbox/alice/sub/chat") is None
    url = "https://x.dev/agents/inbox/alice/sub/unknown/x"
    assert parse_sub_agent_path(url, known_classes=["Inbox"]) is None
    match = parse_sub_agent_path(url)
    assert match is not None and match.child_class == "Unknown"


def test_path_keys_and_json_round_trip() -> None:
    assert path_key([INBOX, CHAT]) == "Inbox:alice/ChatRoom:room%201"
    assert path_from_json(path_to_json([INBOX, CHAT])) == [INBOX, CHAT]
    with pytest.raises(ValueError, match="Not an agent path"):
        path_from_json('[["only-one"]]')


def test_identities_are_unique_to_the_path() -> None:
    first = identity_name("c1", [INBOX, AgentPathStep("Chat", "c1")])
    other = identity_name(
        "c1", [AgentPathStep("Inbox", "bob"), AgentPathStep("Chat", "c1")]
    )
    assert first.startswith("cf-agents:v2:c1:") and first != other


def test_build_agent_path_and_url() -> None:
    assert build_agent_path([INBOX]) == "/agents/inbox/alice"
    assert (
        build_agent_path([INBOX, CHAT], leaf_path="history", prefix="api/v1")
        == "/api/v1/inbox/alice/sub/chat-room/room%201/history"
    )
    assert build_agent_path([INBOX], root_binding="MAIL") == "/agents/mail/alice"
    assert (
        build_agent_url("wss://x.dev", [INBOX, CHAT])
        == "wss://x.dev/agents/inbox/alice/sub/chat-room/room%201"
    )


@pytest.mark.parametrize(
    ("path", "options", "error"),
    [
        ([], {}, "at least one step"),
        ([AgentPathStep("Sub", "x")], {}, "reserved"),
        ([INBOX, AgentPathStep("Chat", "a\0b")], {}, "isn't routable"),
        ([AgentPathStep("Inbox", "sub")], {}, "reserved"),
        ([INBOX], {"prefix": "a/../b"}, "isn't routable"),
        ([INBOX], {"leaf_path": "/a?x=1"}, "plain pathname"),
    ],
)
def test_unroutable_paths_are_rejected(
    path: list[AgentPathStep], options: dict, error: str
) -> None:
    with pytest.raises(ValueError, match=error):
        build_agent_path(path, **options)


def test_build_agent_url_rejects_non_origins() -> None:
    with pytest.raises(ValueError, match="origin"):
        build_agent_url("https://x.dev/path", [INBOX])
