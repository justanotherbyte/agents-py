"""Agent paths: ``/sub/`` URLs, path keys, and facet identity names.

Port of upstream ``sub-routing.ts`` (parsing and ``buildAgentPath`` /
``buildAgentUrl``) and ``dynamic-agents/identity.ts`` (path-scoped identity
names; ``.design/subagents_engine.md``).
"""

import hashlib
import json
from collections.abc import Iterable, Sequence
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from ..core.naming import camel_case_to_kebab_case
from .types import AgentPathStep, SubAgentPathMatch

__all__ = (
    "SUB_PREFIX",
    "build_agent_path",
    "build_agent_url",
    "identity_name",
    "is_same_prefix",
    "parse_sub_agent_path",
    "path_from_json",
    "path_key",
    "path_to_json",
    "rewrite_pathname",
)

SUB_PREFIX = "sub"
"""The URL segment that separates each parent-to-child hop."""

_IDENTITY_PREFIX = "cf-agents:v2:"
# encodeURIComponent leaves these unescaped.
_URI_COMPONENT_SAFE = "-_.!~*'()"


def _encode(value: str) -> str:
    return quote(value, safe=_URI_COMPONENT_SAFE)


def parse_sub_agent_path(
    url: str, known_classes: Iterable[str] | None = None
) -> SubAgentPathMatch | None:
    """Return the first ``/sub/{class}/{name}`` hop in ``url``'s path, or ``None``.

    With ``known_classes``, a class segment must be the kebab case of one of
    them (and resolves to it); without, it's converted back to CamelCase.
    """
    parts = [part for part in urlsplit(url).path.split("/") if part]
    known = list(known_classes) if known_classes is not None else None
    for index, part in enumerate(parts):
        if part != SUB_PREFIX or index + 2 >= len(parts):
            continue
        child_class = _resolve_class(parts[index + 1], known)
        if child_class is None:
            continue
        rest = parts[index + 3 :]
        return SubAgentPathMatch(
            child_class=child_class,
            child_name=unquote(parts[index + 2]),
            remaining_path="/" + "/".join(rest) if rest else "/",
        )
    return None


def _resolve_class(segment: str, known: list[str] | None) -> str | None:
    if known is not None:
        return next(
            (name for name in known if camel_case_to_kebab_case(name) == segment), None
        )
    return "".join(word[:1].upper() + word[1:] for word in segment.split("-"))


def path_key(path: Sequence[AgentPathStep]) -> str:
    """Return a path's stable key (``Class:name/Class:name``, URL-encoded)."""
    return "/".join(f"{_encode(step.class_name)}:{_encode(step.name)}" for step in path)


def path_to_json(path: Sequence[AgentPathStep]) -> str:
    """Return a path as JSON (route addresses and stored parent paths)."""
    return json.dumps([[step.class_name, step.name] for step in path])


def path_from_json(text: str) -> list[AgentPathStep]:
    """Parse a path stored by `path_to_json`.

    Raises
    ------
    ValueError
        If ``text`` isn't a path.
    """
    raw = json.loads(text)
    if not isinstance(raw, list) or not all(
        isinstance(step, list)
        and len(step) == 2
        and all(isinstance(v, str) for v in step)
        for step in raw
    ):
        raise ValueError(f"Not an agent path: {text!r}")
    return [AgentPathStep(class_name=step[0], name=step[1]) for step in raw]


def identity_name(name: str, child_path: Sequence[AgentPathStep]) -> str:
    """Return a facet's identity name, unique to its path from the root.

    Two parents' children with the same name get different identities (and
    so different storage).
    """
    digest = hashlib.sha256(path_to_json(child_path).encode()).hexdigest()
    return f"{_IDENTITY_PREFIX}{_encode(name)}:{digest}"


def is_same_prefix(
    prefix: Sequence[AgentPathStep], path: Sequence[AgentPathStep]
) -> bool:
    """Return whether ``path`` starts with ``prefix``."""
    return len(path) >= len(prefix) and tuple(path[: len(prefix)]) == tuple(prefix)


def rewrite_pathname(url: str, pathname: str) -> str:
    """Return ``url`` with its path replaced by ``pathname`` (query kept)."""
    parts = urlsplit(url)
    return urlunsplit(
        (parts.scheme, parts.netloc, pathname, parts.query, parts.fragment)
    )


def build_agent_path(
    path: Sequence[AgentPathStep],
    *,
    prefix: str = "agents",
    leaf_path: str | None = None,
    root_binding: str | None = None,
) -> str:
    """Return the routable path of an agent (``/agents/root/name/sub/...``).

    Parameters
    ----------
    path
        The agent's path, root first (e.g. an agent's ``self_path``).
    prefix
        The routing prefix (``route_agent_request``'s ``prefix``).
    leaf_path
        A path to append after the agent (e.g. ``/history``).
    root_binding
        The root's env binding name, when it differs from its class name.

    Raises
    ------
    ValueError
        If a part isn't routable (an empty path, a class named ``Sub``, a
        name that's ``.``, ``..``, or contains NUL, a prefix or leaf path
        that isn't a plain pathname).
    """
    if not path:
        raise ValueError("An agent path needs at least one step")
    root, *children = path
    parts = [
        _check_prefix(prefix),
        _class_segment(root_binding or root.class_name),
        _check_root_name(root.name),
    ]
    for child in children:
        parts += [
            SUB_PREFIX,
            _class_segment(child.class_name),
            _child_segment(child.name),
        ]
    leaf = _check_leaf(leaf_path) if leaf_path else ""
    return "/" + "/".join(parts) + leaf


def build_agent_url(
    origin: str,
    path: Sequence[AgentPathStep],
    *,
    prefix: str = "agents",
    leaf_path: str | None = None,
    root_binding: str | None = None,
) -> str:
    """Return an agent's URL on ``origin`` (``https://``, ``wss://``, ...).

    Raises
    ------
    ValueError
        If ``origin`` isn't a bare HTTP(S)/WS(S) origin, or the path isn't
        routable (see `build_agent_path`).
    """
    parts = urlsplit(origin)
    if (
        parts.scheme not in ("http", "https", "ws", "wss")
        or not parts.netloc
        or "@" in parts.netloc
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
    ):
        raise ValueError(
            f"Invalid agent URL origin {origin!r}: pass an HTTP(S) or WS(S) "
            "origin without credentials, a path, query, or fragment"
        )
    built = build_agent_path(
        path, prefix=prefix, leaf_path=leaf_path, root_binding=root_binding
    )
    return f"{parts.scheme}://{parts.netloc}{built}"


def _check_prefix(prefix: str) -> str:
    parts = prefix.split("/")
    if any(
        not part or part in (".", "..", SUB_PREFIX) or _unsafe(part) for part in parts
    ):
        raise ValueError(f"Routing prefix {prefix!r} isn't routable")
    return prefix


def _class_segment(class_name: str) -> str:
    segment = camel_case_to_kebab_case(class_name)
    if segment == SUB_PREFIX:
        raise ValueError(
            f'Agent class {class_name!r} is reserved: it kebab-cases to "sub"'
        )
    if not segment or segment in (".", "..") or _unsafe(segment) or "/" in segment:
        raise ValueError(f"Agent class {class_name!r} isn't routable")
    return segment


def _check_root_name(name: str) -> str:
    if name == SUB_PREFIX:
        raise ValueError('The root agent name "sub" is reserved')
    if (
        not name
        or name in (".", "..")
        or "/" in name
        or _unsafe(name)
        or name != _encode(name)
    ):
        raise ValueError(f"Root agent name {name!r} isn't routable")
    return name


def _child_segment(name: str) -> str:
    if not name or name in (".", "..") or "\0" in name:
        raise ValueError(f"Child agent name {name!r} isn't routable")
    return _encode(name)


def _check_leaf(leaf_path: str) -> str:
    leaf = leaf_path if leaf_path.startswith("/") else f"/{leaf_path}"
    if "//" in leaf or (len(leaf) > 1 and leaf.endswith("/")) or _unsafe(leaf):
        raise ValueError(f"Leaf path {leaf_path!r} isn't a plain pathname")
    return leaf


def _unsafe(text: str) -> bool:
    return any(char in text for char in "?#\\") or any(
        part in (".", "..") for part in text.split("/")
    )
