"""Converting class names to the kebab-case names agent URLs use."""

import re

__all__ = ("camel_case_to_kebab_case",)

_UPPER = re.compile(r"[A-Z]")


def camel_case_to_kebab_case(name: str) -> str:
    """Return ``name`` in kebab case, as upstream's ``camelCaseToKebabCase``.

    ``ChatAgent`` becomes ``chat-agent``; an all-uppercase name is lowercased
    with ``_`` turned into ``-`` (``MY_AGENT`` becomes ``my-agent``).
    """
    if name == name.upper() and name != name.lower():
        return name.lower().replace("_", "-")
    kebab = _UPPER.sub(lambda match: f"-{match.group().lower()}", name)
    kebab = kebab.removeprefix("-")
    kebab = kebab.replace("_", "-")
    return kebab.removesuffix("-")
