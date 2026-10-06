"""Telling platform failures apart from application failures.

Port of upstream ``retries.ts`` (``isPlatformFailure`` and friends). Work that
fails because of the platform (a deploy replaced the isolate, storage reset,
a dropped connection, a memory-limit reset) is preserved and re-run later;
work that fails because of its own code is not. A JS error's message and its
``retryable`` / ``overloaded`` flags are readable on the Python exception
(``.design/platform_verification.md`` §2.5).
"""

import re
from collections.abc import Iterator

__all__ = (
    "is_code_update_reset",
    "is_error_retryable",
    "is_memory_limit_reset",
    "is_platform_failure",
    "is_platform_transient_error",
    "is_reset_error",
    "is_storage_reset",
)

# The verbatim platform messages; kept narrow so an ordinary error that
# happens to mention "upgraded" or "reset" isn't misclassified.
_SUPERSEDED_ISOLATE = re.compile(
    r"reset because its code was updated|this script has been upgraded", re.I
)
_CONNECTION_LOST = re.compile(r"network connection lost", re.I)
_STORAGE_RESET = re.compile(
    r"Internal error in Durable Object storage caused object to be reset", re.I
)
# Deliberately the broad shared fragment: real surfacings truncate the tail,
# and a missed match means the memory-limit circuit breaker never engages.
_MEMORY_LIMIT_RESET = re.compile(r"exceeded its memory limit", re.I)

_MAX_CAUSE_DEPTH = 8


def _self_and_causes(error: BaseException) -> Iterator[BaseException]:
    # Wrappers such as SqlError keep the platform error as __cause__ and may
    # drop its flags, so classification looks through the chain.
    current: BaseException | None = error
    for _ in range(_MAX_CAUSE_DEPTH):
        if current is None:
            return
        yield current
        current = current.__cause__


def _matches(error: BaseException, pattern: re.Pattern[str]) -> bool:
    return any(pattern.search(str(item)) for item in _self_and_causes(error))


def is_error_retryable(error: BaseException) -> bool:
    """Return whether the platform marked ``error`` retryable (and not overloaded)."""
    return (
        getattr(error, "retryable", False) is True
        and getattr(error, "overloaded", False) is not True
        and "Durable Object is overloaded" not in str(error)
    )


def is_code_update_reset(error: BaseException) -> bool:
    """Return whether ``error`` means a deploy superseded this isolate."""
    return _matches(error, _SUPERSEDED_ISOLATE)


def is_storage_reset(error: BaseException) -> bool:
    """Return whether ``error`` is the Durable Object storage-reset signal."""
    return _matches(error, _STORAGE_RESET)


def is_reset_error(error: BaseException) -> bool:
    """Return whether ``error`` means this Durable Object is being reset."""
    return is_code_update_reset(error) or is_storage_reset(error)


def is_memory_limit_reset(error: BaseException) -> bool:
    """Return whether ``error`` is a Durable Object memory-limit reset.

    Unlike a transient, re-running the same work re-exceeds the limit, so
    callers bound retries tightly and then give up (upstream #1825).
    """
    return _matches(error, _MEMORY_LIMIT_RESET)


def is_platform_transient_error(error: BaseException) -> bool:
    """Return whether ``error`` is a transient platform failure.

    A superseded isolate, a dropped connection, a storage reset, or an error
    the platform flags retryable. The same work succeeds once the platform
    recovers.
    """
    return any(
        _SUPERSEDED_ISOLATE.search(message := str(item))
        or _CONNECTION_LOST.search(message)
        or _STORAGE_RESET.search(message)
        or is_error_retryable(item)
        for item in _self_and_causes(error)
    )


def is_platform_failure(error: BaseException) -> bool:
    """Return whether ``error`` is the platform's fault rather than the code's.

    Failed work in this class is preserved and deferred, never completed as
    an application failure.
    """
    return is_platform_transient_error(error) or is_memory_limit_reset(error)
