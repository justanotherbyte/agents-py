from agents import AgentsException
from agents.core import RetryOptions, SqlError


def test_retry_options_defaults_match_upstream() -> None:
    options = RetryOptions()
    assert (options.max_attempts, options.base_delay, options.max_delay) == (
        3,
        0.1,
        3.0,
    )


def test_sdk_exceptions_share_the_base() -> None:
    assert issubclass(SqlError, AgentsException)
