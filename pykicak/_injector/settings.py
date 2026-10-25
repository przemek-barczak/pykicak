"""How an Injector retries, how long it waits on a blocked connection, and its interval."""

from __future__ import annotations

from pykicak.abstracts import _finite_float

_MAX_RABBITMQ_ATTEMPTS = 3
_INITIAL_RETRY_DELAY_SECONDS = 1.0  # attempts: immediately, after 1 second, after 2 more seconds
_DEFAULT_BLOCKED_TIMEOUT_SECONDS = 60.0


def _retry_delay_seconds(attempt: int) -> float:
    """Return the wait after the failed one-based `attempt`: 1 second, then 2."""
    return _INITIAL_RETRY_DELAY_SECONDS * 2.0 ** (attempt - 1)


def _checked_blocked_timeout_seconds(blocked_timeout_seconds: object) -> float:
    """Return `blocked_timeout_seconds` if it is a finite number greater than 0, `bool` excluded.

    Raises `TypeError` or `ValueError` otherwise: an Injector always gives up eventually, so there
    is no value for waiting until RabbitMQ's alarm clears.
    """
    if isinstance(blocked_timeout_seconds, bool) or not isinstance(
        blocked_timeout_seconds, int | float
    ):
        raise TypeError("blocked_timeout_seconds must be a number")
    seconds = _finite_float(blocked_timeout_seconds)
    if seconds is None or seconds <= 0:
        raise ValueError("blocked_timeout_seconds must be a finite number greater than 0")
    return seconds


def _check_interval_seconds(interval_seconds: object) -> None:
    """Raise unless `interval_seconds` is a finite int or float, `bool` excluded.

    `TypeError` for anything but a number, `ValueError` for NaN, infinity, or an int too large
    for a float; 0 or less is valid and means a single injection.
    """
    if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int | float):
        raise TypeError("interval_seconds must be a number")
    if _finite_float(interval_seconds) is None:
        raise ValueError("interval_seconds must be a finite number")
