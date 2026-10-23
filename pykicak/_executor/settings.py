"""How an Executor retries: processing attempts, retry rounds, delays, and checks of its input."""

from __future__ import annotations

import math
from dataclasses import dataclass

_INITIAL_RETRY_DELAY_SECONDS = 0.5
_MAX_RETRY_DELAY_SECONDS = 30.0
_MAX_BACKOFF_EXPONENT = 16
_MAX_RETRY_DELAY_MILLISECONDS = 2**32 - 1  # about 49.7 days; RabbitMQ refuses far larger ones
_STOP_POLL_SECONDS = 1.0
"""Longest wait, for a message, a reconnection, or held results, before checking `stop()`."""


def _capped_backoff_seconds(attempt: int) -> float:
    """Return the exponential backoff delay for zero-based `attempt`, capped at 30 seconds."""
    return min(
        _INITIAL_RETRY_DELAY_SECONDS * 2.0 ** min(attempt, _MAX_BACKOFF_EXPONENT),
        _MAX_RETRY_DELAY_SECONDS,
    )


def _checked_count(argument: str, value: object, minimum: int, condition: str) -> int:
    """Return `value` if it is an int of at least `minimum`; `condition` says so in the error."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{argument} must be an int")
    if value < minimum:
        raise ValueError(f"{argument} must be {condition}")
    return value


def _checked_seconds(argument: str, value: object) -> float:
    """Return `value` if it is a finite number of seconds, 0 or more."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{argument} must be a number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{argument} must be a finite number, 0 or more")
    return float(value)


@dataclass(frozen=True, slots=True, kw_only=True)
class _ExecutorSettings:
    """How an Executor retries processing a message, and how often it checks its input side.

    Created with `checked()`, which validates the constructor's arguments.
    """

    max_processing_attempts: int
    """How many times `execute()` is called for one delivery while it fails transiently."""

    max_retry_rounds: int
    """How many times a message may be moved to the retry queue before it is dead-lettered."""

    retry_delay_milliseconds: int
    """How long a message waits in the retry queue, in RabbitMQ's unit."""

    input_check_seconds: float
    """How often the input side is verified while waiting for a message."""

    @classmethod
    def checked(
        cls,
        *,
        max_processing_attempts: object,
        max_retry_rounds: object,
        retry_delay_seconds: object,
        input_check_seconds: object,
    ) -> _ExecutorSettings:
        """Return the settings of these arguments, raising `TypeError` or `ValueError` if invalid.

        Counts must be ints, `bool` excluded: `max_processing_attempts` at least 1 and
        `max_retry_rounds` at least 0. Durations must be finite numbers, 0 or more, and
        `retry_delay_seconds` at most 2³²−1 milliseconds, the largest expiration RabbitMQ takes.
        """
        attempts = _checked_count(
            "max_processing_attempts", max_processing_attempts, 1, "at least 1"
        )
        rounds = _checked_count("max_retry_rounds", max_retry_rounds, 0, "0 or more")
        input_check = _checked_seconds("input_check_seconds", input_check_seconds)
        retry_delay = _checked_seconds("retry_delay_seconds", retry_delay_seconds)
        milliseconds = round(retry_delay * 1000)
        if milliseconds > _MAX_RETRY_DELAY_MILLISECONDS:
            raise ValueError(
                f"retry_delay_seconds must be at most {_MAX_RETRY_DELAY_MILLISECONDS / 1000}"
            )
        return cls(
            max_processing_attempts=attempts,
            max_retry_rounds=rounds,
            retry_delay_milliseconds=milliseconds,
            input_check_seconds=input_check,
        )

    @property
    def retry_delay_seconds(self) -> float:
        """How long a message waits in the retry queue, in seconds."""
        return self.retry_delay_milliseconds / 1000
