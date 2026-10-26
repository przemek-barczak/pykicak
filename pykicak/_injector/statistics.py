"""The `Message injected` line an Injector logs for every message it hands to RabbitMQ."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum

from pykicak._injector import logger
from pykicak.abstracts import _log_values, _monotonic
from pykicak.messages import KicakMessage


class _TimedPart(StrEnum):
    """A part of an injection whose duration the `Message injected` line reports."""

    CONNECT = "connect"
    """Opening a connection and declaring the exchange, when the attempt needs a connection."""

    PUBLISH = "publish"
    """Publishing the message and waiting for RabbitMQ's confirmation."""

    BACKOFF = "backoff"
    """The time between two attempts: discarding the failed connection and waiting to retry."""


@dataclass(slots=True)
class _Injection:
    """How the injection of one message goes, for the `Message injected` line logged at its end.

    Times are of `_monotonic()`, durations in seconds.
    """

    message: KicakMessage
    body: bytes
    started_at: float
    generate_seconds: float | None
    """How long `generate()` took, or None for a message given to `publish()`."""

    attempts: int = 0
    seconds: dict[_TimedPart, float] = field(default_factory=lambda: dict.fromkeys(_TimedPart, 0.0))
    attempt_ended_at: float = 0.0
    error: BaseException | None = None
    """What ended the last attempt, if the message was not injected."""

    @contextmanager
    def timing(self, part: _TimedPart) -> Iterator[None]:
        """Add how long the `with` block takes to the duration of `part`, also if it raises."""
        started_at = _monotonic()
        try:
            yield
        finally:
            self.seconds[part] += _monotonic() - started_at

    @contextmanager
    def attempt(self) -> Iterator[None]:
        """Count an attempt, adding the time since the previous one ended to the backoff."""
        if self.attempts:
            self.seconds[_TimedPart.BACKOFF] += _monotonic() - self.attempt_ended_at
        self.attempts += 1
        try:
            yield
        finally:
            self.attempt_ended_at = _monotonic()

    def log_injected(self, exchange_name: str) -> None:
        """Log the `Message injected` line: whether the message was injected, and what it took.

        One line at `INFO` for every message handed to RabbitMQ, by `run()` or `publish()`,
        whatever the outcome, so that statistics can be computed from the logs; nothing is
        logged for a message that was never published, e.g. because `generate()` returned None
        or raised. The record also carries the values, typed, as its `kicak` attribute.
        """
        if not logger.isEnabledFor(logging.INFO):
            return
        message = self.message
        error = self.error
        seconds = self.seconds
        _log_values(
            logger,
            "Message injected",
            "message_injected",
            {
                "outcome": "injected" if error is None else "failed",
                "error": None if error is None else type(error).__name__,
                "type": type(message).__name__,
                "id": message.message_id,
                "exchange": exchange_name,
                "attempts": self.attempts,
                "message_bytes": len(self.body),
                "total_seconds": _monotonic() - self.started_at,
                "generate_seconds": self.generate_seconds,
                "connect_seconds": seconds[_TimedPart.CONNECT],
                "publish_seconds": seconds[_TimedPart.PUBLISH],
                "backoff_seconds": seconds[_TimedPart.BACKOFF],
            },
        )
