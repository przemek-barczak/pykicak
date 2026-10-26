"""The `Message handled` line an Executor logs for every delivery: its outcome, times, and sizes."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum

import pika

from pykicak._executor import logger
from pykicak._executor.headers import (
    _queued_seconds,
    _retry_round_of,
    _sent_message_id,
    _sent_type,
)
from pykicak._executor.status import _DeadLetterReason, _Delivery, _logged_outcome, _Outcome
from pykicak.abstracts import _log_values, _monotonic, _now
from pykicak.messages import KicakMessage


class _TimedPart(StrEnum):
    """A part of handling a delivery whose duration the `Message handled` line reports."""

    EXECUTE = "execute"
    """The calls of `execute()`."""

    BACKOFF = "backoff"
    """The waits between the calls of `execute()` retried after `TransientProcessingError`."""

    PUBLISH = "publish"
    """Verifying the output side, publishing the results, and committing them."""

    HELD = "held"
    """The waits while the results RabbitMQ returned as unroutable are held."""


@dataclass(slots=True)
class _Handling:
    """How the handling of one delivery went, for the `Message handled` line logged at its end.

    Times are of `_monotonic()`, durations in seconds. Created with `start()` when the delivery is
    taken; only the thread running `run()` uses it.
    """

    started_at: float
    queued_seconds: float | None = None
    """How long the delivery waited since it was published: see `_queued_seconds()`."""

    input_bytes: int | None = None
    """The size of the delivery's body, or None for a delivery without one."""

    results: int = 0
    """How many results `execute()` returned, once they are validated and serialized."""

    result_bytes: int = 0
    """The size of all the results' bodies together."""

    largest_result_bytes: int = 0
    """The size of the largest result's body, the one RabbitMQ's `max_message_size` limits."""

    seconds: dict[_TimedPart, float] = field(default_factory=lambda: dict.fromkeys(_TimedPart, 0.0))
    settle_started_at: float | None = None
    """When settling the delivery began, the last part of its handling, or None if it did not."""

    @classmethod
    def start(cls, properties: pika.BasicProperties | None, body: bytes | None) -> _Handling:
        """Start the handling of a delivery, which waited in its queue since it was published."""
        return cls(
            started_at=_monotonic(),
            queued_seconds=_queued_seconds(properties, _now()),
            input_bytes=None if body is None else len(body),
        )

    @contextmanager
    def timing(self, part: _TimedPart) -> Iterator[None]:
        """Add how long the `with` block takes to the duration of `part`, also if it raises."""
        started_at = _monotonic()
        try:
            yield
        finally:
            self.seconds[part] += _monotonic() - started_at

    def start_settling(self) -> None:
        """Note that settling begins: a copy to publish, or the acknowledgement or rejection.

        Only the first call counts, so the copy and the acknowledgement after it are one part.
        """
        if self.settle_started_at is None:
            self.settle_started_at = _monotonic()

    def record_results(self, bodies: list[bytes]) -> None:
        """Note how many results the delivery has, and their sizes.

        Sizes are of the bodies only: RabbitMQ's `max_message_size` limits the body of each
        message, not its headers.
        """
        sizes = [len(body) for body in bodies]
        self.results = len(sizes)
        self.result_bytes = sum(sizes)
        self.largest_result_bytes = max(sizes, default=0)

    def log_handled(
        self,
        outcome: _Outcome | _DeadLetterReason,
        delivery: _Delivery[KicakMessage] | None,
        *,
        queue: str,
        delivery_tag: int,
        redelivered: bool,
        properties: pika.BasicProperties | None,
        accepted_types: Mapping[str, type[KicakMessage]],
    ) -> None:
        """Log the `Message handled` line of a delivery: how it ended, and what its parts took.

        One line at `INFO` for every delivery counted in `ExecutorInfo.counts`, whatever its
        outcome, so that statistics such as averages can be computed from the logs. The record
        also carries the line's values, typed, as its `kicak` attribute, for structured (e.g.
        JSON) log handlers. A delivery that was not decoded (`delivery` None) is named as its
        dead-letter line names it: by the class of `accepted_types` its `type` property names,
        or None, and its sender's `message_id` property.
        """
        if not logger.isEnabledFor(logging.INFO):
            return
        finished_at = _monotonic()
        if delivery is None:
            sent_type = _sent_type(properties)
            accepted = None if sent_type is None else accepted_types.get(sent_type)
            type_name = None if accepted is None else accepted.__name__
            message_id = _sent_message_id(properties)
            attempts = 0
        else:
            type_name = type(delivery.message).__name__
            message_id = delivery.message.message_id
            attempts = delivery.attempt
        logged_outcome, reason = _logged_outcome(outcome)
        settle_started_at = self.settle_started_at
        seconds = self.seconds
        fields: dict[str, object] = {
            "outcome": logged_outcome,
            "reason": reason,
            "type": type_name,
            "id": message_id,
            "queue": queue,
            "delivery_tag": delivery_tag,
            "redelivered": redelivered,
            "retry_round": _retry_round_of(None if properties is None else properties.headers),
            "attempts": attempts,
            "results": self.results,
            "input_bytes": self.input_bytes,
            "result_bytes": self.result_bytes,
            "largest_result_bytes": self.largest_result_bytes,
            "total_seconds": finished_at - self.started_at,
            "execute_seconds": seconds[_TimedPart.EXECUTE],
            "backoff_seconds": seconds[_TimedPart.BACKOFF],
            "publish_seconds": seconds[_TimedPart.PUBLISH],
            "held_seconds": seconds[_TimedPart.HELD],
            "settle_seconds": 0.0 if settle_started_at is None else finished_at - settle_started_at,
            "queued_seconds": self.queued_seconds,
        }
        _log_values(logger, "Message handled", "message_handled", fields)
