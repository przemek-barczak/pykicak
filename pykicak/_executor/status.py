"""What an Executor reports through `get_info()`: its status, the delivery, and the counts."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum, StrEnum, auto
from typing import assert_never

from pykicak._executor import logger
from pykicak.abstracts import _monotonic, _now
from pykicak.messages import KicakMessage


class ExecutorStatus(StrEnum):
    """Lifecycle status of an Executor, reported as `ExecutorInfo.status` by `get_info()`.

    Members are strings, so they compare equal to their values, e.g. `"WAITING_FOR_MESSAGE"`.
    The first attempt to connect, or to verify the topology, keeps the current status; only a
    failed attempt enters a recovery status.
    """

    __module__ = "pykicak.executor"  # where it is public

    CREATED = "CREATED"
    """Created; `run()` has not consumed yet. It stays so during the first connection attempt."""

    UPSTREAM_RECOVER = "UPSTREAM_RECOVER"
    """The connection was lost, or connecting or verifying the input side failed; retrying.

    Also after RabbitMQ cancelled the consumer, and from a `run()` called again after a crash.
    """

    WAITING_FOR_MESSAGE = "WAITING_FOR_MESSAGE"
    """The input side is verified; waiting for the next delivery."""

    PROCESSING = "PROCESSING"
    """Decoding a delivery and running `execute()`, including its retries.

    A delivery with no results to publish is also acknowledged, dead-lettered, or moved to the
    retry queue in this status, and the first verification of the output side happens in it.
    """

    DOWNSTREAM_RECOVER = "DOWNSTREAM_RECOVER"
    """RabbitMQ returned the results as unroutable; holding the input and results, retrying.

    It lasts until the output side is verified again and the results are committed.
    """

    SENDING_RESULT = "SENDING_RESULT"
    """The output side is verified; publishing and committing the results, then acknowledging."""

    STOPPING = "STOPPING"
    """`stop()` was called; `run()` finishes the message in progress, then returns."""

    STOPPED = "STOPPED"
    """`run()` returned after a graceful stop."""

    CRASHED = "CRASHED"
    """`run()` ended by raising an exception: a failure or a hard stop, not a graceful stop."""


_RECOVERY_STATUSES = (ExecutorStatus.UPSTREAM_RECOVER, ExecutorStatus.DOWNSTREAM_RECOVER)


@dataclass(frozen=True, slots=True, kw_only=True)
class DeadLetterCounts:
    """How many deliveries an Executor dead-lettered, by reason: `ExecutorCounts.dead_lettered`.

    The reasons are the values of the `x-kicak-dead-letter-reason` header of the copies in the
    dead-letter queue. A delivery counts once RabbitMQ has confirmed its copy and the delivery is
    acknowledged.
    """

    __module__ = "pykicak.executor"  # where it is public

    malformed: int = 0
    """`MalformedMessageError`: from `execute()`, or for a delivery or results that are invalid."""

    retries_exhausted: int = 0
    """`TransientProcessingError` persisted after `max_retry_rounds` rounds in the retry queue."""

    result_too_large: int = 0
    """RabbitMQ refused one of the delivery's results as larger than its `max_message_size`."""

    unexpected_error: int = 0
    """Any other exception, treated as a bug: each one also stopped the Executor."""

    @property
    def total(self) -> int:
        """All dead-lettered deliveries, whatever the reason."""
        return (
            self.malformed + self.retries_exhausted + self.result_too_large + self.unexpected_error
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutorCounts:
    """How the deliveries an Executor took ended: `ExecutorInfo.counts`.

    Each delivery the Executor starts to process counts once, when it is settled or left to
    RabbitMQ. The counts start at zero when the Executor is created and only grow, across
    `run()` called again after a crash, so they suit monitoring counters such as Prometheus's.
    They count deliveries, not messages: a message delivered again, e.g. after a lost connection,
    counts again. A delivery received after `stop()` is not processed and not counted.
    """

    __module__ = "pykicak.executor"  # where it is public

    completed: int = 0
    """Executed, its results published, if any, and acknowledged."""

    dead_lettered: DeadLetterCounts = field(default_factory=DeadLetterCounts)
    """Copied to the dead-letter queue and acknowledged, by reason."""

    moved_to_retry: int = 0
    """Copied to the retry queue after `TransientProcessingError` persisted, and acknowledged."""

    requeued: int = 0
    """Returned to the queue with `basic_nack(requeue=True)`.

    RabbitMQ did not take its copy for the retry queue or the dead-letter queue, or `stop()`
    ended a hold of its unroutable results.
    """

    abandoned: int = 0
    """Neither acknowledged nor requeued by the Executor, so RabbitMQ delivers them again.

    The connection or channel was lost, RabbitMQ took the delivery back because it outlasted
    `consumer_timeout`, or `run()` ended by raising an exception while processing it. A growing
    number usually means that processing outlasts `heartbeat_seconds` or `consumer_timeout`, so
    messages are executed again.
    """


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class ExecutorInfo[Accepted: KicakMessage]:
    """What an Executor is doing, as returned by `KicakExecutorAbstract.get_info()`.

    One consistent snapshot: every field describes the same moment. Times are timezone-aware
    `datetime` objects in UTC, so they can be compared and subtracted directly, e.g.
    `info.status_checked_at - info.status_changed_at` is how long the status has lasted; use
    `.isoformat()` for text such as `"2026-10-01T17:05:42.123456+00:00"`. The repr shows the
    message only by its type and `message_id`, never its contents, so logging the snapshot is safe.
    """

    __module__ = "pykicak.executor"  # where it is public

    status: ExecutorStatus
    """The current status; a string enum, so `info.status == "STOPPED"` works too."""

    status_changed_at: datetime
    """When the status was entered."""

    status_checked_at: datetime
    """When this snapshot was taken."""

    stop_requested: bool
    """Whether `stop()` has been called; it stays True after the stop."""

    stop_requested_at: datetime | None
    """When `stop()` was first called, or None if it has not been called."""

    message: Accepted | None
    """The input message being processed, of one of the types the Executor accepts, or None.

    Set from when the delivery is decoded until it is acknowledged, dead-lettered, moved to the
    retry queue, or requeued, or the connection is lost; None while waiting or recovering, for a
    delivery that cannot be decoded, and once `run()` has ended. It is the same object
    `execute()` receives, so treat it as read-only: the message is frozen, but lists or dicts
    inside it are shared with `execute()`.
    """

    attempt: int | None
    """Which call of `execute()` for `message` this is: from 1 to `max_processing_attempts`.

    It counts the calls for this delivery only, so it starts at 1 again when the message is
    delivered again or comes back from the retry queue. None when `message` is None.
    """

    redelivered: bool | None
    """Whether RabbitMQ delivered `message` before, without it being acknowledged or rejected.

    RabbitMQ's redelivered flag: True after a lost connection, a crash, a requeue, or an expired
    `consumer_timeout`. When instances keep crashing or losing their connection, a message that
    keeps arriving redelivered may be the cause. A message back from the retry queue is a new
    delivery, so it is not redelivered. None when `message` is None.
    """

    retry_round: int | None
    """How often `message` was moved to the retry queue before: its `x-kicak-retry-round` header.

    0 for a message that never was, and for one sent back from the dead-letter queue, which
    starts its rounds again. None when `message` is None.
    """

    counts: ExecutorCounts
    """How the deliveries the Executor took have ended, since it was created."""

    def __repr__(self) -> str:
        message = self.message
        message_repr = (
            "None"
            if message is None
            else f"<{type(message).__name__} message_id={message.message_id!r}>"
        )
        return (
            f"{type(self).__name__}(status={self.status!r}, "
            f"status_changed_at={self.status_changed_at!r}, "
            f"status_checked_at={self.status_checked_at!r}, "
            f"stop_requested={self.stop_requested!r}, "
            f"stop_requested_at={self.stop_requested_at!r}, message={message_repr}, "
            f"attempt={self.attempt!r}, redelivered={self.redelivered!r}, "
            f"retry_round={self.retry_round!r}, counts={self.counts!r})"
        )


class _DeadLetterReason(StrEnum):
    """Why the executor dead-lettered a delivery: its `x-kicak-dead-letter-reason` header."""

    MALFORMED = "malformed"
    """`MalformedMessageError`: the delivery, or the results `execute()` returned, are invalid."""

    RETRIES_EXHAUSTED = "retries-exhausted"
    """`TransientProcessingError` persisted after `max_retry_rounds` rounds in the retry queue."""

    RESULT_TOO_LARGE = "result-too-large"
    """RabbitMQ refused one of its results as larger than its `max_message_size`."""

    UNEXPECTED_ERROR = "unexpected-error"
    """Any other exception, treated as a bug; the executor stopped."""


class _Outcome(Enum):
    """How a delivery ended, unless it was dead-lettered (`_DeadLetterReason`)."""

    COMPLETED = auto()
    MOVED_TO_RETRY = auto()
    REQUEUED = auto()
    ABANDONED = auto()
    """Left to RabbitMQ: never settled by the Executor."""


def _counted(counts: ExecutorCounts, outcome: _Outcome | _DeadLetterReason) -> ExecutorCounts:
    """Return `counts` with one more delivery that ended with `outcome`."""
    dead_lettered = counts.dead_lettered
    match outcome:
        case _Outcome.COMPLETED:
            return replace(counts, completed=counts.completed + 1)
        case _Outcome.MOVED_TO_RETRY:
            return replace(counts, moved_to_retry=counts.moved_to_retry + 1)
        case _Outcome.REQUEUED:
            return replace(counts, requeued=counts.requeued + 1)
        case _Outcome.ABANDONED:
            return replace(counts, abandoned=counts.abandoned + 1)
        case _DeadLetterReason.MALFORMED:
            dead_lettered = replace(dead_lettered, malformed=dead_lettered.malformed + 1)
        case _DeadLetterReason.RETRIES_EXHAUSTED:
            dead_lettered = replace(
                dead_lettered, retries_exhausted=dead_lettered.retries_exhausted + 1
            )
        case _DeadLetterReason.RESULT_TOO_LARGE:
            dead_lettered = replace(
                dead_lettered, result_too_large=dead_lettered.result_too_large + 1
            )
        case _DeadLetterReason.UNEXPECTED_ERROR:
            dead_lettered = replace(
                dead_lettered, unexpected_error=dead_lettered.unexpected_error + 1
            )
        case _:  # pragma: no cover - every outcome has a case above, as type checkers verify
            assert_never(outcome)
    return replace(counts, dead_lettered=dead_lettered)


def _logged_outcome(outcome: _Outcome | _DeadLetterReason) -> tuple[str, str | None]:
    """Return how a delivery ended as the `Message handled` line names it, and why, if known.

    The outcome is named like the field of `ExecutorCounts` that counts it, and the reason of a
    dead-lettering like its `x-kicak-dead-letter-reason` header.
    """
    if isinstance(outcome, _DeadLetterReason):
        return "dead_lettered", outcome.value
    return outcome.name.lower(), None


@dataclass(frozen=True, slots=True)
class _Delivery[Accepted: KicakMessage]:
    """The decoded delivery the Executor is processing, as `get_info()` reports it."""

    message: Accepted
    attempt: int
    redelivered: bool
    retry_round: int


@dataclass(frozen=True, slots=True)
class _Activity[Accepted: KicakMessage]:
    """What `run()` is doing, replaced as a whole so `get_info()` always reads one moment."""

    status: ExecutorStatus
    entered_at: datetime
    delivery: _Delivery[Accepted] | None = None
    counts: ExecutorCounts = field(default_factory=ExecutorCounts)


class _StatusBoard[Accepted: KicakMessage]:
    """What an Executor is doing, kept without locks for `get_info()` and `stop()`.

    `stop()` may run in a signal handler and `get_info()` in a monitoring thread, so nothing here
    locks. Each field has a single writer instead: the thread running `run()` writes the
    activity and the final status, and `request_stop()` the stop request. `snapshot()` writes
    nothing and reads each field once, the final status first: a stop request is never
    withdrawn, so `STOPPED` always comes with it. The activity, the status with the delivery
    being processed and the counts, is replaced as a whole, so a reader never pairs a status with
    another status's delivery, or sees a delivery both reported and counted, or neither.
    """

    def __init__(self, queue: str) -> None:
        """Start as `CREATED`; `queue`, the Executor's source queue, names it in the logs."""
        self._queue = queue
        self._activity: _Activity[Accepted] = _Activity(ExecutorStatus.CREATED, _now())
        self._entered_monotonic = _monotonic()  # when the status was entered, for durations
        self._final_status: tuple[ExecutorStatus, datetime] | None = None
        self._stop_requested = False
        self._stop_requested_at: datetime | None = None
        self._stop_requested_monotonic: float | None = None  # _stop_requested_at, for durations

    @property
    def stop_requested(self) -> bool:
        """Whether `stop()` was called; it stays True."""
        return self._stop_requested

    @property
    def status(self) -> ExecutorStatus:
        """What `run()` is doing, whatever a stop request or the end of `run()` reports."""
        return self._activity.status

    @property
    def delivery(self) -> _Delivery[Accepted] | None:
        """The decoded delivery being processed, or None."""
        return self._activity.delivery

    @property
    def ended(self) -> bool:
        """Whether `run()` has ended, as `STOPPED` or `CRASHED`."""
        return self._final_status is not None

    def request_stop(self) -> None:
        """Record a stop request, and the time of the first one.

        Safe in a signal handler or any thread: it only assigns, without locking or logging.
        """
        if self._stop_requested_at is None:
            self._stop_requested_monotonic = _monotonic()
            self._stop_requested_at = _now()
        self._stop_requested = True

    def snapshot(self) -> ExecutorInfo[Accepted]:
        """Return what the Executor is doing now: an `ExecutorInfo`. Writes nothing."""
        final_status = self._final_status
        stop_requested_at = self._stop_requested_at
        activity = self._activity
        delivery = activity.delivery
        if final_status is not None:
            status, status_changed_at = final_status
            delivery = None
        elif stop_requested_at is not None:
            # Still finishing the message in progress, if any
            status, status_changed_at = ExecutorStatus.STOPPING, stop_requested_at
        else:
            status, status_changed_at = activity.status, activity.entered_at
        return ExecutorInfo(
            status=status,
            status_changed_at=status_changed_at,
            status_checked_at=_now(),
            stop_requested=stop_requested_at is not None,
            stop_requested_at=stop_requested_at,
            message=None if delivery is None else delivery.message,
            attempt=None if delivery is None else delivery.attempt,
            redelivered=None if delivery is None else delivery.redelivered,
            retry_round=None if delivery is None else delivery.retry_round,
            counts=activity.counts,
        )

    def set_status(self, status: ExecutorStatus) -> None:
        """Record what `run()` is doing, logging when it enters or leaves a recovery status.

        Setting the current status again keeps the time it was entered. The delivery being
        processed and the counts are kept. Leaving a recovery status logs how long it lasted, as
        `recovery_seconds`.
        """
        activity = self._activity
        previous = activity.status
        if status is previous:
            return
        entered_monotonic = _monotonic()
        previous_seconds = entered_monotonic - self._entered_monotonic
        self._activity = replace(activity, status=status, entered_at=_now())
        self._entered_monotonic = entered_monotonic
        if status in _RECOVERY_STATUSES:
            logger.warning("Executor for queue %s is recovering: %s", self._queue, status)
        elif previous in _RECOVERY_STATUSES:
            logger.info(
                "Executor for queue %s recovered from %s: %s recovery_seconds=%.3f",
                self._queue,
                previous,
                status,
                previous_seconds,
                extra={
                    "kicak": {
                        "event": "recovered",
                        "queue": self._queue,
                        "recovered_from": previous.value,
                        "status": status.value,
                        "recovery_seconds": previous_seconds,
                    }
                },
            )

    def set_delivery(self, delivery: _Delivery[Accepted]) -> None:
        """Record the decoded delivery being processed, keeping the status and the counts."""
        self._activity = replace(self._activity, delivery=delivery)

    def set_attempt(self, attempt: int) -> None:
        """Record which call of `execute()` for the delivery being processed is under way."""
        activity = self._activity
        if activity.delivery is not None:  # pragma: no branch - set before every attempt
            self._activity = replace(activity, delivery=replace(activity.delivery, attempt=attempt))

    def finish_delivery(self, outcome: _Outcome | _DeadLetterReason) -> _Delivery[Accepted] | None:
        """Count how a delivery ended, and stop reporting it, in one step; return it, if decoded.

        So a reader never sees a delivery that is already counted, or no longer reported and not
        yet counted.
        """
        activity = self._activity
        self._activity = replace(activity, delivery=None, counts=_counted(activity.counts, outcome))
        return activity.delivery

    def restart(self) -> None:
        """Start a `run()` again after a crash, as `UPSTREAM_RECOVER`; the counts go on."""
        self._activity = _Activity(
            ExecutorStatus.UPSTREAM_RECOVER, _now(), counts=self._activity.counts
        )
        self._entered_monotonic = _monotonic()
        self._final_status = None

    def end(self, status: ExecutorStatus) -> None:
        """Record that `run()` ended, as `STOPPED` or `CRASHED`, which a stop never changes."""
        self._final_status = (status, _now())

    def log_stopped(self, stopped_while: str) -> None:
        """Log that `run()` stopped on request, and how long the stop took, as `stop_seconds`.

        From the first `stop()` to the connection being closed: finishing the message in
        progress, if any, included.
        """
        requested_at = self._stop_requested_monotonic
        stop_seconds = 0.0 if requested_at is None else _monotonic() - requested_at
        logger.info(
            "Executor for queue %s stopped on request%s stop_seconds=%.3f",
            self._queue,
            stopped_while,
            stop_seconds,
            extra={
                "kicak": {
                    "event": "stopped",
                    "queue": self._queue,
                    "stop_seconds": stop_seconds,
                }
            },
        )
