"""Executor agent: long-running node that consumes, executes, and optionally publishes messages."""

from __future__ import annotations

import abc
import logging
import math
import random
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum, StrEnum, auto
from types import NoneType
from typing import assert_never, cast

import pika
from pika.adapters.blocking_connection import BlockingChannel, ReturnedMessage
from pika.exceptions import (
    ChannelClosedByBroker,
    ChannelWrongStateError,
    NackError,
    UnroutableError,
)
from pika.exchange_type import ExchangeType
from pika.spec import Basic

from pykicak.abstracts import (
    _ORIGINAL_PUBLISHER_HEADER,
    _PUBLISHED_AT_HEADER,
    KicakAbstract,
    _check_name,
    _log_values,
    _monotonic,
    _now,
    _type_arguments,
    _union_members,
)
from pykicak.config import KicakConfig
from pykicak.messages import KicakMessage, _type_name_of

logger = logging.getLogger(__name__)

_INITIAL_RETRY_DELAY_SECONDS = 0.5
_MAX_RETRY_DELAY_SECONDS = 30.0
_MAX_BACKOFF_EXPONENT = 16
_PREFETCH_COUNT = 1
_DEFAULT_HEARTBEAT_SECONDS = 600  # room for long LLM calls and heavy computation in execute()
_DEFAULT_INPUT_CHECK_SECONDS = 10.0
_DEFAULT_RETRY_DELAY_SECONDS = 10.0
_MAX_RETRY_DELAY_MILLISECONDS = 2**32 - 1  # about 49.7 days; RabbitMQ refuses far larger ones
_DEAD_LETTER_EXCHANGE_SUFFIX = ".dlx"
_DEAD_LETTER_QUEUE_SUFFIX = ".dlq"
_RETRY_QUEUE_SUFFIX = ".retry"
# A queue an Executor consumes from leaves room in its name for the names derived from it
_LONGEST_QUEUE_NAME_SUFFIX = max(
    (_DEAD_LETTER_EXCHANGE_SUFFIX, _DEAD_LETTER_QUEUE_SUFFIX, _RETRY_QUEUE_SUFFIX), key=len
)
_RETRY_ROUND_HEADER = "x-kicak-retry-round"
_RETRYING_SINCE_HEADER = "x-kicak-retrying-since"
# Why and when a copy in the dead-letter queue was dead-lettered, and from which queue
_DEAD_LETTER_REASON_HEADER = "x-kicak-dead-letter-reason"
_ERROR_TYPE_HEADER = "x-kicak-error-type"
_ERROR_MESSAGE_HEADER = "x-kicak-error-message"
_DEAD_LETTERED_AT_HEADER = "x-kicak-dead-lettered-at"
_DEAD_LETTERED_FROM_HEADER = "x-kicak-dead-lettered-from"
_DEAD_LETTER_HEADERS = frozenset(
    {
        _DEAD_LETTER_REASON_HEADER,
        _ERROR_TYPE_HEADER,
        _ERROR_MESSAGE_HEADER,
        _DEAD_LETTERED_AT_HEADER,
        _DEAD_LETTERED_FROM_HEADER,
    }
)
# Keeps the message properties well within one frame (frame_max is 128 KiB by default)
_MAX_ERROR_MESSAGE_BYTES = 1024
_TRUNCATION_MARK = "…"
# Headers RabbitMQ sets itself; a copy must not carry stale ones, e.g. a quorum delivery count
_BROKER_HEADERS = frozenset({"x-death", "x-delivery-count"})
_BROKER_HEADER_PREFIXES = ("x-first-death-", "x-last-death-")
_STOP_POLL_SECONDS = 1.0  # longest wait, for a message or a reconnection, before checking stop()
_PRECONDITION_FAILED = 406


class _StopRequested(Exception):
    """Raised inside run() when stop() cancels a connection attempt or the wait before one."""


class ExecutorStatus(StrEnum):
    """Lifecycle status of an Executor, reported as `ExecutorInfo.status` by `get_info()`.

    Members are strings, so they compare equal to their values, e.g. `"WAITING_FOR_MESSAGE"`.
    The first attempt to connect, or to verify the topology, keeps the current status; only a
    failed attempt enters a recovery status.
    """

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

    malformed: int = 0
    """`MalformedMessageError`: from `execute()`, or for a delivery or results that are invalid."""

    result_too_large: int = 0
    """RabbitMQ refused one of the delivery's results as larger than its `max_message_size`."""

    unexpected_error: int = 0
    """Any other exception, treated as a bug: each one also stopped the Executor."""

    @property
    def total(self) -> int:
        """All dead-lettered deliveries, whatever the reason."""
        return self.malformed + self.result_too_large + self.unexpected_error


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutorCounts:
    """How the deliveries an Executor took ended: `ExecutorInfo.counts`.

    Each delivery the Executor starts to process counts once, when it is settled or left to
    RabbitMQ. The counts start at zero when the Executor is created and only grow, across
    `run()` called again after a crash, so they suit monitoring counters such as Prometheus's.
    They count deliveries, not messages: a message delivered again, e.g. after a lost connection,
    counts again. A delivery received after `stop()` is not processed and not counted.
    """

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

    Times are of `_monotonic()`, durations in seconds. Only the thread running `run()` uses it.
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


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class ExecutorInfo[Accepted: KicakMessage]:
    """What an Executor is doing, as returned by `KicakExecutorAbstract.get_info()`.

    One consistent snapshot: every field describes the same moment. Times are timezone-aware
    `datetime` objects in UTC, so they can be compared and subtracted directly, e.g.
    `info.status_checked_at - info.status_changed_at` is how long the status has lasted; use
    `.isoformat()` for text such as `"2026-10-01T17:05:42.123456+00:00"`. The repr shows the
    message only by its type and `message_id`, never its contents, so logging the snapshot is safe.
    """

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

    0 for a message that never was. None when `message` is None.
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


class QueueType(StrEnum):
    """RabbitMQ type of every queue an Executor declares, its `queue_type` argument.

    That is its input queue, its dead-letter queue, its retry queue, and its destination queues.
    Members are strings, so the values `"classic"` and `"quorum"` are accepted too.
    """

    CLASSIC = "classic"
    """A queue stored on one broker node; the default."""

    QUORUM = "quorum"
    """A queue replicated across the nodes of a RabbitMQ cluster, so it survives a node failure.

    It limits how often a message is redelivered: on RabbitMQ 4.0 and newer, by default, a
    message is dead-lettered on its 21st delivery instead of being requeued again.
    """


class _DeadLetterReason(StrEnum):
    """Why the executor dead-lettered a delivery: its `x-kicak-dead-letter-reason` header."""

    MALFORMED = "malformed"
    """`MalformedMessageError`: the delivery, or the results `execute()` returned, are invalid."""

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
    """Left to RabbitMQ: never passed to `_settle()`."""


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
        case _DeadLetterReason.RESULT_TOO_LARGE:
            dead_lettered = replace(
                dead_lettered, result_too_large=dead_lettered.result_too_large + 1
            )
        case _DeadLetterReason.UNEXPECTED_ERROR:
            dead_lettered = replace(
                dead_lettered, unexpected_error=dead_lettered.unexpected_error + 1
            )
        case _:
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


def _dead_letter_exchange_of(queue_name: str) -> str:
    """Return the name of the dead-letter exchange of `queue_name`: `<queue>.dlx`."""
    return f"{queue_name}{_DEAD_LETTER_EXCHANGE_SUFFIX}"


def _dead_letter_queue_of(queue_name: str) -> str:
    """Return the name of the dead-letter queue of `queue_name`: `<queue>.dlq`."""
    return f"{queue_name}{_DEAD_LETTER_QUEUE_SUFFIX}"


def _retry_queue_of(queue_name: str) -> str:
    """Return the name of the retry queue of `queue_name`: `<queue>.retry`."""
    return f"{queue_name}{_RETRY_QUEUE_SUFFIX}"


def _check_queue_name(argument: str, name: object) -> None:
    """Raise unless `name` can name a queue an Executor consumes from.

    Like any name, but its dead-letter exchange and queue and its retry queue must fit AMQP's
    limit of 255 bytes too, so it may have at most 249.
    """
    _check_name(argument, name, suffix=_LONGEST_QUEUE_NAME_SUFFIX)


class MalformedMessageError(Exception):
    """A delivery can never be processed successfully, so the Executor dead-letters it.

    The Executor raises it for a delivery that has no body or cannot be decoded into the
    expected message type, and for one whose results, returned by `execute()`, are not messages
    or cannot be serialized. `execute()` may raise it to reject a message deterministically,
    for example when its contents fail application validation. The Executor then continues with
    the next message; unlike any other exception from `execute()`, it does not stop it.
    """


class TransientProcessingError(Exception):
    """Processing failed for a reason that may clear on retry, e.g. an unavailable service.

    Raise it from `execute()`, typically from an error of a service it calls, such as a timeout.
    The Executor calls `execute()` again, up to `max_processing_attempts` times, and if the
    failure persists it moves the message to its retry queue `<source_queue>.retry`, which
    returns it to the back of the source queue after `retry_delay_seconds`, to be executed again;
    it is never dead-lettered for it. The Executor continues with the next message.
    """


def _capped_backoff_seconds(attempt: int) -> float:
    """Return the exponential backoff delay for zero-based `attempt`, capped at 30 seconds."""
    return min(
        _INITIAL_RETRY_DELAY_SECONDS * 2.0 ** min(attempt, _MAX_BACKOFF_EXPONENT),
        _MAX_RETRY_DELAY_SECONDS,
    )


def _is_consumer_timeout(error: Exception) -> bool:
    """Return whether RabbitMQ closed a channel because a delivery outlasted `consumer_timeout`.

    The broker's `consumer_timeout` bounds how long a delivery may stay unacknowledged. RabbitMQ
    4.2 reports its expiry as `406 PRECONDITION_FAILED` with a reply text such as "delivery
    acknowledgement on channel 1 timed out", and requeues the delivery. RabbitMQ 4.3 and newer
    cancel the consumer instead (see `_consumer_cancelled()`).
    """
    return (
        isinstance(error, ChannelClosedByBroker)
        and error.reply_code == _PRECONDITION_FAILED
        and "timed out" in error.reply_text
    )


def _is_result_too_large(error: Exception) -> bool:
    """Return whether RabbitMQ refused a published result as larger than its `max_message_size`.

    RabbitMQ reports it as `406 PRECONDITION_FAILED` with the reply text "message size N is larger
    than configured max size M" (RabbitMQ 4.2), closing only the channel the result was published
    on, so its transaction is discarded.
    """
    return (
        isinstance(error, ChannelClosedByBroker)
        and error.reply_code == _PRECONDITION_FAILED
        and "larger than configured max size" in error.reply_text
    )


def _checked_destination_queues(
    destination_exchange: str | None, destination_queues: Sequence[str] | None
) -> tuple[str, ...]:
    """Validate that both destination arguments are given, or neither; return the queue names."""
    if destination_exchange is None:
        if destination_queues is not None:
            raise ValueError(
                "destination_queues requires destination_exchange; a terminator has neither"
            )
        return ()
    _check_name("destination_exchange", destination_exchange)
    if destination_queues is None:
        raise ValueError(
            "destination_queues is required with destination_exchange: name every queue that "
            "must receive its messages"
        )
    if isinstance(destination_queues, str):
        raise TypeError("destination_queues must be a sequence of queue names, not a str")
    queues = tuple(destination_queues)
    if not queues:
        raise ValueError("destination_queues must name at least one queue")
    for index, queue in enumerate(queues):
        _check_queue_name(f"destination_queues[{index}]", queue)
    if len(set(queues)) != len(queues):
        raise ValueError("destination_queues must not contain duplicate names")
    return queues


def _checked_input_check_seconds(input_check_seconds: float) -> float:
    """Validate `input_check_seconds`: a finite number of seconds, 0 or more."""
    if isinstance(input_check_seconds, bool) or not isinstance(input_check_seconds, int | float):
        raise TypeError("input_check_seconds must be a number")
    if not math.isfinite(input_check_seconds) or input_check_seconds < 0:
        raise ValueError("input_check_seconds must be a finite number, 0 or more")
    return float(input_check_seconds)


def _checked_retry_delay_milliseconds(retry_delay_seconds: float) -> int:
    """Validate `retry_delay_seconds` and return it in whole milliseconds, as RabbitMQ takes it."""
    if isinstance(retry_delay_seconds, bool) or not isinstance(retry_delay_seconds, int | float):
        raise TypeError("retry_delay_seconds must be a number")
    if not math.isfinite(retry_delay_seconds) or retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be a finite number, 0 or more")
    milliseconds = round(retry_delay_seconds * 1000)
    if milliseconds > _MAX_RETRY_DELAY_MILLISECONDS:
        raise ValueError(
            f"retry_delay_seconds must be at most {_MAX_RETRY_DELAY_MILLISECONDS / 1000}"
        )
    return milliseconds


def _copied_headers(headers: Mapping[str, object] | None) -> dict[str, object]:
    """Return the headers of a delivery that a copy the executor publishes of it keeps.

    RabbitMQ's own headers are dropped, and so are the reasons of an earlier dead-lettering, which
    a message replayed from a dead-letter queue still carries.
    """
    return {
        name: value
        for name, value in (headers or {}).items()
        if name not in _BROKER_HEADERS
        and not name.startswith(_BROKER_HEADER_PREFIXES)
        and name not in _DEAD_LETTER_HEADERS
    }


def _retry_round_of(headers: Mapping[str, object] | None) -> int:
    """Return how often a delivery was moved to the retry queue: its retry round, or 0.

    A missing or invalid `x-kicak-retry-round` header counts as 0, so the rounds start again.
    """
    retry_round = (headers or {}).get(_RETRY_ROUND_HEADER)
    if isinstance(retry_round, int) and not isinstance(retry_round, bool) and retry_round >= 0:
        return retry_round
    return 0


def _retry_headers(headers: Mapping[str, object] | None, now: datetime) -> dict[str, object]:
    """Return the headers of a message's copy for the retry queue: its next retry round.

    The application's headers are kept, and RabbitMQ's own ones dropped (see `_copied_headers()`).
    The round counts the moves into the retry queue, and `x-kicak-retrying-since` keeps the time
    of the first one; a missing or invalid value starts them again. The copy is published anew,
    so `x-kicak-published-at` becomes `now`, and the `queued_seconds` of its return to the source
    queue includes the retry delay.
    """
    kept = _copied_headers(headers)
    kept[_PUBLISHED_AT_HEADER] = now.isoformat()
    kept[_RETRY_ROUND_HEADER] = _retry_round_of(kept) + 1
    since = kept.get(_RETRYING_SINCE_HEADER)
    if isinstance(since, bytes):
        since = since.decode("utf-8", errors="replace")
    if not (isinstance(since, str) and _is_iso_datetime(since)):
        since = now.isoformat()
    kept[_RETRYING_SINCE_HEADER] = since
    return kept


def _dead_letter_headers(
    headers: Mapping[str, object] | None,
    reason: _DeadLetterReason,
    error: BaseException,
    source_queue: str,
    now: datetime,
) -> dict[str, object]:
    """Return the headers of a message's copy for the dead-letter queue: why it is there.

    The application's headers and the retry headers, its history, are kept, and RabbitMQ's own
    ones dropped (see `_copied_headers()`).
    """
    kept = _copied_headers(headers)
    kept[_DEAD_LETTER_REASON_HEADER] = reason.value
    kept[_ERROR_TYPE_HEADER] = _error_type_name(error)
    kept[_ERROR_MESSAGE_HEADER] = _error_description(error)
    kept[_DEAD_LETTERED_AT_HEADER] = now.isoformat()
    kept[_DEAD_LETTERED_FROM_HEADER] = source_queue
    return kept


def _error_type_name(error: BaseException) -> str:
    """Return the name of the class of `error` with its module, e.g. `json.JSONDecodeError`.

    A built-in exception is named without its module, e.g. `KeyError`.
    """
    error_class = type(error)
    module = error_class.__module__
    if module == "builtins":
        return error_class.__qualname__
    return f"{module}.{error_class.__qualname__}"


def _error_description(error: BaseException) -> str:
    """Return the text of `error` that a header can carry: at most 1024 bytes of valid UTF-8.

    A lone surrogate, which UTF-8 cannot encode and pika would fail on, becomes `?`, and a longer
    text is cut at a character boundary and ends with `…`. An exception whose `str()` raises is
    described as the traceback module does.
    """
    try:
        text = str(error)
    except Exception:
        text = "<exception str() failed>"
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= _MAX_ERROR_MESSAGE_BYTES:
        return encoded.decode("utf-8")
    kept = encoded[: _MAX_ERROR_MESSAGE_BYTES - len(_TRUNCATION_MARK.encode("utf-8"))]
    # Dropping the bytes of a character the cut split leaves valid UTF-8
    return kept.decode("utf-8", errors="ignore") + _TRUNCATION_MARK


def _text_property(value: object) -> str | None:
    """Return the value of a delivery's text property, or None if it is missing or empty.

    pika delivers a property as bytes if it is not valid UTF-8; it is decoded with replacements.
    """
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) and value else None


def _sent_message_id(properties: pika.BasicProperties | None) -> str | None:
    """Return the `message_id` property of a delivery, the ID its sender gave, or None.

    Every pykicak agent sets it to the `message_id` in the body, so it names a delivery whose
    body cannot be decoded; once the body is decoded, its `message_id` is the one used.
    """
    return None if properties is None else _text_property(properties.message_id)


def _sent_type(properties: pika.BasicProperties | None) -> str | None:
    """Return the `type` property of a delivery, the type name its sender gave, or None."""
    return None if properties is None else _text_property(properties.type)


def _original_publisher_of(properties: pika.BasicProperties | None) -> str | None:
    """Return the user of the agent that created a delivered message, if known.

    The `x-kicak-original-publisher` header, which every pykicak agent sets on the messages it
    creates, comes first: a copy from a retry queue has another `user_id`. A message without the
    header, published by another client, was created by its publisher, so its `user_id` names
    it when the client set one. Otherwise the creator is unknown: None.
    """
    if properties is None:
        return None
    header = (properties.headers or {}).get(_ORIGINAL_PUBLISHER_HEADER)
    if isinstance(header, bytes):
        header = header.decode("utf-8", errors="replace")
    if isinstance(header, str) and header:
        return header
    user_id = properties.user_id
    return user_id if isinstance(user_id, str) and user_id else None


def _is_iso_datetime(value: str) -> bool:
    """Return whether `value` is an ISO 8601 date and time with a time zone."""
    try:
        return datetime.fromisoformat(value).tzinfo is not None
    except ValueError:
        return False


def _queued_seconds(properties: pika.BasicProperties | None, received_at: datetime) -> float | None:
    """Return how long a delivery waited since it was published, or None if that is unknown.

    From its `x-kicak-published-at` header, which every pykicak agent sets, to `received_at`.
    The two times come from the clocks of two hosts, so a difference between them shows, and
    the result can even be negative; it is not corrected. None for a delivery without the
    header, e.g. from another client, or whose header is not an ISO 8601 time with a time zone.
    """
    published = None if properties is None else (properties.headers or {}).get(_PUBLISHED_AT_HEADER)
    if isinstance(published, bytes):
        published = published.decode("utf-8", errors="replace")
    if not isinstance(published, str):
        return None
    try:
        published_at = datetime.fromisoformat(published)
    except ValueError:
        return None
    if published_at.utcoffset() is None:
        return None
    return (received_at - published_at).total_seconds()


def _as_messages(results: object) -> tuple[KicakMessage, ...]:
    """Normalize the value returned by execute() to a tuple of messages."""
    if results is None:
        return ()
    if isinstance(results, KicakMessage):
        return (results,)
    messages = tuple(cast(Sequence[object], results))
    for message in messages:
        if not isinstance(message, KicakMessage):
            raise TypeError(
                f"execute() must return KicakMessage instances, got {type(message).__name__}"
            )
    return cast(tuple[KicakMessage, ...], messages)


_EXECUTOR_TYPES_USAGE = (
    "KicakExecutorAbstract[InputMessage, ResultMessage], with a union such as "
    "InputV1 | InputV2 to accept several input types, or KicakExecutorAbstract[InputMessage, None] "
    "for a terminator"
)


def _declared_message_types(
    executor_class: type,
) -> tuple[dict[str, type[KicakMessage]], type[KicakMessage] | None]:
    """Return the input types an Executor class accepts, by type name, and its result type.

    Both come from its class header, `KicakExecutorAbstract[Accepted, Result]`: `Accepted` is a
    message class or a union of several, and `Result` one message class, or None for a
    terminator. Raises `TypeError` for anything else, and for two accepted classes that declare
    the same type name.
    """
    input_argument, result_argument = _type_arguments(
        executor_class, KicakExecutorAbstract, usage=_EXECUTOR_TYPES_USAGE
    )
    name = executor_class.__name__
    accepted: dict[str, type[KicakMessage]] = {}
    for member in _union_members(input_argument):
        if member is NoneType:
            raise TypeError(f"{name} must accept message types only, not None")
        type_name = _type_name_of(member)
        message_class = cast(type[KicakMessage], member)
        if type_name in accepted:
            raise TypeError(
                f"{name} accepts {accepted[type_name].__name__} and {message_class.__name__}, "
                f"which declare the same kicak_type {type_name!r}"
            )
        accepted[type_name] = message_class
    results = _union_members(result_argument)
    if len(results) > 1:
        raise TypeError(
            f"{name} must declare one result type, or None for a terminator, "
            f"got {result_argument!r}"
        )
    if results[0] is NoneType:
        return accepted, None
    _type_name_of(results[0])
    return accepted, cast(type[KicakMessage], results[0])


# Accepted: one message class, or a union of the versions accepted, e.g. ChunkV1 | ChunkV2.
# Result: the class of the results, or None for a terminator.
class KicakExecutorAbstract[Accepted: KicakMessage, Result: KicakMessage | None](
    KicakAbstract, abc.ABC
):
    """Long-running node that consumes messages, executes them, and optionally publishes results.

    A subclass declares its message types once, in its class header, where both type checkers and
    the executor read them: `Accepted`, the message types it accepts from the source queue, and
    `Result`, the type of its results, or None for a terminator, e.g.
    `class Summarizer(KicakExecutorAbstract[Document, Summary])`. They are the contract with the
    agents upstream and downstream:

    - Every message is sent with its class's `kicak_type` as the AMQP `type` property. The
      executor decodes each delivery into the accepted class with that name, and dead-letters
      one whose `type` it does not accept, or that has none, as malformed.
    - To accept several versions of a message, e.g. while its producer is upgraded, give a
      union: `KicakExecutorAbstract[DocumentV1 | DocumentV2, Summary]`. `execute()` receives an
      instance of whichever class the message was sent as.
    - Every result must be exactly of class `Result`; a result of another class is a bug, which
      dead-letters the input and stops the executor. An executor with a result type needs a
      destination exchange, and a terminator, declared with `Result` None, must not have one.

    Each accepted class and the result class must declare their own `kicak_type`, and two
    accepted classes must not declare the same one. The executor raises `TypeError` when it is
    created if a declaration is missing or invalid, e.g. for a subclass of the unparameterized
    class.

    Delivery is at-least-once, so subclasses must be idempotent: a message can be executed, and
    its results published, more than once.

    The executor verifies its topology from both sides, by declaring it again idempotently,
    which restores whatever was deleted:

    - Input side: its source queue, the queue's binding to the source exchange, its
      dead-letter route (`<source_queue>.dlx` and `<source_queue>.dlq`), and its retry queue
      (`<source_queue>.retry`). On every connection, after each message before waiting for the
      next one, and every `input_check_seconds` while waiting. The dead-letter route is also
      declared right before every rejection, and the retry queue before every move into it.
    - Output side: the destination exchange and every queue in `destination_queues`, each
      bound to it. On every connection, and right before the results of each message are
      published.

    Each delivery ends in exactly one outcome:

    - Success: the results returned by `execute()` are published to the destination exchange
      in one RabbitMQ transaction, so RabbitMQ delivers all of them or none, and the delivery is
      acknowledged only after the transaction is committed.
    - Deterministic failure (`MalformedMessageError`): the delivery is dead-lettered. A copy of
      it is published to the dead-letter exchange, with headers saying why, when, and from
      which queue (`x-kicak-dead-letter-reason`, `x-kicak-error-type`, `x-kicak-error-message`,
      `x-kicak-dead-lettered-at`, `x-kicak-dead-lettered-from`), and the delivery is
      acknowledged once RabbitMQ has confirmed the copy. If RabbitMQ returns the copy as
      unroutable or refuses it, the delivery is requeued instead.
    - Transient failure (`TransientProcessingError`): `execute()` is retried, and if the failure
      persists the message is moved to the retry queue, which returns it to the back of the
      source queue after `retry_delay_seconds`. It is never dead-lettered for it.
    - Any other error is unexpected: the delivery is dead-lettered as above, then the error is
      raised, stopping the executor.
    - Results returned by RabbitMQ as unroutable (`UnroutableError`: no queue was bound to the
      destination exchange at the commit): the executor keeps the delivery unacknowledged and
      its results in memory, and verifies the output side and publishes them again after a
      wait (0.5 seconds, doubling up to 30, with jitter) during which the connection keeps
      answering heartbeats. `execute()` is not called again. A `stop()` during the hold
      requeues the delivery, so the message is executed again later.
    - Connection lost while publishing results, or RabbitMQ already took the delivery back
      because it outlasted `consumer_timeout` (closing its channel, or cancelling the consumer on
      RabbitMQ 4.3 and newer), also while holding them: RabbitMQ discards the uncommitted results
      and requeues the delivery, and the executor never re-sends them on a new connection. It
      reconnects, receives the delivery again, and executes it again, publishing its results
      once.
    - A result larger than RabbitMQ's `max_message_size`: RabbitMQ discards the transaction, so
      nothing is published, and the delivery is dead-lettered as above; the executor continues.
    - Any other failure to verify the output side or to publish the results, e.g. RabbitMQ
      refusing a declaration or the commit: the error is raised, stopping the executor, and
      closing the connection requeues the delivery.
    - Connection lost while acknowledging or requeueing the delivery, or moving it to the retry
      queue or the dead-letter queue, or RabbitMQ took the delivery back because it outlasted
      `consumer_timeout`:
      RabbitMQ requeues it, so the executor reconnects and receives it again, unless it was
      stopping anyway because of an unexpected error, which it then still raises.

    Without a destination exchange, and with the result type None, the executor is a terminator,
    and `execute()` must return None.

    The executor takes one message at a time: RabbitMQ delivers the next message only after
    the previous one is acknowledged or rejected (prefetch count 1).

    Call `stop()`, from any thread or a signal handler, to make `run()` finish the message in
    progress and return before taking another one. `get_info()` reports what the Executor is
    doing, since when, whether a stop was requested, and which message it is processing. Every
    delivery it takes ends with a `Message handled` line logged at `INFO`, whatever the outcome,
    telling how it ended and how long each part of its handling took, for statistics.

    All queue/exchange names are application topology, not connection configuration, so they
    are passed in by the caller (typically sourced from an application's messaging/queues.py,
    messaging/exchanges.py, and messaging/bindings.py) rather than read from the `.kicak` file.
    """

    def __init__(
        self,
        config: KicakConfig,
        *,
        source_queue: str,
        source_exchange: str,
        destination_exchange: str | None = None,
        destination_queues: Sequence[str] | None = None,
        queue_type: QueueType | str = QueueType.CLASSIC,
        max_processing_attempts: int = 3,
        heartbeat_seconds: int = _DEFAULT_HEARTBEAT_SECONDS,
        input_check_seconds: float = _DEFAULT_INPUT_CHECK_SECONDS,
        retry_delay_seconds: float = _DEFAULT_RETRY_DELAY_SECONDS,
    ) -> None:
        """Store this executor's topology, retry settings, heartbeat timeout, and check interval.

        The message types come from the class header (see the class docstring), which is checked
        first: an invalid declaration raises `TypeError`.

        `source_exchange` is the upstream exchange that `source_queue` is bound to (e.g. an
        Injector's exchange, or a previous Executor's destination exchange). It is required so
        the Executor always consumes from a queue attached to its intended input exchange.

        Every queue and exchange name must be a non-empty `str` that does not start with `amq.`,
        a prefix RabbitMQ reserves, and fits AMQP's limit of 255 UTF-8 bytes: an exchange name in
        255, and a queue name, of `source_queue` or in `destination_queues`, in 249, leaving room
        for the `.retry` suffix of its retry queue. Otherwise `TypeError` or `ValueError` is
        raised.

        The dead-letter exchange of `source_queue`, set as its `x-dead-letter-exchange`
        argument, is `<source_queue>.dlx`, and the dead-letter queue bound to it is
        `<source_queue>.dlq`, so dead-lettered deliveries are retained for inspection or replay:
        the executor publishes a copy of each, saying why, to the exchange, and RabbitMQ
        dead-letters there what it removes itself, e.g. at a quorum queue's delivery limit. Like
        `source_queue`, both are owned and declared only by this executor.

        `destination_exchange`, if given, receives the results returned by `execute()`, and
        `destination_queues` must then name every queue that must receive them, at least one.
        Before publishing, the executor declares each of these queues, with the same arguments
        as its own Executor declares it, and binds it to the destination exchange, so a deleted
        queue or binding is restored instead of its results being lost. Give neither to run in
        terminator mode; one without the other raises `ValueError`. So does giving them to a
        terminator, declared with the result type None, or omitting them for an executor that
        declares a result type.

        An executor never publishes to itself: a `destination_exchange` equal to
        `source_exchange`, or `destination_queues` including `source_queue`, raises `ValueError`,
        because the results would come back to `source_queue` and be executed again, possibly
        forever. To send results back upstream, let a downstream Executor publish them to
        `source_exchange`.

        `queue_type` is the RabbitMQ queue type of every queue this executor declares: its
        source queue, its dead-letter queue, its retry queue, and its destination queues, so
        every Executor of a system must use the same type. `QueueType.CLASSIC` is the default;
        `QueueType.QUORUM` is a replicated queue that survives the loss of a cluster node. Their
        string values, `"classic"` and `"quorum"`, are accepted too, e.g. from an application's
        configuration; any other value raises `ValueError`. It is always declared explicitly, so
        a vhost's default queue type does not apply. RabbitMQ cannot change the type of an
        existing queue: declaring it with another type fails with `406 PRECONDITION_FAILED`.

        `max_processing_attempts` is how many times `execute()` is called for one delivery, one
        call after the other, while it raises `TransientProcessingError`, before the message is
        moved to the retry queue `<source_queue>.retry`. That queue, owned and declared only by
        this executor, holds the message for `retry_delay_seconds` (default 10, at most about
        49.7 days; 0 returns it at once) and then returns it to the back of the source queue,
        where it is executed again, with another `max_processing_attempts` calls. There is no
        limit on the number of such rounds: the message's headers `x-kicak-retry-round` and
        `x-kicak-retrying-since`, and the warning logged for every move, show a message that
        keeps failing.

        `heartbeat_seconds` (an int from 1 to 65535, default 600) is the AMQP heartbeat timeout of
        this executor's connections. Set it above the longest time processing one message can
        take: every call of `execute()` for it, including the calls retried after
        `TransientProcessingError`, and the waits between them. The connection cannot answer
        heartbeats during that time, so RabbitMQ closes a connection that stays silent for longer
        and redelivers its message. A larger value also delays the detection of a dead connection
        by the same amount. None raises `TypeError`: it would accept the broker's proposal, 60
        seconds by default, which is too short for a long `execute()`.

        `input_check_seconds` (default 10) is how often the input side is verified while the
        executor waits for a message. With 0 it is verified on every wakeup, about once a second.
        """
        accepted_types, result_type = _declared_message_types(type(self))
        _check_queue_name("source_queue", source_queue)
        _check_name("source_exchange", source_exchange)
        queues = _checked_destination_queues(destination_exchange, destination_queues)
        if result_type is None and destination_exchange is not None:
            raise ValueError(
                f"{type(self).__name__} declares the result type None, so it is a terminator, "
                "which publishes nothing: give it no destination_exchange or destination_queues"
            )
        if result_type is not None and destination_exchange is None:
            raise ValueError(
                f"{type(self).__name__} declares its results as {result_type.__name__}: give it "
                "the destination_exchange and destination_queues to publish them to"
            )
        # A self-loop delivers the results back to source_queue, executing them again, possibly
        # forever; a loop through another Executor stays possible, because that one is explicit
        if destination_exchange == source_exchange:
            raise ValueError(
                "destination_exchange must differ from source_exchange: a self-loop, the results "
                "would be delivered back to source_queue"
            )
        if source_queue in queues:
            raise ValueError(
                "destination_queues must not include source_queue: a self-loop, the results "
                "would be delivered back to it"
            )
        dead_letter_exchange = _dead_letter_exchange_of(source_queue)
        if dead_letter_exchange in (source_exchange, destination_exchange):
            raise ValueError(
                "source_exchange and destination_exchange must differ from "
                f"{dead_letter_exchange!r}, the dead-letter exchange of source_queue"
            )
        input_queues = (source_queue, *queues)
        dead_letter_queues = {_dead_letter_queue_of(queue) for queue in input_queues}
        if dead_letter_queues.intersection(input_queues):
            raise ValueError(
                "source_queue and destination_queues must not include the dead-letter queue "
                "<queue>.dlq of one of them"
            )
        if {_retry_queue_of(queue) for queue in input_queues}.intersection(input_queues):
            raise ValueError(
                "source_queue and destination_queues must not include the retry queue "
                "<queue>.retry of one of them"
            )
        if destination_exchange in {_dead_letter_exchange_of(queue) for queue in queues}:
            raise ValueError(
                "destination_exchange must not be the dead-letter exchange <queue>.dlx of a "
                "destination queue"
            )
        try:
            queue_type = QueueType(queue_type)
        except ValueError:
            members = " or ".join(f"QueueType.{member.name}" for member in QueueType)
            raise ValueError(f"queue_type must be {members}, got {queue_type!r}") from None
        if isinstance(max_processing_attempts, bool) or not isinstance(
            max_processing_attempts, int
        ):
            raise TypeError("max_processing_attempts must be an int")
        if max_processing_attempts < 1:
            raise ValueError("max_processing_attempts must be at least 1")
        # The base class also accepts None, the broker's proposal; the range is checked there
        if isinstance(heartbeat_seconds, bool) or not isinstance(heartbeat_seconds, int):
            raise TypeError("heartbeat_seconds must be an int")
        input_check_seconds = _checked_input_check_seconds(input_check_seconds)
        retry_delay_milliseconds = _checked_retry_delay_milliseconds(retry_delay_seconds)
        super().__init__(config, heartbeat_seconds=heartbeat_seconds)
        self._stop_requested = False
        self._running = False
        # Status state; each field has a single writer, so no locking is needed (see get_info()).
        # _activity holds the status, when it was entered, the delivery being processed, and the
        # counts, replaced as a whole so that a reader never mixes two moments, e.g. a status
        # with another status's message, or a message already counted.
        self._activity: _Activity[Accepted] = _Activity(ExecutorStatus.CREATED, _now())
        self._status_entered_monotonic = _monotonic()  # when the status was entered, for durations
        # How the delivery being processed was settled, or None while it is not (yet) settled
        self._settlement: _Outcome | _DeadLetterReason | None = None
        # How the handling of the delivery being processed goes, or went, for its log line
        self._handling = _Handling(started_at=0.0)
        self._stop_requested_at: datetime | None = None
        self._stop_requested_monotonic: float | None = None  # _stop_requested_at, for durations
        self._final_status: tuple[ExecutorStatus, datetime] | None = None
        # The classes Accepted stands for, by the type name they are sent with, and Result's class
        self._accepted_types = cast(dict[str, type[Accepted]], accepted_types)
        self._result_type = result_type
        self._source_queue = source_queue
        self._source_exchange = source_exchange
        self._dead_letter_exchange = dead_letter_exchange
        self._dead_letter_queue = _dead_letter_queue_of(source_queue)
        self._retry_queue = _retry_queue_of(source_queue)
        self._retry_delay_milliseconds = retry_delay_milliseconds
        self._destination_exchange = destination_exchange
        self._destination_queues = queues
        self._queue_type = queue_type
        self._max_processing_attempts = max_processing_attempts
        self._input_check_seconds = input_check_seconds
        self._input_checked_at = 0.0  # time.monotonic() of the last verification of the input side
        # Transactional channel for results, opened on first use for each connection
        self._results_channel: BlockingChannel | None = None
        # The consuming channel whose consumer RabbitMQ cancelled, if any
        self._cancelled_consumer_channel: BlockingChannel | None = None
        self._results_connection: pika.BlockingConnection | None = None
        self._returned_results: list[ReturnedMessage] = []

    def _connection_label(self) -> str:
        """Name the executor's role, `executor` or `terminator`, and its queue in its connection."""
        role = "terminator" if self._destination_exchange is None else "executor"
        return f"{role} queue={self._source_queue}"

    def _declare_topology(self) -> None:
        """Declare the input side and, unless a terminator, the output side, on a new connection."""
        self._declare_input_side()
        if self._destination_exchange is not None:
            self._declare_output_side(self.channel)
        self._input_checked_at = time.monotonic()

    def _declare_input_side(self) -> None:
        """Declare the source queue, its binding, its dead-letter route, and its retry queue."""
        self._declare_input_queue(self.channel, self._source_queue)
        self._declare_fanout_exchange(self.channel, self._source_exchange)
        self._bind_queue(self.channel, self._source_queue, self._source_exchange)
        self._declare_dead_letter_route()
        self._declare_retry_queue()

    def _declare_dead_letter_route(self) -> None:
        """Declare the dead-letter exchange and queue of the source queue, and bind them.

        Only this executor declares them, because only its own queue's arguments name them.
        """
        self._declare_fanout_exchange(self.channel, self._dead_letter_exchange)
        logger.debug("Declaring %s queue %s", self._queue_type, self._dead_letter_queue)
        self.channel.queue_declare(
            queue=self._dead_letter_queue,
            durable=True,
            arguments={"x-queue-type": self._queue_type.value},
        )
        self._bind_queue(self.channel, self._dead_letter_queue, self._dead_letter_exchange)

    def _declare_retry_queue(self) -> None:
        """Declare the retry queue of the source queue, which returns each message after its delay.

        Nothing consumes it: a message expires there after `retry_delay_seconds`, and RabbitMQ
        dead-letters it through the default exchange, which routes by queue name, to the back of
        the source queue. The default exchange cannot be deleted, so the way back has no exchange
        or binding that could go missing. A quorum retry queue dead-letters at least once, keeping
        a message until the source queue takes it; a classic one drops a message that expires
        while the source queue does not exist. Only this executor declares it.
        """
        arguments: dict[str, object] = {
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": self._source_queue,
            "x-queue-type": self._queue_type.value,
        }
        if self._queue_type is QueueType.QUORUM:
            arguments["x-dead-letter-strategy"] = "at-least-once"
            arguments["x-overflow"] = "reject-publish"  # required by at-least-once dead-lettering
        logger.debug(
            "Declaring %s queue %s returning to %s",
            self._queue_type,
            self._retry_queue,
            self._source_queue,
        )
        self.channel.queue_declare(queue=self._retry_queue, durable=True, arguments=arguments)

    def _declare_output_side(self, channel: BlockingChannel) -> None:
        """Declare the destination exchange and every destination queue, and bind each of them."""
        destination_exchange = cast(str, self._destination_exchange)
        self._declare_fanout_exchange(channel, destination_exchange)
        for queue_name in self._destination_queues:
            self._declare_input_queue(channel, queue_name)
            self._bind_queue(channel, queue_name, destination_exchange)

    def _declare_input_queue(self, channel: BlockingChannel, queue_name: str) -> None:
        """Declare a queue that an Executor consumes from, with the arguments every one uses.

        The arguments follow from the name and the queue type alone, so the executor upstream of
        a queue declares it exactly as the queue's own executor does.
        """
        dead_letter_exchange = _dead_letter_exchange_of(queue_name)
        logger.debug(
            "Declaring %s queue %s dead_letter_exchange=%s",
            self._queue_type,
            queue_name,
            dead_letter_exchange,
        )
        channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": dead_letter_exchange,
                "x-queue-type": self._queue_type.value,
            },
        )

    @staticmethod
    def _declare_fanout_exchange(channel: BlockingChannel, exchange_name: str) -> None:
        """Idempotently declare a durable fanout exchange."""
        logger.debug("Declaring exchange %s", exchange_name)
        channel.exchange_declare(
            exchange=exchange_name, exchange_type=ExchangeType.fanout, durable=True
        )

    @staticmethod
    def _bind_queue(channel: BlockingChannel, queue_name: str, exchange_name: str) -> None:
        """Idempotently bind a queue to an exchange."""
        logger.debug("Binding queue %s -> exchange %s", queue_name, exchange_name)
        channel.queue_bind(queue=queue_name, exchange=exchange_name)

    @staticmethod
    def _is_retryable_rabbitmq_error(error: Exception) -> bool:
        """Return whether an error is likely to clear after reconnecting.

        Besides the transient errors of every agent, this includes RabbitMQ closing the consuming
        channel because a delivery outlasted its `consumer_timeout`: as when the connection is
        lost, RabbitMQ requeues the delivery, and the executor reconnects and receives it again.
        """
        return KicakAbstract._is_retryable_rabbitmq_error(error) or _is_consumer_timeout(error)

    def _retry_rabbitmq_operation[T](self, operation: Callable[[], T], operation_name: str) -> T:
        """Retry transient RabbitMQ failures forever with capped exponential full jitter.

        Inside `run()`, a stop request raises `_StopRequested` instead of making the next attempt,
        the first one included, so a stopping executor never opens a connection only to close it,
        and a failed attempt makes the status `UPSTREAM_RECOVER`.
        """
        attempt = 0
        while True:
            if self._running and self._stop_requested:
                raise _StopRequested
            logger.debug("Starting %s attempt %d", operation_name, attempt + 1)
            try:
                result = operation()
                logger.debug("%s succeeded on attempt %d", operation_name, attempt + 1)
                return result
            except Exception as error:
                if not self._is_retryable_rabbitmq_error(error):
                    logger.exception("%s failed", operation_name)
                    raise

                self._discard_connection()
                if self._running:
                    self._set_activity(ExecutorStatus.UPSTREAM_RECOVER)
                delay_seconds = _capped_backoff_seconds(attempt) * random.random()
                logger.warning(
                    "%s failed, retrying attempt %d: %s",
                    operation_name,
                    attempt + 2,
                    error,
                )
                logger.debug(
                    "Waiting %.1f seconds before retrying %s", delay_seconds, operation_name
                )
                if not self._sleep_unless_stopped(delay_seconds):
                    raise _StopRequested from error
                attempt += 1

    def _sleep_unless_stopped(self, seconds: float) -> bool:
        """Sleep for `seconds`, ending early if stop() is called while run() is running.

        Sleeps in steps of at most one second so that a stop request is noticed promptly.
        Returns False if the sleep ended because of a stop request.
        """
        remaining = seconds
        while remaining > 0:
            if self._running and self._stop_requested:
                return False
            step = min(remaining, _STOP_POLL_SECONDS)
            time.sleep(step)
            remaining -= step
        return not (self._running and self._stop_requested)

    @abc.abstractmethod
    def execute(self, message: Accepted) -> Result | Sequence[Result] | None:
        """Process one decoded message and return the results to publish.

        `message` is an instance of the accepted class whose `kicak_type` the delivery's `type`
        property names. A delivery is dead-lettered as malformed, without calling `execute()`, if
        its `type` is missing or not accepted, or its body cannot be decoded into that class:
        not a JSON object, missing or unexpected fields, no valid `message_id`, or refused by the
        class's own `__post_init__`, whatever it raises. The types of other field values are not
        checked; validate them here and raise `MalformedMessageError` if needed.

        Return a message, a sequence of messages, or None. Every result must be exactly of the
        declared result class; any other class is a bug, which dead-letters the input and stops
        the executor. Results are published to the destination exchange only after `execute()`
        succeeds, all in one RabbitMQ transaction, and the input is acknowledged only after that
        transaction is committed. A terminator must return None. The executor creates each
        result, so its user is both the result's `user_id` and its original publisher, in the
        `x-kicak-original-publisher` header.

        The exception it raises decides what happens, and only `execute()` can know which applies:

        - `TransientProcessingError`, for a failure that may clear, e.g. a service that is down:
          `execute()` is called again, and if the failure persists the message waits in the
          retry queue for `retry_delay_seconds` and is then executed again, without limit. The
          executor continues.
        - `MalformedMessageError`, for a message that can never succeed: it is dead-lettered. The
          executor continues.
        - Any other exception, including one from a library it calls, is treated as a bug: the
          message is dead-lettered, and the executor stops, `run()` raising the exception with the
          status `"CRASHED"`. Catch the errors you expect and raise one of the two above instead.

        Must be idempotent: the same message can be executed more than once, also concurrently
        by two instances, and after a call that raised `TransientProcessingError` part-way, and
        its results can be published more than once. Make side effects idempotent in the
        database, and derive result IDs from the input's ID.
        """

    def stop(self) -> None:
        """Request a graceful stop: `run()` returns before it takes another message.

        A message that is already being processed is finished first: its results are published
        and it is acknowledged (or requeued or dead-lettered, as usual). `run()` then closes the
        connection and returns instead of waiting for the next message. Waiting for a message,
        or waiting to reconnect, is interrupted within about a second. A message that RabbitMQ
        delivers after the request, but before `run()` notices it, is not processed; closing
        the connection returns it to the queue.

        Safe to call from any thread and from a signal handler: it only sets a flag and records
        the time, without locking, logging, or using the connection. The request is permanent:
        `run()` on a stopped Executor returns immediately, so create a new instance to consume
        again.

        If the results of the message are held because RabbitMQ returned them as unroutable, the
        message is requeued instead, with `basic_nack(requeue=True)`, so it is executed again
        later; a stop never waits for the downstream side to recover.

        The status becomes `STOPPING`, unless `run()` has already ended as `STOPPED` or
        `CRASHED`; calling `stop()` again changes nothing, and `ExecutorInfo.stop_requested_at`
        keeps the time of the first call.
        """
        if self._stop_requested_at is None:
            self._stop_requested_monotonic = _monotonic()
            self._stop_requested_at = _now()
        self._stop_requested = True

    def get_info(self) -> ExecutorInfo[Accepted]:
        """Return a snapshot of what the executor is doing: an `ExecutorInfo`.

        Its `status` is `"CREATED"` until `run()` has connected. While it consumes,
        `"WAITING_FOR_MESSAGE"`, `"PROCESSING"`, and `"SENDING_RESULT"` follow each other for
        every message, and `"UPSTREAM_RECOVER"` and `"DOWNSTREAM_RECOVER"` show that it is
        retrying after a failure on its input or output side; see `ExecutorStatus`. After
        `stop()`, `"STOPPING"` until `run()` returns, then `"STOPPED"`; `"CRASHED"` if `run()`
        ended by raising an exception. A later `stop()` never changes `"STOPPED"` or `"CRASHED"`;
        calling `run()` again after `"CRASHED"`, without a stop request, starts again in
        `"UPSTREAM_RECOVER"`.

        The snapshot also tells when the status was entered, when the snapshot was taken, whether
        and when `stop()` was called, the input message being processed, if any, with its
        `attempt`, `redelivered` flag, and `retry_round`, and `counts`, how the deliveries taken
        so far have ended. Every field describes the same moment, so a monitor never has to
        combine several calls.

        Safe to call from any thread while `run()` is running: it neither locks nor changes any
        state, and does not use the RabbitMQ connection.
        """
        # Each field has a single writer and is read once. The final status is read first: a
        # stop request is never withdrawn, so a STOPPED status always comes with it.
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

    def _set_activity(self, status: ExecutorStatus) -> None:
        """Record what `run()` is doing, logging when it enters or leaves a recovery status.

        Only the thread running `run()` calls it, so `_activity` has a single writer. Setting the
        current status again keeps the time it was entered. The delivery being processed and the
        counts are kept. Leaving a recovery status logs how long it lasted, as `recovery_seconds`.
        """
        activity = self._activity
        previous = activity.status
        if status is previous:
            return
        entered_monotonic = _monotonic()
        previous_seconds = entered_monotonic - self._status_entered_monotonic
        self._activity = replace(activity, status=status, entered_at=_now())
        self._status_entered_monotonic = entered_monotonic
        if status in _RECOVERY_STATUSES:
            logger.warning("Executor for queue %s is recovering: %s", self._source_queue, status)
        elif previous in _RECOVERY_STATUSES:
            logger.info(
                "Executor for queue %s recovered from %s: %s recovery_seconds=%.3f",
                self._source_queue,
                previous,
                status,
                previous_seconds,
                extra={
                    "kicak": {
                        "event": "recovered",
                        "queue": self._source_queue,
                        "recovered_from": previous.value,
                        "status": status.value,
                        "recovery_seconds": previous_seconds,
                    }
                },
            )

    def _set_delivery(self, delivery: _Delivery[Accepted]) -> None:
        """Record the decoded delivery being processed, keeping the status and the counts.

        Only the thread running `run()` calls it, like `_set_activity()`.
        """
        self._activity = replace(self._activity, delivery=delivery)

    def _set_attempt(self, attempt: int) -> None:
        """Record which call of `execute()` for the delivery being processed is under way."""
        activity = self._activity
        if activity.delivery is not None:
            self._activity = replace(activity, delivery=replace(activity.delivery, attempt=attempt))

    def _finish_delivery(self, outcome: _Outcome | _DeadLetterReason) -> None:
        """Count how a delivery ended, and stop reporting it, in one step.

        So a reader never sees a delivery that is already counted, or no longer reported and not
        yet counted.
        """
        activity = self._activity
        self._activity = replace(activity, delivery=None, counts=_counted(activity.counts, outcome))

    def run(self) -> None:
        """Connect, then consume the source queue until `stop()` is called.

        See the class docstring for when a delivery is acknowledged, requeued, or dead-lettered.

        Returns normally only after `stop()`, with the status `"STOPPED"`, even if closing the
        connection then fails: the failure is logged, and RabbitMQ requeues a delivery left
        unsettled once the connection is gone. Otherwise it stops by raising, with the status
        `"CRASHED"`: on failures, and on a `BaseException` such as `SystemExit` or
        `KeyboardInterrupt`, which interrupts even a message being processed. In every case the
        connection is closed, and RabbitMQ requeues a delivery that was not yet acknowledged or
        rejected. If RabbitMQ cancels the consumer, for example because the queue
        was deleted, it logs an error and reconnects, which declares the queue again.
        """
        if self._stop_requested:
            if self._final_status is None:
                self._final_status = (ExecutorStatus.STOPPED, _now())
            logger.info(
                "Executor for queue %s was stopped before it started; not consuming",
                self._source_queue,
            )
            return
        if self._final_status is not None:  # running again after a crash; the counts go on
            self._activity = _Activity(
                ExecutorStatus.UPSTREAM_RECOVER, _now(), counts=self._activity.counts
            )
            self._status_entered_monotonic = _monotonic()
            self._final_status = None
        try:
            self._consume_until_stopped()
        except BaseException as error:
            self._final_status = (ExecutorStatus.CRASHED, _now())
            logger.error(
                "Executor for queue %s crashed with %s", self._source_queue, type(error).__name__
            )
            raise
        self._final_status = (ExecutorStatus.STOPPED, _now())

    def _consume_until_stopped(self) -> None:
        """Connect and consume until a stop request; raise on any other end."""
        logger.debug("Starting Executor consumer for queue=%s", self._source_queue)
        self._running = True
        stopped_while = ""
        try:
            self.connect()
            consumer = self._retry_rabbitmq_operation(
                self._start_consuming, "RabbitMQ consumer startup"
            )
            self._set_activity(ExecutorStatus.WAITING_FOR_MESSAGE)
            while not self._stop_requested:
                try:
                    method, properties, body = next(consumer)
                except StopIteration:
                    # pika ends the generator when RabbitMQ cancels the consumer, e.g. because the
                    # queue was deleted; like every connection, the reconnection declares it again
                    logger.error(
                        "RabbitMQ cancelled the consumer of queue %s, e.g. because the queue was "
                        "deleted; reconnecting, which declares the queue again",
                        self._source_queue,
                    )
                    self._discard_connection()
                    consumer = self._reconnect()
                    continue
                except Exception as error:
                    if not self._is_retryable_rabbitmq_error(error):
                        raise
                    logger.warning("Connection or channel lost, reconnecting: %s", error)
                    self._discard_connection()
                    consumer = self._reconnect()
                    continue

                if method is None:
                    # No message within _STOP_POLL_SECONDS: check stop(), and the input side if due
                    if (
                        not self._stop_requested
                        and self._input_check_due()
                        and not self._verify_input_side()
                    ):
                        consumer = self._reconnect()
                    continue
                if self._stop_requested:
                    logger.info(
                        "Stop requested; leaving message delivery_tag=%s from queue %s "
                        "unprocessed for RabbitMQ to redeliver",
                        method.delivery_tag,
                        self._source_queue,
                    )
                    break
                # A delivery from RabbitMQ always has a tag; pika types it as optional
                delivery_tag = cast(int, method.delivery_tag)
                self._set_activity(ExecutorStatus.PROCESSING)
                handled = self._handle_delivery(
                    delivery_tag, bool(method.redelivered), properties, body
                )
                if not handled:
                    consumer = self._reconnect()
                elif self._stop_requested:
                    break
                elif self._verify_input_side():
                    self._set_activity(ExecutorStatus.WAITING_FOR_MESSAGE)
                else:
                    consumer = self._reconnect()
        except _StopRequested:
            stopped_while = " while reconnecting"
        except BaseException:
            self._running = False
            self._close_without_masking()  # the error that ended run() is the one to raise
            raise
        self._running = False
        # The stop stays graceful even if closing fails: every message is settled or left to
        # RabbitMQ, which requeues it once the connection is gone. close() logs the failure.
        self._close_without_masking()
        self._log_stopped(stopped_while)

    def _log_stopped(self, stopped_while: str) -> None:
        """Log that `run()` stopped on request, and how long the stop took, as `stop_seconds`.

        From the first `stop()` to the connection being closed: finishing the message in
        progress, if any, included.
        """
        requested_at = self._stop_requested_monotonic
        stop_seconds = 0.0 if requested_at is None else _monotonic() - requested_at
        logger.info(
            "Executor for queue %s stopped on request%s stop_seconds=%.3f",
            self._source_queue,
            stopped_while,
            stop_seconds,
            extra={
                "kicak": {
                    "event": "stopped",
                    "queue": self._source_queue,
                    "stop_seconds": stop_seconds,
                }
            },
        )

    def _reconnect(
        self,
    ) -> Iterator[tuple[Basic.Deliver | None, pika.BasicProperties | None, bytes | None]]:
        """Reconnect after the connection or the input side was lost, and consume again.

        The status is `UPSTREAM_RECOVER` until the new consumer is started.
        """
        self._set_activity(ExecutorStatus.UPSTREAM_RECOVER)
        consumer = self._retry_rabbitmq_operation(
            self._start_consuming, "RabbitMQ consumer reconnection"
        )
        self._set_activity(ExecutorStatus.WAITING_FOR_MESSAGE)
        return consumer

    def _input_check_due(self) -> bool:
        """Return whether the input side is to be verified again while waiting for a message."""
        return time.monotonic() - self._input_checked_at >= self._input_check_seconds

    def _verify_input_side(self) -> bool:
        """Declare the input side again, restoring whatever was deleted, before waiting.

        Returns False if the connection or channel was lost meanwhile; the caller must reconnect,
        which declares the input side again. RabbitMQ refusing a declaration is raised.
        """
        logger.debug("Verifying the input side of queue %s", self._source_queue)
        try:
            self._declare_input_side()
        except Exception as error:
            if not self._is_retryable_rabbitmq_error(error):
                logger.exception(
                    "Verifying the input side of queue %s failed; stopping the executor",
                    self._source_queue,
                )
                raise
            logger.warning(
                "Connection or channel lost while verifying the input side of queue %s, "
                "reconnecting: %s",
                self._source_queue,
                error,
            )
            self._discard_connection()
            return False
        self._input_checked_at = time.monotonic()
        return True

    def _handle_delivery(
        self,
        delivery_tag: int,
        redelivered: bool,
        properties: pika.BasicProperties | None,
        body: bytes | None,
    ) -> bool:
        """Execute one delivery, then acknowledge, dead-letter, or move it to the retry queue.

        Returns False if the connection was lost while publishing the results, settling the
        delivery, or moving it to the retry queue or the dead-letter queue, or RabbitMQ took the
        delivery back because it outlasted `consumer_timeout` (closing its channel, or cancelling
        the consumer), also while the results were held. RabbitMQ then discards the uncommitted
        results and requeues the delivery, and the results are not re-sent on a new connection:
        executing the delivery again publishes them once. The caller must reconnect and resume
        consuming. A stop request while the results are held requeues the delivery.

        However it ends, also by raising, the delivery is counted in `ExecutorInfo.counts`: as
        settled, or as abandoned to RabbitMQ; and its `Message handled` line is logged.
        """
        self._settlement = None
        handling = self._handling = _Handling(
            started_at=_monotonic(),
            queued_seconds=_queued_seconds(properties, _now()),
            input_bytes=None if body is None else len(body),
        )
        try:
            return self._process_delivery(delivery_tag, redelivered, properties, body)
        finally:
            # Settled, or left to RabbitMQ to redeliver: no longer being processed
            settlement = self._settlement
            outcome = _Outcome.ABANDONED if settlement is None else settlement
            delivery = self._activity.delivery
            self._finish_delivery(outcome)
            self._log_handled(handling, outcome, delivery, delivery_tag, redelivered, properties)

    def _log_handled(
        self,
        handling: _Handling,
        outcome: _Outcome | _DeadLetterReason,
        delivery: _Delivery[Accepted] | None,
        delivery_tag: int,
        redelivered: bool,
        properties: pika.BasicProperties | None,
    ) -> None:
        """Log the `Message handled` line of a delivery: how it ended, and what its parts took.

        One line at `INFO` for every delivery counted in `ExecutorInfo.counts`, whatever its
        outcome, so that statistics such as averages can be computed from the logs. The record
        also carries the line's values, typed, as its `kicak` attribute, for structured (e.g.
        JSON) log handlers. A delivery that was not decoded is named as its dead-letter line
        names it: by the accepted class its `type` property names, or None, and its sender's
        `message_id` property.
        """
        if not logger.isEnabledFor(logging.INFO):
            return
        finished_at = _monotonic()
        if delivery is None:
            sent_type = _sent_type(properties)
            accepted = None if sent_type is None else self._accepted_types.get(sent_type)
            type_name = None if accepted is None else accepted.__name__
            message_id = _sent_message_id(properties)
            attempts = 0
        else:
            type_name = type(delivery.message).__name__
            message_id = delivery.message.message_id
            attempts = delivery.attempt
        logged_outcome, reason = _logged_outcome(outcome)
        settle_started_at = handling.settle_started_at
        seconds = handling.seconds
        fields: dict[str, object] = {
            "outcome": logged_outcome,
            "reason": reason,
            "type": type_name,
            "id": message_id,
            "queue": self._source_queue,
            "delivery_tag": delivery_tag,
            "redelivered": redelivered,
            "retry_round": _retry_round_of(None if properties is None else properties.headers),
            "attempts": attempts,
            "results": handling.results,
            "input_bytes": handling.input_bytes,
            "result_bytes": handling.result_bytes,
            "largest_result_bytes": handling.largest_result_bytes,
            "total_seconds": finished_at - handling.started_at,
            "execute_seconds": seconds[_TimedPart.EXECUTE],
            "backoff_seconds": seconds[_TimedPart.BACKOFF],
            "publish_seconds": seconds[_TimedPart.PUBLISH],
            "held_seconds": seconds[_TimedPart.HELD],
            "settle_seconds": 0.0 if settle_started_at is None else finished_at - settle_started_at,
            "queued_seconds": handling.queued_seconds,
        }
        _log_values(logger, "Message handled", "message_handled", fields)

    def _record_results(self, publications: list[tuple[str, KicakMessage, bytes]]) -> None:
        """Note how many results the delivery has, and their sizes, for its `Message handled` line.

        Sizes are of the bodies only: RabbitMQ's `max_message_size` limits the body of each
        message, not its headers.
        """
        sizes = [len(serialized) for _exchange, _message, serialized in publications]
        handling = self._handling
        handling.results = len(sizes)
        handling.result_bytes = sum(sizes)
        handling.largest_result_bytes = max(sizes, default=0)

    def _process_delivery(
        self,
        delivery_tag: int,
        redelivered: bool,
        properties: pika.BasicProperties | None,
        body: bytes | None,
    ) -> bool:
        """Do the work of `_handle_delivery()`, which counts how it ended."""
        # The sender's message_id property until the body is decoded, then the body's own
        message_id = _sent_message_id(properties)
        type_name: str | None = None  # until the type property names an accepted class
        try:
            message_class = self._accepted_class(_sent_type(properties))
            type_name = message_class.__name__
            if body is None:
                raise MalformedMessageError("Received a message with no body")
            message = self._decode(body, message_class)
            message_id = message.message_id
            self._set_delivery(
                _Delivery(
                    message,
                    attempt=1,
                    redelivered=redelivered,
                    retry_round=_retry_round_of(None if properties is None else properties.headers),
                )
            )
            logger.info(
                "Message received type=%s id=%s queue=%s delivery_tag=%i",
                type(message).__name__,
                message_id,
                self._source_queue,
                delivery_tag,
            )
            publications = self._prepare_publications(
                self._execute_with_retries(message, message_id)
            )
            self._record_results(publications)
        except MalformedMessageError as error:
            logger.exception(
                "Message dead-lettered type=%s id=%s queue=%s delivery_tag=%i "
                "dead_letter_exchange=%s: %s",
                type_name,
                message_id,
                self._source_queue,
                delivery_tag,
                self._dead_letter_exchange,
                error,
            )
            return self._dead_letter(
                delivery_tag, properties, body, message_id, _DeadLetterReason.MALFORMED, error
            )
        except TransientProcessingError as error:
            # Only execute() raises it, so the body was decoded, and message_id is the body's
            return self._move_to_retry_queue(
                delivery_tag, properties, cast(bytes, body), cast(str, message_id), error
            )
        except Exception as error:
            logger.exception(
                "Message processing failed unexpectedly type=%s id=%s queue=%s delivery_tag=%i; "
                "dead-lettering it to %s and stopping the executor",
                type_name,
                message_id,
                self._source_queue,
                delivery_tag,
                self._dead_letter_exchange,
            )
            try:
                self._dead_letter(
                    delivery_tag,
                    properties,
                    body,
                    message_id,
                    _DeadLetterReason.UNEXPECTED_ERROR,
                    error,
                )
            except Exception as dead_letter_error:
                # The executor stops either way; the processing error is the one to raise
                logger.error(
                    "Message id=%s could not be dead-lettered after the unexpected error, which "
                    "is raised instead of %r",
                    message_id,
                    dead_letter_error,
                )
            raise
        else:
            try:
                if publications and not self._publish_results_holding_input(
                    publications, message_id
                ):
                    logger.info(
                        "Stop requested while holding the results of message id=%s; requeueing "
                        "it for RabbitMQ to redeliver",
                        message_id,
                    )
                    return self._settle(delivery_tag, message_id, _Outcome.REQUEUED)
            except Exception as error:
                if _is_result_too_large(error):
                    # Only this input's results are refused, and none of them was published
                    logger.error(
                        "Message dead-lettered type=%s id=%s queue=%s delivery_tag=%i "
                        "dead_letter_exchange=%s: RabbitMQ refused one of its results as too "
                        "large: %s",
                        type_name,
                        message_id,
                        self._source_queue,
                        delivery_tag,
                        self._dead_letter_exchange,
                        error,
                    )
                    return self._dead_letter(
                        delivery_tag,
                        properties,
                        body,
                        message_id,
                        _DeadLetterReason.RESULT_TOO_LARGE,
                        error,
                    )
                if not self._is_retryable_rabbitmq_error(error):
                    logger.exception(
                        "Results of message id=%s could not be published to exchange %s; "
                        "stopping the executor",
                        message_id,
                        self._destination_exchange,
                    )
                    raise
                logger.warning(
                    "Connection or channel lost while publishing results of message id=%s; "
                    "RabbitMQ will redeliver the message, and discards the results unless it "
                    "had already committed them: %s",
                    message_id,
                    error,
                )
                self._discard_connection()
                return False
            logger.debug("Acknowledging message id=%s delivery_tag=%i", message_id, delivery_tag)
            if not self._settle(delivery_tag, message_id, _Outcome.COMPLETED):
                return False
            logger.debug("Message acknowledged id=%s", message_id)
            return True

    def _move_to_retry_queue(
        self,
        delivery_tag: int,
        properties: pika.BasicProperties | None,
        body: bytes,
        message_id: str,
        error: TransientProcessingError,
    ) -> bool:
        """Move a delivery that failed transiently to the retry queue, then acknowledge it.

        The copy keeps the body and the application's properties and headers, gets the next
        retry round and the time of the move (`x-kicak-published-at`) in its headers and this
        executor's user as its `user_id`, keeps the user who created the message in
        `x-kicak-original-publisher`, and expires in the retry queue after
        `retry_delay_seconds`, which returns it to the back of the source queue. The delivery is
        acknowledged only after RabbitMQ has confirmed the copy, so a failure in between executes
        the message twice instead of losing it. If RabbitMQ does not take the copy, returning it
        as unroutable or refusing it, the delivery is requeued instead.

        Returns False if the connection was lost, or RabbitMQ took the delivery back (closing its
        channel, or cancelling the consumer) before the copy was published; the caller must
        reconnect, and RabbitMQ redelivers the message. RabbitMQ refusing to declare the retry
        queue is raised.
        """
        self._handling.start_settling()
        original = properties if properties is not None else pika.BasicProperties()
        headers = _retry_headers(original.headers, _now())
        copy_properties = self._copy_properties(
            original,
            headers,
            message_id=message_id,
            expiration=str(self._retry_delay_milliseconds),
        )
        try:
            self._declare_retry_queue()
            self._check_delivery_held()
            logger.debug(
                "Moving message id=%s to retry queue %s round=%s",
                message_id,
                self._retry_queue,
                headers[_RETRY_ROUND_HEADER],
            )
            self.channel.basic_publish(
                exchange="",  # the default exchange routes by queue name
                routing_key=self._retry_queue,
                body=body,
                properties=copy_properties,
                mandatory=True,
            )
        except (UnroutableError, NackError) as move_error:
            logger.error(
                "Message id=%s could not be moved to retry queue %s; requeueing it instead: %r",
                message_id,
                self._retry_queue,
                move_error,
            )
            return self._settle(delivery_tag, message_id, _Outcome.REQUEUED)
        except Exception as move_error:
            if not self._is_retryable_rabbitmq_error(move_error):
                logger.exception(
                    "Moving message id=%s to retry queue %s failed; stopping the executor",
                    message_id,
                    self._retry_queue,
                )
                raise
            logger.warning(
                "Connection or channel lost while moving message id=%s to retry queue %s; "
                "RabbitMQ will redeliver it: %s",
                message_id,
                self._retry_queue,
                move_error,
            )
            self._discard_connection()
            return False
        logger.warning(
            "Message moved to retry queue after %d failed processing attempts id=%s queue=%s "
            "retry_queue=%s retry_round=%s retrying_since=%s retry_in=%.1fs: %s",
            self._max_processing_attempts,
            message_id,
            self._source_queue,
            self._retry_queue,
            headers[_RETRY_ROUND_HEADER],
            headers[_RETRYING_SINCE_HEADER],
            self._retry_delay_milliseconds / 1000,
            error,
        )
        return self._settle(delivery_tag, message_id, _Outcome.MOVED_TO_RETRY)

    def _dead_letter(
        self,
        delivery_tag: int,
        properties: pika.BasicProperties | None,
        body: bytes | None,
        message_id: str | None,
        reason: _DeadLetterReason,
        error: BaseException,
    ) -> bool:
        """Dead-letter a delivery: publish a copy that says why, then acknowledge the delivery.

        The copy keeps the body, the `message_id` property unchanged, and the application's
        properties and headers, including its retry headers, and gets this executor's user as
        its `user_id`, keeping the user who created the message in `x-kicak-original-publisher`.
        Its headers record why it was dead-lettered (`x-kicak-dead-letter-reason`,
        `x-kicak-error-type`, `x-kicak-error-message`), when (`x-kicak-dead-lettered-at`), and
        from which queue (`x-kicak-dead-lettered-from`), which `basic_nack()` cannot add. It is
        published as mandatory to the dead-letter exchange, right after the dead-letter route is
        declared again, and the delivery is acknowledged only after RabbitMQ has confirmed the
        copy, so a failure in between repeats the message instead of losing it: it is
        dead-lettered twice, or executed again. RabbitMQ dead-letters a rejected message at most
        once, dropping it if no queue takes it; a copy RabbitMQ returns as unroutable or refuses
        is noticed instead, and the delivery is requeued.

        Returns False if the connection was lost, or RabbitMQ took the delivery back (closing its
        channel, or cancelling the consumer) before the copy was published; the caller must
        reconnect, and RabbitMQ redelivers the message. RabbitMQ refusing to declare the
        dead-letter route is raised.
        """
        self._handling.start_settling()
        original = properties if properties is not None else pika.BasicProperties()
        headers = _dead_letter_headers(original.headers, reason, error, self._source_queue, _now())
        copy_properties = self._copy_properties(
            original, headers, message_id=original.message_id, expiration=None
        )
        try:
            self._declare_dead_letter_route()
            self._check_delivery_held()
            logger.debug(
                "Dead-lettering message id=%s to %s reason=%s",
                message_id,
                self._dead_letter_exchange,
                reason,
            )
            self.channel.basic_publish(
                exchange=self._dead_letter_exchange,
                routing_key="",  # a fanout exchange ignores it
                body=b"" if body is None else body,
                properties=copy_properties,
                mandatory=True,
            )
        except (UnroutableError, NackError) as copy_error:
            logger.error(
                "Message id=%s could not be dead-lettered to %s; requeueing it instead: %r",
                message_id,
                self._dead_letter_exchange,
                copy_error,
            )
            return self._settle(delivery_tag, message_id, _Outcome.REQUEUED)
        except Exception as copy_error:
            if not self._is_retryable_rabbitmq_error(copy_error):
                logger.exception(
                    "Dead-lettering message id=%s to %s failed; stopping the executor",
                    message_id,
                    self._dead_letter_exchange,
                )
                raise
            logger.warning(
                "Connection or channel lost while dead-lettering message id=%s to %s; RabbitMQ "
                "will redeliver it: %s",
                message_id,
                self._dead_letter_exchange,
                copy_error,
            )
            self._discard_connection()
            return False
        return self._settle(delivery_tag, message_id, reason)

    def _copy_properties(
        self,
        original: pika.BasicProperties,
        headers: Mapping[str, object],
        *,
        message_id: object,
        expiration: str | None,
    ) -> pika.BasicProperties:
        """Return the properties of a copy this executor publishes of a delivery.

        The copy keeps the delivery's application properties, with `headers`, `message_id`, and
        `expiration` (never the delivery's own) as given, and is persistent. Its `user_id` is
        this executor's user, because RabbitMQ refuses any other, so the user who created the
        message is kept in the `x-kicak-original-publisher` header.
        """
        copy_headers = dict(headers)
        original_publisher = _original_publisher_of(original)
        if original_publisher is None:
            copy_headers.pop(_ORIGINAL_PUBLISHER_HEADER, None)  # an invalid value
        else:
            copy_headers[_ORIGINAL_PUBLISHER_HEADER] = original_publisher
        return pika.BasicProperties(
            content_type=original.content_type,
            content_encoding=original.content_encoding,
            headers=copy_headers,
            delivery_mode=2,
            priority=original.priority,
            correlation_id=original.correlation_id,
            reply_to=original.reply_to,
            expiration=expiration,
            message_id=message_id,
            timestamp=original.timestamp,
            type=original.type,
            app_id=original.app_id,
            user_id=self._user_id,
        )

    def _settle(
        self,
        delivery_tag: int,
        message_id: str | None,
        outcome: _Outcome | _DeadLetterReason,
    ) -> bool:
        """Settle the delivery as `outcome`, and record it once RabbitMQ has taken it.

        `_Outcome.REQUEUED` returns the delivery to the queue with `basic_nack()`, after declaring
        the dead-letter route again: a quorum queue dead-letters a requeued message once it
        reaches its delivery limit, and RabbitMQ drops a message dead-lettered into a missing
        exchange. Every other outcome acknowledges it, once it is done with: completed, moved to
        the retry queue, or dead-lettered for the given reason.

        Returns False if the connection was lost, or RabbitMQ took the delivery back because it
        outlasted `consumer_timeout`, closing the channel (RabbitMQ 4.2) or cancelling the
        consumer (4.3 and newer, which ignore the settlement): RabbitMQ then requeues the delivery
        itself, so the settlement is not retried, nor recorded, and the caller must reconnect.
        Other errors are raised.
        """
        self._handling.start_settling()
        requeue = outcome is _Outcome.REQUEUED
        if requeue:
            action = "requeueing"
        elif isinstance(outcome, _DeadLetterReason):
            action = "dead-lettering"
        else:
            action = "acknowledging"
        try:
            if requeue:
                self._declare_dead_letter_route()
                self.channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            else:
                self.channel.basic_ack(delivery_tag)
            cancelled = self._consumer_cancelled()
        except Exception as error:
            if not self._is_retryable_rabbitmq_error(error):
                raise
            logger.warning(
                "Connection or channel lost while %s message id=%s delivery_tag=%i; RabbitMQ "
                "will redeliver it: %s",
                action,
                message_id,
                delivery_tag,
                error,
            )
            self._discard_connection()
            return False
        if cancelled:
            logger.warning(
                "RabbitMQ cancelled the consumer of queue %s while %s message id=%s "
                "delivery_tag=%i, e.g. because the message outlasted the queue's consumer "
                "timeout; it returns the message to the queue and redelivers it; reconnecting",
                self._source_queue,
                action,
                message_id,
                delivery_tag,
            )
            self._discard_connection()
            return False
        self._settlement = outcome
        return True

    def _consumer_cancelled(self) -> bool:
        """Return whether RabbitMQ cancelled the consumer while keeping its channel open.

        RabbitMQ 4.3 and newer do so when a delivery outlasts the consumer timeout, which they
        evaluate for quorum queues only: they return the delivery to the queue and ignore a later
        acknowledgement (RabbitMQ 4.2 closed the channel instead). Deleting the queue cancels the
        consumer too. Events RabbitMQ has already sent, such as a cancellation while `execute()`
        ran, are processed first; `_watch_for_consumer_cancellation()` notes the cancellation.
        """
        cast(pika.BlockingConnection, self._connection).process_data_events(time_limit=0)
        channel = self.channel
        return bool(channel.is_open and self._cancelled_consumer_channel is channel)

    def _check_delivery_held(self) -> None:
        """Raise `ChannelWrongStateError` if RabbitMQ took back the delivery being processed.

        RabbitMQ takes it back, and redelivers it, when it outlasts the consumer timeout: RabbitMQ
        4.2 closes the channel that delivered it, while 4.3 and newer cancel the consumer. Nothing
        may then be published for the delivery, neither its results nor its copy for the retry
        queue or the dead-letter queue, because the redelivery decides its outcome again.
        """
        if self._consumer_cancelled():
            raise ChannelWrongStateError(
                "RabbitMQ cancelled the consumer of the message, which returns it to the queue"
            )
        if not self.channel.is_open:
            raise ChannelWrongStateError("RabbitMQ closed the channel that delivered the message")

    def _accepted_class(self, sent_type: str | None) -> type[Accepted]:
        """Return the accepted class named by a delivery's `type` property, `sent_type`.

        Raises `MalformedMessageError` if the property is missing or names no accepted class: the
        message is not one this executor accepts, even if its body would decode.
        """
        message_class = None if sent_type is None else self._accepted_types.get(sent_type)
        if message_class is not None:
            return message_class
        accepted = ", ".join(repr(type_name) for type_name in self._accepted_types)
        if sent_type is None:
            raise MalformedMessageError(
                f"Received a message without a type property; accepted types: {accepted}"
            )
        raise MalformedMessageError(
            f"Received a message of type {sent_type!r}, which is not accepted; "
            f"accepted types: {accepted}"
        )

    def _decode(self, body: bytes, message_class: type[Accepted]) -> Accepted:
        """Decode `body` into `message_class`, classifying failures as malformed.

        Any exception while building the message counts, including one raised by the message
        class's own validation in `__post_init__`: the body is at fault, whoever produced it.
        """
        try:
            return message_class.from_bytes(body)
        except Exception as error:
            raise MalformedMessageError(
                f"Could not decode message as {message_class.__name__}: {error}"
            ) from error

    def _execute_with_retries(
        self, message: Accepted, message_id: str
    ) -> Result | Sequence[Result] | None:
        """Call execute(), retrying with backoff while it raises TransientProcessingError."""
        for attempt in range(1, self._max_processing_attempts + 1):
            logger.debug(
                "Processing message id=%s attempt %d/%d",
                message_id,
                attempt,
                self._max_processing_attempts,
            )
            self._set_attempt(attempt)
            try:
                with self._handling.timing(_TimedPart.EXECUTE):
                    return self.execute(message)
            except TransientProcessingError as error:
                if attempt == self._max_processing_attempts:
                    raise
                delay_seconds = _capped_backoff_seconds(attempt - 1)
                logger.warning(
                    "Transient processing failure id=%s, retrying attempt %d/%d in %.1f "
                    "seconds: %s",
                    message_id,
                    attempt + 1,
                    self._max_processing_attempts,
                    delay_seconds,
                    error,
                )
                with self._handling.timing(_TimedPart.BACKOFF):
                    time.sleep(delay_seconds)
        raise RuntimeError("Processing retry loop exited unexpectedly")

    def _prepare_publications(self, results: object) -> list[tuple[str, KicakMessage, bytes]]:
        """Validate and serialize every result before anything is published or acknowledged.

        A terminator returning results raises `RuntimeError`, and a result of another class
        than the declared result type `TypeError`: the executor's code contradicts its own
        declaration. A returned value that is not a message, or a result that cannot be
        serialized, raises `MalformedMessageError` instead: it can depend on the data the message
        brought back from outside, and the input's results can never be published.
        """
        terminator_error = (
            "execute() returned results, but the executor has no destination_exchange"
        )
        try:
            messages = _as_messages(results)
        except TypeError as error:
            if self._destination_exchange is None:
                raise RuntimeError(terminator_error) from error
            raise MalformedMessageError(f"execute() returned an invalid result: {error}") from error
        if not messages:
            return []
        destination_exchange = self._destination_exchange
        result_type = self._result_type
        if destination_exchange is None or result_type is None:
            raise RuntimeError(terminator_error)
        for message in messages:
            # Exactly the declared class: a subclass would be sent with its own type name
            if type(message) is not result_type:
                raise TypeError(
                    f"execute() returned a result of type {type(message).__name__}, but "
                    f"{type(self).__name__} declares its results as {result_type.__name__}"
                )
        publications = []
        for message in messages:
            try:
                body = message.to_bytes()
            except Exception as error:
                raise MalformedMessageError(
                    "execute() returned a result that cannot be serialized, "
                    f"type={type(message).__name__}: {error}"
                ) from error
            publications.append((destination_exchange, message, body))
        return publications

    def _publish_results_holding_input(
        self, publications: list[tuple[str, KicakMessage, bytes]], message_id: str
    ) -> bool:
        """Publish the results, holding the input and its results while they are unroutable.

        Returns True once the results are committed, and False if `stop()` is called while they
        are held; the caller then requeues the input. Between attempts the connection keeps
        answering heartbeats, and `execute()` is not called again. Errors of `_publish_results()`
        other than `UnroutableError` are raised, as is `ChannelWrongStateError` if RabbitMQ
        takes the input back during the hold, closing its channel or cancelling the consumer,
        e.g. on `consumer_timeout`.
        """
        handling = self._handling
        attempt = 0
        while True:
            try:
                with handling.timing(_TimedPart.PUBLISH):
                    self._publish_results(publications)
            except UnroutableError as error:
                delay_seconds = _capped_backoff_seconds(attempt) * random.random()
                attempt += 1
                self._set_activity(ExecutorStatus.DOWNSTREAM_RECOVER)
                logger.error(
                    "RabbitMQ returned %d result(s) of message id=%s as unroutable: no queue is "
                    "bound to exchange %s; holding the message and retrying in %.1f seconds",
                    len(error.messages),
                    message_id,
                    self._destination_exchange,
                    delay_seconds,
                )
                with handling.timing(_TimedPart.HELD):
                    held = self._hold_delivery(delay_seconds)
                if not held:
                    return False
            else:
                self._set_activity(ExecutorStatus.SENDING_RESULT)
                return True

    def _hold_delivery(self, seconds: float) -> bool:
        """Wait for `seconds` while holding an unacknowledged delivery, answering heartbeats.

        Waits in steps of at most one second. Returns False if `stop()` was called. Raises
        `ChannelWrongStateError` if RabbitMQ took the delivery back, closing its channel or
        cancelling the consumer, which requeues it, and connection errors if the connection is
        lost.
        """
        connection = cast(pika.BlockingConnection, self._connection)
        remaining = seconds
        while True:
            if self._stop_requested:
                return False
            self._check_delivery_held()
            if remaining <= 0:
                return True
            step = min(remaining, _STOP_POLL_SECONDS)
            connection.sleep(step)
            remaining -= step

    def _publish_results(self, publications: list[tuple[str, KicakMessage, bytes]]) -> None:
        """Verify the output side, then publish all results in one transaction.

        RabbitMQ delivers all of the results or none. The destination exchange and every
        destination queue are declared and bound first, on the results channel, so a deleted
        exchange, queue, or binding is restored right before publishing; the status is then
        `SENDING_RESULT`.

        Returns once RabbitMQ has committed the transaction and taken responsibility for every
        result. RabbitMQ routes the results only at the commit, so a result that no queue is bound
        to is reported only after it: this raises `UnroutableError` with the returned results.
        Connection and channel errors, including RabbitMQ refusing a declaration or the commit,
        are raised as they are; RabbitMQ discards an uncommitted transaction when its channel
        closes. If RabbitMQ has taken the input back, closing its channel or cancelling the
        consumer, which requeues it, this raises `ChannelWrongStateError` instead of committing.
        Nothing is retried or reconnected here.
        """
        channel = self._transactional_results_channel()
        logger.debug("Verifying the output side of exchange %s", self._destination_exchange)
        self._declare_output_side(channel)
        if self._activity.status is not ExecutorStatus.DOWNSTREAM_RECOVER:
            # Held results keep that status until they are committed
            self._set_activity(ExecutorStatus.SENDING_RESULT)
        self._returned_results.clear()
        for exchange_name, result, body in publications:
            self._basic_publish(channel, exchange_name, result, body)
        # RabbitMQ requeues an input that outlasted its consumer timeout, so results committed for
        # it now would be published again when the redelivery is executed
        self._check_delivery_held()
        logger.debug("Committing the transaction of %d results", len(publications))
        channel.tx_commit()
        # RabbitMQ sends returned messages before the commit's reply, but pika hands them to the
        # return callback only while it processes connection events
        cast(pika.BlockingConnection, self._results_connection).process_data_events(time_limit=0)
        if self._returned_results:
            raise UnroutableError(list(self._returned_results))
        for exchange_name, result, _body in publications:
            self._log_published(exchange_name, result)

    def _transactional_results_channel(self) -> BlockingChannel:
        """Return this connection's transactional channel for results, opening it on first use.

        Results need a channel of their own: a channel in transaction mode cannot also be in
        confirm mode, and the input's acknowledgement must stay outside the transaction. RabbitMQ
        routes transactional messages only at the commit, so an acknowledgement committed with
        results that turn out to be unroutable would remove the input while its results are
        dropped, losing the message.
        """
        channel = self._results_channel
        if (
            channel is not None
            and channel.is_open
            and self._results_connection is not None
            and self._results_connection is self._connection
        ):
            return channel
        connection = self._connection
        if connection is None:
            raise RuntimeError("Not connected. Call connect() first.")
        logger.debug("Opening the transactional channel for results")
        channel = connection.channel()
        channel.tx_select()
        channel.add_on_return_callback(self._on_result_returned)
        self._results_channel = channel
        self._results_connection = connection
        return channel

    def _on_result_returned(
        self,
        _channel: BlockingChannel,
        method: Basic.Return,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        """Collect a result that RabbitMQ returned because no queue is bound to its exchange."""
        self._returned_results.append(ReturnedMessage(method, properties, body))

    def _start_consuming(
        self,
    ) -> Iterator[tuple[Basic.Deliver | None, pika.BasicProperties | None, bytes | None]]:
        """Start consuming, one message at a time, after ensuring a usable RabbitMQ connection."""
        self._ensure_connection_once()
        logger.debug("Setting RabbitMQ prefetch count to %d", _PREFETCH_COUNT)
        channel = self.channel
        channel.basic_qos(prefetch_count=_PREFETCH_COUNT)
        self._watch_for_consumer_cancellation(channel)
        logger.debug("Consuming from RabbitMQ queue=%s", self._source_queue)
        return iter(channel.consume(self._source_queue, inactivity_timeout=_STOP_POLL_SECONDS))

    def _watch_for_consumer_cancellation(self, channel: BlockingChannel) -> None:
        """Note RabbitMQ cancelling the consumer on `channel` as soon as pika processes it.

        RabbitMQ 4.3 and newer cancel the consumer of a delivery that outlasts a quorum queue's
        consumer timeout, and return the delivery to the queue; `_consumer_cancelled()` reads the
        note. pika's consume() generator learns of a cancellation only when it is advanced, and
        the public `BlockingChannel.add_on_cancel_callback()` never fires for it (pika 1.4 passes
        the generator's events around those callbacks), so the callback is registered on the
        channel that BlockingChannel wraps, its private `_impl`. This is the only place that uses
        a private pika attribute. Without it, a timed-out delivery is still executed again, but
        its results or its retry copy may be published for it too, and so twice.
        """
        register = getattr(getattr(channel, "_impl", None), "add_on_cancel_callback", None)
        if not callable(register):
            logger.warning(
                "Cannot watch for RabbitMQ cancelling the consumer of queue %s: this pika "
                "version's BlockingChannel has no _impl.add_on_cancel_callback(). A delivery that "
                "outlasts a quorum queue's consumer timeout on RabbitMQ 4.3 or newer is still "
                "executed again, but its results or retry copy may then be published twice",
                self._source_queue,
            )
            return
        register(lambda _method_frame: self._note_consumer_cancelled(channel))

    def _note_consumer_cancelled(self, channel: BlockingChannel) -> None:
        """Note that RabbitMQ cancelled the consumer on `channel`; see `_consumer_cancelled()`."""
        self._cancelled_consumer_channel = channel
