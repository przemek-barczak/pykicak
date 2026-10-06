"""Executor agent: long-running node that consumes, executes, and optionally publishes messages."""

from __future__ import annotations

import abc
import logging
import random
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import TypeVar, cast

import pika
from pika.adapters.blocking_connection import BlockingChannel, ReturnedMessage
from pika.exceptions import UnroutableError
from pika.exchange_type import ExchangeType
from pika.spec import Basic

from pykicak.abstracts import KicakAbstract
from pykicak.config import KicakConfig
from pykicak.messages import KicakMessage

logger = logging.getLogger(__name__)

_INITIAL_RETRY_DELAY_SECONDS = 0.5
_MAX_RETRY_DELAY_SECONDS = 30.0
_MAX_BACKOFF_EXPONENT = 16
_PREFETCH_COUNT = 1
_DEFAULT_HEARTBEAT_SECONDS = 600  # room for long LLM calls and heavy computation in execute()
_STOP_POLL_SECONDS = 1.0  # longest wait, for a message or a reconnection, before checking stop()
_T = TypeVar("_T")


class _StopRequested(Exception):
    """Raised inside run() when stop() interrupts the wait before a reconnection attempt."""


class ExecutorStatus(StrEnum):
    """Lifecycle status of an Executor, as returned by `KicakExecutorAbstract.get_status()`.

    Members are strings, so they compare equal to their values, e.g. `"RUNNING"`.
    """

    RUNNING = "RUNNING"
    """From creation, and while `run()` consumes, until `stop()` is called."""

    STOPPING = "STOPPING"
    """`stop()` was called; `run()` finishes the message in progress, then returns."""

    STOPPED = "STOPPED"
    """`run()` returned after a graceful stop."""

    CRASHED = "CRASHED"
    """`run()` ended by raising an exception: a failure or a hard stop, not a graceful stop."""


class QueueType(StrEnum):
    """RabbitMQ type of an Executor's input and dead-letter queues, its `queue_type` argument.

    Members are strings, so the values `"classic"` and `"quorum"` are accepted too.
    """

    CLASSIC = "classic"
    """A queue stored on one broker node; the default."""

    QUORUM = "quorum"
    """A queue replicated across the nodes of a RabbitMQ cluster, so it survives a node failure.

    It limits how often a message is redelivered: on RabbitMQ 4.0 and newer, by default, a
    message is dead-lettered on its 21st delivery instead of being requeued again.
    """


def _now() -> datetime:
    """The current time, timezone-aware in UTC."""
    return datetime.now(UTC)


class MalformedMessageError(Exception):
    """A delivery can never be processed successfully, so the Executor dead-letters it.

    The Executor raises it for a delivery that has no body or cannot be decoded into the
    expected message type. `execute()` may raise it to reject a message deterministically,
    for example when its contents fail application validation.
    """


class TransientProcessingError(Exception):
    """Processing failed for a reason that may clear on retry, e.g. an unavailable service.

    Raise it from `execute()`. The Executor calls `execute()` again, and if the failure
    persists it requeues the delivery with `basic_nack(requeue=True)`.
    """


def _capped_backoff_seconds(attempt: int) -> float:
    """Return the exponential backoff delay for zero-based `attempt`, capped at 30 seconds."""
    return min(
        _INITIAL_RETRY_DELAY_SECONDS * 2.0 ** min(attempt, _MAX_BACKOFF_EXPONENT),
        _MAX_RETRY_DELAY_SECONDS,
    )


def _as_messages(
    results: KicakMessage | Sequence[KicakMessage] | None,
) -> tuple[KicakMessage, ...]:
    """Normalize the value returned by execute() to a tuple of messages."""
    if results is None:
        return ()
    if isinstance(results, KicakMessage):
        return (results,)
    messages = tuple(results)
    for message in messages:
        if not isinstance(message, KicakMessage):
            raise TypeError(
                f"execute() must return KicakMessage instances, got {type(message).__name__}"
            )
    return messages


class KicakExecutorAbstract[M: KicakMessage](KicakAbstract, abc.ABC):
    """Long-running node that consumes messages, executes them, and optionally publishes results.

    `M` is the message type consumed from the source queue. Subclass
    `KicakExecutorAbstract[MyMessage]` so that `execute()` receives a `MyMessage` for type
    checkers; a subclass of the unparameterized class receives a `KicakMessage`.

    Delivery is at-least-once, so subclasses must be idempotent: a message can be executed, and
    its results published, more than once.

    Each delivery ends in exactly one outcome:

    - Success: the results returned by `execute()` are published to the destination exchange
      in one RabbitMQ transaction, so RabbitMQ delivers all of them or none, and the delivery is
      acknowledged only after the transaction is committed.
    - Deterministic failure (`MalformedMessageError`): the delivery is rejected with
      `basic_nack(requeue=False)`, so RabbitMQ routes it to the dead-letter exchange.
    - Transient failure (`TransientProcessingError`): `execute()` is retried, and if the failure
      persists the delivery is requeued with `basic_nack(requeue=True)`.
    - Any other error is unexpected: the delivery is dead-lettered as above, then the error is
      raised, stopping the executor.
    - Results returned by RabbitMQ as unroutable (`UnroutableError`: no queue is bound to the
      destination exchange): the delivery is requeued with `basic_nack(requeue=True)`, then the
      error is raised, stopping the executor.
    - Connection lost while publishing results: RabbitMQ discards the uncommitted results and
      requeues the delivery, and the executor never re-sends them on a new connection. It
      reconnects, receives the delivery again, and executes it again, publishing its results
      once.
    - Any other failure to publish the results, e.g. RabbitMQ refusing the commit: the error is
      raised, stopping the executor, and closing the connection requeues the delivery.
    - Connection lost while acknowledging or rejecting the delivery: RabbitMQ requeues it, so the
      executor reconnects and receives it again, unless it was stopping anyway because of an
      unexpected error or unroutable results, which it then still raises.

    Without a destination exchange the executor is a terminator, and `execute()` must return
    None.

    The executor takes one message at a time: RabbitMQ delivers the next message only after
    the previous one is acknowledged or rejected (prefetch count 1).

    Call `stop()`, from any thread or a signal handler, to make `run()` finish the message in
    progress and return before taking another one. `get_status()` and `get_timestamp()` report
    the Executor's lifecycle status and when it last changed or was read.

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
        dead_letter_exchange: str,
        dead_letter_queue: str,
        destination_exchange: str | None = None,
        queue_type: QueueType | str = QueueType.CLASSIC,
        max_processing_attempts: int = 3,
        heartbeat_seconds: int = _DEFAULT_HEARTBEAT_SECONDS,
    ) -> None:
        """Store this executor's topology, processing retry limit, and heartbeat timeout.

        `source_exchange` is the upstream exchange that `source_queue` is bound to (e.g. an
        Injector's destination exchange, or a previous Executor's). It is required so the
        Executor always consumes from a queue attached to its intended input exchange.

        `dead_letter_exchange` is set as the `x-dead-letter-exchange` argument of `source_queue`,
        and `dead_letter_queue` is bound to it, so rejected deliveries are retained for inspection
        or replay. Like `source_queue`, both are owned and declared by this executor.

        `destination_exchange`, if given, receives the results returned by `execute()`. This
        executor does not create or bind downstream queues; downstream consumers own their input
        queues and bindings. Omit the destination exchange to run in terminator mode.

        `queue_type` is the RabbitMQ queue type of both `source_queue` and `dead_letter_queue`:
        `QueueType.CLASSIC` (the default) or `QueueType.QUORUM`, a replicated queue that survives
        the loss of a cluster node. Their string values, `"classic"` and `"quorum"`, are accepted
        too, e.g. from an application's configuration; any other value raises `ValueError`. It
        is always declared explicitly, so a vhost's default queue type does not apply. RabbitMQ
        cannot change the type of an existing queue: declaring it with another type fails with
        `406 PRECONDITION_FAILED`.

        `max_processing_attempts` is how many times `execute()` is called for one delivery while
        it raises `TransientProcessingError`, before the delivery is requeued.

        `heartbeat_seconds` (1 to 65535, default 600) is the AMQP heartbeat timeout of this
        executor's connections. Set it above the longest time `execute()` can take for one
        message: the connection cannot answer heartbeats while `execute()` runs, so RabbitMQ
        closes a connection that stays silent for longer and redelivers its message. A larger
        value also delays the detection of a dead connection by the same amount.
        """
        for name, value in (
            ("source_queue", source_queue),
            ("source_exchange", source_exchange),
            ("dead_letter_exchange", dead_letter_exchange),
            ("dead_letter_queue", dead_letter_queue),
        ):
            if not value:
                raise ValueError(f"{name} must be a non-empty name")
        if destination_exchange is not None and not destination_exchange:
            raise ValueError("destination_exchange must be a non-empty name or None")
        if dead_letter_exchange in (source_exchange, destination_exchange):
            raise ValueError(
                "dead_letter_exchange must differ from source_exchange and destination_exchange"
            )
        if dead_letter_queue == source_queue:
            raise ValueError("dead_letter_queue must differ from source_queue")
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
        super().__init__(config, heartbeat_seconds=heartbeat_seconds)
        self._stop_requested = False
        self._running = False
        # Status state; each field has a single writer, so no locking is needed (see status docs)
        self._running_since = _now()
        self._stop_requested_at: datetime | None = None
        self._final_status: tuple[ExecutorStatus, datetime] | None = None
        self._status_read_at: datetime | None = None
        self._source_queue = source_queue
        self._source_exchange = source_exchange
        self._dead_letter_exchange = dead_letter_exchange
        self._dead_letter_queue = dead_letter_queue
        self._destination_exchange = destination_exchange
        self._queue_type = queue_type
        self._max_processing_attempts = max_processing_attempts
        # Transactional channel for results, opened on first use for each connection
        self._results_channel: BlockingChannel | None = None
        self._results_connection: pika.BlockingConnection | None = None
        self._returned_results: list[ReturnedMessage] = []

    def _declare_topology(self) -> None:
        """Declare the source queue and binding, the dead-letter route, and the destination."""
        logger.debug(
            "Declaring %s queue %s dead_letter_exchange=%s",
            self._queue_type,
            self._source_queue,
            self._dead_letter_exchange,
        )
        self.channel.queue_declare(
            queue=self._source_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": self._dead_letter_exchange,
                "x-queue-type": self._queue_type.value,
            },
        )
        self._declare_fanout_exchange(self._source_exchange)
        self._bind_queue(self._source_queue, self._source_exchange)

        self._declare_fanout_exchange(self._dead_letter_exchange)
        logger.debug("Declaring %s queue %s", self._queue_type, self._dead_letter_queue)
        self.channel.queue_declare(
            queue=self._dead_letter_queue,
            durable=True,
            arguments={"x-queue-type": self._queue_type.value},
        )
        self._bind_queue(self._dead_letter_queue, self._dead_letter_exchange)

        if self._destination_exchange is not None:
            self._declare_fanout_exchange(self._destination_exchange)

    def _declare_fanout_exchange(self, exchange_name: str) -> None:
        """Idempotently declare a durable fanout exchange."""
        logger.debug("Declaring exchange %s", exchange_name)
        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type=ExchangeType.fanout, durable=True
        )

    def _bind_queue(self, queue_name: str, exchange_name: str) -> None:
        """Bind a queue to an exchange."""
        logger.debug("Binding queue %s -> exchange %s", queue_name, exchange_name)
        self.channel.queue_bind(queue=queue_name, exchange=exchange_name)

    def _retry_rabbitmq_operation(
        self, operation: Callable[[], _T], operation_name: str
    ) -> _T:
        """Retry transient RabbitMQ failures forever with capped exponential full jitter."""
        attempt = 0
        while True:
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
    def message_type(self) -> type[M]:
        """Return the `KicakMessage` subclass that messages on the source queue are decoded into.

        A delivery that cannot be decoded into this type is dead-lettered as malformed: one that
        is not a JSON object, has missing or unexpected fields, or has no valid `message_id`. The
        types of other field values are not checked; validate them in `execute()` and raise
        `MalformedMessageError` if needed.
        """

    @abc.abstractmethod
    def execute(self, message: M) -> KicakMessage | Sequence[KicakMessage] | None:
        """Process one decoded message and return the results to publish.

        Return a message, a sequence of messages, or None. Results are published to the
        destination exchange only after `execute()` succeeds, all in one RabbitMQ transaction,
        and the input is acknowledged only after that transaction is committed. A terminator
        must return None.

        Raise `TransientProcessingError` for failures that may clear on retry, and
        `MalformedMessageError` to reject the message deterministically (dead-letter it).

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

        The status becomes `STOPPING`, unless `run()` has already ended as `STOPPED` or
        `CRASHED`; calling `stop()` again changes nothing.
        """
        if self._stop_requested_at is None:
            self._stop_requested_at = _now()
        self._stop_requested = True

    def get_status(self) -> str:
        """Return the current status as a string: one of the `ExecutorStatus` values.

        `"RUNNING"` from creation until `stop()` is called, `"STOPPING"` from then until `run()`
        returns, `"STOPPED"` once `run()` has returned after a graceful stop, and `"CRASHED"` if
        `run()` ended by raising an exception. A later `stop()` never changes `"STOPPED"` or
        `"CRASHED"`; calling `run()` again after `"CRASHED"`, without a stop request, makes the
        Executor `"RUNNING"` again.

        Reading the status updates the timestamp returned by `get_timestamp()`. Safe to call
        from any thread.
        """
        status, _changed_at = self._status_and_change_time()
        self._status_read_at = _now()
        return status.value

    def get_timestamp(self) -> str:
        """Return when the status last changed or was read by `get_status()`, in ISO 8601.

        The time is in UTC with its offset, e.g. `"2026-10-01T17:05:42.123456+00:00"`. Reading
        the timestamp does not update it. Safe to call from any thread.
        """
        _status, changed_at = self._status_and_change_time()
        read_at = self._status_read_at
        latest = changed_at if read_at is None or changed_at >= read_at else read_at
        return latest.isoformat()

    def _status_and_change_time(self) -> tuple[ExecutorStatus, datetime]:
        """Derive the current status, and when it was entered, from the single-writer fields."""
        final_status = self._final_status
        if final_status is not None:
            return final_status
        stop_requested_at = self._stop_requested_at
        if stop_requested_at is not None:
            return ExecutorStatus.STOPPING, stop_requested_at
        return ExecutorStatus.RUNNING, self._running_since

    def run(self) -> None:
        """Connect, then consume the source queue until `stop()` is called.

        See the class docstring for when a delivery is acknowledged, requeued, or dead-lettered.

        Returns normally only after `stop()`, with the status `"STOPPED"`. Otherwise it stops by
        raising, with the status `"CRASHED"`: on failures, on RabbitMQ cancelling the consumer
        (`RuntimeError`), and on a `BaseException` such as `SystemExit` or `KeyboardInterrupt`,
        which interrupts even a message being processed. In every case the connection is closed,
        and RabbitMQ requeues a delivery that was not yet acknowledged or rejected.
        """
        if self._stop_requested:
            if self._final_status is None:
                self._final_status = (ExecutorStatus.STOPPED, _now())
            logger.info(
                "Executor for queue %s was stopped before it started; not consuming",
                self._source_queue,
            )
            return
        if self._final_status is not None:  # running again after a crash
            self._running_since = _now()
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
        try:
            self.connect()
            consumer = self._retry_rabbitmq_operation(
                self._start_consuming, "RabbitMQ consumer startup"
            )
            while not self._stop_requested:
                try:
                    method, _properties, body = next(consumer)
                except StopIteration:
                    logger.error(
                        "RabbitMQ cancelled the consumer of queue %s; stopping the executor",
                        self._source_queue,
                    )
                    raise RuntimeError(
                        f"RabbitMQ cancelled the consumer of queue {self._source_queue!r}, "
                        "e.g. because the queue was deleted"
                    ) from None
                except Exception as error:
                    if not self._is_retryable_rabbitmq_error(error):
                        raise
                    logger.warning("Connection lost, reconnecting")
                    self._discard_connection()
                    consumer = self._retry_rabbitmq_operation(
                        self._start_consuming,
                        "RabbitMQ consumer reconnection",
                    )
                    continue

                if method is None:
                    continue  # no message within _STOP_POLL_SECONDS; check stop() again
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
                if not self._handle_delivery(delivery_tag, body):
                    consumer = self._retry_rabbitmq_operation(
                        self._start_consuming,
                        "RabbitMQ consumer reconnection",
                    )
            logger.info("Executor for queue %s stopped on request", self._source_queue)
        except _StopRequested:
            logger.info(
                "Executor for queue %s stopped on request while reconnecting", self._source_queue
            )
        except BaseException:
            self._running = False
            self._close_without_masking()  # the error that ended run() is the one to raise
            raise
        self._running = False
        self.close()

    def _handle_delivery(self, delivery_tag: int, body: bytes | None) -> bool:
        """Execute one delivery, then acknowledge, requeue, or dead-letter it.

        Returns False if the connection was lost while publishing the results or settling the
        delivery. RabbitMQ then discards the uncommitted results and requeues the delivery, and
        the results are not re-sent on a new connection: executing the delivery again publishes
        them once. The caller must reconnect and resume consuming.
        """
        message_id = "unknown"  # until the body is decoded
        try:
            if body is None:
                raise MalformedMessageError("Received a message with no body")
            message = self._decode(body)
            message_id = message.message_id
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
        except MalformedMessageError:
            logger.exception(
                "Message dead-lettered id=%s queue=%s delivery_tag=%i dead_letter_exchange=%s",
                message_id,
                self._source_queue,
                delivery_tag,
                self._dead_letter_exchange,
            )
            return self._settle(delivery_tag, message_id, "dead-lettering", requeue=False)
        except TransientProcessingError as error:
            logger.warning(
                "Message requeued after %d failed processing attempts id=%s queue=%s "
                "delivery_tag=%i: %s",
                self._max_processing_attempts,
                message_id,
                self._source_queue,
                delivery_tag,
                error,
            )
            return self._settle(delivery_tag, message_id, "requeueing", requeue=True)
        except Exception:
            logger.exception(
                "Message processing failed unexpectedly id=%s queue=%s delivery_tag=%i; "
                "dead-lettering it to %s and stopping the executor",
                message_id,
                self._source_queue,
                delivery_tag,
                self._dead_letter_exchange,
            )
            self._settle(delivery_tag, message_id, "dead-lettering", requeue=False)
            raise
        else:
            try:
                if publications:
                    self._publish_results(publications)
            except UnroutableError:
                logger.exception(
                    "RabbitMQ returned the results of message id=%s as unroutable; requeueing it "
                    "and stopping the executor",
                    message_id,
                )
                self._settle(delivery_tag, message_id, "requeueing", requeue=True)
                raise
            except Exception as error:
                if not self._is_retryable_rabbitmq_error(error):
                    logger.exception(
                        "Results of message id=%s could not be published; stopping the executor",
                        message_id,
                    )
                    raise
                logger.warning(
                    "Connection lost while publishing results of message id=%s; discarding the "
                    "uncommitted results, RabbitMQ will redeliver the message: %s",
                    message_id,
                    error,
                )
                self._discard_connection()
                return False
            logger.debug(
                "Acknowledging message id=%s delivery_tag=%i", message_id, delivery_tag
            )
            if not self._settle(delivery_tag, message_id, "acknowledging"):
                return False
            logger.debug("Message acknowledged id=%s", message_id)
            return True

    def _settle(
        self, delivery_tag: int, message_id: str, action: str, *, requeue: bool | None = None
    ) -> bool:
        """Acknowledge the delivery, or with `requeue` given, reject it with `basic_nack()`.

        Returns False if the connection was lost: RabbitMQ then requeues the delivery itself, so
        the settlement is not retried, and the caller must reconnect. Other errors are raised.
        """
        try:
            if requeue is None:
                self.channel.basic_ack(delivery_tag)
            else:
                self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        except Exception as error:
            if not self._is_retryable_rabbitmq_error(error):
                raise
            logger.warning(
                "Connection lost while %s message id=%s delivery_tag=%i; RabbitMQ will "
                "redeliver it: %s",
                action,
                message_id,
                delivery_tag,
                error,
            )
            self._discard_connection()
            return False
        return True

    def _decode(self, body: bytes) -> M:
        """Decode `body` into the expected message type, classifying failures as malformed."""
        expected_type = self.message_type()
        try:
            return expected_type.from_bytes(body)
        except (ValueError, TypeError) as error:
            raise MalformedMessageError(
                f"Could not decode message as {expected_type.__name__}: {error}"
            ) from error

    def _execute_with_retries(
        self, message: M, message_id: str
    ) -> KicakMessage | Sequence[KicakMessage] | None:
        """Call execute(), retrying with backoff while it raises TransientProcessingError."""
        for attempt in range(1, self._max_processing_attempts + 1):
            logger.debug(
                "Processing message id=%s attempt %d/%d",
                message_id,
                attempt,
                self._max_processing_attempts,
            )
            try:
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
                time.sleep(delay_seconds)
        raise RuntimeError("Processing retry loop exited unexpectedly")

    def _prepare_publications(
        self, results: KicakMessage | Sequence[KicakMessage] | None
    ) -> list[tuple[str, KicakMessage, bytes]]:
        """Validate and serialize every result before anything is published or acknowledged."""
        messages = _as_messages(results)
        if not messages:
            return []
        if self._destination_exchange is None:
            raise RuntimeError(
                "execute() returned results, but the executor has no destination_exchange"
            )
        return [
            (self._destination_exchange, message, message.to_bytes()) for message in messages
        ]

    def _publish_results(self, publications: list[tuple[str, KicakMessage, bytes]]) -> None:
        """Publish all results in one transaction, so RabbitMQ delivers all of them or none.

        Returns once RabbitMQ has committed the transaction and taken responsibility for every
        result. RabbitMQ routes the results only at the commit, so a result that no queue is bound
        to is reported only after it: this raises `UnroutableError` with the returned results.
        Connection and channel errors, including RabbitMQ refusing the commit, are raised as they
        are; RabbitMQ discards an uncommitted transaction when its channel closes. Nothing is
        retried or reconnected here.
        """
        channel = self._transactional_results_channel()
        self._returned_results.clear()
        for exchange_name, result, body in publications:
            self._basic_publish(channel, exchange_name, result, body)
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
        self.channel.basic_qos(prefetch_count=_PREFETCH_COUNT)
        logger.debug("Consuming from RabbitMQ queue=%s", self._source_queue)
        return iter(
            self.channel.consume(self._source_queue, inactivity_timeout=_STOP_POLL_SECONDS)
        )
