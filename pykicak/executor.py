"""Executor agent: long-running node that consumes, executes, and optionally publishes messages."""

from __future__ import annotations

import abc
import random
import time
from collections.abc import Callable, Sequence
from typing import cast

import pika
from pika.exceptions import UnroutableError
from pika.spec import Basic

from pykicak._executor import logger
from pykicak._executor.headers import _retry_round_of, _retrying_since_of, _sent_type
from pykicak._executor.message_types import _as_messages, _declared_message_types
from pykicak._executor.session import (
    _Consumer,
    _ConsumingSession,
    _is_result_too_large,
    _is_retryable_error,
)
from pykicak._executor.settings import (
    _STOP_POLL_SECONDS,
    _capped_backoff_seconds,
    _ExecutorSettings,
)
from pykicak._executor.settlement import _Settlement
from pykicak._executor.statistics import _Handling, _TimedPart
from pykicak._executor.status import (
    DeadLetterCounts,
    ExecutorCounts,
    ExecutorInfo,
    ExecutorStatus,
    _DeadLetterReason,
    _Delivery,
    _Outcome,
    _StatusBoard,
)
from pykicak._executor.topology import QueueType, _ExecutorTopology
from pykicak.abstracts import KicakAbstract
from pykicak.config import KicakConfig
from pykicak.messages import KicakMessage

__all__ = [
    "DeadLetterCounts",
    "ExecutorCounts",
    "ExecutorInfo",
    "ExecutorStatus",
    "KicakExecutorAbstract",
    "MalformedMessageError",
    "QueueType",
    "TransientProcessingError",
]

_DEFAULT_HEARTBEAT_SECONDS = 600  # room for long LLM calls and heavy computation in execute()
_DEFAULT_INPUT_CHECK_SECONDS = 10.0
_DEFAULT_RETRY_DELAY_SECONDS = 10.0

type _Publication = tuple[str, KicakMessage, bytes]
"""A result ready to publish: the exchange, the message, and its body."""


class _StopRequested(Exception):
    """Raised inside run() when stop() cancels a connection attempt or the wait before one."""


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
    returns it to the back of the source queue after `retry_delay_seconds`, to be executed again,
    up to `max_retry_rounds` times. A message still failing after that is dead-lettered, with the
    reason `retries-exhausted`. The Executor continues with the next message.
    """


def _check_results_destination(
    executor_class: type, result_type: type[KicakMessage] | None, destination_exchange: str | None
) -> None:
    """Raise `ValueError` unless an executor with results has a destination, and only then."""
    name = executor_class.__name__
    if result_type is None and destination_exchange is not None:
        raise ValueError(
            f"{name} declares the result type None, so it is a terminator, "
            "which publishes nothing: give it no destination_exchange or destination_queues"
        )
    if result_type is not None and destination_exchange is None:
        raise ValueError(
            f"{name} declares its results as {result_type.__name__}: give it "
            "the destination_exchange and destination_queues to publish them to"
        )


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
      source queue after `retry_delay_seconds`, up to `max_retry_rounds` times. Once it has had
      all its rounds, it is dead-lettered as above, with the reason `retries-exhausted`.
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
        max_retry_rounds: int,
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
        where it is executed again, with another `max_processing_attempts` calls.

        `max_retry_rounds` (required, an int, 0 or more) is how many times a message may be
        moved to the retry queue. A message whose last round fails too is dead-lettered instead,
        with the reason `retries-exhausted`, so `execute()` is called at most
        `(max_retry_rounds + 1) * max_processing_attempts` times for it, and it is dead-lettered no
        sooner than `max_retry_rounds * retry_delay_seconds` after its first move. With 0, it is
        never moved: a message still failing after its first `max_processing_attempts` calls is
        dead-lettered. The rounds are counted in the message's `x-kicak-retry-round` header, which
        any instance can read, and start again for a message sent back from the dead-letter
        queue. There is deliberately no default: how long a failing service may stay down, and
        how much retrying it may cost, depends on the agent.

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
        accepted_types, result_type = _declared_message_types(type(self), KicakExecutorAbstract)
        topology = _ExecutorTopology.checked(
            source_queue=source_queue,
            source_exchange=source_exchange,
            destination_exchange=destination_exchange,
            destination_queues=destination_queues,
            queue_type=queue_type,
        )
        _check_results_destination(type(self), result_type, topology.destination_exchange)
        settings = _ExecutorSettings.checked(
            max_processing_attempts=max_processing_attempts,
            max_retry_rounds=max_retry_rounds,
            retry_delay_seconds=retry_delay_seconds,
            input_check_seconds=input_check_seconds,
        )
        # The base class also accepts None, the broker's proposal; the range is checked there
        if isinstance(heartbeat_seconds, bool) or not isinstance(heartbeat_seconds, int):
            raise TypeError("heartbeat_seconds must be an int")
        super().__init__(config, heartbeat_seconds=heartbeat_seconds)
        # The classes Accepted stands for, by the type name they are sent with, and Result's class
        self._accepted_types = cast(dict[str, type[Accepted]], accepted_types)
        self._result_type = result_type
        self._topology = topology
        self._settings = settings
        # What the executor is doing, written without locks by run() and stop(); see get_info()
        self._status: _StatusBoard[Accepted] = _StatusBoard(topology.source_queue)
        self._running = False
        self._input_checked_at = 0.0  # time.monotonic() of the last verification of the input side
        # The consumer of the current connection, created for it on first use
        self._session: _ConsumingSession | None = None

    def _connection_label(self) -> str:
        """Name the executor's role, `executor` or `terminator`, and its queue in its connection."""
        role = "terminator" if self._topology.destination_exchange is None else "executor"
        return f"{role} queue={self._topology.source_queue}"

    def _declare_topology(self) -> None:
        """Declare the input side and, unless a terminator, the output side, on a new connection."""
        channel = self.channel
        self._topology.declare_input_side(channel)
        self._topology.declare_output_side(channel)
        self._input_checked_at = time.monotonic()

    def _current_session(self) -> _ConsumingSession:
        """Return the session of the current connection's consumer, creating it on first use.

        Raises `RuntimeError` when not connected.
        """
        connection, channel = self._connection, self.channel
        session = self._session
        if (
            session is None
            or session.connection is not connection
            or session.channel is not channel
        ):
            session = _ConsumingSession(
                cast(pika.BlockingConnection, connection), channel, self._user_id
            )
            self._session = session
        return session

    @staticmethod
    def _is_retryable_rabbitmq_error(error: Exception) -> bool:
        """Return whether an error is likely to clear after reconnecting.

        Besides the transient errors of every agent, this includes RabbitMQ closing the consuming
        channel because a delivery outlasted its `consumer_timeout`: as when the connection is
        lost, RabbitMQ requeues the delivery, and the executor reconnects and receives it again.
        """
        return _is_retryable_error(error)

    def _retry_rabbitmq_operation[T](self, operation: Callable[[], T], operation_name: str) -> T:
        """Retry transient RabbitMQ failures forever with capped exponential full jitter.

        Inside `run()`, a stop request raises `_StopRequested` instead of making the next attempt,
        the first one included, so a stopping executor never opens a connection only to close it,
        and a failed attempt makes the status `UPSTREAM_RECOVER`.
        """
        attempt = 0
        while True:
            if self._running and self._status.stop_requested:
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
                    self._status.set_status(ExecutorStatus.UPSTREAM_RECOVER)
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
            if self._running and self._status.stop_requested:
                return False
            step = min(remaining, _STOP_POLL_SECONDS)
            time.sleep(step)
            remaining -= step
        return not (self._running and self._status.stop_requested)

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
          retry queue for `retry_delay_seconds` and is then executed again, up to
          `max_retry_rounds` times; then it is dead-lettered. The executor continues.
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
        self._status.request_stop()

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
        return self._status.snapshot()

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
        status = self._status
        if status.stop_requested:
            if not status.ended:
                status.end(ExecutorStatus.STOPPED)
            logger.info(
                "Executor for queue %s was stopped before it started; not consuming",
                self._topology.source_queue,
            )
            return
        if status.ended:  # running again after a crash; the counts go on
            status.restart()
        try:
            self._consume_until_stopped()
        except BaseException as error:
            status.end(ExecutorStatus.CRASHED)
            logger.error(
                "Executor for queue %s crashed with %s",
                self._topology.source_queue,
                type(error).__name__,
            )
            raise
        status.end(ExecutorStatus.STOPPED)

    def _consume_until_stopped(self) -> None:
        """Connect and consume until a stop request; raise on any other end."""
        logger.debug("Starting Executor consumer for queue=%s", self._topology.source_queue)
        self._running = True
        stopped_while = ""
        try:
            self.connect()
            consumer = self._retry_rabbitmq_operation(
                self._start_consuming, "RabbitMQ consumer startup"
            )
            self._status.set_status(ExecutorStatus.WAITING_FOR_MESSAGE)
            while not self._status.stop_requested:
                consumer = self._consume_next(consumer)
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
        self._status.log_stopped(stopped_while)

    def _consume_next(self, consumer: _Consumer) -> _Consumer:
        """Wait for the next delivery and handle it; return the consumer to wait on next.

        That is a new consumer after reconnecting: when the connection or channel was lost, when
        RabbitMQ cancelled the consumer, or when the input side could not be verified.
        """
        try:
            method, properties, body = next(consumer)
        except StopIteration:
            # pika ends the generator when RabbitMQ cancels the consumer, e.g. because the queue
            # was deleted; like every connection, the reconnection declares it again
            logger.error(
                "RabbitMQ cancelled the consumer of queue %s, e.g. because the queue was "
                "deleted; reconnecting, which declares the queue again",
                self._topology.source_queue,
            )
            self._discard_connection()
            return self._reconnect()
        except Exception as error:
            if not self._is_retryable_rabbitmq_error(error):
                raise
            logger.warning("Connection or channel lost, reconnecting: %s", error)
            self._discard_connection()
            return self._reconnect()
        if method is None:
            return self._while_idle(consumer)
        if self._status.stop_requested:
            logger.info(
                "Stop requested; leaving message delivery_tag=%s from queue %s "
                "unprocessed for RabbitMQ to redeliver",
                method.delivery_tag,
                self._topology.source_queue,
            )
            return consumer
        return self._take_delivery(consumer, method, properties, body)

    def _while_idle(self, consumer: _Consumer) -> _Consumer:
        """No message came within a second: verify the input side, if due, unless stopping."""
        if (
            not self._status.stop_requested
            and self._input_check_due()
            and not self._verify_input_side()
        ):
            return self._reconnect()
        return consumer

    def _take_delivery(
        self,
        consumer: _Consumer,
        method: Basic.Deliver,
        properties: pika.BasicProperties | None,
        body: bytes | None,
    ) -> _Consumer:
        """Handle a delivery, then verify the input side before waiting for the next one."""
        # A delivery from RabbitMQ always has a tag; pika types it as optional
        delivery_tag = cast(int, method.delivery_tag)
        self._status.set_status(ExecutorStatus.PROCESSING)
        if not self._handle_delivery(delivery_tag, bool(method.redelivered), properties, body):
            return self._reconnect()
        if self._status.stop_requested:
            return consumer
        if not self._verify_input_side():
            return self._reconnect()
        self._status.set_status(ExecutorStatus.WAITING_FOR_MESSAGE)
        return consumer

    def _reconnect(self) -> _Consumer:
        """Reconnect after the connection or the input side was lost, and consume again.

        The status is `UPSTREAM_RECOVER` until the new consumer is started.
        """
        self._status.set_status(ExecutorStatus.UPSTREAM_RECOVER)
        consumer = self._retry_rabbitmq_operation(
            self._start_consuming, "RabbitMQ consumer reconnection"
        )
        self._status.set_status(ExecutorStatus.WAITING_FOR_MESSAGE)
        return consumer

    def _start_consuming(self) -> _Consumer:
        """Start consuming, one message at a time, after ensuring a usable RabbitMQ connection."""
        self._ensure_connection_once()
        return self._current_session().consume(self._topology.source_queue)

    def _input_check_due(self) -> bool:
        """Return whether the input side is to be verified again while waiting for a message."""
        return time.monotonic() - self._input_checked_at >= self._settings.input_check_seconds

    def _verify_input_side(self) -> bool:
        """Declare the input side again, restoring whatever was deleted, before waiting.

        Returns False if the connection or channel was lost meanwhile; the caller must reconnect,
        which declares the input side again. RabbitMQ refusing a declaration is raised.
        """
        source_queue = self._topology.source_queue
        logger.debug("Verifying the input side of queue %s", source_queue)
        try:
            self._topology.declare_input_side(self.channel)
        except Exception as error:
            if not self._is_retryable_rabbitmq_error(error):
                logger.exception(
                    "Verifying the input side of queue %s failed; stopping the executor",
                    source_queue,
                )
                raise
            logger.warning(
                "Connection or channel lost while verifying the input side of queue %s, "
                "reconnecting: %s",
                source_queue,
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
        handling = _Handling.start(properties, body)
        settlement = _Settlement(
            session=self._current_session(),
            topology=self._topology,
            settings=self._settings,
            handling=handling,
            delivery_tag=delivery_tag,
            properties=properties,
            body=body,
            discard_connection=self._discard_connection,
        )
        try:
            return self._process_delivery(settlement, redelivered)
        finally:
            # Settled, or left to RabbitMQ to redeliver: no longer being processed
            outcome = _Outcome.ABANDONED if settlement.outcome is None else settlement.outcome
            delivery = self._status.finish_delivery(outcome)
            handling.log_handled(
                outcome,
                delivery,
                queue=self._topology.source_queue,
                delivery_tag=delivery_tag,
                redelivered=redelivered,
                properties=properties,
                accepted_types=self._accepted_types,
            )

    def _process_delivery(self, settlement: _Settlement, redelivered: bool) -> bool:
        """Do the work of `_handle_delivery()`, which counts how it ended.

        Executes the delivery, then settles it as the outcome demands; the exception that
        `execute()` raises decides the outcome of a failure.
        """
        try:
            publications = self._execute_delivery(settlement, redelivered)
        except MalformedMessageError as error:
            return self._reject_malformed(settlement, error)
        except TransientProcessingError as error:
            return self._retry_later(settlement, error)
        except Exception as error:
            self._dead_letter_after_bug(settlement, error)
            raise
        return self._publish_and_acknowledge(settlement, publications)

    def _execute_delivery(self, settlement: _Settlement, redelivered: bool) -> list[_Publication]:
        """Decode the delivery, execute it, and return its results, validated and serialized.

        Raises `MalformedMessageError` for a delivery or results that are invalid, and whatever
        `execute()` raised in its last attempt.
        """
        properties, body = settlement.properties, settlement.body
        message_class = self._accepted_class(_sent_type(properties))
        settlement.type_name = message_class.__name__
        if body is None:
            raise MalformedMessageError("Received a message with no body")
        message = self._decode(body, message_class)
        settlement.message_id = message.message_id
        self._status.set_delivery(
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
            message.message_id,
            self._topology.source_queue,
            settlement.delivery_tag,
        )
        publications = self._prepare_publications(self._execute_with_retries(message, settlement))
        settlement.handling.record_results([serialized for _, _, serialized in publications])
        return publications

    def _reject_malformed(self, settlement: _Settlement, error: MalformedMessageError) -> bool:
        """Dead-letter a delivery that can never succeed, as malformed; the executor continues."""
        logger.exception(
            "Message dead-lettered type=%s id=%s queue=%s delivery_tag=%i "
            "dead_letter_exchange=%s: %s",
            settlement.type_name,
            settlement.message_id,
            self._topology.source_queue,
            settlement.delivery_tag,
            self._topology.dead_letter_exchange,
            error,
        )
        return settlement.dead_letter(_DeadLetterReason.MALFORMED, error)

    def _retry_later(self, settlement: _Settlement, error: TransientProcessingError) -> bool:
        """Move a delivery still failing transiently to the retry queue, if it has rounds left.

        Dead-letters it instead, as `retries-exhausted`, once it was moved there
        `max_retry_rounds` times.
        """
        properties = settlement.properties
        headers = None if properties is None else properties.headers
        retry_round = _retry_round_of(headers)
        settings = self._settings
        if retry_round < settings.max_retry_rounds:
            return settlement.move_to_retry_queue(error)
        logger.error(
            "Message dead-lettered type=%s id=%s queue=%s delivery_tag=%i "
            "dead_letter_exchange=%s retry_round=%i retrying_since=%s: still failing "
            "after %d processing attempts, and max_retry_rounds=%i allows no more moves "
            "to the retry queue: %s",
            settlement.type_name,
            settlement.message_id,
            self._topology.source_queue,
            settlement.delivery_tag,
            self._topology.dead_letter_exchange,
            retry_round,
            _retrying_since_of(headers),
            settings.max_processing_attempts,
            settings.max_retry_rounds,
            error,
        )
        return settlement.dead_letter(_DeadLetterReason.RETRIES_EXHAUSTED, error)

    def _dead_letter_after_bug(self, settlement: _Settlement, error: Exception) -> None:
        """Dead-letter a delivery whose processing failed unexpectedly, before the error is raised.

        A failure to dead-letter it is logged, never raised: the executor stops either way, and
        the processing error is the one to raise.
        """
        logger.exception(
            "Message processing failed unexpectedly type=%s id=%s queue=%s delivery_tag=%i; "
            "dead-lettering it to %s and stopping the executor",
            settlement.type_name,
            settlement.message_id,
            self._topology.source_queue,
            settlement.delivery_tag,
            self._topology.dead_letter_exchange,
        )
        try:
            settlement.dead_letter(_DeadLetterReason.UNEXPECTED_ERROR, error)
        except Exception as dead_letter_error:
            logger.error(
                "Message id=%s could not be dead-lettered after the unexpected error, which "
                "is raised instead of %r",
                settlement.message_id,
                dead_letter_error,
            )

    def _publish_and_acknowledge(
        self, settlement: _Settlement, publications: list[_Publication]
    ) -> bool:
        """Publish the results of an executed delivery, if any, then acknowledge it.

        A stop request while its results are held requeues it instead, and a result too large
        for RabbitMQ dead-letters it. Returns False if the connection was lost, or RabbitMQ took
        the delivery back; other errors of publishing are raised.
        """
        try:
            if publications and not self._publish_results_holding_input(publications, settlement):
                logger.info(
                    "Stop requested while holding the results of message id=%s; requeueing "
                    "it for RabbitMQ to redeliver",
                    settlement.message_id,
                )
                return settlement.requeue()
        except Exception as error:
            if not (_is_result_too_large(error) or self._is_retryable_rabbitmq_error(error)):
                logger.exception(
                    "Results of message id=%s could not be published to exchange %s; "
                    "stopping the executor",
                    settlement.message_id,
                    self._topology.destination_exchange,
                )
                raise
            return self._results_not_published(settlement, error)
        logger.debug(
            "Acknowledging message id=%s delivery_tag=%i",
            settlement.message_id,
            settlement.delivery_tag,
        )
        if not settlement.acknowledge():
            return False
        logger.debug("Message acknowledged id=%s", settlement.message_id)
        return True

    def _results_not_published(self, settlement: _Settlement, error: Exception) -> bool:
        """Settle a delivery whose results RabbitMQ did not take, for a reason that is no bug.

        A result too large dead-letters the delivery: only its results are refused, and none of
        them was published. A lost connection, or a delivery RabbitMQ took back, returns False:
        RabbitMQ redelivers it, and discards the results unless it had already committed them.
        """
        if _is_result_too_large(error):
            logger.error(
                "Message dead-lettered type=%s id=%s queue=%s delivery_tag=%i "
                "dead_letter_exchange=%s: RabbitMQ refused one of its results as too "
                "large: %s",
                settlement.type_name,
                settlement.message_id,
                self._topology.source_queue,
                settlement.delivery_tag,
                self._topology.dead_letter_exchange,
                error,
            )
            return settlement.dead_letter(_DeadLetterReason.RESULT_TOO_LARGE, error)
        logger.warning(
            "Connection or channel lost while publishing results of message id=%s; "
            "RabbitMQ will redeliver the message, and discards the results unless it "
            "had already committed them: %s",
            settlement.message_id,
            error,
        )
        self._discard_connection()
        return False

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
        self, message: Accepted, settlement: _Settlement
    ) -> Result | Sequence[Result] | None:
        """Call execute(), retrying with backoff while it raises TransientProcessingError."""
        message_id = settlement.message_id
        max_attempts = self._settings.max_processing_attempts
        handling = settlement.handling
        for attempt in range(1, max_attempts + 1):
            logger.debug(
                "Processing message id=%s attempt %d/%d", message_id, attempt, max_attempts
            )
            self._status.set_attempt(attempt)
            try:
                with handling.timing(_TimedPart.EXECUTE):
                    return self.execute(message)
            except TransientProcessingError as error:
                if attempt == max_attempts:
                    raise
                delay_seconds = _capped_backoff_seconds(attempt - 1)
                logger.warning(
                    "Transient processing failure id=%s, retrying attempt %d/%d in %.1f "
                    "seconds: %s",
                    message_id,
                    attempt + 1,
                    max_attempts,
                    delay_seconds,
                    error,
                )
                with handling.timing(_TimedPart.BACKOFF):
                    time.sleep(delay_seconds)
        # Unreachable: the last attempt returns or raises
        raise RuntimeError("Processing retry loop exited unexpectedly")  # pragma: no cover

    def _prepare_publications(self, results: object) -> list[_Publication]:
        """Validate and serialize every result before anything is published or acknowledged.

        A terminator returning results raises `RuntimeError`, and a result of another class
        than the declared result type `TypeError`: the executor's code contradicts its own
        declaration. A returned value that is not a message, or a result that cannot be
        serialized, raises `MalformedMessageError` instead: it can depend on the data the message
        brought back from outside, and the input's results can never be published.
        """
        destination_exchange = self._topology.destination_exchange
        terminator_error = (
            "execute() returned results, but the executor has no destination_exchange"
        )
        try:
            messages = _as_messages(results)
        except TypeError as error:
            if destination_exchange is None:
                raise RuntimeError(terminator_error) from error
            raise MalformedMessageError(f"execute() returned an invalid result: {error}") from error
        if not messages:
            return []
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
        self, publications: list[_Publication], settlement: _Settlement
    ) -> bool:
        """Publish the results, holding the input and its results while they are unroutable.

        Returns True once the results are committed, and False if `stop()` is called while they
        are held; the caller then requeues the input. Between attempts the connection keeps
        answering heartbeats, and `execute()` is not called again. Errors of `_publish_results()`
        other than `UnroutableError` are raised, as is `ChannelWrongStateError` if RabbitMQ
        takes the input back during the hold, closing its channel or cancelling the consumer,
        e.g. on `consumer_timeout`.
        """
        handling = settlement.handling
        attempt = 0
        while True:
            try:
                with handling.timing(_TimedPart.PUBLISH):
                    self._publish_results(publications, settlement.session)
            except UnroutableError as error:
                delay_seconds = _capped_backoff_seconds(attempt) * random.random()
                attempt += 1
                self._status.set_status(ExecutorStatus.DOWNSTREAM_RECOVER)
                logger.error(
                    "RabbitMQ returned %d result(s) of message id=%s as unroutable: no queue is "
                    "bound to exchange %s; holding the message and retrying in %.1f seconds",
                    len(error.messages),
                    settlement.message_id,
                    self._topology.destination_exchange,
                    delay_seconds,
                )
                with handling.timing(_TimedPart.HELD):
                    held = self._hold_delivery(delay_seconds, settlement.session)
                if not held:
                    return False
            else:
                self._status.set_status(ExecutorStatus.SENDING_RESULT)
                return True

    def _hold_delivery(self, seconds: float, session: _ConsumingSession) -> bool:
        """Wait for `seconds` while holding an unacknowledged delivery, answering heartbeats.

        Waits in steps of at most one second. Returns False if `stop()` was called. Raises
        `ChannelWrongStateError` if RabbitMQ took the delivery back, closing its channel or
        cancelling the consumer, which requeues it, and connection errors if the connection is
        lost.
        """
        remaining = seconds
        while True:
            if self._status.stop_requested:
                return False
            session.check_delivery_held()
            if remaining <= 0:
                return True
            step = min(remaining, _STOP_POLL_SECONDS)
            session.connection.sleep(step)
            remaining -= step

    def _publish_results(
        self, publications: list[_Publication], session: _ConsumingSession
    ) -> None:
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
        channel = session.begin_results()
        logger.debug(
            "Verifying the output side of exchange %s", self._topology.destination_exchange
        )
        self._topology.declare_output_side(channel)
        if self._status.status is not ExecutorStatus.DOWNSTREAM_RECOVER:
            # Held results keep that status until they are committed
            self._status.set_status(ExecutorStatus.SENDING_RESULT)
        for exchange_name, result, body in publications:
            self._basic_publish(channel, exchange_name, result, body)
        logger.debug("Committing the transaction of %d results", len(publications))
        session.commit_results(channel)
        for exchange_name, result, _body in publications:
            self._log_published(exchange_name, result)
