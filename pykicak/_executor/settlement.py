"""Settling a delivery: acknowledging it, requeueing it, dead-lettering it, or retrying it later."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pika
from pika.exceptions import NackError, UnroutableError

from pykicak._executor import logger
from pykicak._executor.headers import (
    _RETRY_ROUND_HEADER,
    _RETRYING_SINCE_HEADER,
    _copy_properties,
    _dead_letter_headers,
    _retry_headers,
    _sent_message_id,
)
from pykicak._executor.session import _ConsumingSession, _is_retryable_error
from pykicak._executor.settings import _ExecutorSettings
from pykicak._executor.statistics import _Handling
from pykicak._executor.status import _DeadLetterReason, _Outcome
from pykicak._executor.topology import _ExecutorTopology
from pykicak.abstracts import _now


class _Settlement:
    """How one delivery is settled, on the channel of the session that delivered it.

    It is acknowledged, requeued, dead-lettered, or moved to the retry queue. Where a copy is
    published first, the delivery is settled only after RabbitMQ has confirmed the copy, so a
    failure in between repeats the message instead of losing it. Each method returns False if the
    connection was lost, or RabbitMQ took the delivery back because it outlasted
    `consumer_timeout` (closing its channel, or cancelling the consumer): RabbitMQ redelivers it,
    the connection is then discarded with `discard_connection`, and the caller must reconnect.
    Other errors are raised. `outcome` records the settlement once RabbitMQ has taken it.
    """

    def __init__(
        self,
        *,
        session: _ConsumingSession,
        topology: _ExecutorTopology,
        settings: _ExecutorSettings,
        handling: _Handling,
        delivery_tag: int,
        properties: pika.BasicProperties | None,
        body: bytes | None,
        discard_connection: Callable[[], None],
    ) -> None:
        self.session = session
        """The consumer that delivered it: only its channel can settle the delivery."""

        self.handling = handling
        """How its handling goes, for its `Message handled` line."""

        self._topology = topology
        self._settings = settings
        self._discard_connection = discard_connection
        self.delivery_tag = delivery_tag
        self.properties = properties
        self.body = body
        self.message_id = _sent_message_id(properties)
        """How the logs name the delivery: its sender's `message_id` property until the body is
        decoded, then the body's own."""

        self.type_name: str | None = None
        """The accepted class the delivery's `type` property names, once it is known."""

        self.outcome: _Outcome | _DeadLetterReason | None = None
        """How the delivery was settled, or None while it is not (yet) settled."""

    def acknowledge(self) -> bool:
        """Acknowledge the delivery: it was executed, and its results, if any, committed."""
        return self._settle(_Outcome.COMPLETED)

    def requeue(self) -> bool:
        """Return the delivery to the queue, for RabbitMQ to deliver it again."""
        return self._settle(_Outcome.REQUEUED)

    def move_to_retry_queue(self, error: Exception) -> bool:
        """Move a delivery that failed transiently, with `error`, to the retry queue; acknowledge it.

        The copy keeps the body and the application's properties and headers, gets the next
        retry round and the time of the move (`x-kicak-published-at`) in its headers and this
        executor's user as its `user_id`, keeps the user who created the message in
        `x-kicak-original-publisher`, and expires in the retry queue after
        `retry_delay_seconds`, which returns it to the back of the source queue. The delivery is
        acknowledged only after RabbitMQ has confirmed the copy, so a failure in between executes
        the message twice instead of losing it. If RabbitMQ does not take the copy, returning it
        as unroutable or refusing it, the delivery is requeued instead. RabbitMQ refusing to
        declare the retry queue is raised.
        """
        self.handling.start_settling()
        # Only execute() fails transiently, so the body was decoded, and message_id is the body's
        body, message_id = cast(bytes, self.body), cast(str, self.message_id)
        retry_queue = self._topology.retry_queue
        original = self.properties if self.properties is not None else pika.BasicProperties()
        headers = _retry_headers(original.headers, _now())
        copy_properties = _copy_properties(
            original,
            headers,
            user_id=self.session.user_id,
            message_id=message_id,
            expiration=str(self._settings.retry_delay_milliseconds),
        )
        channel = self.session.channel
        try:
            self._topology.declare_retry_queue(channel)
            self.session.check_delivery_held()
            logger.debug(
                "Moving message id=%s to retry queue %s round=%s",
                message_id,
                retry_queue,
                headers[_RETRY_ROUND_HEADER],
            )
            channel.basic_publish(
                exchange="",  # the default exchange routes by queue name
                routing_key=retry_queue,
                body=body,
                properties=copy_properties,
                mandatory=True,
            )
        except (UnroutableError, NackError) as move_error:
            logger.error(
                "Message id=%s could not be moved to retry queue %s; requeueing it instead: %r",
                message_id,
                retry_queue,
                move_error,
            )
            return self.requeue()
        except Exception as move_error:
            if not _is_retryable_error(move_error):
                logger.exception(
                    "Moving message id=%s to retry queue %s failed; stopping the executor",
                    message_id,
                    retry_queue,
                )
                raise
            logger.warning(
                "Connection or channel lost while moving message id=%s to retry queue %s; "
                "RabbitMQ will redeliver it: %s",
                message_id,
                retry_queue,
                move_error,
            )
            self._discard_connection()
            return False
        settings = self._settings
        logger.warning(
            "Message moved to retry queue after %d failed processing attempts id=%s queue=%s "
            "retry_queue=%s retry_round=%s max_retry_rounds=%i retrying_since=%s retry_in=%.1fs: "
            "%s",
            settings.max_processing_attempts,
            message_id,
            self._topology.source_queue,
            retry_queue,
            headers[_RETRY_ROUND_HEADER],
            settings.max_retry_rounds,
            headers[_RETRYING_SINCE_HEADER],
            settings.retry_delay_seconds,
            error,
        )
        return self._settle(_Outcome.MOVED_TO_RETRY)

    def dead_letter(self, reason: _DeadLetterReason, error: BaseException) -> bool:
        """Dead-letter the delivery: publish a copy that says why, then acknowledge the delivery.

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
        is noticed instead, and the delivery is requeued. RabbitMQ refusing to declare the
        dead-letter route is raised.
        """
        self.handling.start_settling()
        message_id = self.message_id
        dead_letter_exchange = self._topology.dead_letter_exchange
        original = self.properties if self.properties is not None else pika.BasicProperties()
        headers = _dead_letter_headers(
            original.headers, reason, error, self._topology.source_queue, _now()
        )
        copy_properties = _copy_properties(
            original,
            headers,
            user_id=self.session.user_id,
            message_id=original.message_id,
            expiration=None,
        )
        channel = self.session.channel
        try:
            self._topology.declare_dead_letter_route(channel)
            self.session.check_delivery_held()
            logger.debug(
                "Dead-lettering message id=%s to %s reason=%s",
                message_id,
                dead_letter_exchange,
                reason,
            )
            channel.basic_publish(
                exchange=dead_letter_exchange,
                routing_key="",  # a fanout exchange ignores it
                body=b"" if self.body is None else self.body,
                properties=copy_properties,
                mandatory=True,
            )
        except (UnroutableError, NackError) as copy_error:
            logger.error(
                "Message id=%s could not be dead-lettered to %s; requeueing it instead: %r",
                message_id,
                dead_letter_exchange,
                copy_error,
            )
            return self.requeue()
        except Exception as copy_error:
            if not _is_retryable_error(copy_error):
                logger.exception(
                    "Dead-lettering message id=%s to %s failed; stopping the executor",
                    message_id,
                    dead_letter_exchange,
                )
                raise
            logger.warning(
                "Connection or channel lost while dead-lettering message id=%s to %s; RabbitMQ "
                "will redeliver it: %s",
                message_id,
                dead_letter_exchange,
                copy_error,
            )
            self._discard_connection()
            return False
        return self._settle(reason)

    def _settle(self, outcome: _Outcome | _DeadLetterReason) -> bool:
        """Settle the delivery as `outcome`, and record it once RabbitMQ has taken it.

        `_Outcome.REQUEUED` returns the delivery to the queue with `basic_nack()`, after declaring
        the dead-letter route again: a quorum queue can dead-letter a requeued message at its
        delivery limit (RabbitMQ 4.2 counts every requeue toward it, 4.3+ only failed deliveries),
        and RabbitMQ drops a message dead-lettered into a missing exchange. Every other outcome acknowledges it, once it is done with: completed, moved to
        the retry queue, or dead-lettered for the given reason.

        Returns False if the connection was lost, or RabbitMQ took the delivery back because it
        outlasted `consumer_timeout`, closing the channel (RabbitMQ 4.2) or cancelling the
        consumer (4.3 and newer, which ignore the settlement): RabbitMQ then requeues the delivery
        itself, so the settlement is not retried, nor recorded.
        """
        self.handling.start_settling()
        requeue = outcome is _Outcome.REQUEUED
        if requeue:
            action = "requeueing"
        elif isinstance(outcome, _DeadLetterReason):
            action = "dead-lettering"
        else:
            action = "acknowledging"
        channel = self.session.channel
        try:
            if requeue:
                self._topology.declare_dead_letter_route(channel)
                channel.basic_nack(delivery_tag=self.delivery_tag, requeue=True)
            else:
                channel.basic_ack(self.delivery_tag)
            cancelled = self.session.consumer_cancelled()
        except Exception as error:
            if not _is_retryable_error(error):
                raise
            logger.warning(
                "Connection or channel lost while %s message id=%s delivery_tag=%i; RabbitMQ "
                "will redeliver it: %s",
                action,
                self.message_id,
                self.delivery_tag,
                error,
            )
            self._discard_connection()
            return False
        if cancelled:
            logger.warning(
                "RabbitMQ cancelled the consumer of queue %s while %s message id=%s "
                "delivery_tag=%i, e.g. because the message outlasted the queue's consumer "
                "timeout; it returns the message to the queue and redelivers it; reconnecting",
                self._topology.source_queue,
                action,
                self.message_id,
                self.delivery_tag,
            )
            self._discard_connection()
            return False
        self.outcome = outcome
        return True
