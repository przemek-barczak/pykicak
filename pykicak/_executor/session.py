"""The consumer of one connection, and the channel on which it publishes the results."""

from __future__ import annotations

from collections.abc import Iterator

import pika
from pika.adapters.blocking_connection import BlockingChannel, ReturnedMessage
from pika.exceptions import ChannelClosedByBroker, ChannelWrongStateError, UnroutableError
from pika.spec import Basic

from pykicak._executor import logger
from pykicak._executor.settings import _STOP_POLL_SECONDS
from pykicak.abstracts import KicakAbstract

_PREFETCH_COUNT = 1
_PRECONDITION_FAILED = 406

type _Delivered = tuple[Basic.Deliver | None, pika.BasicProperties | None, bytes | None]
"""What pika's consume() yields: a delivery, or three Nones when it waited in vain."""

type _Consumer = Iterator[_Delivered]


def _is_consumer_timeout(error: Exception) -> bool:
    """Return whether RabbitMQ closed a channel because a delivery outlasted `consumer_timeout`.

    The broker's `consumer_timeout` bounds how long a delivery may stay unacknowledged. RabbitMQ
    4.2 reports its expiry as `406 PRECONDITION_FAILED` with a reply text such as "delivery
    acknowledgement on channel 1 timed out", and requeues the delivery. RabbitMQ 4.3 and newer
    cancel the consumer instead (see `_ConsumingSession.consumer_cancelled()`).
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


def _is_retryable_error(error: Exception) -> bool:
    """Return whether an error of an Executor is likely to clear after reconnecting.

    Besides the transient errors of every agent, this includes RabbitMQ closing the consuming
    channel because a delivery outlasted its `consumer_timeout`: as when the connection is lost,
    RabbitMQ requeues the delivery, and the executor reconnects and receives it again.
    """
    return KicakAbstract._is_retryable_rabbitmq_error(error) or _is_consumer_timeout(error)


class _ConsumingSession:
    """The consumer of one connection: the channel that delivers, and the one for results.

    A delivery can be acknowledged or rejected only on the channel that delivered it, so every
    connection gets a session of its own, and everything noted about a consumer or its results
    ends with the connection. `user_id` is the user the connection authenticated as, which
    RabbitMQ requires as the `user_id` of every message published on it.
    """

    def __init__(
        self, connection: pika.BlockingConnection, channel: BlockingChannel, user_id: str | None
    ) -> None:
        self.connection = connection
        self.channel = channel
        """The channel that delivers, in confirm mode: copies are published on it too."""

        self.user_id = user_id
        self._consumer_cancelled = False
        # Transactional channel for results, opened on first use
        self._results_channel: BlockingChannel | None = None
        self._returned_results: list[ReturnedMessage] = []

    def consume(self, queue: str) -> _Consumer:
        """Start consuming `queue`, one message at a time, waking up at least once a second."""
        logger.debug("Setting RabbitMQ prefetch count to %d", _PREFETCH_COUNT)
        self.channel.basic_qos(prefetch_count=_PREFETCH_COUNT)
        self._watch_for_consumer_cancellation(queue)
        logger.debug("Consuming from RabbitMQ queue=%s", queue)
        return iter(self.channel.consume(queue, inactivity_timeout=_STOP_POLL_SECONDS))

    def _watch_for_consumer_cancellation(self, queue: str) -> None:
        """Note RabbitMQ cancelling the consumer as soon as pika processes it.

        RabbitMQ 4.3 and newer cancel the consumer of a delivery that outlasts a quorum queue's
        consumer timeout, and return the delivery to the queue; `consumer_cancelled()` reads the
        note. pika's consume() generator learns of a cancellation only when it is advanced, and
        the public `BlockingChannel.add_on_cancel_callback()` never fires for it (pika 1.4 passes
        the generator's events around those callbacks), so the callback is registered on the
        channel that BlockingChannel wraps, its private `_impl`. This is the only place that uses
        a private pika attribute. Without it, a timed-out delivery is still executed again, but
        its results or its retry copy may be published for it too, and so twice.
        """
        register = getattr(getattr(self.channel, "_impl", None), "add_on_cancel_callback", None)
        if not callable(register):
            logger.warning(
                "Cannot watch for RabbitMQ cancelling the consumer of queue %s: this pika "
                "version's BlockingChannel has no _impl.add_on_cancel_callback(). A delivery that "
                "outlasts a quorum queue's consumer timeout on RabbitMQ 4.3 or newer is still "
                "executed again, but its results or retry copy may then be published twice",
                queue,
            )
            return
        register(lambda _method_frame: self._note_consumer_cancelled())

    def _note_consumer_cancelled(self) -> None:
        """Note that RabbitMQ cancelled the consumer; see `consumer_cancelled()`."""
        self._consumer_cancelled = True

    def consumer_cancelled(self) -> bool:
        """Return whether RabbitMQ cancelled the consumer while keeping its channel open.

        RabbitMQ 4.3 and newer do so when a delivery outlasts the consumer timeout, which they
        evaluate for quorum queues only: they return the delivery to the queue and ignore a later
        acknowledgement (RabbitMQ 4.2 closed the channel instead). Deleting the queue cancels the
        consumer too. Events RabbitMQ has already sent, such as a cancellation while `execute()`
        ran, are processed first.
        """
        self.connection.process_data_events(time_limit=0)
        return bool(self.channel.is_open and self._consumer_cancelled)

    def check_delivery_held(self) -> None:
        """Raise `ChannelWrongStateError` if RabbitMQ took back the delivery being processed.

        RabbitMQ takes it back, and redelivers it, when it outlasts the consumer timeout: RabbitMQ
        4.2 closes the channel that delivered it, while 4.3 and newer cancel the consumer. Nothing
        may then be published for the delivery, neither its results nor its copy for the retry
        queue or the dead-letter queue, because the redelivery decides its outcome again.
        """
        if self.consumer_cancelled():
            raise ChannelWrongStateError(
                "RabbitMQ cancelled the consumer of the message, which returns it to the queue"
            )
        if not self.channel.is_open:
            raise ChannelWrongStateError("RabbitMQ closed the channel that delivered the message")

    def begin_results(self) -> BlockingChannel:
        """Return the transactional channel for the results of one input, opening it if needed.

        Results need a channel of their own: a channel in transaction mode cannot also be in
        confirm mode, and the input's acknowledgement must stay outside the transaction. RabbitMQ
        routes transactional messages only at the commit, so an acknowledgement committed with
        results that turn out to be unroutable would remove the input while its results are
        dropped, losing the message. RabbitMQ closes the channel when it refuses a result, e.g.
        one that is too large, so a closed channel is opened again.
        """
        channel = self._results_channel
        if channel is None or not channel.is_open:
            logger.debug("Opening the transactional channel for results")
            channel = self.connection.channel()
            channel.tx_select()
            channel.add_on_return_callback(self._on_result_returned)
            self._results_channel = channel
        self._returned_results.clear()
        return channel

    def commit_results(self, channel: BlockingChannel) -> None:
        """Commit the results published on `channel` since `begin_results()`.

        Raises `ChannelWrongStateError` instead of committing if RabbitMQ has taken the input
        back, closing its channel or cancelling the consumer, which requeues it: results committed
        for it now would be published again when the redelivery is executed. RabbitMQ routes the
        results only at the commit, so a result that no queue is bound to is reported only after
        it: this then raises `UnroutableError` with the returned results.
        """
        self.check_delivery_held()
        channel.tx_commit()
        # RabbitMQ sends returned messages before the commit's reply, but pika hands them to the
        # return callback only while it processes connection events
        self.connection.process_data_events(time_limit=0)
        if self._returned_results:
            raise UnroutableError(list(self._returned_results))

    def _on_result_returned(
        self,
        _channel: BlockingChannel,
        method: Basic.Return,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        """Collect a result that RabbitMQ returned because no queue is bound to its exchange."""
        self._returned_results.append(ReturnedMessage(method, properties, body))
