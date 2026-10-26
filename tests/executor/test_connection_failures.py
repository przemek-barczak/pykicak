"""Lost connections, consumer timeouts, and cancelled consumers."""

from unittest import mock

import pika
import pika.channel
import pytest
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    StreamLostError,
)

from pykicak._executor import logger as executor_logger
from pykicak.executor import (
    DeadLetterCounts,
    ExecutorCounts,
    ExecutorStatus,
    MalformedMessageError,
)
from tests.executor.helpers import (
    BindFailingChannel,
    FailingResultsChannel,
    SampleMessage,
    SettleFailingChannel,
    UnroutableResultsChannel,
    always_transient,
    connection_lost,
    dead_letter_copies,
    dead_lettered,
    handled_records,
    make_executor,
    reconnecting_executor,
    run_until_stopped,
)
from tests.fakes import (
    BoundQueue,
    FakeChannel,
    FakeConnection,
    kicak_values,
)


def consumer_timeout() -> ChannelClosedByBroker:
    """The error pika raised on RabbitMQ 4.2 when a delivery outlasted its consumer_timeout."""
    return ChannelClosedByBroker(
        406,
        "PRECONDITION_FAILED - delivery acknowledgement on channel 1 timed out. Timeout value "
        "used: 1800000 ms. This timeout value can be configured, see consumers doc guide to "
        "learn more",
    )


LOST_DELIVERY_ERRORS = [
    pytest.param(connection_lost, id="connection-lost"),
    pytest.param(consumer_timeout, id="consumer-timeout"),
]
"""Errors after which RabbitMQ has requeued the delivery and the executor reconnects."""


class OversizedResultChannel(FakeChannel):
    """Results channel whose commit RabbitMQ refuses: a result exceeds its max_message_size.

    Like RabbitMQ, it discards the transaction and closes only this channel.
    """

    def tx_commit(self) -> None:
        self._uncommitted = []
        self.is_open = False
        raise ChannelClosedByBroker(
            406,
            "PRECONDITION_FAILED - message size 17825792 is larger than configured max size "
            "16777216",
        )


def record_backoff(delays: list[float]):
    """Stand-in for _sleep_unless_stopped() that records each backoff delay without sleeping."""

    def sleep_unless_stopped(seconds: float) -> bool:
        delays.append(seconds)
        return True

    return sleep_unless_stopped


def test_pika_blocking_channel_still_wraps_the_channel_the_cancel_hook_needs():
    # _watch_for_consumer_cancellation() registers on the private `_impl` of BlockingChannel,
    # because the public add_on_cancel_callback() never fires for a consume() generator
    impl = mock.MagicMock()

    channel = BlockingChannel(impl, mock.MagicMock())

    assert channel._impl is impl
    assert callable(pika.channel.Channel.add_on_cancel_callback)


def test_run_warns_and_still_consumes_when_pika_offers_no_cancel_hook(config, monkeypatch, caplog):
    channel = FakeChannel()
    monkeypatch.setattr(channel, "_impl", object())  # a pika whose BlockingChannel lacks the hook
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel)

    run_until_stopped(executor)

    assert channel.acked_delivery_tags == [1]
    [warning] = [r for r in caplog.records if "Cannot watch for RabbitMQ cancelling" in r.message]
    assert warning.levelname == "WARNING"
    assert "may then be published twice" in warning.getMessage()


def test_run_declares_the_queue_again_when_rabbitmq_cancels_the_consumer(
    config, monkeypatch, caplog
):
    class CancellingChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            # pika ends the consume() generator when RabbitMQ cancels the consumer
            return iter(())

    cancelled_channel = CancellingChannel()
    replacement_channel = FakeChannel()
    replacement_channel.queue_deliveries(
        SampleMessage(message_id="after", text="after the deletion").to_bytes()
    )
    executor = make_executor(config, cancelled_channel)
    channels = [cancelled_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert cancelled_channel.is_open is False
    declared_on_reconnection = replacement_channel.declared_queues[:2]
    assert [q.queue for q in declared_on_reconnection] == ["input-queue", "input-queue.dlq"]
    assert BoundQueue("input-queue", "input-exchange") in replacement_channel.bound_queues
    assert executor.processed_messages == [
        SampleMessage(message_id="after", text="after the deletion")
    ]
    assert replacement_channel.acked_delivery_tags == [1]
    cancel_records = [r for r in caplog.records if "cancelled the consumer" in r.message]
    assert [record.levelname for record in cancel_records] == ["ERROR"]


@pytest.mark.parametrize(
    ("error", "retryable"),
    [
        (connection_lost(), True),
        (consumer_timeout(), True),
        (ChannelClosedByBroker(406, "PRECONDITION_FAILED - unknown delivery tag 1"), False),
        (ChannelClosedByBroker(404, "NOT_FOUND - no queue 'input-queue' in vhost '/'"), False),
        (ChannelClosedByBroker(403, "ACCESS_REFUSED - access to queue refused"), False),
    ],
    ids=["connection-lost", "consumer-timeout", "other-406", "not-found", "access-refused"],
)
def test_consumer_timeout_is_retryable_unlike_other_channel_errors(config, error, retryable):
    executor = make_executor(config, FakeChannel())

    assert executor._is_retryable_rabbitmq_error(error) is retryable


@pytest.mark.parametrize("make_error", LOST_DELIVERY_ERRORS)
def test_run_reconnects_after_consumer_connection_loss(config, monkeypatch, caplog, make_error):
    class DisconnectingChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            def disconnected():
                raise make_error()
                yield None, None, None

            return disconnected()

    disconnected_channel = DisconnectingChannel()
    recovered_channel = FakeChannel()
    recovered_channel.queue_deliveries(
        SampleMessage(message_id="recovered", text="recovered").to_bytes()
    )
    channels = [disconnected_channel, recovered_channel]
    executor = make_executor(config, disconnected_channel)
    monkeypatch.setattr("pykicak.executor.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        executor,
        "_create_connection",
        lambda: FakeConnection(channels.pop(0)),
    )

    run_until_stopped(executor)

    assert executor.processed_messages == [SampleMessage(message_id="recovered", text="recovered")]
    assert recovered_channel.acked_delivery_tags == [1]
    assert recovered_channel.prefetch_count_when_consuming == 1
    assert "Connection or channel lost, reconnecting" in caplog.text


def test_run_raises_when_rabbitmq_refuses_the_consumer(config):
    class RefusingChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            raise ChannelClosedByBroker(404, "NOT_FOUND - no queue 'input-queue' in vhost '/'")
            yield None, None, None

    channel = RefusingChannel()
    executor = make_executor(config, channel)

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED


@pytest.mark.parametrize("make_error", LOST_DELIVERY_ERRORS)
def test_run_reconnects_when_the_input_side_cannot_be_verified(
    config, monkeypatch, caplog, make_error
):
    # Calls 1 and 2 bind on connecting; call 3 is the verification after the first message
    original_channel = BindFailingChannel(3, make_error())
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(SampleMessage(message_id="first", text="first").to_bytes())
    replacement_channel.queue_deliveries(
        SampleMessage(message_id="second", text="second").to_bytes()
    )
    channels = [original_channel, replacement_channel]
    executor = make_executor(config, original_channel)
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == [1]
    assert replacement_channel.acked_delivery_tags == [1]
    assert [m.message_id for m in executor.processed_messages] == ["first", "second"]
    warnings = [
        r for r in caplog.records if "lost while verifying the input side of queue" in r.message
    ]
    assert [record.levelname for record in warnings] == ["WARNING"]


@pytest.mark.parametrize("make_error", LOST_DELIVERY_ERRORS)
def test_run_reconnects_when_the_input_side_cannot_be_verified_while_waiting(
    config, monkeypatch, caplog, make_error
):
    # Calls 1 and 2 bind on connecting; call 3 is the verification at the first idle wakeup
    idle_channel = BindFailingChannel(3, make_error())
    idle_channel.on_idle = lambda: None
    replacement_channel = FakeChannel()
    replacement_channel.queue_deliveries(SampleMessage(message_id="after", text="after").to_bytes())
    channels = [idle_channel, replacement_channel]
    executor = make_executor(config, idle_channel, input_check_seconds=0)
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))
    statuses: list[ExecutorStatus] = []
    replacement_channel.before_delivery = lambda _tag: statuses.append(executor.get_info().status)

    run_until_stopped(executor)

    assert channels == []  # reconnected once, on the replacement channel
    assert idle_channel.bound_queues.count(BoundQueue("input-queue", "input-exchange")) == 1
    assert [m.message_id for m in executor.processed_messages] == ["after"]
    assert replacement_channel.acked_delivery_tags == [1]
    assert statuses == [ExecutorStatus.WAITING_FOR_MESSAGE]
    warnings = [
        r for r in caplog.records if "lost while verifying the input side of queue" in r.message
    ]
    assert [record.levelname for record in warnings] == ["WARNING"]
    recovering = [r for r in caplog.records if "is recovering: UPSTREAM_RECOVER" in r.message]
    assert len(recovering) == 1


@pytest.mark.parametrize("step", ["publish 1", "publish 2", "commit"])
def test_run_discards_uncommitted_results_and_reconnects_when_connection_is_lost(
    config, monkeypatch, sleeps, caplog, step
):
    results = [
        SampleMessage(message_id="chunk-1", text="first"),
        SampleMessage(message_id="chunk-2", text="second"),
    ]
    message = SampleMessage(message_id="input", text="input")
    original_channel = FakeChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(message.to_bytes())
    replacement_channel.queue_deliveries(message.to_bytes())  # RabbitMQ's redelivery
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [
            (original_channel, FailingResultsChannel(step, StreamLostError("connection lost"))),
            (replacement_channel, None),
        ],
        lambda _message: results,
    )

    run_until_stopped(executor)

    # No result of the interrupted transaction is delivered, so none is published twice
    assert original_channel.published_messages == []
    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    assert executor.processed_messages == [message, message]
    assert [p.body for p in replacement_channel.published_messages] == [
        result.to_bytes() for result in results
    ]
    assert replacement_channel.acked_delivery_tags == [1]
    assert replacement_channel.prefetch_count_when_consuming == 1
    assert sleeps == []
    warnings = [
        r for r in caplog.records if "Connection or channel lost while publishing" in r.message
    ]
    assert [record.levelname for record in warnings] == ["WARNING"]


def test_run_does_not_commit_results_when_rabbitmq_closed_the_delivery_channel(
    config, monkeypatch, sleeps, caplog
):
    message = SampleMessage(message_id="input", text="input")
    original_channel = FakeChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(message.to_bytes())
    replacement_channel.queue_deliveries(message.to_bytes())  # RabbitMQ's redelivery

    class ClosingResultsChannel(FakeChannel):
        """Processes, as pika does while publishing, RabbitMQ's close of the consuming channel."""

        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            original_channel.is_open = False  # e.g. its consumer_timeout expired in execute()
            super().basic_publish(exchange, routing_key, body, properties, mandatory)

    executor = reconnecting_executor(
        config,
        monkeypatch,
        [(original_channel, ClosingResultsChannel()), (replacement_channel, None)],
        lambda received: received,
    )

    run_until_stopped(executor)

    assert "commit" not in original_channel.operations
    assert original_channel.published_messages == []
    assert original_channel.acked_delivery_tags == []
    assert executor.processed_messages == [message, message]
    assert [p.body for p in replacement_channel.published_messages] == [message.to_bytes()]
    assert replacement_channel.acked_delivery_tags == [1]
    warnings = [
        r for r in caplog.records if "Connection or channel lost while publishing" in r.message
    ]
    assert [record.levelname for record in warnings] == ["WARNING"]


def test_run_does_not_commit_results_when_rabbitmq_cancelled_the_consumer(
    config, monkeypatch, sleeps, caplog
):
    # RabbitMQ 4.3+ returns a delivery that outlasted a quorum queue's consumer timeout to the
    # queue and cancels the consumer, keeping the channel open
    message = SampleMessage(message_id="input", text="input")
    original_channel = FakeChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(message.to_bytes())
    replacement_channel.queue_deliveries(message.to_bytes())  # RabbitMQ's redelivery
    executions = 0

    def outlasting_the_consumer_timeout(received):
        nonlocal executions
        executions += 1
        if executions == 1:
            original_channel.cancel_consumer()
        return received

    executor = reconnecting_executor(
        config,
        monkeypatch,
        [(original_channel, None), (replacement_channel, None)],
        outlasting_the_consumer_timeout,
    )

    run_until_stopped(executor)

    assert "commit" not in original_channel.operations
    assert original_channel.published_messages == []
    assert original_channel.acked_delivery_tags == []
    assert executor.processed_messages == [message, message]
    assert [p.body for p in replacement_channel.published_messages] == [message.to_bytes()]
    assert replacement_channel.acked_delivery_tags == [1]
    [warning] = [
        r for r in caplog.records if "Connection or channel lost while publishing" in r.message
    ]
    assert warning.levelname == "WARNING"
    assert "cancelled the consumer" in warning.getMessage()


@pytest.mark.parametrize("outcome", ["acknowledging", "dead-lettering"])
def test_run_reconnects_when_rabbitmq_cancelled_the_consumer_of_a_settled_delivery(
    config, monkeypatch, caplog, outcome
):
    # A terminator's settlement reaches RabbitMQ 4.3+ after it has taken the delivery back, and
    # is ignored: the executor reconnects and executes the redelivery, warning, not erring
    class CancellingAfterTheCopyChannel(FakeChannel):
        """RabbitMQ cancels the consumer right after confirming a dead-letter copy."""

        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            super().basic_publish(exchange, routing_key, body, properties, mandatory)
            if exchange == "input-queue.dlx":
                self.cancel_consumer()

    message = SampleMessage(message_id="input", text="input")
    original_channel = CancellingAfterTheCopyChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(message.to_bytes())
    replacement_channel.queue_deliveries(message.to_bytes())  # RabbitMQ's redelivery

    executed = False

    def outlasting_the_consumer_timeout(_received):
        nonlocal executed
        if executed:
            return None  # the redelivery
        executed = True
        if outcome == "dead-lettering":
            raise MalformedMessageError("rejected by the application")
        original_channel.cancel_consumer()
        return None

    executor = make_executor(config, original_channel, handler=outlasting_the_consumer_timeout)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert executor.processed_messages == [message, message]
    assert original_channel.is_open is False  # the connection was discarded
    assert replacement_channel.acked_delivery_tags == [1]
    if outcome == "dead-lettering":
        # The copy stays in the dead-letter queue although the redelivery succeeded
        assert len(dead_letter_copies(original_channel)) == 1
    [warning] = [r for r in caplog.records if "cancelled the consumer" in r.message]
    assert warning.levelname == "WARNING"
    assert f"while {outcome} message id=input" in warning.getMessage()


def test_run_publishes_no_dead_letter_copy_when_rabbitmq_cancelled_the_consumer(
    config, monkeypatch, caplog
):
    # execute() outlasted a quorum queue's consumer timeout on RabbitMQ 4.3+, which has returned
    # the message to the queue: the redelivery decides its fate, not this delivery
    message = SampleMessage(message_id="input", text="input")
    original_channel = FakeChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(message.to_bytes())
    replacement_channel.queue_deliveries(message.to_bytes())  # RabbitMQ's redelivery

    def reject_beyond_the_consumer_timeout(_received):
        original_channel.cancel_consumer()
        raise MalformedMessageError("rejected by the application")

    executor = make_executor(config, original_channel, handler=reject_beyond_the_consumer_timeout)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert dead_letter_copies(original_channel) == []
    assert original_channel.acked_delivery_tags == []
    assert dead_lettered(replacement_channel) == [1]
    assert "Connection or channel lost while dead-lettering message id=input" in caplog.text


@pytest.mark.parametrize(
    ("step", "error"),
    [
        ("publish 1", ChannelClosedByBroker(403, "ACCESS_REFUSED")),
        ("commit", ChannelClosedByBroker(406, "PRECONDITION_FAILED")),
    ],
    ids=["publish-refused", "commit-refused"],
)
def test_run_raises_without_reconnecting_when_results_cannot_be_published(
    config, monkeypatch, caplog, step, error
):
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [(channel, FailingResultsChannel(step, error))],
        lambda received: received,
    )

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert channel.published_messages == []
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues the input
    errors = [r for r in caplog.records if "could not be published" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]


def test_run_dead_letters_input_whose_result_is_too_large_and_continues(
    config, monkeypatch, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")

    class ResultsChannelsConnection(FakeConnection):
        """Opens the given results channels in order, as the executor reopens a closed one."""

        def __init__(self, channel: FakeChannel, *results_channels: FakeChannel) -> None:
            super().__init__(channel)
            self._next_results_channels = list(results_channels)

        def channel(self) -> FakeChannel:
            if self._opened:
                self._results_channel = self._next_results_channels.pop(0)
            return super().channel()

    channel = FakeChannel()
    huge = SampleMessage(message_id="huge", text="huge")
    small = SampleMessage(message_id="small", text="small")
    channel.queue_deliveries(huge.to_bytes(), small.to_bytes())
    executor = make_executor(
        config, channel, handler=lambda message: message, destination_exchange="output-exchange"
    )
    connection = ResultsChannelsConnection(channel, OversizedResultChannel(), FakeChannel())
    monkeypatch.setattr(executor, "_create_connection", lambda: connection)

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert [p.exchange for p in channel.published_messages] == [
        "input-queue.dlx",
        "output-exchange",
    ]
    assert channel.published_messages[1].body == small.to_bytes()
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.get_info().counts == ExecutorCounts(
        completed=1, dead_lettered=DeadLetterCounts(result_too_large=1)
    )
    records = [
        r for r in caplog.records if "Message dead-lettered type=SampleMessage id=huge" in r.message
    ]
    assert [record.levelname for record in records] == ["ERROR"]
    assert "larger than configured max size" in records[0].message
    headers = dead_letter_copies(channel)[0].properties.headers
    assert headers["x-kicak-dead-letter-reason"] == "result-too-large"
    assert headers["x-kicak-error-type"] == "pika.exceptions.ChannelClosedByBroker"
    assert "larger than configured max size" in headers["x-kicak-error-message"]
    # Its line tells how large the results it could not publish were
    first = kicak_values(handled_records(caplog)[0])
    assert (first["outcome"], first["reason"]) == ("dead_lettered", "result-too-large")
    assert (first["results"], first["result_bytes"], first["largest_result_bytes"]) == (
        1,
        len(huge.to_bytes()),
        len(huge.to_bytes()),
    )


def test_run_opens_a_new_results_channel_after_reconnecting(config, monkeypatch, sleeps):
    class DeliverThenDisconnectChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            deliveries = super().consume(queue, inactivity_timeout)
            yield next(deliveries)
            raise StreamLostError("connection lost")

    first_channel = DeliverThenDisconnectChannel()
    first_results_channel = FakeChannel()  # stays "open": only the connection was lost
    second_channel = FakeChannel()
    first_channel.queue_deliveries(SampleMessage(message_id="first", text="first").to_bytes())
    second_channel.queue_deliveries(SampleMessage(message_id="second", text="second").to_bytes())
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [(first_channel, first_results_channel), (second_channel, None)],
        lambda message: message,
    )

    run_until_stopped(executor)

    assert [p.body for p in first_channel.published_messages] == [
        SampleMessage(message_id="first", text="first").to_bytes()
    ]
    assert second_channel.results_channel is not first_results_channel
    assert [p.body for p in second_channel.published_messages] == [
        SampleMessage(message_id="second", text="second").to_bytes()
    ]
    assert first_results_channel.tx_select_calls == 1


@pytest.mark.parametrize(
    ("body", "handler", "action", "settled"),
    [
        (SampleMessage(message_id="input", text="input").to_bytes(), None, "acknowledging", "ack"),
        (b"not json", None, "dead-lettering", "dead-letter"),
        # Moved to the retry queue, then acknowledged
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            always_transient,
            "acknowledging",
            "ack",
        ),
    ],
    ids=["ack", "dead-letter", "retry"],
)
@pytest.mark.parametrize("make_error", LOST_DELIVERY_ERRORS)
def test_run_reconnects_when_connection_is_lost_while_settling(
    config, monkeypatch, sleeps, caplog, body, handler, action, settled, make_error
):
    original_channel = SettleFailingChannel(make_error())
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel, handler=handler)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    assert replacement_channel.acked_delivery_tags == [1]
    if settled == "dead-letter":
        assert dead_lettered(replacement_channel) == [1]
    counts = executor.get_info().counts
    assert counts.abandoned == 1
    assert counts.completed + counts.dead_lettered.total + counts.moved_to_retry == 1
    assert replacement_channel.prefetch_count_when_consuming == 1
    warnings = [
        r for r in caplog.records if f"Connection or channel lost while {action}" in r.message
    ]
    assert [record.levelname for record in warnings] == ["WARNING"]


@pytest.mark.parametrize("make_error", LOST_DELIVERY_ERRORS)
def test_run_raises_the_processing_error_when_connection_is_lost_while_dead_lettering(
    config, monkeypatch, make_error
):
    def broken(_message):
        raise ZeroDivisionError("bug in application code")

    channel = SettleFailingChannel(make_error())
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=broken)
    connections = [FakeConnection(channel)]
    monkeypatch.setattr(executor, "_create_connection", lambda: connections.pop(0))

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert channel.nacked_deliveries == []  # RabbitMQ requeues it with the lost connection
    assert executor.get_info().status == ExecutorStatus.CRASHED


def test_run_continues_when_connection_is_lost_while_requeueing_held_results_on_stop(
    config, monkeypatch, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = SettleFailingChannel(StreamLostError("connection lost"))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=UnroutableResultsChannel(True),
        destination_exchange="output-exchange",
    )
    monkeypatch.setattr("pykicak.executor.time.sleep", lambda _seconds: executor.stop())

    executor.run()

    assert channel.nacked_deliveries == []  # RabbitMQ requeues it with the lost connection
    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert "stopped on request while reconnecting" in caplog.text


def test_run_raises_when_acknowledgement_fails_without_losing_the_connection(config, caplog):
    channel = SettleFailingChannel(ChannelClosedByBroker(406, "PRECONDITION_FAILED"))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel)

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert channel.acked_delivery_tags == []
    assert "lost while" not in caplog.text


def test_retry_policy_is_infinite_with_capped_jittered_backoff(config, monkeypatch):
    executor = make_executor(config, FakeChannel())
    delays: list[float] = []
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= 8:
            raise AMQPConnectionError("temporary failure")
        return "connected"

    monkeypatch.setattr(executor, "_sleep_unless_stopped", record_backoff(delays))
    monkeypatch.setattr("pykicak.executor.random.random", lambda: 0.5)

    result = executor._retry_rabbitmq_operation(operation, "test operation")

    assert result == "connected"
    assert attempts == 9
    assert delays == [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 15.0]


def test_retry_policy_survives_long_outages(config, monkeypatch):
    executor = make_executor(config, FakeChannel())
    delays: list[float] = []
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= 1100:
            raise AMQPConnectionError("temporary failure")
        return "connected"

    monkeypatch.setattr(executor, "_sleep_unless_stopped", record_backoff(delays))
    monkeypatch.setattr("pykicak.executor.random.random", lambda: 1.0)
    monkeypatch.setattr(executor_logger, "disabled", True)

    result = executor._retry_rabbitmq_operation(operation, "test operation")

    assert result == "connected"
    assert max(delays) == 30.0


def test_backoff_sleeps_in_steps_of_at_most_one_second(config, sleeps):
    executor = make_executor(config, FakeChannel())

    assert executor._sleep_unless_stopped(2.5) is True

    assert sleeps == [1.0, 1.0, 0.5]


def test_backoff_ignores_stop_outside_run(config, sleeps):
    executor = make_executor(config, FakeChannel())
    executor.stop()

    assert executor._sleep_unless_stopped(2.0) is True

    assert sleeps == [1.0, 1.0]
