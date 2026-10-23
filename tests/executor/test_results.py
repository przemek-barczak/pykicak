"""Executing deliveries and publishing their results, held while unroutable."""

from typing import Any

import pika
import pika.channel
import pytest
from pika.exceptions import StreamLostError

from pykicak.executor import (
    ExecutorCounts,
    ExecutorStatus,
)
from tests.executor.helpers import (
    ProcessingExecutor,
    SampleMessage,
    UnroutableResultsChannel,
    make_executor,
    reconnecting_executor,
    run_until_stopped,
)
from tests.fakes import (
    SAMPLE_TYPE,
    BoundQueue,
    FakeChannel,
    NackedDelivery,
)


def test_run_consumes_and_processes_messages(config):
    channel = FakeChannel()
    executor = make_executor(config, channel, destination_exchange="output-exchange")
    msg1 = SampleMessage(message_id="message1", text="message1")
    msg2 = SampleMessage(message_id="message2", text="message2")
    channel.queue_deliveries(msg1.to_bytes(), msg2.to_bytes())

    run_until_stopped(executor)

    assert executor.processed_messages == [msg1, msg2]


def test_run_acknowledges_messages_after_processing(config, caplog):
    channel = FakeChannel()
    executor = make_executor(config, channel, destination_exchange="output-exchange")
    msg1 = SampleMessage(message_id="message1", text="message1")
    msg2 = SampleMessage(message_id="message2", text="message2")
    channel.queue_deliveries(msg1.to_bytes(), msg2.to_bytes())

    caplog.set_level("DEBUG", logger="pykicak.executor")
    run_until_stopped(executor)

    assert channel.acked_delivery_tags == [1, 2]
    received_records = [record for record in caplog.records if "Message received" in record.message]
    assert len(received_records) == 2
    assert all(record.levelname == "INFO" for record in received_records)
    assert all("type=SampleMessage" in record.message for record in received_records)
    assert "id=message1" in received_records[0].message
    assert "id=message2" in received_records[1].message
    assert all("queue=input-queue" in record.message for record in received_records)
    assert any("Message acknowledged id=message1" in record.message for record in caplog.records)


def test_run_publishes_returned_result_before_acknowledging(config):
    channel = FakeChannel()
    result = SampleMessage(message_id="result", text="result")
    executor = make_executor(
        config, channel, handler=lambda _message: result, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert [published.body for published in channel.published_messages] == [result.to_bytes()]
    assert channel.operations == ["publish output-exchange", "commit", "ack 1"]


def test_run_publishes_every_returned_result(config):
    channel = FakeChannel()
    results = [
        SampleMessage(message_id="first", text="first"),
        SampleMessage(message_id="second", text="second"),
    ]
    executor = make_executor(
        config, channel, handler=lambda _message: results, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert [published.body for published in channel.published_messages] == [
        result.to_bytes() for result in results
    ]
    assert channel.acked_delivery_tags == [1]


def test_run_acknowledges_without_publishing_when_execute_returns_none(config):
    channel = FakeChannel()
    executor = make_executor(config, channel, destination_exchange="output-exchange")
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert channel.published_messages == []
    assert channel.acked_delivery_tags == [1]


def test_run_consumes_one_message_at_a_time(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert channel.prefetch_count_when_consuming == 1


def test_run_publishes_results_as_mandatory(config):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda message: message, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert channel.published_messages[0].mandatory is True


def test_run_publishes_all_results_in_one_transaction_on_their_own_channel(config):
    results = [
        SampleMessage(message_id="chunk-1", text="first"),
        SampleMessage(message_id="chunk-2", text="second"),
    ]
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda _message: results, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    results_channel = channel.results_channel
    assert results_channel is not None and results_channel is not channel
    assert results_channel.transactional is True
    assert channel.confirm_delivery_enabled is True and channel.transactional is False
    # Both results are committed together, and the input is acknowledged only afterwards, on
    # the consuming channel, outside the transaction
    assert channel.operations == [
        "publish output-exchange",
        "publish output-exchange",
        "commit",
        "ack 1",
    ]
    assert [p.body for p in channel.published_messages] == [r.to_bytes() for r in results]
    assert channel.acked_delivery_tags == [1]
    assert results_channel.acked_delivery_tags == []
    assert results_channel.nacked_deliveries == []


def test_run_opens_one_results_channel_per_connection(config):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda message: message, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(
        SampleMessage(message_id="first", text="first").to_bytes(),
        SampleMessage(message_id="second", text="second").to_bytes(),
    )

    run_until_stopped(executor)

    assert channel.results_channel is not None
    assert channel.results_channel.tx_select_calls == 1
    assert channel.operations == [
        "publish output-exchange",
        "commit",
        "ack 1",
        "publish output-exchange",
        "commit",
        "ack 2",
    ]


@pytest.mark.parametrize("handler", [None, lambda _message: []], ids=["none", "empty"])
def test_run_opens_no_results_channel_without_results(config, handler):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=handler, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert channel.results_channel is None
    assert channel.operations == ["ack 1"]


def test_run_publishes_each_result_with_its_message_id_property(config):
    results = [
        SampleMessage(message_id="result-1", text="first"),
        SampleMessage(message_id="result-2", text="second"),
    ]
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda _message: results, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    published = channel.published_messages
    assert [message.properties.message_id for message in published] == ["result-1", "result-2"]
    assert all(message.properties.content_type == "application/json" for message in published)


@pytest.mark.parametrize(
    "received",
    [
        pika.BasicProperties(
            type=SAMPLE_TYPE,
            user_id="upstream-executor",
            headers={"x-kicak-original-publisher": "injector-user"},
        ),
        pika.BasicProperties(type=SAMPLE_TYPE, user_id="alice"),
        pika.BasicProperties(type=SAMPLE_TYPE),
    ],
    ids=["from-a-pykicak-agent", "from-another-client", "unknown-publisher"],
)
def test_results_name_the_executor_as_their_original_publisher(config, clock, received):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: SampleMessage(message_id="result", text="result"),
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=received
    )

    run_until_stopped(executor)

    # The executor created the result, whoever created its input
    properties = channel.published_messages[0].properties
    assert properties.user_id == "guest"
    assert properties.headers == {
        "x-kicak-original-publisher": "guest",
        "x-kicak-published-at": "2026-10-01T12:00:00+00:00",
    }


def test_run_holds_input_and_results_while_unroutable_then_publishes_them(
    config, monkeypatch, sleeps, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")
    monkeypatch.setattr("pykicak.executor.random.random", lambda: 1.0)
    first = SampleMessage(message_id="first", text="first")
    second = SampleMessage(message_id="second", text="second")
    channel = FakeChannel()
    channel.queue_deliveries(first.to_bytes(), second.to_bytes())
    results_channel = UnroutableResultsChannel(True, True, False, True, False)
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )

    run_until_stopped(executor)

    # Each input is executed once, held unacknowledged, and acknowledged once its results commit
    assert executor.processed_messages == [first, second]
    assert channel.operations == [
        *["publish output-exchange", "commit"] * 3,
        "ack 1",
        *["publish output-exchange", "commit"] * 2,
        "ack 2",
    ]
    assert [p.body for p in channel.published_messages] == [first.to_bytes(), second.to_bytes()]
    assert channel.nacked_deliveries == []
    # The output side is declared again before every attempt
    assert [q.queue for q in results_channel.declared_queues] == ["output-queue"] * 5
    assert results_channel.bound_queues == [BoundQueue("output-queue", "output-exchange")] * 5
    # The wait doubles while the results stay unroutable, and starts over for the next input
    assert sleeps == [0.5, 1.0, 0.5]
    unroutable_records = [r for r in caplog.records if "as unroutable" in r.message]
    assert [record.levelname for record in unroutable_records] == ["ERROR"] * 3
    assert "holding the message and retrying in 0.5 seconds" in unroutable_records[0].message
    assert [r.levelname for r in caplog.records if "is recovering" in r.message] == ["WARNING"] * 2
    recovered = [r for r in caplog.records if "recovered from DOWNSTREAM_RECOVER" in r.message]
    assert [record.levelname for record in recovered] == ["INFO"] * 2


def test_holding_results_waits_in_steps_of_at_most_one_second(config, monkeypatch, sleeps):
    monkeypatch.setattr("pykicak.executor.random.random", lambda: 1.0)
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=UnroutableResultsChannel(True, True, True, False),
        destination_exchange="output-exchange",
    )

    run_until_stopped(executor)

    assert sleeps == [0.5, 1.0, 1.0, 1.0]  # waits of 0.5, 1.0, and 2.0 seconds
    assert channel.acked_delivery_tags == [1]


def test_stop_while_holding_results_requeues_the_input_and_returns(config, monkeypatch, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        SampleMessage(message_id="never-taken", text="never taken").to_bytes(),
    )
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=UnroutableResultsChannel(True),
        destination_exchange="output-exchange",
    )
    monkeypatch.setattr("pykicak.executor.time.sleep", lambda _seconds: executor.stop())

    executor.run()

    assert len(executor.processed_messages) == 1
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert channel.acked_delivery_tags == []
    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert executor.get_info().counts == ExecutorCounts(requeued=1)
    assert "Stop requested while holding the results of message id=input" in caplog.text
    assert "stopped on request" in caplog.text


def test_stop_requested_during_execute_requeues_unroutable_results_without_waiting(
    config, monkeypatch, sleeps
):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_while_processing(message):
        executor_holder[0].stop()
        return message

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=stop_while_processing,
        results_channel=UnroutableResultsChannel(True),
        destination_exchange="output-exchange",
    )
    executor_holder.append(executor)

    executor.run()

    assert sleeps == []
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert executor.get_info().status == ExecutorStatus.STOPPED


@pytest.mark.parametrize(
    "interruption",
    ["consumer-timeout", "consumer-cancelled", "connection-lost"],
    ids=["consumer-timeout", "consumer-cancelled", "lost"],
)
def test_run_reconnects_when_the_held_input_is_requeued_by_rabbitmq(
    config, monkeypatch, caplog, interruption
):
    message = SampleMessage(message_id="input", text="input")
    original_channel = FakeChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(message.to_bytes())
    replacement_channel.queue_deliveries(message.to_bytes())  # RabbitMQ's redelivery
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [(original_channel, UnroutableResultsChannel(True)), (replacement_channel, None)],
        lambda received: received,
    )
    interrupted = False

    def sleep(_seconds: float) -> None:
        nonlocal interrupted
        if interrupted:
            return
        interrupted = True
        if interruption == "consumer-timeout":
            original_channel.is_open = False  # RabbitMQ closed it while pika answered heartbeats
        elif interruption == "consumer-cancelled":
            original_channel.cancel_consumer()  # RabbitMQ 4.3+ on a quorum queue's timeout
        else:
            raise StreamLostError("connection lost")

    monkeypatch.setattr("pykicak.executor.time.sleep", sleep)

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    assert executor.processed_messages == [message, message]
    assert [p.body for p in replacement_channel.published_messages] == [message.to_bytes()]
    assert replacement_channel.acked_delivery_tags == [1]
    assert "Connection or channel lost while publishing results of message id=input" in (
        caplog.text
    )
