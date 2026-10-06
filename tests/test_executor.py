import dataclasses
import os
import signal
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    StreamLostError,
    UnroutableError,
)

import pykicak.executor as executor_module
from pykicak.config import KicakConfig
from pykicak.executor import (
    ExecutorStatus,
    KicakExecutorAbstract,
    MalformedMessageError,
    QueueType,
    TransientProcessingError,
)
from pykicak.messages import KicakMessage
from tests.fakes import (
    CONNECTION_VALUES,
    BoundQueue,
    FakeChannel,
    FakeConnection,
    NackedDelivery,
    StopConsuming,
    captured_connection_parameters,
)


@dataclasses.dataclass(frozen=True, slots=True)
class SampleMessage(KicakMessage):
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class UnserializableMessage(KicakMessage):
    value: object


TOPOLOGY = {
    "source_queue": "input-queue",
    "source_exchange": "input-exchange",
    "dead_letter_exchange": "input-dlx",
    "dead_letter_queue": "input-dlq",
}


class ProcessingExecutor(KicakExecutorAbstract):
    """Records executed messages; `handler`, if given, computes execute()'s return value."""

    def __init__(
        self,
        config: KicakConfig,
        channel: FakeChannel,
        handler=None,
        results_channel: FakeChannel | None = None,
        **topology,
    ) -> None:
        super().__init__(config, **topology)
        self._fake_channel = channel
        self._fake_results_channel = results_channel
        self._handler = handler
        self.processed_messages: list[KicakMessage] = []

    def _create_connection(self):
        return FakeConnection(self._fake_channel, self._fake_results_channel)

    def message_type(self) -> type[KicakMessage]:
        return SampleMessage

    def execute(self, message: KicakMessage):
        self.processed_messages.append(message)
        if self._handler is None:
            return None
        return self._handler(message)


def make_executor(
    config, channel, handler=None, results_channel=None, **overrides
) -> ProcessingExecutor:
    topology = {**TOPOLOGY, **overrides}
    return ProcessingExecutor(config, channel, handler, results_channel, **topology)


def run_until_stopped(executor: KicakExecutorAbstract) -> None:
    """Run until FakeChannel.consume() runs out of deliveries and signals the external stop."""
    with pytest.raises(StopConsuming):
        executor.run()


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values={})


@pytest.fixture
def sleeps(monkeypatch) -> list[float]:
    delays: list[float] = []
    monkeypatch.setattr(executor_module.time, "sleep", delays.append)
    return delays


# --- construction ---------------------------------------------------------------------------


def test_init_stores_source_queue_and_exchange(config):
    executor = make_executor(config, FakeChannel())

    assert executor._source_queue == "input-queue"
    assert executor._source_exchange == "input-exchange"


def test_init_stores_dead_letter_topology(config):
    executor = make_executor(config, FakeChannel())

    assert executor._dead_letter_exchange == "input-dlx"
    assert executor._dead_letter_queue == "input-dlq"


def test_init_stores_destination_exchange(config):
    executor = make_executor(config, FakeChannel(), destination_exchange="output-exchange")

    assert executor._destination_exchange == "output-exchange"


def test_init_allows_terminator_mode_without_destination_exchange(config):
    executor = make_executor(config, FakeChannel())

    assert executor._source_exchange == "input-exchange"
    assert executor._destination_exchange is None


def test_init_rejects_empty_source_exchange(config):
    with pytest.raises(ValueError, match="source_exchange must be a non-empty"):
        make_executor(config, FakeChannel(), source_exchange="")


@pytest.mark.parametrize(
    "name", ["source_queue", "dead_letter_exchange", "dead_letter_queue", "destination_exchange"]
)
def test_init_rejects_empty_topology_names(config, name):
    with pytest.raises(ValueError, match=f"{name} must be a non-empty"):
        make_executor(config, FakeChannel(), **{name: ""})


def test_init_requires_source_exchange_argument(config):
    with pytest.raises(TypeError, match="source_exchange"):
        ProcessingExecutor(config, FakeChannel(), source_queue="input-queue")


def test_init_requires_dead_letter_arguments(config):
    with pytest.raises(TypeError, match=r"dead_letter_exchange.*dead_letter_queue"):
        ProcessingExecutor(
            config, FakeChannel(), source_queue="input-queue", source_exchange="input-exchange"
        )


def test_subclass_must_implement_message_type(config):
    class WithoutMessageType(KicakExecutorAbstract):
        def execute(self, message: KicakMessage) -> None:
            return None

    with pytest.raises(TypeError, match="message_type"):
        WithoutMessageType(config, **TOPOLOGY)


def test_init_requires_keyword_topology_arguments(config):
    with pytest.raises(TypeError):
        KicakExecutorAbstract.__init__(
            object.__new__(ProcessingExecutor), config, "q", "x", "dlx", "dlq"
        )


@pytest.mark.parametrize("reused", ["source_exchange", "destination_exchange"])
def test_init_rejects_dead_letter_exchange_reused_for_message_flow(config, reused):
    topology = {"destination_exchange": "output-exchange", reused: "input-dlx"}

    with pytest.raises(ValueError, match="dead_letter_exchange must differ"):
        make_executor(config, FakeChannel(), **topology)


def test_init_uses_classic_queue_type_by_default(config):
    executor = make_executor(config, FakeChannel())

    assert executor._queue_type is QueueType.CLASSIC


@pytest.mark.parametrize(
    ("queue_type", "expected"),
    [
        (QueueType.CLASSIC, QueueType.CLASSIC),
        (QueueType.QUORUM, QueueType.QUORUM),
        ("classic", QueueType.CLASSIC),
        ("quorum", QueueType.QUORUM),
    ],
)
def test_init_accepts_queue_type_members_and_their_values(config, queue_type, expected):
    executor = make_executor(config, FakeChannel(), queue_type=queue_type)

    assert executor._queue_type is expected


@pytest.mark.parametrize("queue_type", ["Quorum", "stream", "", None])
def test_init_rejects_unknown_queue_type(config, queue_type):
    with pytest.raises(
        ValueError, match="queue_type must be QueueType.CLASSIC or QueueType.QUORUM, got"
    ):
        make_executor(config, FakeChannel(), queue_type=queue_type)


def test_init_rejects_dead_letter_queue_equal_to_source_queue(config):
    with pytest.raises(ValueError, match="dead_letter_queue must differ"):
        make_executor(config, FakeChannel(), dead_letter_queue="input-queue")


def test_init_rejects_non_positive_max_processing_attempts(config):
    with pytest.raises(ValueError, match="max_processing_attempts"):
        make_executor(config, FakeChannel(), max_processing_attempts=0)


@pytest.mark.parametrize("max_processing_attempts", [2.5, True, "3", None])
def test_init_rejects_non_integer_max_processing_attempts(config, max_processing_attempts):
    with pytest.raises(TypeError, match="max_processing_attempts must be an int"):
        make_executor(config, FakeChannel(), max_processing_attempts=max_processing_attempts)


def test_subclass_parameterized_with_its_message_type_receives_that_type(config):
    class TypedExecutor(KicakExecutorAbstract[SampleMessage]):
        def __init__(self, channel: FakeChannel) -> None:
            super().__init__(config, **TOPOLOGY)
            self._fake_channel = channel
            self.texts: list[str] = []

        def _create_connection(self):
            return FakeConnection(self._fake_channel)

        def message_type(self) -> type[SampleMessage]:
            return SampleMessage

        def execute(self, message: SampleMessage) -> None:
            self.texts.append(message.text)

    channel = FakeChannel()
    executor = TypedExecutor(channel)
    channel.queue_deliveries(SampleMessage(message_id="typed", text="typed").to_bytes())

    run_until_stopped(executor)

    assert executor.texts == ["typed"]
    assert channel.acked_delivery_tags == [1]


# --- heartbeat ------------------------------------------------------------------------------


def test_connection_uses_default_heartbeat_of_ten_minutes(monkeypatch):
    executor = make_executor(KicakConfig(values=CONNECTION_VALUES), FakeChannel())

    assert captured_connection_parameters(executor, monkeypatch).heartbeat == 600


def test_connection_uses_configured_heartbeat(monkeypatch):
    executor = make_executor(
        KicakConfig(values=CONNECTION_VALUES), FakeChannel(), heartbeat_seconds=1800
    )

    assert captured_connection_parameters(executor, monkeypatch).heartbeat == 1800


def test_connection_waits_while_rabbitmq_blocks_it(monkeypatch):
    executor = make_executor(KicakConfig(values=CONNECTION_VALUES), FakeChannel())

    parameters = captured_connection_parameters(executor, monkeypatch)

    assert parameters.blocked_connection_timeout is None


@pytest.mark.parametrize("heartbeat_seconds", [1, 65535])
def test_init_accepts_heartbeat_within_amqp_limits(config, heartbeat_seconds):
    executor = make_executor(config, FakeChannel(), heartbeat_seconds=heartbeat_seconds)

    assert executor._heartbeat_seconds == heartbeat_seconds


@pytest.mark.parametrize("heartbeat_seconds", [0, -1, 65536])
def test_init_rejects_heartbeat_outside_amqp_limits(config, heartbeat_seconds):
    with pytest.raises(ValueError, match="heartbeat_seconds must be from 1 to 65535"):
        make_executor(config, FakeChannel(), heartbeat_seconds=heartbeat_seconds)


@pytest.mark.parametrize("heartbeat_seconds", [1.5, "600", True])
def test_init_rejects_non_integer_heartbeat(config, heartbeat_seconds):
    with pytest.raises(TypeError, match="heartbeat_seconds must be an int"):
        make_executor(config, FakeChannel(), heartbeat_seconds=heartbeat_seconds)


# --- topology -------------------------------------------------------------------------------


def test_declare_topology_binds_source_queue_to_source_exchange(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    assert channel.declared_queues[0].queue == "input-queue"
    assert channel.declared_queues[0].durable is True
    assert channel.declared_exchanges[0].exchange == "input-exchange"
    assert channel.declared_exchanges[0].exchange_type == "fanout"
    assert channel.bound_queues[0] == BoundQueue(queue="input-queue", exchange="input-exchange")


def test_declare_topology_sets_dead_letter_exchange_on_source_queue(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    assert channel.declared_queues[0].arguments == {
        "x-dead-letter-exchange": "input-dlx",
        "x-queue-type": "classic",
    }


def test_declare_topology_declares_both_queues_as_classic_by_default(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    assert [(q.queue, q.arguments["x-queue-type"]) for q in channel.declared_queues] == [
        ("input-queue", "classic"),
        ("input-dlq", "classic"),
    ]


def test_declare_topology_declares_both_queues_as_quorum_when_requested(config):
    channel = FakeChannel()
    executor = make_executor(config, channel, queue_type=QueueType.QUORUM)

    executor.connect()

    source_queue, dead_letter_queue = channel.declared_queues
    assert source_queue.queue == "input-queue"
    assert source_queue.durable is True
    assert source_queue.arguments == {
        "x-dead-letter-exchange": "input-dlx",
        "x-queue-type": "quorum",
    }
    assert dead_letter_queue.queue == "input-dlq"
    assert dead_letter_queue.durable is True
    assert dead_letter_queue.arguments == {"x-queue-type": "quorum"}


def test_declare_topology_declares_and_binds_dead_letter_queue(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    dead_letter_exchange = next(e for e in channel.declared_exchanges if e.exchange == "input-dlx")
    assert dead_letter_exchange.exchange_type == "fanout"
    assert dead_letter_exchange.durable is True
    dead_letter_queue = next(q for q in channel.declared_queues if q.queue == "input-dlq")
    assert dead_letter_queue.durable is True
    assert BoundQueue(queue="input-dlq", exchange="input-dlx") in channel.bound_queues


def test_declare_destination_exchange_without_managing_downstream_queues(config):
    channel = FakeChannel()
    executor = make_executor(config, channel, destination_exchange="output-exchange")

    executor.connect()

    assert [queue.queue for queue in channel.declared_queues] == ["input-queue", "input-dlq"]
    assert [exchange.exchange for exchange in channel.declared_exchanges] == [
        "input-exchange",
        "input-dlx",
        "output-exchange",
    ]
    assert channel.bound_queues == [
        BoundQueue(queue="input-queue", exchange="input-exchange"),
        BoundQueue(queue="input-dlq", exchange="input-dlx"),
    ]


def test_declare_topology_in_terminator_mode_binds_source_queue(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    assert [queue.queue for queue in channel.declared_queues] == ["input-queue", "input-dlq"]
    assert [exchange.exchange for exchange in channel.declared_exchanges] == [
        "input-exchange",
        "input-dlx",
    ]
    assert channel.bound_queues[0] == BoundQueue(queue="input-queue", exchange="input-exchange")


# --- successful processing ------------------------------------------------------------------


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


# --- deterministic failures: dead-letter and continue ---------------------------------------


def test_run_dead_letters_undecodable_message_and_continues(config, caplog):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    valid = SampleMessage(message_id="valid", text="valid")
    channel.queue_deliveries(b"not json", valid.to_bytes())

    run_until_stopped(executor)

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]
    assert channel.acked_delivery_tags == [2]
    assert executor.processed_messages == [valid]
    dead_letter_records = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert len(dead_letter_records) == 1
    assert dead_letter_records[0].levelname == "ERROR"
    assert "dead_letter_exchange=input-dlx" in dead_letter_records[0].message


def test_run_dead_letters_message_with_unexpected_fields(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(b'{"message_id": "hello", "text": "hello", "extra": true}')

    run_until_stopped(executor)

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]
    assert executor.processed_messages == []


@pytest.mark.parametrize(
    "body",
    [
        b'{"text": "hello"}',
        b'{"message_id": "", "text": "hello"}',
        b'{"message_id": 7, "text": "hello"}',
    ],
    ids=["missing", "empty", "not-a-string"],
)
def test_run_dead_letters_message_without_valid_message_id_and_continues(config, body):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(body, SampleMessage(message_id="valid", text="valid").to_bytes())

    run_until_stopped(executor)

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]
    assert channel.acked_delivery_tags == [2]
    assert executor.processed_messages == [SampleMessage(message_id="valid", text="valid")]


def test_run_dead_letters_message_without_body(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(None)

    run_until_stopped(executor)

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]
    assert channel.acked_delivery_tags == []


def test_run_dead_letters_message_rejected_by_execute_without_publishing(config):
    def reject(_message):
        raise MalformedMessageError("fails application validation")

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=reject, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="invalid", text="invalid").to_bytes())

    run_until_stopped(executor)

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]
    assert channel.published_messages == []
    assert channel.acked_delivery_tags == []


# --- transient failures: retry, then requeue ------------------------------------------------


def test_run_retries_transient_failure_then_publishes_and_acknowledges(config, sleeps, caplog):
    attempts = 0
    result = SampleMessage(message_id="result", text="result")

    def flaky(_message):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TransientProcessingError("service unavailable")
        return result

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=flaky, destination_exchange="output-exchange")
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert attempts == 2
    assert sleeps == [0.5]
    assert channel.operations == ["publish output-exchange", "commit", "ack 1"]
    assert "Transient processing failure id=input, retrying attempt 2/3" in caplog.text


def test_run_requeues_message_after_exhausting_transient_retries(config, sleeps, caplog):
    def unavailable(_message):
        raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=unavailable, destination_exchange="output-exchange"
    )
    later = SampleMessage(message_id="later", text="later")
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), later.to_bytes()
    )

    run_until_stopped(executor)

    assert sleeps == [0.5, 1.0, 0.5, 1.0]
    assert len(executor.processed_messages) == 6
    assert channel.nacked_deliveries == [
        NackedDelivery(delivery_tag=1, requeue=True),
        NackedDelivery(delivery_tag=2, requeue=True),
    ]
    assert channel.published_messages == []
    assert channel.acked_delivery_tags == []
    requeue_records = [r for r in caplog.records if "Message requeued" in r.message]
    assert [record.levelname for record in requeue_records] == ["WARNING", "WARNING"]


def test_run_requeues_without_retrying_when_one_attempt_is_allowed(config, sleeps):
    def unavailable(_message):
        raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable, max_processing_attempts=1)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert sleeps == []
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]


# --- unexpected failures: dead-letter and stop ----------------------------------------------


def test_run_dead_letters_and_raises_on_unexpected_processing_error(config, caplog):
    def broken(_message):
        raise ZeroDivisionError("bug in application code")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=broken)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        SampleMessage(message_id="never-processed", text="never processed").to_bytes(),
    )

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]
    assert len(executor.processed_messages) == 1
    assert channel.is_open is False
    assert "stopping the executor" in caplog.text


def test_run_raises_the_processing_error_when_closing_the_connection_also_fails(config, caplog):
    class FailingCloseConnection(FakeConnection):
        def close(self) -> None:
            raise StreamLostError("connection close failed")

    def broken(_message):
        raise ZeroDivisionError("bug in application code")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=broken)
    executor._create_connection = lambda: FailingCloseConnection(channel)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert executor.get_status() == ExecutorStatus.CRASHED
    assert "Failed to close RabbitMQ connection" in caplog.text


def test_run_dead_letters_and_raises_when_terminator_returns_results(config):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=lambda message: message)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(RuntimeError, match="no destination_exchange"):
        executor.run()

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]
    assert channel.published_messages == []


def test_run_publishes_nothing_when_any_result_cannot_be_serialized(config):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: [
            SampleMessage(message_id="ok", text="ok"),
            UnserializableMessage(message_id="unserializable", value=object()),
        ],
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(TypeError):
        executor.run()

    assert channel.published_messages == []
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]


def test_run_dead_letters_and_raises_when_execute_returns_non_message(config):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: [{"text": "not a message"}],
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(TypeError, match="must return KicakMessage instances"):
        executor.run()

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=False)]


# --- one message at a time, confirmed results, and stopping ---------------------------------


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


def test_run_requeues_input_and_raises_when_results_are_returned_as_unroutable(config, caplog):
    results_channel = FakeChannel()
    results_channel.unroutable = True  # RabbitMQ routes at the commit and returns the results
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        SampleMessage(message_id="never-processed", text="never processed").to_bytes(),
    )

    with pytest.raises(UnroutableError) as raised:
        executor.run()

    assert [returned.body for returned in raised.value.messages] == [
        SampleMessage(message_id="input", text="input").to_bytes()
    ]
    assert channel.published_messages == []
    assert channel.operations == ["publish output-exchange", "commit", "nack 1 requeue=True"]
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert channel.acked_delivery_tags == []
    assert len(executor.processed_messages) == 1
    assert channel.is_open is False
    assert "requeueing it and stopping the executor" in caplog.text


def test_run_raises_when_rabbitmq_cancels_the_consumer(config, caplog):
    class CancellingChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            # pika ends the consume() generator when RabbitMQ cancels the consumer
            return iter(())

    channel = CancellingChannel()
    executor = make_executor(config, channel)

    with pytest.raises(RuntimeError, match="cancelled the consumer of queue 'input-queue'"):
        executor.run()

    assert channel.is_open is False
    cancel_records = [r for r in caplog.records if "cancelled the consumer" in r.message]
    assert [record.levelname for record in cancel_records] == ["ERROR"]


def test_run_leaves_in_flight_message_unsettled_when_stopped_externally(config):
    def interrupted(_message):
        raise KeyboardInterrupt

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=interrupted)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(KeyboardInterrupt):
        executor.run()

    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []
    assert channel.is_open is False


# --- graceful stop --------------------------------------------------------------------------


def test_stop_during_processing_finishes_the_message_then_returns(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor_holder: list[ProcessingExecutor] = []

    def stop_while_processing(message):
        executor_holder[0].stop()
        return message

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=stop_while_processing, destination_exchange="output-exchange"
    )
    executor_holder.append(executor)
    first = SampleMessage(message_id="in-progress", text="in progress")
    channel.queue_deliveries(
        first.to_bytes(), SampleMessage(message_id="never-taken", text="never taken").to_bytes()
    )

    executor.run()  # returns normally instead of raising StopConsuming

    assert executor.processed_messages == [first]
    assert channel.operations == ["publish output-exchange", "commit", "ack 1"]
    assert channel.is_open is False
    assert "stopped on request" in caplog.text


def test_stop_while_waiting_for_a_message_returns(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(
        SampleMessage(
            message_id="processed-before-the-wait", text="processed before the wait"
        ).to_bytes()
    )
    channel.on_idle = executor.stop

    executor.run()

    assert len(executor.processed_messages) == 1
    assert channel.acked_delivery_tags == [1]
    assert channel.inactivity_timeout == 1.0
    assert channel.is_open is False


def test_message_delivered_after_stop_request_is_left_unprocessed(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(
        SampleMessage(message_id="arrives-after-stop", text="arrives after stop").to_bytes()
    )
    channel.before_delivery = lambda _tag: executor.stop()

    executor.run()

    assert executor.processed_messages == []
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []
    assert channel.is_open is False
    assert "unprocessed for RabbitMQ to redeliver" in caplog.text


def test_run_after_stop_returns_without_connecting(config, monkeypatch):
    executor = make_executor(config, FakeChannel())

    def unexpected_connection():
        raise AssertionError("a stopped executor must not connect")

    monkeypatch.setattr(executor, "_create_connection", unexpected_connection)
    executor.stop()

    executor.run()


def test_stop_during_reconnection_backoff_returns(config, monkeypatch, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor = make_executor(config, FakeChannel())
    connection_attempts = 0

    def unavailable():
        nonlocal connection_attempts
        connection_attempts += 1
        raise AMQPConnectionError("broker down")

    monkeypatch.setattr(executor, "_create_connection", unavailable)
    monkeypatch.setattr(executor_module.time, "sleep", lambda _seconds: executor.stop())

    executor.run()

    assert connection_attempts == 1
    assert "stopped on request while reconnecting" in caplog.text


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="needs POSIX signals")
def test_stop_can_be_called_from_a_signal_handler(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="processed", text="processed").to_bytes())
    channel.on_idle = lambda: time.sleep(0.01)
    previous = signal.signal(signal.SIGUSR1, lambda _signum, _frame: executor.stop())
    timer = threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGUSR1))
    try:
        timer.start()
        executor.run()
    finally:
        timer.cancel()
        signal.signal(signal.SIGUSR1, previous)

    assert channel.acked_delivery_tags == [1]
    assert channel.is_open is False


def test_stop_can_be_called_from_another_thread(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="processed", text="processed").to_bytes())
    channel.on_idle = lambda: time.sleep(0.01)
    errors: list[BaseException] = []

    def run() -> None:
        try:
            executor.run()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    time.sleep(0.2)
    executor.stop()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    assert channel.acked_delivery_tags == [1]


# --- status and timestamp -------------------------------------------------------------------


class FakeClock:
    """Stands in for executor._now(); tests move the time forward explicitly."""

    def __init__(self) -> None:
        self.current = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)

    def advance(self, seconds: float = 1.0) -> datetime:
        self.current += timedelta(seconds=seconds)
        return self.current

    def __call__(self) -> datetime:
        return self.current


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(executor_module, "_now", fake)
    return fake


def crash_on_first_call(executor_holder: list[ProcessingExecutor], statuses: list[str]):
    """execute() handler: raises on the first message, records the status on later ones."""
    calls = 0

    def handler(_message):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ZeroDivisionError("bug in application code")
        statuses.append(executor_holder[0].get_status())

    return handler


def test_status_values_are_plain_strings():
    assert [status.value for status in ExecutorStatus] == [
        "RUNNING",
        "STOPPING",
        "STOPPED",
        "CRASHED",
    ]
    assert ExecutorStatus.CRASHED == "CRASHED"


def test_status_is_running_after_creation(config):
    executor = make_executor(config, FakeChannel())

    status = executor.get_status()

    assert status == "RUNNING"
    assert type(status) is str


def test_status_is_stopping_after_stop(config):
    executor = make_executor(config, FakeChannel())

    executor.stop()

    assert executor.get_status() == "STOPPING"


def test_status_is_stopping_while_the_message_in_progress_finishes(config):
    executor_holder: list[ProcessingExecutor] = []
    statuses: list[str] = []

    def stop_then_read_status(_message):
        executor_holder[0].stop()
        statuses.append(executor_holder[0].get_status())

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=stop_then_read_status)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="in-progress", text="in progress").to_bytes())

    executor.run()

    assert statuses == ["STOPPING"]
    assert executor.get_status() == "STOPPED"


def test_status_is_stopped_after_stop_while_waiting(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.on_idle = executor.stop

    executor.run()

    assert executor.get_status() == "STOPPED"


def test_status_is_stopped_when_run_follows_stop(config):
    executor = make_executor(config, FakeChannel())
    executor.stop()

    executor.run()

    assert executor.get_status() == "STOPPED"


@pytest.mark.parametrize(
    "failure", [ZeroDivisionError("bug"), KeyboardInterrupt()], ids=["error", "hard-stop"]
)
def test_status_is_crashed_when_run_raises(config, failure, caplog):
    def fail(_message):
        raise failure

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=fail)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(type(failure)):
        executor.run()

    assert executor.get_status() == "CRASHED"
    assert f"crashed with {type(failure).__name__}" in caplog.text


def test_stop_after_crash_keeps_crashed_status_and_timestamp(config, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=crash_on_first_call([], []))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    crashed_at = clock.advance()
    with pytest.raises(ZeroDivisionError):
        executor.run()

    clock.advance()
    executor.stop()

    assert executor.get_timestamp() == crashed_at.isoformat()
    assert executor.get_status() == "CRASHED"


def test_status_is_running_again_when_run_follows_a_crash(config):
    executor_holder: list[ProcessingExecutor] = []
    statuses: list[str] = []
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=crash_on_first_call(executor_holder, statuses)
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    with pytest.raises(ZeroDivisionError):
        executor.run()
    channel.is_open = True

    run_until_stopped(executor)  # consumes the queued delivery again

    assert statuses == ["RUNNING"]


def test_status_can_be_read_from_another_thread_while_running(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.on_idle = lambda: time.sleep(0.01)
    thread = threading.Thread(target=executor.run)
    thread.start()
    time.sleep(0.1)

    running = executor.get_status()
    executor.stop()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert (running, executor.get_status()) == ("RUNNING", "STOPPED")


def test_timestamp_is_iso_8601_in_utc(config):
    executor = make_executor(config, FakeChannel())

    parsed = datetime.fromisoformat(executor.get_timestamp())

    assert parsed.utcoffset() == timedelta(0)


def test_timestamp_starts_at_creation(config, clock):
    executor = make_executor(config, FakeChannel())
    clock.advance()

    assert executor.get_timestamp() == "2026-10-01T12:00:00+00:00"


def test_timestamp_follows_status_changes(config, clock):
    executor = make_executor(config, FakeChannel())

    stopping_at = clock.advance()
    executor.stop()
    assert executor.get_timestamp() == stopping_at.isoformat()

    stopped_at = clock.advance()
    executor.run()
    assert executor.get_timestamp() == stopped_at.isoformat()


def test_reading_the_status_updates_the_timestamp(config, clock):
    executor = make_executor(config, FakeChannel())

    read_at = clock.advance(5)
    executor.get_status()
    clock.advance(5)

    assert executor.get_timestamp() == read_at.isoformat()  # get_timestamp() does not update it


def test_repeated_stop_does_not_change_the_timestamp(config, clock):
    executor = make_executor(config, FakeChannel())
    first_stop_at = clock.advance()
    executor.stop()

    clock.advance()
    executor.stop()

    assert executor.get_timestamp() == first_stop_at.isoformat()


# --- connection failures --------------------------------------------------------------------


def test_run_reconnects_after_consumer_connection_loss(config, monkeypatch, caplog):
    class DisconnectingChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            def disconnected():
                raise StreamLostError("connection lost")
                yield None, None, None

            return disconnected()

    disconnected_channel = DisconnectingChannel()
    recovered_channel = FakeChannel()
    recovered_channel.queue_deliveries(
        SampleMessage(message_id="recovered", text="recovered").to_bytes()
    )
    channels = [disconnected_channel, recovered_channel]
    executor = make_executor(config, disconnected_channel)
    monkeypatch.setattr(executor_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        executor,
        "_create_connection",
        lambda: FakeConnection(channels.pop(0)),
    )

    run_until_stopped(executor)

    assert executor.processed_messages == [SampleMessage(message_id="recovered", text="recovered")]
    assert recovered_channel.acked_delivery_tags == [1]
    assert recovered_channel.prefetch_count_when_consuming == 1
    assert "Connection lost, reconnecting" in caplog.text


class FailingResultsChannel(FakeChannel):
    """Results channel that raises `error` at `step`: "publish 1", "publish 2", ..., or "commit".

    A connection error also marks the channel closed, as pika does.
    """

    def __init__(self, step: str, error: Exception) -> None:
        super().__init__()
        self._step = step
        self._error = error
        self.publish_attempts = 0

    def _fail_at(self, step: str) -> None:
        if step == self._step:
            if isinstance(self._error, StreamLostError):
                self.is_open = False
            raise self._error

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        self.publish_attempts += 1
        self._fail_at(f"publish {self.publish_attempts}")
        super().basic_publish(exchange, routing_key, body, properties, mandatory)

    def tx_commit(self) -> None:
        self._fail_at("commit")
        super().tx_commit()


def reconnecting_executor(config, monkeypatch, connections, handler):
    """Executor whose (re)connections open `connections` in order.

    Each item is a (consuming channel, results channel or None) pair.
    """
    executor = make_executor(
        config, connections[0][0], handler=handler, destination_exchange="output-exchange"
    )
    remaining = list(connections)
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(*remaining.pop(0)))
    return executor


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
    warnings = [r for r in caplog.records if "discarding the uncommitted results" in r.message]
    assert [record.levelname for record in warnings] == ["WARNING"]


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


class SettleFailingChannel(FakeChannel):
    """Consuming channel whose first basic_ack or basic_nack raises `error`.

    A connection error also marks the channel closed, as pika does.
    """

    def __init__(self, error: Exception) -> None:
        super().__init__()
        self._error: Exception | None = error

    def _fail_once(self) -> None:
        error, self._error = self._error, None
        if error is not None:
            if isinstance(error, StreamLostError):
                self.is_open = False
            raise error

    def basic_ack(self, delivery_tag: int) -> None:
        self._fail_once()
        super().basic_ack(delivery_tag)

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        self._fail_once()
        super().basic_nack(delivery_tag, multiple, requeue)


def always_transient(_message):
    raise TransientProcessingError("service unavailable")


@pytest.mark.parametrize(
    ("body", "handler", "action", "settled"),
    [
        (SampleMessage(message_id="input", text="input").to_bytes(), None, "acknowledging", "ack"),
        (b"not json", None, "dead-lettering", NackedDelivery(1, requeue=False)),
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            always_transient,
            "requeueing",
            NackedDelivery(1, requeue=True),
        ),
    ],
    ids=["ack", "dead-letter", "requeue"],
)
def test_run_reconnects_when_connection_is_lost_while_settling(
    config, monkeypatch, sleeps, caplog, body, handler, action, settled
):
    original_channel = SettleFailingChannel(StreamLostError("connection lost"))
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel, handler=handler)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    if settled == "ack":
        assert replacement_channel.acked_delivery_tags == [1]
    else:
        assert replacement_channel.nacked_deliveries == [settled]
    assert replacement_channel.prefetch_count_when_consuming == 1
    warnings = [r for r in caplog.records if f"Connection lost while {action}" in r.message]
    assert [record.levelname for record in warnings] == ["WARNING"]


def test_run_raises_the_processing_error_when_connection_is_lost_while_dead_lettering(
    config, monkeypatch
):
    def broken(_message):
        raise ZeroDivisionError("bug in application code")

    channel = SettleFailingChannel(StreamLostError("connection lost"))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=broken)
    connections = [FakeConnection(channel)]
    monkeypatch.setattr(executor, "_create_connection", lambda: connections.pop(0))

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert channel.nacked_deliveries == []  # RabbitMQ requeues it with the lost connection
    assert executor.get_status() == ExecutorStatus.CRASHED


def test_run_raises_unroutable_error_when_connection_is_lost_while_requeueing(
    config, monkeypatch
):
    results_channel = FakeChannel()
    results_channel.unroutable = True
    channel = SettleFailingChannel(StreamLostError("connection lost"))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )

    with pytest.raises(UnroutableError):
        executor.run()

    assert channel.nacked_deliveries == []
    assert executor.get_status() == ExecutorStatus.CRASHED


def test_run_raises_when_acknowledgement_fails_without_losing_the_connection(config, caplog):
    channel = SettleFailingChannel(ChannelClosedByBroker(406, "PRECONDITION_FAILED"))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel)

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert channel.acked_delivery_tags == []
    assert "Connection lost" not in caplog.text


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
    monkeypatch.setattr(executor_module.logger, "disabled", True)

    result = executor._retry_rabbitmq_operation(operation, "test operation")

    assert result == "connected"
    assert max(delays) == 30.0


def record_backoff(delays: list[float]):
    """Stand-in for _sleep_unless_stopped() that records each backoff delay without sleeping."""

    def sleep_unless_stopped(seconds: float) -> bool:
        delays.append(seconds)
        return True

    return sleep_unless_stopped


def test_backoff_sleeps_in_steps_of_at_most_one_second(config, sleeps):
    executor = make_executor(config, FakeChannel())

    assert executor._sleep_unless_stopped(2.5) is True

    assert sleeps == [1.0, 1.0, 0.5]


def test_backoff_ignores_stop_outside_run(config, sleeps):
    executor = make_executor(config, FakeChannel())
    executor.stop()

    assert executor._sleep_unless_stopped(2.0) is True

    assert sleeps == [1.0, 1.0]
