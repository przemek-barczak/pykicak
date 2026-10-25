"""Declaring the topology, on connecting and again while the Executor runs."""

import pytest
from pika.exceptions import ChannelClosedByBroker

from pykicak.executor import (
    ExecutorStatus,
    QueueType,
)
from tests.executor.helpers import (
    BindFailingChannel,
    SampleMessage,
    always_transient,
    make_executor,
    run_until_stopped,
)
from tests.fakes import (
    BoundQueue,
    FakeChannel,
    StopConsuming,
)


class DeclarationRecordingChannel(FakeChannel):
    """Also records declarations and bindings in `operations`, to check their order."""

    def queue_declare(
        self, queue, durable=False, exclusive=False, auto_delete=False, arguments=None
    ):
        self.operations.append(f"queue {queue}")
        super().queue_declare(queue, durable, exclusive, auto_delete, arguments)

    def exchange_declare(self, exchange, exchange_type, durable=False):
        self.operations.append(f"exchange {exchange}")
        super().exchange_declare(exchange, exchange_type, durable)

    def queue_bind(self, queue, exchange):
        self.operations.append(f"bind {queue} {exchange}")
        super().queue_bind(queue, exchange)


INPUT_SIDE = [
    "queue input-queue",
    "exchange input-exchange",
    "bind input-queue input-exchange",
    "exchange input-queue.dlx",
    "queue input-queue.dlq",
    "bind input-queue.dlq input-queue.dlx",
    "queue input-queue.retry",
]
"""Operations of one verification of the input side of the default TOPOLOGY."""


DEAD_LETTER_ROUTE = INPUT_SIDE[3:6]


OUTPUT_SIDE = [
    "exchange output-exchange",
    "queue result-a",
    "bind result-a output-exchange",
    "queue result-b",
    "bind result-b output-exchange",
]
"""Operations of one verification of the output side, with destination queues result-a/b."""


class FakeMonotonicClock:
    """Stands in for time.monotonic(); each wakeup of FakeChannel.consume() advances it."""

    def __init__(self, seconds_per_wakeup: float) -> None:
        self.current = 0.0
        self.seconds_per_wakeup = seconds_per_wakeup

    def __call__(self) -> float:
        return self.current

    def advance(self) -> None:
        self.current += self.seconds_per_wakeup


def binds_of_source_queue(channel: FakeChannel) -> int:
    return channel.bound_queues.count(BoundQueue("input-queue", "input-exchange"))


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
        "x-dead-letter-exchange": "input-queue.dlx",
        "x-queue-type": "classic",
    }


def test_declare_topology_declares_every_queue_as_classic_by_default(config):
    channel = FakeChannel()
    executor = make_executor(config, channel, destination_exchange="output-exchange")

    executor.connect()

    assert [(q.queue, q.arguments["x-queue-type"]) for q in channel.declared_queues] == [
        ("input-queue", "classic"),
        ("input-queue.dlq", "classic"),
        ("input-queue.retry", "classic"),
        ("output-queue", "classic"),
    ]


def test_declare_topology_declares_every_queue_as_quorum_when_requested(config):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, destination_exchange="output-exchange", queue_type=QueueType.QUORUM
    )

    executor.connect()

    source_queue, dead_letter_queue, retry_queue, destination_queue = channel.declared_queues
    assert source_queue.queue == "input-queue"
    assert source_queue.durable is True
    assert source_queue.arguments == {
        "x-dead-letter-exchange": "input-queue.dlx",
        "x-queue-type": "quorum",
        # Dead-lettered at least once: RabbitMQ keeps a message no queue takes until one does
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }
    assert dead_letter_queue.queue == "input-queue.dlq"
    assert dead_letter_queue.durable is True
    assert dead_letter_queue.arguments == {"x-queue-type": "quorum"}
    assert retry_queue.queue == "input-queue.retry"
    assert retry_queue.durable is True
    assert retry_queue.arguments == {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "input-queue",
        "x-queue-type": "quorum",
        # A quorum queue can dead-letter at least once, which needs reject-publish
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }
    assert destination_queue.queue == "output-queue"
    assert destination_queue.durable is True
    # Exactly as its own executor declares it
    assert destination_queue.arguments == {
        "x-dead-letter-exchange": "output-queue.dlx",
        "x-queue-type": "quorum",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }


def test_declare_topology_declares_and_binds_dead_letter_queue(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    dead_letter_exchange = next(
        e for e in channel.declared_exchanges if e.exchange == "input-queue.dlx"
    )
    assert dead_letter_exchange.exchange_type == "fanout"
    assert dead_letter_exchange.durable is True
    dead_letter_queue = next(q for q in channel.declared_queues if q.queue == "input-queue.dlq")
    assert dead_letter_queue.durable is True
    assert BoundQueue(queue="input-queue.dlq", exchange="input-queue.dlx") in channel.bound_queues


def test_declare_topology_declares_the_retry_queue_returning_to_the_source_queue(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    retry_queue = next(q for q in channel.declared_queues if q.queue == "input-queue.retry")
    assert retry_queue.durable is True
    # Expired messages go through the default exchange, which routes by queue name
    assert retry_queue.arguments == {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "input-queue",
        "x-queue-type": "classic",
    }
    assert not [bound for bound in channel.bound_queues if bound.queue == "input-queue.retry"]


def test_declare_topology_declares_and_binds_every_destination_queue(config):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        destination_exchange="output-exchange",
        destination_queues=["result-a", "result-b"],
    )

    executor.connect()

    assert [queue.queue for queue in channel.declared_queues] == [
        "input-queue",
        "input-queue.dlq",
        "input-queue.retry",
        "result-a",
        "result-b",
    ]
    # A destination queue is declared exactly as its own Executor declares it
    assert channel.declared_queues[4].arguments == {
        "x-dead-letter-exchange": "result-b.dlx",
        "x-queue-type": "classic",
    }
    assert [exchange.exchange for exchange in channel.declared_exchanges] == [
        "input-exchange",
        "input-queue.dlx",
        "output-exchange",
    ]
    assert channel.bound_queues == [
        BoundQueue(queue="input-queue", exchange="input-exchange"),
        BoundQueue(queue="input-queue.dlq", exchange="input-queue.dlx"),
        BoundQueue(queue="result-a", exchange="output-exchange"),
        BoundQueue(queue="result-b", exchange="output-exchange"),
    ]


def test_declare_topology_in_terminator_mode_binds_source_queue(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)

    executor.connect()

    assert [queue.queue for queue in channel.declared_queues] == [
        "input-queue",
        "input-queue.dlq",
        "input-queue.retry",
    ]
    assert [exchange.exchange for exchange in channel.declared_exchanges] == [
        "input-exchange",
        "input-queue.dlx",
    ]
    assert channel.bound_queues[0] == BoundQueue(queue="input-queue", exchange="input-exchange")


def test_run_verifies_the_input_side_after_each_message(config):
    channel = DeclarationRecordingChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(
        SampleMessage(message_id="first", text="first").to_bytes(),
        SampleMessage(message_id="second", text="second").to_bytes(),
    )

    run_until_stopped(executor)

    assert channel.operations == [*INPUT_SIDE, "ack 1", *INPUT_SIDE, "ack 2", *INPUT_SIDE]


def test_run_verifies_the_output_side_right_before_publishing_results(config):
    channel = DeclarationRecordingChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=DeclarationRecordingChannel(),
        destination_exchange="output-exchange",
        destination_queues=["result-a", "result-b"],
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert channel.operations == [
        *INPUT_SIDE,
        *OUTPUT_SIDE,  # on connecting
        *OUTPUT_SIDE,  # on the results channel, before publishing
        "publish output-exchange",
        "commit",
        "ack 1",
        *INPUT_SIDE,
    ]
    assert channel.results_channel is not None
    assert channel.results_channel.bound_queues == [
        BoundQueue("result-a", "output-exchange"),
        BoundQueue("result-b", "output-exchange"),
    ]


def test_run_does_not_verify_the_output_side_without_results(config):
    channel = DeclarationRecordingChannel()
    executor = make_executor(
        config,
        channel,
        destination_exchange="output-exchange",
        destination_queues=["result-a", "result-b"],
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert channel.operations == [*INPUT_SIDE, *OUTPUT_SIDE, "ack 1", *INPUT_SIDE]


@pytest.mark.parametrize(
    ("body", "handler"),
    [
        (b"not json", None),
        (SampleMessage(message_id="input", text="input").to_bytes(), lambda _m: 1 / 0),
    ],
    ids=["dead-letter", "unexpected-error"],
)
def test_run_declares_the_dead_letter_route_right_before_dead_lettering(config, body, handler):
    channel = DeclarationRecordingChannel()
    executor = make_executor(config, channel, handler=handler)
    channel.queue_deliveries(body)

    with pytest.raises((StopConsuming, ZeroDivisionError)):
        executor.run()

    assert channel.operations[: len(INPUT_SIDE) + len(DEAD_LETTER_ROUTE) + 2] == [
        *INPUT_SIDE,
        *DEAD_LETTER_ROUTE,
        "publish input-queue.dlx",
        "ack 1",
    ]


def test_run_declares_the_retry_queue_right_before_moving_a_message_to_it(config, sleeps):
    channel = DeclarationRecordingChannel()
    executor = make_executor(config, channel, handler=always_transient)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert channel.operations == [
        *INPUT_SIDE,
        "queue input-queue.retry",
        "publish ",  # to the default exchange, which routes by queue name
        "ack 1",
        *INPUT_SIDE,
    ]


def test_run_verifies_the_input_side_every_input_check_seconds_while_waiting(config, monkeypatch):
    clock = FakeMonotonicClock(seconds_per_wakeup=4)
    monkeypatch.setattr("pykicak.executor.time.monotonic", clock)
    channel = FakeChannel()
    executor = make_executor(config, channel, input_check_seconds=10)
    wakeups = 0

    def on_idle() -> None:
        nonlocal wakeups
        wakeups += 1
        clock.advance()
        if wakeups == 7:
            executor.stop()

    channel.on_idle = on_idle

    executor.run()

    # On connecting (t=0), then at the first wakeups at least 10 s later: t=12 and t=24
    assert binds_of_source_queue(channel) == 3


def test_run_verifies_the_input_side_on_every_wakeup_with_zero_interval(config):
    channel = FakeChannel()
    executor = make_executor(config, channel, input_check_seconds=0)
    wakeups = 0

    def on_idle() -> None:
        nonlocal wakeups
        wakeups += 1
        if wakeups == 4:
            executor.stop()  # no verification after the stop request

    channel.on_idle = on_idle

    executor.run()

    assert binds_of_source_queue(channel) == 1 + 3


def test_run_raises_when_rabbitmq_refuses_to_verify_the_input_side(config, caplog):
    refused = ChannelClosedByBroker(403, "ACCESS_REFUSED - access to exchange refused")
    channel = BindFailingChannel(3, refused)
    channel.queue_deliveries(SampleMessage(message_id="first", text="first").to_bytes())
    executor = make_executor(config, channel)

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert channel.acked_delivery_tags == [1]
    assert executor.get_info().status == ExecutorStatus.CRASHED
    errors = [r for r in caplog.records if "Verifying the input side" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]


def test_run_raises_when_rabbitmq_refuses_to_verify_the_output_side(config, caplog):
    class RefusingResultsChannel(FakeChannel):
        def queue_declare(
            self, queue, durable=False, exclusive=False, auto_delete=False, arguments=None
        ):
            self.is_open = False
            raise ChannelClosedByBroker(
                406, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type' for queue"
            )

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=RefusingResultsChannel(),
        destination_exchange="output-exchange",
    )

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert channel.published_messages == []
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues the input
    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert "could not be published to exchange output-exchange" in caplog.text
