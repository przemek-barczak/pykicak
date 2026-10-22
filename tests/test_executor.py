import dataclasses
import os
import signal
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict
from unittest import mock

import pika
import pika.channel
import pytest
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    NackError,
    StreamLostError,
    UnroutableError,
)

import pykicak.executor as executor_module
from pykicak.config import KicakConfig
from pykicak.executor import (
    DeadLetterCounts,
    ExecutorCounts,
    ExecutorInfo,
    ExecutorStatus,
    KicakExecutorAbstract,
    MalformedMessageError,
    QueueType,
    TransientProcessingError,
)
from pykicak.messages import KicakMessage
from tests.fakes import (
    CONNECTION_VALUES,
    SAMPLE_TYPE,
    BoundQueue,
    FakeChannel,
    FakeConnection,
    NackedDelivery,
    PublishedMessage,
    StopConsuming,
    captured_connection_parameters,
)


@dataclasses.dataclass(frozen=True, slots=True)
class SampleMessage(KicakMessage):
    kicak_type = SAMPLE_TYPE
    text: str


class InvalidTextError(Exception):
    """An application's own validation error: neither ValueError nor TypeError."""


@dataclasses.dataclass(frozen=True, slots=True)
class ValidatedMessage(KicakMessage):
    kicak_type = "validated.v1"
    text: str

    def __post_init__(self) -> None:
        KicakMessage.__post_init__(self)
        if self.text == "invalid":
            raise InvalidTextError("text must not be 'invalid'")


class Topology(TypedDict):
    source_queue: str
    source_exchange: str


TOPOLOGY: Topology = {"source_queue": "input-queue", "source_exchange": "input-exchange"}

DESTINATION_QUEUES = ("output-queue",)
"""Default `destination_queues` of make_executor() when a test gives a destination_exchange."""


class ProcessingExecutor[M: KicakMessage, R: KicakMessage | None](KicakExecutorAbstract[M, R]):
    """Records executed messages; `handler`, if given, computes execute()'s return value.

    Generic, like the Executor: its subclasses declare the message types.
    """

    def __init__(
        self,
        config: KicakConfig,
        channel: FakeChannel,
        handler=None,
        results_channel: FakeChannel | None = None,
        connection_class: type[FakeConnection] = FakeConnection,
        **topology,
    ) -> None:
        super().__init__(config, **topology)
        self._fake_channel = channel
        self._fake_results_channel = results_channel
        self._connection_class = connection_class
        self._handler = handler
        self.processed_messages: list[M] = []

    def _create_connection(self):
        return self._connection_class(self._fake_channel, self._fake_results_channel)

    def execute(self, message: M):
        self.processed_messages.append(message)
        if self._handler is None:
            return None
        return self._handler(message)


class SampleExecutor(ProcessingExecutor[SampleMessage, SampleMessage]):
    """Accepts SampleMessage and publishes SampleMessage results."""


class SampleTerminator(ProcessingExecutor[SampleMessage, None]):
    """Accepts SampleMessage and publishes nothing."""


def make_executor(
    config,
    channel,
    handler=None,
    results_channel=None,
    connection_class=FakeConnection,
    **overrides,
) -> ProcessingExecutor[Any, Any]:
    """Create a SampleExecutor with a destination exchange, and a SampleTerminator without."""
    topology = {**TOPOLOGY, **overrides}
    executor_class: type[ProcessingExecutor[Any, Any]] = SampleTerminator
    if topology.get("destination_exchange") is not None:
        topology.setdefault("destination_queues", DESTINATION_QUEUES)
        executor_class = SampleExecutor
    return executor_class(config, channel, handler, results_channel, connection_class, **topology)


def run_until_stopped(executor: KicakExecutorAbstract[Any, Any]) -> None:
    """Run until FakeChannel.consume() runs out of deliveries and signals the external stop."""
    with pytest.raises(StopConsuming):
        executor.run()


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values=CONNECTION_VALUES)


@pytest.fixture
def sleeps(monkeypatch) -> list[float]:
    delays: list[float] = []
    monkeypatch.setattr("pykicak.executor.time.sleep", delays.append)
    return delays


# --- construction ---------------------------------------------------------------------------


def test_init_stores_source_queue_and_exchange(config):
    executor = make_executor(config, FakeChannel())

    assert executor._source_queue == "input-queue"
    assert executor._source_exchange == "input-exchange"


def test_init_derives_dead_letter_names_from_the_source_queue(config):
    executor = make_executor(config, FakeChannel())

    assert executor._dead_letter_exchange == "input-queue.dlx"
    assert executor._dead_letter_queue == "input-queue.dlq"


def test_init_stores_destination_exchange_and_queues(config):
    executor = make_executor(
        config,
        FakeChannel(),
        destination_exchange="output-exchange",
        destination_queues=["result-a", "result-b"],
    )

    assert executor._destination_exchange == "output-exchange"
    assert executor._destination_queues == ("result-a", "result-b")


def test_init_allows_terminator_mode_without_destination(config):
    executor = make_executor(config, FakeChannel())

    assert executor._source_exchange == "input-exchange"
    assert executor._destination_exchange is None
    assert executor._destination_queues == ()


def test_init_rejects_empty_source_exchange(config):
    with pytest.raises(ValueError, match="source_exchange must be a non-empty"):
        make_executor(config, FakeChannel(), source_exchange="")


@pytest.mark.parametrize("name", ["source_queue", "destination_exchange"])
def test_init_rejects_empty_topology_names(config, name):
    with pytest.raises(ValueError, match=f"{name} must be a non-empty"):
        make_executor(config, FakeChannel(), **{name: ""})


def test_init_requires_source_exchange_argument(config):
    with pytest.raises(TypeError, match="source_exchange"):
        ProcessingExecutor(config, FakeChannel(), source_queue="input-queue")


def test_init_requires_destination_queues_with_destination_exchange(config):
    with pytest.raises(ValueError, match="destination_queues is required"):
        make_executor(
            config, FakeChannel(), destination_exchange="output-exchange", destination_queues=None
        )


def test_init_rejects_destination_queues_without_destination_exchange(config):
    with pytest.raises(ValueError, match="destination_queues requires destination_exchange"):
        make_executor(config, FakeChannel(), destination_queues=["output-queue"])


@pytest.mark.parametrize(
    ("destination_queues", "error", "reason"),
    [
        ([], ValueError, "at least one queue"),
        ((), ValueError, "at least one queue"),
        (["output-queue", ""], ValueError, r"destination_queues\[1\] must be a non-empty name"),
        (["output-queue", "output-queue"], ValueError, "duplicate names"),
        ("output-queue", TypeError, "not a str"),
        ([b"output-queue"], TypeError, r"destination_queues\[0\] must be a str, got bytes"),
    ],
    ids=["empty-list", "empty-tuple", "empty-name", "duplicate", "str", "bytes"],
)
def test_init_rejects_invalid_destination_queues(config, destination_queues, error, reason):
    with pytest.raises(error, match=reason):
        make_executor(
            config,
            FakeChannel(),
            destination_exchange="output-exchange",
            destination_queues=destination_queues,
        )


PUBLISHING_TOPOLOGY = {
    "source_queue": "input-queue",
    "source_exchange": "input-exchange",
    "destination_exchange": "output-exchange",
    "destination_queues": ["output-queue"],
}


def with_name(argument: str, name: object) -> dict[str, object]:
    """PUBLISHING_TOPOLOGY with `name` as `argument`, or as the only destination queue."""
    if argument == "destination_queues":
        return {**PUBLISHING_TOPOLOGY, "destination_queues": [name]}
    return {**PUBLISHING_TOPOLOGY, argument: name}


QUEUE_ARGUMENTS = ["source_queue", "destination_queues"]
EXCHANGE_ARGUMENTS = ["source_exchange", "destination_exchange"]


@pytest.mark.parametrize("argument", QUEUE_ARGUMENTS)
@pytest.mark.parametrize("name", ["q" * 249, "ł" * 124 + "q"], ids=["ascii", "two-byte"])
def test_init_accepts_queue_names_of_249_bytes(config, argument, name):
    make_executor(config, FakeChannel(), **with_name(argument, name))


@pytest.mark.parametrize("argument", QUEUE_ARGUMENTS)
@pytest.mark.parametrize("name", ["q" * 250, "ł" * 125], ids=["ascii", "two-byte"])
def test_init_rejects_queue_names_leaving_no_room_for_the_retry_queue(config, argument, name):
    # <queue>.retry, the longest name derived from a queue, must fit AMQP's 255 bytes
    with pytest.raises(ValueError, match="at most 249 bytes in UTF-8, leaving room for .*retry"):
        make_executor(config, FakeChannel(), **with_name(argument, name))


@pytest.mark.parametrize("argument", EXCHANGE_ARGUMENTS)
def test_init_accepts_exchange_names_of_255_bytes(config, argument):
    make_executor(config, FakeChannel(), **with_name(argument, "x" * 255))


@pytest.mark.parametrize("argument", EXCHANGE_ARGUMENTS)
def test_init_rejects_exchange_names_longer_than_255_bytes(config, argument):
    with pytest.raises(ValueError, match="at most 255 bytes in UTF-8$"):
        make_executor(config, FakeChannel(), **with_name(argument, "x" * 256))


@pytest.mark.parametrize("argument", QUEUE_ARGUMENTS + EXCHANGE_ARGUMENTS)
def test_init_rejects_names_with_the_prefix_rabbitmq_reserves(config, argument):
    with pytest.raises(ValueError, match="must not start with 'amq.', which RabbitMQ reserves"):
        make_executor(config, FakeChannel(), **with_name(argument, "amq.name"))


@pytest.mark.parametrize("argument", QUEUE_ARGUMENTS + EXCHANGE_ARGUMENTS)
def test_init_accepts_the_reserved_prefix_in_another_letter_case(config, argument):
    # RabbitMQ reserves only the lower-case prefix (verified on 4.2.5)
    make_executor(config, FakeChannel(), **with_name(argument, "AMQ.name"))


@pytest.mark.parametrize("argument", QUEUE_ARGUMENTS + EXCHANGE_ARGUMENTS)
@pytest.mark.parametrize("name", [42, b"name"], ids=["int", "bytes"])
def test_init_rejects_names_that_are_not_str(config, argument, name):
    with pytest.raises(TypeError, match=r"must be a str, got"):
        make_executor(config, FakeChannel(), **with_name(argument, name))


@pytest.mark.parametrize("argument", ["source_queue", "source_exchange"])
def test_init_rejects_a_missing_source_name(config, argument):
    with pytest.raises(TypeError, match=f"{argument} must be a str, got NoneType"):
        make_executor(config, FakeChannel(), **{argument: None})


def test_subclass_must_implement_execute(config):
    class WithoutExecute(KicakExecutorAbstract[SampleMessage, None]):
        pass

    with pytest.raises(TypeError, match="execute"):
        WithoutExecute(config, **TOPOLOGY)  # type: ignore[abstract]


def test_init_requires_keyword_topology_arguments(config):
    executor = object.__new__(ProcessingExecutor)

    with pytest.raises(TypeError):
        KicakExecutorAbstract.__init__(executor, config, "q", "x")  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "topology",
    [
        {"destination_exchange": "input-exchange", "destination_queues": ["result"]},
        {
            "destination_exchange": "output-exchange",
            "destination_queues": ["result", "input-queue"],
        },
    ],
    ids=["destination-exchange-is-source-exchange", "destination-queues-include-source-queue"],
)
def test_init_rejects_a_self_loop(config, topology):
    with pytest.raises(ValueError, match="a self-loop"):
        make_executor(config, FakeChannel(), **topology)


def test_init_accepts_a_loop_through_another_executor(config):
    # The downstream Executor sends the results back upstream, explicitly configured for it
    make_executor(
        config,
        FakeChannel(),
        destination_exchange="output-exchange",
        destination_queues=["downstream-queue"],
    )
    downstream = make_executor(
        config,
        FakeChannel(),
        source_queue="downstream-queue",
        source_exchange="output-exchange",
        destination_exchange="input-exchange",
        destination_queues=["input-queue"],
    )

    assert downstream._destination_queues == ("input-queue",)


@pytest.mark.parametrize("reused", ["source_exchange", "destination_exchange"])
def test_init_rejects_dead_letter_exchange_reused_for_message_flow(config, reused):
    topology = {"destination_exchange": "output-exchange", reused: "input-queue.dlx"}

    with pytest.raises(ValueError, match="must differ from 'input-queue.dlx'"):
        make_executor(config, FakeChannel(), **topology)


@pytest.mark.parametrize(
    "topology",
    [
        {"destination_queues": ["input-queue.dlq"]},
        {"destination_queues": ["result", "result.dlq"]},
        {"source_queue": "result.dlq", "destination_queues": ["result"]},
    ],
    ids=["own-dead-letter-queue", "destination-dead-letter-queue", "source-is-dead-letter-queue"],
)
def test_init_rejects_a_dead_letter_queue_as_input_queue(config, topology):
    with pytest.raises(ValueError, match="must not include the dead-letter queue"):
        make_executor(config, FakeChannel(), destination_exchange="output-exchange", **topology)


@pytest.mark.parametrize(
    "topology",
    [
        {"destination_queues": ["input-queue.retry"]},
        {"destination_queues": ["result", "result.retry"]},
        {"source_queue": "result.retry", "destination_queues": ["result"]},
    ],
    ids=["own-retry-queue", "destination-retry-queue", "source-is-retry-queue"],
)
def test_init_rejects_a_retry_queue_as_input_queue(config, topology):
    with pytest.raises(ValueError, match="must not include the retry queue"):
        make_executor(config, FakeChannel(), destination_exchange="output-exchange", **topology)


def test_init_rejects_destination_exchange_that_dead_letters_a_destination_queue(config):
    with pytest.raises(ValueError, match="must not be the dead-letter exchange"):
        make_executor(
            config,
            FakeChannel(),
            destination_exchange="result.dlx",
            destination_queues=["result"],
        )


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


def test_init_checks_the_input_side_every_ten_seconds_by_default(config):
    executor = make_executor(config, FakeChannel())

    assert executor._input_check_seconds == 10.0


@pytest.mark.parametrize("input_check_seconds", [0, 0.5, 60])
def test_init_accepts_input_check_interval(config, input_check_seconds):
    executor = make_executor(config, FakeChannel(), input_check_seconds=input_check_seconds)

    assert executor._input_check_seconds == input_check_seconds


@pytest.mark.parametrize("input_check_seconds", [-1, -0.5, float("inf"), float("nan")])
def test_init_rejects_negative_or_infinite_input_check_interval(config, input_check_seconds):
    with pytest.raises(ValueError, match="input_check_seconds must be a finite number"):
        make_executor(config, FakeChannel(), input_check_seconds=input_check_seconds)


@pytest.mark.parametrize("input_check_seconds", ["10", None, True])
def test_init_rejects_non_numeric_input_check_interval(config, input_check_seconds):
    with pytest.raises(TypeError, match="input_check_seconds must be a number"):
        make_executor(config, FakeChannel(), input_check_seconds=input_check_seconds)


def test_init_delays_retries_by_ten_seconds_by_default(config):
    executor = make_executor(config, FakeChannel())

    assert executor._retry_delay_milliseconds == 10_000


@pytest.mark.parametrize(
    ("retry_delay_seconds", "milliseconds"),
    [(0, 0), (0.25, 250), (60, 60_000), (4294967.295, 2**32 - 1)],
)
def test_init_accepts_retry_delay(config, retry_delay_seconds, milliseconds):
    executor = make_executor(config, FakeChannel(), retry_delay_seconds=retry_delay_seconds)

    assert executor._retry_delay_milliseconds == milliseconds


@pytest.mark.parametrize("retry_delay_seconds", [-1, float("inf"), float("nan")])
def test_init_rejects_negative_or_infinite_retry_delay(config, retry_delay_seconds):
    with pytest.raises(ValueError, match="retry_delay_seconds must be a finite number"):
        make_executor(config, FakeChannel(), retry_delay_seconds=retry_delay_seconds)


def test_init_rejects_retry_delay_longer_than_rabbitmq_takes(config):
    with pytest.raises(ValueError, match="retry_delay_seconds must be at most 4294967.295"):
        make_executor(config, FakeChannel(), retry_delay_seconds=4294967.296)


@pytest.mark.parametrize("retry_delay_seconds", ["10", None, True])
def test_init_rejects_non_numeric_retry_delay(config, retry_delay_seconds):
    with pytest.raises(TypeError, match="retry_delay_seconds must be a number"):
        make_executor(config, FakeChannel(), retry_delay_seconds=retry_delay_seconds)


def test_init_rejects_non_positive_max_processing_attempts(config):
    with pytest.raises(ValueError, match="max_processing_attempts"):
        make_executor(config, FakeChannel(), max_processing_attempts=0)


@pytest.mark.parametrize("max_processing_attempts", [2.5, True, "3", None])
def test_init_rejects_non_integer_max_processing_attempts(config, max_processing_attempts):
    with pytest.raises(TypeError, match="max_processing_attempts must be an int"):
        make_executor(config, FakeChannel(), max_processing_attempts=max_processing_attempts)


def test_subclass_parameterized_with_its_message_type_receives_that_type(config):
    class TypedExecutor(KicakExecutorAbstract[SampleMessage, None]):
        def __init__(self, channel: FakeChannel) -> None:
            super().__init__(config, **TOPOLOGY)
            self._fake_channel = channel
            self.texts: list[str] = []

        def _create_connection(self):
            return FakeConnection(self._fake_channel)

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


@pytest.mark.parametrize("heartbeat_seconds", [1.5, "600", True, None])
def test_init_rejects_non_integer_heartbeat(config, heartbeat_seconds):
    # None would accept the broker's proposal, 60 seconds by default: too short for execute()
    with pytest.raises(TypeError, match="heartbeat_seconds must be an int$"):
        make_executor(config, FakeChannel(), heartbeat_seconds=heartbeat_seconds)


@pytest.mark.parametrize(
    ("destination_exchange", "role"),
    [("output-exchange", "executor"), (None, "terminator")],
)
def test_connection_is_named_by_role_queue_host_and_pid(monkeypatch, destination_exchange, role):
    monkeypatch.setattr("socket.gethostname", lambda: "worker-3")
    monkeypatch.setattr("os.getpid", lambda: 4242)
    executor = make_executor(
        KicakConfig(values=CONNECTION_VALUES),
        FakeChannel(),
        destination_exchange=destination_exchange,
    )

    parameters = captured_connection_parameters(executor, monkeypatch)

    assert parameters.client_properties == {
        "connection_name": f"{role} queue=input-queue host=worker-3 pid=4242"
    }


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
    assert destination_queue.arguments == {
        "x-dead-letter-exchange": "output-queue.dlx",
        "x-queue-type": "quorum",
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


# --- topology verification ------------------------------------------------------------------


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


class BindFailingChannel(FakeChannel):
    """Consuming channel whose queue_bind fails with `error` on call number `failing_call`."""

    def __init__(self, failing_call: int, error: Exception) -> None:
        super().__init__()
        self._failing_call = failing_call
        self._error = error
        self._calls = 0

    def queue_bind(self, queue: str, exchange: str) -> None:
        self._calls += 1
        if self._calls == self._failing_call:
            self.is_open = False
            raise self._error
        super().queue_bind(queue, exchange)


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


def dead_letter_copies(channel: FakeChannel) -> list[PublishedMessage]:
    """The messages published to the dead-letter exchange of the default TOPOLOGY."""
    return [
        published
        for published in channel.published_messages
        if published.exchange == "input-queue.dlx"
    ]


def dead_lettered(channel: FakeChannel) -> list[int]:
    """The delivery tags acknowledged right after a copy went to the dead-letter exchange.

    The executor dead-letters a delivery by publishing a copy of it to the dead-letter exchange of
    the default TOPOLOGY and acknowledging it once RabbitMQ has confirmed the copy.
    """
    operations = channel.operations
    return [
        int(operation.removeprefix("ack "))
        for previous, operation in zip(operations, operations[1:], strict=False)
        if previous == "publish input-queue.dlx" and operation.startswith("ack ")
    ]


def test_run_dead_letters_undecodable_message_and_continues(config, caplog):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    valid = SampleMessage(message_id="valid", text="valid")
    channel.queue_deliveries(
        b"not json", properties=pika.BasicProperties(type=SAMPLE_TYPE, message_id="sent-id")
    )
    channel.queue_deliveries(valid.to_bytes())

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [valid]
    dead_letter_records = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert len(dead_letter_records) == 1
    assert dead_letter_records[0].levelname == "ERROR"
    # The body names no ID, so the sender's message_id property does
    assert "type=SampleMessage id=sent-id" in dead_letter_records[0].message
    assert "dead_letter_exchange=input-queue.dlx" in dead_letter_records[0].message
    assert "Could not decode message as SampleMessage" in dead_letter_records[0].message
    # Logged once, by the Executor: the serialization code logs it at DEBUG only
    assert not [
        r for r in caplog.records if r.name == "pykicak.messages" and r.levelname == "ERROR"
    ]


@pytest.mark.parametrize(
    ("body", "properties", "logged_id"),
    [
        (b"not json", pika.BasicProperties(type=SAMPLE_TYPE), "None"),
        (b"not json", None, "None"),
        (b"not json", pika.BasicProperties(type=SAMPLE_TYPE, message_id=""), "None"),
        (b"not json", pika.BasicProperties(type=SAMPLE_TYPE, message_id=b"bytes-id"), "bytes-id"),
        (None, pika.BasicProperties(type=SAMPLE_TYPE, message_id="no-body"), "no-body"),
    ],
    ids=["no-message-id", "no-properties", "empty-message-id", "bytes-message-id", "no-body"],
)
def test_undecodable_delivery_is_logged_with_the_sent_message_id(
    config, caplog, body, properties, logged_id
):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(body, properties=properties)

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    [record] = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert f"type=SampleMessage id={logged_id} queue=" in record.message


def test_decoded_delivery_is_logged_with_the_message_id_of_its_body(config, caplog):
    def refuse(message):
        raise MalformedMessageError("refused by the application")

    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=refuse)
    channel.queue_deliveries(
        SampleMessage(message_id="body-id", text="body").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, message_id="property-id"),
    )

    run_until_stopped(executor)

    assert "Message received type=SampleMessage id=body-id" in caplog.text
    assert "Message dead-lettered type=SampleMessage id=body-id" in caplog.text
    assert "property-id" not in caplog.text


def test_run_dead_letters_json_nested_too_deeply_and_continues(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    valid = SampleMessage(message_id="valid", text="valid")
    depth = 1_000_000  # beyond the JSON decoder's recursion limit
    channel.queue_deliveries(b"[" * depth + b"]" * depth, valid.to_bytes())

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [valid]


def test_run_dead_letters_message_its_class_refuses_with_any_exception_and_continues(
    config, caplog
):
    class ValidatingExecutor(ProcessingExecutor[ValidatedMessage, None]):
        pass

    channel = FakeChannel()
    executor = ValidatingExecutor(config, channel, **TOPOLOGY)
    valid = ValidatedMessage(message_id="valid", text="valid")
    channel.queue_deliveries(
        b'{"message_id": "bad", "text": "invalid"}',
        properties=pika.BasicProperties(type="validated.v1", message_id="bad"),
    )
    channel.queue_deliveries(valid.to_bytes(), properties=pika.BasicProperties(type="validated.v1"))

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [valid]
    assert "Message dead-lettered type=ValidatedMessage id=bad" in caplog.text
    assert "text must not be 'invalid'" in caplog.text


# --- message types: declared in the class header, sent as the type property ------------------


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkV1(KicakMessage):
    kicak_type = "chunk.v1"
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkV2(KicakMessage):
    kicak_type = "chunk.v2"
    text: str  # the same fields as ChunkV1: only the type property tells them apart


@dataclasses.dataclass(frozen=True, slots=True)
class UnnamedSample(SampleMessage):
    """Inherits SampleMessage's kicak_type without declaring its own."""


@dataclasses.dataclass(frozen=True, slots=True)
class RenamedSample(SampleMessage):
    kicak_type = "renamed-sample.v1"


@dataclasses.dataclass(frozen=True, slots=True)
class SameNameAsChunkV1(KicakMessage):
    kicak_type = "chunk.v1"
    text: str


class DeliveryArguments(TypedDict):
    properties: pika.BasicProperties


def delivery_of(message: KicakMessage) -> DeliveryArguments:
    """Keyword arguments of queue_deliveries() for `message`, sent as a pykicak agent sends it."""
    return {
        "properties": pika.BasicProperties(
            type=type(message).kicak_type, message_id=message.message_id
        )
    }


class Open[M: KicakMessage](ProcessingExecutor[M, None]):
    """A generic executor class that leaves its input type open."""


@pytest.mark.parametrize(
    ("declaration", "error", "match"),
    [
        (lambda: KicakExecutorAbstract, TypeError, "must declare its message types"),
        (lambda: ProcessingExecutor, TypeError, "must declare its message types"),
        (lambda: Open, TypeError, "must declare its message types"),
        (lambda: ProcessingExecutor[UnnamedSample, None], TypeError, "its own kicak_type"),
        (lambda: ProcessingExecutor[KicakMessage, None], TypeError, "KicakMessage subclass"),
        (
            lambda: ProcessingExecutor[SampleMessage | None, None],  # type: ignore[type-var]
            TypeError,
            "not None",
        ),
        (
            lambda: ProcessingExecutor[ChunkV1 | SameNameAsChunkV1, None],
            TypeError,
            "ChunkV1 and SameNameAsChunkV1, which declare the same kicak_type 'chunk.v1'",
        ),
        (lambda: ProcessingExecutor[SampleMessage, ChunkV1 | ChunkV2], TypeError, "one result"),
        (lambda: ProcessingExecutor[SampleMessage, UnnamedSample], TypeError, "own kicak_type"),
    ],
    ids=[
        "unparameterized",
        "unparameterized-generic-subclass",
        "open-type-parameter",
        "inherited-type-name",
        "base-message-class",
        "none-accepted",
        "duplicate-type-name",
        "union-result",
        "result-without-type-name",
    ],
)
def test_init_rejects_an_invalid_declaration_of_message_types(config, declaration, error, match):
    base = declaration()

    class Declared(base):  # type: ignore[valid-type, misc]
        def execute(self, message):
            return None

    arguments = (config, FakeChannel()) if issubclass(Declared, ProcessingExecutor) else (config,)
    with pytest.raises(error, match=match):
        Declared(*arguments, **TOPOLOGY)


def test_init_requires_a_destination_for_a_declared_result_type(config):
    with pytest.raises(ValueError, match="declares its results as SampleMessage"):
        SampleExecutor(config, FakeChannel(), **TOPOLOGY)


def test_init_rejects_a_destination_for_a_terminator(config):
    with pytest.raises(ValueError, match="declares the result type None, so it is a terminator"):
        SampleTerminator(
            config,
            FakeChannel(),
            **TOPOLOGY,
            destination_exchange="output-exchange",
            destination_queues=DESTINATION_QUEUES,
        )


def test_message_types_are_resolved_through_subclasses_and_generic_classes(config):
    class Swapped[R: KicakMessage | None, M: KicakMessage](ProcessingExecutor[M, R]):
        pass

    class Concrete(Swapped[None, ChunkV1 | ChunkV2]):
        pass

    class WithOpenMember[M: KicakMessage](ProcessingExecutor[M | ChunkV1, None]):
        pass

    class Closed(WithOpenMember[ChunkV2]):
        pass

    class Inherited(SampleTerminator):
        pass

    assert Concrete(config, FakeChannel(), **TOPOLOGY)._accepted_types == {
        "chunk.v1": ChunkV1,
        "chunk.v2": ChunkV2,
    }
    assert set(Closed(config, FakeChannel(), **TOPOLOGY)._accepted_types.values()) == {
        ChunkV1,
        ChunkV2,
    }
    inherited = Inherited(config, FakeChannel(), **TOPOLOGY)
    assert inherited._accepted_types == {SAMPLE_TYPE: SampleMessage}
    assert inherited._result_type is None


type AnyChunk = ChunkV1 | ChunkV2
type OnlyV1 = ChunkV1
type WithSample[T] = T | SampleMessage
type NoResult = None


def test_message_types_are_resolved_through_type_statement_aliases(config):
    class AliasedUnion(ProcessingExecutor[AnyChunk, SampleMessage]):
        pass

    class AliasAsUnionMember(ProcessingExecutor[OnlyV1 | ChunkV2, None]):
        pass

    class GenericAlias(ProcessingExecutor[WithSample[ChunkV1], None]):
        pass

    class AliasedResults(ProcessingExecutor[SampleMessage, OnlyV1]):
        pass

    class AliasedTerminator(ProcessingExecutor[SampleMessage, NoResult]):
        pass

    publishing: dict[str, Any] = {
        "destination_exchange": "output-exchange",
        "destination_queues": ["output"],
    }
    aliased = AliasedUnion(config, FakeChannel(), **TOPOLOGY, **publishing)
    assert aliased._accepted_types == {"chunk.v1": ChunkV1, "chunk.v2": ChunkV2}
    assert aliased._result_type is SampleMessage
    member = AliasAsUnionMember(config, FakeChannel(), **TOPOLOGY)
    assert member._accepted_types == {"chunk.v1": ChunkV1, "chunk.v2": ChunkV2}
    generic = GenericAlias(config, FakeChannel(), **TOPOLOGY)
    assert generic._accepted_types == {"chunk.v1": ChunkV1, SAMPLE_TYPE: SampleMessage}
    results = AliasedResults(config, FakeChannel(), **TOPOLOGY, **publishing)
    assert results._result_type is ChunkV1
    assert AliasedTerminator(config, FakeChannel(), **TOPOLOGY)._result_type is None


class ChunkTerminator(ProcessingExecutor[ChunkV1 | ChunkV2, None]):
    """Accepts both versions of a chunk, e.g. during an upgrade of its producer."""


def test_run_decodes_each_delivery_into_the_version_its_type_property_names(config):
    channel = FakeChannel()
    executor = ChunkTerminator(config, channel, **TOPOLOGY)
    old = ChunkV1(message_id="old", text="same fields")
    new = ChunkV2(message_id="new", text="same fields")
    channel.queue_deliveries(old.to_bytes(), **delivery_of(old))
    channel.queue_deliveries(new.to_bytes(), **delivery_of(new))

    run_until_stopped(executor)

    # The bodies are alike; the type property alone decides the class
    assert old.to_bytes().replace(b"old", b"new") == new.to_bytes()
    assert [type(message) for message in executor.processed_messages] == [ChunkV1, ChunkV2]
    assert executor.processed_messages == [old, new]
    assert channel.acked_delivery_tags == [1, 2]


def test_run_decodes_a_type_property_delivered_as_bytes(config):
    channel = FakeChannel()
    executor = ChunkTerminator(config, channel, **TOPOLOGY)
    chunk = ChunkV2(message_id="bytes-type", text="text")
    channel.queue_deliveries(chunk.to_bytes(), properties=pika.BasicProperties(type=b"chunk.v2"))

    run_until_stopped(executor)

    assert executor.processed_messages == [chunk]


@pytest.mark.parametrize(
    ("properties", "reason"),
    [
        (
            pika.BasicProperties(type="chunk.v3", message_id="unaccepted"),
            "Received a message of type 'chunk.v3', which is not accepted; "
            "accepted types: 'chunk.v1', 'chunk.v2'",
        ),
        (
            pika.BasicProperties(type=SAMPLE_TYPE, message_id="unaccepted"),
            f"Received a message of type '{SAMPLE_TYPE}', which is not accepted",
        ),
        (
            pika.BasicProperties(message_id="unaccepted"),
            "Received a message without a type property; accepted types: 'chunk.v1', 'chunk.v2'",
        ),
        (
            pika.BasicProperties(type="", message_id="unaccepted"),
            "Received a message without a type property",
        ),
    ],
    ids=["unknown-version", "another-type", "no-type", "empty-type"],
)
def test_run_dead_letters_a_delivery_whose_type_is_not_accepted_and_continues(
    config, caplog, properties, reason
):
    channel = FakeChannel()
    executor = ChunkTerminator(config, channel, **TOPOLOGY)
    accepted = ChunkV1(message_id="accepted", text="text")
    # The body would decode into either version: without an accepted type it is still refused
    channel.queue_deliveries(
        ChunkV1(message_id="unaccepted", text="text").to_bytes(), properties=properties
    )
    channel.queue_deliveries(accepted.to_bytes(), **delivery_of(accepted))

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [accepted]
    [record] = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert record.levelname == "ERROR"
    assert "type=None id=unaccepted queue=input-queue" in record.message
    assert reason in record.message


def test_results_are_published_with_the_type_name_of_their_class(config):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda message: SampleMessage(message_id=f"{message.message_id}.result", text=""),
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    [published] = channel.published_messages
    assert published.properties.type == SAMPLE_TYPE
    assert published.properties.message_id == "input.result"


@pytest.mark.parametrize(
    "result",
    [
        RenamedSample(message_id="subclass", text="a subclass of the declared class"),
        ChunkV1(message_id="other", text="another class"),
    ],
    ids=["subclass", "other-class"],
)
def test_run_dead_letters_and_raises_when_a_result_is_not_of_the_declared_type(
    config, caplog, result
):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: [SampleMessage(message_id="valid", text="valid"), result],
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        SampleMessage(message_id="never-processed", text="never processed").to_bytes(),
    )

    with pytest.raises(TypeError, match="declares its results as SampleMessage"):
        executor.run()

    # A bug in the executor's own code: nothing of the input's results is published
    assert channel.published_messages == dead_letter_copies(channel)
    assert dead_lettered(channel) == [1]
    assert len(executor.processed_messages) == 1
    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert "failed unexpectedly type=SampleMessage id=input" in caplog.text
    assert f"result of type {type(result).__name__}" in caplog.text


def test_run_dead_letters_message_with_unexpected_fields(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(b'{"message_id": "hello", "text": "hello", "extra": true}')

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
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

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [SampleMessage(message_id="valid", text="valid")]


def test_run_dead_letters_message_without_body(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(None)

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert dead_letter_copies(channel)[0].body == b""


def test_run_dead_letters_message_rejected_by_execute_without_publishing(config):
    def reject(_message):
        raise MalformedMessageError("fails application validation")

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=reject, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="invalid", text="invalid").to_bytes())

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.published_messages == dead_letter_copies(channel)


# --- transient failures: retry, then the retry queue ----------------------------------------------


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


def unavailable(_message):
    raise TransientProcessingError("service unavailable")


def retry_copies(channel: FakeChannel) -> list[PublishedMessage]:
    """The messages published to the retry queue of the default TOPOLOGY."""
    return [
        published
        for published in channel.published_messages
        if published.exchange == "" and published.routing_key == "input-queue.retry"
    ]


def test_run_moves_message_to_retry_queue_after_exhausting_transient_retries(
    config, sleeps, clock, caplog
):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=unavailable, destination_exchange="output-exchange"
    )
    first = SampleMessage(message_id="input", text="input")
    later = SampleMessage(message_id="later", text="later")
    channel.queue_deliveries(first.to_bytes(), later.to_bytes())

    run_until_stopped(executor)

    assert sleeps == [0.5, 1.0, 0.5, 1.0]
    assert len(executor.processed_messages) == 6
    copies = retry_copies(channel)
    assert [copy.body for copy in copies] == [first.to_bytes(), later.to_bytes()]
    assert all(copy.mandatory for copy in copies)
    properties = copies[0].properties
    assert properties.expiration == "10000"  # retry_delay_seconds, in milliseconds
    assert properties.delivery_mode == 2
    assert properties.message_id == "input"
    assert properties.headers == {
        "x-kicak-retry-round": 1,
        "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
    }
    # Acknowledged after the copy, never requeued or dead-lettered
    assert channel.operations == ["publish ", "ack 1", "publish ", "ack 2"]
    assert channel.nacked_deliveries == []
    moved = [r for r in caplog.records if "Message moved to retry queue" in r.message]
    assert [record.levelname for record in moved] == ["WARNING", "WARNING"]
    assert "id=input queue=input-queue retry_queue=input-queue.retry retry_round=1" in (
        moved[0].message
    )


def test_run_moves_to_retry_queue_without_retrying_when_one_attempt_is_allowed(config, sleeps):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable, max_processing_attempts=1)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert sleeps == []
    assert len(retry_copies(channel)) == 1
    assert channel.acked_delivery_tags == [1]


@pytest.mark.parametrize(("retry_delay_seconds", "expiration"), [(2.5, "2500"), (0, "0")])
def test_retry_copy_expires_after_the_retry_delay(config, sleeps, retry_delay_seconds, expiration):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=unavailable, retry_delay_seconds=retry_delay_seconds
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert retry_copies(channel)[0].properties.expiration == expiration


def test_retry_copy_keeps_application_properties_but_not_rabbitmq_headers(config, sleeps, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    received = pika.BasicProperties(
        type=SAMPLE_TYPE,
        content_type="application/json",
        correlation_id="flow-7",
        message_id="input",
        user_id="publisher",
        expiration="5000",
        headers={
            "tenant": "acme",
            "x-death": [{"queue": "input-queue", "reason": "rejected", "count": 1}],
            "x-delivery-count": 3,
            "x-first-death-queue": "input-queue",
            "x-last-death-reason": "rejected",
        },
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=received
    )

    run_until_stopped(executor)

    copy = retry_copies(channel)[0].properties
    assert copy.content_type == "application/json"
    assert copy.type == SAMPLE_TYPE
    assert copy.correlation_id == "flow-7"
    assert copy.headers == {
        "tenant": "acme",
        "x-kicak-retry-round": 1,
        "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
        # The original user_id, which RabbitMQ would refuse from this executor's user
        "x-kicak-original-publisher": "publisher",
    }
    assert copy.user_id == "guest"  # the executor's own user, from CONNECTION_VALUES
    assert copy.expiration == "10000"


def test_retry_round_increases_and_keeps_the_time_of_the_first_round(config, sleeps, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    received = pika.BasicProperties(
        type=SAMPLE_TYPE,
        headers={
            "x-kicak-retry-round": 3,
            "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
        },
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=received
    )

    run_until_stopped(executor)

    assert retry_copies(channel)[0].properties.headers == {
        "x-kicak-retry-round": 4,
        "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"x-kicak-retry-round": "3"}, 1),
        ({"x-kicak-retry-round": -1}, 1),
        ({"x-kicak-retry-round": True}, 1),
        ({"x-kicak-retrying-since": "yesterday"}, "2026-10-01T12:00:00+00:00"),
        ({"x-kicak-retrying-since": "2026-09-30T08:00:00"}, "2026-10-01T12:00:00+00:00"),
        ({"x-kicak-retrying-since": 42}, "2026-10-01T12:00:00+00:00"),
        ({"x-kicak-retrying-since": b"2026-09-30T08:00:00+00:00"}, "2026-09-30T08:00:00+00:00"),
    ],
    ids=[
        "round-text",
        "round-negative",
        "round-bool",
        "since-not-a-time",
        "since-without-zone",
        "since-number",
        "since-bytes",
    ],
)
def test_invalid_retry_headers_start_again(config, sleeps, clock, headers, expected):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, headers=headers),
    )

    run_until_stopped(executor)

    copy_headers = retry_copies(channel)[0].properties.headers
    name = next(iter(headers))
    assert copy_headers[name] == expected


class CopyRefusingChannel(FakeChannel):
    """Consuming channel (confirm mode) whose publishes of copies raise `error`.

    The copies to `target`: the retry queue of the default TOPOLOGY, routed by its name through
    the default exchange, or its dead-letter exchange.
    """

    def __init__(self, error: Exception, target: str = "input-queue.retry") -> None:
        super().__init__()
        self._error = error
        self._target = target

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        if self._target in (exchange, routing_key):
            if isinstance(self._error, StreamLostError):
                self.is_open = False
            raise self._error
        super().basic_publish(exchange, routing_key, body, properties, mandatory)


@pytest.mark.parametrize(
    "error",
    [UnroutableError([]), NackError([])],
    ids=["unroutable", "refused"],
)
def test_run_requeues_the_message_when_rabbitmq_does_not_take_its_retry_copy(
    config, sleeps, caplog, error
):
    channel = CopyRefusingChannel(error)
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)  # continues with the next message

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert channel.acked_delivery_tags == []
    assert executor.get_info().counts == ExecutorCounts(requeued=1)
    errors = [r for r in caplog.records if "could not be moved to retry queue" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]


def test_run_reconnects_when_the_connection_is_lost_while_moving_to_the_retry_queue(
    config, monkeypatch, sleeps
):
    body = SampleMessage(message_id="input", text="input").to_bytes()
    original_channel = CopyRefusingChannel(StreamLostError("connection lost"))
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel, handler=unavailable)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    assert len(retry_copies(replacement_channel)) == 1
    assert replacement_channel.acked_delivery_tags == [1]


def test_run_moves_no_copy_to_the_retry_queue_when_rabbitmq_cancelled_the_consumer(
    config, monkeypatch, sleeps, caplog
):
    # Transient retries outlasted a quorum queue's consumer timeout on RabbitMQ 4.3+, which has
    # returned the message to the queue: a copy in the retry queue would execute it twice
    body = SampleMessage(message_id="input", text="input").to_bytes()
    original_channel = FakeChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery

    cancelled = False

    def unavailable_beyond_the_consumer_timeout(message):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            original_channel.cancel_consumer()
        unavailable(message)

    executor = make_executor(
        config, original_channel, handler=unavailable_beyond_the_consumer_timeout
    )
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert retry_copies(original_channel) == []
    assert original_channel.acked_delivery_tags == []
    assert len(retry_copies(replacement_channel)) == 1
    assert replacement_channel.acked_delivery_tags == [1]
    assert "Connection or channel lost while moving message id=input" in caplog.text


def test_run_stops_when_rabbitmq_refuses_to_declare_the_retry_queue(config, sleeps):
    class RefusingRetryQueueChannel(FakeChannel):
        """Refuses the retry queue after the declarations of the first connection."""

        def queue_declare(
            self, queue, durable=False, exclusive=False, auto_delete=False, arguments=None
        ):
            if (
                queue == "input-queue.retry"
                and self.acked_delivery_tags == []
                and any(q.queue == "input-queue.retry" for q in self.declared_queues)
            ):
                self.is_open = False
                raise ChannelClosedByBroker(406, "PRECONDITION_FAILED - inequivalent arg")
            super().queue_declare(queue, durable, exclusive, auto_delete, arguments)

    channel = RefusingRetryQueueChannel()
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues it


def test_stop_during_transient_retries_still_moves_the_message_to_the_retry_queue(config, sleeps):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_then_fail(_message):
        executor_holder[0].stop()
        raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=stop_then_fail)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    executor.run()

    assert len(executor.processed_messages) == 3  # stop() does not interrupt the retries
    assert len(retry_copies(channel)) == 1
    assert channel.acked_delivery_tags == [1]
    assert executor.get_info().status == ExecutorStatus.STOPPED


# --- dead-letter copies: why, when, and from which queue ------------------------------------


def test_dead_letter_copy_records_why_when_and_from_which_queue(config, clock):
    def reject(_message):
        raise MalformedMessageError("fails application validation")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=reject)
    received = pika.BasicProperties(
        type=SAMPLE_TYPE,
        content_type="application/json",
        correlation_id="flow-7",
        message_id="property-id",
        user_id="publisher",
        expiration="5000",
        headers={
            "tenant": "acme",
            "x-death": [{"queue": "input-queue.retry", "reason": "expired", "count": 1}],
            "x-delivery-count": 3,
            "x-first-death-queue": "input-queue.retry",
            "x-kicak-retry-round": 2,
            "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
        },
    )
    body = SampleMessage(message_id="body-id", text="input").to_bytes()
    channel.queue_deliveries(body, properties=received)

    run_until_stopped(executor)

    [copy] = dead_letter_copies(channel)
    assert copy.routing_key == ""
    assert copy.mandatory is True
    assert copy.body == body
    properties = copy.properties
    assert properties.headers == {
        "tenant": "acme",
        # Its history: how often it was moved to the retry queue before
        "x-kicak-retry-round": 2,
        "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
        "x-kicak-dead-letter-reason": "malformed",
        "x-kicak-error-type": "pykicak.executor.MalformedMessageError",
        "x-kicak-error-message": "fails application validation",
        "x-kicak-dead-lettered-at": "2026-10-01T12:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
        # The original user_id, which RabbitMQ would refuse from this executor's user
        "x-kicak-original-publisher": "publisher",
    }
    # The message_id property as the delivery had it, so the copy is found by the same ID
    assert properties.message_id == "property-id"
    assert properties.user_id == "guest"  # the executor's own user, from CONNECTION_VALUES
    assert properties.expiration is None  # it must not expire in the dead-letter queue
    assert properties.delivery_mode == 2
    assert properties.type == SAMPLE_TYPE
    assert properties.content_type == "application/json"
    assert properties.correlation_id == "flow-7"


class UnprintableError(Exception):
    def __str__(self) -> str:
        raise RuntimeError("cannot describe this error")


def raise_error(error: Exception):
    """execute() handler raising `error`."""

    def handler(_message):
        raise error

    return handler


@pytest.mark.parametrize(
    ("body", "handler", "reason", "error_type", "error_message"),
    [
        (
            b"not json",
            None,
            "malformed",
            "pykicak.executor.MalformedMessageError",
            "Could not decode message as SampleMessage: ",
        ),
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            lambda _message: 1 / 0,
            "unexpected-error",
            "ZeroDivisionError",
            "division by zero",
        ),
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            raise_error(InvalidTextError("text is invalid")),
            "unexpected-error",
            "tests.test_executor.InvalidTextError",
            "text is invalid",
        ),
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            raise_error(UnprintableError()),
            "unexpected-error",
            "tests.test_executor.UnprintableError",
            "<exception str() failed>",
        ),
    ],
    ids=["malformed", "builtin-error", "application-error", "unprintable-error"],
)
def test_dead_letter_copy_names_the_reason_and_the_error(
    config, body, handler, reason, error_type, error_message
):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=handler)
    channel.queue_deliveries(body)

    with pytest.raises((StopConsuming, ZeroDivisionError, InvalidTextError, UnprintableError)):
        executor.run()

    assert dead_lettered(channel) == [1]
    headers = dead_letter_copies(channel)[0].properties.headers
    assert headers["x-kicak-dead-letter-reason"] == reason
    assert headers["x-kicak-error-type"] == error_type
    assert headers["x-kicak-error-message"].startswith(error_message)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("q" * 1024, "q" * 1024),
        ("q" * 1025, "q" * 1021 + "…"),
        # 2000 bytes, cut between the two bytes of a character, which is dropped
        ("ł" * 1000, "ł" * 510 + "…"),
        # A lone surrogate, e.g. from the JSON string "\ud800", cannot be encoded in UTF-8
        ("text \ud800 here", "text ? here"),
    ],
    ids=["fits", "too-long", "too-long-multibyte", "lone-surrogate"],
)
def test_dead_letter_copy_carries_at_most_1024_bytes_of_utf8_error_text(config, text, expected):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=raise_error(MalformedMessageError(text)))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    [copy] = dead_letter_copies(channel)
    assert copy.properties.headers["x-kicak-error-message"] == expected
    assert len(expected.encode("utf-8")) <= 1024
    copy.properties.encode()  # pika can send it


def test_dead_letter_copy_of_a_delivery_without_body_or_message_id(config, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(None)

    run_until_stopped(executor)

    [copy] = dead_letter_copies(channel)
    assert copy.body == b""
    assert copy.properties.message_id is None
    assert copy.properties.headers == {
        "x-kicak-dead-letter-reason": "malformed",
        "x-kicak-error-type": "pykicak.executor.MalformedMessageError",
        "x-kicak-error-message": "Received a message with no body",
        "x-kicak-dead-lettered-at": "2026-10-01T12:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
    }


def test_retry_copy_of_a_replayed_message_drops_its_dead_letter_headers(config, sleeps, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    replayed = pika.BasicProperties(
        type=SAMPLE_TYPE,
        headers={
            "tenant": "acme",
            "x-kicak-dead-letter-reason": "unexpected-error",
            "x-kicak-error-type": "KeyError",
            "x-kicak-error-message": "'text'",
            "x-kicak-dead-lettered-at": "2026-09-30T08:00:00+00:00",
            "x-kicak-dead-lettered-from": "input-queue",
        },
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=replayed
    )

    run_until_stopped(executor)

    assert retry_copies(channel)[0].properties.headers == {
        "tenant": "acme",
        "x-kicak-retry-round": 1,
        "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
    }


@pytest.mark.parametrize(
    "error",
    [UnroutableError([]), NackError([])],
    ids=["unroutable", "refused"],
)
def test_run_requeues_the_message_when_rabbitmq_does_not_take_its_dead_letter_copy(
    config, caplog, error
):
    channel = CopyRefusingChannel(error, target="input-queue.dlx")
    executor = make_executor(config, channel)
    channel.queue_deliveries(b"not json")

    run_until_stopped(executor)  # continues with the next message

    # Never rejected without requeueing, which RabbitMQ may drop when no queue takes it
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert channel.acked_delivery_tags == []
    assert executor.get_info().counts == ExecutorCounts(requeued=1)
    errors = [r for r in caplog.records if "could not be dead-lettered to" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]


def test_run_requeues_and_raises_when_rabbitmq_does_not_take_a_dead_letter_copy_after_a_bug(
    config,
):
    channel = CopyRefusingChannel(NackError([]), target="input-queue.dlx")
    executor = make_executor(config, channel, handler=lambda _message: 1 / 0)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert executor.get_info().status == ExecutorStatus.CRASHED


def test_run_reconnects_when_the_connection_is_lost_while_publishing_a_dead_letter_copy(
    config, monkeypatch, sleeps, caplog
):
    original_channel = CopyRefusingChannel(
        StreamLostError("connection lost"), target="input-queue.dlx"
    )
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(b"not json")
    replacement_channel.queue_deliveries(b"not json")  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    assert dead_lettered(replacement_channel) == [1]
    warnings = [
        r for r in caplog.records if "Connection or channel lost while dead-lettering" in r.message
    ]
    assert [record.levelname for record in warnings] == ["WARNING"]


def test_run_stops_when_rabbitmq_refuses_a_dead_letter_copy(config, caplog):
    channel = CopyRefusingChannel(
        ChannelClosedByBroker(403, "ACCESS_REFUSED"), target="input-queue.dlx"
    )
    executor = make_executor(config, channel)
    channel.queue_deliveries(b"not json")

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues it
    errors = [r for r in caplog.records if "Dead-lettering message id=None" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]
    assert "stopping the executor" in errors[0].message


def test_run_raises_the_processing_error_when_its_dead_letter_copy_is_refused(config, caplog):
    channel = CopyRefusingChannel(
        ChannelClosedByBroker(403, "ACCESS_REFUSED"), target="input-queue.dlx"
    )
    executor = make_executor(config, channel, handler=lambda _message: 1 / 0)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ZeroDivisionError):  # not masked by the refusal
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues it
    assert "could not be dead-lettered after the unexpected error" in caplog.text


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

    assert dead_lettered(channel) == [1]
    assert len(executor.processed_messages) == 1
    assert channel.is_open is False
    assert executor.get_info().counts == ExecutorCounts(
        dead_lettered=DeadLetterCounts(unexpected_error=1)
    )
    assert "failed unexpectedly type=SampleMessage id=input" in caplog.text
    assert "stopping the executor" in caplog.text


def test_graceful_stop_returns_normally_when_closing_the_connection_fails(config, caplog):
    class FailingCloseConnection(FakeConnection):
        def close(self) -> None:
            raise StreamLostError("connection close failed")

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, connection_class=FailingCloseConnection)
    channel.on_idle = executor.stop

    executor.run()  # does not raise

    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert channel.acked_delivery_tags == [1]
    close_errors = [r for r in caplog.records if "Failed to close RabbitMQ connection" in r.message]
    assert [record.levelname for record in close_errors] == ["ERROR"]


def test_run_raises_the_processing_error_when_closing_the_connection_also_fails(config, caplog):
    class FailingCloseConnection(FakeConnection):
        def close(self) -> None:
            raise StreamLostError("connection close failed")

    def broken(_message):
        raise ZeroDivisionError("bug in application code")

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=broken, connection_class=FailingCloseConnection
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert "Failed to close RabbitMQ connection" in caplog.text


@pytest.mark.parametrize(
    "handler",
    [lambda message: message, lambda _message: [{"text": "not a message"}]],
    ids=["message", "not-a-message"],
)
def test_run_dead_letters_and_raises_when_terminator_returns_results(config, handler):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=handler)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(RuntimeError, match="no destination_exchange"):
        executor.run()

    assert dead_lettered(channel) == [1]
    assert channel.published_messages == dead_letter_copies(channel)


def results_for_input_only(results):
    """execute() handler returning `results` for the message "input", and the message otherwise."""
    return lambda message: results if message.message_id == "input" else message


@pytest.mark.parametrize(
    ("results", "reason"),
    [
        (
            [
                SampleMessage(message_id="ok", text="ok"),
                SampleMessage(message_id="unserializable", text=object()),  # type: ignore[arg-type]
            ],
            "cannot be serialized, type=SampleMessage",
        ),
        (
            [SampleMessage(message_id="nan", text=float("nan"))],  # type: ignore[arg-type]
            "cannot be serialized, type=SampleMessage",
        ),
        ([{"text": "not a message"}], "must return KicakMessage instances"),
        ("not a message", "must return KicakMessage instances"),
        (42, "not iterable"),
    ],
    ids=["unserializable", "nan", "list-of-dicts", "string", "number"],
)
def test_run_dead_letters_input_whose_results_are_invalid_and_continues(
    config, caplog, results, reason
):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=results_for_input_only(results),
        destination_exchange="output-exchange",
    )
    following = SampleMessage(message_id="following", text="following")
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), following.to_bytes()
    )

    run_until_stopped(executor)

    # Nothing of the invalid results is published; the next message is processed as usual
    assert [p.exchange for p in channel.published_messages] == [
        "input-queue.dlx",
        "output-exchange",
    ]
    assert channel.published_messages[1].body == following.to_bytes()
    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    dead_letter_records = [
        r
        for r in caplog.records
        if "Message dead-lettered type=SampleMessage id=input" in r.message
    ]
    assert [record.levelname for record in dead_letter_records] == ["ERROR"]
    assert reason in dead_letter_records[0].message
    # Logged once, by the Executor: the serialization code logs it at DEBUG only
    assert not [
        r for r in caplog.records if r.name == "pykicak.messages" and r.levelname == "ERROR"
    ]


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


# --- user_id and x-kicak-original-publisher -------------------------------------------------


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
def test_results_name_the_executor_as_their_original_publisher(config, received):
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
    assert properties.headers == {"x-kicak-original-publisher": "guest"}


@pytest.mark.parametrize(
    ("received", "original_publisher"),
    [
        # Created by an injector, delivered here by an upstream executor's retry copy
        (
            pika.BasicProperties(
                type=SAMPLE_TYPE,
                user_id="upstream-executor",
                headers={"x-kicak-original-publisher": "injector-user"},
            ),
            "injector-user",
        ),
        # Created by another client, which set no header
        (pika.BasicProperties(type=SAMPLE_TYPE, user_id="alice"), "alice"),
        (
            pika.BasicProperties(
                type=SAMPLE_TYPE, headers={"x-kicak-original-publisher": b"injector-user"}
            ),
            "injector-user",
        ),
        # An invalid header is ignored
        (
            pika.BasicProperties(
                type=SAMPLE_TYPE, user_id="alice", headers={"x-kicak-original-publisher": 42}
            ),
            "alice",
        ),
        (pika.BasicProperties(type=SAMPLE_TYPE, headers={"x-kicak-original-publisher": ""}), None),
        (pika.BasicProperties(type=SAMPLE_TYPE), None),
    ],
    ids=["header", "user-id", "header-bytes", "invalid-header", "empty-header", "unknown"],
)
def test_retry_copy_carries_the_original_publisher(config, sleeps, received, original_publisher):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=received
    )

    run_until_stopped(executor)

    copy = retry_copies(channel)[0].properties
    assert copy.user_id == "guest"
    assert copy.headers.get("x-kicak-original-publisher") == original_publisher


class UnroutableResultsChannel(FakeChannel):
    """Results channel whose commits are unroutable or routed, in the order given."""

    def __init__(self, *unroutable_commits: bool) -> None:
        super().__init__()
        self._unroutable_commits = list(unroutable_commits)

    def tx_commit(self) -> None:
        self.unroutable = self._unroutable_commits.pop(0)
        super().tx_commit()


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
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

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
    monkeypatch.setattr("pykicak.executor.time.sleep", lambda _seconds: executor.stop())

    executor.run()

    assert connection_attempts == 1
    assert "stopped on request while reconnecting" in caplog.text


def single_connection(channel: FakeChannel):
    """Stand-in for _create_connection() that opens one connection, then fails the test."""
    connections = [FakeConnection(channel)]

    def create_connection() -> FakeConnection:
        if not connections:
            raise AssertionError("a stopping executor must not reconnect")
        return connections.pop()

    return create_connection


def test_stop_returns_without_reconnecting_when_connection_is_lost_while_settling(
    config, monkeypatch, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_while_processing(_message):
        executor_holder[0].stop()

    channel = SettleFailingChannel(StreamLostError("connection lost"))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=stop_while_processing)
    executor_holder.append(executor)
    monkeypatch.setattr(executor, "_create_connection", single_connection(channel))

    executor.run()

    assert channel.acked_delivery_tags == []  # RabbitMQ requeues it with the lost connection
    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert "stopped on request while reconnecting" in caplog.text


def test_stop_returns_without_reconnecting_when_connection_is_lost_while_waiting(
    config, monkeypatch
):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    class StopThenDisconnectChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            executor_holder[0].stop()  # e.g. a signal handler, while pika waits for a message
            raise StreamLostError("connection lost")
            yield None, None, None

    channel = StopThenDisconnectChannel()
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    monkeypatch.setattr(executor, "_create_connection", single_connection(channel))

    executor.run()

    assert executor.get_info().status == ExecutorStatus.STOPPED


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


# --- status information (get_info) ----------------------------------------------------------


CLOCK_START = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)
"""The time a FakeClock starts at."""


class FakeClock:
    """Stands in for executor._now(); tests move the time forward explicitly."""

    def __init__(self) -> None:
        self.current = CLOCK_START

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


def crash_on_first_call(executor_holder: list[ProcessingExecutor[Any, Any]], statuses: list[str]):
    """execute() handler: raises on the first message, records the status on later ones."""
    calls = 0

    def handler(_message):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ZeroDivisionError("bug in application code")
        statuses.append(executor_holder[0].get_info().status)

    return handler


def test_status_values_are_plain_strings():
    assert [status.value for status in ExecutorStatus] == [
        "CREATED",
        "UPSTREAM_RECOVER",
        "WAITING_FOR_MESSAGE",
        "PROCESSING",
        "DOWNSTREAM_RECOVER",
        "SENDING_RESULT",
        "STOPPING",
        "STOPPED",
        "CRASHED",
    ]
    assert ExecutorStatus.CRASHED == "CRASHED"


def test_info_after_creation(config, clock):
    executor = make_executor(config, FakeChannel())
    checked_at = clock.advance()

    info = executor.get_info()

    assert info == ExecutorInfo(
        status=ExecutorStatus.CREATED,
        status_changed_at=CLOCK_START,
        status_checked_at=checked_at,
        stop_requested=False,
        stop_requested_at=None,
        message=None,
        attempt=None,
        redelivered=None,
        retry_round=None,
        counts=ExecutorCounts(),
    )
    assert type(info.status) is ExecutorStatus
    assert info.status == "CREATED"


def test_info_is_frozen(config):
    info = make_executor(config, FakeChannel()).get_info()

    with pytest.raises(dataclasses.FrozenInstanceError):
        info.status = ExecutorStatus.STOPPED  # type: ignore[misc]


def test_info_repr_lists_every_field(config, clock):
    executor = make_executor(config, FakeChannel())

    assert repr(executor.get_info()) == (
        "ExecutorInfo(status=<ExecutorStatus.CREATED: 'CREATED'>, "
        "status_changed_at=datetime.datetime(2026, 10, 1, 12, 0, tzinfo=datetime.timezone.utc), "
        "status_checked_at=datetime.datetime(2026, 10, 1, 12, 0, tzinfo=datetime.timezone.utc), "
        "stop_requested=False, stop_requested_at=None, message=None, attempt=None, "
        "redelivered=None, retry_round=None, counts=ExecutorCounts(completed=0, "
        "dead_lettered=DeadLetterCounts(malformed=0, result_too_large=0, unexpected_error=0), "
        "moved_to_retry=0, requeued=0, abandoned=0))"
    )


def test_info_repr_shows_the_message_only_by_type_and_id(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[ExecutorInfo[Any]] = []
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda _message: reported.append(executor_holder[0].get_info())
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="secret payload").to_bytes())

    run_until_stopped(executor)

    for text in (repr(reported[0]), str(reported[0])):
        assert "message=<SampleMessage message_id='input'>" in text
        assert "secret payload" not in text


def test_info_reports_the_message_execute_receives(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[ExecutorInfo[Any]] = []
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda _message: reported.append(executor_holder[0].get_info())
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert reported[0].status == ExecutorStatus.PROCESSING
    assert reported[0].message is executor.processed_messages[0]


class StatusRecordingChannel(FakeChannel):
    """Records the executor's status, read by a monitor, at each step of a delivery."""

    def __init__(
        self, executor_holder: list[ProcessingExecutor[Any, Any]], statuses: list[str]
    ) -> None:
        super().__init__()
        self._executor_holder = executor_holder
        self._statuses = statuses

    def _record(self, step: str) -> None:
        self._statuses.append(f"{step}: {self._executor_holder[0].get_info().status}")

    def queue_declare(
        self, queue, durable=False, exclusive=False, auto_delete=False, arguments=None
    ):
        self._record(f"declare {queue}")
        super().queue_declare(queue, durable, exclusive, auto_delete, arguments)

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        self._record("publish")
        super().basic_publish(exchange, routing_key, body, properties, mandatory)

    def basic_ack(self, delivery_tag: int) -> None:
        self._record("ack")
        super().basic_ack(delivery_tag)

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        self._record("nack")
        super().basic_nack(delivery_tag, multiple, requeue)


class MessageRecordingChannel(StatusRecordingChannel):
    """Records the message_id of the message that get_info() reports at each step, or None."""

    def _record(self, step: str) -> None:
        message = self._executor_holder[0].get_info().message
        self._statuses.append(f"{step}: {None if message is None else message.message_id}")


def test_status_follows_each_step_of_a_message(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    statuses: list[str] = []

    def record_status(message):
        statuses.append(f"execute: {executor_holder[0].get_info().status}")
        return message

    channel = StatusRecordingChannel(executor_holder, statuses)
    results_channel = StatusRecordingChannel(executor_holder, statuses)
    executor = make_executor(
        config,
        channel,
        handler=record_status,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.on_idle = lambda: statuses.append(f"idle: {executor.get_info().status}")

    run_until_stopped(executor)

    assert statuses[:14] == [
        # The first connection, with its topology, is attempted while the status is CREATED
        "declare input-queue: CREATED",
        "declare input-queue.dlq: CREATED",
        "declare input-queue.retry: CREATED",
        "declare output-queue: CREATED",
        "execute: PROCESSING",
        # The first verification of the output side happens in PROCESSING
        "declare output-queue: PROCESSING",
        "publish: SENDING_RESULT",
        "ack: SENDING_RESULT",
        # The input side is verified before waiting for the next message
        "declare input-queue: SENDING_RESULT",
        "declare input-queue.dlq: SENDING_RESULT",
        "declare input-queue.retry: SENDING_RESULT",
        "idle: WAITING_FOR_MESSAGE",
        "idle: WAITING_FOR_MESSAGE",
        "idle: WAITING_FOR_MESSAGE",
    ]


def test_terminator_acknowledges_in_processing_status(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    statuses: list[str] = []
    channel = StatusRecordingChannel(executor_holder, statuses)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert "ack: PROCESSING" in statuses


def test_info_reports_the_message_until_it_is_settled(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    steps: list[str] = []

    def record_message(_message):
        message = executor_holder[0].get_info().message
        steps.append(f"execute: {None if message is None else message.message_id}")
        return SampleMessage(message_id="result", text="result")

    channel = MessageRecordingChannel(executor_holder, steps)
    results_channel = MessageRecordingChannel(executor_holder, steps)
    executor = make_executor(
        config,
        channel,
        handler=record_message,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.on_idle = lambda: steps.append(f"idle: {executor.get_info().message}")

    run_until_stopped(executor)

    assert steps[:12] == [
        "declare input-queue: None",
        "declare input-queue.dlq: None",
        "declare input-queue.retry: None",
        "declare output-queue: None",
        "execute: input",
        "declare output-queue: input",
        "publish: input",
        "ack: input",
        # Acknowledged: the input side is verified without a message being processed
        "declare input-queue: None",
        "declare input-queue.dlq: None",
        "declare input-queue.retry: None",
        "idle: None",
    ]


def test_info_has_no_message_for_a_delivery_that_cannot_be_decoded(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    steps: list[str] = []
    channel = MessageRecordingChannel(executor_holder, steps)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(b"not json")

    run_until_stopped(executor)

    assert "publish: None" in steps
    assert dead_lettered(channel) == [1]


def test_info_drops_the_message_when_the_connection_is_lost_while_settling(
    config, monkeypatch, sleeps
):
    body = SampleMessage(message_id="input", text="input").to_bytes()
    original_channel = SettleFailingChannel(connection_lost())
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel)
    channels = [original_channel, replacement_channel]
    reconnecting: list[ExecutorInfo[Any]] = []

    def create_connection() -> FakeConnection:
        reconnecting.append(executor.get_info())
        return FakeConnection(channels.pop(0))

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)

    assert [(info.status, info.message) for info in reconnecting] == [
        (ExecutorStatus.CREATED, None),
        (ExecutorStatus.UPSTREAM_RECOVER, None),
    ]
    assert replacement_channel.acked_delivery_tags == [1]


def test_status_is_upstream_recover_while_reconnecting(config, monkeypatch, sleeps, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel)
    statuses: list[str] = []
    attempts = 0

    def create_connection() -> FakeConnection:
        nonlocal attempts
        attempts += 1
        statuses.append(executor.get_info().status)
        if attempts < 3:
            raise AMQPConnectionError("broker down")
        return FakeConnection(channel)

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)

    assert statuses == ["CREATED", "UPSTREAM_RECOVER", "UPSTREAM_RECOVER"]
    assert channel.acked_delivery_tags == [1]
    entered = [r for r in caplog.records if "is recovering: UPSTREAM_RECOVER" in r.message]
    assert [record.levelname for record in entered] == ["WARNING"]
    left = [r for r in caplog.records if "recovered from UPSTREAM_RECOVER" in r.message]
    assert [record.levelname for record in left] == ["INFO"]
    assert "WAITING_FOR_MESSAGE" in left[0].message


def test_status_is_downstream_recover_with_the_message_while_results_are_held(config, monkeypatch):
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    results_channel = UnroutableResultsChannel(True, False)
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )
    reported: list[ExecutorInfo[Any]] = []
    monkeypatch.setattr(
        "pykicak.executor.time.sleep", lambda _seconds: reported.append(executor.get_info())
    )

    run_until_stopped(executor)

    assert [info.status for info in reported] == ["DOWNSTREAM_RECOVER"]
    assert reported[0].message == SampleMessage(message_id="input", text="input")
    assert channel.acked_delivery_tags == [1]


def test_setting_the_same_status_again_keeps_its_change_time(config, clock):
    executor = make_executor(config, FakeChannel())
    entered_at = clock.advance()
    executor._set_activity(ExecutorStatus.WAITING_FOR_MESSAGE)

    clock.advance()
    executor._set_activity(ExecutorStatus.WAITING_FOR_MESSAGE)

    assert executor.get_info().status_changed_at == entered_at


def test_info_after_stop(config, clock):
    executor = make_executor(config, FakeChannel())
    stop_at = clock.advance()

    executor.stop()

    info = executor.get_info()
    assert (info.status, info.status_changed_at) == ("STOPPING", stop_at)
    assert (info.stop_requested, info.stop_requested_at) == (True, stop_at)


def test_info_reports_the_message_in_progress_while_stopping(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[ExecutorInfo[Any]] = []

    def stop_then_read_info(_message):
        executor_holder[0].stop()
        reported.append(executor_holder[0].get_info())

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=stop_then_read_info)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="in-progress", text="in progress").to_bytes())

    executor.run()

    assert reported[0].status == "STOPPING"
    assert reported[0].message == SampleMessage(message_id="in-progress", text="in progress")
    info = executor.get_info()
    assert (info.status, info.stop_requested, info.message) == ("STOPPED", True, None)


def test_status_is_stopped_after_stop_while_waiting(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.on_idle = executor.stop

    executor.run()

    assert executor.get_info().status == "STOPPED"


def test_status_is_stopped_when_run_follows_stop(config):
    executor = make_executor(config, FakeChannel())
    executor.stop()

    executor.run()

    assert executor.get_info().status == "STOPPED"


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

    info = executor.get_info()
    assert (info.status, info.stop_requested, info.message) == ("CRASHED", False, None)
    assert executor._activity.delivery is None  # the message is not kept after the crash
    assert f"crashed with {type(failure).__name__}" in caplog.text


def test_stop_after_crash_keeps_crashed_status_and_its_change_time(config, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=crash_on_first_call([], []))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    crashed_at = clock.advance()
    with pytest.raises(ZeroDivisionError):
        executor.run()

    stop_at = clock.advance()
    executor.stop()

    info = executor.get_info()
    assert (info.status, info.status_changed_at) == ("CRASHED", crashed_at)
    assert (info.stop_requested, info.stop_requested_at) == (True, stop_at)


def test_status_is_upstream_recover_when_run_follows_a_crash(config, monkeypatch):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
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

    def create_connection() -> FakeConnection:
        statuses.append(f"connecting: {executor.get_info().status}")
        return FakeConnection(channel)

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)  # consumes the queued delivery again

    assert statuses == ["connecting: UPSTREAM_RECOVER", "PROCESSING"]
    # Counted across both runs
    assert executor.get_info().counts == ExecutorCounts(
        completed=1, dead_lettered=DeadLetterCounts(unexpected_error=1)
    )


def test_info_can_be_read_from_another_thread_while_running(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.on_idle = lambda: time.sleep(0.01)
    thread = threading.Thread(target=executor.run)
    thread.start()
    time.sleep(0.1)

    waiting = executor.get_info()
    executor.stop()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert (waiting.status, waiting.stop_requested) == ("WAITING_FOR_MESSAGE", False)
    assert executor.get_info().status == "STOPPED"


def test_info_times_are_timezone_aware_datetimes_in_utc(config):
    executor = make_executor(config, FakeChannel())
    executor.stop()

    info = executor.get_info()

    for value in (info.status_changed_at, info.status_checked_at, info.stop_requested_at):
        assert isinstance(value, datetime)
        assert value.utcoffset() == timedelta(0)
    # Datetimes compare and subtract directly, e.g. to tell how long a status has lasted
    assert info.status_checked_at - info.status_changed_at >= timedelta(0)


def test_status_changed_at_follows_status_changes(config, clock):
    executor = make_executor(config, FakeChannel())

    stopping_at = clock.advance()
    executor.stop()
    assert executor.get_info().status_changed_at == stopping_at

    stopped_at = clock.advance()
    executor.run()
    info = executor.get_info()
    assert info.status_changed_at == stopped_at
    assert info.stop_requested_at == stopping_at


def test_status_checked_at_is_the_time_of_each_call(config, clock):
    executor = make_executor(config, FakeChannel())

    first_at = clock.advance(5)
    first = executor.get_info()
    second_at = clock.advance(5)
    second = executor.get_info()

    assert first.status_checked_at == first_at
    assert second.status_checked_at == second_at
    # Reading changes nothing: the status is still the one entered at creation
    assert first.status_changed_at == second.status_changed_at == CLOCK_START


def test_repeated_stop_keeps_the_first_stop_time(config, clock):
    executor = make_executor(config, FakeChannel())
    first_stop_at = clock.advance()
    executor.stop()

    clock.advance()
    executor.stop()

    info = executor.get_info()
    assert info.status_changed_at == first_stop_at
    assert info.stop_requested_at == first_stop_at


# --- counts and the delivery in progress (get_info) -----------------------------------------


def test_counts_record_how_each_delivery_ended(config, sleeps):
    def handle(message):
        if message.message_id == "flaky":
            raise TransientProcessingError("service unavailable")
        if message.message_id == "invalid":
            raise MalformedMessageError("fails application validation")
        return None

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=handle)
    channel.queue_deliveries(
        SampleMessage(message_id="ok", text="ok").to_bytes(),
        b"not json",
        SampleMessage(message_id="invalid", text="invalid").to_bytes(),
        SampleMessage(message_id="flaky", text="flaky").to_bytes(),
        SampleMessage(message_id="also-ok", text="also ok").to_bytes(),
    )

    run_until_stopped(executor)

    assert executor.get_info().counts == ExecutorCounts(
        completed=2, dead_lettered=DeadLetterCounts(malformed=2), moved_to_retry=1
    )


def test_dead_letter_counts_add_up_to_their_total():
    counts = DeadLetterCounts(malformed=3, result_too_large=2, unexpected_error=1)

    assert counts.total == 6
    assert DeadLetterCounts().total == 0


def test_counts_a_delivery_abandoned_by_a_hard_stop(config):
    def interrupted(_message):
        raise KeyboardInterrupt

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=interrupted)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(KeyboardInterrupt):
        executor.run()

    assert executor.get_info().counts == ExecutorCounts(abandoned=1)


def test_counts_a_delivery_whose_results_were_lost_with_the_connection(config, monkeypatch):
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    replacement_channel = FakeChannel()
    replacement_channel.queue_deliveries(  # RabbitMQ's redelivery
        SampleMessage(message_id="input", text="input").to_bytes(), redelivered=True
    )
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [
            (channel, FailingResultsChannel("commit", StreamLostError("connection lost"))),
            (replacement_channel, FakeChannel()),
        ],
        lambda received: received,
    )

    run_until_stopped(executor)

    assert executor.get_info().counts == ExecutorCounts(completed=1, abandoned=1)


def test_counts_nothing_for_a_delivery_received_after_stop(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.before_delivery = lambda _tag: executor.stop()

    executor.run()

    assert executor.processed_messages == []
    assert executor.get_info().counts == ExecutorCounts()


class InfoRecordingChannel(FakeChannel):
    """Records the executor's get_info() just before each acknowledgement, and while idle."""

    def __init__(self, executor_holder: list[ProcessingExecutor[Any, Any]]) -> None:
        super().__init__()
        self._executor_holder = executor_holder
        self.reported: list[ExecutorInfo[Any]] = []
        self.on_idle = self._record

    def _record(self) -> None:
        self.reported.append(self._executor_holder[0].get_info())

    def basic_ack(self, delivery_tag: int) -> None:
        self._record()
        super().basic_ack(delivery_tag)


def test_a_delivery_is_counted_when_it_is_no_longer_reported(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    channel = InfoRecordingChannel(executor_holder)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    acknowledging, waiting = channel.reported[:2]
    # While it is acknowledged, the message is reported and not yet counted; then the reverse
    assert acknowledging.message is not None and acknowledging.counts.completed == 0
    assert waiting.message is None and waiting.counts.completed == 1


def test_info_reports_which_call_of_execute_is_under_way(config, sleeps):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    attempts: list[int | None] = []

    def fail_twice(_message):
        attempts.append(executor_holder[0].get_info().attempt)
        if len(attempts) < 3:
            raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=fail_twice)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert attempts == [1, 2, 3]
    assert executor.get_info().counts == ExecutorCounts(completed=1)


def test_info_attempts_start_again_for_every_delivery(config, sleeps):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    attempts: list[int | None] = []

    def record_attempt(_message):
        attempts.append(executor_holder[0].get_info().attempt)

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=record_attempt)
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="first", text="first").to_bytes(),
        SampleMessage(message_id="second", text="second").to_bytes(),
    )

    run_until_stopped(executor)

    assert attempts == [1, 1]


def test_info_reports_whether_the_delivery_was_redelivered(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[bool | None] = []
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: reported.append(executor_holder[0].get_info().redelivered),
    )
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="again", text="again").to_bytes(), redelivered=True
    )
    channel.queue_deliveries(SampleMessage(message_id="first", text="first").to_bytes())

    run_until_stopped(executor)

    assert reported == [True, False]


@pytest.mark.parametrize(
    ("headers", "retry_round"),
    [
        (None, 0),
        ({"x-kicak-retry-round": 3}, 3),
        ({"x-kicak-retry-round": 0}, 0),
        ({"x-kicak-retry-round": "3"}, 0),
        ({"x-kicak-retry-round": -1}, 0),
        ({"x-kicak-retry-round": True}, 0),
    ],
    ids=["never-retried", "third-round", "zero", "text", "negative", "bool"],
)
def test_info_reports_the_retry_round_of_the_message(config, headers, retry_round):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[int | None] = []
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: reported.append(executor_holder[0].get_info().retry_round),
    )
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, headers=headers),
    )

    run_until_stopped(executor)

    assert reported == [retry_round]


def test_info_reports_no_delivery_context_without_a_message(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    channel = InfoRecordingChannel(executor_holder)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(b"not json", redelivered=True)

    run_until_stopped(executor)

    # Dead-lettering an undecodable delivery, then waiting: no delivery is reported
    assert [
        (info.message, info.attempt, info.redelivered, info.retry_round)
        for info in channel.reported[:2]
    ] == [(None, None, None, None)] * 2


def test_info_drops_the_delivery_context_once_run_has_ended(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    channel = FakeChannel()

    def crash(_message):
        raise ZeroDivisionError("bug in application code")

    executor = make_executor(config, channel, handler=crash)
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), redelivered=True
    )

    with pytest.raises(ZeroDivisionError):
        executor.run()

    info = executor.get_info()
    assert (info.message, info.attempt, info.redelivered, info.retry_round) == (
        None,
        None,
        None,
        None,
    )


# --- connection failures --------------------------------------------------------------------


def connection_lost() -> StreamLostError:
    return StreamLostError("connection lost")


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


def test_run_dead_letters_input_whose_result_is_too_large_and_continues(
    config, monkeypatch, caplog
):
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


# --- durations in the logs ------------------------------------------------------------------


class FakeStopwatch:
    """Stands in for executor._monotonic(); tests move it forward explicitly.

    Its steps are binary fractions, so the durations computed from it are exact.
    """

    def __init__(self) -> None:
        self.current = 100.0

    def advance(self, seconds: float) -> None:
        self.current += seconds

    def __call__(self) -> float:
        return self.current


@pytest.fixture
def stopwatch(monkeypatch) -> FakeStopwatch:
    fake = FakeStopwatch()
    monkeypatch.setattr(executor_module, "_monotonic", fake)
    return fake


class SlowChannel(FakeChannel):
    """Takes `seconds` of the stopwatch for every publish, commit, acknowledgement, and nack."""

    def __init__(self, stopwatch: FakeStopwatch, seconds: float) -> None:
        super().__init__()
        self._stopwatch = stopwatch
        self._seconds = seconds

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        self._stopwatch.advance(self._seconds)
        super().basic_publish(exchange, routing_key, body, properties, mandatory)

    def tx_commit(self) -> None:
        self._stopwatch.advance(self._seconds)
        super().tx_commit()

    def basic_ack(self, delivery_tag: int) -> None:
        self._stopwatch.advance(self._seconds)
        super().basic_ack(delivery_tag)

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        self._stopwatch.advance(self._seconds)
        super().basic_nack(delivery_tag, multiple, requeue)


def handled_records(caplog) -> list[Any]:
    return [record for record in caplog.records if record.message.startswith("Message handled")]


def kicak_values(record) -> dict[str, Any]:
    """The values a record carries, typed, as its `kicak` attribute."""
    values: dict[str, Any] = vars(record)["kicak"]
    return values


def logged_pairs(record) -> dict[str, str]:
    """The key=value pairs of a record's text."""
    return dict(token.split("=", 1) for token in record.message.split() if "=" in token)


def test_logs_how_long_each_part_of_a_completed_delivery_took(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.executor")
    message = SampleMessage(message_id="input", text="input")
    second = SampleMessage(message_id="input-2", text="second result")

    def handle(received):
        stopwatch.advance(2.0)
        return [received, second]

    channel = SlowChannel(stopwatch, 0.125)
    channel.queue_deliveries(message.to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=handle,
        results_channel=SlowChannel(stopwatch, 0.25),
        destination_exchange="output-exchange",
    )

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    assert record.levelname == "INFO"
    assert record.message == (
        "Message handled outcome=completed reason=None type=SampleMessage id=input "
        "queue=input-queue delivery_tag=1 redelivered=False retry_round=0 attempts=1 results=2 "
        "total_seconds=2.875 execute_seconds=2.000 backoff_seconds=0.000 publish_seconds=0.750 "
        "held_seconds=0.000 settle_seconds=0.125"
    )
    assert kicak_values(record) == {
        "event": "message_handled",
        "outcome": "completed",
        "reason": None,
        "type": "SampleMessage",
        "id": "input",
        "queue": "input-queue",
        "delivery_tag": 1,
        "redelivered": False,
        "retry_round": 0,
        "attempts": 1,
        "results": 2,
        "total_seconds": 2.875,  # the parts below, two publishes and a commit taking 0.75
        "execute_seconds": 2.0,
        "backoff_seconds": 0.0,
        "publish_seconds": 0.75,
        "held_seconds": 0.0,
        "settle_seconds": 0.125,
    }


def test_logs_one_handled_line_for_every_counted_delivery_whatever_its_outcome(
    config, sleeps, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")

    def handle(message):
        if message.message_id == "flaky":
            raise TransientProcessingError("service unavailable")
        if message.message_id == "invalid":
            raise MalformedMessageError("fails application validation")
        return None

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=handle)
    channel.queue_deliveries(
        SampleMessage(message_id="ok", text="ok").to_bytes(),
        b"not json",
        SampleMessage(message_id="invalid", text="invalid").to_bytes(),
        SampleMessage(message_id="flaky", text="flaky").to_bytes(),
    )

    run_until_stopped(executor)

    records = handled_records(caplog)
    assert [record.levelname for record in records] == ["INFO"] * 4
    values = [kicak_values(record) for record in records]
    assert [(v["outcome"], v["reason"], v["id"], v["attempts"]) for v in values] == [
        ("completed", None, "ok", 1),
        ("dead_lettered", "malformed", None, 0),  # not decoded: no message_id property either
        ("dead_lettered", "malformed", "invalid", 1),
        ("moved_to_retry", None, "flaky", 3),
    ]
    assert executor.get_info().counts == ExecutorCounts(
        completed=1, dead_lettered=DeadLetterCounts(malformed=2), moved_to_retry=1
    )
    for record, value in zip(records, values, strict=True):
        # The text holds the same values as the kicak attribute, durations to the millisecond
        assert logged_pairs(record) == {
            name: f"{item:.3f}" if name.endswith("_seconds") else str(item)
            for name, item in value.items()
            if name != "event"
        }
        # Measured with the real clock: the parts never exceed the whole
        parts = [value[f"{part}_seconds"] for part in ("execute", "backoff", "publish", "held")]
        assert all(seconds >= 0 for seconds in [*parts, value["settle_seconds"]])
        assert sum(parts) + value["settle_seconds"] <= value["total_seconds"]


def test_handled_line_names_each_outcome_like_the_count_of_it():
    counted = {count.name for count in dataclasses.fields(ExecutorCounts)}

    for outcome in executor_module._Outcome:
        assert executor_module._logged_outcome(outcome) == (outcome.name.lower(), None)
        assert outcome.name.lower() in counted
    for reason in executor_module._DeadLetterReason:
        assert executor_module._logged_outcome(reason) == ("dead_lettered", reason.value)


def test_handled_line_adds_up_the_attempts_of_execute_and_the_waits_between_them(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    monkeypatch.setattr("pykicak.executor.time.sleep", stopwatch.advance)
    attempts = 0

    def flaky(_message):
        nonlocal attempts
        attempts += 1
        stopwatch.advance(1.0)
        if attempts < 3:
            raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=flaky)

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert values["outcome"] == "completed"
    assert values["attempts"] == 3
    assert values["execute_seconds"] == 3.0
    assert values["backoff_seconds"] == 1.5  # 0.5 and 1.0 seconds
    assert values["total_seconds"] == 4.5
    assert values["results"] == 0  # a terminator


def test_handled_line_counts_the_copy_to_the_retry_queue_as_settling(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.executor")

    def unavailable(_message):
        raise TransientProcessingError("service unavailable")

    channel = SlowChannel(stopwatch, 0.25)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, headers={"x-kicak-retry-round": 2}),
    )
    executor = make_executor(config, channel, handler=unavailable, max_processing_attempts=1)

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["retry_round"], values["attempts"]) == (
        "moved_to_retry",
        2,
        1,
    )
    assert values["settle_seconds"] == 0.5  # the copy, then the acknowledgement
    assert values["total_seconds"] == 0.5


def test_handled_line_is_logged_for_an_unexpected_error_before_it_stops_the_executor(
    config, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")

    def broken(_message):
        stopwatch.advance(0.5)
        raise RuntimeError("bug")

    channel = SlowChannel(stopwatch, 0.25)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=broken)

    with pytest.raises(RuntimeError, match="bug"):
        executor.run()

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["reason"]) == ("dead_lettered", "unexpected-error")
    assert (values["execute_seconds"], values["settle_seconds"]) == (0.5, 0.5)
    crashed = next(i for i, r in enumerate(caplog.records) if "crashed with" in r.message)
    assert caplog.records.index(record) < crashed


def test_handled_line_reports_a_delivery_abandoned_by_a_hard_stop(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")

    def interrupted(_message):
        raise KeyboardInterrupt

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=interrupted)

    with pytest.raises(KeyboardInterrupt):
        executor.run()

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["reason"], values["attempts"]) == ("abandoned", None, 1)
    assert values["settle_seconds"] == 0.0


def test_handled_line_reports_results_lost_with_the_connection_as_abandoned(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    message = SampleMessage(message_id="input", text="input")
    channel = FakeChannel()
    channel.queue_deliveries(message.to_bytes())

    class LosingCommitChannel(FailingResultsChannel):
        def tx_commit(self) -> None:
            stopwatch.advance(0.25)
            super().tx_commit()

    replacement_channel = FakeChannel()
    replacement_channel.queue_deliveries(message.to_bytes(), redelivered=True)
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [
            (channel, LosingCommitChannel("commit", StreamLostError("connection lost"))),
            (replacement_channel, FakeChannel()),
        ],
        lambda received: received,
    )

    run_until_stopped(executor)

    first, second = (kicak_values(record) for record in handled_records(caplog))
    assert (first["outcome"], first["redelivered"], first["results"]) == ("abandoned", False, 1)
    assert (first["publish_seconds"], first["settle_seconds"]) == (0.25, 0.0)
    assert (second["outcome"], second["redelivered"]) == ("completed", True)


def test_handled_line_reports_how_long_unroutable_results_were_held(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    monkeypatch.setattr("pykicak.executor.random.random", lambda: 1.0)
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=UnroutableResultsChannel(True),
        destination_exchange="output-exchange",
    )

    def sleep_then_stop(seconds: float) -> None:
        stopwatch.advance(seconds)
        executor.stop()

    monkeypatch.setattr("pykicak.executor.time.sleep", sleep_then_stop)

    executor.run()

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert values["outcome"] == "requeued"
    assert values["held_seconds"] == 0.5
    assert values["total_seconds"] == 0.5


def test_logs_no_handled_line_for_a_delivery_received_after_stop(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.before_delivery = lambda _tag: executor.stop()

    executor.run()

    assert handled_records(caplog) == []


def test_recovered_line_tells_how_long_the_recovery_lasted(
    config, monkeypatch, sleeps, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel)
    attempts = 0

    def create_connection() -> FakeConnection:
        nonlocal attempts
        attempts += 1
        stopwatch.advance(1.0)
        if attempts < 3:
            raise AMQPConnectionError("broker down")
        return FakeConnection(channel)

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)

    [record] = [r for r in caplog.records if "recovered from" in r.message]
    # Entered once the first attempt failed, left once the third one connected
    assert record.message == (
        "Executor for queue input-queue recovered from UPSTREAM_RECOVER: WAITING_FOR_MESSAGE "
        "recovery_seconds=2.000"
    )
    assert kicak_values(record) == {
        "event": "recovered",
        "queue": "input-queue",
        "recovered_from": "UPSTREAM_RECOVER",
        "status": "WAITING_FOR_MESSAGE",
        "recovery_seconds": 2.0,
    }


def test_stopped_line_tells_how_long_the_stop_took(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_then_finish(_message):
        executor_holder[0].stop()
        stopwatch.advance(3.0)  # the message in progress is finished first

    executor = make_executor(config, channel, handler=stop_then_finish)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    executor.run()

    [record] = [r for r in caplog.records if "stopped on request" in r.message]
    assert record.message == "Executor for queue input-queue stopped on request stop_seconds=3.000"
    assert kicak_values(record) == {
        "event": "stopped",
        "queue": "input-queue",
        "stop_seconds": 3.0,
    }
    # Logged once the connection is closed, after the message's own line
    assert caplog.records.index(handled_records(caplog)[0]) < caplog.records.index(record)


def test_stopped_line_tells_how_long_a_stop_while_reconnecting_took(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor = make_executor(config, FakeChannel())

    def create_connection() -> FakeConnection:
        executor.stop()
        stopwatch.advance(0.5)
        raise AMQPConnectionError("broker down")

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    executor.run()

    [record] = [r for r in caplog.records if "stopped on request" in r.message]
    assert record.message == (
        "Executor for queue input-queue stopped on request while reconnecting stop_seconds=0.500"
    )
    assert kicak_values(record)["stop_seconds"] == 0.5
