"""The Executor's constructor: its arguments, their checks, and defaults."""

import pytest

from pykicak.config import KicakConfig
from pykicak.executor import (
    KicakExecutorAbstract,
    QueueType,
)
from tests.executor.helpers import (
    TOPOLOGY,
    ProcessingExecutor,
    SampleMessage,
    SampleTerminator,
    make_executor,
    run_until_stopped,
)
from tests.fakes import (
    CONNECTION_VALUES,
    FakeChannel,
    FakeConnection,
    captured_connection_parameters,
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


def test_init_stores_source_queue_and_exchange(config):
    executor = make_executor(config, FakeChannel())

    assert executor._topology.source_queue == "input-queue"
    assert executor._topology.source_exchange == "input-exchange"


def test_init_derives_dead_letter_names_from_the_source_queue(config):
    executor = make_executor(config, FakeChannel())

    assert executor._topology.dead_letter_exchange == "input-queue.dlx"
    assert executor._topology.dead_letter_queue == "input-queue.dlq"


def test_init_stores_destination_exchange_and_queues(config):
    executor = make_executor(
        config,
        FakeChannel(),
        destination_exchange="output-exchange",
        destination_queues=["result-a", "result-b"],
    )

    assert executor._topology.destination_exchange == "output-exchange"
    assert executor._topology.destination_queues == ("result-a", "result-b")


def test_init_allows_terminator_mode_without_destination(config):
    executor = make_executor(config, FakeChannel())

    assert executor._topology.source_exchange == "input-exchange"
    assert executor._topology.destination_exchange is None
    assert executor._topology.destination_queues == ()


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

    assert downstream._topology.destination_queues == ("input-queue",)


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

    assert executor._topology.queue_type is QueueType.CLASSIC


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

    assert executor._topology.queue_type is expected


@pytest.mark.parametrize("queue_type", ["Quorum", "stream", "", None])
def test_init_rejects_unknown_queue_type(config, queue_type):
    with pytest.raises(
        ValueError, match="queue_type must be QueueType.CLASSIC or QueueType.QUORUM, got"
    ):
        make_executor(config, FakeChannel(), queue_type=queue_type)


def test_init_checks_the_input_side_every_ten_seconds_by_default(config):
    executor = make_executor(config, FakeChannel())

    assert executor._settings.input_check_seconds == 10.0


@pytest.mark.parametrize("input_check_seconds", [0, 0.5, 60])
def test_init_accepts_input_check_interval(config, input_check_seconds):
    executor = make_executor(config, FakeChannel(), input_check_seconds=input_check_seconds)

    assert executor._settings.input_check_seconds == input_check_seconds


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

    assert executor._settings.retry_delay_milliseconds == 10_000


@pytest.mark.parametrize(
    ("retry_delay_seconds", "milliseconds"),
    [(0, 0), (0.25, 250), (60, 60_000), (4294967.295, 2**32 - 1)],
)
def test_init_accepts_retry_delay(config, retry_delay_seconds, milliseconds):
    executor = make_executor(config, FakeChannel(), retry_delay_seconds=retry_delay_seconds)

    assert executor._settings.retry_delay_milliseconds == milliseconds


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


def test_init_requires_max_retry_rounds(config):
    # No default: every agent decides how long a failure may keep a message going round
    with pytest.raises(TypeError, match="max_retry_rounds"):
        SampleTerminator(
            config, FakeChannel(), source_queue="input-queue", source_exchange="input-exchange"
        )


@pytest.mark.parametrize("max_retry_rounds", [0, 1, 1000])
def test_init_stores_max_retry_rounds(config, max_retry_rounds):
    executor = make_executor(config, FakeChannel(), max_retry_rounds=max_retry_rounds)

    assert executor._settings.max_retry_rounds == max_retry_rounds


def test_init_rejects_negative_max_retry_rounds(config):
    with pytest.raises(ValueError, match="max_retry_rounds must be 0 or more"):
        make_executor(config, FakeChannel(), max_retry_rounds=-1)


@pytest.mark.parametrize("max_retry_rounds", [2.5, True, "3", None])
def test_init_rejects_non_integer_max_retry_rounds(config, max_retry_rounds):
    # None would mean no limit: a failure that never clears would keep the message going forever
    with pytest.raises(TypeError, match="max_retry_rounds must be an int"):
        make_executor(config, FakeChannel(), max_retry_rounds=max_retry_rounds)


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
