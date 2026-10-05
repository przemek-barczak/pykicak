"""End-to-end tests against a real RabbitMQ broker.

Requires a broker reachable via the .kicak file named by KICAK_CONFIG_PATH (default: .kicak in
the working directory), e.g.:

    docker run -p 5672:5672 rabbitmq

These tests are excluded from the default `pytest` run (see the `integration`
marker and `addopts` in pyproject.toml) and must be run explicitly with:

    pytest -m integration

Every queue and exchange a test declares gets a unique name and is deleted afterwards.
"""

from __future__ import annotations

import dataclasses
import os
import threading
import time
import uuid
from collections.abc import Iterator

import pika
import pytest
from pika.exceptions import UnroutableError
from pika.spec import Basic

from pykicak.config import KicakConfig
from pykicak.executor import (
    KicakExecutorAbstract,
    MalformedMessageError,
    QueueType,
    TransientProcessingError,
)
from pykicak.injector import InjectionError, KicakInjectorAbstract
from pykicak.messages import KicakMessage

pytestmark = pytest.mark.integration

_DELIVERY_TIMEOUT_SECONDS = 10


@dataclasses.dataclass(frozen=True, slots=True)
class IntegrationMessage(KicakMessage):
    payload: str


class OneShotInjector(KicakInjectorAbstract):
    def __init__(self, config: KicakConfig, exchange_name: str, message: KicakMessage) -> None:
        super().__init__(config, exchange_name=exchange_name)
        self._message = message

    def generate(self) -> KicakMessage | None:
        return self._message


class RecordingExecutor(KicakExecutorAbstract):
    """Forwards messages if it has a destination; payloads "reject"/"flaky" exercise failures."""

    def __init__(self, config: KicakConfig, **topology) -> None:
        super().__init__(config, **topology)
        self.received: list[KicakMessage] = []

    def message_type(self) -> type[KicakMessage]:
        return IntegrationMessage

    def execute(self, message: KicakMessage) -> KicakMessage | None:
        self.received.append(message)
        payload = getattr(message, "payload", None)
        if payload == "reject":
            raise MalformedMessageError("rejected by the test")
        if payload == "flaky" and len(self.received) == 1:
            raise TransientProcessingError("first attempt fails")
        return message if self._destination_exchange is not None else None

    def handle_next_delivery(self, queue: str) -> Basic.Deliver:
        """Handle one delivery through the executor's real decode/execute/settle path."""
        method, _properties, body = next(
            self.channel.consume(queue, inactivity_timeout=_DELIVERY_TIMEOUT_SECONDS)
        )
        assert method is not None, f"No delivery on {queue} within {_DELIVERY_TIMEOUT_SECONDS}s"
        assert self._handle_delivery(method.delivery_tag, body)
        return method


class TemporaryTopology:
    """Unique queue and exchange names for one test."""

    def __init__(self) -> None:
        self._prefix = f"pykicak-test.{uuid.uuid4().hex}"
        self.queues: list[str] = []
        self.exchanges: list[str] = []

    def queue(self, suffix: str) -> str:
        name = f"{self._prefix}.{suffix}"
        self.queues.append(name)
        return name

    def exchange(self, suffix: str) -> str:
        name = f"{self._prefix}.{suffix}"
        self.exchanges.append(name)
        return name

    def executor_topology(self, suffix: str, source_exchange: str) -> dict[str, str]:
        """Source queue bound to `source_exchange`, with its own dead-letter exchange and queue."""
        return {
            "source_queue": self.queue(suffix),
            "source_exchange": source_exchange,
            "dead_letter_exchange": self.exchange(f"{suffix}.dlx"),
            "dead_letter_queue": self.queue(f"{suffix}.dlq"),
        }


def _open_connection(config: KicakConfig) -> pika.BlockingConnection:
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config.rabbitmq_host,
            port=config.rabbitmq_port,
            virtual_host=config.rabbitmq_virtual_host,
            credentials=pika.PlainCredentials(
                config.rabbitmq_username, config.rabbitmq_password
            ),
        )
    )


def _get_dead_letter(
    channel, queue: str
) -> tuple[pika.BasicProperties, bytes]:
    """Poll `queue` until the broker has routed a dead-lettered message into it."""
    deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        method, properties, body = channel.basic_get(queue=queue, auto_ack=True)
        if method is not None:
            return properties, body
        time.sleep(0.1)
    pytest.fail(f"No dead-lettered message on {queue} within {_DELIVERY_TIMEOUT_SECONDS}s")


@pytest.fixture
def config() -> KicakConfig:
    """Load the broker config, failing fast if the broker is unreachable.

    Without this check, an unreachable broker would leave the executors retrying forever.
    """
    config = KicakConfig.from_file(os.environ.get("KICAK_CONFIG_PATH", ".kicak"))
    try:
        _open_connection(config).close()
    except pika.exceptions.AMQPConnectionError as error:
        pytest.fail(f"RabbitMQ broker is not reachable: {error!r}")
    return config


@pytest.fixture
def topology(config) -> Iterator[TemporaryTopology]:
    names = TemporaryTopology()
    yield names
    connection = _open_connection(config)
    try:
        channel = connection.channel()
        for queue in names.queues:
            channel.queue_delete(queue=queue)
        for exchange in names.exchanges:
            channel.exchange_delete(exchange=exchange)
    finally:
        connection.close()


def _ready_message_count(config: KicakConfig, queue: str) -> tuple[int, int]:
    """Return (ready messages, consumers) of `queue`, as reported by the broker."""
    connection = _open_connection(config)
    try:
        declared = connection.channel().queue_declare(queue=queue, passive=True)
        return declared.method.message_count, declared.method.consumer_count
    finally:
        connection.close()


def test_injector_executor_fanout_round_trip(config, topology):
    inject_exchange = topology.exchange("inject")
    fanout_exchange = topology.exchange("fanout")
    terminator_1 = topology.executor_topology("terminator-1", fanout_exchange)
    terminator_2 = topology.executor_topology("terminator-2", fanout_exchange)
    fanout = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="hello", payload="hello")

    with RecordingExecutor(config, **terminator_1) as executor_1, RecordingExecutor(
        config, **terminator_2
    ) as executor_2, RecordingExecutor(
        config, destination_exchange=fanout_exchange, **fanout
    ) as fanout_executor:
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)

        fanout_executor.handle_next_delivery(fanout["source_queue"])
        executor_1.handle_next_delivery(terminator_1["source_queue"])
        executor_2.handle_next_delivery(terminator_2["source_queue"])

    assert fanout_executor.received == [message]
    assert executor_1.received == [message]
    assert executor_2.received == [message]


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_dead_letters_undecodable_message(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)

    with RecordingExecutor(config, queue_type=queue_type, **names) as executor:
        executor.channel.basic_publish(exchange=inject_exchange, routing_key="", body=b"not json")
        executor.handle_next_delivery(names["source_queue"])
        properties, body = _get_dead_letter(executor.channel, names["dead_letter_queue"])

    assert executor.received == []
    assert body == b"not json"
    death = properties.headers["x-death"][0]
    assert death["reason"] == "rejected"
    assert death["queue"] == names["source_queue"]


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_dead_letters_message_rejected_by_execute(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="reject", payload="reject")

    with RecordingExecutor(config, queue_type=queue_type, **names) as executor:
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        executor.handle_next_delivery(names["source_queue"])
        properties, body = _get_dead_letter(executor.channel, names["dead_letter_queue"])

    assert IntegrationMessage.from_bytes(body) == message
    assert properties.message_id == "reject"
    assert properties.content_type == "application/json"


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_requeues_message_after_transient_failures(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="flaky", payload="flaky")

    with RecordingExecutor(
        config, queue_type=queue_type, max_processing_attempts=1, **names
    ) as executor:
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        first = executor.handle_next_delivery(names["source_queue"])
        second = executor.handle_next_delivery(names["source_queue"])
        dead_letter = executor.channel.basic_get(names["dead_letter_queue"], auto_ack=True)

    assert first.redelivered is False
    assert second.redelivered is True
    assert executor.received == [message, message]
    assert dead_letter[0] is None


@pytest.mark.parametrize(
    ("queue_type", "other_type"),
    [(QueueType.CLASSIC, QueueType.QUORUM), (QueueType.QUORUM, QueueType.CLASSIC)],
)
def test_executor_declares_both_queues_with_the_requested_type(
    config, topology, queue_type, other_type
):
    names = topology.executor_topology("source", topology.exchange("inject"))

    with RecordingExecutor(config, queue_type=queue_type, **names):
        pass

    # RabbitMQ reports no queue type on declare, but rejects a redeclaration with another type.
    connection = _open_connection(config)
    try:
        for queue, arguments in (
            (names["source_queue"], {"x-dead-letter-exchange": names["dead_letter_exchange"]}),
            (names["dead_letter_queue"], {}),
        ):
            connection.channel().queue_declare(
                queue=queue, durable=True, arguments={**arguments, "x-queue-type": queue_type}
            )
            with pytest.raises(pika.exceptions.ChannelClosedByBroker) as raised:
                connection.channel().queue_declare(
                    queue=queue, durable=True, arguments={**arguments, "x-queue-type": other_type}
                )
            assert raised.value.reply_code == 406
    finally:
        connection.close()


def test_classic_executor_accepts_queues_declared_without_queue_type(config, topology):
    """Queues declared before queue_type existed (no x-queue-type) keep working."""
    names = topology.executor_topology("source", topology.exchange("inject"))
    connection = _open_connection(config)
    try:
        channel = connection.channel()
        channel.queue_declare(
            queue=names["source_queue"],
            durable=True,
            arguments={"x-dead-letter-exchange": names["dead_letter_exchange"]},
        )
        channel.queue_declare(queue=names["dead_letter_queue"], durable=True)
    finally:
        connection.close()

    with RecordingExecutor(config, **names) as executor:
        assert executor.channel.is_open


def test_injector_raises_when_no_queue_is_bound(config, topology):
    unbound_exchange = topology.exchange("unbound")

    message = IntegrationMessage(message_id="lost", payload="lost")

    with (
        OneShotInjector(config, unbound_exchange, message) as injector,
        pytest.raises(InjectionError) as raised,
    ):
        injector.publish(message)

    assert isinstance(raised.value.__cause__, UnroutableError)


def test_executor_requeues_input_when_result_is_unroutable(config, topology):
    inject_exchange = topology.exchange("inject")
    unbound_exchange = topology.exchange("unbound")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="hello", payload="hello")

    with RecordingExecutor(config, destination_exchange=unbound_exchange, **names) as executor:
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        deliveries = executor.channel.consume(
            names["source_queue"], inactivity_timeout=_DELIVERY_TIMEOUT_SECONDS
        )
        method, _properties, body = next(deliveries)
        assert method is not None
        with pytest.raises(UnroutableError):
            executor._handle_delivery(method.delivery_tag, body)
        redelivered, _properties, redelivered_body = next(deliveries)

    assert redelivered is not None
    assert redelivered.redelivered is True
    assert IntegrationMessage.from_bytes(redelivered_body) == message


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_takes_one_message_at_a_time(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)

    with RecordingExecutor(config, queue_type=queue_type, **names) as executor:
        for payload in ("first", "second", "third"):
            executor.channel.basic_publish(
                exchange=inject_exchange,
                routing_key="",
                body=IntegrationMessage(message_id=payload, payload=payload).to_bytes(),
            )
        consumer = executor._start_consuming()
        method, _properties, _body = next(consumer)
        assert method is not None

        ready, consumers = _ready_message_count(config, names["source_queue"])

    assert (ready, consumers) == (2, 1)


def test_run_raises_when_rabbitmq_cancels_the_consumer(config, topology):
    names = topology.executor_topology("source", topology.exchange("inject"))
    executor = RecordingExecutor(config, **names)
    outcome: list[BaseException] = []

    def run_executor() -> None:
        try:
            executor.run()
        except BaseException as error:
            outcome.append(error)

    thread = threading.Thread(target=run_executor, daemon=True)
    thread.start()
    deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            if _ready_message_count(config, names["source_queue"])[1] == 1:
                break
        except pika.exceptions.ChannelClosedByBroker:
            pass  # the executor has not declared its queue yet
        time.sleep(0.1)

    connection = _open_connection(config)
    try:
        connection.channel().queue_delete(queue=names["source_queue"])
    finally:
        connection.close()
    thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], RuntimeError)
    assert "cancelled the consumer" in str(outcome[0])
    assert executor.get_status() == "CRASHED"


@pytest.mark.parametrize("heartbeat_seconds", [None, 900], ids=["default", "configured"])
def test_executor_connection_uses_requested_heartbeat(config, topology, heartbeat_seconds):
    names = topology.executor_topology("source", topology.exchange("inject"))
    if heartbeat_seconds is not None:
        names["heartbeat_seconds"] = heartbeat_seconds

    with RecordingExecutor(config, **names) as executor:
        negotiated = executor._connection._impl.params.heartbeat

    assert negotiated == (heartbeat_seconds or 600)


class SlowExecutor(RecordingExecutor):
    """Takes `EXECUTE_SECONDS` per message and signals when processing has started."""

    EXECUTE_SECONDS = 1.5

    def __init__(self, config: KicakConfig, **topology) -> None:
        super().__init__(config, **topology)
        self.started = threading.Event()

    def execute(self, message: KicakMessage) -> KicakMessage | None:
        self.started.set()
        time.sleep(self.EXECUTE_SECONDS)
        return super().execute(message)


def _start_in_thread(executor: KicakExecutorAbstract) -> tuple[threading.Thread, list[BaseException]]:
    """Run `executor.run()` in a thread; the list collects anything it raises."""
    errors: list[BaseException] = []

    def run() -> None:
        try:
            executor.run()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, errors


def _wait_until_consuming(config: KicakConfig, queue: str) -> None:
    deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            if _ready_message_count(config, queue)[1] == 1:
                return
        except pika.exceptions.ChannelClosedByBroker:
            pass  # the executor has not declared its queue yet
        time.sleep(0.1)
    pytest.fail(f"No consumer on {queue} within {_DELIVERY_TIMEOUT_SECONDS}s")


def test_stop_finishes_message_in_progress_and_leaves_the_rest_queued(config, topology):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    executor = SlowExecutor(config, **names)
    thread, errors = _start_in_thread(executor)
    _wait_until_consuming(config, names["source_queue"])
    first = IntegrationMessage(message_id="first", payload="first")
    second = IntegrationMessage(message_id="second", payload="second")
    with OneShotInjector(config, inject_exchange, first) as injector:
        injector.publish(first)
        injector.publish(second)

    assert executor.started.wait(_DELIVERY_TIMEOUT_SECONDS)
    executor.stop()
    thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert executor.received == [first]
    assert executor.get_status() == "STOPPED"
    assert _ready_message_count(config, names["source_queue"]) == (1, 0)


def test_stop_while_waiting_returns_promptly(config, topology):
    names = topology.executor_topology("source", topology.exchange("inject"))
    executor = RecordingExecutor(config, **names)
    thread, errors = _start_in_thread(executor)
    _wait_until_consuming(config, names["source_queue"])

    stop_requested_at = time.monotonic()
    executor.stop()
    thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert time.monotonic() - stop_requested_at < 3
    assert executor.get_status() == "STOPPED"
    assert _ready_message_count(config, names["source_queue"]) == (0, 0)


def test_injector_run_publishes_on_a_short_lived_connection(config, topology):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="injected-by-run", payload="injected by run")

    with RecordingExecutor(config, **names) as executor:  # installs the queue and binding
        injector = OneShotInjector(config, inject_exchange, message)
        injector.run()
        executor.handle_next_delivery(names["source_queue"])

    assert executor.received == [message]
    assert injector._connection is None


def test_injector_run_raises_after_closing_when_no_queue_is_bound(config, topology):
    injector = OneShotInjector(
        config, topology.exchange("unbound"), IntegrationMessage(message_id="lost", payload="lost")
    )

    with pytest.raises(InjectionError) as raised:
        injector.run()

    assert isinstance(raised.value.__cause__, UnroutableError)
    assert injector._connection is None
