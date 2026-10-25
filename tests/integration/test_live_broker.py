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
from collections.abc import Iterator, Sequence
from datetime import datetime
from typing import Any, cast

import pika
import pytest
from pika.exceptions import UnroutableError
from pika.spec import Basic

from examples.replay_dead_letters import replay_dead_letters
from pykicak._executor.topology import _ExecutorTopology
from pykicak.abstracts import _tls_options
from pykicak.config import KicakConfig
from pykicak.executor import (
    DeadLetterCounts,
    ExecutorCounts,
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
    kicak_type = "integration.v1"
    payload: str


@dataclasses.dataclass(frozen=True, slots=True)
class IntegrationMessageV2(KicakMessage):
    kicak_type = "integration.v2"
    payload: str
    language: str


class OneShot[M: KicakMessage](KicakInjectorAbstract[M]):
    def __init__(self, config: KicakConfig, exchange_name: str, message: M) -> None:
        super().__init__(config, exchange_name=exchange_name)
        self._message = message

    def generate(self) -> M | None:
        return self._message


class OneShotInjector(OneShot[IntegrationMessage]):
    """Injects an IntegrationMessage."""


class OneShotInjectorV2(OneShot[IntegrationMessageV2]):
    """Injects the next version of IntegrationMessage."""


class RecordingAgent[M: KicakMessage, R: KicakMessage | None](KicakExecutorAbstract[M, R]):
    """Forwards messages if it has a destination; some payloads exercise failures.

    "reject" is malformed, "flaky" fails transiently once, and "unavailable" always.
    """

    def __init__(self, config: KicakConfig, **topology) -> None:
        topology.setdefault("max_retry_rounds", 10)  # more than any test uses up
        super().__init__(config, **topology)
        self.received: list[M] = []
        self._last_properties: pika.BasicProperties | None = None

    @property
    def last_properties(self) -> pika.BasicProperties:
        """The properties of the delivery handle_next_delivery() handled last."""
        assert self._last_properties is not None, "no delivery handled yet"
        return self._last_properties

    def execute(self, message: M) -> R | Sequence[R] | None:
        self.received.append(message)
        payload = getattr(message, "payload", None)
        if payload == "reject":
            raise MalformedMessageError("rejected by the test")
        if payload == "flaky" and len(self.received) == 1:
            raise TransientProcessingError("first attempt fails")
        if payload == "unavailable":
            raise TransientProcessingError("service unavailable")
        # Only the forwarding RecordingExecutor has a destination, and it publishes what it accepts
        return cast(R, message) if self._topology.destination_exchange is not None else None

    def handle_next_delivery(self, queue: str) -> Basic.Deliver:
        """Handle one delivery through the executor's real decode/execute/settle path."""
        method, properties, body = next(
            self.channel.consume(queue, inactivity_timeout=_DELIVERY_TIMEOUT_SECONDS)
        )
        assert method is not None, f"No delivery on {queue} within {_DELIVERY_TIMEOUT_SECONDS}s"
        self._last_properties = properties
        assert self._handle_delivery(method.delivery_tag, method.redelivered, properties, body)
        return method


class RecordingExecutor(RecordingAgent[IntegrationMessage, IntegrationMessage]):
    """Publishes every message it receives as its result."""


class RecordingTerminator(RecordingAgent[IntegrationMessage, None]):
    """Publishes nothing."""


class UpgradingTerminator(RecordingAgent[IntegrationMessage | IntegrationMessageV2, None]):
    """Accepts both versions of IntegrationMessage, as during an upgrade of its producer."""


class ChunkingExecutor(RecordingExecutor):
    """Splits each message into three chunks with IDs derived from the input's, like a chunker."""

    def execute(self, message: IntegrationMessage) -> list[IntegrationMessage]:
        self.received.append(message)
        return [
            IntegrationMessage(
                message_id=f"{message.message_id}.chunk-{index}", payload=f"chunk {index}"
            )
            for index in range(1, 4)
        ]


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
        """Source queue bound to `source_exchange`; its dead-letter route and retry queue too."""
        source_queue = self.queue(suffix)
        self.exchange(f"{suffix}.dlx")
        self.queue(f"{suffix}.dlq")
        self.queue(f"{suffix}.retry")
        return {"source_queue": source_queue, "source_exchange": source_exchange}


def _dead_letter_queue(names: dict[str, str]) -> str:
    """The dead-letter queue the executor of `names` declares for its source queue."""
    return f"{names['source_queue']}.dlq"


def _dead_letter_exchange(names: dict[str, str]) -> str:
    """The dead-letter exchange the executor of `names` declares for its source queue."""
    return f"{names['source_queue']}.dlx"


def _open_connection(config: KicakConfig) -> pika.BlockingConnection:
    """Open a connection of the test itself, with TLS if `config` asks for it."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config.rabbitmq_host,
            port=config.rabbitmq_port,
            virtual_host=config.rabbitmq_virtual_host,
            credentials=pika.PlainCredentials(config.rabbitmq_username, config.rabbitmq_password),
            ssl_options=_tls_options(config),
        )
    )


def _get_dead_letter(channel, queue: str) -> tuple[pika.BasicProperties, bytes]:
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
    _delete_topology(config, names)


def _delete_topology(config: KicakConfig, names: TemporaryTopology) -> None:
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

    with (
        RecordingTerminator(config, **terminator_1) as executor_1,
        RecordingTerminator(config, **terminator_2) as executor_2,
        RecordingExecutor(
            config,
            destination_exchange=fanout_exchange,
            destination_queues=[terminator_1["source_queue"], terminator_2["source_queue"]],
            **fanout,
        ) as fanout_executor,
    ):
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)

        fanout_executor.handle_next_delivery(fanout["source_queue"])
        executor_1.handle_next_delivery(terminator_1["source_queue"])
        executor_2.handle_next_delivery(terminator_2["source_queue"])

    assert fanout_executor.received == [message]
    assert executor_1.received == [message]
    assert executor_2.received == [message]


def test_every_message_names_its_verified_user_and_its_original_publisher(config, topology):
    inject_exchange = topology.exchange("inject")
    fanout_exchange = topology.exchange("fanout")
    terminator = topology.executor_topology("terminator", fanout_exchange)
    source = topology.executor_topology("source", inject_exchange)
    injected = IntegrationMessage(message_id="injected", payload="injected")
    # Fails once in the source executor, so it goes through the retry queue
    foreign = IntegrationMessage(message_id="foreign", payload="flaky")
    user = config.rabbitmq_username

    with (
        RecordingTerminator(config, **terminator) as last,
        RecordingExecutor(
            config,
            destination_exchange=fanout_exchange,
            destination_queues=[terminator["source_queue"]],
            max_processing_attempts=1,
            retry_delay_seconds=0.2,
            **source,
        ) as first,
    ):
        # Another client's message, naming its creator in the header
        first.channel.basic_publish(
            exchange=inject_exchange,
            routing_key="",
            body=foreign.to_bytes(),
            properties=pika.BasicProperties(
                type=IntegrationMessage.kicak_type,
                headers={"x-kicak-original-publisher": "origin-user"},
            ),
        )
        with OneShotInjector(config, inject_exchange, injected) as injector:
            injector.publish(injected)
        first.handle_next_delivery(source["source_queue"])  # foreign fails: retry queue
        first.handle_next_delivery(source["source_queue"])
        from_injector = first.last_properties
        last.handle_next_delivery(terminator["source_queue"])
        injected_result = last.last_properties
        first.handle_next_delivery(source["source_queue"])  # foreign, back from the retry queue
        retry_copy = first.last_properties
        last.handle_next_delivery(terminator["source_queue"])
        foreign_result = last.last_properties

    # RabbitMQ accepted every user_id, which it verifies against the publishing connection
    for properties in (from_injector, retry_copy, injected_result, foreign_result):
        assert properties.user_id == user
        assert properties.type == IntegrationMessage.kicak_type  # kept by the retry copy too
    # The injector and the executor name themselves on the messages they create...
    assert from_injector.headers["x-kicak-original-publisher"] == user
    assert injected_result.headers["x-kicak-original-publisher"] == user
    assert foreign_result.headers["x-kicak-original-publisher"] == user
    # ...and a retry copy keeps the creator of the message
    assert retry_copy.headers["x-kicak-original-publisher"] == "origin-user"
    # Every message an agent publishes, and every retry copy, says when it was published,
    # including the copy of the foreign message, which had no such header
    published_at = {
        name: datetime.fromisoformat(properties.headers["x-kicak-published-at"])
        for name, properties in (
            ("from_injector", from_injector),
            ("injected_result", injected_result),
            ("retry_copy", retry_copy),
            ("foreign_result", foreign_result),
        )
    }
    assert all(time.utcoffset() is not None for time in published_at.values())
    # In the order they were published: the foreign message failed, and was copied to the
    # retry queue, before the injected message was executed
    assert published_at["from_injector"] <= published_at["retry_copy"]
    assert published_at["retry_copy"] <= published_at["injected_result"]
    assert published_at["injected_result"] <= published_at["foreign_result"]


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_dead_letters_undecodable_message(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)

    with RecordingTerminator(config, queue_type=queue_type, **names) as executor:
        executor.channel.basic_publish(
            exchange=inject_exchange,
            routing_key="",
            body=b"not json",
            properties=pika.BasicProperties(type=IntegrationMessage.kicak_type),
        )
        executor.handle_next_delivery(names["source_queue"])
        properties, body = _get_dead_letter(executor.channel, _dead_letter_queue(names))

    assert executor.received == []
    assert executor.get_info().counts == ExecutorCounts(dead_lettered=DeadLetterCounts(malformed=1))
    assert body == b"not json"
    headers = properties.headers
    assert "x-death" not in headers  # the executor's copy: RabbitMQ did not dead-letter it
    assert headers["x-kicak-dead-letter-reason"] == "malformed"
    assert headers["x-kicak-error-type"] == "pykicak.executor.MalformedMessageError"
    assert headers["x-kicak-error-message"].startswith("Could not decode message as")
    assert headers["x-kicak-dead-lettered-from"] == names["source_queue"]
    assert datetime.fromisoformat(headers["x-kicak-dead-lettered-at"]).tzinfo is not None
    assert properties.user_id == config.rabbitmq_username


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_accepts_the_versions_it_declares_and_dead_letters_others(
    config, topology, queue_type
):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    old = IntegrationMessage(message_id="old", payload="queued before the upgrade")
    new = IntegrationMessageV2(message_id="new", payload="after the upgrade", language="pl")
    unknown = IntegrationMessage(message_id="unknown", payload="a version nobody accepts")

    with UpgradingTerminator(config, queue_type=queue_type, **names) as executor:
        with OneShotInjector(config, inject_exchange, old) as injector:
            injector.publish(old)
        with OneShotInjectorV2(config, inject_exchange, new) as injector:
            injector.publish(new)
        executor.channel.basic_publish(
            exchange=inject_exchange,
            routing_key="",
            body=unknown.to_bytes(),
            properties=pika.BasicProperties(type="integration.v3", message_id="unknown"),
        )
        for _ in range(3):
            executor.handle_next_delivery(names["source_queue"])
        properties, body = _get_dead_letter(executor.channel, _dead_letter_queue(names))

    # The queue held both versions at once; each arrived as the class it was sent as
    assert executor.received == [old, new]
    assert [type(message) for message in executor.received] == [
        IntegrationMessage,
        IntegrationMessageV2,
    ]
    # The version nobody accepts is kept in the dead-letter queue, with its type
    assert body == unknown.to_bytes()
    assert properties.type == "integration.v3"
    assert properties.headers["x-kicak-dead-letter-reason"] == "malformed"
    assert "'integration.v3', which is not accepted" in properties.headers["x-kicak-error-message"]


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_dead_letters_message_rejected_by_execute(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="reject", payload="reject")

    with RecordingTerminator(config, queue_type=queue_type, **names) as executor:
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        executor.handle_next_delivery(names["source_queue"])
        properties, body = _get_dead_letter(executor.channel, _dead_letter_queue(names))

    assert IntegrationMessage.from_bytes(body) == message
    assert properties.message_id == "reject"
    assert properties.content_type == "application/json"
    assert properties.headers["x-kicak-dead-letter-reason"] == "malformed"
    assert properties.headers["x-kicak-original-publisher"] == config.rabbitmq_username


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_retries_a_transient_failure_through_its_retry_queue(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    flaky = IntegrationMessage(message_id="flaky", payload="flaky")
    behind = IntegrationMessage(message_id="behind", payload="behind")

    with RecordingTerminator(
        config,
        queue_type=queue_type,
        max_processing_attempts=1,
        retry_delay_seconds=1,
        **names,
    ) as executor:
        with OneShotInjector(config, inject_exchange, flaky) as injector:
            injector.publish(flaky)
            injector.publish(behind)
        moved_at = time.monotonic()
        executor.handle_next_delivery(names["source_queue"])  # fails: moved to the retry queue
        executor.handle_next_delivery(names["source_queue"])  # the next one is not held up
        executor.handle_next_delivery(names["source_queue"])  # back after the retry delay
        returned_after = time.monotonic() - moved_at
        returned = executor.last_properties
        dead_letter = executor.channel.basic_get(_dead_letter_queue(names), auto_ack=True)

    assert executor.received == [flaky, behind, flaky]
    assert executor.get_info().counts == ExecutorCounts(completed=2, moved_to_retry=1)
    assert returned_after >= 0.9
    assert returned.headers["x-kicak-retry-round"] == 1
    assert returned.expiration is None  # RabbitMQ removes it on the way back
    assert returned.message_id == "flaky"
    assert returned.user_id == config.rabbitmq_username
    assert returned.headers["x-kicak-original-publisher"] == config.rabbitmq_username
    assert dead_letter[0] is None
    assert _ready_message_count(config, f"{names['source_queue']}.retry") == (0, 0)


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_retry_copy_of_a_message_with_cc_headers_waits_in_the_retry_queue(
    config, topology, queue_type
):
    # Another publisher's CC header names the source queue; a copy keeping it would reach the
    # source queue at once, besides the retry queue, and skip the retry delay
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    source_queue = names["source_queue"]
    message = IntegrationMessage(message_id="routed", payload="unavailable")

    with RecordingTerminator(
        config,
        queue_type=queue_type,
        max_processing_attempts=1,
        retry_delay_seconds=30,
        **names,
    ) as executor:
        executor.channel.basic_publish(
            exchange=inject_exchange,  # a fanout exchange, which ignores CC itself
            routing_key="",
            body=message.to_bytes(),
            properties=pika.BasicProperties(
                type=IntegrationMessage.kicak_type,
                message_id="routed",
                headers={"CC": [source_queue], "BCC": [source_queue]},
            ),
        )
        executor.handle_next_delivery(source_queue)  # fails: moved to the retry queue
        _wait_for_ready_messages(config, f"{source_queue}.retry")
        time.sleep(0.5)  # a copy routed to the source queue too would be there by now
        # Stop consuming, so a copy the consumer was sent returns to the queue and is counted
        executor.channel.cancel()

        assert executor.get_info().counts == ExecutorCounts(moved_to_retry=1)
        assert _ready_message_count(config, source_queue)[0] == 0
        assert _ready_message_count(config, f"{source_queue}.retry")[0] == 1


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_dead_letters_a_message_that_used_up_its_retry_rounds(
    config, topology, queue_type
):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="unavailable", payload="unavailable")

    with RecordingTerminator(
        config,
        queue_type=queue_type,
        max_processing_attempts=1,
        max_retry_rounds=1,
        retry_delay_seconds=0.2,
        **names,
    ) as executor:
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        executor.handle_next_delivery(names["source_queue"])  # fails: moved to the retry queue
        executor.handle_next_delivery(names["source_queue"])  # fails again: no rounds left
        properties, body = _get_dead_letter(executor.channel, _dead_letter_queue(names))

    assert executor.received == [message, message]
    assert executor.get_info().counts == ExecutorCounts(
        dead_lettered=DeadLetterCounts(retries_exhausted=1), moved_to_retry=1
    )
    assert IntegrationMessage.from_bytes(body) == message
    headers = properties.headers
    assert headers["x-kicak-dead-letter-reason"] == "retries-exhausted"
    assert headers["x-kicak-error-type"] == "pykicak.executor.TransientProcessingError"
    assert headers["x-kicak-error-message"] == "service unavailable"
    assert headers["x-kicak-retry-round"] == 1
    assert datetime.fromisoformat(headers["x-kicak-retrying-since"]).tzinfo is not None
    assert _ready_message_count(config, f"{names['source_queue']}.retry") == (0, 0)


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_dead_letters_sent_back_by_the_example_tool_get_their_retry_rounds_again(
    config, topology, queue_type
):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    source_queue = names["source_queue"]
    unavailable = IntegrationMessage(message_id="unavailable", payload="unavailable")
    rejected = IntegrationMessage(message_id="reject", payload="reject")

    with RecordingTerminator(
        config,
        queue_type=queue_type,
        max_processing_attempts=1,
        max_retry_rounds=1,
        retry_delay_seconds=0.2,
        **names,
    ) as executor:
        with OneShotInjector(config, inject_exchange, unavailable) as injector:
            injector.publish(unavailable)
            injector.publish(rejected)
        executor.handle_next_delivery(source_queue)  # unavailable: moved to the retry queue
        executor.handle_next_delivery(source_queue)  # rejected: dead-lettered as malformed
        executor.handle_next_delivery(source_queue)  # unavailable again: dead-lettered
        _wait_for_ready_messages(config, _dead_letter_queue(names), 2)

        dry_run = replay_dead_letters(config, source_queue, {"retries-exhausted"}, dry_run=True)
        assert _ready_message_count(config, _dead_letter_queue(names)) == (2, 0)
        report = replay_dead_letters(config, source_queue, {"retries-exhausted"})

        # The malformed message stays in the dead-letter queue
        assert _ready_message_count(config, _dead_letter_queue(names)) == (1, 0)
        executor.handle_next_delivery(source_queue)  # sent back: its rounds start again
        sent_back = executor.last_properties
        executor.handle_next_delivery(source_queue)  # back from the retry queue: no rounds left
        properties, _body = _get_dead_letter(executor.channel, _dead_letter_queue(names))
        if properties.headers["x-kicak-dead-letter-reason"] == "malformed":
            properties, _body = _get_dead_letter(executor.channel, _dead_letter_queue(names))

    assert dry_run.sent_back == {"retries-exhausted": 1}
    assert report.sent_back == {"retries-exhausted": 1}
    assert report.left == {"malformed": 1}
    # Sent back as it was dead-lettered, by the tool's own (verified) user
    assert sent_back.message_id == "unavailable"
    assert sent_back.type == IntegrationMessage.kicak_type
    assert sent_back.user_id == config.rabbitmq_username
    assert sent_back.headers["x-kicak-dead-letter-reason"] == "retries-exhausted"
    assert sent_back.headers["x-kicak-original-publisher"] == config.rabbitmq_username
    # It was moved to the retry queue once more, as round 1, then dead-lettered again
    assert executor.get_info().counts == ExecutorCounts(
        dead_lettered=DeadLetterCounts(malformed=1, retries_exhausted=2), moved_to_retry=2
    )
    assert properties.headers["x-kicak-dead-letter-reason"] == "retries-exhausted"
    assert properties.headers["x-kicak-retry-round"] == 1


@pytest.mark.parametrize(
    ("queue_type", "other_type"),
    [(QueueType.CLASSIC, QueueType.QUORUM), (QueueType.QUORUM, QueueType.CLASSIC)],
)
def test_executor_declares_both_queues_with_the_requested_type(
    config, topology, queue_type, other_type
):
    names = topology.executor_topology("source", topology.exchange("inject"))

    with RecordingTerminator(config, queue_type=queue_type, **names):
        pass

    # RabbitMQ reports no queue type on declare, but rejects a redeclaration with another type.
    connection = _open_connection(config)
    try:
        retry_arguments = {
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": names["source_queue"],
        }
        for queue, arguments in (
            (names["source_queue"], {"x-dead-letter-exchange": _dead_letter_exchange(names)}),
            (_dead_letter_queue(names), {}),
            (f"{names['source_queue']}.retry", retry_arguments),
        ):
            if queue.endswith(".retry") and queue_type == QueueType.QUORUM:
                arguments = {
                    **arguments,
                    "x-dead-letter-strategy": "at-least-once",
                    "x-overflow": "reject-publish",
                }
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
            arguments={"x-dead-letter-exchange": _dead_letter_exchange(names)},
        )
        channel.queue_declare(queue=_dead_letter_queue(names), durable=True)
    finally:
        connection.close()

    with RecordingTerminator(config, **names) as executor:
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


def _wait_for_ready_messages(config: KicakConfig, queue: str, count: int = 1) -> None:
    deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
    while _ready_message_count(config, queue)[0] < count:
        assert time.monotonic() < deadline, f"fewer than {count} message(s) reached {queue}"
        time.sleep(0.1)


def _with_broker_channel(config: KicakConfig, action) -> None:
    """Run `action(channel)` on a separate connection, e.g. to delete topology like an operator."""
    connection = _open_connection(config)
    try:
        action(connection.channel())
    finally:
        connection.close()


def _publish_through_running_executor(
    config: KicakConfig, topology: TemporaryTopology, damage
) -> tuple[RecordingExecutor, dict[str, str], dict[str, str], dict[str, str]]:
    """Start an executor publishing to two downstream queues, `damage` the topology, inject.

    Returns the stopped executor and the topology names of its input and both downstream queues.
    """
    inject_exchange = topology.exchange("inject")
    results_exchange = topology.exchange("results")
    names = topology.executor_topology("source", inject_exchange)
    downstream_a = topology.executor_topology("downstream-a", results_exchange)
    downstream_b = topology.executor_topology("downstream-b", results_exchange)
    executor = RecordingExecutor(
        config,
        destination_exchange=results_exchange,
        destination_queues=[downstream_a["source_queue"], downstream_b["source_queue"]],
        **names,
    )
    thread, errors = _start_in_thread(executor)
    try:
        _wait_until_consuming(config, names["source_queue"])
        _with_broker_channel(
            config, lambda channel: damage(channel, results_exchange, downstream_a, downstream_b)
        )
        message = IntegrationMessage(message_id="hello", payload="hello")
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        _wait_for_ready_messages(config, downstream_a["source_queue"])
        _wait_for_ready_messages(config, downstream_b["source_queue"])
    finally:
        executor.stop()
        thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)
    assert not thread.is_alive()
    assert errors == []
    assert executor.get_info().status == "STOPPED"
    assert executor.received == [message]
    assert _ready_message_count(config, names["source_queue"]) == (0, 0)  # input acknowledged
    return executor, names, downstream_a, downstream_b


def test_executor_declares_and_binds_its_downstream_queues_before_any_consumer_exists(
    config, topology
):
    _executor, _names, downstream_a, downstream_b = _publish_through_running_executor(
        config, topology, lambda *_: None
    )

    assert _ready_message_count(config, downstream_a["source_queue"]) == (1, 0)
    assert _ready_message_count(config, downstream_b["source_queue"]) == (1, 0)


def test_executor_restores_a_deleted_destination_exchange_for_every_queue(config, topology):
    def delete_exchange(channel, results_exchange, _downstream_a, _downstream_b) -> None:
        channel.exchange_delete(exchange=results_exchange)  # removes both bindings

    _executor, _names, downstream_a, downstream_b = _publish_through_running_executor(
        config, topology, delete_exchange
    )

    assert _ready_message_count(config, downstream_a["source_queue"]) == (1, 0)
    assert _ready_message_count(config, downstream_b["source_queue"]) == (1, 0)


def test_executor_restores_a_removed_downstream_binding(config, topology):
    def unbind_one(channel, results_exchange, downstream_a, _downstream_b) -> None:
        channel.queue_unbind(queue=downstream_a["source_queue"], exchange=results_exchange)

    _executor, _names, downstream_a, downstream_b = _publish_through_running_executor(
        config, topology, unbind_one
    )

    assert _ready_message_count(config, downstream_a["source_queue"]) == (1, 0)
    assert _ready_message_count(config, downstream_b["source_queue"]) == (1, 0)


def test_executor_recreates_a_deleted_downstream_queue_as_its_own_executor_declares_it(
    config, topology
):
    def delete_queue(channel, _results_exchange, downstream_a, _downstream_b) -> None:
        channel.queue_delete(queue=downstream_a["source_queue"])

    _executor, _names, downstream_a, _downstream_b = _publish_through_running_executor(
        config, topology, delete_queue
    )

    # Its own executor declares it with the same arguments, so it connects without 406
    with RecordingTerminator(config, **downstream_a) as downstream_executor:
        downstream_executor.handle_next_delivery(downstream_a["source_queue"])
    assert [m.message_id for m in downstream_executor.received] == ["hello"]


def _as_topology_class[T: _ExecutorTopology](
    topology: _ExecutorTopology, topology_class: type[T]
) -> T:
    """Return `topology` as an instance of `topology_class`, which changes how it is declared."""
    fields = {field.name: getattr(topology, field.name) for field in dataclasses.fields(topology)}
    return topology_class(**fields)


class _RacingTopology(_ExecutorTopology):
    """Loses the downstream bindings right after declaring the output side the second time."""

    output_declarations = 0

    def declare_output_side(self, channel) -> None:
        super().declare_output_side(channel)
        # The topology is a frozen dataclass, whose __setattr__ fails for a subclass on Python
        # 3.12 when it is slotted
        object.__setattr__(self, "output_declarations", self.output_declarations + 1)
        if self.output_declarations == 2:  # 1 is on connecting, 2 right before publishing
            for queue in self.destination_queues:
                channel.queue_unbind(queue=queue, exchange=self.destination_exchange)


class BindingRaceExecutor(RecordingExecutor):
    """Loses its downstream bindings right after verifying the output side the first time.

    That is the race an output-side verification cannot prevent: RabbitMQ then returns the
    results as unroutable at the commit.
    """

    def __init__(self, config: KicakConfig, **topology) -> None:
        super().__init__(config, **topology)
        self._topology = _as_topology_class(self._topology, _RacingTopology)

    @property
    def output_verifications(self) -> int:
        """How often it declared its output side."""
        return cast(_RacingTopology, self._topology).output_declarations


def test_executor_holds_unroutable_results_and_publishes_them_without_executing_again(
    config, topology
):
    inject_exchange = topology.exchange("inject")
    results_exchange = topology.exchange("results")
    names = topology.executor_topology("source", inject_exchange)
    downstream = topology.executor_topology("downstream", results_exchange)
    executor = BindingRaceExecutor(
        config,
        destination_exchange=results_exchange,
        destination_queues=[downstream["source_queue"]],
        **names,
    )
    thread, errors = _start_in_thread(executor)
    try:
        _wait_until_consuming(config, names["source_queue"])
        message = IntegrationMessage(message_id="hello", payload="hello")
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        _wait_for_ready_messages(config, downstream["source_queue"])
    finally:
        executor.stop()
        thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert executor.received == [message]  # held, not executed again
    assert executor.output_verifications == 3
    assert _ready_message_count(config, downstream["source_queue"]) == (1, 0)
    assert _ready_message_count(config, names["source_queue"]) == (0, 0)  # input acknowledged


def test_executor_restores_its_input_binding_while_waiting(config, topology):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    executor = RecordingTerminator(config, input_check_seconds=0, **names)
    thread, errors = _start_in_thread(executor)
    try:
        _wait_until_consuming(config, names["source_queue"])
        _with_broker_channel(
            config,
            lambda channel: channel.queue_unbind(
                queue=names["source_queue"], exchange=inject_exchange
            ),
        )
        message = IntegrationMessage(message_id="after", payload="after the unbinding")
        connection = _open_connection(config)
        try:
            channel = connection.channel()
            channel.confirm_delivery()
            deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
            while True:  # unroutable until the executor's idle verification binds it again
                try:
                    channel.basic_publish(
                        exchange=inject_exchange,
                        routing_key="",
                        body=message.to_bytes(),
                        properties=pika.BasicProperties(type=IntegrationMessage.kicak_type),
                        mandatory=True,
                    )
                    break
                except UnroutableError:
                    assert time.monotonic() < deadline, "the binding was never restored"
                    time.sleep(0.2)
        finally:
            connection.close()
        deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
        while not executor.received and time.monotonic() < deadline:
            time.sleep(0.1)
    finally:
        executor.stop()
        thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert executor.received == [message]


def test_executor_declares_its_dead_letter_exchange_again_before_dead_lettering(config, topology):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="reject", payload="reject")

    with RecordingTerminator(config, **names) as executor:
        _with_broker_channel(
            config, lambda channel: channel.exchange_delete(exchange=_dead_letter_exchange(names))
        )
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        executor.handle_next_delivery(names["source_queue"])
        properties, body = _get_dead_letter(executor.channel, _dead_letter_queue(names))

    assert IntegrationMessage.from_bytes(body) == message
    assert properties.headers["x-kicak-dead-letter-reason"] == "malformed"


class ResultSizeExecutor(RecordingExecutor):
    """Forwards each message; for the payload "huge", its result exceeds RabbitMQ's 16 MiB default
    max_message_size."""

    def execute(self, message: IntegrationMessage) -> IntegrationMessage:
        self.received.append(message)
        size = 17 * 1024 * 1024 if getattr(message, "payload", None) == "huge" else 1
        return IntegrationMessage(message_id=f"{message.message_id}.result", payload="x" * size)


def test_executor_dead_letters_input_whose_result_is_too_large(config, topology):
    inject_exchange = topology.exchange("inject")
    results_exchange = topology.exchange("results")
    names = topology.executor_topology("source", inject_exchange)
    downstream = topology.executor_topology("downstream", results_exchange)
    with RecordingTerminator(config, **downstream):  # declares and binds the downstream queue
        pass
    executor = ResultSizeExecutor(
        config,
        destination_exchange=results_exchange,
        destination_queues=[downstream["source_queue"]],
        **names,
    )
    thread, errors = _start_in_thread(executor)
    try:
        _wait_until_consuming(config, names["source_queue"])
        for payload in ("huge", "small"):
            message = IntegrationMessage(message_id=payload, payload=payload)
            with OneShotInjector(config, inject_exchange, message) as injector:
                injector.publish(message)
        deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
        while _ready_message_count(config, downstream["source_queue"])[0] == 0:
            assert time.monotonic() < deadline, "the small result never reached downstream"
            time.sleep(0.1)
    finally:
        executor.stop()
        thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert executor.get_info().status == "STOPPED"
    assert _ready_message_count(config, downstream["source_queue"]) == (1, 0)  # small result only
    connection = _open_connection(config)
    try:
        properties, body = _get_dead_letter(connection.channel(), _dead_letter_queue(names))
    finally:
        connection.close()
    assert IntegrationMessage.from_bytes(body).message_id == "huge"
    assert properties.headers["x-kicak-dead-letter-reason"] == "result-too-large"


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_publishes_every_chunk_before_acknowledging_the_input(
    config, topology, queue_type
):
    inject_exchange = topology.exchange("inject")
    chunks_exchange = topology.exchange("chunks")
    chunker = topology.executor_topology("chunker", inject_exchange)
    analyser = topology.executor_topology("analyser", chunks_exchange)
    document = IntegrationMessage(message_id="doc-1", payload="a large document")

    with RecordingTerminator(config, queue_type=queue_type, **analyser) as analyser_executor:
        with ChunkingExecutor(
            config,
            destination_exchange=chunks_exchange,
            destination_queues=[analyser["source_queue"]],
            queue_type=queue_type,
            **chunker,
        ) as chunker_executor:
            with OneShotInjector(config, inject_exchange, document) as injector:
                injector.publish(document)
            chunker_executor.handle_next_delivery(chunker["source_queue"])
        for _ in range(3):
            analyser_executor.handle_next_delivery(analyser["source_queue"])

    assert [chunk.message_id for chunk in analyser_executor.received] == [
        "doc-1.chunk-1",
        "doc-1.chunk-2",
        "doc-1.chunk-3",
    ]
    assert _ready_message_count(config, chunker["source_queue"]) == (0, 0)  # input acknowledged


@pytest.mark.parametrize("queue_type", list(QueueType))
def test_executor_takes_one_message_at_a_time(config, topology, queue_type):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)

    with RecordingTerminator(config, queue_type=queue_type, **names) as executor:
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


def test_executor_declares_its_queue_again_when_it_is_deleted(config, topology):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    executor = RecordingTerminator(config, **names)
    thread, errors = _start_in_thread(executor)
    try:
        _wait_until_consuming(config, names["source_queue"])
        connection = _open_connection(config)
        try:
            connection.channel().queue_delete(queue=names["source_queue"])
        finally:
            connection.close()
        _wait_until_consuming(config, names["source_queue"])  # declared and consumed again
        message = IntegrationMessage(message_id="after", payload="after the deletion")
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)  # routed: the binding was declared again too
        deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
        while not executor.received and time.monotonic() < deadline:
            time.sleep(0.1)
    finally:
        executor.stop()
        thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert executor.received == [message]
    assert executor.get_info().status == "STOPPED"


@pytest.mark.parametrize("heartbeat_seconds", [None, 900], ids=["default", "configured"])
def test_executor_connection_uses_requested_heartbeat(config, topology, heartbeat_seconds):
    names = topology.executor_topology("source", topology.exchange("inject"))
    if heartbeat_seconds is not None:
        names["heartbeat_seconds"] = heartbeat_seconds

    with RecordingTerminator(config, **names) as executor:
        assert executor._connection is not None
        negotiated = executor._connection._impl.params.heartbeat

    assert negotiated == (heartbeat_seconds or 600)


class OutlastingTerminator(RecordingTerminator):
    """Outlasts its heartbeat timeout on the first execute(), then stops after the next one."""

    def execute(self, message: IntegrationMessage) -> None:
        if not self.received:
            time.sleep(5)  # RabbitMQ closes the connection meanwhile (heartbeat_seconds=1)
        else:
            self.stop()
        super().execute(message)


def test_terminator_reconnects_when_execute_outlasts_the_heartbeat(config, topology):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    executor = OutlastingTerminator(config, heartbeat_seconds=1, **names)
    thread, errors = _start_in_thread(executor)
    _wait_until_consuming(config, names["source_queue"])
    message = IntegrationMessage(message_id="slow", payload="slow")
    with OneShotInjector(config, inject_exchange, message) as injector:
        injector.publish(message)

    thread.join(timeout=30)

    assert not thread.is_alive()
    assert errors == []
    # The acknowledgement failed on the closed connection; the redelivery was executed again
    assert executor.received == [message, message]
    assert executor.get_info().status == "STOPPED"
    assert _ready_message_count(config, names["source_queue"]) == (0, 0)


_CONSUMER_TIMEOUT_MILLISECONDS = 1000
_CONSUMER_TIMEOUT_WAIT_SECONDS = 90
"""How long a test waits for RabbitMQ to end a delivery that outlasted its consumer timeout.

RabbitMQ 4.2 checks the timeout on each channel's tick, once a minute by default: on RabbitMQ
4.2.5, a 2-second timeout closed the channel 60 seconds after the delivery, and 3 seconds after it
with `{rabbit, [{channel_tick_interval, 1000}]}` in `advanced.config`, which the CI broker sets. On
RabbitMQ 4.4.0, a quorum queue cancelled the consumer of a delivery held past a 1-second timeout
about 11 seconds after the delivery, whatever the tick.
"""


def _broker_version(config: KicakConfig) -> tuple[int, int]:
    """Return RabbitMQ's major and minor version, as it reports them to a connection."""
    connection = _open_connection(config)
    try:
        version = connection._impl.server_properties["version"]
    finally:
        connection.close()
    if isinstance(version, bytes):
        version = version.decode()
    major, minor = version.split(".")[:2]
    return int(major), int(minor)


def _timing_out_queue_type(config: KicakConfig) -> QueueType:
    """Return the queue type whose consumer timeout the broker under test enforces.

    RabbitMQ 4.3 stopped evaluating consumer timeouts for classic queues, and refuses their
    `x-consumer-timeout` argument; quorum queues return a timed-out delivery to the queue and
    cancel its consumer, keeping the channel open. RabbitMQ 4.2 closes the channel instead, and
    there `_wait_until_unconsumed()` needs a classic queue: a quorum queue kept listing the closed
    channel's consumer for longer than the test waits (measured on 4.2.9).
    """
    return QueueType.CLASSIC if _broker_version(config) < (4, 3) else QueueType.QUORUM


class _ConsumerTimeoutChannel:
    """Wraps a channel so that the queues declared on it get a short `x-consumer-timeout`."""

    def __init__(self, channel) -> None:
        self._channel = channel

    def queue_declare(self, *, arguments: dict[str, object], **parameters):
        timeout = {"x-consumer-timeout": _CONSUMER_TIMEOUT_MILLISECONDS}
        return self._channel.queue_declare(arguments={**arguments, **timeout}, **parameters)


class _TimingOutTopology(_ExecutorTopology):
    """Declares the source queue with a short `x-consumer-timeout`, and every other one as usual.

    The other queues, e.g. a destination queue, are declared as their own consumers declare them.
    """

    def declare_input_queue(self, channel, queue_name: str) -> None:
        if queue_name == self.source_queue:
            channel = _ConsumerTimeoutChannel(channel)
        super().declare_input_queue(channel, queue_name)


class TimedOutTerminator(RecordingTerminator):
    """Holds its first message until RabbitMQ's consumer timeout takes it back, then stops after
    executing the redelivery.

    Its source queue is declared with the library's arguments plus a short `x-consumer-timeout`.
    While it holds the message it does not use its own connection, like an `execute()` busy with
    its work, so its acknowledgement is what meets the channel RabbitMQ closed (RabbitMQ 4.2), or
    the cancelled consumer (4.3 and newer).
    """

    def __init__(self, config: KicakConfig, **topology) -> None:
        super().__init__(config, **topology)
        self._topology = _as_topology_class(self._topology, _TimingOutTopology)
        self._test_config = config
        self.redelivered: list[bool | None] = []  # as get_info() reports it for each execution

    def execute(self, message: IntegrationMessage) -> None:
        self.redelivered.append(self.get_info().redelivered)
        if not self.received:
            _wait_until_unconsumed(self._test_config, self._topology.source_queue)
        else:
            self.stop()
        super().execute(message)


class TimedOutExecutor(RecordingExecutor):
    """Like TimedOutTerminator, but publishes each message it receives as its result.

    Only its source queue gets the short `x-consumer-timeout`; its destination queue is declared
    as its own consumer declares it.
    """

    def __init__(self, config: KicakConfig, **topology) -> None:
        super().__init__(config, **topology)
        self._topology = _as_topology_class(self._topology, _TimingOutTopology)
        self._test_config = config

    def execute(
        self, message: IntegrationMessage
    ) -> IntegrationMessage | Sequence[IntegrationMessage] | None:
        if not self.received:
            _wait_until_unconsumed(self._test_config, self._topology.source_queue)
        else:
            self.stop()
        return super().execute(message)


def _wait_until_unconsumed(config: KicakConfig, queue: str) -> None:
    """Wait until `queue` has no consumer, as RabbitMQ shows it to a connection of the test."""
    connection = _open_connection(config)
    try:
        channel = connection.channel()
        deadline = time.monotonic() + _CONSUMER_TIMEOUT_WAIT_SECONDS
        while time.monotonic() < deadline:
            if channel.queue_declare(queue=queue, passive=True).method.consumer_count == 0:
                return
            connection.sleep(0.2)
    finally:
        connection.close()
    raise AssertionError(
        f"RabbitMQ did not end the delivery on {queue} within {_CONSUMER_TIMEOUT_WAIT_SECONDS}s"
    )


def test_terminator_reconnects_when_its_delivery_outlasts_the_consumer_timeout(
    config, topology, caplog
):
    queue_type = _timing_out_queue_type(config)
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    executor = TimedOutTerminator(config, queue_type=queue_type, **names)
    thread, errors = _start_in_thread(executor)
    _wait_until_consuming(config, names["source_queue"])
    message = IntegrationMessage(message_id="held", payload="held")
    with OneShotInjector(config, inject_exchange, message) as injector:
        injector.publish(message)

    thread.join(timeout=_CONSUMER_TIMEOUT_WAIT_SECONDS + 30)

    assert not thread.is_alive()
    assert errors == []  # not CRASHED: the timeout was not mistaken for a fatal channel error
    [warning] = [
        record
        for record in caplog.records
        if record.name == "pykicak.executor" and "while acknowledging" in record.getMessage()
    ]
    assert warning.levelname == "WARNING"
    if queue_type is QueueType.CLASSIC:
        # RabbitMQ 4.2: the acknowledgement met the channel RabbitMQ closed, recognized by its
        # reply text...
        assert "406" in warning.getMessage()
        assert "timed out" in warning.getMessage()
    else:
        # RabbitMQ 4.3+: RabbitMQ ignored the acknowledgement and cancelled the consumer...
        assert "cancelled the consumer" in warning.getMessage()
    # ...so the executor reconnected and executed the redelivered message again
    assert executor.received == [message, message]
    assert executor.redelivered == [False, True]
    info = executor.get_info()
    assert info.status == "STOPPED"
    assert info.counts == ExecutorCounts(completed=1, abandoned=1)
    assert _ready_message_count(config, names["source_queue"]) == (0, 0)


def test_executor_commits_no_results_for_a_delivery_that_outlasted_the_consumer_timeout(
    config, topology, caplog
):
    queue_type = _timing_out_queue_type(config)
    inject_exchange = topology.exchange("inject")
    results_exchange = topology.exchange("results")
    names = topology.executor_topology("source", inject_exchange)
    downstream = topology.executor_topology("downstream", results_exchange)
    with RecordingTerminator(config, queue_type=queue_type, **downstream):
        pass  # declares the downstream queue as its own consumer does
    executor = TimedOutExecutor(
        config,
        queue_type=queue_type,
        destination_exchange=results_exchange,
        destination_queues=[downstream["source_queue"]],
        **names,
    )
    thread, errors = _start_in_thread(executor)
    _wait_until_consuming(config, names["source_queue"])
    message = IntegrationMessage(message_id="held", payload="held")
    with OneShotInjector(config, inject_exchange, message) as injector:
        injector.publish(message)

    thread.join(timeout=_CONSUMER_TIMEOUT_WAIT_SECONDS + 30)

    assert not thread.is_alive()
    assert errors == []
    # RabbitMQ had taken the input back, so the results of the timed-out execution were not
    # committed: the redelivery's are the only ones published
    [warning] = [
        record
        for record in caplog.records
        if record.name == "pykicak.executor"
        and "while publishing results of message id=held" in record.getMessage()
    ]
    assert warning.levelname == "WARNING"
    assert executor.received == [message, message]
    assert executor.get_info().status == "STOPPED"
    assert _ready_message_count(config, downstream["source_queue"]) == (1, 0)
    assert _ready_message_count(config, names["source_queue"]) == (0, 0)


class SlowExecutor(RecordingExecutor):
    """Takes `EXECUTE_SECONDS` per message and signals when processing has started."""

    EXECUTE_SECONDS = 1.5

    def __init__(self, config: KicakConfig, **topology) -> None:
        super().__init__(config, **topology)
        self.started = threading.Event()

    def execute(
        self, message: IntegrationMessage
    ) -> IntegrationMessage | Sequence[IntegrationMessage] | None:
        self.started.set()
        time.sleep(self.EXECUTE_SECONDS)
        return super().execute(message)


def _start_in_thread(
    executor: KicakExecutorAbstract[Any, Any],
) -> tuple[threading.Thread, list[BaseException]]:
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
    results_exchange = topology.exchange("results")
    names = topology.executor_topology("source", inject_exchange)
    downstream = topology.executor_topology("downstream", results_exchange)
    with RecordingTerminator(config, **downstream):  # declares and binds the downstream queue
        pass
    executor = SlowExecutor(
        config,
        destination_exchange=results_exchange,
        destination_queues=[downstream["source_queue"]],
        **names,
    )
    thread, errors = _start_in_thread(executor)
    _wait_until_consuming(config, names["source_queue"])
    first = IntegrationMessage(message_id="first", payload="first")
    second = IntegrationMessage(message_id="second", payload="second")
    with OneShotInjector(config, inject_exchange, first) as injector:
        injector.publish(first)
        injector.publish(second)

    assert executor.started.wait(_DELIVERY_TIMEOUT_SECONDS)
    executor.stop()  # while execute() is still running
    thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert executor.received == [first]
    assert executor.get_info().status == "STOPPED"
    # The message in progress was finished: its result was published and it was acknowledged
    assert _ready_message_count(config, downstream["source_queue"]) == (1, 0)
    assert _ready_message_count(config, names["source_queue"]) == (1, 0)  # only `second` is left


def test_stop_while_waiting_returns_promptly(config, topology):
    names = topology.executor_topology("source", topology.exchange("inject"))
    executor = RecordingTerminator(config, **names)
    thread, errors = _start_in_thread(executor)
    _wait_until_consuming(config, names["source_queue"])

    stop_requested_at = time.monotonic()
    executor.stop()
    thread.join(timeout=_DELIVERY_TIMEOUT_SECONDS)

    assert not thread.is_alive()
    assert errors == []
    assert time.monotonic() - stop_requested_at < 3
    assert executor.get_info().status == "STOPPED"
    assert _ready_message_count(config, names["source_queue"]) == (0, 0)


def test_injector_run_publishes_on_a_short_lived_connection(config, topology):
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="injected-by-run", payload="injected by run")

    with RecordingTerminator(config, **names) as executor:  # installs the queue and binding
        injector = OneShotInjector(config, inject_exchange, message)
        injector.run()
        executor.handle_next_delivery(names["source_queue"])

    assert executor.received == [message]
    assert injector._connection is None


def test_statistics_lines_of_an_injection_and_its_delivery_agree(config, topology, caplog):
    caplog.set_level("INFO", logger="pykicak")
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="measured", payload="measured")

    with RecordingTerminator(config, **names) as executor:
        OneShotInjector(config, inject_exchange, message).run()
        executor.handle_next_delivery(names["source_queue"])

    [injected] = [vars(r)["kicak"] for r in caplog.records if r.message.startswith("Message inj")]
    [handled] = [vars(r)["kicak"] for r in caplog.records if r.message.startswith("Message hand")]
    assert (injected["outcome"], injected["attempts"]) == ("injected", 1)
    assert injected["connect_seconds"] > 0  # a real connection, opened for this injection
    assert injected["publish_seconds"] > 0  # until RabbitMQ confirmed it
    assert handled["outcome"] == "completed"
    # The body crossed the broker unchanged, and its publish time came with it
    assert handled["input_bytes"] == injected["message_bytes"] == len(message.to_bytes())
    assert handled["queued_seconds"] is not None
    assert -1 < handled["queued_seconds"] < 60  # both stamps come from this host's clock


def test_injector_run_raises_after_closing_when_no_queue_is_bound(config, topology):
    injector = OneShotInjector(
        config, topology.exchange("unbound"), IntegrationMessage(message_id="lost", payload="lost")
    )

    with pytest.raises(InjectionError) as raised:
        injector.run()

    assert isinstance(raised.value.__cause__, UnroutableError)
    assert injector._connection is None
