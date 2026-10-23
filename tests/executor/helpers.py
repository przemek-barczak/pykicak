"""Messages, executors, and helpers the Executor's tests share."""

import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict

import pytest
from pika.exceptions import StreamLostError

from pykicak.config import KicakConfig
from pykicak.executor import (
    KicakExecutorAbstract,
    TransientProcessingError,
)
from pykicak.messages import KicakMessage
from tests.fakes import (
    SAMPLE_TYPE,
    FakeChannel,
    FakeConnection,
    PublishedMessage,
    StopConsuming,
)


@dataclasses.dataclass(frozen=True, slots=True)
class SampleMessage(KicakMessage):
    kicak_type = SAMPLE_TYPE
    text: str


class Topology(TypedDict):
    """The arguments every Executor requires: its source and `max_retry_rounds`."""

    source_queue: str
    source_exchange: str
    max_retry_rounds: int


MAX_RETRY_ROUNDS = 10
"""The `max_retry_rounds` of the default TOPOLOGY, above the rounds the other tests use."""


TOPOLOGY: Topology = {
    "source_queue": "input-queue",
    "source_exchange": "input-exchange",
    "max_retry_rounds": MAX_RETRY_ROUNDS,
}


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


def unavailable(_message):
    raise TransientProcessingError("service unavailable")


def retry_copies(channel: FakeChannel) -> list[PublishedMessage]:
    """The messages published to the retry queue of the default TOPOLOGY."""
    return [
        published
        for published in channel.published_messages
        if published.exchange == "" and published.routing_key == "input-queue.retry"
    ]


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


def raise_error(error: Exception):
    """execute() handler raising `error`."""

    def handler(_message):
        raise error

    return handler


class UnroutableResultsChannel(FakeChannel):
    """Results channel whose commits are unroutable or routed, in the order given."""

    def __init__(self, *unroutable_commits: bool) -> None:
        super().__init__()
        self._unroutable_commits = list(unroutable_commits)

    def tx_commit(self) -> None:
        self.unroutable = self._unroutable_commits.pop(0)
        super().tx_commit()


CLOCK_START = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)
"""The time a FakeClock starts at."""


class FakeClock:
    """Stands in for `_now()`; tests move the time forward explicitly."""

    def __init__(self) -> None:
        self.current = CLOCK_START

    def advance(self, seconds: float = 1.0) -> datetime:
        self.current += timedelta(seconds=seconds)
        return self.current

    def __call__(self) -> datetime:
        return self.current


def connection_lost() -> StreamLostError:
    return StreamLostError("connection lost")


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


def handled_records(caplog) -> list[Any]:
    return [record for record in caplog.records if record.message.startswith("Message handled")]
