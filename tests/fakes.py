"""Hand-written test doubles standing in for pika's connection/channel objects."""

from __future__ import annotations

import dataclasses
import logging
import sys
import time
from collections.abc import Callable
from typing import Any

import pika
import pytest
from pika.exceptions import ChannelClosedByBroker

from pykicak.abstracts import KicakAbstract

CONNECTION_VALUES = {
    "RABBIT_MQ_USERNAME": "guest",
    "RABBIT_MQ_PASSWORD": "guest",
    "RABBIT_MQ_HOST": "localhost",
    "RABBIT_MQ_PORT": "5672",
    "RABBIT_MQ_VIRTUAL_HOST": "/",
    "RABBIT_MQ_TLS": "false",
}
"""Config values that let KicakAbstract._create_connection() build connection parameters."""

SAMPLE_TYPE = "sample.v1"
"""The `kicak_type` of the tests' SampleMessage, and the `type` property of a fake delivery queued
without properties, so that an Executor accepting SampleMessage decodes it."""


def captured_connection_parameters(agent: KicakAbstract, monkeypatch) -> pika.ConnectionParameters:
    """Return the parameters `agent` would open a real connection with, without connecting."""
    captured: list[pika.ConnectionParameters] = []
    monkeypatch.setattr(pika, "BlockingConnection", captured.append)
    KicakAbstract._create_connection(agent)
    return captured[0]


@dataclasses.dataclass
class DeclaredQueue:
    queue: str
    durable: bool
    exclusive: bool
    auto_delete: bool
    arguments: dict[str, object]


@dataclasses.dataclass
class DeclaredExchange:
    exchange: str
    exchange_type: str
    durable: bool


@dataclasses.dataclass
class BoundQueue:
    queue: str
    exchange: str


class StopConsuming(BaseException):
    """Raised by FakeChannel.consume() when its deliveries run out.

    Stands in for the external stop mechanism (e.g. SystemExit from a signal handler): like it,
    it is a BaseException, so the executor does not catch it.
    """


@dataclasses.dataclass
class PublishedMessage:
    exchange: str
    routing_key: str
    body: bytes
    properties: pika.BasicProperties
    mandatory: bool = False


@dataclasses.dataclass
class NackedDelivery:
    delivery_tag: int
    requeue: bool


@dataclasses.dataclass
class ConsumedMethod:
    delivery_tag: int
    redelivered: bool = False


class FakeChannelImpl:
    """Stands in for the pika channel that a BlockingChannel wraps as `_impl`."""

    def __init__(self) -> None:
        self.cancel_callbacks: list[Callable[[object], None]] = []

    def add_on_cancel_callback(self, callback: Callable[[object], None]) -> None:
        """`callback(method_frame)` runs when RabbitMQ cancels a consumer, as in pika."""
        self.cancel_callbacks.append(callback)


class FakeChannel:
    """Records calls made against it and can be fed canned consume() output.

    Like RabbitMQ, a channel is in confirm mode or transaction mode, never both. In transaction
    mode, published messages reach `published_messages` only at `tx_commit()`; if `unroutable`
    is set, the commit returns them instead, to the return callbacks registered with
    `add_on_return_callback()`, which run when the connection processes events (as in pika).
    """

    def __init__(self) -> None:
        self.is_open = True
        self.declared_queues: list[DeclaredQueue] = []
        self.declared_exchanges: list[DeclaredExchange] = []
        self.bound_queues: list[BoundQueue] = []
        self.published_messages: list[PublishedMessage] = []
        self.acked_delivery_tags: list[int] = []
        self.nacked_deliveries: list[NackedDelivery] = []
        self.operations: list[str] = []
        self.confirm_delivery_enabled = False
        self.prefetch_count: int | None = None
        self.prefetch_count_when_consuming: int | None = None
        self.inactivity_timeout: float | None = None
        self.before_delivery: Callable[[int], None] | None = None
        self.on_idle: Callable[[], None] | None = None
        self._impl = FakeChannelImpl()
        self._pending_deliveries: list[tuple[bytes | None, pika.BasicProperties, bool]] = []
        self._delivered_delivery_tags: set[int] = set()
        self.transactional = False
        self.tx_select_calls = 0
        self.unroutable = False
        self.results_channel: FakeChannel | None = None
        self._uncommitted: list[PublishedMessage] = []
        self._return_callbacks: list[Callable[..., None]] = []
        self._pending_returns: list[PublishedMessage] = []

    def queue_declare(
        self,
        queue: str,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict[str, object] | None = None,
    ) -> None:
        assert arguments is not None, "pykicak declares every queue with x-queue-type"
        self.declared_queues.append(
            DeclaredQueue(queue, durable, exclusive, auto_delete, arguments)
        )

    def exchange_declare(self, exchange: str, exchange_type: str, durable: bool = False) -> None:
        self.declared_exchanges.append(DeclaredExchange(exchange, exchange_type, durable))

    def queue_bind(self, queue: str, exchange: str) -> None:
        self.bound_queues.append(BoundQueue(queue, exchange))

    def confirm_delivery(self) -> None:
        if self.transactional:
            raise ChannelClosedByBroker(
                406, "PRECONDITION_FAILED - cannot switch from tx to confirm mode"
            )
        self.confirm_delivery_enabled = True

    def tx_select(self) -> None:
        if self.confirm_delivery_enabled:
            raise ChannelClosedByBroker(
                406, "PRECONDITION_FAILED - cannot switch from confirm to tx mode"
            )
        self.transactional = True
        self.tx_select_calls += 1

    def tx_commit(self) -> None:
        if not self.transactional:
            raise ChannelClosedByBroker(406, "PRECONDITION_FAILED - channel is not transactional")
        self.operations.append("commit")
        committed, self._uncommitted = self._uncommitted, []
        if self.unroutable:
            self._pending_returns.extend(committed)
        else:
            self.published_messages.extend(committed)

    def tx_rollback(self) -> None:
        self._uncommitted = []

    def add_on_return_callback(self, callback: Callable[..., None]) -> None:
        self._return_callbacks.append(callback)

    def dispatch_returns(self) -> None:
        """Hand returned messages to the return callbacks, as pika does while processing events."""
        returned, self._pending_returns = self._pending_returns, []
        for message in returned:
            for callback in self._return_callbacks:
                method = pika.spec.Basic.Return(312, "NO_ROUTE", message.exchange, "")
                callback(self, method, message.properties, message.body)

    def basic_qos(
        self, prefetch_size: int = 0, prefetch_count: int = 0, global_qos: bool = False
    ) -> None:
        self.prefetch_count = prefetch_count

    def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        properties=None,
        mandatory: bool = False,
    ) -> None:
        assert properties is not None, "pykicak publishes every message with properties"
        message = PublishedMessage(exchange, routing_key, body, properties, mandatory)
        if self.transactional:
            self._uncommitted.append(message)
        else:
            self.published_messages.append(message)
        self.operations.append(f"publish {exchange}")

    def basic_ack(self, delivery_tag: int) -> None:
        self._settle(delivery_tag)
        self.acked_delivery_tags.append(delivery_tag)
        self.operations.append(f"ack {delivery_tag}")

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        self._settle(delivery_tag)
        self.nacked_deliveries.append(NackedDelivery(delivery_tag, requeue))
        self.operations.append(f"nack {delivery_tag} requeue={requeue}")

    def _settle(self, delivery_tag: int) -> None:
        """Reject tags this channel never delivered, as RabbitMQ does (406 PRECONDITION_FAILED)."""
        if delivery_tag not in self._delivered_delivery_tags:
            raise ValueError(f"Delivery tag {delivery_tag} was not delivered on this channel")
        self._delivered_delivery_tags.remove(delivery_tag)

    def close(self) -> None:
        self.is_open = False

    def cancel_consumer(self) -> None:
        """Test helper: RabbitMQ 4.3+ cancels the consumer, e.g. on a quorum queue's consumer
        timeout, returning the delivery in progress to the queue; the channel stays open.

        As in pika, only the callbacks registered on the wrapped channel (`_impl`) learn of it;
        consume() would end only when advanced, which a test does not reach while executing.
        """
        for callback in self._impl.cancel_callbacks:
            callback(object())

    def queue_deliveries(
        self,
        *bodies: bytes | None,
        properties: pika.BasicProperties | None = None,
        redelivered: bool = False,
    ) -> None:
        """Test helper: queue up message bodies, with `properties`, for consume() to yield.

        Without `properties`, each delivery has only the `type` property SAMPLE_TYPE. With
        `redelivered`, RabbitMQ flags them as delivered before.
        """
        delivered = properties if properties is not None else pika.BasicProperties(type=SAMPLE_TYPE)
        self._pending_deliveries.extend((body, delivered, redelivered) for body in bodies)

    def consume(self, queue: str, inactivity_timeout: float | None = None):
        """Yield the queued deliveries, then end the test with StopConsuming.

        `before_delivery(tag)`, if set, runs just before each delivery is yielded. If `on_idle` is
        set, the queue then stays empty instead: like pika with an inactivity timeout, consume()
        yields (None, None, None) after calling `on_idle()`, up to 1000 times.
        """
        self.prefetch_count_when_consuming = self.prefetch_count
        self.inactivity_timeout = inactivity_timeout
        for index, (body, properties, redelivered) in enumerate(self._pending_deliveries, start=1):
            if self.before_delivery is not None:
                self.before_delivery(index)
            self._delivered_delivery_tags.add(index)
            yield ConsumedMethod(delivery_tag=index, redelivered=redelivered), properties, body
        if self.on_idle is not None:
            for _ in range(1000):
                self.on_idle()
                yield None, None, None
        raise StopConsuming


class FakeConnection:
    """Opens `channel` first; any later channel is the Executor's results channel.

    The results channel, `results_channel` or a new FakeChannel, records its operations and
    published messages into `channel`'s lists, so tests can check the order of publishes,
    commits, and acknowledgements across both channels. It is also available as
    `channel.results_channel`.
    """

    def __init__(self, channel: FakeChannel, results_channel: FakeChannel | None = None) -> None:
        self.is_open = True
        self._channel = channel
        self._results_channel = results_channel
        self._opened: list[FakeChannel] = []

    def channel(self) -> FakeChannel:
        if not self._opened:
            opened = self._channel
        else:
            opened = self._results_channel or FakeChannel()
            opened.operations = self._channel.operations
            opened.published_messages = self._channel.published_messages
            self._channel.results_channel = opened
        self._opened.append(opened)
        return opened

    def process_data_events(self, time_limit: float | None = 0) -> None:
        for channel in self._opened:
            channel.dispatch_returns()

    def sleep(self, duration: float) -> None:
        """Sleep with time.sleep(), which tests replace; pika's sleep() also answers heartbeats."""
        time.sleep(duration)

    def close(self) -> None:
        self.is_open = False


def replace_in_pykicak(monkeypatch: pytest.MonkeyPatch, name: str, replacement: object) -> None:
    """Replace `name`, e.g. the clock `_now`, in every pykicak module that defines or imported it.

    A module that imported a function holds its own reference to it, so replacing it where it is
    defined is not enough; this finds every holder, wherever the code using it lives.
    """
    for module_name, module in list(sys.modules.items()):
        in_pykicak = module_name == "pykicak" or module_name.startswith("pykicak.")
        if in_pykicak and hasattr(module, name):
            monkeypatch.setattr(module, name, replacement)


class FakeStopwatch:
    """Stands in for `_monotonic()`, the clock of durations; tests move it forward explicitly.

    Its steps are binary fractions, so the durations computed from it are exact.
    """

    def __init__(self) -> None:
        self.current = 100.0

    def advance(self, seconds: float) -> None:
        self.current += seconds

    def __call__(self) -> float:
        return self.current


def kicak_values(record: logging.LogRecord) -> dict[str, Any]:
    """The values a statistics line carries, typed, as its record's `kicak` attribute."""
    values: dict[str, Any] = vars(record)["kicak"]
    return values


def logged_pairs(record: logging.LogRecord) -> dict[str, str]:
    """The key=value pairs of a record's text."""
    return dict(token.split("=", 1) for token in record.getMessage().split() if "=" in token)


def expected_pairs(values: dict[str, Any]) -> dict[str, str]:
    """The key=value pairs a statistics line's text shows for `values`, its `kicak` attribute.

    Durations to the millisecond, everything else as `str()` shows it; `event` names the line
    and is not in its text.
    """
    return {
        name: f"{value:.3f}" if name.endswith("_seconds") and value is not None else str(value)
        for name, value in values.items()
        if name != "event"
    }
