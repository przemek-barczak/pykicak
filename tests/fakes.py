"""Hand-written test doubles standing in for pika's connection/channel objects."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable

import pika
from pika.exceptions import ChannelClosedByBroker

from pykicak.abstracts import KicakAbstract

CONNECTION_VALUES = {
    "RABBIT_MQ_USERNAME": "guest",
    "RABBIT_MQ_PASSWORD": "guest",
    "RABBIT_MQ_HOST": "localhost",
    "RABBIT_MQ_PORT": "5672",
    "RABBIT_MQ_VIRTUAL_HOST": "/",
}
"""Config values that let KicakAbstract._create_connection() build connection parameters."""


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
    arguments: dict[str, object] | None = None


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
    mandatory: bool = False
    properties: pika.BasicProperties | None = None


@dataclasses.dataclass
class NackedDelivery:
    delivery_tag: int
    requeue: bool


@dataclasses.dataclass
class ConsumedMethod:
    delivery_tag: int


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
        self._pending_deliveries: list[bytes | None] = []
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
        self.declared_queues.append(
            DeclaredQueue(queue, durable, exclusive, auto_delete, arguments)
        )

    def exchange_declare(
        self, exchange: str, exchange_type: str, durable: bool = False
    ) -> None:
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
        message = PublishedMessage(exchange, routing_key, body, mandatory, properties)
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

    def queue_deliveries(self, *bodies: bytes | None) -> None:
        """Test helper: queue up message bodies for consume() to yield."""
        self._pending_deliveries.extend(bodies)

    def consume(self, queue: str, inactivity_timeout: float | None = None):
        """Yield the queued deliveries, then end the test with StopConsuming.

        `before_delivery(tag)`, if set, runs just before each delivery is yielded. If `on_idle` is
        set, the queue then stays empty instead: like pika with an inactivity timeout, consume()
        yields (None, None, None) after calling `on_idle()`, up to 1000 times.
        """
        self.prefetch_count_when_consuming = self.prefetch_count
        self.inactivity_timeout = inactivity_timeout
        for index, body in enumerate(self._pending_deliveries, start=1):
            if self.before_delivery is not None:
                self.before_delivery(index)
            self._delivered_delivery_tags.add(index)
            yield ConsumedMethod(delivery_tag=index), None, body
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

    def close(self) -> None:
        self.is_open = False
