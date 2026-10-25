"""The queues and exchanges of an Executor: their names, and how they are declared."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType

from pykicak._executor import logger
from pykicak.abstracts import _check_name

_DEAD_LETTER_EXCHANGE_SUFFIX = ".dlx"
_DEAD_LETTER_QUEUE_SUFFIX = ".dlq"
_RETRY_QUEUE_SUFFIX = ".retry"
# A queue an Executor consumes from leaves room in its name for the names derived from it
_LONGEST_QUEUE_NAME_SUFFIX = max(
    (_DEAD_LETTER_EXCHANGE_SUFFIX, _DEAD_LETTER_QUEUE_SUFFIX, _RETRY_QUEUE_SUFFIX), key=len
)


class QueueType(StrEnum):
    """RabbitMQ type of every queue an Executor declares, its `queue_type` argument.

    That is its input queue, its dead-letter queue, its retry queue, and its destination queues.
    Members are strings, so the values `"classic"` and `"quorum"` are accepted too.
    """

    __module__ = "pykicak.executor"  # where it is public

    CLASSIC = "classic"
    """A queue stored on one broker node; the default.

    Its retry queue returns messages at most once: see "When a message can still be lost" in the
    documentation.
    """

    QUORUM = "quorum"
    """A queue replicated across the nodes of a RabbitMQ cluster, so it survives a node failure.

    It limits how often the delivery of a message may fail: on RabbitMQ 4.0 and newer, by default,
    a message is dead-lettered instead of being delivered a 21st time. RabbitMQ 4.2 counts every
    requeue, 4.3 and newer only failed deliveries, e.g. after a lost connection. See "Queue type"
    in the documentation.
    """


def _dead_letter_exchange_of(queue_name: str) -> str:
    """Return the name of the dead-letter exchange of `queue_name`: `<queue>.dlx`."""
    return f"{queue_name}{_DEAD_LETTER_EXCHANGE_SUFFIX}"


def _dead_letter_queue_of(queue_name: str) -> str:
    """Return the name of the dead-letter queue of `queue_name`: `<queue>.dlq`."""
    return f"{queue_name}{_DEAD_LETTER_QUEUE_SUFFIX}"


def _retry_queue_of(queue_name: str) -> str:
    """Return the name of the retry queue of `queue_name`: `<queue>.retry`."""
    return f"{queue_name}{_RETRY_QUEUE_SUFFIX}"


def _check_queue_name(argument: str, name: object) -> None:
    """Raise unless `name` can name a queue an Executor consumes from.

    Like any name, but its dead-letter exchange and queue and its retry queue must fit AMQP's
    limit of 255 bytes too, so it may have at most 249.
    """
    _check_name(argument, name, suffix=_LONGEST_QUEUE_NAME_SUFFIX)


def _checked_destination_queues(
    destination_exchange: str | None, destination_queues: Sequence[str] | None
) -> tuple[str, ...]:
    """Validate that both destination arguments are given, or neither; return the queue names."""
    if destination_exchange is None:
        if destination_queues is not None:
            raise ValueError(
                "destination_queues requires destination_exchange; a terminator has neither"
            )
        return ()
    _check_name("destination_exchange", destination_exchange)
    if destination_queues is None:
        raise ValueError(
            "destination_queues is required with destination_exchange: name every queue that "
            "must receive its messages"
        )
    if isinstance(destination_queues, str):
        raise TypeError("destination_queues must be a sequence of queue names, not a str")
    queues = tuple(destination_queues)
    if not queues:
        raise ValueError("destination_queues must name at least one queue")
    for index, queue in enumerate(queues):
        _check_queue_name(f"destination_queues[{index}]", queue)
    if len(set(queues)) != len(queues):
        raise ValueError("destination_queues must not contain duplicate names")
    return queues


def _check_no_loop(
    source_queue: str,
    source_exchange: str,
    destination_exchange: str | None,
    destination_queues: tuple[str, ...],
) -> None:
    """Raise `ValueError` if the results would be delivered back to `source_queue`.

    They would be executed again, possibly forever. A loop through another Executor stays
    possible, because that one is configured explicitly.
    """
    if destination_exchange == source_exchange:
        raise ValueError(
            "destination_exchange must differ from source_exchange: a self-loop, the results "
            "would be delivered back to source_queue"
        )
    if source_queue in destination_queues:
        raise ValueError(
            "destination_queues must not include source_queue: a self-loop, the results "
            "would be delivered back to it"
        )


def _check_no_derived_name(
    source_queue: str,
    source_exchange: str,
    destination_exchange: str | None,
    destination_queues: tuple[str, ...],
) -> None:
    """Raise `ValueError` if a name given is one the library derives from a queue.

    A dead-letter queue is declared without a dead-letter exchange, and a retry queue with
    arguments of its own, so declaring either as an input queue would fail with `406`.
    """
    dead_letter_exchange = _dead_letter_exchange_of(source_queue)
    if dead_letter_exchange in (source_exchange, destination_exchange):
        raise ValueError(
            "source_exchange and destination_exchange must differ from "
            f"{dead_letter_exchange!r}, the dead-letter exchange of source_queue"
        )
    input_queues = (source_queue, *destination_queues)
    if {_dead_letter_queue_of(queue) for queue in input_queues}.intersection(input_queues):
        raise ValueError(
            "source_queue and destination_queues must not include the dead-letter queue "
            "<queue>.dlq of one of them"
        )
    if {_retry_queue_of(queue) for queue in input_queues}.intersection(input_queues):
        raise ValueError(
            "source_queue and destination_queues must not include the retry queue "
            "<queue>.retry of one of them"
        )
    if destination_exchange in {_dead_letter_exchange_of(queue) for queue in destination_queues}:
        raise ValueError(
            "destination_exchange must not be the dead-letter exchange <queue>.dlx of a "
            "destination queue"
        )


def _checked_queue_type(queue_type: QueueType | str) -> QueueType:
    """Return `queue_type` as a `QueueType`, which also accepts its string values."""
    try:
        return QueueType(queue_type)
    except ValueError:
        members = " or ".join(f"QueueType.{member.name}" for member in QueueType)
        raise ValueError(f"queue_type must be {members}, got {queue_type!r}") from None


def _declare_fanout_exchange(channel: BlockingChannel, exchange_name: str) -> None:
    """Idempotently declare a durable fanout exchange."""
    logger.debug("Declaring exchange %s", exchange_name)
    channel.exchange_declare(
        exchange=exchange_name, exchange_type=ExchangeType.fanout, durable=True
    )


def _bind_queue(channel: BlockingChannel, queue_name: str, exchange_name: str) -> None:
    """Idempotently bind a queue to an exchange."""
    logger.debug("Binding queue %s -> exchange %s", queue_name, exchange_name)
    channel.queue_bind(queue=queue_name, exchange=exchange_name)


@dataclass(frozen=True, slots=True, kw_only=True)
class _ExecutorTopology:
    """The queues and exchanges of an Executor, and how it declares them.

    Created with `checked()`, which validates the names. Every declaration is idempotent: it is a
    no-op while the queue, exchange, or binding exists, and restores it if it was deleted.

    - Input side: the source queue, its binding to the source exchange, its dead-letter route
      (`<source_queue>.dlx` and `<source_queue>.dlq`), and its retry queue
      (`<source_queue>.retry`), all owned and declared only by this Executor.
    - Output side: the destination exchange and every destination queue, each bound to it.
    """

    source_queue: str
    source_exchange: str
    destination_exchange: str | None
    """None for a terminator, which publishes nothing."""

    destination_queues: tuple[str, ...]
    queue_type: QueueType

    @classmethod
    def checked(
        cls,
        *,
        source_queue: str,
        source_exchange: str,
        destination_exchange: str | None,
        destination_queues: Sequence[str] | None,
        queue_type: QueueType | str,
    ) -> _ExecutorTopology:
        """Return the topology of these arguments, raising `TypeError` or `ValueError` if invalid.

        Every name must be valid (see `pykicak.abstracts._check_name()`), a queue an Executor
        consumes from leaving room for the suffixes of the names derived from it. Both
        destination arguments are given, or neither. The results never come back to
        `source_queue`, and no name given is one derived from a queue.
        """
        _check_queue_name("source_queue", source_queue)
        _check_name("source_exchange", source_exchange)
        queues = _checked_destination_queues(destination_exchange, destination_queues)
        _check_no_loop(source_queue, source_exchange, destination_exchange, queues)
        _check_no_derived_name(source_queue, source_exchange, destination_exchange, queues)
        return cls(
            source_queue=source_queue,
            source_exchange=source_exchange,
            destination_exchange=destination_exchange,
            destination_queues=queues,
            queue_type=_checked_queue_type(queue_type),
        )

    @property
    def dead_letter_exchange(self) -> str:
        """The dead-letter exchange of the source queue: `<source_queue>.dlx`."""
        return _dead_letter_exchange_of(self.source_queue)

    @property
    def dead_letter_queue(self) -> str:
        """The dead-letter queue of the source queue: `<source_queue>.dlq`."""
        return _dead_letter_queue_of(self.source_queue)

    @property
    def retry_queue(self) -> str:
        """The retry queue of the source queue: `<source_queue>.retry`."""
        return _retry_queue_of(self.source_queue)

    def declare_input_side(self, channel: BlockingChannel) -> None:
        """Declare the source queue, its binding, its dead-letter route, and its retry queue."""
        self.declare_input_queue(channel, self.source_queue)
        _declare_fanout_exchange(channel, self.source_exchange)
        _bind_queue(channel, self.source_queue, self.source_exchange)
        self.declare_dead_letter_route(channel)
        self.declare_retry_queue(channel)

    def declare_dead_letter_route(self, channel: BlockingChannel) -> None:
        """Declare the dead-letter exchange and queue of the source queue, and bind them.

        Only this executor declares them, because only its own queue's arguments name them.
        """
        _declare_fanout_exchange(channel, self.dead_letter_exchange)
        logger.debug("Declaring %s queue %s", self.queue_type, self.dead_letter_queue)
        channel.queue_declare(
            queue=self.dead_letter_queue,
            durable=True,
            arguments={"x-queue-type": self.queue_type.value},
        )
        _bind_queue(channel, self.dead_letter_queue, self.dead_letter_exchange)

    def declare_retry_queue(self, channel: BlockingChannel) -> None:
        """Declare the retry queue of the source queue, which returns each message after its delay.

        Nothing consumes it: a message expires there after `retry_delay_seconds`, and RabbitMQ
        dead-letters it through the default exchange, which routes by queue name, to the back of
        the source queue. The default exchange cannot be deleted, so the way back has no exchange
        or binding that could go missing. A quorum retry queue dead-letters at least once, keeping
        a message until the source queue takes it; a classic one drops a message that expires
        while the source queue does not exist. Only this executor declares it.
        """
        arguments: dict[str, object] = {
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": self.source_queue,
            "x-queue-type": self.queue_type.value,
        }
        if self.queue_type is QueueType.QUORUM:
            arguments["x-dead-letter-strategy"] = "at-least-once"
            arguments["x-overflow"] = "reject-publish"  # required by at-least-once dead-lettering
        logger.debug(
            "Declaring %s queue %s returning to %s",
            self.queue_type,
            self.retry_queue,
            self.source_queue,
        )
        channel.queue_declare(queue=self.retry_queue, durable=True, arguments=arguments)

    def declare_output_side(self, channel: BlockingChannel) -> None:
        """Declare the destination exchange and every destination queue, and bind each of them.

        A terminator has no output side, so this does nothing for it.
        """
        destination_exchange = self.destination_exchange
        if destination_exchange is None:
            return
        _declare_fanout_exchange(channel, destination_exchange)
        for queue_name in self.destination_queues:
            self.declare_input_queue(channel, queue_name)
            _bind_queue(channel, queue_name, destination_exchange)

    def declare_input_queue(self, channel: BlockingChannel, queue_name: str) -> None:
        """Declare a queue that an Executor consumes from, with the arguments every one uses.

        The arguments follow from the name and the queue type alone, so the executor upstream of
        a queue declares it exactly as the queue's own executor does.
        """
        dead_letter_exchange = _dead_letter_exchange_of(queue_name)
        logger.debug(
            "Declaring %s queue %s dead_letter_exchange=%s",
            self.queue_type,
            queue_name,
            dead_letter_exchange,
        )
        channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": dead_letter_exchange,
                "x-queue-type": self.queue_type.value,
            },
        )
