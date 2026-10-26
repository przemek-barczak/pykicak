"""The exchange an Injector publishes to, and how it is declared."""

from __future__ import annotations

from dataclasses import dataclass

from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType

from pykicak._injector import logger
from pykicak.abstracts import _check_name


@dataclass(frozen=True, slots=True, kw_only=True)
class _InjectorTopology:
    """The exchange of an Injector, which it declares on every connection.

    Created with `checked()`, which validates the name. The Injector declares nothing else: the
    queue bound to its exchange belongs to the Executor consuming it.
    """

    exchange_name: str

    @classmethod
    def checked(cls, *, exchange_name: str) -> _InjectorTopology:
        """Return the topology of `exchange_name`, raising `TypeError` or `ValueError` if invalid.

        It must be a non-empty `str` of at most 255 UTF-8 bytes that does not start with `amq.`
        (see `pykicak.abstracts._check_name()`).
        """
        _check_name("exchange_name", exchange_name)
        return cls(exchange_name=exchange_name)

    def declare(self, channel: BlockingChannel) -> None:
        """Idempotently declare the exchange as a durable fanout exchange."""
        logger.debug("Declaring exchange %s", self.exchange_name)
        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=ExchangeType.fanout, durable=True
        )
