"""Messages, injectors, and helpers the Injector's tests share."""

import dataclasses

from pika.exceptions import StreamLostError

from pykicak.config import KicakConfig
from pykicak.injector import KicakInjectorAbstract
from pykicak.messages import KicakMessage
from tests.fakes import (
    FakeChannel,
    FakeConnection,
)


@dataclasses.dataclass(frozen=True, slots=True)
class SampleMessage(KicakMessage):
    kicak_type = "sample.v1"
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class OtherMessage(KicakMessage):
    kicak_type = "other.v1"
    text: str


class CountingInjector(KicakInjectorAbstract[SampleMessage]):
    def __init__(self, config: KicakConfig, channel: FakeChannel, exchange_name: str) -> None:
        super().__init__(config, exchange_name=exchange_name)
        self._fake_channel = channel
        self.generate_calls = 0

    def _create_connection(self):
        return FakeConnection(self._fake_channel)

    def generate(self) -> SampleMessage | None:
        self.generate_calls += 1
        return SampleMessage(
            message_id=f"message-{self.generate_calls}", text=f"message-{self.generate_calls}"
        )


def scripted_connections(injector, monkeypatch, *outcomes) -> list[FakeConnection]:
    """Make each connection attempt use the next outcome: a FakeChannel, or an error to raise.

    Returns the list of connections that were opened, so tests can check they were closed.
    """
    opened: list[FakeConnection] = []
    remaining = list(outcomes)

    def create_connection() -> FakeConnection:
        outcome = remaining.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        connection = FakeConnection(outcome)
        opened.append(connection)
        return connection

    monkeypatch.setattr(injector, "_create_connection", create_connection)
    return opened


class ConnectionLostChannel(FakeChannel):
    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        raise StreamLostError("connection lost")
