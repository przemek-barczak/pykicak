"""The Injector's constructor: its arguments, message type, and connections."""

import dataclasses

import pytest

from pykicak.config import KicakConfig
from pykicak.injector import KicakInjectorAbstract
from pykicak.messages import KicakMessage
from tests.fakes import (
    CONNECTION_VALUES,
    FakeChannel,
    captured_connection_parameters,
)
from tests.injector.helpers import (
    CountingInjector,
    OtherMessage,
    SampleMessage,
)


class SilentInjector(KicakInjectorAbstract[SampleMessage]):
    """Generates nothing; for tests of the constructor's arguments."""

    def generate(self) -> SampleMessage | None:
        return None


@dataclasses.dataclass(frozen=True, slots=True)
class UnnamedSample(SampleMessage):
    """Inherits SampleMessage's kicak_type without declaring its own."""


class GenericInjector[M: KicakMessage](KicakInjectorAbstract[M]):
    def generate(self) -> M | None:
        return None


type Published = OtherMessage


type EitherMessage = SampleMessage | OtherMessage


def test_init_stores_exchange_name_from_constructor(config):
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")

    assert injector._topology.exchange_name == "my-exchange"


def test_init_requires_keyword_exchange_name(config):
    class MinimalInjector(KicakInjectorAbstract[SampleMessage]):
        def generate(self) -> SampleMessage | None:
            return None

    with pytest.raises(TypeError):
        MinimalInjector(config, "my-exchange")  # type: ignore[call-arg]


def test_connection_accepts_broker_heartbeat(monkeypatch):
    injector = CountingInjector(
        KicakConfig(values=CONNECTION_VALUES), FakeChannel(), exchange_name="my-exchange"
    )

    assert captured_connection_parameters(injector, monkeypatch).heartbeat is None


def test_connection_times_out_when_rabbitmq_blocks_it(monkeypatch):
    injector = CountingInjector(
        KicakConfig(values=CONNECTION_VALUES), FakeChannel(), exchange_name="my-exchange"
    )

    parameters = captured_connection_parameters(injector, monkeypatch)

    assert parameters.blocked_connection_timeout == 60.0


@pytest.mark.parametrize("seconds", [5, 0.5, 3600])
def test_blocked_timeout_seconds_sets_how_long_a_blocked_connection_waits(monkeypatch, seconds):
    injector = SilentInjector(
        KicakConfig(values=CONNECTION_VALUES),
        exchange_name="my-exchange",
        blocked_timeout_seconds=seconds,
    )

    parameters = captured_connection_parameters(injector, monkeypatch)

    assert parameters.blocked_connection_timeout == seconds


@pytest.mark.parametrize(
    ("seconds", "error"),
    [
        (None, TypeError),  # an injector always gives up eventually
        (True, TypeError),
        ("60", TypeError),
        (0, ValueError),  # often read as "no timeout"
        (-1, ValueError),
        (float("nan"), ValueError),
        (float("inf"), ValueError),
        (10**1000, ValueError),  # too large for a float: counts as infinite
    ],
    ids=["none", "bool", "str", "zero", "negative", "nan", "infinity", "int-beyond-float"],
)
def test_init_rejects_an_invalid_blocked_timeout_seconds(config, seconds, error):
    with pytest.raises(error, match="blocked_timeout_seconds"):
        SilentInjector(config, exchange_name="my-exchange", blocked_timeout_seconds=seconds)


def test_declare_topology_declares_durable_fanout_exchange(config):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")

    injector.connect()

    assert len(channel.declared_exchanges) == 1
    assert channel.declared_exchanges[0].exchange == "my-exchange"
    assert channel.declared_exchanges[0].exchange_type == "fanout"
    assert channel.declared_exchanges[0].durable is True


@pytest.mark.parametrize(
    ("declaration", "error", "match"),
    [
        (lambda: KicakInjectorAbstract, TypeError, "must declare its message types"),
        (lambda: GenericInjector, TypeError, "must declare its message types"),
        (lambda: KicakInjectorAbstract[UnnamedSample], TypeError, "its own kicak_type"),
        (lambda: KicakInjectorAbstract[KicakMessage], TypeError, "KicakMessage subclass"),
        (lambda: KicakInjectorAbstract[SampleMessage | OtherMessage], TypeError, "one message"),
        (
            lambda: KicakInjectorAbstract[SampleMessage | None],  # type: ignore[type-var]
            TypeError,
            "one message",
        ),
    ],
    ids=[
        "unparameterized",
        "open-type-parameter",
        "inherited-type-name",
        "base-message-class",
        "union",
        "optional",
    ],
)
def test_init_rejects_an_invalid_declaration_of_the_message_type(config, declaration, error, match):
    base = declaration()

    class Declared(base):  # type: ignore[valid-type, misc]
        def generate(self):
            return None

    with pytest.raises(error, match=match):
        Declared(config, exchange_name="my-exchange")


def test_message_type_is_resolved_through_subclasses_and_generic_classes(config):
    class Concrete(GenericInjector[OtherMessage]):
        pass

    class Inherited(CountingInjector):
        pass

    assert Concrete(config, exchange_name="my-exchange")._message_type is OtherMessage
    inherited = Inherited(config, FakeChannel(), exchange_name="my-exchange")
    assert inherited._message_type is SampleMessage


def test_message_type_is_resolved_through_a_type_statement_alias(config):
    class Aliased(GenericInjector[Published]):
        pass

    class AliasedUnion(GenericInjector[EitherMessage]):
        pass

    assert Aliased(config, exchange_name="my-exchange")._message_type is OtherMessage
    with pytest.raises(TypeError, match="must declare one message type"):
        AliasedUnion(config, exchange_name="my-exchange")


def test_init_rejects_empty_exchange_name(config):
    with pytest.raises(ValueError, match="exchange_name must be a non-empty"):
        CountingInjector(config, FakeChannel(), exchange_name="")


@pytest.mark.parametrize("exchange_name", [None, 42, b"my-exchange"], ids=["none", "int", "bytes"])
def test_init_rejects_exchange_name_that_is_not_str(config, exchange_name):
    with pytest.raises(TypeError, match="exchange_name must be a str, got"):
        CountingInjector(config, FakeChannel(), exchange_name=exchange_name)


@pytest.mark.parametrize("exchange_name", ["x" * 255, "ł" * 127 + "x"], ids=["ascii", "two-byte"])
def test_init_accepts_exchange_name_of_255_bytes(config, exchange_name):
    CountingInjector(config, FakeChannel(), exchange_name=exchange_name)


@pytest.mark.parametrize("exchange_name", ["x" * 256, "ł" * 128], ids=["ascii", "two-byte"])
def test_init_rejects_exchange_name_longer_than_255_bytes(config, exchange_name):
    with pytest.raises(ValueError, match="exchange_name must be at most 255 bytes in UTF-8$"):
        CountingInjector(config, FakeChannel(), exchange_name=exchange_name)


def test_init_rejects_exchange_name_with_the_prefix_rabbitmq_reserves(config):
    # Even amq.fanout, which RabbitMQ lets a client declare again: the whole vhost shares it
    with pytest.raises(ValueError, match="must not start with 'amq.', which RabbitMQ reserves"):
        CountingInjector(config, FakeChannel(), exchange_name="amq.fanout")


def test_connection_is_named_by_role_exchange_host_and_pid(config, monkeypatch):
    monkeypatch.setattr("socket.gethostname", lambda: "worker-3")
    monkeypatch.setattr("os.getpid", lambda: 4242)
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")

    parameters = captured_connection_parameters(injector, monkeypatch)

    assert parameters.client_properties == {
        "connection_name": "injector exchange=my-exchange host=worker-3 pid=4242"
    }
