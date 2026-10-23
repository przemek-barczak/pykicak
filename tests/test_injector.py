import dataclasses
import pickle
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pika.exceptions import (
    AMQPConnectionError,
    ConnectionBlockedTimeout,
    NackError,
    ProbableAuthenticationError,
    StreamLostError,
    UnroutableError,
)

from pykicak.config import KicakConfig
from pykicak.injector import InjectionError, KicakInjectorAbstract
from pykicak.messages import KicakMessage
from tests.fakes import (
    CONNECTION_VALUES,
    FakeChannel,
    FakeConnection,
    FakeStopwatch,
    captured_connection_parameters,
    expected_pairs,
    kicak_values,
    logged_pairs,
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


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values=CONNECTION_VALUES)


def test_init_stores_exchange_name_from_constructor(config):
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")

    assert injector._exchange_name == "my-exchange"


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


class SilentInjector(KicakInjectorAbstract[SampleMessage]):
    """Generates nothing; for tests of the constructor's arguments."""

    def generate(self) -> SampleMessage | None:
        return None


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
    ],
    ids=["none", "bool", "str", "zero", "negative", "nan", "infinity"],
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


def test_publish_sends_message_to_exchange(config, caplog):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    injector.connect()

    caplog.set_level("INFO")
    injector.publish(SampleMessage(message_id="hello", text="hello"))

    assert len(channel.published_messages) == 1
    published = channel.published_messages[0]
    assert published.exchange == "my-exchange"
    assert published.body == SampleMessage(message_id="hello", text="hello").to_bytes()
    publish_records = [record for record in caplog.records if "Message published" in record.message]
    assert len(publish_records) == 1
    assert publish_records[0].levelname == "INFO"
    assert "type=SampleMessage" in publish_records[0].message
    assert "id=hello" in publish_records[0].message
    assert "exchange=my-exchange" in publish_records[0].message


def test_publish_is_mandatory_in_confirm_mode(config):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    injector.connect()

    injector.publish(SampleMessage(message_id="hello", text="hello"))

    assert channel.confirm_delivery_enabled is True
    assert channel.published_messages[0].mandatory is True


@pytest.mark.parametrize(
    "refusal", [UnroutableError([]), NackError([])], ids=["unroutable", "nacked"]
)
def test_publish_raises_without_retrying_when_rabbitmq_refuses_message(
    config, monkeypatch, refusal
):
    class RefusingChannel(FakeChannel):
        publish_attempts = 0

        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            self.publish_attempts += 1
            raise refusal

    channel = RefusingChannel()
    delays: list[float] = []
    monkeypatch.setattr("pykicak.injector.time.sleep", delays.append)
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    injector.connect()

    with pytest.raises(InjectionError) as raised:
        injector.publish(SampleMessage(message_id="hello", text="hello"))

    assert isinstance(raised.value.__cause__, type(refusal))

    assert channel.publish_attempts == 1
    assert delays == []


def test_run_closes_connection_and_raises_when_message_is_unroutable(config):
    class UnroutableChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise UnroutableError([])

    channel = UnroutableChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")

    with pytest.raises(InjectionError) as raised:
        injector.run()

    assert isinstance(raised.value.__cause__, UnroutableError)
    assert channel.is_open is False


def test_publish_reconnects_and_retries_transient_failure(config, monkeypatch, caplog):
    class FailingChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise StreamLostError("connection lost")

    retry_channel = FakeChannel()
    channels = [FailingChannel(), retry_channel]
    injector = CountingInjector(config, channels[0], exchange_name="my-exchange")
    monkeypatch.setattr("pykicak.injector.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        injector,
        "_create_connection",
        lambda: FakeConnection(channels.pop(0)),
    )
    injector.connect()

    injector.publish(SampleMessage(message_id="hello", text="hello"))

    assert len(channels) == 0
    assert retry_channel.published_messages[0].exchange == "my-exchange"
    assert "retrying attempt 2/3" in caplog.text


def test_connect_retries_transient_failure_at_most_three_times(config, monkeypatch, caplog):
    class UnavailableInjector(CountingInjector):
        connection_attempts = 0

        def _create_connection(self):
            self.connection_attempts += 1
            raise AMQPConnectionError("unavailable")

    delays: list[float] = []
    monkeypatch.setattr("pykicak.injector.time.sleep", delays.append)
    injector = UnavailableInjector(config, FakeChannel(), exchange_name="my-exchange")

    with pytest.raises(AMQPConnectionError):
        injector.connect()

    assert injector.connection_attempts == 3
    assert delays == [1.0, 2.0]
    assert "failed after 3 attempts" in caplog.text
    [record] = [r for r in caplog.records if "failed after 3 attempts" in r.getMessage()]
    assert record.levelname == "ERROR"
    assert record.exc_info is not None and record.exc_info[0] is AMQPConnectionError


def test_connect_does_not_retry_authentication_failure(config, monkeypatch):
    class AuthenticationFailureInjector(CountingInjector):
        connection_attempts = 0

        def _create_connection(self):
            self.connection_attempts += 1
            raise ProbableAuthenticationError("denied")

    delays: list[float] = []
    monkeypatch.setattr("pykicak.injector.time.sleep", delays.append)
    injector = AuthenticationFailureInjector(config, FakeChannel(), exchange_name="my-exchange")

    with pytest.raises(ProbableAuthenticationError):
        injector.connect()

    assert injector.connection_attempts == 1
    assert delays == []


def test_run_one_shot_generates_and_publishes_once(config):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")

    injector.run(interval_seconds=0)

    assert injector.generate_calls == 1
    assert len(channel.published_messages) == 1


@pytest.mark.parametrize(
    ("interval_seconds", "error", "reason"),
    [
        (float("nan"), ValueError, "interval_seconds must be a finite number"),
        (float("inf"), ValueError, "interval_seconds must be a finite number"),
        (float("-inf"), ValueError, "interval_seconds must be a finite number"),
        (True, TypeError, "interval_seconds must be a number"),
        ("60", TypeError, "interval_seconds must be a number"),
        (None, TypeError, "interval_seconds must be a number"),
    ],
    ids=["nan", "inf", "-inf", "bool", "str", "none"],
)
def test_run_rejects_an_invalid_interval_before_generating(
    config, monkeypatch, interval_seconds, error, reason
):
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch)

    with pytest.raises(error, match=reason):
        injector.run(interval_seconds=interval_seconds)

    assert injector.generate_calls == 0
    assert opened == []


def test_run_continuous_publishes_on_interval(config, monkeypatch):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    sleep_calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 2:
            raise StopIteration

    monkeypatch.setattr("pykicak.injector.time.sleep", fake_sleep)

    with pytest.raises(StopIteration):
        injector.run(interval_seconds=60)

    assert sleep_calls == [60, 60]
    assert len(channel.published_messages) == 2


def test_run_skips_publishing_when_generate_returns_none(config, monkeypatch):
    channel = FakeChannel()

    class SkippingInjector(CountingInjector):
        def generate(self):
            super().generate()
            return None

    injector = SkippingInjector(config, channel, exchange_name="my-exchange")

    def fake_sleep(seconds: float) -> None:
        raise StopIteration

    monkeypatch.setattr("pykicak.injector.time.sleep", fake_sleep)

    with pytest.raises(StopIteration):
        injector.run(interval_seconds=60)

    assert channel.published_messages == []


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


def test_run_injects_on_a_short_lived_connection(config, monkeypatch):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch, channel)

    injector.run()

    assert [published.exchange for published in channel.published_messages] == ["my-exchange"]
    assert channel.published_messages[0].mandatory is True
    assert len(opened) == 1
    assert opened[0].is_open is False
    assert channel.is_open is False


def test_run_opens_no_connection_when_generate_returns_none(config, monkeypatch):
    class SkippingInjector(CountingInjector):
        def generate(self):
            super().generate()
            return None

    injector = SkippingInjector(config, FakeChannel(), exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch)

    injector.run()

    assert injector.generate_calls == 1
    assert opened == []


def test_run_retries_on_a_new_connection_with_the_same_message(config, monkeypatch, caplog):
    class ConnectionLostChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise StreamLostError("connection lost")

    retry_channel = FakeChannel()
    injector = CountingInjector(config, retry_channel, exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch, ConnectionLostChannel(), retry_channel)
    delays: list[float] = []
    monkeypatch.setattr("pykicak.injector.time.sleep", delays.append)

    injector.run()

    assert injector.generate_calls == 1
    assert [published.body for published in retry_channel.published_messages] == [
        SampleMessage(message_id="message-1", text="message-1").to_bytes()
    ]
    assert delays == [1.0]
    assert [connection.is_open for connection in opened] == [False, False]
    assert "Injection of message id=message-1 failed, retrying attempt 2/3" in caplog.text


def test_run_stamps_a_retried_message_with_the_time_of_the_attempt_that_published_it(
    config, monkeypatch
):
    class ConnectionLostChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise StreamLostError("connection lost")

    now = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)

    def sleep(seconds: float) -> None:
        nonlocal now
        now += timedelta(seconds=seconds)

    monkeypatch.setattr("pykicak.abstracts._now", lambda: now)
    monkeypatch.setattr("pykicak.injector.time.sleep", sleep)
    retry_channel = FakeChannel()
    injector = CountingInjector(config, retry_channel, exchange_name="my-exchange")
    scripted_connections(injector, monkeypatch, ConnectionLostChannel(), retry_channel)

    injector.run()

    [published] = retry_channel.published_messages
    assert published.properties.headers["x-kicak-published-at"] == "2026-10-01T12:00:01+00:00"


def test_run_retries_when_a_blocked_connection_times_out(config, monkeypatch, caplog):
    class BlockedChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise ConnectionBlockedTimeout("Blocked connection timeout expired.")

    retry_channel = FakeChannel()
    injector = CountingInjector(config, retry_channel, exchange_name="my-exchange")
    scripted_connections(injector, monkeypatch, BlockedChannel(), retry_channel)
    monkeypatch.setattr("pykicak.injector.time.sleep", lambda _seconds: None)

    injector.run()

    assert [published.body for published in retry_channel.published_messages] == [
        SampleMessage(message_id="message-1", text="message-1").to_bytes()
    ]
    assert "Blocked connection timeout expired" in caplog.text


def test_run_raises_after_three_failed_attempts_with_no_connection_left_open(
    config, monkeypatch, caplog
):
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")
    scripted_connections(
        injector,
        monkeypatch,
        AMQPConnectionError("broker down"),
        AMQPConnectionError("broker down"),
        AMQPConnectionError("broker down"),
    )
    delays: list[float] = []
    monkeypatch.setattr("pykicak.injector.time.sleep", delays.append)

    with pytest.raises(InjectionError) as raised:
        injector.run()

    assert isinstance(raised.value.__cause__, AMQPConnectionError)
    assert injector.generate_calls == 1
    assert delays == [1.0, 2.0]
    assert injector._connection is None
    assert "failed after 3 attempts" in caplog.text


def test_repeating_mode_keeps_no_connection_open_between_injections(config, monkeypatch):
    channels = [FakeChannel(), FakeChannel()]
    injector = CountingInjector(config, channels[0], exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch, *channels)
    open_while_sleeping: list[bool] = []

    def fake_sleep(_seconds: float) -> None:
        open_while_sleeping.append(any(connection.is_open for connection in opened))
        if len(open_while_sleeping) == 2:
            raise StopIteration  # end the endless loop

    monkeypatch.setattr("pykicak.injector.time.sleep", fake_sleep)

    with pytest.raises(StopIteration):
        injector.run(interval_seconds=60)

    assert open_while_sleeping == [False, False]
    assert [len(channel.published_messages) for channel in channels] == [1, 1]


def test_repeating_mode_ends_with_the_error_of_a_failed_injection(config, monkeypatch):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    scripted_connections(
        injector,
        monkeypatch,
        channel,
        AMQPConnectionError("broker down"),
        AMQPConnectionError("broker down"),
        AMQPConnectionError("broker down"),
    )
    sleeps: list[float] = []
    monkeypatch.setattr("pykicak.injector.time.sleep", sleeps.append)

    with pytest.raises(InjectionError) as raised:
        injector.run(interval_seconds=60)

    assert isinstance(raised.value.__cause__, AMQPConnectionError)
    assert raised.value.message == SampleMessage(message_id="message-2", text="message-2")

    assert len(channel.published_messages) == 1
    assert injector.generate_calls == 2
    assert sleeps == [60, 1.0, 2.0]  # the interval, then the failed injection's retry delays


@dataclasses.dataclass(frozen=True, slots=True)
class IdentifiedMessage(KicakMessage):
    secret_payload: str


def test_injection_error_describes_message_by_type_and_id_with_cause():
    message = IdentifiedMessage(message_id="m-1", secret_payload="do not log me")
    try:
        try:
            raise UnroutableError([])
        except UnroutableError as cause:
            raise InjectionError(message, "my-exchange") from cause
    except InjectionError as error:
        text = str(error)

    assert text.startswith("IdentifiedMessage message id=m-1 was not injected into exchange")
    assert "'my-exchange'" in text
    assert "UnroutableError" in text
    assert "do not log me" not in text


def test_injection_error_keeps_message_and_exchange_and_survives_pickling():
    message = IdentifiedMessage(message_id="m-1", secret_payload="payload")
    error = InjectionError(message, "my-exchange")

    restored = pickle.loads(pickle.dumps(error))

    assert (restored.message, restored.exchange_name) == (message, "my-exchange")
    # Pickling keeps no __cause__, so the text ends without one
    assert str(restored) == (
        "IdentifiedMessage message id=m-1 was not injected into exchange 'my-exchange'"
    )


def test_run_logs_and_does_not_wrap_errors_from_generate(config, monkeypatch, caplog):
    class BrokenInjector(CountingInjector):
        def generate(self):
            raise ZeroDivisionError("bug in application code")

    injector = BrokenInjector(config, FakeChannel(), exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch)

    with pytest.raises(ZeroDivisionError):
        injector.run()

    assert opened == []
    [record] = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.getMessage() == "BrokenInjector.generate() failed exchange=my-exchange"
    assert record.exc_info[0] is ZeroDivisionError


def test_run_logs_and_does_not_wrap_serialization_errors(config, monkeypatch, caplog):
    class UnserializableInjector(CountingInjector):
        def generate(self):
            return SampleMessage(message_id="unserializable", text=object())  # type: ignore[arg-type]

    injector = UnserializableInjector(config, FakeChannel(), exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch)

    with pytest.raises(TypeError):
        injector.run()

    assert opened == []
    # Logged once, by the Injector: the serialization code logs it at DEBUG only
    [record] = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.name == "pykicak.injector"
    assert record.getMessage() == (
        "Failed to serialize message type=SampleMessage exchange=my-exchange"
    )
    assert record.exc_info[0] is TypeError


def test_publish_logs_and_does_not_wrap_serialization_errors(config, caplog):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")

    with injector, pytest.raises(TypeError):
        message = SampleMessage(message_id="unserializable", text=object())  # type: ignore[arg-type]
        injector.publish(message)

    assert channel.published_messages == []
    [record] = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.name == "pykicak.injector"
    assert record.getMessage() == (
        "Failed to serialize message type=SampleMessage exchange=my-exchange"
    )


@dataclasses.dataclass(frozen=True, slots=True)
class UnnamedSample(SampleMessage):
    """Inherits SampleMessage's kicak_type without declaring its own."""


@dataclasses.dataclass(frozen=True, slots=True)
class RenamedSample(SampleMessage):
    kicak_type = "renamed-sample.v1"


class GenericInjector[M: KicakMessage](KicakInjectorAbstract[M]):
    def generate(self) -> M | None:
        return None


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


type Published = OtherMessage
type EitherMessage = SampleMessage | OtherMessage


def test_message_type_is_resolved_through_a_type_statement_alias(config):
    class Aliased(GenericInjector[Published]):
        pass

    class AliasedUnion(GenericInjector[EitherMessage]):
        pass

    assert Aliased(config, exchange_name="my-exchange")._message_type is OtherMessage
    with pytest.raises(TypeError, match="must declare one message type"):
        AliasedUnion(config, exchange_name="my-exchange")


@pytest.mark.parametrize(
    "message",
    [
        RenamedSample(message_id="subclass", text="a subclass of the declared class"),
        OtherMessage(message_id="other", text="another class"),
    ],
    ids=["subclass", "other-class"],
)
def test_run_logs_and_raises_when_generate_returns_another_type(
    config, monkeypatch, caplog, message
):
    class WrongTypeInjector(CountingInjector):
        def generate(self):
            return message

    injector = WrongTypeInjector(config, FakeChannel(), exchange_name="my-exchange")
    opened = scripted_connections(injector, monkeypatch)

    with pytest.raises(TypeError, match="declares its messages as SampleMessage"):
        injector.run()

    assert opened == []  # refused before connecting
    [record] = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.getMessage() == (
        f"Message refused type={type(message).__name__} exchange=my-exchange: "
        "WrongTypeInjector declares its messages as SampleMessage"
    )


def test_publish_raises_for_a_message_of_another_type(config):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")

    with injector, pytest.raises(TypeError, match="declares its messages as SampleMessage"):
        injector.publish(OtherMessage(message_id="other", text="other"))  # type: ignore[arg-type]

    assert channel.published_messages == []


def test_publish_before_connect_raises_runtime_error(config):
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")

    with pytest.raises(RuntimeError, match="Not connected"):
        injector.publish(SampleMessage(message_id="hello", text="hello"))


def test_run_does_not_wrap_keyboard_interrupt(config, monkeypatch):
    class InterruptedChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise KeyboardInterrupt

    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")
    scripted_connections(injector, monkeypatch, InterruptedChannel())

    with pytest.raises(KeyboardInterrupt):
        injector.run()

    assert injector._connection is None


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


def test_publish_after_leaving_the_with_block_raises_runtime_error(config):
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")
    with injector:
        injector.publish(SampleMessage(message_id="inside", text="inside"))

    with pytest.raises(RuntimeError, match="Not connected"):
        injector.publish(SampleMessage(message_id="after", text="after"))

    assert injector._connection is None


def test_publish_marks_message_as_persistent_json_with_its_message_id(config):
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    injector.connect()

    injector.publish(SampleMessage(message_id="hello", text="hello"))

    properties = channel.published_messages[0].properties
    assert properties.content_type == "application/json"
    assert properties.delivery_mode == 2
    assert properties.message_id == "hello"
    assert properties.type == "sample.v1"  # the declared class's kicak_type


def test_publish_names_its_own_user_and_the_time_it_published_the_message(config, monkeypatch):
    monkeypatch.setattr("pykicak.abstracts._now", lambda: datetime(2026, 10, 1, 12, 0, tzinfo=UTC))
    channel = FakeChannel()
    injector = CountingInjector(config, channel, exchange_name="my-exchange")
    injector.connect()

    injector.publish(SampleMessage(message_id="hello", text="hello"))

    properties = channel.published_messages[0].properties
    # RabbitMQ verifies user_id; the injector starts the flow, so it is also its origin
    assert properties.user_id == "guest"
    assert properties.headers == {
        "x-kicak-original-publisher": "guest",
        "x-kicak-published-at": "2026-10-01T12:00:00+00:00",
    }


# --- the Message injected line --------------------------------------------------------------


@pytest.fixture
def stopwatch(monkeypatch) -> FakeStopwatch:
    fake = FakeStopwatch()
    monkeypatch.setattr("pykicak.injector._monotonic", fake)
    return fake


def injected_records(caplog) -> list[Any]:
    return [record for record in caplog.records if record.message.startswith("Message injected")]


class ConnectionLostChannel(FakeChannel):
    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        raise StreamLostError("connection lost")


def test_run_logs_how_long_each_part_of_an_injection_took(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.injector")

    class SlowChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            stopwatch.advance(0.125)  # until RabbitMQ confirms it
            super().basic_publish(exchange, routing_key, body, properties, mandatory)

    class SlowInjector(CountingInjector):
        def generate(self) -> SampleMessage | None:
            stopwatch.advance(0.25)
            return super().generate()

        def _create_connection(self):
            stopwatch.advance(0.5)
            return super()._create_connection()

    injector = SlowInjector(config, SlowChannel(), exchange_name="my-exchange")

    injector.run()

    [record] = injected_records(caplog)
    size = len(SampleMessage(message_id="message-1", text="message-1").to_bytes())
    assert record.levelname == "INFO"
    assert record.message == (
        "Message injected outcome=injected error=None type=SampleMessage id=message-1 "
        f"exchange=my-exchange attempts=1 message_bytes={size} total_seconds=0.875 "
        "generate_seconds=0.250 connect_seconds=0.500 publish_seconds=0.125 "
        "backoff_seconds=0.000"
    )
    assert kicak_values(record) == {
        "event": "message_injected",
        "outcome": "injected",
        "error": None,
        "type": "SampleMessage",
        "id": "message-1",
        "exchange": "my-exchange",
        "attempts": 1,
        "message_bytes": size,
        "total_seconds": 0.875,
        "generate_seconds": 0.25,
        "connect_seconds": 0.5,
        "publish_seconds": 0.125,
        "backoff_seconds": 0.0,
    }
    # Logged where the injector logs it, not in the helper that formats it
    assert (record.name, record.filename, record.funcName) == (
        "pykicak.injector",
        "injector.py",
        "_log_injected",
    )


def test_injected_line_counts_the_attempts_and_the_time_between_them(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.injector")
    monkeypatch.setattr("pykicak.injector.time.sleep", stopwatch.advance)
    retry_channel = FakeChannel()
    injector = CountingInjector(config, retry_channel, exchange_name="my-exchange")
    scripted_connections(injector, monkeypatch, ConnectionLostChannel(), retry_channel)

    injector.run()

    [record] = injected_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["error"], values["attempts"]) == ("injected", None, 2)
    assert values["backoff_seconds"] == 1.0  # the wait before the second attempt
    assert logged_pairs(record) == expected_pairs(values)


def test_injected_line_reports_an_injection_that_ran_out_of_attempts(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.injector")
    monkeypatch.setattr("pykicak.injector.time.sleep", stopwatch.advance)
    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")
    scripted_connections(
        injector,
        monkeypatch,
        ConnectionLostChannel(),
        ConnectionLostChannel(),
        ConnectionLostChannel(),
    )

    with pytest.raises(InjectionError):
        injector.run()

    [record] = injected_records(caplog)
    values = kicak_values(record)
    assert record.levelname == "INFO"
    assert (values["outcome"], values["error"], values["attempts"]) == (
        "failed",
        "StreamLostError",  # what ended the last attempt, the cause of the InjectionError
        3,
    )
    assert values["backoff_seconds"] == 3.0  # 1 and 2 seconds
    assert logged_pairs(record) == expected_pairs(values)


def test_injected_line_reports_an_injection_that_is_not_retried(config, caplog):
    caplog.set_level("INFO", logger="pykicak.injector")

    class UnroutableChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise UnroutableError([])

    injector = CountingInjector(config, UnroutableChannel(), exchange_name="my-exchange")

    with pytest.raises(InjectionError):
        injector.run()

    [record] = injected_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["error"], values["attempts"]) == (
        "failed",
        "UnroutableError",
        1,
    )


def test_publish_logs_the_injected_line_without_generate_seconds(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.injector")

    class SlowChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            stopwatch.advance(0.25)
            super().basic_publish(exchange, routing_key, body, properties, mandatory)

    injector = CountingInjector(config, SlowChannel(), exchange_name="my-exchange")
    injector.connect()

    injector.publish(SampleMessage(message_id="hello", text="hello"))

    [record] = injected_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["id"], values["attempts"]) == ("injected", "hello", 1)
    assert values["generate_seconds"] is None  # the caller made the message
    assert values["connect_seconds"] == 0.0  # the connection was open
    assert (values["publish_seconds"], values["total_seconds"]) == (0.25, 0.25)
    assert " generate_seconds=None " in record.message


def test_injected_line_reports_an_interrupted_injection(config, caplog):
    caplog.set_level("INFO", logger="pykicak.injector")

    class InterruptedChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise KeyboardInterrupt

    injector = CountingInjector(config, InterruptedChannel(), exchange_name="my-exchange")

    with pytest.raises(KeyboardInterrupt):  # not wrapped in an InjectionError
        injector.run()

    [record] = injected_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["error"]) == ("failed", "KeyboardInterrupt")


@pytest.mark.parametrize("generated", ["none", "error", "other-class"])
def test_logs_no_injected_line_for_a_message_never_published(config, caplog, generated):
    caplog.set_level("INFO", logger="pykicak.injector")

    class Injector(CountingInjector):
        def generate(self) -> SampleMessage | None:
            if generated == "error":
                raise ValueError("no data")
            if generated == "other-class":
                return OtherMessage(message_id="other", text="other")  # type: ignore[return-value]
            return None

    injector = Injector(config, FakeChannel(), exchange_name="my-exchange")

    if generated == "none":
        injector.run()
    else:
        with pytest.raises((ValueError, TypeError)):
            injector.run()

    assert injected_records(caplog) == []
