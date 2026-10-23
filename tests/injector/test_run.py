"""`run()`: short-lived injections, once or at an interval, and how they fail."""

import dataclasses
from datetime import UTC, datetime, timedelta

import pytest
from pika.exceptions import (
    AMQPConnectionError,
    ConnectionBlockedTimeout,
    StreamLostError,
    UnroutableError,
)

from pykicak.injector import InjectionError
from tests.fakes import FakeChannel
from tests.injector.helpers import (
    CountingInjector,
    OtherMessage,
    SampleMessage,
    scripted_connections,
)


@dataclasses.dataclass(frozen=True, slots=True)
class RenamedSample(SampleMessage):
    kicak_type = "renamed-sample.v1"


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


def test_run_does_not_wrap_keyboard_interrupt(config, monkeypatch):
    class InterruptedChannel(FakeChannel):
        def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
            raise KeyboardInterrupt

    injector = CountingInjector(config, FakeChannel(), exchange_name="my-exchange")
    scripted_connections(injector, monkeypatch, InterruptedChannel())

    with pytest.raises(KeyboardInterrupt):
        injector.run()

    assert injector._connection is None
