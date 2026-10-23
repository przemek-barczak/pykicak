"""The `Message injected` line logged for every message handed to RabbitMQ."""

from typing import Any

import pytest
from pika.exceptions import UnroutableError

from pykicak.injector import InjectionError
from tests.fakes import (
    FakeChannel,
    FakeStopwatch,
    expected_pairs,
    kicak_values,
    logged_pairs,
    replace_in_pykicak,
)
from tests.injector.helpers import (
    ConnectionLostChannel,
    CountingInjector,
    OtherMessage,
    SampleMessage,
    scripted_connections,
)


@pytest.fixture
def stopwatch(monkeypatch) -> FakeStopwatch:
    fake = FakeStopwatch()
    replace_in_pykicak(monkeypatch, "_monotonic", fake)
    return fake


def injected_records(caplog) -> list[Any]:
    return [record for record in caplog.records if record.message.startswith("Message injected")]


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
    # Logged where the injector logs it, not in the helper that formats it, and with the logger
    # of the public module, whichever part of the injector logs it
    assert (record.name, record.filename, record.funcName) == (
        "pykicak.injector",
        "statistics.py",
        "log_injected",
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
