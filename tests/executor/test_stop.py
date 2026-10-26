"""Stopping gracefully with `stop()`, and the ways `run()` ends otherwise."""

import os
import signal
import threading
import time
from typing import Any

import pytest
from pika.exceptions import (
    AMQPConnectionError,
    StreamLostError,
)

from pykicak.executor import ExecutorStatus
from tests.executor.helpers import (
    ProcessingExecutor,
    SampleMessage,
    SettleFailingChannel,
    make_executor,
)
from tests.fakes import (
    FakeChannel,
    FakeConnection,
)


def single_connection(channel: FakeChannel):
    """Stand-in for _create_connection() that opens one connection, then fails the test."""
    connections = [FakeConnection(channel)]

    def create_connection() -> FakeConnection:
        if not connections:
            raise AssertionError("a stopping executor must not reconnect")
        return connections.pop()

    return create_connection


def test_graceful_stop_returns_normally_when_closing_the_connection_fails(config, caplog):
    class FailingCloseConnection(FakeConnection):
        def close(self) -> None:
            raise StreamLostError("connection close failed")

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, connection_class=FailingCloseConnection)
    channel.on_idle = executor.stop

    executor.run()  # does not raise

    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert channel.acked_delivery_tags == [1]
    close_errors = [r for r in caplog.records if "Failed to close RabbitMQ connection" in r.message]
    assert [record.levelname for record in close_errors] == ["ERROR"]


def test_run_leaves_in_flight_message_unsettled_when_stopped_externally(config):
    def interrupted(_message):
        raise KeyboardInterrupt

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=interrupted)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(KeyboardInterrupt):
        executor.run()

    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []
    assert channel.is_open is False


def test_stop_during_processing_finishes_the_message_then_returns(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_while_processing(message):
        executor_holder[0].stop()
        return message

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=stop_while_processing, destination_exchange="output-exchange"
    )
    executor_holder.append(executor)
    first = SampleMessage(message_id="in-progress", text="in progress")
    channel.queue_deliveries(
        first.to_bytes(), SampleMessage(message_id="never-taken", text="never taken").to_bytes()
    )

    executor.run()  # returns normally instead of raising StopConsuming

    assert executor.processed_messages == [first]
    assert channel.operations == ["publish output-exchange", "commit", "ack 1"]
    assert channel.is_open is False
    assert "stopped on request" in caplog.text


def test_stop_while_waiting_for_a_message_returns(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(
        SampleMessage(
            message_id="processed-before-the-wait", text="processed before the wait"
        ).to_bytes()
    )
    channel.on_idle = executor.stop

    executor.run()

    assert len(executor.processed_messages) == 1
    assert channel.acked_delivery_tags == [1]
    assert channel.inactivity_timeout == 1.0
    assert channel.is_open is False


def test_message_delivered_after_stop_request_is_left_unprocessed(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(
        SampleMessage(message_id="arrives-after-stop", text="arrives after stop").to_bytes()
    )
    channel.before_delivery = lambda _tag: executor.stop()

    executor.run()

    assert executor.processed_messages == []
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []
    assert channel.is_open is False
    assert "unprocessed for RabbitMQ to redeliver" in caplog.text


def test_run_after_stop_returns_without_connecting(config, monkeypatch):
    executor = make_executor(config, FakeChannel())

    def unexpected_connection():
        raise AssertionError("a stopped executor must not connect")

    monkeypatch.setattr(executor, "_create_connection", unexpected_connection)
    executor.stop()

    executor.run()


def test_run_after_connect_and_stop_closes_the_connection_connect_opened(config, monkeypatch):
    # run() promises the connection is closed at the end, also when it never starts consuming
    channel = FakeChannel()
    executor = make_executor(config, channel)
    executor.connect()
    connection = executor._connection
    assert connection is not None

    def unexpected_connection():
        raise AssertionError("a stopped executor must not connect")

    monkeypatch.setattr(executor, "_create_connection", unexpected_connection)
    executor.stop()

    executor.run()

    assert connection.is_open is False
    assert channel.is_open is False
    assert executor.get_info().status == ExecutorStatus.STOPPED
    with pytest.raises(RuntimeError, match="Not connected"):
        _ = executor.channel


def test_stop_during_reconnection_backoff_returns(config, monkeypatch, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor = make_executor(config, FakeChannel())
    connection_attempts = 0

    def unavailable():
        nonlocal connection_attempts
        connection_attempts += 1
        raise AMQPConnectionError("broker down")

    monkeypatch.setattr(executor, "_create_connection", unavailable)
    monkeypatch.setattr("pykicak.executor.time.sleep", lambda _seconds: executor.stop())

    executor.run()

    assert connection_attempts == 1
    assert "stopped on request while reconnecting" in caplog.text


def test_stop_returns_without_reconnecting_when_connection_is_lost_while_settling(
    config, monkeypatch, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_while_processing(_message):
        executor_holder[0].stop()

    channel = SettleFailingChannel(StreamLostError("connection lost"))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=stop_while_processing)
    executor_holder.append(executor)
    monkeypatch.setattr(executor, "_create_connection", single_connection(channel))

    executor.run()

    assert channel.acked_delivery_tags == []  # RabbitMQ requeues it with the lost connection
    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert "stopped on request while reconnecting" in caplog.text


def test_stop_returns_without_reconnecting_when_connection_is_lost_while_waiting(
    config, monkeypatch
):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    class StopThenDisconnectChannel(FakeChannel):
        def consume(self, queue, inactivity_timeout=None):
            executor_holder[0].stop()  # e.g. a signal handler, while pika waits for a message
            raise StreamLostError("connection lost")
            yield None, None, None

    channel = StopThenDisconnectChannel()
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    monkeypatch.setattr(executor, "_create_connection", single_connection(channel))

    executor.run()

    assert executor.get_info().status == ExecutorStatus.STOPPED


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="needs POSIX signals")
def test_stop_can_be_called_from_a_signal_handler(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="processed", text="processed").to_bytes())
    channel.on_idle = lambda: time.sleep(0.01)
    previous = signal.signal(signal.SIGUSR1, lambda _signum, _frame: executor.stop())
    timer = threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGUSR1))
    try:
        timer.start()
        executor.run()
    finally:
        timer.cancel()
        signal.signal(signal.SIGUSR1, previous)

    assert channel.acked_delivery_tags == [1]
    assert channel.is_open is False


def test_stop_can_be_called_from_another_thread(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="processed", text="processed").to_bytes())
    channel.on_idle = lambda: time.sleep(0.01)
    errors: list[BaseException] = []

    def run() -> None:
        try:
            executor.run()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    time.sleep(0.2)
    executor.stop()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    assert channel.acked_delivery_tags == [1]
