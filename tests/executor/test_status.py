"""What `get_info()` reports: the status, the delivery, and the counts."""

import dataclasses
import threading
import time
from datetime import datetime, timedelta
from typing import Any

import pika
import pika.channel
import pytest
from pika.exceptions import (
    AMQPConnectionError,
    StreamLostError,
)

from pykicak.executor import (
    DeadLetterCounts,
    ExecutorCounts,
    ExecutorInfo,
    ExecutorStatus,
    MalformedMessageError,
    TransientProcessingError,
)
from tests.executor.helpers import (
    CLOCK_START,
    FailingResultsChannel,
    ProcessingExecutor,
    SampleMessage,
    SettleFailingChannel,
    UnroutableResultsChannel,
    connection_lost,
    dead_lettered,
    make_executor,
    reconnecting_executor,
    run_until_stopped,
)
from tests.fakes import (
    SAMPLE_TYPE,
    FakeChannel,
    FakeConnection,
)


def crash_on_first_call(executor_holder: list[ProcessingExecutor[Any, Any]], statuses: list[str]):
    """execute() handler: raises on the first message, records the status on later ones."""
    calls = 0

    def handler(_message):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ZeroDivisionError("bug in application code")
        statuses.append(executor_holder[0].get_info().status)

    return handler


class StatusRecordingChannel(FakeChannel):
    """Records the executor's status, read by a monitor, at each step of a delivery."""

    def __init__(
        self, executor_holder: list[ProcessingExecutor[Any, Any]], statuses: list[str]
    ) -> None:
        super().__init__()
        self._executor_holder = executor_holder
        self._statuses = statuses

    def _record(self, step: str) -> None:
        self._statuses.append(f"{step}: {self._executor_holder[0].get_info().status}")

    def queue_declare(
        self, queue, durable=False, exclusive=False, auto_delete=False, arguments=None
    ):
        self._record(f"declare {queue}")
        super().queue_declare(queue, durable, exclusive, auto_delete, arguments)

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        self._record("publish")
        super().basic_publish(exchange, routing_key, body, properties, mandatory)

    def basic_ack(self, delivery_tag: int) -> None:
        self._record("ack")
        super().basic_ack(delivery_tag)

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        self._record("nack")
        super().basic_nack(delivery_tag, multiple, requeue)


class MessageRecordingChannel(StatusRecordingChannel):
    """Records the message_id of the message that get_info() reports at each step, or None."""

    def _record(self, step: str) -> None:
        message = self._executor_holder[0].get_info().message
        self._statuses.append(f"{step}: {None if message is None else message.message_id}")


class InfoRecordingChannel(FakeChannel):
    """Records the executor's get_info() just before each acknowledgement, and while idle."""

    def __init__(self, executor_holder: list[ProcessingExecutor[Any, Any]]) -> None:
        super().__init__()
        self._executor_holder = executor_holder
        self.reported: list[ExecutorInfo[Any]] = []
        self.on_idle = self._record

    def _record(self) -> None:
        self.reported.append(self._executor_holder[0].get_info())

    def basic_ack(self, delivery_tag: int) -> None:
        self._record()
        super().basic_ack(delivery_tag)


def test_status_values_are_plain_strings():
    assert [status.value for status in ExecutorStatus] == [
        "CREATED",
        "UPSTREAM_RECOVER",
        "WAITING_FOR_MESSAGE",
        "PROCESSING",
        "DOWNSTREAM_RECOVER",
        "SENDING_RESULT",
        "STOPPING",
        "STOPPED",
        "CRASHED",
    ]
    assert ExecutorStatus.CRASHED == "CRASHED"


def test_info_after_creation(config, clock):
    executor = make_executor(config, FakeChannel())
    checked_at = clock.advance()

    info = executor.get_info()

    assert info == ExecutorInfo(
        status=ExecutorStatus.CREATED,
        status_changed_at=CLOCK_START,
        status_checked_at=checked_at,
        stop_requested=False,
        stop_requested_at=None,
        message=None,
        attempt=None,
        redelivered=None,
        retry_round=None,
        counts=ExecutorCounts(),
    )
    assert type(info.status) is ExecutorStatus
    assert info.status == "CREATED"


def test_info_is_frozen(config):
    info = make_executor(config, FakeChannel()).get_info()

    with pytest.raises(dataclasses.FrozenInstanceError):
        info.status = ExecutorStatus.STOPPED  # type: ignore[misc]


def test_info_repr_lists_every_field(config, clock):
    executor = make_executor(config, FakeChannel())

    assert repr(executor.get_info()) == (
        "ExecutorInfo(status=<ExecutorStatus.CREATED: 'CREATED'>, "
        "status_changed_at=datetime.datetime(2026, 10, 1, 12, 0, tzinfo=datetime.timezone.utc), "
        "status_checked_at=datetime.datetime(2026, 10, 1, 12, 0, tzinfo=datetime.timezone.utc), "
        "stop_requested=False, stop_requested_at=None, message=None, attempt=None, "
        "redelivered=None, retry_round=None, counts=ExecutorCounts(completed=0, "
        "dead_lettered=DeadLetterCounts(malformed=0, retries_exhausted=0, result_too_large=0, "
        "unexpected_error=0), moved_to_retry=0, requeued=0, abandoned=0))"
    )


def test_info_repr_shows_the_message_only_by_type_and_id(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[ExecutorInfo[Any]] = []
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda _message: reported.append(executor_holder[0].get_info())
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="secret payload").to_bytes())

    run_until_stopped(executor)

    for text in (repr(reported[0]), str(reported[0])):
        assert "message=<SampleMessage message_id='input'>" in text
        assert "secret payload" not in text


def test_info_reports_the_message_execute_receives(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[ExecutorInfo[Any]] = []
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=lambda _message: reported.append(executor_holder[0].get_info())
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert reported[0].status == ExecutorStatus.PROCESSING
    assert reported[0].message is executor.processed_messages[0]


def test_status_follows_each_step_of_a_message(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    statuses: list[str] = []

    def record_status(message):
        statuses.append(f"execute: {executor_holder[0].get_info().status}")
        return message

    channel = StatusRecordingChannel(executor_holder, statuses)
    results_channel = StatusRecordingChannel(executor_holder, statuses)
    executor = make_executor(
        config,
        channel,
        handler=record_status,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.on_idle = lambda: statuses.append(f"idle: {executor.get_info().status}")

    run_until_stopped(executor)

    assert statuses[:14] == [
        # The first connection, with its topology, is attempted while the status is CREATED
        "declare input-queue: CREATED",
        "declare input-queue.dlq: CREATED",
        "declare input-queue.retry: CREATED",
        "declare output-queue: CREATED",
        "execute: PROCESSING",
        # The first verification of the output side happens in PROCESSING
        "declare output-queue: PROCESSING",
        "publish: SENDING_RESULT",
        "ack: SENDING_RESULT",
        # The input side is verified before waiting for the next message
        "declare input-queue: SENDING_RESULT",
        "declare input-queue.dlq: SENDING_RESULT",
        "declare input-queue.retry: SENDING_RESULT",
        "idle: WAITING_FOR_MESSAGE",
        "idle: WAITING_FOR_MESSAGE",
        "idle: WAITING_FOR_MESSAGE",
    ]


def test_terminator_acknowledges_in_processing_status(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    statuses: list[str] = []
    channel = StatusRecordingChannel(executor_holder, statuses)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert "ack: PROCESSING" in statuses


def test_info_reports_the_message_until_it_is_settled(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    steps: list[str] = []

    def record_message(_message):
        message = executor_holder[0].get_info().message
        steps.append(f"execute: {None if message is None else message.message_id}")
        return SampleMessage(message_id="result", text="result")

    channel = MessageRecordingChannel(executor_holder, steps)
    results_channel = MessageRecordingChannel(executor_holder, steps)
    executor = make_executor(
        config,
        channel,
        handler=record_message,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.on_idle = lambda: steps.append(f"idle: {executor.get_info().message}")

    run_until_stopped(executor)

    assert steps[:12] == [
        "declare input-queue: None",
        "declare input-queue.dlq: None",
        "declare input-queue.retry: None",
        "declare output-queue: None",
        "execute: input",
        "declare output-queue: input",
        "publish: input",
        "ack: input",
        # Acknowledged: the input side is verified without a message being processed
        "declare input-queue: None",
        "declare input-queue.dlq: None",
        "declare input-queue.retry: None",
        "idle: None",
    ]


def test_info_has_no_message_for_a_delivery_that_cannot_be_decoded(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    steps: list[str] = []
    channel = MessageRecordingChannel(executor_holder, steps)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(b"not json")

    run_until_stopped(executor)

    assert "publish: None" in steps
    assert dead_lettered(channel) == [1]


def test_info_drops_the_message_when_the_connection_is_lost_while_settling(
    config, monkeypatch, sleeps
):
    body = SampleMessage(message_id="input", text="input").to_bytes()
    original_channel = SettleFailingChannel(connection_lost())
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel)
    channels = [original_channel, replacement_channel]
    reconnecting: list[ExecutorInfo[Any]] = []

    def create_connection() -> FakeConnection:
        reconnecting.append(executor.get_info())
        return FakeConnection(channels.pop(0))

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)

    assert [(info.status, info.message) for info in reconnecting] == [
        (ExecutorStatus.CREATED, None),
        (ExecutorStatus.UPSTREAM_RECOVER, None),
    ]
    assert replacement_channel.acked_delivery_tags == [1]


def test_status_is_upstream_recover_while_reconnecting(config, monkeypatch, sleeps, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel)
    statuses: list[str] = []
    attempts = 0

    def create_connection() -> FakeConnection:
        nonlocal attempts
        attempts += 1
        statuses.append(executor.get_info().status)
        if attempts < 3:
            raise AMQPConnectionError("broker down")
        return FakeConnection(channel)

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)

    assert statuses == ["CREATED", "UPSTREAM_RECOVER", "UPSTREAM_RECOVER"]
    assert channel.acked_delivery_tags == [1]
    entered = [r for r in caplog.records if "is recovering: UPSTREAM_RECOVER" in r.message]
    assert [record.levelname for record in entered] == ["WARNING"]
    left = [r for r in caplog.records if "recovered from UPSTREAM_RECOVER" in r.message]
    assert [record.levelname for record in left] == ["INFO"]
    assert "WAITING_FOR_MESSAGE" in left[0].message


def test_status_is_downstream_recover_with_the_message_while_results_are_held(config, monkeypatch):
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    results_channel = UnroutableResultsChannel(True, False)
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=results_channel,
        destination_exchange="output-exchange",
    )
    reported: list[ExecutorInfo[Any]] = []
    monkeypatch.setattr(
        "pykicak.executor.time.sleep", lambda _seconds: reported.append(executor.get_info())
    )

    run_until_stopped(executor)

    assert [info.status for info in reported] == ["DOWNSTREAM_RECOVER"]
    assert reported[0].message == SampleMessage(message_id="input", text="input")
    assert channel.acked_delivery_tags == [1]


def test_setting_the_same_status_again_keeps_its_change_time(config, clock):
    executor = make_executor(config, FakeChannel())
    entered_at = clock.advance()
    executor._status.set_status(ExecutorStatus.WAITING_FOR_MESSAGE)

    clock.advance()
    executor._status.set_status(ExecutorStatus.WAITING_FOR_MESSAGE)

    assert executor.get_info().status_changed_at == entered_at


def test_info_after_stop(config, clock):
    executor = make_executor(config, FakeChannel())
    stop_at = clock.advance()

    executor.stop()

    info = executor.get_info()
    assert (info.status, info.status_changed_at) == ("STOPPING", stop_at)
    assert (info.stop_requested, info.stop_requested_at) == (True, stop_at)


def test_info_reports_the_message_in_progress_while_stopping(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[ExecutorInfo[Any]] = []

    def stop_then_read_info(_message):
        executor_holder[0].stop()
        reported.append(executor_holder[0].get_info())

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=stop_then_read_info)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="in-progress", text="in progress").to_bytes())

    executor.run()

    assert reported[0].status == "STOPPING"
    assert reported[0].message == SampleMessage(message_id="in-progress", text="in progress")
    info = executor.get_info()
    assert (info.status, info.stop_requested, info.message) == ("STOPPED", True, None)


def test_status_is_stopped_after_stop_while_waiting(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.on_idle = executor.stop

    executor.run()

    assert executor.get_info().status == "STOPPED"


def test_status_is_stopped_when_run_follows_stop(config):
    executor = make_executor(config, FakeChannel())
    executor.stop()

    executor.run()

    assert executor.get_info().status == "STOPPED"


@pytest.mark.parametrize(
    "failure", [ZeroDivisionError("bug"), KeyboardInterrupt()], ids=["error", "hard-stop"]
)
def test_status_is_crashed_when_run_raises(config, failure, caplog):
    def fail(_message):
        raise failure

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=fail)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(type(failure)):
        executor.run()

    info = executor.get_info()
    assert (info.status, info.stop_requested, info.message) == ("CRASHED", False, None)
    assert executor._status.delivery is None  # the message is not kept after the crash
    assert f"crashed with {type(failure).__name__}" in caplog.text


def test_stop_after_crash_keeps_crashed_status_and_its_change_time(config, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=crash_on_first_call([], []))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    crashed_at = clock.advance()
    with pytest.raises(ZeroDivisionError):
        executor.run()

    stop_at = clock.advance()
    executor.stop()

    info = executor.get_info()
    assert (info.status, info.status_changed_at) == ("CRASHED", crashed_at)
    assert (info.stop_requested, info.stop_requested_at) == (True, stop_at)


def test_run_after_a_crash_and_stop_keeps_crashed_and_does_not_connect(config, clock, monkeypatch):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=crash_on_first_call([], []))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    crashed_at = clock.advance()
    with pytest.raises(ZeroDivisionError):
        executor.run()
    executor.stop()

    def unexpected_connection():
        raise AssertionError("a stopped executor must not connect")

    monkeypatch.setattr(executor, "_create_connection", unexpected_connection)
    clock.advance()

    executor.run()

    info = executor.get_info()
    assert (info.status, info.status_changed_at) == ("CRASHED", crashed_at)


def test_run_again_after_a_graceful_stop_keeps_the_time_it_stopped(config, clock):
    executor = make_executor(config, FakeChannel())
    executor.stop()
    stopped_at = clock.advance()
    executor.run()
    clock.advance()

    executor.run()

    info = executor.get_info()
    assert (info.status, info.status_changed_at) == ("STOPPED", stopped_at)


def test_status_is_upstream_recover_when_run_follows_a_crash(config, monkeypatch):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    statuses: list[str] = []
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=crash_on_first_call(executor_holder, statuses)
    )
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    with pytest.raises(ZeroDivisionError):
        executor.run()
    channel.is_open = True

    def create_connection() -> FakeConnection:
        statuses.append(f"connecting: {executor.get_info().status}")
        return FakeConnection(channel)

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)  # consumes the queued delivery again

    assert statuses == ["connecting: UPSTREAM_RECOVER", "PROCESSING"]
    # Counted across both runs
    assert executor.get_info().counts == ExecutorCounts(
        completed=1, dead_lettered=DeadLetterCounts(unexpected_error=1)
    )


def test_info_can_be_read_from_another_thread_while_running(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.on_idle = lambda: time.sleep(0.01)
    thread = threading.Thread(target=executor.run)
    thread.start()
    time.sleep(0.1)

    waiting = executor.get_info()
    executor.stop()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert (waiting.status, waiting.stop_requested) == ("WAITING_FOR_MESSAGE", False)
    assert executor.get_info().status == "STOPPED"


def test_info_times_are_timezone_aware_datetimes_in_utc(config):
    executor = make_executor(config, FakeChannel())
    executor.stop()

    info = executor.get_info()

    for value in (info.status_changed_at, info.status_checked_at, info.stop_requested_at):
        assert isinstance(value, datetime)
        assert value.utcoffset() == timedelta(0)
    # Datetimes compare and subtract directly, e.g. to tell how long a status has lasted
    assert info.status_checked_at - info.status_changed_at >= timedelta(0)


def test_status_changed_at_follows_status_changes(config, clock):
    executor = make_executor(config, FakeChannel())

    stopping_at = clock.advance()
    executor.stop()
    assert executor.get_info().status_changed_at == stopping_at

    stopped_at = clock.advance()
    executor.run()
    info = executor.get_info()
    assert info.status_changed_at == stopped_at
    assert info.stop_requested_at == stopping_at


def test_status_checked_at_is_the_time_of_each_call(config, clock):
    executor = make_executor(config, FakeChannel())

    first_at = clock.advance(5)
    first = executor.get_info()
    second_at = clock.advance(5)
    second = executor.get_info()

    assert first.status_checked_at == first_at
    assert second.status_checked_at == second_at
    # Reading changes nothing: the status is still the one entered at creation
    assert first.status_changed_at == second.status_changed_at == CLOCK_START


def test_repeated_stop_keeps_the_first_stop_time(config, clock):
    executor = make_executor(config, FakeChannel())
    first_stop_at = clock.advance()
    executor.stop()

    clock.advance()
    executor.stop()

    info = executor.get_info()
    assert info.status_changed_at == first_stop_at
    assert info.stop_requested_at == first_stop_at


def test_counts_record_how_each_delivery_ended(config, sleeps):
    def handle(message):
        if message.message_id == "flaky":
            raise TransientProcessingError("service unavailable")
        if message.message_id == "invalid":
            raise MalformedMessageError("fails application validation")
        return None

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=handle)
    channel.queue_deliveries(
        SampleMessage(message_id="ok", text="ok").to_bytes(),
        b"not json",
        SampleMessage(message_id="invalid", text="invalid").to_bytes(),
        SampleMessage(message_id="flaky", text="flaky").to_bytes(),
        SampleMessage(message_id="also-ok", text="also ok").to_bytes(),
    )

    run_until_stopped(executor)

    assert executor.get_info().counts == ExecutorCounts(
        completed=2, dead_lettered=DeadLetterCounts(malformed=2), moved_to_retry=1
    )


def test_dead_letter_counts_add_up_to_their_total():
    counts = DeadLetterCounts(
        malformed=3, retries_exhausted=4, result_too_large=2, unexpected_error=1
    )

    assert counts.total == 10
    assert DeadLetterCounts().total == 0


def test_counts_a_delivery_abandoned_by_a_hard_stop(config):
    def interrupted(_message):
        raise KeyboardInterrupt

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=interrupted)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(KeyboardInterrupt):
        executor.run()

    assert executor.get_info().counts == ExecutorCounts(abandoned=1)


def test_counts_a_delivery_whose_results_were_lost_with_the_connection(config, monkeypatch):
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    replacement_channel = FakeChannel()
    replacement_channel.queue_deliveries(  # RabbitMQ's redelivery
        SampleMessage(message_id="input", text="input").to_bytes(), redelivered=True
    )
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [
            (channel, FailingResultsChannel("commit", StreamLostError("connection lost"))),
            (replacement_channel, FakeChannel()),
        ],
        lambda received: received,
    )

    run_until_stopped(executor)

    assert executor.get_info().counts == ExecutorCounts(completed=1, abandoned=1)


def test_counts_nothing_for_a_delivery_received_after_stop(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.before_delivery = lambda _tag: executor.stop()

    executor.run()

    assert executor.processed_messages == []
    assert executor.get_info().counts == ExecutorCounts()


def test_a_delivery_is_counted_when_it_is_no_longer_reported(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    channel = InfoRecordingChannel(executor_holder)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    acknowledging, waiting = channel.reported[:2]
    # While it is acknowledged, the message is reported and not yet counted; then the reverse
    assert acknowledging.message is not None and acknowledging.counts.completed == 0
    assert waiting.message is None and waiting.counts.completed == 1


def test_info_reports_which_call_of_execute_is_under_way(config, sleeps):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    attempts: list[int | None] = []

    def fail_twice(_message):
        attempts.append(executor_holder[0].get_info().attempt)
        if len(attempts) < 3:
            raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=fail_twice)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert attempts == [1, 2, 3]
    assert executor.get_info().counts == ExecutorCounts(completed=1)


def test_info_attempts_start_again_for_every_delivery(config, sleeps):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    attempts: list[int | None] = []

    def record_attempt(_message):
        attempts.append(executor_holder[0].get_info().attempt)

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=record_attempt)
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="first", text="first").to_bytes(),
        SampleMessage(message_id="second", text="second").to_bytes(),
    )

    run_until_stopped(executor)

    assert attempts == [1, 1]


def test_info_reports_whether_the_delivery_was_redelivered(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[bool | None] = []
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: reported.append(executor_holder[0].get_info().redelivered),
    )
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="again", text="again").to_bytes(), redelivered=True
    )
    channel.queue_deliveries(SampleMessage(message_id="first", text="first").to_bytes())

    run_until_stopped(executor)

    assert reported == [True, False]


@pytest.mark.parametrize(
    ("headers", "retry_round"),
    [
        (None, 0),
        ({"x-kicak-retry-round": 3}, 3),
        ({"x-kicak-retry-round": 0}, 0),
        ({"x-kicak-retry-round": "3"}, 0),
        ({"x-kicak-retry-round": -1}, 0),
        ({"x-kicak-retry-round": True}, 0),
        ({"x-kicak-retry-round": 3, "x-kicak-dead-letter-reason": "retries-exhausted"}, 0),
    ],
    ids=["never-retried", "third-round", "zero", "text", "negative", "bool", "sent-back"],
)
def test_info_reports_the_retry_round_of_the_message(config, headers, retry_round):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    reported: list[int | None] = []
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: reported.append(executor_holder[0].get_info().retry_round),
    )
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, headers=headers),
    )

    run_until_stopped(executor)

    assert reported == [retry_round]


def test_info_reports_no_delivery_context_without_a_message(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    channel = InfoRecordingChannel(executor_holder)
    executor = make_executor(config, channel)
    executor_holder.append(executor)
    channel.queue_deliveries(b"not json", redelivered=True)

    run_until_stopped(executor)

    # Dead-lettering an undecodable delivery, then waiting: no delivery is reported
    assert [
        (info.message, info.attempt, info.redelivered, info.retry_round)
        for info in channel.reported[:2]
    ] == [(None, None, None, None)] * 2


def test_info_drops_the_delivery_context_once_run_has_ended(config):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []
    channel = FakeChannel()

    def crash(_message):
        raise ZeroDivisionError("bug in application code")

    executor = make_executor(config, channel, handler=crash)
    executor_holder.append(executor)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), redelivered=True
    )

    with pytest.raises(ZeroDivisionError):
        executor.run()

    info = executor.get_info()
    assert (info.message, info.attempt, info.redelivered, info.retry_round) == (
        None,
        None,
        None,
        None,
    )
