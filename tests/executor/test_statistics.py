"""The `Message handled` line and the other lines that tell durations."""

import dataclasses
from typing import Any

import pika
import pika.channel
import pytest
from pika.exceptions import (
    AMQPConnectionError,
    StreamLostError,
)

import pykicak._executor.status as status_module
from pykicak.executor import (
    DeadLetterCounts,
    ExecutorCounts,
    MalformedMessageError,
    TransientProcessingError,
)
from tests.executor.helpers import (
    FailingResultsChannel,
    ProcessingExecutor,
    SampleMessage,
    UnroutableResultsChannel,
    handled_records,
    make_executor,
    reconnecting_executor,
    run_until_stopped,
)
from tests.fakes import (
    SAMPLE_TYPE,
    FakeChannel,
    FakeConnection,
    FakeStopwatch,
    expected_pairs,
    kicak_values,
    logged_pairs,
    replace_in_pykicak,
)


@pytest.fixture
def stopwatch(monkeypatch) -> FakeStopwatch:
    fake = FakeStopwatch()
    replace_in_pykicak(monkeypatch, "_monotonic", fake)
    return fake


class SlowChannel(FakeChannel):
    """Takes `seconds` of the stopwatch for every publish, commit, acknowledgement, and nack."""

    def __init__(self, stopwatch: FakeStopwatch, seconds: float) -> None:
        super().__init__()
        self._stopwatch = stopwatch
        self._seconds = seconds

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False):
        self._stopwatch.advance(self._seconds)
        super().basic_publish(exchange, routing_key, body, properties, mandatory)

    def tx_commit(self) -> None:
        self._stopwatch.advance(self._seconds)
        super().tx_commit()

    def basic_ack(self, delivery_tag: int) -> None:
        self._stopwatch.advance(self._seconds)
        super().basic_ack(delivery_tag)

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        self._stopwatch.advance(self._seconds)
        super().basic_nack(delivery_tag, multiple, requeue)


def test_logs_how_long_each_part_of_a_completed_delivery_took(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.executor")
    message = SampleMessage(message_id="input", text="input")
    second = SampleMessage(message_id="input-2", text="second result")

    def handle(received):
        stopwatch.advance(2.0)
        return [received, second]

    channel = SlowChannel(stopwatch, 0.125)
    channel.queue_deliveries(message.to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=handle,
        results_channel=SlowChannel(stopwatch, 0.25),
        destination_exchange="output-exchange",
    )

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    assert record.levelname == "INFO"
    assert record.message == (
        "Message handled outcome=completed reason=None type=SampleMessage id=input "
        "queue=input-queue delivery_tag=1 redelivered=False retry_round=0 attempts=1 results=2 "
        "input_bytes=40 result_bytes=90 largest_result_bytes=50 "
        "total_seconds=2.875 execute_seconds=2.000 backoff_seconds=0.000 publish_seconds=0.750 "
        "held_seconds=0.000 settle_seconds=0.125 queued_seconds=None"
    )
    # Logged where the executor logs it, not in the helper that formats it, and with the logger
    # of the public module, whichever part of the executor logs it
    assert (record.name, record.filename, record.funcName) == (
        "pykicak.executor",
        "statistics.py",
        "log_handled",
    )
    assert kicak_values(record) == {
        "event": "message_handled",
        "outcome": "completed",
        "reason": None,
        "type": "SampleMessage",
        "id": "input",
        "queue": "input-queue",
        "delivery_tag": 1,
        "redelivered": False,
        "retry_round": 0,
        "attempts": 1,
        "results": 2,
        "input_bytes": len(message.to_bytes()),  # 40
        "result_bytes": len(message.to_bytes()) + len(second.to_bytes()),  # 90
        "largest_result_bytes": len(second.to_bytes()),  # 50
        "total_seconds": 2.875,  # the parts below, two publishes and a commit taking 0.75
        "execute_seconds": 2.0,
        "backoff_seconds": 0.0,
        "publish_seconds": 0.75,
        "held_seconds": 0.0,
        "settle_seconds": 0.125,
        "queued_seconds": None,  # delivered without an x-kicak-published-at header
    }


def test_logs_one_handled_line_for_every_counted_delivery_whatever_its_outcome(
    config, sleeps, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")

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
    )

    run_until_stopped(executor)

    records = handled_records(caplog)
    assert [record.levelname for record in records] == ["INFO"] * 4
    values = [kicak_values(record) for record in records]
    assert [(v["outcome"], v["reason"], v["id"], v["attempts"]) for v in values] == [
        ("completed", None, "ok", 1),
        ("dead_lettered", "malformed", None, 0),  # not decoded: no message_id property either
        ("dead_lettered", "malformed", "invalid", 1),
        ("moved_to_retry", None, "flaky", 3),
    ]
    # The size of every body, also of one that could not be decoded
    assert [v["input_bytes"] for v in values] == [
        len(SampleMessage(message_id="ok", text="ok").to_bytes()),
        len(b"not json"),
        len(SampleMessage(message_id="invalid", text="invalid").to_bytes()),
        len(SampleMessage(message_id="flaky", text="flaky").to_bytes()),
    ]
    # A terminator has no results
    assert {(v["results"], v["result_bytes"], v["largest_result_bytes"]) for v in values} == {
        (0, 0, 0)
    }
    assert executor.get_info().counts == ExecutorCounts(
        completed=1, dead_lettered=DeadLetterCounts(malformed=2), moved_to_retry=1
    )
    for record, value in zip(records, values, strict=True):
        # The text holds the same values as the kicak attribute, durations to the millisecond
        assert logged_pairs(record) == expected_pairs(value)
        # Measured with the real clock: the parts never exceed the whole
        parts = [value[f"{part}_seconds"] for part in ("execute", "backoff", "publish", "held")]
        assert all(seconds >= 0 for seconds in [*parts, value["settle_seconds"]])
        assert sum(parts) + value["settle_seconds"] <= value["total_seconds"]


def test_handled_line_names_each_outcome_like_the_count_of_it():
    counted = {count.name for count in dataclasses.fields(ExecutorCounts)}

    for outcome in status_module._Outcome:
        assert status_module._logged_outcome(outcome) == (outcome.name.lower(), None)
        assert outcome.name.lower() in counted
    for reason in status_module._DeadLetterReason:
        assert status_module._logged_outcome(reason) == ("dead_lettered", reason.value)


def test_handled_line_adds_up_the_attempts_of_execute_and_the_waits_between_them(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    monkeypatch.setattr("pykicak.executor.time.sleep", stopwatch.advance)
    attempts = 0

    def flaky(_message):
        nonlocal attempts
        attempts += 1
        stopwatch.advance(1.0)
        if attempts < 3:
            raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=flaky)

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert values["outcome"] == "completed"
    assert values["attempts"] == 3
    assert values["execute_seconds"] == 3.0
    assert values["backoff_seconds"] == 1.5  # 0.5 and 1.0 seconds
    assert values["total_seconds"] == 4.5
    assert values["results"] == 0  # a terminator


def test_handled_line_counts_the_copy_to_the_retry_queue_as_settling(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.executor")

    def unavailable(_message):
        raise TransientProcessingError("service unavailable")

    channel = SlowChannel(stopwatch, 0.25)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, headers={"x-kicak-retry-round": 2}),
    )
    executor = make_executor(config, channel, handler=unavailable, max_processing_attempts=1)

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["retry_round"], values["attempts"]) == (
        "moved_to_retry",
        2,
        1,
    )
    assert values["settle_seconds"] == 0.5  # the copy, then the acknowledgement
    assert values["total_seconds"] == 0.5


def test_handled_line_is_logged_for_an_unexpected_error_before_it_stops_the_executor(
    config, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")

    def broken(_message):
        stopwatch.advance(0.5)
        raise RuntimeError("bug")

    channel = SlowChannel(stopwatch, 0.25)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=broken)

    with pytest.raises(RuntimeError, match="bug"):
        executor.run()

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["reason"]) == ("dead_lettered", "unexpected-error")
    assert (values["execute_seconds"], values["settle_seconds"]) == (0.5, 0.5)
    crashed = next(i for i, r in enumerate(caplog.records) if "crashed with" in r.message)
    assert caplog.records.index(record) < crashed


def test_handled_line_reports_a_delivery_abandoned_by_a_hard_stop(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")

    def interrupted(_message):
        raise KeyboardInterrupt

    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(config, channel, handler=interrupted)

    with pytest.raises(KeyboardInterrupt):
        executor.run()

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert (values["outcome"], values["reason"], values["attempts"]) == ("abandoned", None, 1)
    assert values["settle_seconds"] == 0.0


def test_handled_line_reports_results_lost_with_the_connection_as_abandoned(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    message = SampleMessage(message_id="input", text="input")
    channel = FakeChannel()
    channel.queue_deliveries(message.to_bytes())

    class LosingCommitChannel(FailingResultsChannel):
        def tx_commit(self) -> None:
            stopwatch.advance(0.25)
            super().tx_commit()

    replacement_channel = FakeChannel()
    replacement_channel.queue_deliveries(message.to_bytes(), redelivered=True)
    executor = reconnecting_executor(
        config,
        monkeypatch,
        [
            (channel, LosingCommitChannel("commit", StreamLostError("connection lost"))),
            (replacement_channel, FakeChannel()),
        ],
        lambda received: received,
    )

    run_until_stopped(executor)

    first, second = (kicak_values(record) for record in handled_records(caplog))
    assert (first["outcome"], first["redelivered"], first["results"]) == ("abandoned", False, 1)
    assert (first["publish_seconds"], first["settle_seconds"]) == (0.25, 0.0)
    assert (second["outcome"], second["redelivered"]) == ("completed", True)


def test_handled_line_reports_how_long_unroutable_results_were_held(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    monkeypatch.setattr("pykicak.executor.random.random", lambda: 1.0)
    channel = FakeChannel()
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    executor = make_executor(
        config,
        channel,
        handler=lambda message: message,
        results_channel=UnroutableResultsChannel(True),
        destination_exchange="output-exchange",
    )

    def sleep_then_stop(seconds: float) -> None:
        stopwatch.advance(seconds)
        executor.stop()

    monkeypatch.setattr("pykicak.executor.time.sleep", sleep_then_stop)

    executor.run()

    [record] = handled_records(caplog)
    values = kicak_values(record)
    assert values["outcome"] == "requeued"
    assert values["held_seconds"] == 0.5
    assert values["total_seconds"] == 0.5


def test_logs_no_handled_line_for_a_delivery_received_after_stop(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())
    channel.before_delivery = lambda _tag: executor.stop()

    executor.run()

    assert handled_records(caplog) == []


def test_recovered_line_tells_how_long_the_recovery_lasted(
    config, monkeypatch, sleeps, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel)
    attempts = 0

    def create_connection() -> FakeConnection:
        nonlocal attempts
        attempts += 1
        stopwatch.advance(1.0)
        if attempts < 3:
            raise AMQPConnectionError("broker down")
        return FakeConnection(channel)

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    run_until_stopped(executor)

    [record] = [r for r in caplog.records if "recovered from" in r.message]
    # Entered once the first attempt failed, left once the third one connected
    assert record.message == (
        "Executor for queue input-queue recovered from UPSTREAM_RECOVER: WAITING_FOR_MESSAGE "
        "recovery_seconds=2.000"
    )
    assert kicak_values(record) == {
        "event": "recovered",
        "queue": "input-queue",
        "recovered_from": "UPSTREAM_RECOVER",
        "status": "WAITING_FOR_MESSAGE",
        "recovery_seconds": 2.0,
    }


def test_stopped_line_tells_how_long_the_stop_took(config, caplog, stopwatch):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_then_finish(_message):
        executor_holder[0].stop()
        stopwatch.advance(3.0)  # the message in progress is finished first

    executor = make_executor(config, channel, handler=stop_then_finish)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    executor.run()

    [record] = [r for r in caplog.records if "stopped on request" in r.message]
    assert record.message == "Executor for queue input-queue stopped on request stop_seconds=3.000"
    assert kicak_values(record) == {
        "event": "stopped",
        "queue": "input-queue",
        "stop_seconds": 3.0,
    }
    # Logged once the connection is closed, after the message's own line
    assert caplog.records.index(handled_records(caplog)[0]) < caplog.records.index(record)


def test_stopped_line_tells_how_long_a_stop_while_reconnecting_took(
    config, monkeypatch, caplog, stopwatch
):
    caplog.set_level("INFO", logger="pykicak.executor")
    executor = make_executor(config, FakeChannel())

    def create_connection() -> FakeConnection:
        executor.stop()
        stopwatch.advance(0.5)
        raise AMQPConnectionError("broker down")

    monkeypatch.setattr(executor, "_create_connection", create_connection)

    executor.run()

    [record] = [r for r in caplog.records if "stopped on request" in r.message]
    assert record.message == (
        "Executor for queue input-queue stopped on request while reconnecting stop_seconds=0.500"
    )
    assert kicak_values(record)["stop_seconds"] == 0.5


@pytest.mark.parametrize(
    ("published_at", "queued_seconds"),
    [
        ("2026-10-01T11:59:57.500000+00:00", 2.5),
        (b"2026-10-01T11:59:57.500000+00:00", 2.5),
        ("2026-10-01T13:59:58+02:00", 2.0),  # another time zone
        # The publisher's clock is ahead: shown as it is, not corrected
        ("2026-10-01T12:00:01.500000+00:00", -1.5),
        ("2026-10-01T11:59:58", None),  # no time zone
        ("yesterday", None),
        (42, None),
        (None, None),  # from another client, without the header
    ],
    ids=[
        "utc",
        "bytes",
        "other-zone",
        "publisher-ahead",
        "without-zone",
        "not-a-time",
        "number",
        "missing",
    ],
)
def test_handled_line_tells_how_long_the_delivery_waited_since_it_was_published(
    config, caplog, clock, published_at, queued_seconds
):
    caplog.set_level("INFO", logger="pykicak.executor")
    headers = {} if published_at is None else {"x-kicak-published-at": published_at}
    channel = FakeChannel()
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, headers=headers),
    )
    executor = make_executor(config, channel)

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    assert kicak_values(record)["queued_seconds"] == queued_seconds
    expected_text = "None" if queued_seconds is None else f"{queued_seconds:.3f}"
    assert record.message.endswith(f" queued_seconds={expected_text}")


def test_results_are_stamped_with_the_time_they_were_published(config, clock):
    def slow(_message):
        clock.advance(5)
        return SampleMessage(message_id="result", text="result")

    channel = FakeChannel()
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(
            type=SAMPLE_TYPE, headers={"x-kicak-published-at": "2026-09-30T08:00:00+00:00"}
        ),
    )
    executor = make_executor(config, channel, handler=slow, destination_exchange="output-exchange")

    run_until_stopped(executor)

    # When the executor published the result, not when its input was published or received
    [result] = channel.published_messages
    assert result.properties.headers["x-kicak-published-at"] == "2026-10-01T12:00:05+00:00"


def test_handled_line_has_no_input_size_for_a_delivery_without_body(config, caplog):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    channel.queue_deliveries(None)
    executor = make_executor(config, channel)

    run_until_stopped(executor)

    [record] = handled_records(caplog)
    assert kicak_values(record)["input_bytes"] is None
    assert " input_bytes=None " in record.message
