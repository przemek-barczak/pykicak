"""Transient failures: retried, moved to the retry queue, then limited."""

from typing import Any

import pika
import pika.channel
import pytest
from pika.exceptions import (
    ChannelClosedByBroker,
    NackError,
    StreamLostError,
    UnroutableError,
)

from pykicak.executor import (
    DeadLetterCounts,
    ExecutorCounts,
    ExecutorStatus,
    KicakExecutorAbstract,
    TransientProcessingError,
)
from tests.executor.helpers import (
    CopyRefusingChannel,
    ProcessingExecutor,
    SampleMessage,
    dead_letter_copies,
    dead_lettered,
    handled_records,
    make_executor,
    raise_error,
    retry_copies,
    run_until_stopped,
    unavailable,
)
from tests.fakes import (
    SAMPLE_TYPE,
    FakeChannel,
    FakeConnection,
    NackedDelivery,
    kicak_values,
)


def in_retry_round(retry_round: int, **headers: object) -> pika.BasicProperties:
    """Properties of a delivery back from the retry queue after `retry_round` moves."""
    return pika.BasicProperties(
        type=SAMPLE_TYPE,
        headers={
            "x-kicak-retry-round": retry_round,
            "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
            **headers,
        },
    )


def dead_letter_headers(reason: str = "retries-exhausted") -> dict[str, object]:
    """The headers a dead-letter copy carries, as a message sent back from the `.dlq` keeps them."""
    return {
        "x-kicak-dead-letter-reason": reason,
        "x-kicak-error-type": "pykicak.executor.TransientProcessingError",
        "x-kicak-error-message": "service unavailable",
        "x-kicak-dead-lettered-at": "2026-09-30T20:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
    }


def test_run_retries_transient_failure_then_publishes_and_acknowledges(config, sleeps, caplog):
    attempts = 0
    result = SampleMessage(message_id="result", text="result")

    def flaky(_message):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TransientProcessingError("service unavailable")
        return result

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=flaky, destination_exchange="output-exchange")
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert attempts == 2
    assert sleeps == [0.5]
    assert channel.operations == ["publish output-exchange", "commit", "ack 1"]
    assert "Transient processing failure id=input, retrying attempt 2/3" in caplog.text


def test_run_moves_message_to_retry_queue_after_exhausting_transient_retries(
    config, sleeps, clock, caplog
):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=unavailable, destination_exchange="output-exchange"
    )
    first = SampleMessage(message_id="input", text="input")
    later = SampleMessage(message_id="later", text="later")
    channel.queue_deliveries(first.to_bytes(), later.to_bytes())

    run_until_stopped(executor)

    assert sleeps == [0.5, 1.0, 0.5, 1.0]
    assert len(executor.processed_messages) == 6
    copies = retry_copies(channel)
    assert [copy.body for copy in copies] == [first.to_bytes(), later.to_bytes()]
    assert all(copy.mandatory for copy in copies)
    properties = copies[0].properties
    assert properties.expiration == "10000"  # retry_delay_seconds, in milliseconds
    assert properties.delivery_mode == 2
    assert properties.message_id == "input"
    assert properties.headers == {
        "x-kicak-retry-round": 1,
        "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
        "x-kicak-published-at": "2026-10-01T12:00:00+00:00",
    }
    # Acknowledged after the copy, never requeued or dead-lettered
    assert channel.operations == ["publish ", "ack 1", "publish ", "ack 2"]
    assert channel.nacked_deliveries == []
    moved = [r for r in caplog.records if "Message moved to retry queue" in r.message]
    assert [record.levelname for record in moved] == ["WARNING", "WARNING"]
    assert (
        "id=input queue=input-queue retry_queue=input-queue.retry retry_round=1 "
        "max_retry_rounds=10 retrying_since=2026-10-01T12:00:00+00:00 retry_in=10.0s: "
        "service unavailable"
    ) in moved[0].message


def test_run_moves_to_retry_queue_without_retrying_when_one_attempt_is_allowed(config, sleeps):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable, max_processing_attempts=1)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert sleeps == []
    assert len(retry_copies(channel)) == 1
    assert channel.acked_delivery_tags == [1]


@pytest.mark.parametrize(("retry_delay_seconds", "expiration"), [(2.5, "2500"), (0, "0")])
def test_retry_copy_expires_after_the_retry_delay(config, sleeps, retry_delay_seconds, expiration):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=unavailable, retry_delay_seconds=retry_delay_seconds
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert retry_copies(channel)[0].properties.expiration == expiration


def test_retry_copy_keeps_application_properties_but_not_rabbitmq_headers(config, sleeps, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    received = pika.BasicProperties(
        type=SAMPLE_TYPE,
        content_type="application/json",
        correlation_id="flow-7",
        message_id="input",
        user_id="publisher",
        expiration="5000",
        headers={
            "tenant": "acme",
            "x-death": [{"queue": "input-queue", "reason": "rejected", "count": 1}],
            "x-delivery-count": 3,
            "x-first-death-queue": "input-queue",
            "x-last-death-reason": "rejected",
        },
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=received
    )

    run_until_stopped(executor)

    copy = retry_copies(channel)[0].properties
    assert copy.content_type == "application/json"
    assert copy.type == SAMPLE_TYPE
    assert copy.correlation_id == "flow-7"
    assert copy.headers == {
        "tenant": "acme",
        "x-kicak-retry-round": 1,
        "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
        "x-kicak-published-at": "2026-10-01T12:00:00+00:00",
        # The original user_id, which RabbitMQ would refuse from this executor's user
        "x-kicak-original-publisher": "publisher",
    }
    assert copy.user_id == "guest"  # the executor's own user, from CONNECTION_VALUES
    assert copy.expiration == "10000"


def test_retry_round_increases_and_keeps_the_time_of_the_first_round(config, sleeps, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    received = pika.BasicProperties(
        type=SAMPLE_TYPE,
        headers={
            "x-kicak-retry-round": 3,
            "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
            "x-kicak-published-at": "2026-09-30T08:30:00+00:00",
        },
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=received
    )

    run_until_stopped(executor)

    assert retry_copies(channel)[0].properties.headers == {
        "x-kicak-retry-round": 4,
        "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
        # Published anew: the time it waits in the source queue includes the retry delay
        "x-kicak-published-at": "2026-10-01T12:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"x-kicak-retry-round": "3"}, 1),
        ({"x-kicak-retry-round": -1}, 1),
        ({"x-kicak-retry-round": True}, 1),
        ({"x-kicak-retrying-since": "yesterday"}, "2026-10-01T12:00:00+00:00"),
        ({"x-kicak-retrying-since": "2026-09-30T08:00:00"}, "2026-10-01T12:00:00+00:00"),
        ({"x-kicak-retrying-since": 42}, "2026-10-01T12:00:00+00:00"),
        ({"x-kicak-retrying-since": b"2026-09-30T08:00:00+00:00"}, "2026-09-30T08:00:00+00:00"),
    ],
    ids=[
        "round-text",
        "round-negative",
        "round-bool",
        "since-not-a-time",
        "since-without-zone",
        "since-number",
        "since-bytes",
    ],
)
def test_invalid_retry_headers_start_again(config, sleeps, clock, headers, expected):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, headers=headers),
    )

    run_until_stopped(executor)

    copy_headers = retry_copies(channel)[0].properties.headers
    name = next(iter(headers))
    assert copy_headers[name] == expected


@pytest.mark.parametrize(
    ("max_retry_rounds", "retry_round", "moved"),
    [(2, 0, True), (2, 1, True), (2, 2, False), (2, 5, False), (0, 0, False)],
    ids=["first-failure", "below-the-limit", "at-the-limit", "above-the-limit", "no-rounds"],
)
def test_transient_failure_moves_a_message_until_its_retry_rounds_are_used_up(
    config, sleeps, max_retry_rounds, retry_round, moved
):
    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=unavailable, max_retry_rounds=max_retry_rounds
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=in_retry_round(retry_round),
    )

    run_until_stopped(executor)

    # Every round, the last one included, calls execute() max_processing_attempts times
    assert len(executor.processed_messages) == 3
    assert sleeps == [0.5, 1.0]
    assert len(retry_copies(channel)) == (1 if moved else 0)
    assert dead_lettered(channel) == ([] if moved else [1])
    assert channel.acked_delivery_tags == [1]
    assert channel.nacked_deliveries == []


def test_message_that_used_up_its_retry_rounds_is_dead_lettered_saying_why(
    config, sleeps, clock, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable, max_retry_rounds=2)
    body = SampleMessage(message_id="input", text="input").to_bytes()
    later = SampleMessage(message_id="later", text="later")
    channel.queue_deliveries(body, properties=in_retry_round(2, tenant="acme"))
    channel.queue_deliveries(later.to_bytes())

    run_until_stopped(executor)

    [copy] = dead_letter_copies(channel)
    assert copy.body == body
    assert copy.properties.expiration is None
    assert copy.properties.headers == {
        "tenant": "acme",
        # Its history: all the rounds it had
        "x-kicak-retry-round": 2,
        "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
        "x-kicak-dead-letter-reason": "retries-exhausted",
        # The failure of its last attempt
        "x-kicak-error-type": "pykicak.executor.TransientProcessingError",
        "x-kicak-error-message": "service unavailable",
        "x-kicak-dead-lettered-at": "2026-10-01T12:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
    }
    # The executor continues: the next message is executed, and moved to the retry queue
    assert executor.processed_messages[-1] == later
    assert [copy.properties.message_id for copy in retry_copies(channel)] == ["later"]
    assert executor.get_info().counts == ExecutorCounts(
        dead_lettered=DeadLetterCounts(retries_exhausted=1), moved_to_retry=1
    )
    dead_letter_records = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert [record.levelname for record in dead_letter_records] == ["ERROR"]
    assert dead_letter_records[0].message == (
        "Message dead-lettered type=SampleMessage id=input queue=input-queue delivery_tag=1 "
        "dead_letter_exchange=input-queue.dlx retry_round=2 "
        "retrying_since=2026-09-30T08:00:00+00:00: still failing after 3 processing attempts, "
        "and max_retry_rounds=2 allows no more moves to the retry queue: service unavailable"
    )
    handled = kicak_values(handled_records(caplog)[0])
    assert (handled["outcome"], handled["reason"], handled["retry_round"]) == (
        "dead_lettered",
        "retries-exhausted",
        2,
    )


def test_application_subclass_of_the_transient_error_is_named_in_the_dead_letter_copy(
    config, sleeps
):
    class ServiceUnavailableError(TransientProcessingError):
        pass

    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=raise_error(ServiceUnavailableError("summarizer down")),
        max_retry_rounds=0,
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    headers = dead_letter_copies(channel)[0].properties.headers
    assert headers["x-kicak-dead-letter-reason"] == "retries-exhausted"
    assert headers["x-kicak-error-type"] == (
        "tests.executor.test_retry_queue."
        "test_application_subclass_of_the_transient_error_is_named_in_the_dead_letter_copy."
        "<locals>.ServiceUnavailableError"
    )
    assert headers["x-kicak-error-message"] == "summarizer down"


def test_no_retry_rounds_dead_letters_a_message_after_its_first_attempts(config, sleeps, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable, max_retry_rounds=0)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    assert len(executor.processed_messages) == 3
    assert retry_copies(channel) == []
    [copy] = dead_letter_copies(channel)
    # Never moved, so it has no retry headers
    assert copy.properties.headers == {
        "x-kicak-dead-letter-reason": "retries-exhausted",
        "x-kicak-error-type": "pykicak.executor.TransientProcessingError",
        "x-kicak-error-message": "service unavailable",
        "x-kicak-dead-lettered-at": "2026-10-01T12:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
    }


def test_message_sent_back_from_the_dead_letter_queue_gets_its_retry_rounds_again(
    config, sleeps, clock, caplog
):
    caplog.set_level("INFO", logger="pykicak.executor")
    reported: list[int | None] = []
    executor_holder: list[KicakExecutorAbstract[Any, Any]] = []

    def unavailable_reporting(_message):
        reported.append(executor_holder[0].get_info().retry_round)
        raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable_reporting, max_retry_rounds=2)
    executor_holder.append(executor)
    # Dead-lettered after its last round, then sent back by an operator
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=in_retry_round(2, **dead_letter_headers()),
    )

    run_until_stopped(executor)

    assert dead_letter_copies(channel) == []
    [copy] = retry_copies(channel)
    assert copy.properties.headers == {
        "x-kicak-retry-round": 1,
        "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
        "x-kicak-published-at": "2026-10-01T12:00:00+00:00",
    }
    assert reported == [0, 0, 0]
    assert kicak_values(handled_records(caplog)[0])["retry_round"] == 0


@pytest.mark.parametrize("reason", ["retries-exhausted", "malformed", "unexpected-error"])
def test_dead_letter_copy_of_a_message_sent_back_carries_no_stale_rounds(
    config, sleeps, clock, caplog, reason
):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable, max_retry_rounds=0)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        properties=in_retry_round(4, **dead_letter_headers(reason)),
    )

    run_until_stopped(executor)

    # Its rounds started again, and it had none before it was dead-lettered again
    [copy] = dead_letter_copies(channel)
    assert copy.properties.headers == {
        "x-kicak-dead-letter-reason": "retries-exhausted",
        "x-kicak-error-type": "pykicak.executor.TransientProcessingError",
        "x-kicak-error-message": "service unavailable",
        "x-kicak-dead-lettered-at": "2026-10-01T12:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
    }
    [record] = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert "retry_round=0 retrying_since=None:" in record.message


def test_message_that_used_up_its_retry_rounds_is_requeued_if_its_copy_is_refused(config, sleeps):
    channel = CopyRefusingChannel(NackError([]), target="input-queue.dlx")
    executor = make_executor(config, channel, handler=unavailable, max_retry_rounds=0)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    # Never acknowledged without its copy, and never moved to the retry queue instead
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert retry_copies(channel) == []
    assert executor.get_info().counts == ExecutorCounts(requeued=1)


@pytest.mark.parametrize(
    "error",
    [UnroutableError([]), NackError([])],
    ids=["unroutable", "refused"],
)
def test_run_requeues_the_message_when_rabbitmq_does_not_take_its_retry_copy(
    config, sleeps, caplog, error
):
    channel = CopyRefusingChannel(error)
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)  # continues with the next message

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert channel.acked_delivery_tags == []
    assert executor.get_info().counts == ExecutorCounts(requeued=1)
    errors = [r for r in caplog.records if "could not be moved to retry queue" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]


def test_run_reconnects_when_the_connection_is_lost_while_moving_to_the_retry_queue(
    config, monkeypatch, sleeps
):
    body = SampleMessage(message_id="input", text="input").to_bytes()
    original_channel = CopyRefusingChannel(StreamLostError("connection lost"))
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel, handler=unavailable)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    assert len(retry_copies(replacement_channel)) == 1
    assert replacement_channel.acked_delivery_tags == [1]


def test_run_moves_no_copy_to_the_retry_queue_when_rabbitmq_cancelled_the_consumer(
    config, monkeypatch, sleeps, caplog
):
    # Transient retries outlasted a quorum queue's consumer timeout on RabbitMQ 4.3+, which has
    # returned the message to the queue: a copy in the retry queue would execute it twice
    body = SampleMessage(message_id="input", text="input").to_bytes()
    original_channel = FakeChannel()
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(body)
    replacement_channel.queue_deliveries(body)  # RabbitMQ's redelivery

    cancelled = False

    def unavailable_beyond_the_consumer_timeout(message):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            original_channel.cancel_consumer()
        unavailable(message)

    executor = make_executor(
        config, original_channel, handler=unavailable_beyond_the_consumer_timeout
    )
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert retry_copies(original_channel) == []
    assert original_channel.acked_delivery_tags == []
    assert len(retry_copies(replacement_channel)) == 1
    assert replacement_channel.acked_delivery_tags == [1]
    assert "Connection or channel lost while moving message id=input" in caplog.text


def test_run_stops_when_rabbitmq_refuses_to_declare_the_retry_queue(config, sleeps):
    class RefusingRetryQueueChannel(FakeChannel):
        """Refuses the retry queue after the declarations of the first connection."""

        def queue_declare(
            self, queue, durable=False, exclusive=False, auto_delete=False, arguments=None
        ):
            if (
                queue == "input-queue.retry"
                and self.acked_delivery_tags == []
                and any(q.queue == "input-queue.retry" for q in self.declared_queues)
            ):
                self.is_open = False
                raise ChannelClosedByBroker(406, "PRECONDITION_FAILED - inequivalent arg")
            super().queue_declare(queue, durable, exclusive, auto_delete, arguments)

    channel = RefusingRetryQueueChannel()
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues it


def test_stop_during_transient_retries_still_moves_the_message_to_the_retry_queue(config, sleeps):
    executor_holder: list[ProcessingExecutor[Any, Any]] = []

    def stop_then_fail(_message):
        executor_holder[0].stop()
        raise TransientProcessingError("service unavailable")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=stop_then_fail)
    executor_holder.append(executor)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    executor.run()

    assert len(executor.processed_messages) == 3  # stop() does not interrupt the retries
    assert len(retry_copies(channel)) == 1
    assert channel.acked_delivery_tags == [1]
    assert executor.get_info().status == ExecutorStatus.STOPPED


@pytest.mark.parametrize(
    ("received", "original_publisher"),
    [
        # Created by an injector, delivered here by an upstream executor's retry copy
        (
            pika.BasicProperties(
                type=SAMPLE_TYPE,
                user_id="upstream-executor",
                headers={"x-kicak-original-publisher": "injector-user"},
            ),
            "injector-user",
        ),
        # Created by another client, which set no header
        (pika.BasicProperties(type=SAMPLE_TYPE, user_id="alice"), "alice"),
        (
            pika.BasicProperties(
                type=SAMPLE_TYPE, headers={"x-kicak-original-publisher": b"injector-user"}
            ),
            "injector-user",
        ),
        # An invalid header is ignored
        (
            pika.BasicProperties(
                type=SAMPLE_TYPE, user_id="alice", headers={"x-kicak-original-publisher": 42}
            ),
            "alice",
        ),
        (pika.BasicProperties(type=SAMPLE_TYPE, headers={"x-kicak-original-publisher": ""}), None),
        (pika.BasicProperties(type=SAMPLE_TYPE), None),
    ],
    ids=["header", "user-id", "header-bytes", "invalid-header", "empty-header", "unknown"],
)
def test_retry_copy_carries_the_original_publisher(config, sleeps, received, original_publisher):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=received
    )

    run_until_stopped(executor)

    copy = retry_copies(channel)[0].properties
    assert copy.user_id == "guest"
    assert copy.headers.get("x-kicak-original-publisher") == original_publisher
