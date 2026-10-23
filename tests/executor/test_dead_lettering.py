"""Dead-lettering: malformed messages, bugs, and the copies' headers."""

import dataclasses

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
    MalformedMessageError,
)
from pykicak.messages import KicakMessage
from tests.executor.helpers import (
    TOPOLOGY,
    CopyRefusingChannel,
    ProcessingExecutor,
    SampleMessage,
    dead_letter_copies,
    dead_lettered,
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
    StopConsuming,
)


class InvalidTextError(Exception):
    """An application's own validation error: neither ValueError nor TypeError."""


@dataclasses.dataclass(frozen=True, slots=True)
class ValidatedMessage(KicakMessage):
    kicak_type = "validated.v1"
    text: str

    def __post_init__(self) -> None:
        KicakMessage.__post_init__(self)
        if self.text == "invalid":
            raise InvalidTextError("text must not be 'invalid'")


class UnprintableError(Exception):
    def __str__(self) -> str:
        raise RuntimeError("cannot describe this error")


def results_for_input_only(results):
    """execute() handler returning `results` for the message "input", and the message otherwise."""
    return lambda message: results if message.message_id == "input" else message


def test_run_dead_letters_undecodable_message_and_continues(config, caplog):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    valid = SampleMessage(message_id="valid", text="valid")
    channel.queue_deliveries(
        b"not json", properties=pika.BasicProperties(type=SAMPLE_TYPE, message_id="sent-id")
    )
    channel.queue_deliveries(valid.to_bytes())

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [valid]
    dead_letter_records = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert len(dead_letter_records) == 1
    assert dead_letter_records[0].levelname == "ERROR"
    # The body names no ID, so the sender's message_id property does
    assert "type=SampleMessage id=sent-id" in dead_letter_records[0].message
    assert "dead_letter_exchange=input-queue.dlx" in dead_letter_records[0].message
    assert "Could not decode message as SampleMessage" in dead_letter_records[0].message
    # Logged once, by the Executor: the serialization code logs it at DEBUG only
    assert not [
        r for r in caplog.records if r.name == "pykicak.messages" and r.levelname == "ERROR"
    ]


@pytest.mark.parametrize(
    ("body", "properties", "logged_id"),
    [
        (b"not json", pika.BasicProperties(type=SAMPLE_TYPE), "None"),
        (b"not json", None, "None"),
        (b"not json", pika.BasicProperties(type=SAMPLE_TYPE, message_id=""), "None"),
        (b"not json", pika.BasicProperties(type=SAMPLE_TYPE, message_id=b"bytes-id"), "bytes-id"),
        (None, pika.BasicProperties(type=SAMPLE_TYPE, message_id="no-body"), "no-body"),
    ],
    ids=["no-message-id", "no-properties", "empty-message-id", "bytes-message-id", "no-body"],
)
def test_undecodable_delivery_is_logged_with_the_sent_message_id(
    config, caplog, body, properties, logged_id
):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(body, properties=properties)

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    [record] = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert f"type=SampleMessage id={logged_id} queue=" in record.message


def test_decoded_delivery_is_logged_with_the_message_id_of_its_body(config, caplog):
    def refuse(message):
        raise MalformedMessageError("refused by the application")

    caplog.set_level("INFO", logger="pykicak.executor")
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=refuse)
    channel.queue_deliveries(
        SampleMessage(message_id="body-id", text="body").to_bytes(),
        properties=pika.BasicProperties(type=SAMPLE_TYPE, message_id="property-id"),
    )

    run_until_stopped(executor)

    assert "Message received type=SampleMessage id=body-id" in caplog.text
    assert "Message dead-lettered type=SampleMessage id=body-id" in caplog.text
    assert "property-id" not in caplog.text


def test_run_dead_letters_json_nested_too_deeply_and_continues(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    valid = SampleMessage(message_id="valid", text="valid")
    depth = 1_000_000  # beyond the JSON decoder's recursion limit
    channel.queue_deliveries(b"[" * depth + b"]" * depth, valid.to_bytes())

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [valid]


def test_run_dead_letters_message_its_class_refuses_with_any_exception_and_continues(
    config, caplog
):
    class ValidatingExecutor(ProcessingExecutor[ValidatedMessage, None]):
        pass

    channel = FakeChannel()
    executor = ValidatingExecutor(config, channel, **TOPOLOGY)
    valid = ValidatedMessage(message_id="valid", text="valid")
    channel.queue_deliveries(
        b'{"message_id": "bad", "text": "invalid"}',
        properties=pika.BasicProperties(type="validated.v1", message_id="bad"),
    )
    channel.queue_deliveries(valid.to_bytes(), properties=pika.BasicProperties(type="validated.v1"))

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [valid]
    assert "Message dead-lettered type=ValidatedMessage id=bad" in caplog.text
    assert "text must not be 'invalid'" in caplog.text


def test_dead_letter_copy_records_why_when_and_from_which_queue(config, clock):
    def reject(_message):
        raise MalformedMessageError("fails application validation")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=reject)
    received = pika.BasicProperties(
        type=SAMPLE_TYPE,
        content_type="application/json",
        correlation_id="flow-7",
        message_id="property-id",
        user_id="publisher",
        expiration="5000",
        headers={
            "tenant": "acme",
            "x-death": [{"queue": "input-queue.retry", "reason": "expired", "count": 1}],
            "x-delivery-count": 3,
            "x-first-death-queue": "input-queue.retry",
            "x-kicak-retry-round": 2,
            "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
            "x-kicak-published-at": "2026-09-30T08:30:00+00:00",
        },
    )
    body = SampleMessage(message_id="body-id", text="input").to_bytes()
    channel.queue_deliveries(body, properties=received)

    run_until_stopped(executor)

    [copy] = dead_letter_copies(channel)
    assert copy.routing_key == ""
    assert copy.mandatory is True
    assert copy.body == body
    properties = copy.properties
    assert properties.headers == {
        "tenant": "acme",
        # Its history: how often it was moved to the retry queue before, and when it was
        # published to the source queue last
        "x-kicak-retry-round": 2,
        "x-kicak-retrying-since": "2026-09-30T08:00:00+00:00",
        "x-kicak-published-at": "2026-09-30T08:30:00+00:00",
        "x-kicak-dead-letter-reason": "malformed",
        "x-kicak-error-type": "pykicak.executor.MalformedMessageError",
        "x-kicak-error-message": "fails application validation",
        "x-kicak-dead-lettered-at": "2026-10-01T12:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
        # The original user_id, which RabbitMQ would refuse from this executor's user
        "x-kicak-original-publisher": "publisher",
    }
    # The message_id property as the delivery had it, so the copy is found by the same ID
    assert properties.message_id == "property-id"
    assert properties.user_id == "guest"  # the executor's own user, from CONNECTION_VALUES
    assert properties.expiration is None  # it must not expire in the dead-letter queue
    assert properties.delivery_mode == 2
    assert properties.type == SAMPLE_TYPE
    assert properties.content_type == "application/json"
    assert properties.correlation_id == "flow-7"


@pytest.mark.parametrize(
    ("body", "handler", "reason", "error_type", "error_message"),
    [
        (
            b"not json",
            None,
            "malformed",
            "pykicak.executor.MalformedMessageError",
            "Could not decode message as SampleMessage: ",
        ),
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            lambda _message: 1 / 0,
            "unexpected-error",
            "ZeroDivisionError",
            "division by zero",
        ),
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            raise_error(InvalidTextError("text is invalid")),
            "unexpected-error",
            "tests.executor.test_dead_lettering.InvalidTextError",
            "text is invalid",
        ),
        (
            SampleMessage(message_id="input", text="input").to_bytes(),
            raise_error(UnprintableError()),
            "unexpected-error",
            "tests.executor.test_dead_lettering.UnprintableError",
            "<exception str() failed>",
        ),
    ],
    ids=["malformed", "builtin-error", "application-error", "unprintable-error"],
)
def test_dead_letter_copy_names_the_reason_and_the_error(
    config, body, handler, reason, error_type, error_message
):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=handler)
    channel.queue_deliveries(body)

    with pytest.raises((StopConsuming, ZeroDivisionError, InvalidTextError, UnprintableError)):
        executor.run()

    assert dead_lettered(channel) == [1]
    headers = dead_letter_copies(channel)[0].properties.headers
    assert headers["x-kicak-dead-letter-reason"] == reason
    assert headers["x-kicak-error-type"] == error_type
    assert headers["x-kicak-error-message"].startswith(error_message)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("q" * 1024, "q" * 1024),
        ("q" * 1025, "q" * 1021 + "…"),
        # 2000 bytes, cut between the two bytes of a character, which is dropped
        ("ł" * 1000, "ł" * 510 + "…"),
        # A lone surrogate, e.g. from the JSON string "\ud800", cannot be encoded in UTF-8
        ("text \ud800 here", "text ? here"),
    ],
    ids=["fits", "too-long", "too-long-multibyte", "lone-surrogate"],
)
def test_dead_letter_copy_carries_at_most_1024_bytes_of_utf8_error_text(config, text, expected):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=raise_error(MalformedMessageError(text)))
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    [copy] = dead_letter_copies(channel)
    assert copy.properties.headers["x-kicak-error-message"] == expected
    assert len(expected.encode("utf-8")) <= 1024
    copy.properties.encode()  # pika can send it


def test_dead_letter_copy_of_a_delivery_without_body_or_message_id(config, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(None)

    run_until_stopped(executor)

    [copy] = dead_letter_copies(channel)
    assert copy.body == b""
    assert copy.properties.message_id is None
    assert copy.properties.headers == {
        "x-kicak-dead-letter-reason": "malformed",
        "x-kicak-error-type": "pykicak.executor.MalformedMessageError",
        "x-kicak-error-message": "Received a message with no body",
        "x-kicak-dead-lettered-at": "2026-10-01T12:00:00+00:00",
        "x-kicak-dead-lettered-from": "input-queue",
    }


def test_retry_copy_of_a_replayed_message_drops_its_dead_letter_headers(config, sleeps, clock):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=unavailable)
    replayed = pika.BasicProperties(
        type=SAMPLE_TYPE,
        headers={
            "tenant": "acme",
            # Its rounds before it was dead-lettered, which start again
            "x-kicak-retry-round": 7,
            "x-kicak-retrying-since": "2026-09-29T08:00:00+00:00",
            "x-kicak-dead-letter-reason": "unexpected-error",
            "x-kicak-error-type": "KeyError",
            "x-kicak-error-message": "'text'",
            "x-kicak-dead-lettered-at": "2026-09-30T08:00:00+00:00",
            "x-kicak-dead-lettered-from": "input-queue",
        },
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), properties=replayed
    )

    run_until_stopped(executor)

    assert retry_copies(channel)[0].properties.headers == {
        "tenant": "acme",
        "x-kicak-retry-round": 1,
        "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
        "x-kicak-published-at": "2026-10-01T12:00:00+00:00",
    }


@pytest.mark.parametrize(
    "error",
    [UnroutableError([]), NackError([])],
    ids=["unroutable", "refused"],
)
def test_run_requeues_the_message_when_rabbitmq_does_not_take_its_dead_letter_copy(
    config, caplog, error
):
    channel = CopyRefusingChannel(error, target="input-queue.dlx")
    executor = make_executor(config, channel)
    channel.queue_deliveries(b"not json")

    run_until_stopped(executor)  # continues with the next message

    # Never rejected without requeueing, which RabbitMQ may drop when no queue takes it
    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert channel.acked_delivery_tags == []
    assert executor.get_info().counts == ExecutorCounts(requeued=1)
    errors = [r for r in caplog.records if "could not be dead-lettered to" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]


def test_run_requeues_and_raises_when_rabbitmq_does_not_take_a_dead_letter_copy_after_a_bug(
    config,
):
    channel = CopyRefusingChannel(NackError([]), target="input-queue.dlx")
    executor = make_executor(config, channel, handler=lambda _message: 1 / 0)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert channel.nacked_deliveries == [NackedDelivery(delivery_tag=1, requeue=True)]
    assert executor.get_info().status == ExecutorStatus.CRASHED


def test_run_reconnects_when_the_connection_is_lost_while_publishing_a_dead_letter_copy(
    config, monkeypatch, sleeps, caplog
):
    original_channel = CopyRefusingChannel(
        StreamLostError("connection lost"), target="input-queue.dlx"
    )
    replacement_channel = FakeChannel()
    original_channel.queue_deliveries(b"not json")
    replacement_channel.queue_deliveries(b"not json")  # RabbitMQ's redelivery
    executor = make_executor(config, original_channel)
    channels = [original_channel, replacement_channel]
    monkeypatch.setattr(executor, "_create_connection", lambda: FakeConnection(channels.pop(0)))

    run_until_stopped(executor)

    assert original_channel.acked_delivery_tags == []
    assert original_channel.nacked_deliveries == []
    assert dead_lettered(replacement_channel) == [1]
    warnings = [
        r for r in caplog.records if "Connection or channel lost while dead-lettering" in r.message
    ]
    assert [record.levelname for record in warnings] == ["WARNING"]


def test_run_stops_when_rabbitmq_refuses_a_dead_letter_copy(config, caplog):
    channel = CopyRefusingChannel(
        ChannelClosedByBroker(403, "ACCESS_REFUSED"), target="input-queue.dlx"
    )
    executor = make_executor(config, channel)
    channel.queue_deliveries(b"not json")

    with pytest.raises(ChannelClosedByBroker):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues it
    errors = [r for r in caplog.records if "Dead-lettering message id=None" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]
    assert "stopping the executor" in errors[0].message


def test_run_raises_the_processing_error_when_its_dead_letter_copy_is_refused(config, caplog):
    channel = CopyRefusingChannel(
        ChannelClosedByBroker(403, "ACCESS_REFUSED"), target="input-queue.dlx"
    )
    executor = make_executor(config, channel, handler=lambda _message: 1 / 0)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ZeroDivisionError):  # not masked by the refusal
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert channel.acked_delivery_tags == []
    assert channel.nacked_deliveries == []  # closing the connection requeues it
    assert "could not be dead-lettered after the unexpected error" in caplog.text


def test_run_dead_letters_and_raises_on_unexpected_processing_error(config, caplog):
    def broken(_message):
        raise ZeroDivisionError("bug in application code")

    channel = FakeChannel()
    executor = make_executor(config, channel, handler=broken)
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        SampleMessage(message_id="never-processed", text="never processed").to_bytes(),
    )

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert dead_lettered(channel) == [1]
    assert len(executor.processed_messages) == 1
    assert channel.is_open is False
    assert executor.get_info().counts == ExecutorCounts(
        dead_lettered=DeadLetterCounts(unexpected_error=1)
    )
    assert "failed unexpectedly type=SampleMessage id=input" in caplog.text
    assert "stopping the executor" in caplog.text


def test_run_raises_the_processing_error_when_closing_the_connection_also_fails(config, caplog):
    class FailingCloseConnection(FakeConnection):
        def close(self) -> None:
            raise StreamLostError("connection close failed")

    def broken(_message):
        raise ZeroDivisionError("bug in application code")

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=broken, connection_class=FailingCloseConnection
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(ZeroDivisionError):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert "Failed to close RabbitMQ connection" in caplog.text


@pytest.mark.parametrize(
    "handler",
    [lambda message: message, lambda _message: [{"text": "not a message"}]],
    ids=["message", "not-a-message"],
)
def test_run_dead_letters_and_raises_when_terminator_returns_results(config, handler):
    channel = FakeChannel()
    executor = make_executor(config, channel, handler=handler)
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    with pytest.raises(RuntimeError, match="no destination_exchange"):
        executor.run()

    assert dead_lettered(channel) == [1]
    assert channel.published_messages == dead_letter_copies(channel)


@pytest.mark.parametrize(
    ("results", "reason"),
    [
        (
            [
                SampleMessage(message_id="ok", text="ok"),
                SampleMessage(message_id="unserializable", text=object()),  # type: ignore[arg-type]
            ],
            "cannot be serialized, type=SampleMessage",
        ),
        (
            [SampleMessage(message_id="nan", text=float("nan"))],  # type: ignore[arg-type]
            "cannot be serialized, type=SampleMessage",
        ),
        ([{"text": "not a message"}], "must return KicakMessage instances"),
        ("not a message", "must return KicakMessage instances"),
        (42, "not iterable"),
    ],
    ids=["unserializable", "nan", "list-of-dicts", "string", "number"],
)
def test_run_dead_letters_input_whose_results_are_invalid_and_continues(
    config, caplog, results, reason
):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=results_for_input_only(results),
        destination_exchange="output-exchange",
    )
    following = SampleMessage(message_id="following", text="following")
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(), following.to_bytes()
    )

    run_until_stopped(executor)

    # Nothing of the invalid results is published; the next message is processed as usual
    assert [p.exchange for p in channel.published_messages] == [
        "input-queue.dlx",
        "output-exchange",
    ]
    assert channel.published_messages[1].body == following.to_bytes()
    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    dead_letter_records = [
        r
        for r in caplog.records
        if "Message dead-lettered type=SampleMessage id=input" in r.message
    ]
    assert [record.levelname for record in dead_letter_records] == ["ERROR"]
    assert reason in dead_letter_records[0].message
    # Logged once, by the Executor: the serialization code logs it at DEBUG only
    assert not [
        r for r in caplog.records if r.name == "pykicak.messages" and r.levelname == "ERROR"
    ]
