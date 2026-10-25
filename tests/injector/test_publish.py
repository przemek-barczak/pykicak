"""`publish()`: publishing on an open connection, its retries, and `InjectionError`."""

import dataclasses
import pickle
from datetime import UTC, datetime

import pytest
from pika.exceptions import (
    AMQPConnectionError,
    NackError,
    ProbableAuthenticationError,
    StreamLostError,
    UnroutableError,
)

from pykicak.injector import InjectionError
from pykicak.messages import KicakMessage
from tests.fakes import (
    FakeChannel,
    FakeConnection,
)
from tests.injector.helpers import (
    CountingInjector,
    OtherMessage,
    SampleMessage,
)


@dataclasses.dataclass(frozen=True, slots=True)
class IdentifiedMessage(KicakMessage):
    secret_payload: str


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


def test_injection_error_repr_shows_the_message_by_type_and_id_never_its_payload():
    # Exception's own repr would show its args, and the message with them, e.g. in a log
    # formatted with %r or in a crash report
    error = InjectionError(
        IdentifiedMessage(message_id="m-1", secret_payload="do not log me"), "my-exchange"
    )
    expected = (
        "InjectionError(message=<IdentifiedMessage message_id='m-1'>, exchange_name='my-exchange')"
    )

    assert repr(error) == expected
    assert repr(pickle.loads(pickle.dumps(error))) == expected
    assert "do not log me" not in repr(error)


def test_injection_error_keeps_message_and_exchange_and_survives_pickling():
    message = IdentifiedMessage(message_id="m-1", secret_payload="payload")
    error = InjectionError(message, "my-exchange")

    restored = pickle.loads(pickle.dumps(error))

    assert (restored.message, restored.exchange_name) == (message, "my-exchange")
    # Pickling keeps no __cause__, so the text ends without one
    assert str(restored) == (
        "IdentifiedMessage message id=m-1 was not injected into exchange 'my-exchange'"
    )


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
