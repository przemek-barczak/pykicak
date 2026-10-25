"""Unit tests of the example tool examples/replay_dead_letters.py, against a fake broker.

The tool is an example, not part of pykicak, so coverage does not measure it; the integration test
`test_dead_letters_sent_back_by_the_example_tool_get_their_retry_rounds_again` runs it against a
real broker.
"""

import argparse
import dataclasses
import ssl
import types
from collections import Counter
from typing import Any

import pika
import pytest
from pika.exceptions import ChannelClosedByBroker, StreamLostError, UnroutableError

import examples.replay_dead_letters as tool
from pykicak import KicakConfig, KicakConfigError
from tests.fakes import CONNECTION_VALUES


@dataclasses.dataclass
class DeadLetter:
    """A message waiting in the dead-letter queue."""

    message_id: str
    reason: str | None  # its x-kicak-dead-letter-reason, or None for one RabbitMQ dead-lettered
    delivery_tag: int = 0


class FakeReplayChannel:
    """Serves `dead_letters` from `<queue>.dlq` with basic_get, and records what the tool does."""

    def __init__(self, queue: str, dead_letters: list[DeadLetter]) -> None:
        self.queue = queue
        self.waiting = [
            dataclasses.replace(letter, delivery_tag=tag)
            for tag, letter in enumerate(dead_letters, start=1)
        ]
        self.confirming = False
        self.published: list[tuple[str, str, bytes, pika.BasicProperties, bool]] = []
        self.acked: list[int] = []
        self.publish_error: Exception | None = None
        self.taken_meanwhile = 0  # counted as waiting, but taken by someone else before the tool
        self.connection: FakeReplayConnection | None = None

    def confirm_delivery(self) -> None:
        self.confirming = True

    def queue_declare(self, queue: str, passive: bool = False) -> Any:
        assert passive, "the tool must never create a queue"
        count = len(self.waiting) + self.taken_meanwhile if queue == f"{self.queue}.dlq" else 0
        return types.SimpleNamespace(method=types.SimpleNamespace(message_count=count))

    def basic_get(self, queue: str, auto_ack: bool = False) -> tuple[Any, Any, Any]:
        assert (queue, auto_ack) == (f"{self.queue}.dlq", False)
        if not self.waiting:
            return None, None, None
        letter = self.waiting.pop(0)
        headers: dict[str, object] = {"x-death": [{"reason": "rejected"}], "tenant": "acme"}
        if letter.reason is not None:
            headers["x-kicak-dead-letter-reason"] = letter.reason
        properties = pika.BasicProperties(
            message_id=letter.message_id, type="sample.v1", user_id="executor", headers=headers
        )
        method = types.SimpleNamespace(delivery_tag=letter.delivery_tag)
        return method, properties, f"body of {letter.message_id}".encode()

    def basic_publish(self, exchange, routing_key, body, properties=None, mandatory=False) -> None:
        assert self.confirming, "a copy must be confirmed before the original is removed"
        if isinstance(self.publish_error, StreamLostError) and self.connection is not None:
            self.connection.is_open = False
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((exchange, routing_key, body, properties, mandatory))

    def basic_ack(self, delivery_tag: int) -> None:
        self.acked.append(delivery_tag)


class FakeReplayConnection:
    def __init__(self, channel: FakeReplayChannel) -> None:
        self._channel = channel
        channel.connection = self
        self.is_open = True

    def channel(self) -> FakeReplayChannel:
        return self._channel

    def close(self) -> None:
        # pika raises ConnectionWrongStateError for a connection that is already closed
        assert self.is_open, "closing a closed connection"
        self.is_open = False


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values=CONNECTION_VALUES)


def broker(monkeypatch, dead_letters: list[DeadLetter]) -> tuple[FakeReplayChannel, list[Any]]:
    """Make the tool connect to a fake broker; return its channel and the connections opened."""
    channel = FakeReplayChannel("classifier", dead_letters)
    connections: list[Any] = []

    def connect(_config, _queue):
        connection = FakeReplayConnection(channel)
        connections.append(connection)
        return connection

    monkeypatch.setattr(tool, "_connect", connect)
    return channel, connections


WAITING = [
    DeadLetter("outage-1", "retries-exhausted"),
    DeadLetter("invalid", "malformed"),
    DeadLetter("outage-2", "retries-exhausted"),
    DeadLetter("limit", None),
]


def test_sends_back_only_the_chosen_reasons_and_leaves_the_others(config, monkeypatch):
    channel, connections = broker(monkeypatch, WAITING)

    report = tool.replay_dead_letters(config, "classifier", {"retries-exhausted"})

    assert report.sent_back == {"retries-exhausted": 2}
    assert report.left == {"malformed": 1, "rabbitmq": 1}
    # To the queue itself through the default exchange, mandatory; each removed after its copy
    assert [(p[0], p[1], p[2], p[4]) for p in channel.published] == [
        ("", "classifier", b"body of outage-1", True),
        ("", "classifier", b"body of outage-2", True),
    ]
    assert channel.acked == [1, 3]
    # The others stay unacknowledged until the connection is closed, which returns them
    assert connections[0].is_open is False


def test_rabbitmq_selects_the_messages_rabbitmq_dead_lettered_itself(config, monkeypatch):
    channel, _connections = broker(monkeypatch, WAITING)

    report = tool.replay_dead_letters(config, "classifier", {"rabbitmq"})

    assert report.sent_back == {"rabbitmq": 1}
    assert channel.acked == [4]


def test_dry_run_sends_nothing_back(config, monkeypatch, capsys):
    channel, connections = broker(monkeypatch, WAITING)

    report = tool.replay_dead_letters(config, "classifier", {"retries-exhausted"}, dry_run=True)

    assert report.sent_back == {"retries-exhausted": 2}
    assert (channel.published, channel.acked) == ([], [])
    assert connections[0].is_open is False
    assert "would send back id=outage-1 type=sample.v1 reason=retries-exhausted" in (
        capsys.readouterr().out
    )


def test_limit_stops_after_that_many_messages_sent_back(config, monkeypatch):
    channel, _connections = broker(monkeypatch, WAITING)

    report = tool.replay_dead_letters(config, "classifier", {"retries-exhausted"}, limit=1)

    assert report.sent_back == {"retries-exhausted": 1}
    assert channel.acked == [1]
    assert len(channel.waiting) == 3  # not even looked at


def test_a_copy_rabbitmq_does_not_take_stops_the_run_without_removing_the_message(
    config, monkeypatch
):
    channel, connections = broker(monkeypatch, WAITING)
    channel.publish_error = UnroutableError([])

    with pytest.raises(UnroutableError):
        tool.replay_dead_letters(config, "classifier", {"retries-exhausted"})

    assert channel.acked == []
    assert connections[0].is_open is False


def test_a_lost_connection_stops_the_run_and_is_not_closed_again(config, monkeypatch):
    channel, connections = broker(monkeypatch, WAITING)
    channel.publish_error = StreamLostError("connection lost")

    with pytest.raises(StreamLostError):
        tool.replay_dead_letters(config, "classifier", {"retries-exhausted"})

    assert channel.acked == []
    assert connections[0].is_open is False


def test_messages_taken_by_someone_else_meanwhile_end_the_run(config, monkeypatch):
    channel, _connections = broker(monkeypatch, WAITING[:1])
    channel.taken_meanwhile = 2

    report = tool.replay_dead_letters(config, "classifier", {"retries-exhausted"})

    assert report.sent_back == {"retries-exhausted": 1}


def test_copy_keeps_the_message_and_its_dead_letter_headers_but_not_rabbitmq_s():
    dead_lettered = pika.BasicProperties(
        content_type="application/json",
        correlation_id="flow-7",
        message_id="input",
        type="sample.v1",
        user_id="executor",
        expiration="5000",
        headers={
            "tenant": "acme",
            "x-death": [{"reason": "expired"}],
            "x-delivery-count": 3,
            "x-first-death-queue": "classifier",
            "x-last-death-reason": "expired",
            # Sender-selected routing: RabbitMQ would also deliver the copy to these queues
            "CC": ["other-queue"],
            "BCC": ["other-queue"],
            "x-kicak-retry-round": 5,
            "x-kicak-dead-letter-reason": "retries-exhausted",
            "x-kicak-original-publisher": "injector",
        },
    )

    copy = tool._sent_back_properties(dead_lettered, "operator")

    assert copy.headers == {
        "tenant": "acme",
        "x-kicak-retry-round": 5,
        # Kept, so the Executor sees the message was sent back and gives it its rounds again
        "x-kicak-dead-letter-reason": "retries-exhausted",
        "x-kicak-original-publisher": "injector",
    }
    assert copy.user_id == "operator"  # RabbitMQ refuses any other than the connection's user
    assert (copy.message_id, copy.type, copy.correlation_id) == ("input", "sample.v1", "flow-7")
    assert copy.content_type == "application/json"
    assert copy.delivery_mode == 2
    assert copy.expiration is None


def test_copy_names_the_creator_of_a_message_without_the_original_publisher_header():
    dead_lettered = pika.BasicProperties(user_id="other-client", headers={"x-death": []})

    copy = tool._sent_back_properties(dead_lettered, "operator")

    assert copy.headers == {"x-kicak-original-publisher": "other-client"}


@pytest.mark.parametrize(
    ("headers", "reason"),
    [
        ({"x-kicak-dead-letter-reason": "malformed"}, "malformed"),
        ({"x-kicak-dead-letter-reason": b"unexpected-error"}, "unexpected-error"),
        ({"x-death": []}, "rabbitmq"),
        (None, "rabbitmq"),
    ],
    ids=["text", "bytes", "rabbitmq", "no-headers"],
)
def test_reason_of_a_dead_letter(headers, reason):
    assert tool._dead_letter_reason(pika.BasicProperties(headers=headers)) == reason


def test_connection_is_named_and_unencrypted_only_when_asked(config, monkeypatch):
    opened: list[pika.ConnectionParameters] = []
    monkeypatch.setattr(pika, "BlockingConnection", opened.append)

    tool._connect(config, "classifier")

    [parameters] = opened
    assert parameters.ssl_options is None
    assert parameters.client_properties["connection_name"].startswith(
        "replay-dead-letters queue=classifier host="
    )
    assert parameters.credentials.username == "guest"


def test_connection_uses_tls_with_the_ca_file(monkeypatch, tmp_path):
    opened: list[pika.ConnectionParameters] = []
    monkeypatch.setattr(pika, "BlockingConnection", opened.append)
    contexts: list[str | None] = []
    context = ssl.create_default_context()

    def create_default_context(cafile=None):
        contexts.append(cafile)
        return context

    monkeypatch.setattr(ssl, "create_default_context", create_default_context)
    ca_file = str(tmp_path / "ca.pem")
    config = KicakConfig(
        values={**CONNECTION_VALUES, "RABBIT_MQ_TLS": "true", "RABBIT_MQ_CA_FILE": ca_file}
    )

    tool._connect(config, "classifier")

    assert contexts == [ca_file]
    assert opened[0].ssl_options.server_hostname == "localhost"


def test_connection_presents_the_client_certificate(monkeypatch):
    opened: list[pika.ConnectionParameters] = []
    monkeypatch.setattr(pika, "BlockingConnection", opened.append)
    loaded: list[tuple[str, str | None]] = []

    class RecordingContext(ssl.SSLContext):
        def load_cert_chain(self, certfile, keyfile=None, password=None) -> None:
            loaded.append((certfile, keyfile))

    monkeypatch.setattr(
        ssl, "create_default_context", lambda cafile=None: RecordingContext(ssl.PROTOCOL_TLS_CLIENT)
    )
    config = KicakConfig(
        values={
            **CONNECTION_VALUES,
            "RABBIT_MQ_TLS": "true",
            "RABBIT_MQ_CERT_FILE": "agent.pem",
            "RABBIT_MQ_KEY_FILE": "agent.key",
        }
    )

    tool._connect(config, "classifier")

    assert loaded == [("agent.pem", "agent.key")]
    assert isinstance(opened[0].ssl_options.context, RecordingContext)


@pytest.mark.parametrize("key", ["RABBIT_MQ_CA_FILE", "RABBIT_MQ_CERT_FILE", "RABBIT_MQ_KEY_FILE"])
def test_connection_refuses_a_tls_file_without_tls(monkeypatch, key):
    monkeypatch.setattr(pika, "BlockingConnection", pytest.fail)
    config = KicakConfig(values={**CONNECTION_VALUES, key: "file.pem"})

    with pytest.raises(KicakConfigError, match=f"^{key} is given, but RABBIT_MQ_TLS is false$"):
        tool._connect(config, "classifier")


@pytest.fixture
def command_line(monkeypatch, config):
    """Run main() with the config of the fixture; return the arguments replay_dead_letters got."""
    monkeypatch.setattr(KicakConfig, "from_file", lambda _path: config)
    calls: list[dict[str, Any]] = []

    def replay(config, queue, reasons, *, limit, dry_run):
        calls.append({"queue": queue, "reasons": reasons, "limit": limit, "dry_run": dry_run})
        return tool.ReplayReport(
            sent_back=Counter({"retries-exhausted": 2}), left=Counter({"malformed": 1})
        )

    monkeypatch.setattr(tool, "replay_dead_letters", replay)
    return calls


def test_main_sends_back_the_reasons_given_and_reports_the_counts(command_line, capsys):
    status = tool.main(
        ["classifier", "--reason", "retries-exhausted", "--reason", "rabbitmq", "--limit", "5"]
    )

    assert status == 0
    assert command_line == [
        {
            "queue": "classifier",
            "reasons": {"retries-exhausted", "rabbitmq"},
            "limit": 5,
            "dry_run": False,
        }
    ]
    output = capsys.readouterr().out
    assert "Sent back 2: {'retries-exhausted': 2}" in output
    assert "Left in classifier.dlq, for other reasons: {'malformed': 1}" in output


def test_main_reports_a_dry_run(command_line, capsys):
    assert tool.main(["classifier", "--reason", "retries-exhausted", "--dry-run"]) == 0

    assert command_line[0]["dry_run"] is True
    assert "Would send back 2" in capsys.readouterr().out


def test_main_exits_with_1_when_rabbitmq_stops_the_run(monkeypatch, config, capsys):
    monkeypatch.setattr(KicakConfig, "from_file", lambda _path: config)

    def missing_queue(*_arguments, **_options):
        raise ChannelClosedByBroker(404, "NOT_FOUND - no queue 'classifier'")

    monkeypatch.setattr(tool, "replay_dead_letters", missing_queue)

    assert tool.main(["classifier", "--reason", "malformed"]) == 1
    assert "the messages not sent back stay in classifier.dlq" in capsys.readouterr().err


@pytest.mark.parametrize(
    "arguments",
    [
        ["classifier"],  # no reason: nothing is sent back by default
        ["classifier", "--reason", "bogus"],
        ["classifier", "--reason", "malformed", "--limit", "0"],
    ],
    ids=["no-reason", "unknown-reason", "limit-zero"],
)
def test_main_refuses_invalid_arguments(command_line, arguments):
    with pytest.raises(SystemExit) as exit_info:
        tool.main(arguments)

    assert exit_info.value.code == 2
    assert command_line == []


def test_limit_must_be_a_positive_whole_number():
    assert tool._positive_int("3") == 3
    with pytest.raises(argparse.ArgumentTypeError, match="1 or more"):
        tool._positive_int("0")
