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
import trustme
from cryptography.hazmat.primitives import serialization
from pika.exceptions import ChannelClosedByBroker, StreamLostError, UnroutableError

import examples.replay_dead_letters as tool
from pykicak import KicakConfig, KicakConfigError
from pykicak._executor.headers import _retry_round_of
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
        # Kept, so the Executor sees the message was sent back; its retry round is dropped
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


@pytest.mark.parametrize(
    "dead_letter_headers",
    [
        {"x-kicak-dead-letter-reason": "retries-exhausted"},
        {"x-death": [{"reason": "delivery_limit"}]},
    ],
    ids=["by-an-executor", "by-rabbitmq"],
)
def test_message_sent_back_gets_all_its_retry_rounds_again(dead_letter_headers):
    # A message RabbitMQ dead-lettered itself, e.g. at a quorum queue's delivery limit, has no
    # dead-letter headers of an Executor, so only dropping its rounds gives it them all again
    dead_lettered = pika.BasicProperties(
        message_id="input",
        headers={
            "x-kicak-retry-round": 7,
            "x-kicak-retrying-since": "2026-10-01T12:00:00+00:00",
            **dead_letter_headers,
        },
    )

    copy = tool._sent_back_properties(dead_lettered, "operator")

    assert "x-kicak-retry-round" not in copy.headers
    assert "x-kicak-retrying-since" not in copy.headers
    assert _retry_round_of(copy.headers) == 0


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


def test_connection_uses_the_tls_options_of_pykicak(monkeypatch):
    opened: list[pika.ConnectionParameters] = []
    monkeypatch.setattr(pika, "BlockingConnection", opened.append)
    options = pika.SSLOptions(ssl.create_default_context(), server_hostname="localhost")
    configs: list[KicakConfig] = []

    def recording_tls_options(config: KicakConfig) -> pika.SSLOptions:
        configs.append(config)
        return options

    monkeypatch.setattr(tool, "tls_options", recording_tls_options)
    config = KicakConfig(values={**CONNECTION_VALUES, "RABBIT_MQ_TLS": "true"})

    tool._connect(config, "classifier")

    assert configs == [config]
    assert opened[0].ssl_options is options


def write_tls_files(tmp_path) -> dict[str, str]:
    """Write a CA file and a client certificate with its key; return the keys naming them."""
    ca = trustme.CA()
    client = ca.issue_cert("operator.example")
    paths = {name: str(tmp_path / name) for name in ("ca.pem", "client.pem", "client.key")}
    ca.cert_pem.write_to_path(paths["ca.pem"])
    client.cert_chain_pems[0].write_to_path(paths["client.pem"])
    client.private_key_pem.write_to_path(paths["client.key"])
    return {
        "RABBIT_MQ_CA_FILE": paths["ca.pem"],
        "RABBIT_MQ_CERT_FILE": paths["client.pem"],
        "RABBIT_MQ_KEY_FILE": paths["client.key"],
    }


def test_connection_applies_the_tls_policy_of_pykicak(monkeypatch, tmp_path):
    opened: list[pika.ConnectionParameters] = []
    monkeypatch.setattr(pika, "BlockingConnection", opened.append)
    config = KicakConfig(
        values={**CONNECTION_VALUES, "RABBIT_MQ_TLS": "true", **write_tls_files(tmp_path)}
    )

    tool._connect(config, "classifier")

    ssl_options = opened[0].ssl_options
    assert ssl_options.server_hostname == "localhost"
    context = ssl_options.context
    assert context.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert context.verify_flags & ssl.VERIFY_X509_STRICT
    assert context.check_hostname is True


def encrypted_key_file(tmp_path, key_file: str) -> str:
    key = serialization.load_pem_private_key(open(key_file, "rb").read(), password=None)
    path = tmp_path / "encrypted.key"
    path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(b"key-password"),
        )
    )
    return str(path)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("tls-off", "RABBIT_MQ_CA_FILE requires RABBIT_MQ_TLS=true"),
        ("key-only", "RABBIT_MQ_CERT_FILE and RABBIT_MQ_KEY_FILE must be given together"),
        (
            "encrypted-key",
            "RABBIT_MQ_KEY_FILE holds an encrypted private key, which is not supported",
        ),
    ],
)
def test_connection_refuses_what_pykicak_refuses(monkeypatch, tmp_path, change, message):
    # Before tls_options(), the tool ignored a key without its certificate, and an encrypted key
    # made OpenSSL ask for its password on the terminal, which left the tool waiting
    monkeypatch.setattr(pika, "BlockingConnection", pytest.fail)
    values = {**CONNECTION_VALUES, "RABBIT_MQ_TLS": "true", **write_tls_files(tmp_path)}
    if change == "tls-off":
        values["RABBIT_MQ_TLS"] = "false"
    elif change == "key-only":
        del values["RABBIT_MQ_CERT_FILE"]
    else:
        values["RABBIT_MQ_KEY_FILE"] = encrypted_key_file(tmp_path, values["RABBIT_MQ_KEY_FILE"])

    with pytest.raises(KicakConfigError, match=f"^{message}$"):
        tool._connect(KicakConfig(values=values), "classifier")


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
