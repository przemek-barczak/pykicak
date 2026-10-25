"""Send dead-lettered messages back to the queue of the Executor that dead-lettered them.

An operator's tool, an example built on pika and `KicakConfig` only, not part of pykicak. Run it
once the cause is fixed: a service that was down long enough for messages to use up their
`max_retry_rounds`, a bug fixed in `execute()`, or a message type the Executor now accepts. From
the repository root, with `KICAK_CONFIG_PATH` naming the connection file (default `.kicak`):

    python -m examples.replay_dead_letters classifier --reason retries-exhausted --dry-run
    python -m examples.replay_dead_letters classifier --reason retries-exhausted

It takes the messages waiting in `<queue>.dlq` one by one and sends back those whose
`x-kicak-dead-letter-reason` header is one of the given reasons; `rabbitmq` selects the messages
RabbitMQ dead-lettered itself, which have no such header, e.g. at a quorum queue's delivery
limit. Each is published to `<queue>` through the default exchange, which routes by queue name,
so it reaches that queue only, not every queue bound to the Executor's source exchange, and it is
removed from the dead-letter queue only after RabbitMQ has confirmed the copy. The others stay
unacknowledged until the end, when closing the connection returns them to the dead-letter queue.

The copy keeps the body and the properties, so the message comes back as the same version and
with the same ID, and its headers, the dead-letter headers included: from those the Executor sees
that the message was sent back, and gives it all its retry rounds again. Its `user_id` becomes the
user of this tool's connection, because RabbitMQ refuses any other, while its
`x-kicak-original-publisher` header keeps naming the agent that created the message. RabbitMQ's
own headers (`x-death` and the others) are dropped, and so are `CC` and `BCC`, with which RabbitMQ
would also deliver the copy to the queues they name.

Like every delivery in pykicak, sending back is at-least-once: if the connection is lost after
RabbitMQ confirmed a copy but before the message was removed from the dead-letter queue, the
message is back in its queue and still in the dead-letter queue, and running the tool again sends
it back twice. The agents are idempotent, so that is safe. Send back `unexpected-error` messages
only once the bug is fixed: each of them would stop an Executor instance again.

The messages taken stay unacknowledged until the run ends, so RabbitMQ's `consumer_timeout` (30
minutes by default) bounds how long a run may take; use `--limit` for a very long queue.
"""

from __future__ import annotations

import argparse
import os
import socket
import ssl
import sys
from collections import Counter
from collections.abc import Collection, Sequence
from dataclasses import dataclass, field

import pika
import pika.exceptions

from pykicak import KicakConfig, KicakConfigError

DEAD_LETTER_REASONS = ("malformed", "retries-exhausted", "result-too-large", "unexpected-error")
"""The values of the `x-kicak-dead-letter-reason` header of an Executor's dead-letter copies."""

RABBITMQ_REASON = "rabbitmq"
"""Selects the messages RabbitMQ dead-lettered itself: they have no `x-kicak-dead-letter-reason`."""

_REASON_HEADER = "x-kicak-dead-letter-reason"
_ORIGINAL_PUBLISHER_HEADER = "x-kicak-original-publisher"
# Headers RabbitMQ sets itself when it dead-letters a message
_BROKER_HEADERS = frozenset({"x-death", "x-delivery-count"})
_BROKER_HEADER_PREFIXES = ("x-first-death-", "x-last-death-")
# Sender-selected distribution: RabbitMQ would also route the copy to the queues these name
_ROUTING_HEADERS = frozenset({"CC", "BCC"})


@dataclass
class ReplayReport:
    """What a run did: the messages it sent back, and those it left, by dead-letter reason."""

    sent_back: Counter[str] = field(default_factory=Counter)
    """Sent back to the queue (with `dry_run`, those that would have been)."""

    left: Counter[str] = field(default_factory=Counter)
    """Looked at but not selected, so left in the dead-letter queue."""


def replay_dead_letters(
    config: KicakConfig,
    queue: str,
    reasons: Collection[str],
    *,
    limit: int | None = None,
    dry_run: bool = False,
) -> ReplayReport:
    """Send the messages in `<queue>.dlq` dead-lettered for one of `reasons` back to `queue`.

    At most `limit` messages, if given. With `dry_run`, nothing is sent back: the report tells
    what would be. Raises a pika error if a queue is missing or RabbitMQ does not take a copy;
    the messages not yet sent back then stay in the dead-letter queue.
    """
    dead_letter_queue = f"{queue}.dlq"
    report = ReplayReport()
    connection = _connect(config, queue)
    try:
        channel = connection.channel()
        channel.confirm_delivery()
        # Both exist once the queue's Executor has run; a missing one raises 404 NOT_FOUND
        channel.queue_declare(queue=queue, passive=True)
        waiting = channel.queue_declare(queue=dead_letter_queue, passive=True).method.message_count
        # Only the messages waiting now: a message dead-lettered meanwhile waits for the next run
        for _ in range(waiting):
            if limit is not None and report.sent_back.total() >= limit:
                break
            method, properties, body = channel.basic_get(dead_letter_queue, auto_ack=False)
            if method is None:
                break
            reason = _dead_letter_reason(properties)
            if reason not in reasons:
                report.left[reason] += 1
                continue  # left unacknowledged: closing the connection returns it
            if not dry_run:
                channel.basic_publish(
                    exchange="",  # the default exchange routes by queue name
                    routing_key=queue,
                    body=body,
                    properties=_sent_back_properties(properties, config.rabbitmq_username),
                    mandatory=True,
                )
                # Removed only once RabbitMQ has confirmed the copy
                channel.basic_ack(method.delivery_tag)
            report.sent_back[reason] += 1
            print(
                f"{'would send back' if dry_run else 'sent back'} id={properties.message_id} "
                f"type={properties.type} reason={reason}"
            )
    finally:
        # Returns every message left unacknowledged to the dead-letter queue
        if connection.is_open:
            connection.close()
    return report


def _connect(config: KicakConfig, queue: str) -> pika.BlockingConnection:
    """Connect as `config` says, with TLS if it asks for it, verifying the broker's certificate."""
    tls_files = {
        "RABBIT_MQ_CA_FILE": config.rabbitmq_ca_file,
        "RABBIT_MQ_CERT_FILE": config.rabbitmq_cert_file,
        "RABBIT_MQ_KEY_FILE": config.rabbitmq_key_file,
    }
    ssl_options = None
    if config.rabbitmq_tls:
        context = ssl.create_default_context(cafile=tls_files["RABBIT_MQ_CA_FILE"])
        if tls_files["RABBIT_MQ_CERT_FILE"] is not None:
            # The client certificate, for a broker that verifies its clients (mutual TLS)
            context.load_cert_chain(
                tls_files["RABBIT_MQ_CERT_FILE"], tls_files["RABBIT_MQ_KEY_FILE"]
            )
        ssl_options = pika.SSLOptions(context, server_hostname=config.rabbitmq_host)
    else:
        for key, path in tls_files.items():
            if path is not None:
                # A TLS file shows that TLS was intended: never send the credentials unencrypted
                raise KicakConfigError(f"{key} is given, but RABBIT_MQ_TLS is false")
    parameters = pika.ConnectionParameters(
        host=config.rabbitmq_host,
        port=config.rabbitmq_port,
        virtual_host=config.rabbitmq_virtual_host,
        credentials=pika.PlainCredentials(config.rabbitmq_username, config.rabbitmq_password),
        ssl_options=ssl_options,
        client_properties={
            "connection_name": (
                f"replay-dead-letters queue={queue} host={socket.gethostname()} pid={os.getpid()}"
            )
        },
    )
    return pika.BlockingConnection(parameters)


def _dead_letter_reason(properties: pika.BasicProperties) -> str:
    """Return why a message was dead-lettered: its reason header, or `rabbitmq` without one."""
    reason = (properties.headers or {}).get(_REASON_HEADER)
    if isinstance(reason, bytes):
        reason = reason.decode("utf-8", errors="replace")
    return reason if isinstance(reason, str) else RABBITMQ_REASON


def _sent_back_properties(properties: pika.BasicProperties, user_id: str) -> pika.BasicProperties:
    """Return the properties of the copy sent back: the message's own, as it was dead-lettered.

    Its headers lose those RabbitMQ sets itself, and `CC` and `BCC`, which would route the copy
    to other queues too. The dead-letter headers stay, so that the Executor gives the message its
    retry rounds again. The `user_id` must be the user of this connection, so a message RabbitMQ
    dead-lettered itself, whose `user_id` names its creator, keeps the creator in
    `x-kicak-original-publisher` if the header is missing.
    """
    headers = {
        name: value
        for name, value in (properties.headers or {}).items()
        if name not in _BROKER_HEADERS
        and name not in _ROUTING_HEADERS
        and not name.startswith(_BROKER_HEADER_PREFIXES)
    }
    if _ORIGINAL_PUBLISHER_HEADER not in headers and properties.user_id:
        headers[_ORIGINAL_PUBLISHER_HEADER] = properties.user_id
    return pika.BasicProperties(
        content_type=properties.content_type,
        content_encoding=properties.content_encoding,
        headers=headers,
        delivery_mode=2,  # persistent
        priority=properties.priority,
        correlation_id=properties.correlation_id,
        reply_to=properties.reply_to,
        message_id=properties.message_id,
        timestamp=properties.timestamp,
        type=properties.type,
        app_id=properties.app_id,
        user_id=user_id,
    )


def _positive_int(text: str) -> int:
    """Parse `--limit`: a whole number, 1 or more."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be 1 or more")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """Run the tool from the command line; return the exit status."""
    parser = argparse.ArgumentParser(
        prog="python -m examples.replay_dead_letters",
        description="Send dead-lettered messages from <queue>.dlq back to <queue>.",
    )
    parser.add_argument("queue", help="the source queue of the Executor that dead-lettered them")
    parser.add_argument(
        "--reason",
        action="append",
        required=True,
        choices=(*DEAD_LETTER_REASONS, RABBITMQ_REASON),
        help="send back the messages dead-lettered for this reason; may be given more than once",
    )
    parser.add_argument("--limit", type=_positive_int, help="send back at most this many")
    parser.add_argument("--dry-run", action="store_true", help="only list what would be sent back")
    arguments = parser.parse_args(argv)
    config = KicakConfig.from_file(os.environ.get("KICAK_CONFIG_PATH", ".kicak"))
    try:
        report = replay_dead_letters(
            config,
            arguments.queue,
            set(arguments.reason),
            limit=arguments.limit,
            dry_run=arguments.dry_run,
        )
    except pika.exceptions.AMQPError as error:
        print(
            f"Stopped: {error!r}; the messages not sent back stay in {arguments.queue}.dlq",
            file=sys.stderr,
        )
        return 1
    verb = "Would send back" if arguments.dry_run else "Sent back"
    print(f"{verb} {report.sent_back.total()}: {dict(report.sent_back)}")
    print(f"Left in {arguments.queue}.dlq, for other reasons: {dict(report.left)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
