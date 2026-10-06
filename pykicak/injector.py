"""Injector agent: short-lived node that publishes an initial message."""

from __future__ import annotations

import abc
import logging
import time
from collections.abc import Callable
from typing import TypeVar

from pika.exchange_type import ExchangeType

from pykicak.abstracts import KicakAbstract
from pykicak.config import KicakConfig
from pykicak.messages import KicakMessage

logger = logging.getLogger(__name__)

_MAX_RABBITMQ_ATTEMPTS = 3
_INITIAL_RETRY_DELAY_SECONDS = 1.0  # attempts: immediately, after 1 second, after 2 more seconds
_BLOCKED_CONNECTION_TIMEOUT_SECONDS = 60.0
_T = TypeVar("_T")


class InjectionError(Exception):
    """A generated message was not injected: RabbitMQ did not accept it or was not reachable.

    Raised by `KicakInjectorAbstract.run()` and `publish()` once every attempt has failed, or
    at once for an error that is not retried. The original error, for example a pika
    connection error, `UnroutableError` (no queue bound to the exchange), or `NackError`, is
    chained as `__cause__`. `message` is the message that was not injected, and
    `exchange_name` the exchange it was meant for.

    Errors raised by the application's own `generate()`, or by serializing its message, are not
    wrapped. A message may still have reached RabbitMQ if the connection was lost before the
    confirmation arrived, so consumers must be idempotent.
    """

    def __init__(self, message: KicakMessage, exchange_name: str) -> None:
        """Record the message that was not injected and its exchange."""
        super().__init__(message, exchange_name)
        self.message = message
        self.exchange_name = exchange_name

    def __str__(self) -> str:
        """Describe the message by type and ID, never by payload, followed by the cause."""
        text = (
            f"{type(self.message).__name__} message "
            f"id={self.message.message_id} "
            f"was not injected into exchange {self.exchange_name!r}"
        )
        if self.__cause__ is not None:
            text += f": {self.__cause__!r}"
        return text


class KicakInjectorAbstract(KicakAbstract, abc.ABC):
    """Short-lived node that creates and publishes messages to the configured exchange.

    The Injector represents the entry point of a processing flow. It creates an
    application-specific message and publishes it to an exchange, then terminates
    and closes its RabbitMQ connection.

    The destination exchange is application topology, not connection configuration,
    so it is passed in by the caller (typically sourced from an application's
    messaging/exchanges.py) rather than read from the `.kicak` file.
    """

    # A memory or disk alarm blocks publishing connections; without a timeout, a publish would
    # wait for its confirmation until the alarm clears. The Executor keeps waiting instead.
    _blocked_connection_timeout_seconds = _BLOCKED_CONNECTION_TIMEOUT_SECONDS

    def __init__(self, config: KicakConfig, *, exchange_name: str) -> None:
        """Store the exchange this injector publishes to.

        `exchange_name` is keyword-only, like the Executor's topology arguments.
        """
        if not exchange_name:
            raise ValueError("exchange_name must be a non-empty name")
        super().__init__(config)
        self._exchange_name = exchange_name

    def _declare_topology(self) -> None:
        """Idempotently declare the exchange as durable."""
        logger.debug("Declaring exchange %s", self._exchange_name)
        self.channel.exchange_declare(
            exchange=self._exchange_name, exchange_type=ExchangeType.fanout, durable=True
        )

    def _retry_rabbitmq_operation(
        self, operation: Callable[[], _T], operation_name: str
    ) -> _T:
        """Try a RabbitMQ operation up to three times: now, after 1 second, after 2 more seconds.

        Only transient errors are retried; the last error is raised once the attempts run out.
        """
        for attempt in range(1, _MAX_RABBITMQ_ATTEMPTS + 1):
            logger.debug(
                "Starting %s attempt %d/%d",
                operation_name,
                attempt,
                _MAX_RABBITMQ_ATTEMPTS,
            )
            try:
                result = operation()
                logger.debug("%s succeeded on attempt %d", operation_name, attempt)
                return result
            except Exception as error:
                if not self._is_retryable_rabbitmq_error(error):
                    logger.exception("%s failed", operation_name)
                    raise

                self._discard_connection()
                if attempt == _MAX_RABBITMQ_ATTEMPTS:
                    logger.exception("%s failed after %d attempts", operation_name, attempt)
                    raise

                logger.warning(
                    "%s failed, retrying attempt %d/%d: %s",
                    operation_name,
                    attempt + 1,
                    _MAX_RABBITMQ_ATTEMPTS,
                    error,
                )
                delay_seconds = _INITIAL_RETRY_DELAY_SECONDS * 2 ** (attempt - 1)
                logger.debug(
                    "Waiting %.1f seconds before retrying %s", delay_seconds, operation_name
                )
                time.sleep(delay_seconds)
        raise RuntimeError("RabbitMQ retry loop exited unexpectedly")

    @abc.abstractmethod
    def generate(self) -> KicakMessage | None:
        """Generate the message to be published, or None if nothing should be sent this cycle."""

    def publish(self, message: KicakMessage) -> None:
        """Publish `message` to the configured exchange and wait for RabbitMQ to confirm it.

        Requires an open connection (`connect()` or a `with` block); otherwise, including after
        `close()` or the end of the `with` block, raises `RuntimeError`. Raises `InjectionError`
        if the message was not injected, with the original error as `__cause__`:
        `UnroutableError` if no queue is bound to the exchange (the consuming Executor's topology
        is not installed), `NackError` if RabbitMQ refuses the message, or a connection error
        after three attempts. A connection that a RabbitMQ memory or disk alarm blocks for more
        than 60 seconds fails with `ConnectionBlockedTimeout`, a connection error.

        A connection lost before RabbitMQ confirms the message is retried by publishing it again,
        so RabbitMQ may receive it twice; the consuming agents must be idempotent.
        """
        if self._channel is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._publish_with_retries(message, message.to_bytes())

    def _publish_with_retries(self, message: KicakMessage, body: bytes) -> None:
        """Connect if needed and publish, up to three attempts; raise InjectionError on failure."""

        def connect_and_publish() -> None:
            self._ensure_connection_once()
            self._publish_serialized(self._exchange_name, message, body)

        try:
            self._retry_rabbitmq_operation(
                connect_and_publish,
                f"Injection of message id={message.message_id}",
            )
        except Exception as error:
            raise InjectionError(message, self._exchange_name) from error

    def run(self, interval_seconds: float = 0) -> None:
        """Inject one generated message on a short-lived connection, or repeat it at an interval.

        One injection calls `generate()` once. If it returns a message, the Injector opens a
        connection, declares the exchange, publishes the message, waits for RabbitMQ's
        confirmation, and closes the connection. Connecting and publishing are attempted up to
        three times (immediately, after 1 second, and after 2 more seconds), each time on a new
        connection and always with the same generated message. If `generate()` returns None,
        nothing is published and no connection is opened.

        With `interval_seconds` of 0 or less (the default), `run()` injects once and returns.
        With a positive `interval_seconds`, it injects, sleeps for `interval_seconds` with no
        connection open, and repeats until the process ends; there is no `stop()`.

        `run()` returns normally only when every injection succeeded. If one fails, the
        connection is closed and `InjectionError` is raised, ending `run()` in either mode, so
        no message is silently skipped. Its `__cause__` is the error that ended the last attempt
        (for example a pika connection error, `UnroutableError`, or `NackError`), and its
        `message` is the message that was not injected. Errors raised by `generate()` or by
        serializing the message propagate unchanged. An attempt never waits more than 60
        seconds on a connection that a RabbitMQ memory or disk alarm blocks: it then fails with
        `ConnectionBlockedTimeout` and is retried, so an injection cannot hang during an alarm.

        A retried attempt publishes the message again, and RabbitMQ may already have accepted
        the earlier one, so it can arrive twice; the consuming agents must be idempotent.
        """
        logger.debug(
            "Starting Injector run exchange=%s interval_seconds=%s",
            self._exchange_name,
            interval_seconds,
        )
        if interval_seconds <= 0:
            self._inject()
            return
        while True:
            self._inject()
            logger.debug("Waiting %.1f seconds before the next injection", interval_seconds)
            time.sleep(interval_seconds)

    def _inject(self) -> None:
        """Generate one message and publish it on a connection that is closed afterwards."""
        message = self.generate()
        if message is None:
            logger.debug("Injector generated no message; nothing to publish")
            return
        logger.debug("Injector generated message type=%s", type(message).__name__)
        body = message.to_bytes()
        try:
            self._publish_with_retries(message, body)
        finally:
            self._discard_connection()  # short-lived: no connection is kept between injections
