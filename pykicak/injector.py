"""Injector agent: short-lived node that publishes an initial message."""

from __future__ import annotations

import abc
import time
from collections.abc import Callable
from typing import cast

from pykicak._injector import logger
from pykicak._injector.message_type import _declared_message_type, _serialized
from pykicak._injector.settings import (
    _DEFAULT_BLOCKED_TIMEOUT_SECONDS,
    _MAX_RABBITMQ_ATTEMPTS,
    _check_interval_seconds,
    _checked_blocked_timeout_seconds,
    _retry_delay_seconds,
)
from pykicak._injector.statistics import _Injection, _TimedPart
from pykicak._injector.topology import _InjectorTopology
from pykicak.abstracts import KicakAbstract, _monotonic
from pykicak.config import KicakConfig
from pykicak.messages import KicakMessage


class InjectionError(Exception):
    """A generated message was not injected: RabbitMQ did not accept it or was not reachable.

    Raised by `KicakInjectorAbstract.run()` and `publish()` once every attempt has failed, or
    at once for an error that is not retried. The original error, for example a pika
    connection error, `UnroutableError` (no queue bound to the exchange), or `NackError`, is
    chained as `__cause__`. `message` is the message that was not injected, and
    `exchange_name` the exchange it was meant for.

    Errors raised by the application's own `generate()`, or by serializing its message, are not
    wrapped, nor is the `TypeError` for a message of another class than the declared one. A
    message may still have reached RabbitMQ if the connection was lost before the
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

    def __repr__(self) -> str:
        """Show the message by type and ID, never by payload, which `Exception`'s repr would."""
        return (
            f"{type(self).__name__}(message=<{type(self.message).__name__} "
            f"message_id={self.message.message_id!r}>, exchange_name={self.exchange_name!r})"
        )


class KicakInjectorAbstract[M: KicakMessage](KicakAbstract, abc.ABC):
    """Short-lived node that creates and publishes the first message of a processing flow.

    A subclass implements `generate()` and declares the type of its messages, `M`, once, in its
    class header, where both type checkers and the injector read it, e.g.
    `class Starter(KicakInjectorAbstract[Order])`. The Executor consuming the exchange must accept
    that type. The class must declare its own `kicak_type`; an invalid declaration raises
    `TypeError` when the injector is created, and a message of another class than `M` is refused
    with `TypeError` before anything is published.

    `run()` publishes each generated message on a connection of its own, which it then closes;
    `publish()`, after `connect()` or in a `with` block, publishes on a connection kept open.

    The exchange name is application topology, passed in by the caller rather than read from the
    `.kicak` file. Documentation:
    https://github.com/przemek-barczak/pykicak/blob/main/docs/README.md
    """

    def __init__(
        self,
        config: KicakConfig,
        *,
        exchange_name: str,
        blocked_timeout_seconds: float = _DEFAULT_BLOCKED_TIMEOUT_SECONDS,
    ) -> None:
        """Validate and store the exchange this injector publishes to. Does not connect.

        The message type comes from the class header (see the class docstring), which is checked
        first: an invalid declaration raises `TypeError`.

        `exchange_name` (keyword-only) must be a non-empty `str` of at most 255 UTF-8 bytes that
        does not start with `amq.`, a prefix RabbitMQ reserves; otherwise `TypeError` or
        `ValueError` is raised.

        `blocked_timeout_seconds` (default 60, a finite number greater than 0) is how long an
        attempt waits on a connection that a RabbitMQ memory or disk alarm blocks. The attempt then
        fails with `ConnectionBlockedTimeout` and is retried, so an injection never hangs; lower it
        for an injector someone waits for. A timed-out publish may still be enqueued once the alarm
        clears, so the message can arrive twice. See "Running an Injector" in the documentation.
        """
        self._message_type = cast(
            type[M], _declared_message_type(type(self), KicakInjectorAbstract)
        )
        self._topology = _InjectorTopology.checked(exchange_name=exchange_name)
        blocked_timeout = _checked_blocked_timeout_seconds(blocked_timeout_seconds)
        super().__init__(config)
        # A memory or disk alarm blocks publishing connections; without a timeout, a publish
        # would wait for its confirmation until the alarm clears. The Executor keeps waiting.
        self._blocked_connection_timeout_seconds = blocked_timeout

    def _connection_label(self) -> str:
        """Name the injector's role and exchange in its connection name."""
        return f"injector exchange={self._topology.exchange_name}"

    def _declare_topology(self) -> None:
        """Idempotently declare the exchange as durable."""
        self._topology.declare(self.channel)

    def _retry_rabbitmq_operation[T](self, operation: Callable[[], T], operation_name: str) -> T:
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
                delay_seconds = _retry_delay_seconds(attempt)
                logger.debug(
                    "Waiting %.1f seconds before retrying %s", delay_seconds, operation_name
                )
                time.sleep(delay_seconds)
        # Unreachable: the last attempt returns or raises
        raise RuntimeError("RabbitMQ retry loop exited unexpectedly")  # pragma: no cover

    @abc.abstractmethod
    def generate(self) -> M | None:
        """Generate the message to be published, or None if nothing should be sent this cycle.

        The message must be exactly of the declared class `M`; any other class raises
        `TypeError` before anything is published.
        """

    def publish(self, message: M) -> None:
        """Publish `message` to the configured exchange and wait for RabbitMQ to confirm it.

        Requires an open connection (`connect()` or a `with` block); otherwise, also after
        `close()`, raises `RuntimeError`. A message of another class than the declared `M` raises
        `TypeError`. Raises `InjectionError` if RabbitMQ did not confirm the message, with the
        original error as `__cause__`: `UnroutableError` if no queue is bound to the exchange, `NackError` if
        RabbitMQ refuses the message, or a connection error after three attempts.

        A connection lost before RabbitMQ confirms the message is retried by publishing it again, so
        RabbitMQ may receive it twice; the consuming agents must be idempotent.
        """
        if self._channel is None:
            raise RuntimeError("Not connected. Call connect() first.")
        started_at = _monotonic()
        injection = _Injection(message, self._serialize(message), started_at, None)
        try:
            self._publish_with_retries(injection)
        finally:
            injection.log_injected(self._topology.exchange_name)

    def _serialize(self, message: KicakMessage) -> bytes:
        """Check `message`'s class and serialize it, logging a failure at ERROR and raising it.

        A message of another class than the declared one raises `TypeError`: exactly that class,
        because a subclass would be sent with its own type name.
        """
        return _serialized(
            message,
            self._message_type,
            injector_name=type(self).__name__,
            exchange_name=self._topology.exchange_name,
        )

    def _publish_with_retries(self, injection: _Injection) -> None:
        """Connect if needed and publish, up to three attempts; raise InjectionError on failure.

        Records the attempts, how long their parts took, and what ended the last one in
        `injection`.
        """
        message = injection.message
        exchange_name = self._topology.exchange_name

        def connect_and_publish() -> None:
            with injection.attempt():
                with injection.timing(_TimedPart.CONNECT):
                    self._ensure_connection_once()
                with injection.timing(_TimedPart.PUBLISH):
                    self._publish_serialized(exchange_name, message, injection.body)

        try:
            self._retry_rabbitmq_operation(
                connect_and_publish,
                f"Injection of message id={message.message_id}",
            )
        except BaseException as error:
            injection.error = error
            if isinstance(error, Exception):
                raise InjectionError(message, exchange_name) from error
            raise  # e.g. KeyboardInterrupt, which is not an injection failure to wrap

    def run(self, interval_seconds: float = 0) -> None:
        """Inject one generated message on a short-lived connection, or repeat it at an interval.

        One injection calls `generate()` once. If it returns a message, the injector opens a
        connection, declares the exchange, publishes the message, waits for RabbitMQ's confirmation,
        and closes the connection; if it returns None, no connection is opened. Connecting and
        publishing are attempted up to three times, each time on a new connection and always with
        the same message.

        With `interval_seconds` of 0 or less (the default), `run()` injects once and returns; with a
        positive value, it repeats the injection at that interval until the process ends. A value
        that is not an int or float (`bool` included) raises `TypeError`, and NaN or infinity
        `ValueError`.

        `run()` returns normally only when RabbitMQ confirmed every message, or `generate()`
        returned None. An injection that was not confirmed closes the connection and raises
        `InjectionError`, with the error of the last attempt as `__cause__`.
        Errors from `generate()` or from serializing its message, and the `TypeError` for a message
        of another class than `M`, propagate unchanged.

        A retried attempt publishes the message again, and RabbitMQ may already have accepted the
        earlier one, so it can arrive twice; the consuming agents must be idempotent. See "Running
        an Injector" in the documentation.
        """
        _check_interval_seconds(interval_seconds)
        logger.debug(
            "Starting Injector run exchange=%s interval_seconds=%s",
            self._topology.exchange_name,
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
        exchange_name = self._topology.exchange_name
        started_at = _monotonic()
        try:
            message = self.generate()
        except Exception:
            logger.exception("%s.generate() failed exchange=%s", type(self).__name__, exchange_name)
            raise
        generate_seconds = _monotonic() - started_at
        if message is None:
            logger.debug("Injector generated no message; nothing to publish")
            return
        logger.debug("Injector generated message type=%s", type(message).__name__)
        injection = _Injection(message, self._serialize(message), started_at, generate_seconds)
        try:
            self._publish_with_retries(injection)
        finally:
            self._discard_connection()  # short-lived: no connection is kept between injections
            injection.log_injected(exchange_name)  # its total includes closing the connection
