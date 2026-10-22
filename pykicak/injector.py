"""Injector agent: short-lived node that publishes an initial message."""

from __future__ import annotations

import abc
import logging
import math
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import cast

from pika.exchange_type import ExchangeType

from pykicak.abstracts import (
    KicakAbstract,
    _check_name,
    _log_values,
    _monotonic,
    _type_arguments,
    _union_members,
)
from pykicak.config import KicakConfig
from pykicak.messages import KicakMessage, _type_name_of

logger = logging.getLogger(__name__)

_MAX_RABBITMQ_ATTEMPTS = 3
_INITIAL_RETRY_DELAY_SECONDS = 1.0  # attempts: immediately, after 1 second, after 2 more seconds
_DEFAULT_BLOCKED_TIMEOUT_SECONDS = 60.0


_INJECTOR_TYPE_USAGE = "KicakInjectorAbstract[MyMessage]"


class _TimedPart(StrEnum):
    """A part of an injection whose duration the `Message injected` line reports."""

    CONNECT = "connect"
    """Opening a connection and declaring the exchange, when the attempt needs a connection."""

    PUBLISH = "publish"
    """Publishing the message and waiting for RabbitMQ's confirmation."""

    BACKOFF = "backoff"
    """The time between two attempts: discarding the failed connection and waiting to retry."""


@dataclass(slots=True)
class _Injection:
    """How the injection of one message goes, for the `Message injected` line logged at its end.

    Times are of `_monotonic()`, durations in seconds.
    """

    message: KicakMessage
    body: bytes
    started_at: float
    generate_seconds: float | None
    """How long `generate()` took, or None for a message given to `publish()`."""

    attempts: int = 0
    seconds: dict[_TimedPart, float] = field(default_factory=lambda: dict.fromkeys(_TimedPart, 0.0))
    attempt_ended_at: float = 0.0
    error: BaseException | None = None
    """What ended the last attempt, if the message was not injected."""

    @contextmanager
    def timing(self, part: _TimedPart) -> Iterator[None]:
        """Add how long the `with` block takes to the duration of `part`, also if it raises."""
        started_at = _monotonic()
        try:
            yield
        finally:
            self.seconds[part] += _monotonic() - started_at

    @contextmanager
    def attempt(self) -> Iterator[None]:
        """Count an attempt, adding the time since the previous one ended to the backoff."""
        if self.attempts:
            self.seconds[_TimedPart.BACKOFF] += _monotonic() - self.attempt_ended_at
        self.attempts += 1
        try:
            yield
        finally:
            self.attempt_ended_at = _monotonic()


def _declared_message_type(injector_class: type) -> type[KicakMessage]:
    """Return the message type an Injector class declares in its class header.

    Raises `TypeError` unless it is one message class that declares its own `kicak_type`.
    """
    (argument,) = _type_arguments(injector_class, KicakInjectorAbstract, usage=_INJECTOR_TYPE_USAGE)
    members = _union_members(argument)
    if len(members) > 1:
        raise TypeError(
            f"{injector_class.__name__} must declare one message type, got {argument!r}"
        )
    _type_name_of(members[0])
    return cast(type[KicakMessage], members[0])


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


class KicakInjectorAbstract[M: KicakMessage](KicakAbstract, abc.ABC):
    """Short-lived node that creates and publishes messages to the configured exchange.

    The Injector represents the entry point of a processing flow. It creates an
    application-specific message and publishes it to an exchange, then terminates
    and closes its RabbitMQ connection.

    A subclass declares the type of its messages, `M`, once, in its class header, where both
    type checkers and the injector read it, e.g. `class Starter(KicakInjectorAbstract[Order])`.
    It is the contract with the Executor consuming the exchange, which must accept that type:
    every message is sent with its class's `kicak_type` as the AMQP `type` property, and a
    message of another class than `M` is refused with `TypeError` before anything is published.
    The class must declare its own `kicak_type`. The injector raises `TypeError` when it is
    created if the declaration is missing or invalid, e.g. for a subclass of the unparameterized
    class, or names more than one class.

    Every message it tries to publish, by `run()` or `publish()`, is logged at `INFO` in a
    `Message injected` line, whether or not it was injected, telling its attempts, its size, and
    how long each part took, for statistics.

    The destination exchange is application topology, not connection configuration,
    so it is passed in by the caller (typically sourced from an application's
    messaging/exchanges.py) rather than read from the `.kicak` file.
    """

    def __init__(
        self,
        config: KicakConfig,
        *,
        exchange_name: str,
        blocked_timeout_seconds: float = _DEFAULT_BLOCKED_TIMEOUT_SECONDS,
    ) -> None:
        """Store the exchange this injector publishes to, and how long it waits while blocked.

        The message type comes from the class header (see the class docstring), which is checked
        first: an invalid declaration raises `TypeError`.

        `exchange_name` is keyword-only, like the Executor's topology arguments. It must be a
        non-empty `str` of at most 255 UTF-8 bytes that does not start with `amq.`, a prefix
        RabbitMQ reserves; otherwise `TypeError` or `ValueError` is raised.

        `blocked_timeout_seconds` (default 60) is how long an attempt waits on a connection that
        a RabbitMQ memory or disk alarm blocks: RabbitMQ then accepts no published message
        until the alarm clears. After that time the attempt fails with `ConnectionBlockedTimeout`
        and is retried, so an injection never hangs; lower it for an injector that someone waits
        for, e.g. one answering an HTTP request. RabbitMQ may still enqueue the publish of a
        timed-out attempt once the alarm clears, so the message can arrive twice. It must be a
        finite int or float greater than 0, `bool` excluded; otherwise `TypeError` or
        `ValueError` is raised. An injector always gives up eventually, so there is no value for
        waiting until the alarm clears.
        """
        self._message_type = cast(type[M], _declared_message_type(type(self)))
        _check_name("exchange_name", exchange_name)
        if isinstance(blocked_timeout_seconds, bool) or not isinstance(
            blocked_timeout_seconds, int | float
        ):
            raise TypeError("blocked_timeout_seconds must be a number")
        if not math.isfinite(blocked_timeout_seconds) or blocked_timeout_seconds <= 0:
            raise ValueError("blocked_timeout_seconds must be a finite number greater than 0")
        super().__init__(config)
        self._exchange_name = exchange_name
        # A memory or disk alarm blocks publishing connections; without a timeout, a publish
        # would wait for its confirmation until the alarm clears. The Executor keeps waiting.
        self._blocked_connection_timeout_seconds = float(blocked_timeout_seconds)

    def _connection_label(self) -> str:
        """Name the injector's role and exchange in its connection name."""
        return f"injector exchange={self._exchange_name}"

    def _declare_topology(self) -> None:
        """Idempotently declare the exchange as durable."""
        logger.debug("Declaring exchange %s", self._exchange_name)
        self.channel.exchange_declare(
            exchange=self._exchange_name, exchange_type=ExchangeType.fanout, durable=True
        )

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
                delay_seconds = _INITIAL_RETRY_DELAY_SECONDS * 2 ** (attempt - 1)
                logger.debug(
                    "Waiting %.1f seconds before retrying %s", delay_seconds, operation_name
                )
                time.sleep(delay_seconds)
        raise RuntimeError("RabbitMQ retry loop exited unexpectedly")

    @abc.abstractmethod
    def generate(self) -> M | None:
        """Generate the message to be published, or None if nothing should be sent this cycle.

        The message must be exactly of the declared class `M`; any other class raises
        `TypeError` before anything is published.
        """

    def publish(self, message: M) -> None:
        """Publish `message` to the configured exchange and wait for RabbitMQ to confirm it.

        Requires an open connection (`connect()` or a `with` block); otherwise, including after
        `close()` or the end of the `with` block, raises `RuntimeError`. A message of another
        class than the declared `M` raises `TypeError`. Raises `InjectionError`
        if the message was not injected, with the original error as `__cause__`:
        `UnroutableError` if no queue is bound to the exchange (the consuming Executor's topology
        is not installed), `NackError` if RabbitMQ refuses the message, or a connection error
        after three attempts. A connection that a RabbitMQ memory or disk alarm blocks for more
        than `blocked_timeout_seconds` fails with `ConnectionBlockedTimeout`, a connection error.

        The injector creates the message, so its user is both the message's `user_id`, which
        RabbitMQ verifies, and its original publisher, in the `x-kicak-original-publisher` header,
        which an Executor keeps when it moves the message to its retry queue.

        A connection lost before RabbitMQ confirms the message is retried by publishing it again,
        so RabbitMQ may receive it twice; the consuming agents must be idempotent.
        """
        if self._channel is None:
            raise RuntimeError("Not connected. Call connect() first.")
        started_at = _monotonic()
        injection = _Injection(message, self._serialize(message), started_at, None)
        try:
            self._publish_with_retries(injection)
        finally:
            self._log_injected(injection)

    def _serialize(self, message: KicakMessage) -> bytes:
        """Check `message`'s class and serialize it, logging a failure at ERROR and raising it.

        A message of another class than the declared one raises `TypeError`: exactly that class,
        because a subclass would be sent with its own type name.
        """
        if type(message) is not self._message_type:
            logger.error(
                "Message refused type=%s exchange=%s: %s declares its messages as %s",
                type(message).__name__,
                self._exchange_name,
                type(self).__name__,
                self._message_type.__name__,
            )
            raise TypeError(
                f"{type(self).__name__} declares its messages as {self._message_type.__name__}, "
                f"got {type(message).__name__}"
            )
        try:
            return message.to_bytes()
        except Exception:
            logger.exception(
                "Failed to serialize message type=%s exchange=%s",
                type(message).__name__,
                self._exchange_name,
            )
            raise

    def _publish_with_retries(self, injection: _Injection) -> None:
        """Connect if needed and publish, up to three attempts; raise InjectionError on failure.

        Records the attempts, how long their parts took, and what ended the last one in
        `injection`.
        """
        message = injection.message

        def connect_and_publish() -> None:
            with injection.attempt():
                with injection.timing(_TimedPart.CONNECT):
                    self._ensure_connection_once()
                with injection.timing(_TimedPart.PUBLISH):
                    self._publish_serialized(self._exchange_name, message, injection.body)

        try:
            self._retry_rabbitmq_operation(
                connect_and_publish,
                f"Injection of message id={message.message_id}",
            )
        except BaseException as error:
            injection.error = error
            if isinstance(error, Exception):
                raise InjectionError(message, self._exchange_name) from error
            raise  # e.g. KeyboardInterrupt, which is not an injection failure to wrap

    def _log_injected(self, injection: _Injection) -> None:
        """Log the `Message injected` line: whether the message was injected, and what it took.

        One line at `INFO` for every message handed to RabbitMQ, by `run()` or `publish()`,
        whatever the outcome, so that statistics can be computed from the logs; nothing is
        logged for a message that was never published, e.g. because `generate()` returned None
        or raised. The record also carries the values, typed, as its `kicak` attribute.
        """
        if not logger.isEnabledFor(logging.INFO):
            return
        message = injection.message
        error = injection.error
        seconds = injection.seconds
        _log_values(
            logger,
            "Message injected",
            "message_injected",
            {
                "outcome": "injected" if error is None else "failed",
                "error": None if error is None else type(error).__name__,
                "type": type(message).__name__,
                "id": message.message_id,
                "exchange": self._exchange_name,
                "attempts": injection.attempts,
                "message_bytes": len(injection.body),
                "total_seconds": _monotonic() - injection.started_at,
                "generate_seconds": injection.generate_seconds,
                "connect_seconds": seconds[_TimedPart.CONNECT],
                "publish_seconds": seconds[_TimedPart.PUBLISH],
                "backoff_seconds": seconds[_TimedPart.BACKOFF],
            },
        )

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
        connection open, and repeats until the process ends; there is no `stop()`. It is checked
        before anything is generated: a value that is not an int or float (`bool` included)
        raises `TypeError`, and NaN or infinity `ValueError`.

        `run()` returns normally only when every injection succeeded. If one fails, the
        connection is closed and `InjectionError` is raised, ending `run()` in either mode, so
        no message is silently skipped. Its `__cause__` is the error that ended the last attempt
        (for example a pika connection error, `UnroutableError`, or `NackError`), and its
        `message` is the message that was not injected. Errors raised by `generate()` or by
        serializing the message, and the `TypeError` for a message of another class than the
        declared one, are logged at ERROR and propagate unchanged. An attempt never waits longer
        than `blocked_timeout_seconds` (60 by default) on a connection that a RabbitMQ memory or
        disk alarm blocks: it then fails with `ConnectionBlockedTimeout` and is retried, so an
        injection cannot hang during an alarm.

        A retried attempt publishes the message again, and RabbitMQ may already have accepted
        the earlier one, so it can arrive twice; the consuming agents must be idempotent.
        """
        if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int | float):
            raise TypeError("interval_seconds must be a number")
        if not math.isfinite(interval_seconds):
            raise ValueError("interval_seconds must be a finite number")
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
        started_at = _monotonic()
        try:
            message = self.generate()
        except Exception:
            logger.exception(
                "%s.generate() failed exchange=%s", type(self).__name__, self._exchange_name
            )
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
            self._log_injected(injection)  # its total includes closing the connection
