"""Internal base class shared by the Injector and Executor; not part of the public API."""

from __future__ import annotations

import abc
import logging
import os
import socket
import ssl
import types
import typing
from collections.abc import Callable
from datetime import UTC, datetime
from types import NoneType, TracebackType
from typing import Self, TypeVar

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    ChannelWrongStateError,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
)

from pykicak.config import KicakConfig, KicakConfigError
from pykicak.messages import KicakMessage

logger = logging.getLogger(__name__)

_RETRYABLE_RABBITMQ_ERRORS = (
    AMQPConnectionError,
    ChannelWrongStateError,
    OSError,
    TimeoutError,
)
_MAX_HEARTBEAT_SECONDS = 65535  # AMQP 0-9-1 encodes the heartbeat timeout in 16 bits
_MAX_NAME_BYTES = 255  # AMQP carries queue and exchange names as short strings
_RESERVED_NAME_PREFIX = "amq."
_ORIGINAL_PUBLISHER_HEADER = "x-kicak-original-publisher"
"""Header naming the user of the agent that created a message; see `_basic_publish()`."""
_PUBLISHED_AT_HEADER = "x-kicak-published-at"
"""Header with the time a message was published, ISO 8601 in UTC; see `_basic_publish()`."""


def _now() -> datetime:
    """The current time, timezone-aware in UTC."""
    return datetime.now(UTC)


def _check_name(argument: str, name: object, *, suffix: str = "") -> None:
    """Raise unless `name` can name a queue or exchange of the application's topology.

    It must be a non-empty `str` that fits AMQP's limit of 255 UTF-8 bytes, together with
    `suffix`, the longest suffix the library appends to it to name another queue or exchange.
    It must not start with `amq.`: RabbitMQ refuses to declare such a queue or exchange
    (`403 ACCESS_REFUSED`), and its own `amq.*` exchanges are shared by the whole virtual host.
    `argument` names the argument in the error.
    """
    if not isinstance(name, str):
        raise TypeError(f"{argument} must be a str, got {type(name).__name__}")
    if not name:
        raise ValueError(f"{argument} must be a non-empty name")
    max_bytes = _MAX_NAME_BYTES - len(suffix.encode("utf-8"))
    if len(name.encode("utf-8")) > max_bytes:
        room = f", leaving room for the suffix {suffix!r}" if suffix else ""
        raise ValueError(f"{argument} must be at most {max_bytes} bytes in UTF-8{room}")
    if name.startswith(_RESERVED_NAME_PREFIX):
        raise ValueError(
            f"{argument} must not start with {_RESERVED_NAME_PREFIX!r}, which RabbitMQ reserves"
        )


def _type_arguments(agent_class: type, generic_class: type, *, usage: str) -> tuple[object, ...]:
    """Return the type arguments that `agent_class` gives `generic_class` in its class header.

    They are found through the class's bases, so a subclass of a concrete agent class inherits
    them, and a generic class in between, e.g. `class Base[M](KicakExecutorAbstract[M, Summary])`,
    passes on the arguments its subclass gives it. Raises `TypeError`, showing `usage`, if
    `agent_class` gives no concrete arguments, e.g. a subclass of the unparameterized class.
    """
    arguments = _type_arguments_or_none(agent_class, generic_class)
    if arguments is None or any(_has_type_parameter(argument) for argument in arguments):
        raise TypeError(
            f"{agent_class.__name__} must declare its message types in its class header: {usage}"
        )
    return arguments


def _type_arguments_or_none(agent_class: type, generic_class: type) -> tuple[object, ...] | None:
    """Return the type arguments of `generic_class` in `agent_class`'s bases, or None."""
    for base in types.get_original_bases(agent_class):
        origin = typing.get_origin(base) or base
        if origin is generic_class:
            return typing.get_args(base) or None  # none if the class itself is the base
        if not (isinstance(origin, type) and issubclass(origin, generic_class)):
            continue
        arguments = _type_arguments_or_none(origin, generic_class)
        if arguments is not None:
            # A generic class in between: replace its type parameters by what this base gives. A
            # base without arguments gives none, leaving them open for _type_arguments() to report.
            parameters = getattr(origin, "__parameters__", ())
            given = dict(zip(parameters, typing.get_args(base), strict=False))
            return tuple(_substituted(argument, given) for argument in arguments)
    return None


def _substituted(argument: object, given: dict[object, object]) -> object:
    """Return `argument` with each type parameter in it replaced by its value in `given`."""
    if isinstance(argument, TypeVar):
        return given.get(argument, argument)
    parameters = getattr(argument, "__parameters__", ())
    if not parameters:
        return argument
    # A union or generic alias that holds type parameters, e.g. `M | Other`
    return typing.cast(typing.Any, argument)[tuple(given.get(name, name) for name in parameters)]


def _has_type_parameter(argument: object) -> bool:
    """Return whether a type argument is, or holds, a type parameter left open."""
    return isinstance(argument, TypeVar) or bool(getattr(argument, "__parameters__", ()))


def _union_members(argument: object) -> tuple[object, ...]:
    """Return the members of a union type argument, or the argument alone.

    Aliases made with the `type` statement, e.g. `type Chunk = ChunkV1 | ChunkV2`, are replaced
    by what they stand for, also when they are members of a union themselves.
    """
    argument = _unaliased(argument)
    if typing.get_origin(argument) in (types.UnionType, typing.Union):
        return tuple(
            member for each in typing.get_args(argument) for member in _union_members(each)
        )
    return (argument,)


def _unaliased(argument: object) -> object:
    """Return the type that an alias made with the `type` statement stands for, or `argument`.

    A generic alias given its arguments, e.g. `Pair[Chunk]` for `type Pair[T] = T | Other`, stands
    for its value with those arguments in place of its type parameters. An alias of None stands
    for `NoneType`, as None does in a class header.
    """
    while True:
        if isinstance(argument, typing.TypeAliasType):
            argument = argument.__value__
            continue
        origin = typing.get_origin(argument)
        if not isinstance(origin, typing.TypeAliasType):
            return NoneType if argument is None else argument
        given: dict[object, object] = dict(
            zip(origin.__type_params__, typing.get_args(argument), strict=False)
        )
        argument = _substituted(origin.__value__, given)


def _tls_options(config: KicakConfig) -> pika.SSLOptions | None:
    """Return pika's TLS options for a connection configured by `config`, or None without TLS.

    Raises `KicakConfigError` for a RABBIT_MQ_CA_FILE given with RABBIT_MQ_TLS=false: the file
    shows that TLS was intended, so connecting without it would send the credentials and messages
    unencrypted.
    """
    ca_file = config.rabbitmq_ca_file
    if not config.rabbitmq_tls:
        if ca_file is not None:
            logger.error("Invalid Kicak config: RABBIT_MQ_CA_FILE requires RABBIT_MQ_TLS=true")
            raise KicakConfigError("RABBIT_MQ_CA_FILE requires RABBIT_MQ_TLS=true")
        return None
    # The host name is also what the broker's certificate must name
    return pika.SSLOptions(_tls_context(ca_file), server_hostname=config.rabbitmq_host)


def _tls_context(ca_file: str | None) -> ssl.SSLContext:
    """Return a client TLS context that verifies the broker's certificate and host name.

    `ssl.create_default_context()` applies Python's recommended client settings: the certificate
    must be valid and issued for the host name, and only secure ciphers are used. Two settings
    are made explicit, so that a certificate is accepted or refused alike on every supported Python
    version: TLS 1.2 or newer, because Python's default minimum depends on how it and OpenSSL were
    built, and the strict certificate checks that Python 3.13 and newer apply by default
    (`VERIFY_X509_STRICT`, with `VERIFY_X509_PARTIAL_CHAIN`, which lets an intermediate CA be
    trusted on its own), because Python 3.12 does not.

    Without `ca_file` it trusts the system's CA certificates; with it, only the certificates in
    the file. A file that cannot be read or holds no PEM certificate raises `KicakConfigError`.
    """
    context = ssl.create_default_context() if ca_file is None else _context_trusting(ca_file)
    if context.minimum_version < ssl.TLSVersion.TLSv1_2:  # e.g. MINIMUM_SUPPORTED, from OpenSSL
        context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_flags |= ssl.VERIFY_X509_STRICT | ssl.VERIFY_X509_PARTIAL_CHAIN
    return context


def _context_trusting(ca_file: str) -> ssl.SSLContext:
    """Return Python's default client TLS context, trusting only the CAs in `ca_file`."""
    try:
        return ssl.create_default_context(cafile=ca_file)
    except ssl.SSLError:  # a subclass of OSError, so it must come first
        problem = "does not contain any PEM CA certificate"
    except OSError as error:
        problem = f"cannot be read: {error.strerror}"
    # Raised outside the except blocks, so the error carries no context naming the file
    logger.error("Invalid Kicak config key RABBIT_MQ_CA_FILE: the file %s", problem)
    raise KicakConfigError(f"RABBIT_MQ_CA_FILE {problem}")


class KicakAbstract(abc.ABC):
    """Internal base class of `KicakInjectorAbstract` and `KicakExecutorAbstract`.

    Opens a pika BlockingConnection using credentials from a KicakConfig,
    and provides the declare/close lifecycle shared by all agent kinds.
    The channel it opens is put into publisher-confirm mode; the Executor publishes its results
    on a separate, transactional channel.

    Not exported by `pykicak`: subclasses must implement private hooks and rely on private
    helpers, so applications subclass the Injector or the Executor instead.
    """

    _blocked_connection_timeout_seconds: float | None = None
    """How long a connection may stay blocked by a RabbitMQ resource alarm before pika closes it
    with the retryable `ConnectionBlockedTimeout`; None, the Executor's, waits until RabbitMQ
    unblocks it. The Injector sets it from its `blocked_timeout_seconds` argument."""

    def __init__(self, config: KicakConfig, *, heartbeat_seconds: int | None = None) -> None:
        """Store the config for later use. Does not open any connection.

        `heartbeat_seconds` is the AMQP heartbeat timeout requested for every connection, from 1
        to 65535. RabbitMQ uses the requested value; None accepts the broker's proposal (60
        seconds by default).
        """
        if heartbeat_seconds is not None:
            if isinstance(heartbeat_seconds, bool) or not isinstance(heartbeat_seconds, int):
                raise TypeError("heartbeat_seconds must be an int or None")
            if not 1 <= heartbeat_seconds <= _MAX_HEARTBEAT_SECONDS:
                raise ValueError(f"heartbeat_seconds must be from 1 to {_MAX_HEARTBEAT_SECONDS}")
        self._config = config
        self._heartbeat_seconds = heartbeat_seconds
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        # The user every connection authenticates as, sent as the user_id of every publish
        self._user_id: str | None = None

    @property
    def channel(self) -> BlockingChannel:
        """The open channel.

        Raises RuntimeError when not connected: before connect() or after close().
        """
        if self._channel is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._channel

    def _create_connection(self) -> pika.BlockingConnection:
        """Build a BlockingConnection from the stored config's RabbitMQ settings.

        With RABBIT_MQ_TLS=true the connection uses TLS and verifies the broker's certificate,
        so the credentials and messages are encrypted and sent only to the intended broker.
        """
        config = self._config
        credentials = pika.PlainCredentials(config.rabbitmq_username, config.rabbitmq_password)
        parameters = pika.ConnectionParameters(
            host=config.rabbitmq_host,
            port=config.rabbitmq_port,
            virtual_host=config.rabbitmq_virtual_host,
            credentials=credentials,
            ssl_options=_tls_options(config),
            heartbeat=self._heartbeat_seconds,
            blocked_connection_timeout=self._blocked_connection_timeout_seconds,
            client_properties={"connection_name": self._connection_name()},
        )
        return pika.BlockingConnection(parameters)

    def _connection_name(self) -> str:
        """Return the name RabbitMQ shows for this agent's connection, e.g. in its management UI.

        It names the agent's role, the queue or exchange it serves, and the host and process it
        runs in, e.g. `executor queue=documents host=worker-3 pid=4242`, so that each instance of
        a scaled Executor can be told apart. Computed for every connection, so a forked process
        reports its own pid.
        """
        return f"{self._connection_label()} host={socket.gethostname()} pid={os.getpid()}"

    @abc.abstractmethod
    def _connection_label(self) -> str:
        """Return this agent's role and the queue or exchange it serves, e.g. `executor queue=q`."""

    def connect(self) -> None:
        """Open the connection/channel and declare this agent's topology."""
        if (
            self._connection is not None
            and self._connection.is_open
            and self._channel is not None
            and self._channel.is_open
        ):
            logger.debug("RabbitMQ connection and channel are already open")
            return
        logger.debug("Starting RabbitMQ connection and topology setup")
        self._retry_rabbitmq_operation(
            self._establish_connection, "RabbitMQ connection or topology setup"
        )

    def _establish_connection(self) -> None:
        """Establish one connection and declare topology without retrying."""
        logger.debug("Connecting to RabbitMQ")
        try:
            self._connection = self._create_connection()
            self._user_id = self._config.rabbitmq_username
            self._channel = self._connection.channel()
            logger.debug("RabbitMQ connection and channel established")
            self._channel.confirm_delivery()
            logger.debug("RabbitMQ publisher confirms enabled")
            self._declare_topology()
            logger.debug("RabbitMQ topology setup completed")
        except Exception:
            self._discard_connection()
            raise

    def _ensure_connection_once(self) -> None:
        """Establish a single connection attempt when the current channel is unavailable."""
        if (
            self._connection is None
            or not self._connection.is_open
            or self._channel is None
            or not self._channel.is_open
        ):
            self._establish_connection()

    @abc.abstractmethod
    def _retry_rabbitmq_operation[T](self, operation: Callable[[], T], operation_name: str) -> T:
        """Apply this agent's retry policy to a RabbitMQ operation."""

    @staticmethod
    def _is_retryable_rabbitmq_error(error: Exception) -> bool:
        """Return whether an error is likely to clear after reconnecting.

        Refused credentials and a broker certificate that fails verification are not: the
        configuration is wrong, or the server is not the intended broker. Other TLS errors, e.g.
        a handshake cut off, are connection errors like any other.
        """
        if isinstance(
            error,
            (
                ProbableAuthenticationError,
                ProbableAccessDeniedError,
                ssl.SSLCertVerificationError,
            ),
        ):
            return False
        return isinstance(error, _RETRYABLE_RABBITMQ_ERRORS)

    def _discard_connection(self) -> None:
        """Forget and best-effort close the current connection; never raises.

        Used before retrying after a broken connection, and by the Injector after each injection.
        """
        channel, connection = self._channel, self._connection
        self._channel = None
        self._connection = None
        for resource in (channel, connection):
            if resource is not None and resource.is_open:
                try:
                    resource.close()
                except Exception:
                    logger.debug("Error while discarding a RabbitMQ resource", exc_info=True)

    def _publish_serialized(self, exchange_name: str, message: KicakMessage, body: bytes) -> None:
        """Publish `message`, already serialized as `body`, once on the current channel.

        The message's `message_id` is also sent as the AMQP `message_id` property.

        The channel is in publisher-confirm mode and the message is mandatory, so this returns
        only after RabbitMQ has taken responsibility for the message. It raises
        `pika.exceptions.UnroutableError` if no queue is bound to the exchange,
        `pika.exceptions.NackError` if RabbitMQ refuses the message, and connection errors if the
        connection is lost. Nothing is retried or reconnected here.
        """
        self._basic_publish(self.channel, exchange_name, message, body)
        self._log_published(exchange_name, message)

    def _basic_publish(
        self, channel: BlockingChannel, exchange_name: str, message: KicakMessage, body: bytes
    ) -> None:
        """Send `message`, serialized as `body`, as a persistent, mandatory JSON message.

        The message's `message_id` is also sent as the AMQP `message_id` property, and its class's
        `kicak_type` as the AMQP `type` property, from which an Executor learns which of the
        types it accepts to decode the body into. The caller has checked that the message's
        class is the agent's declared one, whose `kicak_type` is validated. This agent
        created the message, an Injector's message or an Executor's result, so it names itself
        twice: in `user_id`, the user its connection authenticated as, which RabbitMQ verifies,
        and in the `x-kicak-original-publisher` header, which a copy moved to a retry queue keeps
        while its `user_id` changes. The `x-kicak-published-at` header records when it was
        published, so the Executor that receives it can tell how long it waited in its queue;
        a copy moved to a retry queue is stamped again. What this waits for depends on the
        channel: in confirm mode, RabbitMQ's confirmation; on a transactional channel, nothing,
        because RabbitMQ delivers nothing before `tx_commit()`.
        """
        logger.debug(
            "Publishing message type=%s id=%s exchange=%s routing_key='' body_bytes=%d",
            type(message).__name__,
            message.message_id,
            exchange_name,
            len(body),
        )
        channel.basic_publish(
            exchange=exchange_name,
            routing_key="",
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=message.message_id,
                type=type(message).kicak_type,
                user_id=self._user_id,
                headers={
                    _ORIGINAL_PUBLISHER_HEADER: self._user_id,
                    _PUBLISHED_AT_HEADER: _now().isoformat(),
                },
            ),
            mandatory=True,
        )

    @staticmethod
    def _log_published(exchange_name: str, message: KicakMessage) -> None:
        """Log that RabbitMQ has taken responsibility for `message`."""
        logger.info(
            "Message published type=%s id=%s exchange=%s",
            type(message).__name__,
            message.message_id,
            exchange_name,
        )

    def close(self) -> None:
        """
        Close the RabbitMQ channel and connection if open.

        The agent forgets both before closing them, so afterwards it is disconnected even if
        closing fails: `channel` raises `RuntimeError` again, and `connect()` opens a new
        connection.

        The channel is closed first. Failure to close the channel is logged but
        does not prevent the connection from being closed and does not propagate
        the channel exception.

        The connection is closed in a ``finally`` block, regardless of whether
        closing the channel succeeds. Failure to close the connection is logged
        and propagated to the caller.

        Therefore:
            - channel closes, connection closes --> no exception
            - channel fails, connection closes --> no exception
            - channel closes, connection fails --> connection exception raised
            - channel fails, connection fails --> connection exception raised
            - channel or connection not declared --> skipped
        """
        logger.debug("Disconnecting from RabbitMQ")

        channel, connection = self._channel, self._connection
        self._channel = None
        self._connection = None

        try:
            if channel is not None and channel.is_open:
                try:
                    channel.close()
                    logger.debug("RabbitMQ channel closed")
                except Exception:
                    logger.exception("Failed to close RabbitMQ channel")
        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                    logger.debug("RabbitMQ connection closed")
                except Exception:
                    logger.exception("Failed to close RabbitMQ connection")
                    raise

    def _close_without_masking(self) -> None:
        """Close, logging a failure to close instead of raising it.

        Used while another exception propagates, which a close failure must not replace, and at
        the end of an Executor's graceful stop. `close()` has already logged the failure.
        """
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self) -> Self:
        """Connect on entering a `with` block."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection on leaving a `with` block.

        If the block raised, a failure to close is logged but not raised, so it does not replace
        the block's exception.
        """
        if exc_value is None:
            self.close()
        else:
            self._close_without_masking()

    @abc.abstractmethod
    def _declare_topology(self) -> None:
        """Idempotently declare this agent's queues/exchanges, on every new connection."""

    @abc.abstractmethod
    def run(self) -> None:
        """Run this agent's main loop."""
