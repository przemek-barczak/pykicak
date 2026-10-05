"""Internal base class shared by the Injector and Executor; not part of the public API."""

from __future__ import annotations

import abc
import logging
from collections.abc import Callable
from types import TracebackType
from typing import Self, TypeVar

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    ChannelWrongStateError,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
)

from pykicak.config import KicakConfig
from pykicak.messages import KicakMessage

logger = logging.getLogger(__name__)

_RETRYABLE_RABBITMQ_ERRORS = (
    AMQPConnectionError,
    ChannelWrongStateError,
    OSError,
    TimeoutError,
)
_MAX_HEARTBEAT_SECONDS = 65535  # AMQP 0-9-1 encodes the heartbeat timeout in 16 bits
_T = TypeVar("_T")


class KicakAbstract(abc.ABC):
    """Internal base class of `KicakInjectorAbstract` and `KicakExecutorAbstract`.

    Opens a pika BlockingConnection using credentials from a KicakConfig,
    and provides the declare/close lifecycle shared by all agent kinds.
    Every channel is put into publisher-confirm mode.

    Not exported by `pykicak`: subclasses must implement private hooks and rely on private
    helpers, so applications subclass the Injector or the Executor instead.
    """

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
                raise ValueError(
                    f"heartbeat_seconds must be from 1 to {_MAX_HEARTBEAT_SECONDS}"
                )
        self._config = config
        self._heartbeat_seconds = heartbeat_seconds
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    @property
    def channel(self) -> BlockingChannel:
        """The open channel.

        Raises RuntimeError when not connected: before connect() or after close().
        """
        if self._channel is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._channel

    def _create_connection(self) -> pika.BlockingConnection:
        """Build a BlockingConnection from the stored config's RabbitMQ credentials."""
        credentials = pika.PlainCredentials(
            self._config.rabbitmq_username, self._config.rabbitmq_password
        )
        parameters = pika.ConnectionParameters(
            host=self._config.rabbitmq_host,
            port=self._config.rabbitmq_port,
            virtual_host=self._config.rabbitmq_virtual_host,
            credentials=credentials,
            heartbeat=self._heartbeat_seconds,
        )
        return pika.BlockingConnection(parameters)

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
    def _retry_rabbitmq_operation(
        self, operation: Callable[[], _T], operation_name: str
    ) -> _T:
        """Apply this agent's retry policy to a RabbitMQ operation."""

    @staticmethod
    def _is_retryable_rabbitmq_error(error: Exception) -> bool:
        """Return whether an error is likely to clear after reconnecting."""
        if isinstance(
            error,
            (
                ProbableAuthenticationError,
                ProbableAccessDeniedError,
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

    def _publish_serialized(
        self, exchange_name: str, message: KicakMessage, body: bytes
    ) -> None:
        """Publish `message`, already serialized as `body`, once on the current channel.

        The message's `message_id` is also sent as the AMQP `message_id` property.

        The channel is in publisher-confirm mode and the message is mandatory, so this returns
        only after RabbitMQ has taken responsibility for the message. It raises
        `pika.exceptions.UnroutableError` if no queue is bound to the exchange,
        `pika.exceptions.NackError` if RabbitMQ refuses the message, and connection errors if the
        connection is lost. Nothing is retried or reconnected here.
        """
        message_id = message.message_id
        message_type = type(message).__name__
        logger.debug(
            "Publishing message type=%s id=%s exchange=%s routing_key='' body_bytes=%d",
            message_type,
            message_id,
            exchange_name,
            len(body),
        )
        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key="",
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json", delivery_mode=2, message_id=message_id
            ),
            mandatory=True,
        )
        logger.info(
            "Message published type=%s id=%s exchange=%s",
            message_type,
            message_id,
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
        """Close while another exception propagates; a close failure must not replace it.

        `close()` has already logged the failure, so it is not raised again.
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
