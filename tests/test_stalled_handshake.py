"""A server that accepts TCP connections but never completes the AMQP handshake.

pika gives up on such a server with `AMQPConnectorStackTimeout`, after its `stack_timeout` (15
seconds by default). The tests shorten that timeout, run a real `BlockingConnection` against a
local server that never answers, and check that agents retry as after any lost connection,
like a broker that is still starting or overloaded, or a proxy in front of one that is down.
"""

from __future__ import annotations

import dataclasses
import functools
import socket
import threading
from collections.abc import Iterator

import pika
import pytest
from pika.adapters.utils.connection_workflow import AMQPConnectorStackTimeout

from pykicak.abstracts import KicakAbstract
from pykicak.config import KicakConfig
from pykicak.executor import ExecutorStatus, KicakExecutorAbstract
from pykicak.injector import InjectionError, KicakInjectorAbstract
from pykicak.messages import KicakMessage
from tests.fakes import CONNECTION_VALUES


class SilentServer:
    """A TCP server on 127.0.0.1 that accepts connections and never sends a byte."""

    def __init__(self) -> None:
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(0.05)
        self.port: int = self._listener.getsockname()[1]
        self._connections: list[socket.socket] = []
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stopped.is_set():
            try:
                connection, _address = self._listener.accept()
            except TimeoutError:
                continue
            with self._lock:
                self._connections.append(connection)  # kept open, and never answered

    @property
    def accepted(self) -> int:
        with self._lock:
            return len(self._connections)

    def close(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=5)
        with self._lock:
            for connection in self._connections:
                connection.close()
        self._listener.close()


@pytest.fixture
def server() -> Iterator[SilentServer]:
    silent_server = SilentServer()
    yield silent_server
    silent_server.close()


@pytest.fixture
def short_stack_timeout(monkeypatch) -> None:
    """Make every connection give up after a second, instead of pika's 15-second default."""
    monkeypatch.setattr(
        pika,
        "ConnectionParameters",
        functools.partial(pika.ConnectionParameters, stack_timeout=1, socket_timeout=1),
    )


def config_for(server: SilentServer) -> KicakConfig:
    return KicakConfig(
        values={
            **CONNECTION_VALUES,
            "RABBIT_MQ_HOST": "127.0.0.1",
            "RABBIT_MQ_PORT": str(server.port),
        }
    )


@dataclasses.dataclass(frozen=True, slots=True)
class StallMessage(KicakMessage):
    kicak_type = "stall.v1"


class StallExecutor(KicakExecutorAbstract[StallMessage, None]):
    def __init__(self, config: KicakConfig) -> None:
        super().__init__(
            config, source_queue="input-queue", source_exchange="input-exchange", max_retry_rounds=0
        )

    def execute(self, message: StallMessage) -> None:
        return None


class StallInjector(KicakInjectorAbstract[StallMessage]):
    def __init__(self, config: KicakConfig) -> None:
        super().__init__(config, exchange_name="input-exchange")

    def generate(self) -> StallMessage:
        return StallMessage(message_id="message-1")


def test_pika_raises_a_connector_timeout_that_agents_retry(server, short_stack_timeout):
    parameters = pika.ConnectionParameters(host="127.0.0.1", port=server.port)

    with pytest.raises(AMQPConnectorStackTimeout) as exc_info:
        pika.BlockingConnection(parameters)

    assert KicakAbstract._is_retryable_rabbitmq_error(exc_info.value) is True


def test_injector_makes_every_attempt_against_a_server_that_never_answers(
    server, short_stack_timeout, monkeypatch
):
    monkeypatch.setattr("pykicak.injector.time.sleep", lambda _seconds: None)

    with pytest.raises(InjectionError) as exc_info:
        StallInjector(config_for(server)).run()

    assert isinstance(exc_info.value.__cause__, AMQPConnectorStackTimeout)
    assert server.accepted == 3


def test_executor_reconnects_instead_of_crashing_against_a_server_that_never_answers(
    server, short_stack_timeout, monkeypatch
):
    executor = StallExecutor(config_for(server))
    statuses: list[ExecutorStatus] = []

    def stop_instead_of_waiting(_seconds: float) -> bool:
        statuses.append(executor.get_info().status)
        executor.stop()
        return False

    monkeypatch.setattr(executor, "_sleep_unless_stopped", stop_instead_of_waiting)

    executor.run()

    assert statuses == [ExecutorStatus.UPSTREAM_RECOVER]
    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert server.accepted == 1
