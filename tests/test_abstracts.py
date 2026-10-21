import pytest
from pika.exceptions import StreamLostError

from pykicak.abstracts import KicakAbstract
from pykicak.config import KicakConfig
from tests.fakes import (
    CONNECTION_VALUES,
    FakeChannel,
    FakeConnection,
    captured_connection_parameters,
)


class ConcreteAgent(KicakAbstract):
    def __init__(
        self,
        config: KicakConfig,
        channel: FakeChannel,
        connection_class: type[FakeConnection] = FakeConnection,
    ) -> None:
        super().__init__(config)
        self._fake_channel = channel
        self._connection_class = connection_class
        self.topology_declared = False

    def _create_connection(self):
        return self._connection_class(self._fake_channel)

    def _connection_label(self) -> str:
        return "agent exchange=test-exchange"

    def _declare_topology(self) -> None:
        self.topology_declared = True

    def _retry_rabbitmq_operation(self, operation, operation_name):
        return operation()

    def run(self) -> None:
        pass


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values=CONNECTION_VALUES)


def test_channel_raises_before_connect(config):
    agent = ConcreteAgent(config, FakeChannel())

    with pytest.raises(RuntimeError):
        _ = agent.channel


def test_connect_opens_channel_and_declares_topology(config):
    channel = FakeChannel()
    agent = ConcreteAgent(config, channel)

    agent.connect()

    assert agent.channel is channel
    assert agent.topology_declared is True


def test_connect_enables_publisher_confirms(config):
    channel = FakeChannel()
    agent = ConcreteAgent(config, channel)

    agent.connect()

    assert channel.confirm_delivery_enabled is True


def test_connect_is_idempotent(config):
    channel = FakeChannel()
    agent = ConcreteAgent(config, channel)

    agent.connect()
    agent.topology_declared = False
    agent.connect()

    assert agent.topology_declared is False


def test_close_closes_channel_and_connection(config):
    channel = FakeChannel()
    agent = ConcreteAgent(config, channel)
    agent.connect()

    agent.close()

    assert channel.is_open is False


def test_close_is_safe_before_connect(config):
    agent = ConcreteAgent(config, FakeChannel())

    agent.close()


def test_context_manager_connects_and_closes(config):
    channel = FakeChannel()

    with ConcreteAgent(config, channel) as agent:
        assert agent.topology_declared is True
        assert channel.is_open is True

    assert channel.is_open is False


class FailingCloseConnection(FakeConnection):
    def close(self) -> None:
        raise StreamLostError("connection close failed")


class FailingCloseChannel(FakeChannel):
    def close(self) -> None:
        raise StreamLostError("channel close failed")


def test_close_logs_channel_close_failure_and_still_closes_the_connection(config, caplog):
    agent = ConcreteAgent(config, FailingCloseChannel())
    agent.connect()
    connection = agent._connection
    assert connection is not None

    agent.close()  # does not raise

    assert connection.is_open is False
    errors = [r for r in caplog.records if "Failed to close RabbitMQ channel" in r.message]
    assert [record.levelname for record in errors] == ["ERROR"]
    with pytest.raises(RuntimeError, match="Not connected"):
        _ = agent.channel


def test_discard_connection_still_closes_the_connection_when_closing_the_channel_fails(config):
    agent = ConcreteAgent(config, FailingCloseChannel())
    agent.connect()
    connection = agent._connection
    assert connection is not None

    agent._discard_connection()  # does not raise

    assert connection.is_open is False
    assert agent._connection is None
    assert agent._channel is None


def test_discard_connection_never_raises_when_closing_fails(config, caplog):
    agent = ConcreteAgent(config, FailingCloseChannel(), FailingCloseConnection)
    agent.connect()
    caplog.set_level("DEBUG", logger="pykicak.abstracts")

    agent._discard_connection()  # does not raise

    with pytest.raises(RuntimeError, match="Not connected"):
        _ = agent.channel
    failures = [r for r in caplog.records if "Error while discarding" in r.message]
    assert [record.levelname for record in failures] == ["DEBUG", "DEBUG"]


@pytest.mark.parametrize("heartbeat_seconds", [1.5, "600", True])
def test_init_rejects_non_integer_heartbeat(config, heartbeat_seconds):
    agent = ConcreteAgent(config, FakeChannel())

    with pytest.raises(TypeError, match="heartbeat_seconds must be an int or None"):
        KicakAbstract.__init__(agent, config, heartbeat_seconds=heartbeat_seconds)


def test_connection_is_named_by_role_topology_host_and_pid(config, monkeypatch):
    monkeypatch.setattr("pykicak.abstracts.socket.gethostname", lambda: "worker-3")
    monkeypatch.setattr("pykicak.abstracts.os.getpid", lambda: 4242)
    agent = ConcreteAgent(config, FakeChannel())

    parameters = captured_connection_parameters(agent, monkeypatch)

    assert parameters.client_properties == {
        "connection_name": "agent exchange=test-exchange host=worker-3 pid=4242"
    }


def test_close_disconnects_the_agent(config):
    agent = ConcreteAgent(config, FakeChannel())
    agent.connect()

    agent.close()

    with pytest.raises(RuntimeError, match="Not connected"):
        _ = agent.channel


def test_connect_after_close_opens_a_new_connection(config):
    agent = ConcreteAgent(config, FakeChannel())
    agent.connect()
    agent.close()
    agent.topology_declared = False
    channel = FakeChannel()
    agent._fake_channel = channel

    agent.connect()

    assert agent.channel is channel
    assert agent.topology_declared is True


def test_close_raises_connection_close_failure_but_still_disconnects(config):
    agent = ConcreteAgent(config, FakeChannel(), FailingCloseConnection)
    agent.connect()

    with pytest.raises(StreamLostError):
        agent.close()

    with pytest.raises(RuntimeError, match="Not connected"):
        _ = agent.channel


def test_context_manager_raises_close_failure_when_block_succeeds(config):
    agent = ConcreteAgent(config, FakeChannel(), FailingCloseConnection)

    with pytest.raises(StreamLostError):
        with agent:
            pass


def test_context_manager_close_failure_does_not_replace_the_block_error(config, caplog):
    agent = ConcreteAgent(config, FakeChannel(), FailingCloseConnection)

    with pytest.raises(ZeroDivisionError):
        with agent:
            raise ZeroDivisionError("error inside the block")

    assert "Failed to close RabbitMQ connection" in caplog.text
