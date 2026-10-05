import pytest
from pika.exceptions import StreamLostError

from pykicak.abstracts import KicakAbstract
from pykicak.config import KicakConfig
from tests.fakes import FakeChannel, FakeConnection


class ConcreteAgent(KicakAbstract):
    def __init__(self, config: KicakConfig, channel: FakeChannel) -> None:
        super().__init__(config)
        self._fake_channel = channel
        self.topology_declared = False

    def _create_connection(self):
        return FakeConnection(self._fake_channel)

    def _declare_topology(self) -> None:
        self.topology_declared = True

    def _retry_rabbitmq_operation(self, operation, operation_name):
        return operation()

    def run(self) -> None:
        pass


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values={})


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
    agent = ConcreteAgent(config, FakeChannel())
    agent._create_connection = lambda: FailingCloseConnection(agent._fake_channel)
    agent.connect()

    with pytest.raises(StreamLostError):
        agent.close()

    with pytest.raises(RuntimeError, match="Not connected"):
        _ = agent.channel


def test_context_manager_raises_close_failure_when_block_succeeds(config):
    agent = ConcreteAgent(config, FakeChannel())
    agent._create_connection = lambda: FailingCloseConnection(agent._fake_channel)

    with pytest.raises(StreamLostError):
        with agent:
            pass


def test_context_manager_close_failure_does_not_replace_the_block_error(config, caplog):
    agent = ConcreteAgent(config, FakeChannel())
    agent._create_connection = lambda: FailingCloseConnection(agent._fake_channel)

    with pytest.raises(ZeroDivisionError):
        with agent:
            raise ZeroDivisionError("error inside the block")

    assert "Failed to close RabbitMQ connection" in caplog.text
