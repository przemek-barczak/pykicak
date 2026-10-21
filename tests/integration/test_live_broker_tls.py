"""End-to-end TLS tests against a real RabbitMQ broker with a TLS listener.

Requires the .kicak file named by KICAK_TLS_CONFIG_PATH, with RABBIT_MQ_TLS=true and
RABBIT_MQ_CA_FILE pointing to the CA that issued the broker's certificate; without the variable
these tests are skipped. `python -m tests.integration.tls_broker_files <directory>` writes a test
CA, a broker certificate for `localhost`, and the broker's TLS configuration; see its docstring
and the integration job of .github/workflows/ci.yml for running the broker with them.

Run them with the other integration tests: `pytest -m integration`.
"""

from __future__ import annotations

import os
import ssl
from collections.abc import Iterator

import pika
import pytest
import trustme

from pykicak.config import KicakConfig
from pykicak.executor import ExecutorStatus
from tests.integration.test_live_broker import (
    IntegrationMessage,
    OneShotInjector,
    RecordingTerminator,
    TemporaryTopology,
    _delete_topology,
    _open_connection,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def tls_config() -> KicakConfig:
    """Load the TLS broker config, failing fast if the broker is unreachable over TLS."""
    path = os.environ.get("KICAK_TLS_CONFIG_PATH")
    if not path:
        pytest.skip("KICAK_TLS_CONFIG_PATH is not set: no TLS broker to test against")
    config = KicakConfig.from_file(path)
    assert config.rabbitmq_tls, f"{path} must set RABBIT_MQ_TLS=true"
    try:
        _open_connection(config).close()
    except pika.exceptions.AMQPConnectionError as error:
        pytest.fail(f"RabbitMQ TLS listener is not reachable: {error!r}")
    return config


@pytest.fixture
def tls_topology(tls_config) -> Iterator[TemporaryTopology]:
    names = TemporaryTopology()
    yield names
    _delete_topology(tls_config, names)


@pytest.fixture
def no_retry_waits(monkeypatch) -> None:
    """Fail the test if the executor waits to retry: a refused certificate must not be retried."""

    def fail(_seconds: float) -> None:
        raise AssertionError("the connection was retried")

    monkeypatch.setattr("pykicak.executor.time.sleep", fail)


def test_injector_and_executor_exchange_a_message_over_tls(tls_config, tls_topology):
    inject_exchange = tls_topology.exchange("inject")
    names = tls_topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id="over-tls", payload="encrypted")

    with RecordingTerminator(tls_config, **names) as executor:
        with OneShotInjector(tls_config, inject_exchange, message) as injector:
            injector.publish(message)
        executor.handle_next_delivery(names["source_queue"])

    assert executor.received == [message]


def test_executor_stops_when_the_broker_certificate_is_from_another_ca(
    tls_config, tmp_path, no_retry_waits
):
    other_ca_file = tmp_path / "other-ca.pem"
    trustme.CA().cert_pem.write_to_path(str(other_ca_file))
    config = KicakConfig(values={**tls_config.values, "RABBIT_MQ_CA_FILE": str(other_ca_file)})
    executor = RecordingTerminator(
        config, source_queue="pykicak-test.never-declared", source_exchange="unused"
    )

    with pytest.raises(ssl.SSLCertVerificationError):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED


def test_executor_stops_when_the_broker_certificate_names_another_host(tls_config, no_retry_waits):
    if tls_config.rabbitmq_host != "localhost":
        pytest.skip("needs the broker certificate of tls_broker_files, issued for localhost")
    config = KicakConfig(values={**tls_config.values, "RABBIT_MQ_HOST": "127.0.0.1"})
    executor = RecordingTerminator(
        config, source_queue="pykicak-test.never-declared", source_exchange="unused"
    )

    with pytest.raises(ssl.SSLCertVerificationError, match="127.0.0.1"):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
