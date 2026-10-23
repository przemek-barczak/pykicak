"""End-to-end TLS tests against a real RabbitMQ broker with a TLS listener.

Requires the .kicak file named by KICAK_TLS_CONFIG_PATH, with RABBIT_MQ_TLS=true and
RABBIT_MQ_CA_FILE pointing to the CA that issued the broker's certificate; without the variable
these tests are skipped. The client certificate tests also need RABBIT_MQ_CERT_FILE and
RABBIT_MQ_KEY_FILE in it, and a broker that verifies the certificates clients present but also
accepts clients without one; without the keys they are skipped. `python -m
tests.integration.tls_broker_files <directory>` writes a test CA, a broker certificate for
`localhost`, a client certificate, and the broker's TLS configuration; see its docstring and the
integration job of .github/workflows/ci.yml for running the broker with them.

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
from pykicak.injector import InjectionError
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


_CLIENT_CERTIFICATE_KEYS = ("RABBIT_MQ_CERT_FILE", "RABBIT_MQ_KEY_FILE")


def without_client_certificate(config: KicakConfig) -> KicakConfig:
    values = dict(config.values)
    for key in _CLIENT_CERTIFICATE_KEYS:
        values.pop(key, None)
    return KicakConfig(values=values)


@pytest.fixture
def mutual_tls_config(tls_config: KicakConfig) -> KicakConfig:
    """The TLS broker config with its client certificate, for a broker that verifies it."""
    if tls_config.rabbitmq_cert_file is None:
        pytest.skip(
            "KICAK_TLS_CONFIG_PATH has no RABBIT_MQ_CERT_FILE: no client certificate to test with"
        )
    return tls_config


def _exchange_a_message(config: KicakConfig, topology: TemporaryTopology, message_id: str) -> None:
    inject_exchange = topology.exchange("inject")
    names = topology.executor_topology("source", inject_exchange)
    message = IntegrationMessage(message_id=message_id, payload="encrypted")

    with RecordingTerminator(config, **names) as executor:
        with OneShotInjector(config, inject_exchange, message) as injector:
            injector.publish(message)
        executor.handle_next_delivery(names["source_queue"])

    assert executor.received == [message]


def test_injector_and_executor_exchange_a_message_over_tls(tls_config, tls_topology):
    _exchange_a_message(without_client_certificate(tls_config), tls_topology, "over-tls")


def test_injector_and_executor_exchange_a_message_with_a_client_certificate(
    mutual_tls_config, tls_topology
):
    _exchange_a_message(mutual_tls_config, tls_topology, "over-mutual-tls")


def test_injector_fails_when_the_broker_refuses_its_client_certificate(
    mutual_tls_config, tmp_path, monkeypatch
):
    # Issued by a CA the broker does not trust; refused like this, retried like a lost connection
    monkeypatch.setattr("pykicak.injector.time.sleep", lambda _seconds: None)
    other_certificate = trustme.CA().issue_cert("agent.pykicak.test")
    cert_file, key_file = tmp_path / "other-client.pem", tmp_path / "other-client.key"
    other_certificate.cert_chain_pems[0].write_to_path(str(cert_file))
    other_certificate.private_key_pem.write_to_path(str(key_file))
    config = KicakConfig(
        values={
            **mutual_tls_config.values,
            "RABBIT_MQ_CERT_FILE": str(cert_file),
            "RABBIT_MQ_KEY_FILE": str(key_file),
        }
    )
    message = IntegrationMessage(message_id="refused-client", payload="never sent")

    with pytest.raises(InjectionError) as exc_info:
        OneShotInjector(config, "pykicak-test.never-declared", message).run()

    assert "unknown ca" in str(exc_info.value.__cause__).lower()


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
