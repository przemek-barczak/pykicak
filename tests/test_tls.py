"""TLS connections: the parameters built from the config, and certificate checks.

The certificate checks run a real TLS handshake against `TlsServer`, an in-process server that
completes or refuses handshakes and never speaks AMQP, so no broker is needed.
"""

from __future__ import annotations

import dataclasses
import datetime
import ipaddress
import socket
import ssl
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
import trustme
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from pika.exceptions import AMQPConnectionError

from pykicak.abstracts import KicakAbstract
from pykicak.config import KicakConfig, KicakConfigError
from pykicak.executor import ExecutorStatus, KicakExecutorAbstract
from pykicak.injector import InjectionError, KicakInjectorAbstract
from pykicak.messages import KicakMessage
from tests.fakes import CONNECTION_VALUES, captured_connection_parameters


class TlsServer:
    """TLS server on 127.0.0.1 that closes every connection once the TLS handshake is done.

    `handshakes()` returns, for each connection, whether its handshake succeeded.
    """

    def __init__(self, context: ssl.SSLContext) -> None:
        self._context = context
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(0.05)
        self.port: int = self._listener.getsockname()[1]
        self._handshakes: list[bool] = []
        self._client_certificates: list[dict[str, object]] = []
        self._handshake_done = threading.Condition()
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stopped.is_set():
            try:
                connection, _address = self._listener.accept()
            except TimeoutError:
                continue
            with connection:
                connection.settimeout(5)
                client_certificate: dict[str, object] = {}
                try:
                    with self._context.wrap_socket(connection, server_side=True) as tls_connection:
                        succeeded = True
                        client_certificate = dict(tls_connection.getpeercert() or {})
                except OSError:  # ssl.SSLError included: one side refused the other's certificate
                    succeeded = False
                with self._handshake_done:
                    self._handshakes.append(succeeded)
                    if succeeded:
                        self._client_certificates.append(client_certificate)
                    self._handshake_done.notify_all()

    def handshakes(self, count: int) -> list[bool]:
        """Return the handshake results once there are `count`, waiting up to 5 seconds.

        The server records a handshake a moment after the client has finished or given up.
        """
        with self._handshake_done:
            self._handshake_done.wait_for(lambda: len(self._handshakes) >= count, timeout=5)
            return list(self._handshakes)

    def client_certificates(self) -> list[dict[str, object]]:
        """Return the certificate each client presented in a successful handshake, so far.

        Empty for a client that presented none; call `handshakes()` first to wait for them.
        """
        with self._handshake_done:
            return list(self._client_certificates)

    @classmethod
    def issued_by(cls, ca: trustme.CA, *identities: str) -> TlsServer:
        """A server whose certificate `ca` issued for `identities`, sent with its CA chain."""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ca.issue_cert(*identities).configure_cert(context)
        return cls(context)

    @classmethod
    def verifying_clients(cls, ca: trustme.CA, client_ca: trustme.CA) -> TlsServer:
        """A server for 127.0.0.1, its certificate issued by `ca`, that requires every client to
        present a certificate issued by `client_ca` (mutual TLS)."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ca.issue_cert("127.0.0.1").configure_cert(context)
        client_ca.configure_trust(context)
        context.verify_mode = ssl.CERT_REQUIRED
        return cls(context)

    def close(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=5)
        self._listener.close()


def old_style_ca_server(tmp_path) -> tuple[TlsServer, str]:
    """A server for 127.0.0.1 whose CA was made as older tools make one; return its CA file.

    The CA's Basic Constraints extension is not marked critical, which only the strict checks
    (`VERIFY_X509_STRICT`) refuse. Otherwise the certificates are valid.
    """
    now = datetime.datetime.now(datetime.UTC)

    def certificate(subject, issuer, key, signing_key, extensions) -> x509.Certificate:
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
        )
        for extension, critical in extensions:
            builder = builder.add_extension(extension, critical=critical)
        return builder.sign(signing_key, hashes.SHA256())

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "old-style test CA")])
    ca = certificate(
        ca_name,
        ca_name,
        ca_key,
        ca_key,
        [
            (x509.BasicConstraints(ca=True, path_length=None), False),  # not critical
            (x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False),
        ],
    )
    server_key = ec.generate_private_key(ec.SECP256R1())
    server_certificate = certificate(
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]),
        ca_name,
        server_key,
        ca_key,
        [
            (
                x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
                False,
            ),
            (x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False),
        ],
    )
    ca_file = tmp_path / "old-style-ca.pem"
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    certificate_file = tmp_path / "server.pem"
    certificate_file.write_bytes(server_certificate.public_bytes(serialization.Encoding.PEM))
    key_file = tmp_path / "server.key"
    key_file.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certificate_file, key_file)
    return TlsServer(context), str(ca_file)


@pytest.fixture
def server_ca() -> trustme.CA:
    """The CA that issued the certificate of the `server` fixture."""
    return trustme.CA()


@pytest.fixture
def server(server_ca) -> Iterator[TlsServer]:
    """A TLS server whose certificate names 127.0.0.1."""
    tls_server = TlsServer.issued_by(server_ca, "127.0.0.1")
    yield tls_server
    tls_server.close()


def write_ca_file(ca: trustme.CA, tmp_path) -> str:
    path = tmp_path / "ca.pem"
    ca.cert_pem.write_to_path(str(path))
    return str(path)


def write_client_certificate(ca: trustme.CA, tmp_path, *, chain: bool = False) -> dict[str, str]:
    """Write a client certificate `ca` issued for `agent.example`, and its private key; return
    the config values naming them. With `chain`, the certificate file also holds its CA chain."""
    leaf = ca.issue_cert("agent.example")
    cert_file, key_file = tmp_path / "client.pem", tmp_path / "client.key"
    pems = leaf.cert_chain_pems if chain else leaf.cert_chain_pems[:1]
    cert_file.write_bytes(b"".join(pem.bytes() for pem in pems))
    leaf.private_key_pem.write_to_path(str(key_file))
    return {"RABBIT_MQ_CERT_FILE": str(cert_file), "RABBIT_MQ_KEY_FILE": str(key_file)}


def tls_config(port: int = 5671, /, **values: str) -> KicakConfig:
    return KicakConfig(
        values={
            **CONNECTION_VALUES,
            "RABBIT_MQ_HOST": "127.0.0.1",
            "RABBIT_MQ_PORT": str(port),
            "RABBIT_MQ_TLS": "true",
            **values,
        }
    )


@dataclasses.dataclass(frozen=True, slots=True)
class TlsMessage(KicakMessage):
    kicak_type = "tls.v1"


class TlsExecutor(KicakExecutorAbstract[TlsMessage, None]):
    def __init__(self, config: KicakConfig) -> None:
        super().__init__(
            config, source_queue="input-queue", source_exchange="input-exchange", max_retry_rounds=3
        )

    def execute(self, message: TlsMessage) -> None:
        return None


class TlsInjector(KicakInjectorAbstract[TlsMessage]):
    def __init__(self, config: KicakConfig) -> None:
        super().__init__(config, exchange_name="input-exchange")

    def generate(self) -> TlsMessage:
        return TlsMessage(message_id="message-1")


@pytest.fixture
def no_retry_waits(monkeypatch) -> None:
    """Fail the test if an agent waits to retry: a refused certificate must not be retried."""

    def fail(_seconds: float) -> None:
        raise AssertionError("the connection was retried")

    monkeypatch.setattr("pykicak.executor.time.sleep", fail)
    monkeypatch.setattr("pykicak.injector.time.sleep", fail)


# --- connection parameters ------------------------------------------------------------------


def test_connection_without_tls_has_no_tls_options(monkeypatch):
    executor = TlsExecutor(KicakConfig(values=CONNECTION_VALUES))

    assert captured_connection_parameters(executor, monkeypatch).ssl_options is None


def test_connection_with_tls_verifies_the_certificate_and_the_host_name(monkeypatch):
    executor = TlsExecutor(tls_config(RABBIT_MQ_HOST="broker.example.com"))

    tls_options = captured_connection_parameters(executor, monkeypatch).ssl_options

    assert tls_options.server_hostname == "broker.example.com"
    context = tls_options.context
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.minimum_version >= ssl.TLSVersion.TLSv1_2


def python_defaults_to_minimum(monkeypatch, minimum: ssl.TLSVersion) -> None:
    """Make Python's default TLS context start with `minimum`, which depends on how Python and
    OpenSSL were built, so a test covers the same code on every build."""
    create_default_context = ssl.create_default_context

    def default_context(*args, **kwargs) -> ssl.SSLContext:
        context = create_default_context(*args, **kwargs)
        context.minimum_version = minimum
        return context

    monkeypatch.setattr("pykicak.abstracts.ssl.create_default_context", default_context)


@pytest.mark.parametrize("ca_file", [False, True], ids=["system-cas", "ca-file"])
def test_connection_requires_tls_1_2_whatever_python_defaults_to(monkeypatch, tmp_path, ca_file):
    # As some builds do, e.g. Ubuntu's Python 3.14: the minimum is left to OpenSSL
    python_defaults_to_minimum(monkeypatch, ssl.TLSVersion.MINIMUM_SUPPORTED)
    values = {"RABBIT_MQ_CA_FILE": write_ca_file(trustme.CA(), tmp_path)} if ca_file else {}
    executor = TlsExecutor(tls_config(**values))

    context = captured_connection_parameters(executor, monkeypatch).ssl_options.context

    assert context.minimum_version == ssl.TLSVersion.TLSv1_2


@pytest.mark.parametrize("ca_file", [False, True], ids=["system-cas", "ca-file"])
def test_connection_keeps_a_stricter_minimum_python_defaults_to(monkeypatch, tmp_path, ca_file):
    # TLS 1.2 is only the floor: a Python that already requires TLS 1.3 is not lowered to 1.2
    python_defaults_to_minimum(monkeypatch, ssl.TLSVersion.TLSv1_3)
    values = {"RABBIT_MQ_CA_FILE": write_ca_file(trustme.CA(), tmp_path)} if ca_file else {}
    executor = TlsExecutor(tls_config(**values))

    context = captured_connection_parameters(executor, monkeypatch).ssl_options.context

    assert context.minimum_version == ssl.TLSVersion.TLSv1_3


@pytest.mark.parametrize("ca_file", [False, True], ids=["system-cas", "ca-file"])
def test_connection_applies_the_strict_certificate_checks_of_python_3_13(
    monkeypatch, tmp_path, ca_file
):
    values = {"RABBIT_MQ_CA_FILE": write_ca_file(trustme.CA(), tmp_path)} if ca_file else {}
    executor = TlsExecutor(tls_config(**values))

    context = captured_connection_parameters(executor, monkeypatch).ssl_options.context

    assert context.verify_flags & ssl.VERIFY_X509_STRICT
    assert context.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN


def test_connection_with_a_ca_file_trusts_only_its_certificates(monkeypatch, tmp_path):
    ca = trustme.CA()
    executor = TlsExecutor(tls_config(RABBIT_MQ_CA_FILE=write_ca_file(ca, tmp_path)))

    context = captured_connection_parameters(executor, monkeypatch).ssl_options.context

    assert context.get_ca_certs(binary_form=True) == [
        ssl.PEM_cert_to_DER_cert(ca.cert_pem.bytes().decode())
    ]


@pytest.mark.parametrize(
    ("contents", "problem"),
    [
        (None, "cannot be read: No such file or directory"),
        ("directory", "cannot be read: Is a directory"),
        ("not a certificate\n", "does not contain any PEM CA certificate"),
        ("", "does not contain any PEM CA certificate"),
    ],
    ids=["missing", "directory", "not-pem", "empty"],
)
def test_unusable_ca_file_raises_config_error_without_naming_the_file(
    monkeypatch, tmp_path, caplog, contents, problem
):
    ca_file = tmp_path / "secret-name.pem"
    if contents == "directory":
        ca_file.mkdir()
    elif contents is not None:
        ca_file.write_text(contents)
    executor = TlsExecutor(tls_config(RABBIT_MQ_CA_FILE=str(ca_file)))

    with pytest.raises(KicakConfigError) as exc_info:
        captured_connection_parameters(executor, monkeypatch)

    assert str(exc_info.value) == f"RABBIT_MQ_CA_FILE {problem}"
    assert exc_info.value.__context__ is None
    assert exc_info.value.__cause__ is None
    assert "secret-name" not in caplog.text


def test_ca_file_without_tls_raises_config_error(monkeypatch, tmp_path):
    config = KicakConfig(
        values={**CONNECTION_VALUES, "RABBIT_MQ_CA_FILE": write_ca_file(trustme.CA(), tmp_path)}
    )

    with pytest.raises(KicakConfigError, match="RABBIT_MQ_CA_FILE requires RABBIT_MQ_TLS=true"):
        captured_connection_parameters(TlsExecutor(config), monkeypatch)


def test_connection_requires_the_tls_setting(monkeypatch):
    values = {key: value for key, value in CONNECTION_VALUES.items() if key != "RABBIT_MQ_TLS"}

    with pytest.raises(KicakConfigError, match="RABBIT_MQ_TLS"):
        captured_connection_parameters(TlsExecutor(KicakConfig(values=values)), monkeypatch)


# --- retries --------------------------------------------------------------------------------


@pytest.mark.parametrize("agent_type", [KicakAbstract, TlsExecutor], ids=["base", "executor"])
def test_refused_certificate_is_not_retryable(agent_type):
    error = ssl.SSLCertVerificationError("certificate verify failed")

    assert agent_type._is_retryable_rabbitmq_error(error) is False


@pytest.mark.parametrize("agent_type", [KicakAbstract, TlsExecutor], ids=["base", "executor"])
def test_other_tls_errors_are_retryable(agent_type):
    error = ssl.SSLError("record layer failure")  # e.g. a handshake cut off

    assert agent_type._is_retryable_rabbitmq_error(error) is True


# --- certificate checks against a TLS server ------------------------------------------------


def test_trusted_certificate_completes_the_handshake(server, server_ca, tmp_path):
    executor = TlsExecutor(
        tls_config(server.port, RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path))
    )

    # The server closes the connection after the handshake, before any AMQP is exchanged
    with pytest.raises(Exception) as exc_info:
        KicakAbstract._create_connection(executor)

    assert not isinstance(exc_info.value, ssl.SSLError)
    assert server.handshakes(1) == [True]


def test_executor_stops_without_retrying_when_the_certificate_is_from_another_ca(
    server, tmp_path, no_retry_waits
):
    executor = TlsExecutor(
        tls_config(server.port, RABBIT_MQ_CA_FILE=write_ca_file(trustme.CA(), tmp_path))
    )

    with pytest.raises(ssl.SSLCertVerificationError):
        executor.run()

    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert server.handshakes(1) == [False]


def test_executor_refuses_a_certificate_whose_ca_fails_the_strict_checks(tmp_path, no_retry_waits):
    server, ca_file = old_style_ca_server(tmp_path)
    try:
        # Without the strict checks, as Python 3.12 does by default, the certificate is accepted
        lenient = ssl.create_default_context(cafile=ca_file)
        lenient.verify_flags &= ~(ssl.VERIFY_X509_STRICT | ssl.VERIFY_X509_PARTIAL_CHAIN)
        with socket.create_connection(("127.0.0.1", server.port)) as connection:
            lenient.wrap_socket(connection, server_hostname="127.0.0.1").close()
        executor = TlsExecutor(tls_config(server.port, RABBIT_MQ_CA_FILE=ca_file))

        with pytest.raises(ssl.SSLCertVerificationError):
            executor.run()
    finally:
        server.close()

    assert server.handshakes(2) == [True, False]


def test_intermediate_ca_in_the_ca_file_is_trusted_on_its_own(tmp_path):
    intermediate_ca = trustme.CA().create_child_ca()
    server = TlsServer.issued_by(intermediate_ca, "127.0.0.1")
    try:
        executor = TlsExecutor(
            tls_config(server.port, RABBIT_MQ_CA_FILE=write_ca_file(intermediate_ca, tmp_path))
        )

        # The server closes the connection after the handshake, before any AMQP is exchanged
        with pytest.raises(Exception) as exc_info:
            KicakAbstract._create_connection(executor)
    finally:
        server.close()

    assert not isinstance(exc_info.value, ssl.SSLError)
    assert server.handshakes(1) == [True]


def test_executor_without_a_ca_file_refuses_a_privately_issued_certificate(server, no_retry_waits):
    executor = TlsExecutor(tls_config(server.port))

    with pytest.raises(ssl.SSLCertVerificationError):
        executor.run()

    assert server.handshakes(1) == [False]


def test_executor_refuses_a_certificate_issued_for_another_host(
    server_ca, tmp_path, no_retry_waits
):
    # The executor connects to 127.0.0.1, which the certificate does not name
    other_host_server = TlsServer.issued_by(server_ca, "localhost")
    try:
        executor = TlsExecutor(
            tls_config(other_host_server.port, RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path))
        )

        with pytest.raises(ssl.SSLCertVerificationError, match="127.0.0.1"):
            executor.run()
    finally:
        other_host_server.close()

    assert other_host_server.handshakes(1) == [False]


def test_injector_raises_on_the_first_attempt_when_the_certificate_is_refused(
    server, tmp_path, no_retry_waits
):
    injector = TlsInjector(
        tls_config(server.port, RABBIT_MQ_CA_FILE=write_ca_file(trustme.CA(), tmp_path))
    )

    with pytest.raises(InjectionError) as exc_info:
        injector.run()

    assert isinstance(exc_info.value.__cause__, ssl.SSLCertVerificationError)
    assert server.handshakes(1) == [False]


# --- client certificates (mutual TLS) -------------------------------------------------------


@pytest.fixture
def client_ca() -> trustme.CA:
    """The CA the `verifying_server` fixture trusts to have issued client certificates."""
    return trustme.CA()


@pytest.fixture
def verifying_server(server_ca, client_ca) -> Iterator[TlsServer]:
    """A TLS server for 127.0.0.1 that requires a client certificate `client_ca` issued."""
    tls_server = TlsServer.verifying_clients(server_ca, client_ca)
    yield tls_server
    tls_server.close()


def connect_once(agent: KicakAbstract) -> None:
    """Open one connection as `agent` does, expecting the server to close it after TLS."""
    # The server closes the connection after the handshake, before any AMQP is exchanged
    with pytest.raises(Exception) as exc_info:
        KicakAbstract._create_connection(agent)

    assert not isinstance(exc_info.value, ssl.SSLError)


def test_client_certificate_is_presented_to_a_server_that_requires_one(
    verifying_server, server_ca, client_ca, tmp_path
):
    executor = TlsExecutor(
        tls_config(
            verifying_server.port,
            RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path),
            **write_client_certificate(client_ca, tmp_path),
        )
    )

    connect_once(executor)

    assert verifying_server.handshakes(1) == [True]
    [client_certificate] = verifying_server.client_certificates()
    assert client_certificate["subjectAltName"] == (("DNS", "agent.example"),)


def test_client_certificate_file_may_hold_the_intermediate_ca(server_ca, tmp_path):
    # The server trusts only the root CA, so the client must send the intermediate CA too
    root_ca = trustme.CA()
    verifying_server = TlsServer.verifying_clients(server_ca, root_ca)
    try:
        executor = TlsExecutor(
            tls_config(
                verifying_server.port,
                RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path),
                **write_client_certificate(root_ca.create_child_ca(), tmp_path, chain=True),
            )
        )

        connect_once(executor)
    finally:
        verifying_server.close()

    assert verifying_server.handshakes(1) == [True]


def test_certificate_and_key_may_be_one_file(verifying_server, server_ca, client_ca, tmp_path):
    files = write_client_certificate(client_ca, tmp_path)
    combined = tmp_path / "client-and-key.pem"
    combined.write_bytes(
        Path(files["RABBIT_MQ_CERT_FILE"]).read_bytes()
        + Path(files["RABBIT_MQ_KEY_FILE"]).read_bytes()
    )
    executor = TlsExecutor(
        tls_config(
            verifying_server.port,
            RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path),
            RABBIT_MQ_CERT_FILE=str(combined),
            RABBIT_MQ_KEY_FILE=str(combined),
        )
    )

    connect_once(executor)

    assert verifying_server.handshakes(1) == [True]


def test_client_certificate_is_read_again_for_every_connection(
    verifying_server, server_ca, client_ca, tmp_path
):
    # e.g. a certificate renewed on disk while the agent runs is used from its next connection
    files = write_client_certificate(trustme.CA(), tmp_path)  # refused: another CA issued it
    executor = TlsExecutor(
        tls_config(
            verifying_server.port, RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path), **files
        )
    )
    # pika reports the refusal as an ssl.SSLError (TLS 1.2) or a lost connection (TLS 1.3)
    with pytest.raises((AMQPConnectionError, OSError)):
        KicakAbstract._create_connection(executor)
    verifying_server.handshakes(1)

    write_client_certificate(client_ca, tmp_path)  # overwrites both files
    connect_once(executor)

    assert verifying_server.handshakes(2) == [False, True]


def test_injector_retries_when_the_server_refuses_its_client_certificate(
    verifying_server, server_ca, tmp_path, monkeypatch
):
    # The refusal reaches pika as a lost or failed connection, like an unreachable broker
    monkeypatch.setattr("pykicak.injector.time.sleep", lambda _seconds: None)
    injector = TlsInjector(
        tls_config(
            verifying_server.port,
            RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path),
            **write_client_certificate(trustme.CA(), tmp_path),
        )
    )

    with pytest.raises(InjectionError):
        injector.run()

    assert verifying_server.handshakes(3) == [False, False, False]


def test_executor_keeps_retrying_when_the_server_requires_a_client_certificate(
    verifying_server, server_ca, tmp_path, monkeypatch
):
    executor = TlsExecutor(
        tls_config(verifying_server.port, RABBIT_MQ_CA_FILE=write_ca_file(server_ca, tmp_path))
    )
    statuses: list[ExecutorStatus] = []

    def stop_instead_of_waiting(_seconds: float) -> bool:
        statuses.append(executor.get_info().status)
        executor.stop()
        return False

    monkeypatch.setattr(executor, "_sleep_unless_stopped", stop_instead_of_waiting)

    executor.run()

    assert statuses == [ExecutorStatus.UPSTREAM_RECOVER]
    assert executor.get_info().status == ExecutorStatus.STOPPED
    assert verifying_server.handshakes(1) == [False]


@pytest.mark.parametrize("key", ["RABBIT_MQ_CERT_FILE", "RABBIT_MQ_KEY_FILE"])
def test_client_certificate_without_tls_raises_config_error(monkeypatch, tmp_path, key):
    files = write_client_certificate(trustme.CA(), tmp_path)
    config = KicakConfig(values={**CONNECTION_VALUES, key: files[key]})

    with pytest.raises(KicakConfigError, match=f"^{key} requires RABBIT_MQ_TLS=true$"):
        captured_connection_parameters(TlsExecutor(config), monkeypatch)


@pytest.mark.parametrize("key", ["RABBIT_MQ_CERT_FILE", "RABBIT_MQ_KEY_FILE"])
def test_client_certificate_and_key_must_be_given_together(monkeypatch, tmp_path, key):
    files = write_client_certificate(trustme.CA(), tmp_path)
    executor = TlsExecutor(tls_config(**{key: files[key]}))

    with pytest.raises(
        KicakConfigError,
        match="^RABBIT_MQ_CERT_FILE and RABBIT_MQ_KEY_FILE must be given together$",
    ):
        captured_connection_parameters(executor, monkeypatch)


def encrypted(key_pem: bytes) -> bytes:
    """Return the PEM private key `key_pem`, encrypted with a password."""
    key = serialization.load_pem_private_key(key_pem, password=None)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(b"key-password"),
    )


_OTHER_KEY = trustme.CA().issue_cert("other.example").private_key_pem.bytes()


@pytest.mark.parametrize(
    ("key", "change", "problem"),
    [
        ("RABBIT_MQ_CERT_FILE", None, "cannot be read: No such file or directory"),
        ("RABBIT_MQ_KEY_FILE", None, "cannot be read: No such file or directory"),
        ("RABBIT_MQ_CERT_FILE", "directory", "cannot be read: Is a directory"),
        ("RABBIT_MQ_CERT_FILE", b"", "does not contain any PEM certificate"),
        ("RABBIT_MQ_CERT_FILE", b"not a certificate\n", "does not contain any PEM certificate"),
        ("RABBIT_MQ_CERT_FILE", "key", "does not contain any PEM certificate"),
        (
            "RABBIT_MQ_KEY_FILE",
            b"not a key\n",
            "does not contain the PEM private key of RABBIT_MQ_CERT_FILE",
        ),
        (
            "RABBIT_MQ_KEY_FILE",
            "certificate",
            "does not contain the PEM private key of RABBIT_MQ_CERT_FILE",
        ),
        (
            "RABBIT_MQ_KEY_FILE",
            _OTHER_KEY,
            "does not contain the PEM private key of RABBIT_MQ_CERT_FILE",
        ),
        (
            "RABBIT_MQ_KEY_FILE",
            "encrypted",
            "holds an encrypted private key, which is not supported",
        ),
    ],
    ids=[
        "missing-certificate",
        "missing-key",
        "directory",
        "empty-certificate",
        "not-pem-certificate",
        "key-as-certificate",
        "not-pem-key",
        "certificate-as-key",
        "key-of-another-certificate",
        "encrypted-key",
    ],
)
def test_unusable_client_certificate_raises_config_error_without_naming_the_file(
    monkeypatch, tmp_path, caplog, key, change, problem
):
    files = write_client_certificate(trustme.CA(), tmp_path)
    secret_path = tmp_path / "secret-name.pem"
    if change == "directory":
        secret_path.mkdir()
    elif change == "key":
        secret_path.write_bytes(Path(files["RABBIT_MQ_KEY_FILE"]).read_bytes())
    elif change == "certificate":
        secret_path.write_bytes(Path(files["RABBIT_MQ_CERT_FILE"]).read_bytes())
    elif change == "encrypted":
        secret_path.write_bytes(encrypted(Path(files["RABBIT_MQ_KEY_FILE"]).read_bytes()))
    elif change is not None:
        secret_path.write_bytes(change)
    executor = TlsExecutor(tls_config(**{**files, key: str(secret_path)}))

    with pytest.raises(KicakConfigError) as exc_info:
        captured_connection_parameters(executor, monkeypatch)

    assert str(exc_info.value) == f"{key} {problem}"
    assert exc_info.value.__context__ is None
    assert exc_info.value.__cause__ is None
    assert "secret-name" not in caplog.text
    assert f"Invalid Kicak config key {key}: the file {problem}" in caplog.text
