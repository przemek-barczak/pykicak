"""TLS options of a RabbitMQ connection, built from a `KicakConfig`."""

from __future__ import annotations

import logging
import ssl

import pika

from pykicak.config import KicakConfig, KicakConfigError

logger = logging.getLogger(__name__)


def tls_options(config: KicakConfig) -> pika.SSLOptions | None:
    """Return the TLS options of a pika connection configured by `config`, or None without TLS.

    The options agents connect with, for an application that opens a pika connection of its own,
    e.g. an operator's tool: with RABBIT_MQ_TLS=true, TLS 1.2 or newer, the broker's certificate
    verified against the system's CAs or RABBIT_MQ_CA_FILE, and its host name against
    RABBIT_MQ_HOST, and the client certificate of RABBIT_MQ_CERT_FILE and RABBIT_MQ_KEY_FILE if
    given. Pass them to `pika.ConnectionParameters(ssl_options=...)`. The files are read on every
    call, so build the options for each connection, to pick up renewed certificates.

    Raises `KicakConfigError` for a missing or invalid RABBIT_MQ_TLS, for a CA, certificate, or key
    file given with RABBIT_MQ_TLS=false, because the file shows that TLS was intended, for a client
    certificate without its key or a key without its certificate, and for a file that cannot be
    used. The error names the config key, never the path. See "TLS" in the documentation.
    """
    files = {
        "RABBIT_MQ_CA_FILE": config.rabbitmq_ca_file,
        "RABBIT_MQ_CERT_FILE": config.rabbitmq_cert_file,
        "RABBIT_MQ_KEY_FILE": config.rabbitmq_key_file,
    }
    if not config.rabbitmq_tls:
        for key, path in files.items():
            if path is not None:
                logger.error("Invalid Kicak config: %s requires RABBIT_MQ_TLS=true", key)
                raise KicakConfigError(f"{key} requires RABBIT_MQ_TLS=true")
        return None
    cert_file, key_file = files["RABBIT_MQ_CERT_FILE"], files["RABBIT_MQ_KEY_FILE"]
    if (cert_file is None) != (key_file is None):
        problem = "RABBIT_MQ_CERT_FILE and RABBIT_MQ_KEY_FILE must be given together"
        logger.error("Invalid Kicak config: %s", problem)
        raise KicakConfigError(problem)
    context = _tls_context(files["RABBIT_MQ_CA_FILE"])
    if cert_file is not None and key_file is not None:
        _load_client_certificate(context, cert_file, key_file)
    # The host name is also what the broker's certificate must name
    return pika.SSLOptions(context, server_hostname=config.rabbitmq_host)


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
    raise _file_error("RABBIT_MQ_CA_FILE", problem)


class _EncryptedKeyError(Exception):
    """Raised by `_refuse_key_password()`: the client certificate's key is encrypted."""


def _refuse_key_password() -> str:
    """The password callback of `load_cert_chain()`, which OpenSSL calls for an encrypted key.

    Without a callback, OpenSSL would ask for the password on the terminal, blocking the agent.
    """
    raise _EncryptedKeyError


def _load_client_certificate(context: ssl.SSLContext, cert_file: str, key_file: str) -> None:
    """Make `context` present the certificate in `cert_file`, with the private key in `key_file`.

    The broker asks for it when it verifies its clients (mutual TLS). A file that cannot be read,
    a certificate file without a PEM certificate, and a key file without the certificate's
    unencrypted PEM private key raise `KicakConfigError` naming the config key, never the path.
    """
    for key, path in (("RABBIT_MQ_CERT_FILE", cert_file), ("RABBIT_MQ_KEY_FILE", key_file)):
        _check_readable(key, path)
    key = "RABBIT_MQ_KEY_FILE"
    try:
        context.load_cert_chain(cert_file, key_file, password=_refuse_key_password)
        return
    except _EncryptedKeyError:
        problem = "holds an encrypted private key, which is not supported"
    except ssl.SSLError:
        # OpenSSL's errors do not tell which file is at fault, so the certificate is checked alone
        if _holds_certificate(cert_file):
            problem = "does not contain the PEM private key of RABBIT_MQ_CERT_FILE"
        else:
            key, problem = "RABBIT_MQ_CERT_FILE", "does not contain any PEM certificate"
    # Raised outside the except blocks, so the error carries no context naming the file
    raise _file_error(key, problem)


def _check_readable(key: str, path: str) -> None:
    """Raise `KicakConfigError` naming `key` if the file at `path` cannot be opened."""
    try:
        with open(path, "rb"):
            return
    except OSError as error:
        problem = f"cannot be read: {error.strerror}"
    raise _file_error(key, problem)


def _holds_certificate(path: str) -> bool:
    """Return whether the file at `path` holds a PEM certificate."""
    try:
        ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).load_verify_locations(cafile=path)
    except ssl.SSLError:
        return False
    return True


def _file_error(key: str, problem: str) -> KicakConfigError:
    """Log and return the error for an unusable file named by `key`, e.g. RABBIT_MQ_CA_FILE."""
    logger.error("Invalid Kicak config key %s: the file %s", key, problem)
    return KicakConfigError(f"{key} {problem}")
