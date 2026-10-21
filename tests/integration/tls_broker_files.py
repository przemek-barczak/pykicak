"""Write the files a test RabbitMQ broker needs for a TLS listener on port 5671.

Run it before starting the broker, from the repository root:

    python -m tests.integration.tls_broker_files <directory>

It writes a new test CA and a server certificate for `localhost` issued by it:

- `ca.pem`: the test CA's certificate; point RABBIT_MQ_CA_FILE in the TLS .kicak file to it.
- `server.pem` and `server.key`: the broker's certificate and private key.
- `20-tls.conf`: the RabbitMQ configuration of the TLS listener.

Mount the directory into the broker container at /etc/rabbitmq/tls, and `20-tls.conf` at
/etc/rabbitmq/conf.d/20-tls.conf (see .github/workflows/ci.yml). The key is for tests only, so the
files are world-readable: the broker in the container runs as another user.
"""

from __future__ import annotations

import sys
from pathlib import Path

import trustme

BROKER_HOST_NAME = "localhost"
"""The only name the server certificate is issued for; connecting to 127.0.0.1 must fail."""

_CONTAINER_DIRECTORY = "/etc/rabbitmq/tls"

_RABBITMQ_CONF = f"""\
listeners.ssl.default = 5671
ssl_options.cacertfile = {_CONTAINER_DIRECTORY}/ca.pem
ssl_options.certfile = {_CONTAINER_DIRECTORY}/server.pem
ssl_options.keyfile = {_CONTAINER_DIRECTORY}/server.key
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
"""


def write_tls_broker_files(directory: Path) -> None:
    """Write a new test CA, the broker's certificate and key, and its TLS configuration."""
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o755)
    ca = trustme.CA()
    server_cert = ca.issue_cert(BROKER_HOST_NAME)
    files = {
        "ca.pem": ca.cert_pem.bytes(),
        "server.pem": b"".join(pem.bytes() for pem in server_cert.cert_chain_pems),
        "server.key": server_cert.private_key_pem.bytes(),
        "20-tls.conf": _RABBITMQ_CONF.encode(),
    }
    for name, contents in files.items():
        path = directory / name
        path.write_bytes(contents)
        path.chmod(0o644)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python -m tests.integration.tls_broker_files <directory>")
    write_tls_broker_files(Path(sys.argv[1]))
