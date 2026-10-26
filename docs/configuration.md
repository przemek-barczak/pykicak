# Configuration

Applications load RabbitMQ **connection** details into a `KicakConfig` and pass it to an agent. A
config comes from one of three sources, all with the same keys:

- `KicakConfig.from_file(path)` reads a [`.kicak` file](#the-kicak-file).
- `KicakConfig.from_env()` reads the [environment variables](#environment-variables) of the same
  names, for example in a container.
- `KicakConfig(values={...})` takes a mapping built in code, for example with values from a secrets
  manager. The values are used exactly as given.

The library never picks a source itself: it reads no file and no environment variable unless the
application calls `from_file()` or `from_env()`.

| Key | Description |
| --- | --- |
| `RABBIT_MQ_USERNAME` | RabbitMQ username |
| `RABBIT_MQ_PASSWORD` | RabbitMQ password |
| `RABBIT_MQ_HOST` | RabbitMQ host |
| `RABBIT_MQ_PORT` | RabbitMQ port |
| `RABBIT_MQ_VIRTUAL_HOST` | RabbitMQ virtual host |
| `RABBIT_MQ_TLS` | `true` to connect with TLS (usually port 5671), `false` for plain, unencrypted AMQP (see [TLS](#tls)) |
| `RABBIT_MQ_CA_FILE` | Optional, TLS only: a file of PEM CA certificates, the only CAs trusted to have issued the broker's certificate (see [TLS](#tls)) |
| `RABBIT_MQ_CERT_FILE` | Optional, TLS only: the PEM client certificate agents present to a broker that verifies its clients (see [Client certificates](#client-certificates-mutual-tls)) |
| `RABBIT_MQ_KEY_FILE` | Optional, with `RABBIT_MQ_CERT_FILE` only: the client certificate's unencrypted PEM private key |

Every key except `RABBIT_MQ_CA_FILE`, `RABBIT_MQ_CERT_FILE`, and `RABBIT_MQ_KEY_FILE` is required.
Whatever the source, keys are checked when they are read, that is when an agent connects: a missing
required key raises `KicakConfigError` (from an Injector's `run()` or `publish()`, as the cause of
`InjectionError`), and so does a port that is not an integer from 1 to 65535, a `RABBIT_MQ_TLS`
other than `true` or `false` (in any letter case), and an empty `RABBIT_MQ_CA_FILE`,
`RABBIT_MQ_CERT_FILE`, or `RABBIT_MQ_KEY_FILE`.

`KicakConfig` keeps values out of its `repr()` and its errors, so they do not reach logs or crash
reporters: the repr lists only the keys (`KicakConfig(keys=['RABBIT_MQ_HOST', ...])`), and a
malformed line or invalid value is reported by its line number or key, never its content.

A `KicakConfig` is immutable: its `values` is a read-only copy of the mapping it was created with.
It is hashable, and it can be pickled, for example to pass it to a `multiprocessing` worker.

A config does **not** hold queue, exchange, or binding names — that is application topology, not
connection configuration. See [RabbitMQ topology](topology.md).

## The `.kicak` file

A `.kicak` file is a simple `KEY=VALUE` text file, read as UTF-8 on every platform (a leading byte
order mark is allowed). Blank lines and lines starting with `#` are ignored, keys and values are
stripped of surrounding whitespace, and only the first `=` separates the key from the value. See
[`.kicak.example`](../.kicak.example) for a template.

Never commit a real `.kicak` file; `.kicak` files are excluded by `.gitignore`.

Missing or unreadable files, invalid UTF-8, malformed lines (including a line with an empty key),
and duplicate keys raise `KicakConfigError` from `from_file()`.

## Environment variables

`KicakConfig.from_env()` reads the keys from the environment variables of the same names, such as
`RABBIT_MQ_HOST`, for example as set by a Compose file or a Kubernetes secret:

```python
config = KicakConfig.from_env()
```

It takes only the keys in the table above, never other variables, so the config does not collect
unrelated secrets of the process. As in a `.kicak` file, values are stripped of surrounding
whitespace, which also removes the trailing newline of a secret created from a file; a variable
that is not set leaves its key missing, and one set to an empty value keeps it empty. A value that
is not valid UTF-8 raises `KicakConfigError`, naming only the variable. The environment is read
once, when `from_env()` is called.

Sources can be combined through `values`. Here every key set in the environment, such as a password
from a Kubernetes secret, overrides the file:

```python
file_config = KicakConfig.from_file("/etc/orders/.kicak")
config = KicakConfig(values={**file_config.values, **KicakConfig.from_env().values})
```

## TLS

Without TLS, the connection to RabbitMQ is not encrypted: the username, the password, and every
message cross the network in clear text, and anyone on the path can read or change them. Use
`RABBIT_MQ_TLS=false` only for a broker on the same host or on a network you fully trust. Managed
brokers offer TLS, and some, such as Amazon MQ, accept nothing else. `RABBIT_MQ_TLS` is required,
so an unencrypted connection is always an explicit choice:

```text
RABBIT_MQ_HOST=broker.example.com
RABBIT_MQ_PORT=5671
RABBIT_MQ_TLS=true
```

With `RABBIT_MQ_TLS=true`, agents use Python's recommended TLS client settings
(`ssl.create_default_context()`) with TLS 1.2 or newer, and they connect only if the broker's
certificate is valid and:

- is issued by a trusted CA. Without `RABBIT_MQ_CA_FILE` these are the CAs the operating system
  trusts, which is enough for brokers with a publicly trusted certificate, such as managed ones.
  With it, they are only the CA certificates in that file: set it for a broker whose certificate
  comes from a private CA. The file may also hold an intermediate CA, which is then trusted on its
  own. A relative path is resolved against the working directory of the process.
- names `RABBIT_MQ_HOST`, as a DNS name or an IP address. Connect with the name the certificate was
  issued for.

There is deliberately no setting that turns these checks off; for a self-hosted broker, give its CA
certificate in `RABBIT_MQ_CA_FILE` instead. Certificates are also checked strictly
(`ssl.VERIFY_X509_STRICT`), as Python 3.13 and newer do by default, and pykicak applies the same
checks on Python 3.12, so a certificate is accepted or refused alike on every supported version. A
private CA made with an old tool may therefore be refused, for example one whose Basic Constraints
extension is not marked critical; issue it again with a current tool rather than relaxing the
check. Agents log in with the username and password, protected by the encryption; a broker that
also requires a client certificate needs [two more keys](#client-certificates-mutual-tls).

When TLS fails:

- A broker certificate that fails verification (an untrusted CA, an expired certificate, or another
  host name) raises `ssl.SSLCertVerificationError` and is never retried: either the configuration
  is wrong or the server is not the intended broker. An Executor stops, with the status
  `"CRASHED"`; an Injector raises `InjectionError` with it as the cause, after one attempt. An
  expired broker certificate therefore stops every Executor until it is renewed, so monitor its
  expiry date.
- A `RABBIT_MQ_CA_FILE` that is missing, unreadable, or contains no PEM certificate raises
  `KicakConfigError` when connecting, as does one given with `RABBIT_MQ_TLS=false`, because it
  shows that TLS was intended. The error names the key, never the path.
- Other TLS errors, for example a handshake cut off, are retried like other connection errors. That
  includes a port that does not speak TLS, such as 5672 with `RABBIT_MQ_TLS=true` (see
  [Retries and logging](delivery-guarantees.md#retries)).

### TLS for a connection of your own

An application that opens a pika connection of its own, for example an operator's tool, gets the
TLS settings agents connect with from `tls_options()`:

```python
import pika

from pykicak import KicakConfig, tls_options

config = KicakConfig.from_file(".kicak")
parameters = pika.ConnectionParameters(
    host=config.rabbitmq_host,
    port=config.rabbitmq_port,
    virtual_host=config.rabbitmq_virtual_host,
    credentials=pika.PlainCredentials(config.rabbitmq_username, config.rabbitmq_password),
    ssl_options=tls_options(config),  # None with RABBIT_MQ_TLS=false
)
connection = pika.BlockingConnection(parameters)
```

It returns pika's `SSLOptions`, or `None` with `RABBIT_MQ_TLS=false`, and raises `KicakConfigError`
in the same cases as an agent connecting. It reads the CA, certificate, and key files on every
call, so call it for each connection to pick up renewed certificates. The replay example
[`examples/replay_dead_letters.py`](../examples/replay_dead_letters.py) uses it.

### Client certificates (mutual TLS)

Most brokers do not need this. A broker configured to verify its clients
(`ssl_options.verify = verify_peer` with `ssl_options.fail_if_no_peer_cert = true` in
`rabbitmq.conf`) accepts only clients that present a certificate issued by a CA it trusts. Give
agents theirs with two keys, always together:

```text
RABBIT_MQ_TLS=true
RABBIT_MQ_CERT_FILE=/etc/orders/tls/agent.pem
RABBIT_MQ_KEY_FILE=/etc/orders/tls/agent.key
```

- `RABBIT_MQ_CERT_FILE` holds the agent's certificate, in PEM. If the broker does not trust its
  issuer directly, add the intermediate CA certificates after it, so that the broker gets the chain
  up to a CA it trusts.
- `RABBIT_MQ_KEY_FILE` holds the certificate's private key, in PEM and unencrypted. Encrypted keys
  are not supported: an agent runs unattended, so the key's password would have to be stored next
  to the key, which protects nothing. Protect the key file as you protect the password, for example
  with file permissions or a secret mounted into the container, and decrypt an encrypted key once,
  for example with `openssl pkey -in encrypted.key -out agent.key`. A single file holding both the
  certificate and the key can be given as both keys.

The client certificate is checked in addition to the username and password, not instead of them:
agents still log in with `RABBIT_MQ_USERNAME` and `RABBIT_MQ_PASSWORD`. RabbitMQ's login by
certificate alone (the `EXTERNAL` mechanism of the `rabbitmq_auth_mechanism_ssl` plugin) is not
supported. Both files are read again for every connection, so a certificate renewed on disk, for
example by a certificate manager, is used from the agent's next connection, without a restart.

When a client certificate fails:

- A certificate or key file that is missing or unreadable, a certificate file without a PEM
  certificate, and a key file without the certificate's unencrypted PEM private key raise
  `KicakConfigError` when connecting, as does one of the two keys without the other, or either one
  with `RABBIT_MQ_TLS=false`. The error names the key, never the path.
- A certificate the broker refuses (issued by a CA it does not trust, or expired), and a missing one
  when the broker requires it, are retried like other connection errors: pika does not report the
  broker's refusal as a certificate error, only as a connection lost during the TLS handshake or
  right after it (`IncompatibleProtocolError` or `ssl.SSLError` naming the broker's TLS alert, such
  as `unknown ca` or `certificate required`). An Executor therefore keeps reconnecting, with the
  status `"UPSTREAM_RECOVER"`, until the certificate is renewed (see [Retries and
  logging](delivery-guarantees.md#retries)), and an Injector raises `InjectionError` after three
  attempts. Monitor the expiry date of client certificates as you monitor the broker's.
