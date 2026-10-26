# Development

Install the package in editable mode with the development tools (pytest, pytest-cov, trustme,
cryptography, ruff, mypy, build, and twine), then run the test suite and the checks that CI runs:

```bash
pip install -e . --group dev   # pip 25.1 or newer
pytest --cov                   # fails below 85% statement coverage
ruff check .
ruff format --check .
mypy
```

The tools are pinned to exact versions in `[dependency-groups]` of `pyproject.toml`, so that every
run checks with the same ones; Dependabot proposes updates. `mypy` checks `pykicak` and `examples`
in strict mode, and the code in `tests` and `.github/scripts` too, without requiring annotations on
test functions and fixtures.

`pytest` excludes integration tests, which require a live RabbitMQ broker (e.g.
`docker run -p 5672:5672 rabbitmq`). They connect with the `.kicak` file in the working directory,
or the one whose path is in the `KICAK_CONFIG_PATH` environment variable; with the container above,
a copy of `.kicak.example` works as it is. Run them explicitly with:

```bash
pytest -m integration
```

Two of them check how an Executor handles RabbitMQ's `consumer_timeout` ending a delivery: on a
classic queue with RabbitMQ 4.2, and on a quorum queue with RabbitMQ 4.3 and newer, which evaluate
the timeout for quorum queues only. RabbitMQ 4.2 checks that timeout only once a minute by default,
so each of them takes about a minute there. A broker started with
`[{rabbit, [{channel_tick_interval, 1000}]}].` in `/etc/rabbitmq/advanced.config` checks every
second, and they take a few seconds; the CI integration job starts its brokers that way, with
RabbitMQ 4.2 and 4.4. `rabbitmq.conf` has no setting for this interval. On RabbitMQ 4.4 they take
about 15 seconds whatever the interval.

The TLS integration tests also need a broker with a TLS listener, and the path of a `.kicak` file
for it, with `RABBIT_MQ_TLS=true` and `RABBIT_MQ_CA_FILE`, in the `KICAK_TLS_CONFIG_PATH`
environment variable; without it they are skipped. The client certificate tests also need
`RABBIT_MQ_CERT_FILE` and `RABBIT_MQ_KEY_FILE` in that file, and a broker that verifies the
certificate of a client that presents one but also accepts clients without one; without the keys
they are skipped. `python -m tests.integration.tls_broker_files <directory>` writes a test CA, a
broker certificate for `localhost`, a client certificate, and such a broker's TLS configuration;
the integration job in `.github/workflows/ci.yml` shows how to start RabbitMQ with them.

## CI and releases

GitHub Actions runs these checks, the unit tests on every supported Python version and with the
oldest supported pika, and the integration tests against RabbitMQ, on every push to `main` and every
pull request. Releases are published to PyPI from GitHub Actions alone, when a version tag is pushed
and a maintainer approves it. [`RELEASING.md`](../RELEASING.md) describes the workflows, how to
release a version, and how to recover from a failed release.
