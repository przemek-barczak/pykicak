import copy
import dataclasses
import os
import pickle

import pytest

from pykicak.config import KicakConfig, KicakConfigError


def test_from_file_parses_key_value_pairs(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("RABBIT_MQ_HOST=localhost\nRABBIT_MQ_PORT=5672\n")

    config = KicakConfig.from_file(kicak_file)

    assert config.values == {"RABBIT_MQ_HOST": "localhost", "RABBIT_MQ_PORT": "5672"}


def test_from_file_debug_logs_keys_without_values(tmp_path, caplog):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("RABBIT_MQ_PASSWORD=secret-value\n")
    caplog.set_level("DEBUG", logger="pykicak.config")

    KicakConfig.from_file(kicak_file)

    assert any("RABBIT_MQ_PASSWORD" in record.message for record in caplog.records)
    assert "secret-value" not in caplog.text


def test_from_file_ignores_blank_lines_and_comments(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("\n# a comment\nRABBIT_MQ_HOST=localhost\n\n  # indented comment\n")

    config = KicakConfig.from_file(kicak_file)

    assert config.values == {"RABBIT_MQ_HOST": "localhost"}


def test_from_file_strips_whitespace_around_key_and_value(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("  RABBIT_MQ_HOST = localhost  \n")

    config = KicakConfig.from_file(kicak_file)

    assert config.values == {"RABBIT_MQ_HOST": "localhost"}


def test_from_file_splits_only_on_first_equals(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("SOME_KEY=a=b=c\n")

    config = KicakConfig.from_file(kicak_file)

    assert config.values == {"SOME_KEY": "a=b=c"}


def test_from_file_raises_on_missing_file(tmp_path):
    with pytest.raises(KicakConfigError):
        KicakConfig.from_file(tmp_path / "does-not-exist")


def test_from_file_raises_on_malformed_line(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("NOT_A_KEY_VALUE_LINE\n")

    with pytest.raises(KicakConfigError):
        KicakConfig.from_file(kicak_file)


def test_require_returns_value_when_present():
    config = KicakConfig(values={"KEY": "value"})

    assert config.require("KEY") == "value"


def test_require_raises_when_key_missing():
    config = KicakConfig(values={})

    with pytest.raises(KicakConfigError):
        config.require("MISSING")


def test_get_returns_default_when_key_missing():
    config = KicakConfig(values={})

    assert config.get("MISSING", "fallback") == "fallback"


def test_rabbitmq_typed_accessors():
    config = KicakConfig(
        values={
            "RABBIT_MQ_USERNAME": "guest",
            "RABBIT_MQ_PASSWORD": "guest",
            "RABBIT_MQ_HOST": "localhost",
            "RABBIT_MQ_PORT": "5672",
            "RABBIT_MQ_VIRTUAL_HOST": "/",
            "RABBIT_MQ_TLS": "true",
            "RABBIT_MQ_CA_FILE": "/etc/pykicak/ca.pem",
            "RABBIT_MQ_CERT_FILE": "/etc/pykicak/agent.pem",
            "RABBIT_MQ_KEY_FILE": "/etc/pykicak/agent.key",
        }
    )

    assert config.rabbitmq_username == "guest"
    assert config.rabbitmq_password == "guest"
    assert config.rabbitmq_host == "localhost"
    assert config.rabbitmq_port == 5672
    assert config.rabbitmq_virtual_host == "/"
    assert config.rabbitmq_tls is True
    assert config.rabbitmq_ca_file == "/etc/pykicak/ca.pem"
    assert config.rabbitmq_cert_file == "/etc/pykicak/agent.pem"
    assert config.rabbitmq_key_file == "/etc/pykicak/agent.key"


@pytest.mark.parametrize(
    ("value", "expected"),
    [("true", True), ("false", False), ("TRUE", True), ("False", False)],
)
def test_rabbitmq_tls_accepts_true_or_false_in_any_letter_case(value, expected):
    config = KicakConfig(values={"RABBIT_MQ_TLS": value})

    assert config.rabbitmq_tls is expected


def test_rabbitmq_tls_is_required():
    config = KicakConfig(values={})

    with pytest.raises(KicakConfigError, match="Missing required key 'RABBIT_MQ_TLS'"):
        _ = config.rabbitmq_tls


@pytest.mark.parametrize("value", ["yes", "on", "1", "0", "ture", "enabled", ""])
def test_rabbitmq_tls_rejects_other_values_without_echoing_them(value, caplog):
    config = KicakConfig(values={"RABBIT_MQ_TLS": value})

    with pytest.raises(KicakConfigError, match="RABBIT_MQ_TLS must be true or false") as exc_info:
        _ = config.rabbitmq_tls

    assert exc_info.value.__context__ is None
    if len(value) > 2:  # shorter values occur by chance, e.g. in the line numbers of log records
        assert value not in str(exc_info.value)
        assert value not in caplog.text


@pytest.mark.parametrize(
    "accessor", ["rabbitmq_ca_file", "rabbitmq_cert_file", "rabbitmq_key_file"]
)
def test_rabbitmq_tls_files_are_optional(accessor):
    assert getattr(KicakConfig(values={}), accessor) is None


@pytest.mark.parametrize(
    ("key", "accessor", "remedy"),
    [
        ("RABBIT_MQ_CA_FILE", "rabbitmq_ca_file", "to trust the system's CA certificates"),
        ("RABBIT_MQ_CERT_FILE", "rabbitmq_cert_file", "to connect without a client certificate"),
        ("RABBIT_MQ_KEY_FILE", "rabbitmq_key_file", "to connect without a client certificate"),
    ],
)
def test_rabbitmq_tls_files_reject_an_empty_value(key, accessor, remedy):
    config = KicakConfig(values={key: ""})

    with pytest.raises(KicakConfigError) as exc_info:
        getattr(config, accessor)

    assert str(exc_info.value) == f"{key} must not be empty; remove it {remedy}"


def test_malformed_line_error_does_not_echo_the_line(tmp_path, caplog):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("RABBIT_MQ_HOST=localhost\n# comment\nsecret-value\n")

    with pytest.raises(KicakConfigError) as exc_info:
        KicakConfig.from_file(kicak_file)

    assert "line 3" in str(exc_info.value)
    assert "secret-value" not in str(exc_info.value)
    assert "secret-value" not in caplog.text


def test_repr_lists_keys_without_values():
    config = KicakConfig(values={"RABBIT_MQ_PASSWORD": "secret-value", "RABBIT_MQ_HOST": "host"})

    assert repr(config) == "KicakConfig(keys=['RABBIT_MQ_HOST', 'RABBIT_MQ_PASSWORD'])"
    assert "secret-value" not in str(config)


def test_values_are_a_read_only_copy():
    source = {"RABBIT_MQ_HOST": "localhost"}
    config = KicakConfig(values=source)

    source["RABBIT_MQ_HOST"] = "elsewhere"
    with pytest.raises(TypeError):
        config.values["RABBIT_MQ_HOST"] = "changed"  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.values = {}  # type: ignore[misc]

    assert config.values == {"RABBIT_MQ_HOST": "localhost"}


def test_config_is_hashable_and_compares_by_values():
    config = KicakConfig(values={"RABBIT_MQ_HOST": "localhost", "RABBIT_MQ_PORT": "5672"})
    same = KicakConfig(values={"RABBIT_MQ_PORT": "5672", "RABBIT_MQ_HOST": "localhost"})
    other = KicakConfig(values={"RABBIT_MQ_HOST": "elsewhere", "RABBIT_MQ_PORT": "5672"})

    assert config == same
    assert hash(config) == hash(same)
    assert config != other
    assert {config, same, other} == {config, other}


@pytest.mark.parametrize(
    "duplicate",
    [lambda config: pickle.loads(pickle.dumps(config)), copy.copy, copy.deepcopy],
    ids=["pickle", "copy", "deepcopy"],
)
def test_config_survives_pickling_and_copying_and_stays_read_only(duplicate):
    # Pickling is how a config reaches a multiprocessing worker
    config = KicakConfig(values={"RABBIT_MQ_HOST": "localhost"})

    duplicated = duplicate(config)

    assert duplicated == config
    with pytest.raises(TypeError):
        duplicated.values["RABBIT_MQ_HOST"] = "changed"


def test_from_file_decodes_utf8(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_bytes("RABBIT_MQ_PASSWORD=zażółć\n".encode())

    config = KicakConfig.from_file(kicak_file)

    assert config.values == {"RABBIT_MQ_PASSWORD": "zażółć"}


def test_from_file_ignores_utf8_byte_order_mark(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_bytes(b"\xef\xbb\xbfRABBIT_MQ_HOST=localhost\n")

    config = KicakConfig.from_file(kicak_file)

    assert config.values == {"RABBIT_MQ_HOST": "localhost"}


def test_from_file_rejects_invalid_utf8_without_leaking_contents(tmp_path, caplog):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_bytes(b"RABBIT_MQ_PASSWORD=secret-value\nRABBIT_MQ_HOST=caf\xe9\n")

    with pytest.raises(KicakConfigError) as exc_info:
        KicakConfig.from_file(kicak_file)

    assert "line 2" in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert "secret-value" not in str(exc_info.value)
    assert "secret-value" not in caplog.text


def test_from_file_raises_when_path_is_a_directory(tmp_path):
    with pytest.raises(KicakConfigError, match="Cannot read Kicak config file"):
        KicakConfig.from_file(tmp_path)


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs a non-root POSIX user"
)
def test_from_file_raises_when_file_is_not_readable(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("RABBIT_MQ_HOST=localhost\n")
    kicak_file.chmod(0)
    try:
        with pytest.raises(KicakConfigError, match="Cannot read Kicak config file"):
            KicakConfig.from_file(kicak_file)
    finally:
        kicak_file.chmod(0o600)


def test_from_file_rejects_line_without_key(tmp_path):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("RABBIT_MQ_HOST=localhost\n=secret-value\n")

    with pytest.raises(KicakConfigError, match="Invalid line 2") as exc_info:
        KicakConfig.from_file(kicak_file)

    assert "secret-value" not in str(exc_info.value)


def test_from_file_rejects_duplicate_key_without_echoing_values(tmp_path, caplog):
    kicak_file = tmp_path / ".kicak"
    kicak_file.write_text("RABBIT_MQ_PASSWORD=first-secret\nRABBIT_MQ_PASSWORD=second-secret\n")

    with pytest.raises(
        KicakConfigError, match="Duplicate key 'RABBIT_MQ_PASSWORD' on line 2"
    ) as exc_info:
        KicakConfig.from_file(kicak_file)

    for secret in ("first-secret", "second-secret"):
        assert secret not in str(exc_info.value)
        assert secret not in caplog.text


@pytest.mark.parametrize("port", ["not-a-port", "", "0", "65536", "-1", "56.72"])
def test_rabbitmq_port_rejects_invalid_value_without_echoing_it(port, caplog):
    config = KicakConfig(values={"RABBIT_MQ_PORT": port})

    with pytest.raises(KicakConfigError, match="RABBIT_MQ_PORT must be an integer") as exc_info:
        _ = config.rabbitmq_port

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    if len(port) > 2:  # shorter values occur by chance, e.g. in the line numbers of log records
        assert port not in str(exc_info.value)
        assert port not in caplog.text


@pytest.mark.parametrize("port", ["1", "5672", "65535"])
def test_rabbitmq_port_accepts_valid_tcp_ports(port):
    assert KicakConfig(values={"RABBIT_MQ_PORT": port}).rabbitmq_port == int(port)


_CONNECTION_KEYS = (
    "RABBIT_MQ_USERNAME",
    "RABBIT_MQ_PASSWORD",
    "RABBIT_MQ_HOST",
    "RABBIT_MQ_PORT",
    "RABBIT_MQ_VIRTUAL_HOST",
    "RABBIT_MQ_TLS",
    "RABBIT_MQ_CA_FILE",
    "RABBIT_MQ_CERT_FILE",
    "RABBIT_MQ_KEY_FILE",
)


@pytest.fixture
def environment(monkeypatch):
    """The process environment, without any connection key the test run may have set."""
    for key in _CONNECTION_KEYS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_from_env_reads_every_connection_key(environment):
    values = {
        "RABBIT_MQ_USERNAME": "guest",
        "RABBIT_MQ_PASSWORD": "secret-value",
        "RABBIT_MQ_HOST": "localhost",
        "RABBIT_MQ_PORT": "5671",
        "RABBIT_MQ_VIRTUAL_HOST": "/",
        "RABBIT_MQ_TLS": "true",
        "RABBIT_MQ_CA_FILE": "/etc/pykicak/ca.pem",
        "RABBIT_MQ_CERT_FILE": "/etc/pykicak/agent.pem",
        "RABBIT_MQ_KEY_FILE": "/etc/pykicak/agent.key",
    }
    for key, value in values.items():
        environment.setenv(key, value)

    config = KicakConfig.from_env()

    assert config.values == values
    assert config.rabbitmq_port == 5671
    assert config.rabbitmq_tls is True


def test_from_env_takes_no_other_variables(environment):
    environment.setenv("RABBIT_MQ_HOST", "localhost")
    environment.setenv("RABBIT_MQ_SERVICE_HOST", "10.0.0.1")  # e.g. set by Kubernetes
    environment.setenv("APP_SECRET", "secret-value")

    config = KicakConfig.from_env()

    assert config.values == {"RABBIT_MQ_HOST": "localhost"}
    assert repr(config) == "KicakConfig(keys=['RABBIT_MQ_HOST'])"


def test_from_env_leaves_unset_keys_missing(environment):
    config = KicakConfig.from_env()

    assert config.values == {}
    with pytest.raises(KicakConfigError, match="Missing required key 'RABBIT_MQ_HOST'"):
        _ = config.rabbitmq_host
    assert config.rabbitmq_ca_file is None


def test_from_env_strips_whitespace_around_values(environment):
    # e.g. a Kubernetes secret created from a file that ends with a newline
    environment.setenv("RABBIT_MQ_HOST", "  localhost ")
    environment.setenv("RABBIT_MQ_PASSWORD", "secret-value\n")

    config = KicakConfig.from_env()

    assert config.values == {"RABBIT_MQ_HOST": "localhost", "RABBIT_MQ_PASSWORD": "secret-value"}


def test_from_env_keeps_an_empty_value(environment):
    environment.setenv("RABBIT_MQ_CA_FILE", "")

    config = KicakConfig.from_env()

    assert config.values == {"RABBIT_MQ_CA_FILE": ""}
    with pytest.raises(KicakConfigError, match="RABBIT_MQ_CA_FILE must not be empty"):
        _ = config.rabbitmq_ca_file


@pytest.mark.skipif(
    os.name != "posix", reason="POSIX decodes invalid UTF-8 in the environment to lone surrogates"
)
def test_from_env_rejects_invalid_utf8_without_leaking_the_value(environment, caplog):
    environment.setenv("RABBIT_MQ_HOST", "localhost")
    # Stored as the bytes b"secret-value-\xe9", which Python reads back as this lone surrogate
    environment.setenv("RABBIT_MQ_PASSWORD", "secret-value-\udce9")
    assert os.environb[b"RABBIT_MQ_PASSWORD"] == b"secret-value-\xe9"

    with pytest.raises(
        KicakConfigError, match="Environment variable RABBIT_MQ_PASSWORD is not valid UTF-8"
    ) as exc_info:
        KicakConfig.from_env()

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert "secret-value" not in str(exc_info.value)
    assert "secret-value" not in caplog.text


def test_from_env_debug_logs_keys_without_values(environment, caplog):
    environment.setenv("RABBIT_MQ_PASSWORD", "secret-value")
    caplog.set_level("DEBUG", logger="pykicak.config")

    KicakConfig.from_env()

    assert any(
        "environment" in record.message and "RABBIT_MQ_PASSWORD" in record.message
        for record in caplog.records
    )
    assert "secret-value" not in caplog.text
