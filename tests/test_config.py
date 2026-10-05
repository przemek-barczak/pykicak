import os

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
        }
    )

    assert config.rabbitmq_username == "guest"
    assert config.rabbitmq_password == "guest"
    assert config.rabbitmq_host == "localhost"
    assert config.rabbitmq_port == 5672
    assert config.rabbitmq_virtual_host == "/"


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
