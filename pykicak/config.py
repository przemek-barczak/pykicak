"""Connection configuration: .kicak files and environment variables."""

from __future__ import annotations

import codecs
import dataclasses
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Self

logger = logging.getLogger(__name__)

_MAX_PORT = 65535
_BOOLEAN_VALUES = {"true": True, "false": False}

# The keys the library reads, and the only variables from_env() takes from the environment
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


class KicakConfigError(Exception):
    """Raised for a missing, unreadable, or malformed .kicak file, an environment variable that is
    not valid UTF-8, or a missing or invalid key."""


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class KicakConfig:
    """RabbitMQ connection configuration, from a .kicak file, environment variables, or a mapping.

    Immutable and hashable: `values` is a read-only copy of the mapping the config is created
    with, so changing that mapping afterwards does not change the config. The repr lists only
    the keys: values such as RABBIT_MQ_PASSWORD are secret, and reprs end up in logs and crash
    reports.
    """

    values: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    def __hash__(self) -> int:
        return hash(frozenset(self.values.items()))

    def __reduce__(self) -> tuple[type[Self], tuple[dict[str, str]]]:
        # A mapping proxy cannot be pickled or deep-copied, so rebuild the config from a dict
        return (type(self), (dict(self.values),))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(keys={sorted(self.values)!r})"

    @classmethod
    def from_file(cls, path: str | Path) -> Self:
        """Parse a .kicak file into a KicakConfig, raising KicakConfigError on any problem."""
        file_path = Path(path)
        logger.debug("Reading Kicak config from %s", file_path)
        try:
            content = file_path.read_bytes()
        except FileNotFoundError:
            logger.error("Kicak config file not found: %s", file_path)
            raise KicakConfigError(f"Kicak config file not found: {file_path}") from None
        except OSError as exc:
            # e.g. a directory or missing read permission; the OS error holds no file contents
            logger.error("Cannot read Kicak config file %s: %s", file_path, exc.strerror)
            raise KicakConfigError(
                f"Cannot read Kicak config file {file_path}: {exc.strerror}"
            ) from exc

        # Always UTF-8, so a file reads the same on every platform, without the byte order mark
        # some Windows editors write, which would otherwise prefix the first key. It is removed
        # before decoding, as "utf-8-sig" would, so an error's offset counts in `content` itself.
        content = content.removeprefix(codecs.BOM_UTF8)
        text: str | None
        bad_line_number = 0
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            text = None
            bad_line_number = _line_number_at(content, exc.start)
        # Raised outside the except block so the error has no __context__: the UnicodeDecodeError
        # holds the whole file, secrets included.
        if text is None:
            logger.error(
                "Invalid UTF-8 on line %d of Kicak config file %s", bad_line_number, file_path
            )
            raise KicakConfigError(f"Invalid UTF-8 on line {bad_line_number} of {file_path}")

        values: dict[str, str] = {}
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            key = key.strip()
            if not separator or not key:
                # Report only the line number: the line may hold a secret, e.g. a password
                # written without its key.
                logger.error("Invalid line %d in Kicak config file %s", line_number, file_path)
                raise KicakConfigError(
                    f"Invalid line {line_number} in {file_path}: expected KEY=VALUE"
                )
            if key in values:
                # Keys are not secret (the repr lists them); a second value would silently win
                logger.error(
                    "Duplicate key %s on line %d of Kicak config file %s",
                    key,
                    line_number,
                    file_path,
                )
                raise KicakConfigError(
                    f"Duplicate key '{key}' on line {line_number} of {file_path}"
                )
            values[key] = value.strip()

        logger.debug(
            "Loaded Kicak config from %s with keys=%s",
            file_path,
            sorted(values),
        )
        return cls(values=values)

    @classmethod
    def from_env(cls) -> Self:
        """Read the connection keys from the environment variables of the same names.

        Only the keys the library reads, RABBIT_MQ_USERNAME to RABBIT_MQ_KEY_FILE, are taken, not
        the rest of the environment. Values are stripped of surrounding whitespace, as in a .kicak
        file. A variable that is not set leaves its key missing, which raises KicakConfigError when
        the key is read, as for a file. A value that is not valid UTF-8 raises KicakConfigError.
        """
        values: dict[str, str] = {}
        for key in _CONNECTION_KEYS:
            value = os.environ.get(key)
            if value is None:
                continue
            # On POSIX, Python decodes invalid UTF-8 in the environment to lone surrogates, which
            # cannot be sent to RabbitMQ; report only the key, since the value may be a secret.
            if not _is_valid_utf8(value):
                logger.error("Invalid UTF-8 in environment variable %s", key)
                raise KicakConfigError(f"Environment variable {key} is not valid UTF-8")
            values[key] = value.strip()

        logger.debug("Loaded Kicak config from the environment with keys=%s", sorted(values))
        return cls(values=values)

    def require(self, key: str) -> str:
        """Return the value for `key`, or raise KicakConfigError if it is not set."""
        try:
            value = self.values[key]
        except KeyError:
            logger.error("Missing required Kicak config key %s", key)
            raise KicakConfigError(f"Missing required key '{key}'") from None
        logger.debug("Resolved required Kicak config key %s", key)
        return value

    def get(self, key: str, default: str | None = None) -> str | None:
        """Return the value for `key`, or `default` if it is not set."""
        value = self.values.get(key, default)
        logger.debug("Read optional Kicak config key %s (present=%s)", key, key in self.values)
        return value

    @property
    def rabbitmq_username(self) -> str:
        """The RABBIT_MQ_USERNAME value."""
        return self.require("RABBIT_MQ_USERNAME")

    @property
    def rabbitmq_password(self) -> str:
        """The RABBIT_MQ_PASSWORD value."""
        return self.require("RABBIT_MQ_PASSWORD")

    @property
    def rabbitmq_host(self) -> str:
        """The RABBIT_MQ_HOST value."""
        return self.require("RABBIT_MQ_HOST")

    @property
    def rabbitmq_port(self) -> int:
        """The RABBIT_MQ_PORT value, parsed as an int from 1 to 65535."""
        try:
            port = int(self.require("RABBIT_MQ_PORT"))
        except ValueError:
            port = 0  # reported below; the invalid value itself must not appear in the error
        if not 1 <= port <= _MAX_PORT:
            logger.error("Invalid Kicak config key RABBIT_MQ_PORT")
            raise KicakConfigError(f"RABBIT_MQ_PORT must be an integer from 1 to {_MAX_PORT}")
        return port

    @property
    def rabbitmq_virtual_host(self) -> str:
        """The RABBIT_MQ_VIRTUAL_HOST value."""
        return self.require("RABBIT_MQ_VIRTUAL_HOST")

    @property
    def rabbitmq_tls(self) -> bool:
        """The RABBIT_MQ_TLS value: True to connect with TLS, False for plain, unencrypted AMQP.

        Required, so that an unencrypted connection is always an explicit choice. The value is
        `true` or `false`, in any letter case; anything else raises `KicakConfigError`, so that a
        typo never turns encryption off.
        """
        tls = _BOOLEAN_VALUES.get(self.require("RABBIT_MQ_TLS").lower())
        if tls is None:
            logger.error("Invalid Kicak config key RABBIT_MQ_TLS")
            raise KicakConfigError("RABBIT_MQ_TLS must be true or false")
        return tls

    @property
    def rabbitmq_ca_file(self) -> str | None:
        """The optional RABBIT_MQ_CA_FILE value: a file of PEM CA certificates, or None if unset.

        With TLS, the broker's certificate must then be issued by one of these CAs instead of one
        the system trusts. An empty value raises `KicakConfigError`: remove the key instead.
        """
        return self._optional_file("RABBIT_MQ_CA_FILE", "to trust the system's CA certificates")

    @property
    def rabbitmq_cert_file(self) -> str | None:
        """The optional RABBIT_MQ_CERT_FILE value: the client certificate, or None if unset.

        A PEM file with the certificate agents present to a broker that asks for one (mutual TLS),
        optionally followed by its intermediate CA certificates. With TLS only, and together with
        RABBIT_MQ_KEY_FILE. An empty value raises `KicakConfigError`: remove the key instead.
        """
        return self._optional_file("RABBIT_MQ_CERT_FILE", "to connect without a client certificate")

    @property
    def rabbitmq_key_file(self) -> str | None:
        """The optional RABBIT_MQ_KEY_FILE value: the client certificate's key, or None if unset.

        A PEM file with the unencrypted private key of the certificate in RABBIT_MQ_CERT_FILE. An
        empty value raises `KicakConfigError`: remove the key instead.
        """
        return self._optional_file("RABBIT_MQ_KEY_FILE", "to connect without a client certificate")

    def _optional_file(self, key: str, without_it: str) -> str | None:
        """Return the path in `key`, None if it is not set, or raise if it is empty."""
        path = self.get(key)
        if path == "":
            logger.error("Invalid Kicak config key %s", key)
            raise KicakConfigError(f"{key} must not be empty; remove it {without_it}")
        return path


def _is_valid_utf8(value: str) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False  # returned, not raised, so no error carries the value as its context
    return True


def _line_number_at(content: bytes, offset: int) -> int:
    """Return the number, from 1, of the line of `content` that holds the byte at `offset`.

    Lines are numbered as `from_file()` numbers them, split by `str.splitlines()`, so a file whose
    lines end in carriage returns alone is reported the same way. The bytes before `offset` must
    be valid UTF-8, as those before a decoding error are.
    """
    # A character after them makes the line holding `offset` count even while it is still empty
    return len((content[:offset].decode("utf-8") + "_").splitlines())
