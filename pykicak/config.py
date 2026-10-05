"""Parsing of .kicak configuration files."""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_MAX_PORT = 65535


class KicakConfigError(Exception):
    """Raised for a missing, unreadable, or malformed .kicak file, or a missing or invalid key."""


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class KicakConfig:
    """Parsed contents of a .kicak file.

    The repr lists only the keys: values such as RABBIT_MQ_PASSWORD are secret, and reprs end up
    in logs and crash reports.
    """

    values: dict[str, str]

    def __repr__(self) -> str:
        return f"{type(self).__name__}(keys={sorted(self.values)!r})"

    @classmethod
    def from_file(cls, path: str | Path) -> KicakConfig:
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

        # Always UTF-8, so a file reads the same on every platform; "utf-8-sig" also drops the
        # byte order mark some Windows editors write, which would otherwise prefix the first key.
        text: str | None
        bad_line_number = 0
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            text = None
            bad_line_number = content[: exc.start].count(b"\n") + 1
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
