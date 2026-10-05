"""Message types exchanged over RabbitMQ.

Declared outside the agent abstract classes so the same message type can be
imported by both the publishing side (injector/executor) and the consuming side
(executor).
"""

from __future__ import annotations

import dataclasses
import json
import logging
from typing import Any, Self

logger = logging.getLogger(__name__)

_MAX_MESSAGE_ID_BYTES = 255  # AMQP carries the message-id property as a short string


def _check_message_id(message_id: object) -> None:
    """Raise unless `message_id` is a non-empty string of at most 255 UTF-8 bytes."""
    if not isinstance(message_id, str):
        raise TypeError(f"message_id must be a str, got {type(message_id).__name__}")
    if not message_id:
        raise ValueError("message_id must not be empty")
    if len(message_id.encode("utf-8")) > _MAX_MESSAGE_ID_BYTES:
        raise ValueError(f"message_id must be at most {_MAX_MESSAGE_ID_BYTES} bytes in UTF-8")


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class KicakMessage:
    """Base class for messages exchanged over RabbitMQ.

    Every message has a `message_id`, a keyword-only field declared here: a non-empty string of
    at most 255 UTF-8 bytes that identifies the message. Delivery is at-least-once, so a message
    can be processed more than once, and agents rely on its ID to stay idempotent. Assign the ID
    once, where the message is first created (e.g. `str(uuid.uuid4())` in an Injector's
    `generate()`), and derive the IDs of results from their input's ID (e.g. with `uuid.uuid5`),
    so that executing an input again produces results with the same IDs. There is deliberately
    no default: a generated default would give the results of a re-executed input new IDs. The
    ID is also published as the AMQP `message_id` property.

    Subclasses should be frozen dataclasses with JSON-serializable fields
    (e.g. store timestamps as ISO-8601 strings, not `datetime` objects). A subclass that defines
    `__post_init__` must call `KicakMessage.__post_init__(self)`; on Python 3.12, zero-argument
    `super()` does not work in a `slots=True` dataclass.
    """

    message_id: str

    def __post_init__(self) -> None:
        """Validate `message_id` when the message is created."""
        _check_message_id(self.message_id)

    def to_bytes(self) -> bytes:
        """Serialize this message to JSON bytes suitable for an AMQP message body.

        Validates `message_id` again, so it is checked even if a subclass's `__post_init__` does
        not call `KicakMessage.__post_init__()`.
        """
        try:
            _check_message_id(self.message_id)
            body = json.dumps(dataclasses.asdict(self)).encode("utf-8")
        except Exception:
            logger.exception("Failed to serialize message type=%s", type(self).__name__)
            raise
        logger.debug(
            "Serialized message type=%s id=%s body_bytes=%d",
            type(self).__name__,
            self.message_id,
            len(body),
        )
        return body

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Deserialize an AMQP message body previously produced by to_bytes().

        Raises `ValueError` for a body that is not UTF-8 JSON or has an empty or too long
        `message_id`, and `TypeError` for one that is not a JSON object, whose fields do not match
        this class (a missing `message_id` included), or whose `message_id` is not a string.
        Other field values are not converted or type-checked: a nested dataclass, for example,
        comes back as a `dict`.
        """
        try:
            payload: dict[str, Any] = json.loads(data.decode("utf-8"))
            message = cls(**payload)
            _check_message_id(message.message_id)
        except Exception:
            logger.exception("Invalid message for type=%s", cls.__name__)
            raise
        logger.debug(
            "Deserialized message type=%s id=%s body_bytes=%d",
            cls.__name__,
            message.message_id,
            len(data),
        )
        return message
