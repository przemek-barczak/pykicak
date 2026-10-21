"""Message types exchanged over RabbitMQ.

Declared outside the agent abstract classes so the same message type can be
imported by both the publishing side (injector/executor) and the consuming side
(executor).
"""

from __future__ import annotations

import dataclasses
import json
import logging
import math
from typing import Any, ClassVar, NoReturn, Self

logger = logging.getLogger(__name__)

_MAX_MESSAGE_ID_BYTES = 255  # AMQP carries the message-id property as a short string
_MAX_TYPE_NAME_BYTES = 255  # and the type property too
_TYPE_NAME_ATTRIBUTE = "kicak_type"


def _check_message_id(message_id: object) -> None:
    """Raise unless `message_id` is a non-empty string of at most 255 UTF-8 bytes."""
    if not isinstance(message_id, str):
        raise TypeError(f"message_id must be a str, got {type(message_id).__name__}")
    if not message_id:
        raise ValueError("message_id must not be empty")
    if len(message_id.encode("utf-8")) > _MAX_MESSAGE_ID_BYTES:
        raise ValueError(f"message_id must be at most {_MAX_MESSAGE_ID_BYTES} bytes in UTF-8")


def _type_name_of(message_class: object) -> str:
    """Return the type name that a message class declares itself, its `kicak_type`.

    Raises `TypeError` unless `message_class` is a subclass of `KicakMessage` that declares its
    own `kicak_type` as a class attribute: an inherited one names its base class's type, so a
    subclass would be sent as its base. Raises `ValueError` for an empty name, or one longer
    than 255 UTF-8 bytes, the limit of the AMQP `type` property it is sent as.
    """
    if not (
        isinstance(message_class, type)
        and issubclass(message_class, KicakMessage)
        and message_class is not KicakMessage
    ):
        raise TypeError(f"Expected a KicakMessage subclass, got {message_class!r}")
    name = message_class.__name__
    example = f"e.g. {_TYPE_NAME_ATTRIBUTE} = 'order.v1'"
    if _TYPE_NAME_ATTRIBUTE in {field.name for field in dataclasses.fields(message_class)}:
        raise TypeError(
            f"{name} declares {_TYPE_NAME_ATTRIBUTE} as a field; declare it as a class "
            f"attribute without an annotation, {example}"
        )
    if _TYPE_NAME_ATTRIBUTE not in vars(message_class):
        inherited = getattr(message_class, _TYPE_NAME_ATTRIBUTE, None)
        detail = "" if inherited is None else f"; the inherited {inherited!r} names another type"
        raise TypeError(f"{name} must declare its own {_TYPE_NAME_ATTRIBUTE}, {example}{detail}")
    type_name = vars(message_class)[_TYPE_NAME_ATTRIBUTE]
    if not isinstance(type_name, str):
        raise TypeError(
            f"{name}.{_TYPE_NAME_ATTRIBUTE} must be a str, got {type(type_name).__name__}"
        )
    if not type_name:
        raise ValueError(f"{name}.{_TYPE_NAME_ATTRIBUTE} must not be empty")
    if len(type_name.encode("utf-8")) > _MAX_TYPE_NAME_BYTES:
        raise ValueError(
            f"{name}.{_TYPE_NAME_ATTRIBUTE} must be at most {_MAX_TYPE_NAME_BYTES} bytes in UTF-8"
        )
    return type_name


def _fields_of(instance: Any) -> dict[str, Any]:
    """Return the fields of a dataclass instance by name, their values left as they are."""
    return {field.name: getattr(instance, field.name) for field in dataclasses.fields(instance)}


def _encode_nested(value: object) -> dict[str, Any]:
    """Encode a dataclass nested anywhere in a message as a JSON object of its fields.

    `json.dumps()` calls it for every value it cannot encode itself, so the nested values are
    traversed by the json module, as deep as it decodes, instead of by `dataclasses.asdict()`,
    whose recursion in Python stops at a fraction of that depth. Anything else is refused with
    the `TypeError` the json module raises itself.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _fields_of(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _reject_constant(constant: str) -> NoReturn:
    """Refuse `NaN`, `Infinity`, and `-Infinity`, which Python's json module accepts by default."""
    raise ValueError(f"{constant} is not valid JSON")


def _finite_float(text: str) -> float:
    """Decode a JSON number as a float, refusing one too large for it, e.g. `1e999`."""
    value = float(text)
    if not math.isfinite(value):
        # The number itself is not reported: it is part of the payload
        raise ValueError("JSON number is out of range for a float")
    return value


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class KicakMessage:
    """Base class for messages exchanged over RabbitMQ.

    Every message class names its type with `kicak_type`, a class attribute such as
    `kicak_type = "order.v1"`: a non-empty string of at most 255 UTF-8 bytes, chosen once and
    never derived from the class's name. It is sent as the AMQP `type` property of every message
    of the class, and an Executor decodes a message into the class it accepts under that name, so
    the name is the contract between the agents that publish a type and those that consume it.
    A change of the fields is a new type, a new class with a new name such as `"order.v2"`. Each
    class declares its own name: an agent refuses a class that only inherits one, because that
    one names its base class's type.

    Every message has a `message_id`, a keyword-only field declared here: a non-empty string of
    at most 255 UTF-8 bytes that identifies the message. Delivery is at-least-once, so a message
    can be processed more than once, and agents rely on its ID to stay idempotent. Assign the ID
    once, where the message is first created (e.g. `str(uuid.uuid4())` in an Injector's
    `generate()`), and derive the IDs of results from their input's ID (e.g. with `uuid.uuid5`),
    so that executing an input again produces results with the same IDs. There is deliberately
    no default: a generated default would give the results of a re-executed input new IDs. The
    ID is also published as the AMQP `message_id` property.

    Subclasses should be frozen dataclasses with JSON-serializable fields
    (e.g. store timestamps as ISO-8601 strings, not `datetime` objects). Floats must be finite:
    JSON has no NaN or infinity, so `to_bytes()` refuses them. A subclass that defines
    `__post_init__` must call `KicakMessage.__post_init__(self)`; on Python 3.12, zero-argument
    `super()` does not work in a `slots=True` dataclass.
    """

    kicak_type: ClassVar[str]
    """The name of this message type, sent as the AMQP `type` property; declared by each class."""

    message_id: str

    def __post_init__(self) -> None:
        """Validate `message_id` when the message is created."""
        _check_message_id(self.message_id)

    def to_bytes(self) -> bytes:
        """Serialize this message to JSON bytes suitable for an AMQP message body.

        Validates `message_id` again, so it is checked even if a subclass's `__post_init__` does
        not call `KicakMessage.__post_init__()`. Raises `ValueError` for a NaN or infinite float
        anywhere in the message: JSON has no such values, so Python's default `NaN` and
        `Infinity` output would be refused by the JSON parsers of other languages. A dataclass
        nested in the message, also inside lists, tuples, and dicts, becomes a JSON object of
        its fields. Every character outside ASCII is written as a `\\u` escape, so the body is
        ASCII.
        """
        try:
            _check_message_id(self.message_id)
            body = json.dumps(
                _fields_of(self),
                allow_nan=False,
                default=_encode_nested,
                # Escaping costs bytes, but it can write every string decoding produces, a lone
                # surrogate such as "\ud800" included, which UTF-8 cannot encode
                ensure_ascii=True,
            ).encode("utf-8")
        except Exception:
            # The caller logs the ERROR with its own context: the Injector, or the Executor that
            # dead-letters the input whose result this is
            logger.debug("Failed to serialize message type=%s", type(self).__name__, exc_info=True)
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

        Raises `ValueError` for a body that is not UTF-8 JSON (including `NaN`, `Infinity`, and
        `-Infinity`, which Python's json module would accept), has a number too large for a float,
        is nested too deeply to decode, or has an empty or too long `message_id`, and `TypeError`
        for one that is not a JSON object, whose fields do not match this class (a missing
        `message_id` included), or whose `message_id` is not a string. Other field values are
        not converted or type-checked: a nested dataclass, for example, comes back as a `dict`.

        A decoded message can be serialized again with `to_bytes()`, as long as its class keeps
        to the message contract, e.g. its `__post_init__` does not replace a value with one JSON
        cannot hold, and within the limits of Python's json module: it encodes nesting about as
        deep as it decodes, though not always exactly as deep. The bytes can differ from `data`,
        for example in whitespace or escapes, but they hold the same JSON value.
        """
        try:
            try:
                payload: dict[str, Any] = json.loads(
                    data.decode("utf-8"),
                    parse_constant=_reject_constant,
                    parse_float=_finite_float,
                )
            except RecursionError as error:
                # Deep nesting exhausts the decoder's recursion limit: the body is at fault
                raise ValueError("JSON body is nested too deeply to decode") from error
            message = cls(**payload)
            _check_message_id(message.message_id)
        except Exception:
            # The caller logs the ERROR with its own context, e.g. the Executor that dead-letters
            # the delivery
            logger.debug("Invalid message for type=%s", cls.__name__, exc_info=True)
            raise
        logger.debug(
            "Deserialized message type=%s id=%s body_bytes=%d",
            cls.__name__,
            message.message_id,
            len(data),
        )
        return message
