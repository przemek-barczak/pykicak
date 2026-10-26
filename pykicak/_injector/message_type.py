"""The message type an Injector class declares in its class header, and checking a message."""

from __future__ import annotations

from typing import cast

from pykicak._injector import logger
from pykicak.abstracts import _type_arguments, _union_members
from pykicak.messages import KicakMessage, _type_name_of

_INJECTOR_TYPE_USAGE = "KicakInjectorAbstract[MyMessage]"


def _declared_message_type(injector_class: type, generic_class: type) -> type[KicakMessage]:
    """Return the message type an Injector class declares in its class header.

    The header is `KicakInjectorAbstract[MyMessage]`, where `generic_class` is
    `KicakInjectorAbstract`. Raises `TypeError` unless it is one message class that declares its
    own `kicak_type`.
    """
    (argument,) = _type_arguments(injector_class, generic_class, usage=_INJECTOR_TYPE_USAGE)
    members = _union_members(argument)
    if len(members) > 1:
        raise TypeError(
            f"{injector_class.__name__} must declare one message type, got {argument!r}"
        )
    _type_name_of(members[0])
    return cast(type[KicakMessage], members[0])


def _serialized(
    message: KicakMessage,
    message_type: type[KicakMessage],
    *,
    injector_name: str,
    exchange_name: str,
) -> bytes:
    """Check `message`'s class and serialize it, logging a failure at ERROR and raising it.

    A message of another class than `message_type`, the one the injector named `injector_name`
    declares, raises `TypeError`: exactly that class, because a subclass would be sent with its
    own type name. A failure to serialize it is raised as it is.
    """
    if type(message) is not message_type:
        logger.error(
            "Message refused type=%s exchange=%s: %s declares its messages as %s",
            type(message).__name__,
            exchange_name,
            injector_name,
            message_type.__name__,
        )
        raise TypeError(
            f"{injector_name} declares its messages as {message_type.__name__}, "
            f"got {type(message).__name__}"
        )
    try:
        return message.to_bytes()
    except Exception:
        logger.exception(
            "Failed to serialize message type=%s exchange=%s",
            type(message).__name__,
            exchange_name,
        )
        raise
