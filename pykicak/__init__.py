"""pykicak: build RabbitMQ-connected agent nodes with idempotent message processing.

Delivery is at-least-once, so pykicak is designed for idempotent agents only: every Injector and
Executor built on it must leave the same state when a message is processed more than once.
"""

import logging

from pykicak.config import KicakConfig, KicakConfigError
from pykicak.executor import (
    ExecutorStatus,
    KicakExecutorAbstract,
    MalformedMessageError,
    QueueType,
    TransientProcessingError,
)
from pykicak.injector import InjectionError, KicakInjectorAbstract
from pykicak.messages import KicakMessage

__version__ = "0.1.0"  # the only place the version is set; pyproject.toml reads it from here

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "ExecutorStatus",
    "InjectionError",
    "KicakConfig",
    "KicakConfigError",
    "KicakExecutorAbstract",
    "KicakInjectorAbstract",
    "KicakMessage",
    "MalformedMessageError",
    "QueueType",
    "TransientProcessingError",
    "__version__",
]
