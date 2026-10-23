"""Fixtures the Injector's tests share."""

import pytest

from pykicak.config import KicakConfig
from tests.fakes import CONNECTION_VALUES


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values=CONNECTION_VALUES)
