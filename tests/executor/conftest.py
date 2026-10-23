"""Fixtures the Executor's tests share."""

import pytest

from pykicak.config import KicakConfig
from tests.executor.helpers import FakeClock
from tests.fakes import (
    CONNECTION_VALUES,
    replace_in_pykicak,
)


@pytest.fixture
def config() -> KicakConfig:
    return KicakConfig(values=CONNECTION_VALUES)


@pytest.fixture
def sleeps(monkeypatch) -> list[float]:
    delays: list[float] = []
    monkeypatch.setattr("pykicak.executor.time.sleep", delays.append)
    return delays


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    replace_in_pykicak(monkeypatch, "_now", fake)
    return fake
