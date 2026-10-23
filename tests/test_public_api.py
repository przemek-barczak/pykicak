import importlib
import importlib.metadata
import pickle

import pytest

import pykicak
from pykicak.abstracts import KicakAbstract

PUBLIC_API = [
    "DeadLetterCounts",
    "ExecutorCounts",
    "ExecutorInfo",
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


def test_package_exports_exactly_the_public_api():
    assert sorted(pykicak.__all__) == sorted(PUBLIC_API)


def test_every_public_name_is_importable():
    for name in PUBLIC_API:
        assert hasattr(pykicak, name), name


def test_internal_base_class_is_not_exported():
    assert not hasattr(pykicak, "KicakAbstract")


def test_node_types_share_the_internal_base_class():
    assert issubclass(pykicak.KicakInjectorAbstract, KicakAbstract)
    assert issubclass(pykicak.KicakExecutorAbstract, KicakAbstract)


@pytest.mark.parametrize(
    "name", [name for name in PUBLIC_API if isinstance(getattr(pykicak, name), type)]
)
def test_every_public_class_presents_itself_as_defined_in_a_public_module(name):
    public_class = getattr(pykicak, name)
    module = public_class.__module__

    # Not a private module such as pykicak._executor.status, where some of them are defined
    assert not any(part.startswith("_") for part in module.split("."))
    # Found there by name, e.g. by pickle and by help()
    assert getattr(importlib.import_module(module), public_class.__qualname__) is public_class


def test_public_status_types_survive_pickling():
    counts = pykicak.ExecutorCounts(
        completed=2, dead_lettered=pykicak.DeadLetterCounts(retries_exhausted=1)
    )

    assert pickle.loads(pickle.dumps(counts)) == counts
    assert (
        pickle.loads(pickle.dumps(pykicak.ExecutorStatus.STOPPED)) is pykicak.ExecutorStatus.STOPPED
    )
    assert pickle.loads(pickle.dumps(pykicak.QueueType.QUORUM)) is pykicak.QueueType.QUORUM


def test_star_import_provides_every_public_name():
    namespace: dict[str, object] = {}
    exec("from pykicak import *", namespace)

    assert set(PUBLIC_API) <= namespace.keys()


def test_version_matches_installed_distribution():
    try:
        installed = importlib.metadata.version("pykicak")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("pykicak is not installed, e.g. tests run from a plain checkout")

    assert pykicak.__version__ == installed
