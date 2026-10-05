import importlib.metadata

import pytest

import pykicak
from pykicak.abstracts import KicakAbstract

PUBLIC_API = [
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
