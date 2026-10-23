"""Release checks of the CI and release workflows: the tag, the version, and the distributions.

    python .github/scripts/release_checks.py version      # print the package version
    python .github/scripts/release_checks.py tag v0.3.0   # check a release tag
    python .github/scripts/release_checks.py dist dist    # check the built distributions

Standard library only, so it runs before anything is installed, and it reads the version from
pykicak/__init__.py without importing the package.
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import sys
import tarfile
import zipfile
from email.parser import HeaderParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = "pykicak"

# X.Y.Z, or a pre-release X.Y.ZaN, X.Y.ZbN, X.Y.ZrcN, in the normalized form of PEP 440, so that
# the tag, the version, and the distribution file names agree character for character
VERSION_PATTERN = re.compile(r"\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?")

# What the sdist may hold besides the package: setuptools adds these, and MANIFEST.in prunes
# everything else (tests, examples, docs, the config template)
SDIST_EXTRAS = {
    "LICENSE",
    "MANIFEST.in",
    "PKG-INFO",
    "README.md",
    "pyproject.toml",
    "setup.cfg",
    f"{PACKAGE}.egg-info",
}


class CheckError(Exception):
    """A check failed; the message says which and why."""


def package_version() -> str:
    """Return `__version__` from pykicak/__init__.py, the only place the version is set."""
    tree = ast.parse((ROOT / PACKAGE / "__init__.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__version__"
                for target in node.targets
            )
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            return node.value.value
    raise CheckError(f"{PACKAGE}/__init__.py assigns no string literal to __version__")


def check_tag(tag: str) -> str:
    """Check that `tag` releases the package version, and that the changelog has its section."""
    version = package_version()
    if not VERSION_PATTERN.fullmatch(version):
        raise CheckError(
            f"version {version!r} is not X.Y.Z or a pre-release X.Y.Z(a|b|rc)N in PEP 440 form"
        )
    if tag != f"v{version}":
        raise CheckError(
            f"tag {tag!r} does not match the package version {version!r}: "
            f"release it with the tag 'v{version}', or change __version__ first"
        )
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    heading = re.compile(rf"^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}$", re.MULTILINE)
    if not heading.search(changelog):
        raise CheckError(
            f"CHANGELOG.md has no section '## [{version}] - YYYY-MM-DD' for this release"
        )
    return version


def check_distributions(directory: Path) -> list[str]:
    """Check that `directory` holds exactly the wheel and sdist of the package version.

    Both must name that version in their metadata and contain only the package (plus the
    files setuptools adds to an sdist), never the tests or the examples.
    """
    version = package_version()
    wheel_name = f"{PACKAGE}-{version}-py3-none-any.whl"
    sdist_name = f"{PACKAGE}-{version}.tar.gz"
    found = sorted(path.name for path in directory.iterdir())
    if found != sorted([wheel_name, sdist_name]):
        raise CheckError(
            f"{directory} must hold exactly {sdist_name} and {wheel_name}, but holds {found}"
        )
    _check_wheel(directory / wheel_name, version)
    _check_sdist(directory / sdist_name, version)
    return [sdist_name, wheel_name]


def _check_wheel(path: Path, version: str) -> None:
    dist_info = f"{PACKAGE}-{version}.dist-info/"
    with zipfile.ZipFile(path) as wheel:
        names = wheel.namelist()
        metadata = wheel.read(f"{dist_info}METADATA").decode("utf-8")
    unexpected = [name for name in names if not name.startswith((f"{PACKAGE}/", dist_info))]
    if unexpected:
        raise CheckError(f"{path.name} contains files outside the package: {unexpected}")
    for required in (f"{PACKAGE}/__init__.py", f"{PACKAGE}/py.typed"):
        if required not in names:
            raise CheckError(f"{path.name} lacks {required}")
    _check_metadata(path.name, metadata, version)


def _check_sdist(path: Path, version: str) -> None:
    root = f"{PACKAGE}-{version}"
    with tarfile.open(path) as sdist:
        names = sdist.getnames()
        member = sdist.extractfile(f"{root}/PKG-INFO")
        if member is None:
            raise CheckError(f"{path.name} lacks {root}/PKG-INFO")
        metadata = member.read().decode("utf-8")
    allowed = {PACKAGE, *SDIST_EXTRAS}
    unexpected = [
        name
        for name in names
        if name != root and not (name.startswith(f"{root}/") and name.split("/")[1] in allowed)
    ]
    if unexpected:
        raise CheckError(f"{path.name} contains unexpected files: {unexpected}")
    for required in (f"{root}/{PACKAGE}/__init__.py", f"{root}/{PACKAGE}/py.typed"):
        if required not in names:
            raise CheckError(f"{path.name} lacks {required}")
    _check_metadata(path.name, metadata, version)


def _check_metadata(file_name: str, metadata: str, version: str) -> None:
    headers = HeaderParser().parsestr(metadata)
    if headers["Name"] != PACKAGE or headers["Version"] != version:
        raise CheckError(
            f"{file_name} declares {headers['Name']} {headers['Version']}, not {PACKAGE} {version}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("version", help="print the package version")
    tag_parser = commands.add_parser("tag", help="check a release tag against the version")
    tag_parser.add_argument("tag", help="the tag, e.g. v0.3.0")
    dist_parser = commands.add_parser("dist", help="check the built distributions")
    dist_parser.add_argument("directory", type=Path, help="the directory python -m build wrote")
    arguments = parser.parse_args(argv)

    try:
        if arguments.command == "version":
            print(package_version())
        elif arguments.command == "tag":
            version = check_tag(arguments.tag)
            print(f"Tag {arguments.tag} releases {PACKAGE} {version}, and CHANGELOG.md has it")
        else:
            for name in check_distributions(arguments.directory):
                print(f"{name}: version and contents checked")
    except CheckError as error:
        # An annotation on the workflow run's summary page, when run by GitHub Actions
        prefix = "::error::" if os.environ.get("GITHUB_ACTIONS") == "true" else "error: "
        print(f"{prefix}{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
