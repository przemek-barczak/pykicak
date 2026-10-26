"""The Python examples in README.md and docs/ against the library's current API.

Every agent constructed in an example passes the arguments its constructor requires, and only
arguments it accepts, so an example cannot be left behind when a signature changes. The
examples' agent classes take the constructor of `KicakExecutorAbstract` or
`KicakInjectorAbstract`, which they do not override.
"""

from __future__ import annotations

import ast
import inspect
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from pykicak import KicakExecutorAbstract, KicakInjectorAbstract

ROOT = Path(__file__).resolve().parent.parent
DOCUMENTS = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
BASES = {"Executor": KicakExecutorAbstract, "Injector": KicakInjectorAbstract}


def python_blocks(path: Path) -> list[str]:
    return re.findall(r"```python\n(.*?)```", path.read_text(encoding="utf-8"), flags=re.S)


def agent_constructions() -> Iterator[tuple[str, ast.Call]]:
    """Every construction of an agent in the examples, named by document and class."""
    for path in DOCUMENTS:
        for block in python_blocks(path):
            for node in ast.walk(ast.parse(block)):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id.endswith(tuple(BASES))
                ):
                    yield f"{path.relative_to(ROOT)}: {node.func.id}", node


CONSTRUCTIONS = list(agent_constructions())


def test_examples_construct_agents():
    assert len(CONSTRUCTIONS) >= 5


@pytest.mark.parametrize(
    "construction", [call for _, call in CONSTRUCTIONS], ids=[name for name, _ in CONSTRUCTIONS]
)
def test_example_passes_the_arguments_its_constructor_requires_and_accepts(construction):
    if any(isinstance(arg, ast.Constant) and arg.value is Ellipsis for arg in construction.args):
        pytest.skip("its arguments are elided with ...")
    assert isinstance(construction.func, ast.Name)
    base = next(base for suffix, base in BASES.items() if construction.func.id.endswith(suffix))
    parameters = inspect.signature(base).parameters
    required = {
        name
        for name, parameter in parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
        and parameter.default is inspect.Parameter.empty
    }
    passed = {keyword.arg for keyword in construction.keywords}

    assert required <= passed, f"missing {sorted(required - passed)}"
    assert passed <= set(parameters), f"not accepted: {sorted(passed - set(parameters))}"
