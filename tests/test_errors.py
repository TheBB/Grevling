from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError
from pytest import mark, raises

from grevling import Case

DATADIR = Path(__file__).parent / "data"
ERRORFILES = [
    "empty",
    "list",
    "param1",
    "param2",
    "param3",
    "param4",
    "param5",
    "eval1",
    "eval2",
    "eval3",
    "templates1",
    "templates2",
    "templates3",
]


@mark.parametrize("filename", [DATADIR / "erroring" / f"{fn}.yaml" for fn in ERRORFILES])
def test_raises(filename):
    with raises(ValidationError):
        Case(filename)
