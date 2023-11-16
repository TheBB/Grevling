from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from grevling import Case

# from grevling.typing import Field, Stage


DATADIR = Path(__file__).parent / "data"


@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_parse(suffix):
    case = Case(DATADIR / "valid" / f"diverse{suffix}")

    for name, param in case.parameters.items():
        assert param.name == name
    assert case.parameters["alpha"].values == [1, 2]
    assert case.parameters["bravo"].values == [1.0, 2.0]
    assert case.parameters["charlie"].values == [3, 4.5]
    np.testing.assert_allclose(
        case.parameters["delta"].values,
        [0.0, 0.25, 0.5, 0.75, 1.0],
        atol=1e-6,
        rtol=1e-6,
    )
    np.testing.assert_allclose(
        case.parameters["echo"].values,
        [0.0, 0.186289, 0.409836, 0.678092, 1.0],
        atol=1e-6,
        rtol=1e-6,
    )
    assert case.parameters["foxtrot"].values == ["a", "b", "c"]

    assert case.context_mgr.constants == {
        "int": 14,
        "float": 14.0,
    }
