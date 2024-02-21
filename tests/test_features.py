from __future__ import annotations

from pathlib import Path

import pytest

from grevling import Case

DATADIR = Path(__file__).parent / "data"


@pytest.mark.parametrize("suffix", [".yaml", ".gold"])
def test_conditions(suffix):
    with Case(DATADIR / "valid" / f"conditions{suffix}") as case:
        case.clear_cache()
        contexts = [instance.context for instance in case.create_instances()]
    vals = [(ctx["g_index"], ctx["a"], ctx["b"]) for ctx in contexts]
    assert vals == [
        (0, 1, 2),
        (1, 1, 3),
        (2, 1, 4),
        (3, 2, 3),
        (4, 2, 4),
        (5, 3, 4),
    ]
