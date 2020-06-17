from pathlib import Path

import numpy as np

from badger import Case


DATADIR = Path(__file__).parent / 'data'

def test_echo():
    case = Case(DATADIR / 'run' / 'echo.yaml')
    case.clear_cache()
    case.run()

    data = case.result_array()
    assert data['a'].dtype == int
    np.testing.assert_array_equal(data['a'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['b'], [['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', 'c']])
    np.testing.assert_array_equal(data['c'], [[1, 1, 1], [3, 3, 3], [5, 5, 5]])
