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
    assert data['alpha'].dtype == int
    assert data['b'].dtype == object
    assert data['bravo'].dtype == object
    assert data['c'].dtype == float
    assert data['charlie'].dtype == int
    assert data['echo'].dtype == float
    np.testing.assert_array_equal(data['a'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['alpha'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['b'], [['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', 'c']])
    np.testing.assert_array_equal(data['bravo'], [['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', 'c']])
    np.testing.assert_array_equal(data['c'], [[1, 1, 1], [3, 3, 3], [5, 5, 5]])
    np.testing.assert_array_equal(data['charlie'], [[1, 1, 1], [3, 3, 3], [5, 5, 5]])
