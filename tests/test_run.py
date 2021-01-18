from pathlib import Path

import numpy as np

from badger import Case


DATADIR = Path(__file__).parent / 'data'


def read_file(path: Path) -> str:
    with open(path, 'r') as f:
        return f.read()


def test_echo():
    case = Case(DATADIR / 'run' / 'echo')
    case.clear_cache()
    case.run()

    data = case.result_array()
    assert data['a'].dtype == int
    assert data['alpha'].dtype == int
    assert data['b'].dtype == object
    assert data['bravo'].dtype == object
    assert data['c'].dtype == float
    assert data['charlie'].dtype == int
    assert data['walltime']['echo'].dtype == float
    np.testing.assert_array_equal(data['a'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['alpha'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['b'], [['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', 'c']])
    np.testing.assert_array_equal(data['bravo'], [['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', 'c']])
    np.testing.assert_array_equal(data['c'], [[1, 1, 1], [3, 3, 3], [5, 5, 5]])
    np.testing.assert_array_equal(data['charlie'], [[1, 1, 1], [3, 3, 3], [5, 5, 5]])


def test_cat():
    case = Case(DATADIR / 'run' / 'cat')
    case.clear_cache()
    case.run()

    data = case.result_array()
    assert data['a'].dtype == int
    assert data['alpha'].dtype == int
    assert data['a_auto'].dtype == int
    assert data['b'].dtype == object
    assert data['bravo'].dtype == object
    assert data['c'].dtype == float
    assert data['charlie'].dtype == int
    assert data['walltime']['cat'].dtype == float
    np.testing.assert_array_equal(data['a'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['alpha'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['a_auto'], [[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    np.testing.assert_array_equal(data['b'], [['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', 'c']])
    np.testing.assert_array_equal(data['bravo'], [['a', 'b', 'c'], ['a', 'b', 'c'], ['a', 'b', 'c']])
    np.testing.assert_array_equal(data['c'], [[1, 1, 1], [3, 3, 3], [5, 5, 5]])
    np.testing.assert_array_equal(data['charlie'], [[1, 1, 1], [3, 3, 3], [5, 5, 5]])


def test_files():
    case = Case(DATADIR / 'run' / 'files')
    case.clear_cache()
    case.run()

    for a in range(1,4):
        for b in 'abc':
            path = DATADIR / 'run' / '.badgerdata' / f'{a}-{b}'
            assert read_file(path / 'template.txt') == f'a={a} b={b} c={2*a-1}\n'
            assert read_file(path / 'other-template.txt') == f'a={a} b={b} c={2*a-1}\n'
            assert read_file(path / 'non-template.txt') == 'a=${alpha} b=${bravo} c=${charlie}\n'
            assert read_file(path / 'empty1.dat') == ''
            assert read_file(path / 'empty2.dat') == ''
            assert read_file(path / 'empty3.dat') == ''
