from pathlib import Path

import numpy as np

from badger import Case


DATADIR = Path(__file__).parent / 'data'


def read_file(path: Path) -> str:
    print(path)
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
            path = DATADIR / 'run' / 'files' / '.badgerdata' / f'{a}-{b}'
            assert read_file(path / 'template.txt') == f'a={a} b={b} c={2*a-1}\n'
            assert read_file(path / 'other-template.txt') == f'a={a} b={b} c={2*a-1}\n'
            assert read_file(path / 'non-template.txt') == 'a=${alpha} b=${bravo} c=${charlie}\n'
            assert read_file(path / 'some' / 'deep' / 'directory' / 'empty1.dat') == ''
            assert read_file(path / 'some' / 'deep' / 'directory' / 'empty2.dat') == ''
            assert read_file(path / 'some' / 'deep' / 'directory' / 'empty3.dat') == ''


def test_capture():
    case = Case(DATADIR / 'run' / 'capture')
    case.clear_cache()
    case.run()

    data = case.result_array()
    assert data['alpha'].dtype == float
    assert data['firstalpha'].dtype == float
    assert data['lastalpha'].dtype == float
    assert data['allalpha'].dtype == object
    assert data['bravo'].dtype == int
    assert data['firstbravo'].dtype == int
    assert data['lastbravo'].dtype == int
    assert data['allbravo'].dtype == object
    np.testing.assert_allclose(data['alpha'], [[1.234, 1.234, 1.234], [2.345, 2.345, 2.345], [3.456, 3.456, 3.456]])
    np.testing.assert_allclose(data['firstalpha'], [[1.234, 1.234, 1.234], [2.345, 2.345, 2.345], [3.456, 3.456, 3.456]])
    np.testing.assert_allclose(data['lastalpha'], [[4.936, 4.936, 4.936], [9.38, 9.38, 9.38], [13.824, 13.824, 13.824]])
    np.testing.assert_allclose(data['allalpha'].tolist(), [
        [[1.234, 2.468, 3.702, 4.936], [1.234, 2.468, 3.702, 4.936], [1.234, 2.468, 3.702, 4.936]],
        [[2.345, 4.690, 7.035, 9.380], [2.345, 4.690, 7.035, 9.380], [2.345, 4.690, 7.035, 9.380]],
        [[3.456, 6.912, 10.368, 13.824], [3.456, 6.912, 10.368, 13.824], [3.456, 6.912, 10.368, 13.824]]
    ])
    np.testing.assert_array_equal(data['bravo'], [[1, 2, 3], [1, 2, 3], [1, 2, 3]])
    np.testing.assert_array_equal(data['firstbravo'], [[1, 2, 3], [1, 2, 3], [1, 2, 3]])
    np.testing.assert_array_equal(data['lastbravo'], [[4, 8, 12], [4, 8, 12], [4, 8, 12]])
    np.testing.assert_array_equal(data['allbravo'].tolist(), [
        [[1, 2, 3, 4], [2, 4, 6, 8], [3, 6, 9, 12]],
        [[1, 2, 3, 4], [2, 4, 6, 8], [3, 6, 9, 12]],
        [[1, 2, 3, 4], [2, 4, 6, 8], [3, 6, 9, 12]],
    ])


def test_failing():
    case = Case(DATADIR / 'run' / 'failing')
    case.clear_cache()
    case.run()

    data = case.result_array()
    assert data['retcode'].dtype == int
    assert data['before'].dtype == int
    assert data['return'].dtype == int
    assert data['next'].dtype == int
    assert data['after'].dtype == int
    assert data['retcode'][0] == 0
    assert data['before'][0] == 12
    assert data['return'][0] == 0
    assert data['next'][0] == 0
    assert data['after'][0] == 13
    assert data['retcode'][1] == 1
    assert data['before'][1] == 12
    assert data['return'][1] == 1
    assert data.mask['next'][1] == True
    assert data.mask['after'][1] == True


def test_stdout():
    case = Case(DATADIR / 'run' / 'stdout')
    case.clear_cache()
    case.run()

    path = DATADIR / 'run' / 'stdout' / '.badgerdata'
    assert read_file(path / 'out-0' / 'good.stdout') == 'stdout 0\n'
    assert read_file(path / 'out-0' / 'good.stderr') == 'stderr 0\n'
    assert read_file(path / 'out-0' / 'bad.stdout') == 'stdout 0\n'
    assert read_file(path / 'out-0' / 'bad.stderr') == 'stderr 0\n'
    assert read_file(path / 'out-1' / 'good.stdout') == 'stdout 1\n'
    assert read_file(path / 'out-1' / 'good.stderr') == 'stderr 1\n'
    assert read_file(path / 'out-1' / 'bad.stdout') == 'stdout 1\n'
    assert read_file(path / 'out-1' / 'bad.stderr') == 'stderr 1\n'
