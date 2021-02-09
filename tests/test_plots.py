import csv
from pathlib import Path

import numpy as np

from badger import Case


DATADIR = Path(__file__).parent / 'data'


def float_or_none(text):
    if text == '':
        return np.nan
    return float(text)


def read_csv(path: Path):
    with open(path, 'r', newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)
        data = np.array([list(map(float_or_none, row)) for row in reader])
    return headers, data


def test_plots():
    case = Case(DATADIR / 'run' / 'plot')
    case.clear_cache()
    case.run()

    root = DATADIR / 'run' / 'plot' / '.badgerdata'

    headers, data = read_csv(root / 'i-vs-isq.csv')
    assert headers == ['isq (x-axis)', 'isq']
    np.testing.assert_array_equal(data, [[1, 1], [2, 4], [3, 9], [4, 16], [5, 25]])

    headers, data = read_csv(root / 'misc-vs-i-a.csv')
    assert headers == ['misc (x-axis)', 'misc']
    np.testing.assert_array_equal(data, [
        [1, 98 + 31/5],
        [2, 99 + 31/5],
        [3, 100 + 31/5],
        [4, 101 + 31/5],
        [5, 102 + 31/5],
    ])

    headers, data = read_csv(root / 'misc-vs-i-b.csv')
    assert headers == ['misc (x-axis)', 'misc']
    np.testing.assert_array_equal(data, [
        [1, 99 + 31/5],
        [2, 100 + 31/5],
        [3, 101 + 31/5],
        [4, 102 + 31/5],
        [5, 103 + 31/5],
    ])

    headers, data = read_csv(root / 'misc-vs-i-c.csv')
    assert headers == ['misc (x-axis)', 'misc']
    np.testing.assert_array_equal(data, [
        [1, 100 + 31/5],
        [2, 101 + 31/5],
        [3, 102 + 31/5],
        [4, 103 + 31/5],
        [5, 104 + 31/5],
    ])

    headers, data = read_csv(root / 'fresult.csv')
    assert headers == ['fresult (x-axis)', 'fresult']
    np.testing.assert_array_equal(data.T, [
        np.arange(1, 11),
        np.arange(1, 11),
    ])

    headers, data = read_csv(root / 'vresult.csv')
    assert headers == [
        'i=1 (vresult) (x-axis)',
        'i=1 (vresult)',
        'i=2 (vresult) (x-axis)',
        'i=2 (vresult)',
        'i=3 (vresult) (x-axis)',
        'i=3 (vresult)',
        'i=4 (vresult) (x-axis)',
        'i=4 (vresult)',
        'i=5 (vresult) (x-axis)',
        'i=5 (vresult)',
    ]
    np.testing.assert_array_equal(data, [
        [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        [np.nan, np.nan, 2, 2, 2, 2, 2, 2, 2, 2],
        [np.nan, np.nan, np.nan, np.nan, 3, 3, 3, 3, 3, 3],
        [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 4, 4, 4, 4],
        [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 5, 5],
    ])

    headers, data = read_csv(root / 'fresult-mean.csv')
    assert headers == ['fresult (x-axis)', 'fresult']
    np.testing.assert_array_equal(data.T, [
        np.arange(1, 11),
        np.arange(1, 11),
    ])
