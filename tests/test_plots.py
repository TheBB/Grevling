import csv
from pathlib import Path

import numpy as np

from badger import Case


DATADIR = Path(__file__).parent / 'data'


def read_csv(path: Path):
    with open(path, 'r', newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)
        data = np.array([list(map(float, row)) for row in reader])
    return headers, data


def test_plots():
    case = Case(DATADIR / 'run' / 'plot')
    case.clear_cache()
    case.run()

    root = DATADIR / 'run' / 'plot' / '.badgerdata'

    headers, data = read_csv(root / 'i-vs-isq.csv')
    assert headers == ['isq (x-axis)', 'isq']
    np.testing.assert_array_equal(data, [[1, 1], [2, 4], [3, 9], [4, 16], [5, 25]])
