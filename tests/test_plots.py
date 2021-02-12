import csv
from operator import itemgetter
from pathlib import Path

import numpy as np

from badger import Case
from badger.plotting import MockBackend


DATADIR = Path(__file__).parent / 'data'


def compare_object(actual, expected, sort_xy=False):
    for k in ['mode', 'legend', 'color', 'line', 'marker']:
        assert actual[k] == expected[k]

    x, y = actual['x'], actual['y']
    if sort_xy:
        xy = list(zip(x, y))
        xy = sorted(xy, key=itemgetter(1))
        xy = sorted(xy, key=itemgetter(0))
        x = np.array(list(map(itemgetter(0), xy)))
        y = np.array(list(map(itemgetter(1), xy)))

    np.testing.assert_array_equal(x, expected['x'])
    np.testing.assert_array_equal(y, expected['y'])


def test_plots():
    MockBackend.plots = []
    case = Case(DATADIR / 'run' / 'plot')
    case.clear_cache()
    case.run()

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'i-vs-isq',
    }
    compare_object(plot.objects[0], {
        'mode': 'line',
        'legend': 'isq',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2, 3, 4, 5]),
        'y': np.array([1, 4, 9, 16, 25]),
    })

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'misc-vs-i-a',
        'title': 'This is a plot for k=a',
        'xlabel': 'x (a)',
        'ylabel': 'y (a)',
    }
    compare_object(plot.objects[0], {
        'mode': 'line',
        'legend': 'misc',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2, 3, 4, 5]),
        'y': np.array([1, 2, 3, 4, 5]) + 97 + 31/5,
    })

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'misc-vs-i-b',
        'title': 'This is a plot for k=b',
        'xlabel': 'x (b)',
        'ylabel': 'y (b)',
    }
    compare_object(plot.objects[0], {
        'mode': 'line',
        'legend': 'misc',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2, 3, 4, 5]),
        'y': np.array([1, 2, 3, 4, 5]) + 98 + 31/5,
    })

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'misc-vs-i-c',
        'title': 'This is a plot for k=c',
        'xlabel': 'x (c)',
        'ylabel': 'y (c)',
    }
    compare_object(plot.objects[0], {
        'mode': 'line',
        'legend': 'misc',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2, 3, 4, 5]),
        'y': np.array([1, 2, 3, 4, 5]) + 99 + 31/5,
    })

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'fresult',
    }
    compare_object(plot.objects[0], {
        'mode': 'line',
        'legend': 'fresult',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.arange(1, 11),
        'y': np.arange(1, 11),
    })

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'fresult-mean',
    }
    compare_object(plot.objects[0], {
        'mode': 'line',
        'legend': 'fresult',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.arange(1, 11),
        'y': np.arange(1, 11),
    })

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'vresult',
    }
    compare_object(plot.objects[0], {
        'mode': 'line',
        'legend': 'i is 1 - vresult',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1]),
        'y': np.array([1]),
    })
    compare_object(plot.objects[1], {
        'mode': 'line',
        'legend': 'i is 2 - vresult',
        'color': 'red',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2]),
        'y': np.array([1, 2]),
    })
    compare_object(plot.objects[2], {
        'mode': 'line',
        'legend': 'i is 3 - vresult',
        'color': 'green',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2, 3]),
        'y': np.array([1, 2, 3]),
    })
    compare_object(plot.objects[3], {
        'mode': 'line',
        'legend': 'i is 4 - vresult',
        'color': 'magenta',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2, 3, 4]),
        'y': np.array([1, 2, 3, 4]),
    })
    compare_object(plot.objects[4], {
        'mode': 'line',
        'legend': 'i is 5 - vresult',
        'color': 'cyan',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([1, 2, 3, 4, 5]),
        'y': np.array([1, 2, 3, 4, 5]),
    })

    plot = MockBackend.plots.pop(0)
    assert plot.meta == {
        'filename': 'scatter',
    }
    compare_object(plot.objects[0], {
        'mode': 'scatter',
        'legend': 'misc',
        'color': 'blue',
        'line': 'solid',
        'marker': 'none',
        'x': np.array([
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
            8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
            16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16
        ]),
        'y': np.array([
            99, 100, 100, 101, 101, 101, 102, 102, 102, 103, 103, 103, 104, 104, 105,
            100, 101, 101, 102, 102, 102, 103, 103, 103, 104, 104, 104, 105, 105, 106,
            102, 103, 103, 104, 104, 104, 105, 105, 105, 106, 106, 106, 107, 107, 108,
            106, 107, 107, 108, 108, 108, 109, 109, 109, 110, 110, 110, 111, 111, 112,
            114, 115, 115, 116, 116, 116, 117, 117, 117, 118, 118, 118, 119, 119, 120,
        ]),
    }, sort_xy=True)
