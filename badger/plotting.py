from pathlib import Path
import csv

from typing import List, Dict

import numpy as np

from badger.util import find_subclass, ignore


class PlotBackend:

    name: str

    @staticmethod
    def get_backend(name: str):
        cls = find_subclass(PlotBackend, name, attr='name')
        if not cls.available():
            raise ImportError(f"Additional dependencies required for {name} backend")
        return cls

    set_title = ignore
    set_xlabel = ignore
    set_ylabel = ignore
    set_xmode = ignore
    set_ymode = ignore
    set_grid = ignore


class MockBackend(PlotBackend):

    name = 'mock'
    plots = []

    @classmethod
    def available(cls):
        return True

    def __init__(self):
        type(self).plots.append(self)
        self.objects = []
        self.meta = {}

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float], style: Dict[str, str], mode='line'):
        self.objects.append({
            'legend': legend,
            'x': xpoints,
            'y': ypoints,
            'mode': mode,
            **style,
        })

    def add_scatter(self, *args, **kwargs):
        return self.add_line(*args, **kwargs, mode='scatter')

    def set_title(self, title: str):
        self.meta['title'] = title

    def set_xlabel(self, label: str):
        self.meta['xlabel'] = label

    def set_ylabel(self, label: str):
        self.meta['ylabel'] = label

    def set_xmode(self, value: str):
        self.meta['xmode'] = value

    def set_ymode(self, value: str):
        self.meta['ymode'] = value

    def set_grid(self, value: bool):
        self.meta['grid'] = value

    def generate(self, filename: Path):
        self.meta['filename'] = filename.name


class MatplotilbBackend(PlotBackend):

    name = 'matplotlib'

    @classmethod
    def available(cls):
        try:
            import matplotlib
            return True
        except ImportError:
            return False

    def __init__(self):
        from matplotlib.figure import Figure
        self.figure = Figure(tight_layout=True)
        self.axes = self.figure.add_subplot(1, 1, 1)
        self.legend = []

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float], style: Dict[str, str]):
        self.axes.plot(xpoints, ypoints)
        self.legend.append(legend)

    def add_scatter(self, legend: str, xpoints: List[float], ypoints: List[float], style: Dict[str, str]):
        self.axes.scatter(xpoints, ypoints)
        self.legend.append(legend)

    def set_title(self, title: str):
        self.axes.set_title(title)

    def set_xlabel(self, label: str):
        self.axes.set_xlabel(label)

    def set_ylabel(self, label: str):
        self.axes.set_ylabel(label)

    def set_xmode(self, value: str):
        self.axes.set_xscale(value)

    def set_ymode(self, value: str):
        self.axes.set_yscale(value)

    def set_grid(self, value: bool):
        self.axes.grid(value)

    def generate(self, filename: Path):
        self.axes.legend(self.legend)
        self.figure.savefig(filename.with_suffix('.png'))


class PlotlyBackend(PlotBackend):

    name = 'plotly'

    @classmethod
    def available(cls):
        try:
            import plotly
            return True
        except:
            return False

    def __init__(self):
        import plotly.graph_objects as go
        self.figure = go.Figure()

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float], style: Dict[str, str], mode='lines'):
        self.figure.add_scatter(x=xpoints, y=ypoints, mode=mode, name=legend)

    def add_scatter(self, *args, **kwargs):
        self.add_line(*args, **kwargs, mode='markers')

    def set_title(self, title: str):
        self.figure.layout.title.text = title

    def set_xlabel(self, label: str):
        self.figure.layout.xaxis.title.text = label

    def set_ylabel(self, label: str):
        self.figure.layout.yaxis.title.text = label

    def set_xmode(self, value: str):
        self.figure.layout.xaxis.type = value

    def set_ymode(self, value: str):
        self.figure.layout.yaxis.type = value

    def generate(self, filename: Path):
        self.figure.write_html(str(filename.with_suffix('.html')))


class CSVBackend(PlotBackend):

    name = 'csv'

    @classmethod
    def available(cls):
        return True

    def __init__(self):
        self.columns = []
        self.legend = []

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float], style: Dict[str, str]):
        self.columns.extend([xpoints, ypoints])
        self.legend.extend([f'{legend} (x-axis)', legend])

    add_scatter = add_line

    def generate(self, filename: Path):
        maxlen = max(len(c) for c in self.columns)
        cols = [list(c) + [None] * (maxlen - len(c)) for c in self.columns]
        with open(filename.with_suffix('.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.legend)
            for row in zip(*cols):
                writer.writerow(row)
