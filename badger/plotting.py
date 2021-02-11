from pathlib import Path
import csv

from typing import List

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

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float]):
        self.axes.plot(xpoints, ypoints)
        self.legend.append(legend)

    def add_scatter(self, legend: str, xpoints: List[float], ypoints: List[float]):
        self.axes.scatter(xpoints, ypoints)
        self.legend.append(legend)

    def set_title(self, title: str):
        self.axes.set_title(title)

    def set_xlabel(self, label: str):
        self.axes.set_xlabel(label)

    def set_ylabel(self, label: str):
        self.axes.set_ylabel(label)

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

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float], mode='lines'):
        self.figure.add_scatter(x=xpoints, y=ypoints, mode=mode, name=legend)

    def add_scatter(self, *args, **kwargs):
        self.add_line(*args, **kwargs, mode='markers')

    def set_title(self, title: str):
        self.figure.layout.title.text = title

    def set_xlabel(self, label: str):
        self.figure.layout.xaxis.title.text = label

    def set_ylabel(self, label: str):
        self.figure.layout.yaxis.title.text = label

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

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float]):
        self.columns.extend([xpoints, ypoints])
        self.legend.extend([f'{legend} (x-axis)', legend])

    add_scatter = add_line
    set_title = ignore
    set_xlabel = ignore
    set_ylabel = ignore

    def generate(self, filename: Path):
        maxlen = max(len(c) for c in self.columns)
        cols = [list(c) + [None] * (maxlen - len(c)) for c in self.columns]
        with open(filename.with_suffix('.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.legend)
            for row in zip(*cols):
                writer.writerow(row)
