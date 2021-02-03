from pathlib import Path

from typing import List

import matplotlib as mpl
import matplotlib.figure

from badger.util import find_subclass


class PlotBackend:

    name: str

    @staticmethod
    def get_backend(name: str):
        return find_subclass(PlotBackend, name, attr='name')


class MatplotilbBackend(PlotBackend):

    name = 'matplotlib'

    def __init__(self):
        self.figure = mpl.figure.Figure(tight_layout=True)
        self.axes = self.figure.add_subplot(1, 1, 1)
        self.legend = []

    def add_line(self, legend: str, xpoints: List[float], ypoints: List[float]):
        self.axes.plot(xpoints, ypoints)
        self.legend.append(legend)

    def generate(self, filename: Path):
        self.axes.legend(self.legend)
        self.figure.savefig(filename.with_suffix('.png'))
