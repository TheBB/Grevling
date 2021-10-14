from __future__ import annotations

from abc import abstractclassmethod, ABC, abstractmethod
from pathlib import Path
import csv
import math
import operator

from typing import List, Dict, Optional, Iterable, Any, Tuple

from bidict import bidict
import numpy as np
import pandas as pd

from .parameters import ParameterSpace
from . import util, typing
from .util import ignore, is_list_type
from .render import render


class Backends:

    _backends: List[PlotBackend]

    def __init__(self, *names: str):
        self._backends = [PlotBackend.get_backend(name)() for name in names]

    def __getattr__(self, attr: str):
        def inner(*args, **kwargs):
            for backend in self._backends:
                getattr(backend, attr)(*args, **kwargs)

        return inner


class PlotBackend(ABC):

    name: str

    @staticmethod
    def get_backend(name: str):
        cls = util.find_subclass(PlotBackend, name, attr='name')
        if not cls:
            raise ImportError(f"Unknown plot backend: {name}")
        if not cls.available():
            raise ImportError(f"Additional dependencies required for {name} backend")
        return cls

    @abstractclassmethod
    def available(cls) -> bool:
        ...

    @abstractmethod
    def generate(self, filename: Path):
        ...

    set_title = ignore
    set_xlabel = ignore
    set_ylabel = ignore
    set_xmode = ignore
    set_ymode = ignore
    set_grid = ignore
    set_xlim = ignore
    set_ylim = ignore


class MockBackend(PlotBackend):

    name = 'mock'
    plots = []

    @classmethod
    def available(cls) -> bool:
        return True

    def __init__(self):
        type(self).plots.append(self)
        self.objects = []
        self.meta = {}

    def add_line(
        self,
        legend: str,
        xpoints: List[float],
        ypoints: List[float],
        style: Dict[str, str],
        mode='line',
    ):
        self.objects.append(
            {
                'legend': legend,
                'x': xpoints,
                'y': ypoints,
                'mode': mode,
                **style,
            }
        )

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

    def set_xlim(self, value: List[float]):
        self.meta['xlim'] = value

    def set_ylim(self, value: List[float]):
        self.meta['ylim'] = value

    def generate(self, filename: Path):
        self.meta['filename'] = filename.name


class MatplotilbBackend(PlotBackend):

    name = 'matplotlib'

    @classmethod
    def available(cls) -> bool:
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

    def add_line(
        self,
        legend: str,
        xpoints: List[float],
        ypoints: List[float],
        style: Dict[str, str],
    ):
        self.axes.plot(
            xpoints,
            ypoints,
            color=style['color'],
            linestyle={'dash': 'dashed', 'dot': 'dotted'}.get(
                style['line'], style['line']
            ),
            marker={'circle': 'o', 'triangle': '^', 'square': 's'}.get(style['marker']),
        )
        self.legend.append(legend)

    def add_scatter(
        self,
        legend: str,
        xpoints: List[float],
        ypoints: List[float],
        style: Dict[str, str],
    ):
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

    def set_xlim(self, value: List[float]):
        self.axes.set_xlim(value[0], value[1])

    def set_ylim(self, value: List[float]):
        self.axes.set_ylim(value[0], value[1])

    def generate(self, filename: Path):
        self.axes.legend(self.legend)
        filename = filename.with_suffix('.png')
        util.log.info(f'Written: {filename}')
        self.figure.savefig(filename)


class PlotlyBackend(PlotBackend):

    name = 'plotly'

    @classmethod
    def available(cls) -> bool:
        try:
            import plotly

            return True
        except:
            return False

    def __init__(self):
        import plotly.graph_objects as go

        self.figure = go.Figure()

    def add_line(
        self,
        legend: str,
        xpoints: List[float],
        ypoints: List[float],
        style: Dict[str, str],
        mode='lines',
    ):
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

    def set_xlim(self, value: List[float]):
        if self.figure.layout.xaxis.type == 'log':
            self.figure.layout.xaxis.range = [
                math.log10(value[0]),
                math.log10(value[1]),
            ]
        else:
            self.figure.layout.xaxis.range = value

    def set_ylim(self, value: List[float]):
        if self.figure.layout.yaxis.type == 'log':
            self.figure.layout.yaxis.range = [
                math.log10(value[0]),
                math.log10(value[1]),
            ]
        else:
            self.figure.layout.yaxis.range = value

    def generate(self, filename: Path):
        filename = filename.with_suffix('.html')
        util.log.info(f'Written: {filename}')
        self.figure.write_html(str(filename))


class CSVBackend(PlotBackend):

    name = 'csv'

    columns: List[Tuple[List[float], List[float]]]
    legend: List[str]

    @classmethod
    def available(cls) -> bool:
        return True

    def __init__(self):
        self.columns = []
        self.legend = []

    def add_line(
        self,
        legend: str,
        xpoints: List[float],
        ypoints: List[float],
        style: Dict[str, str],
    ):
        self.columns.extend((xpoints, ypoints))
        self.legend.extend([f'{legend} (x-axis)', legend])

    add_scatter = add_line

    def generate(self, filename: Path):
        filename = filename.with_suffix('.csv')
        util.log.info(f'Written: {filename}')
        maxlen = max(len(c) for c in self.columns)
        cols = [list(c) + [None] * (maxlen - len(c)) for c in self.columns]
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.legend)
            for row in zip(*cols):
                writer.writerow(row)


class PlotStyleManager:

    _category_to_style: bidict
    _custom_styles: Dict[str, List[str]]
    _mode: str
    _defaults = {
        'color': {
            'category': {
                None: ['blue', 'red', 'green', 'magenta', 'cyan', 'black'],
            },
            'single': {
                None: ['blue'],
            },
        },
        'line': {
            'category': {
                'line': ['solid', 'dash', 'dot', 'dashdot'],
                'scatter': ['none'],
            },
            'single': {
                'line': ['solid'],
                'scatter': ['none'],
            },
        },
        'marker': {
            'category': {
                None: ['circle', 'triangle', 'square'],
            },
            'single': {
                'line': ['none'],
                'scatter': ['circle'],
            },
        },
    }

    def __init__(self, mode: str):
        self._category_to_style = bidict()
        self._custom_styles = dict()
        self._mode = mode

    def assigned(self, category: str):
        return category in self._category_to_style

    def assign(self, category: str, style: Optional[str] = None):
        if style is None:
            candidates = list(
                s for s in self._defaults if s not in self._category_to_style.inverse
            )
            if self._mode == 'scatter':
                try:
                    candidates.remove('line')
                except ValueError:
                    pass
            assert candidates
            style = candidates[0]
        assert style != 'line' or self._mode != 'scatter'
        self._category_to_style[category] = style

    def set_values(self, style: str, values: List[str]):
        self._custom_styles[style] = values

    def get_values(self, style: str) -> List[str]:
        # Prioritize user customizations
        if style in self._custom_styles:
            return self._custom_styles[style]
        getter = lambda d, k: d.get(k, d.get(None, []))
        s = getter(self._defaults, style)
        s = getter(
            s, 'category' if style in self._category_to_style.inverse else 'single'
        )
        s = getter(s, self._mode)
        return s

    def styles(
        self, space: ParameterSpace, *categories: str
    ) -> Iterable[Dict[str, str]]:
        names, values = [], []
        for c in categories:
            style = self._category_to_style[c]
            available_values = self.get_values(style)
            assert len(available_values) >= len(space[c])
            names.append(style)
            values.append(available_values[: len(space[c])])
        yield from util.dict_product(names, values)

    def supplement(self, basestyle: Dict[str, str]):
        basestyle = dict(basestyle)
        for style in self._defaults:
            if style not in basestyle and self._category_to_style.get('yaxis') != style:
                basestyle[style] = self.get_values(style)[0]
        if 'yaxis' in self._category_to_style:
            ystyle = self._category_to_style['yaxis']
            for v in self.get_values(ystyle):
                yield {**basestyle, ystyle: v}
        else:
            yield basestyle


class PlotMode:
    @classmethod
    def load(cls, spec):
        if isinstance(spec, str):
            return cls(spec, None)
        if spec['mode'] == 'category':
            return cls('category', spec.get('style'))
        if spec['mode'] == 'ignore':
            return cls('ignore', spec.get('value'))

    def __init__(self, kind: str, arg: Any):
        self.kind = kind
        self.arg = arg


class Plot:

    _parameters: Dict[str, PlotMode]
    _filename: str
    _format: List[str]
    _yaxis: List[str]
    _xaxis: str
    _type: str
    _legend: Optional[str]
    _xlabel: Optional[str]
    _ylabel: Optional[str]
    _xmode: str
    _ymode: str
    _title: Optional[str]
    _grid: bool
    _styles: PlotStyleManager
    _xlim: List[float]
    _ylim: List[float]

    @classmethod
    def load(cls, spec, parameters, types):
        # All parameters not mentioned are assumed to be ignored
        spec.setdefault('parameters', {})
        for param in parameters:
            spec['parameters'].setdefault(param, 'ignore')

        # If there is exactly one variate, and the x-axis is not given, assume that is the x-axis
        variates = [
            param for param, kind in spec['parameters'].items() if kind == 'variate'
        ]
        nvariate = len(variates)
        if nvariate == 1 and 'xaxis' not in spec:
            spec['xaxis'] = next(iter(variates))
        elif 'xaxis' not in spec:
            spec['xaxis'] = None

        # Listify possible scalars
        for k in ('format', 'yaxis'):
            if isinstance(spec[k], str):
                spec[k] = [spec[k]]

        # Either all the axes are list type or none of them are
        list_type = is_list_type(types[spec['yaxis'][0]].tp)
        assert all(is_list_type(types[k].tp) == list_type for k in spec['yaxis'][1:])
        if spec['xaxis']:
            assert is_list_type(types[spec['xaxis']].tp) == list_type

        # If the x-axis has list type, the effective number of variates is one higher
        eff_variates = nvariate + list_type

        # If there are more than one effective variate, the plot must be scatter
        if eff_variates > 1:
            if spec.get('type', 'scatter') != 'scatter':
                util.log.warning("Line plots can have at most one variate dimension")
            spec['type'] = 'scatter'
        elif eff_variates == 0:
            util.log.error("Plot has no effective variate dimensions")
            return
        else:
            spec.setdefault('type', 'line')

        return util.call_yaml(cls, spec)

    def __init__(
        self,
        parameters,
        filename,
        format,
        yaxis,
        xaxis,
        type,
        legend=None,
        xlabel=None,
        ylabel=None,
        title=None,
        grid=True,
        xmode='linear',
        ymode='linear',
        xlim=[],
        ylim=[],
        style={},
    ):
        self._parameters = {
            name: PlotMode.load(value) for name, value in parameters.items()
        }
        self._filename = filename
        self._format = format
        self._yaxis = yaxis
        self._xaxis = xaxis
        self._type = type
        self._legend = legend
        self._xlabel = xlabel
        self._ylabel = ylabel
        self._xmode = xmode
        self._ymode = ymode
        self._title = title
        self._grid = grid
        self._xlim = xlim
        self._ylim = ylim

        self._styles = PlotStyleManager(type)
        for key, value in style.items():
            if isinstance(value, list):
                self._styles.set_values(key, value)
            else:
                self._styles.set_values(key, [value])
        for param in self._parameters_of_kind('category', req_arg=True):
            self._styles.assign(param, self._parameters[param].arg)
        for param in self._parameters_of_kind('category', req_arg=False):
            self._styles.assign(param)
        if len(self._yaxis) > 1 and not self._styles.assigned('yaxis'):
            self._styles.assign('yaxis')

    def _parameters_of_kind(self, *kinds: str, req_arg: Optional[bool] = None):
        return [
            param
            for param, mode in self._parameters.items()
            if mode.kind in kinds
            and (
                req_arg is None
                or req_arg is True
                and mode.arg is not None
                or req_arg is False
                and mode.arg is None
            )
        ]

    def _parameters_not_of_kind(self, *kinds: str):
        return [
            param for param, mode in self._parameters.items() if mode.kind not in kinds
        ]

    def generate_all(self, case):
        # Collect all the fixed parameters and iterate over all those combinations
        fixed = self._parameters_of_kind('fixed')

        constants = {
            param: self._parameters[param].arg
            for param in self._parameters_of_kind('ignore', req_arg=True)
        }

        for index in case.parameters.subspace(*fixed):
            index = {**index, **constants}
            context = case.context_mgr.raw_evaluate(index.copy(), allowed_missing=True)
            self.generate_single(case, context, index)

    def generate_single(self, case, context: dict, index):
        # Collect all the categorized parameters and iterate over all those combinations
        categories = self._parameters_of_kind('category')
        backends = Backends(*self._format)
        plotter = operator.attrgetter(f'add_{self._type}')

        sub_indices = case.parameters.subspace(*categories)
        styles = self._styles.styles(case.parameters, *categories)
        for sub_index, basestyle in zip(sub_indices, styles):
            sub_context = case.context_mgr.evaluate_context(
                {**context, **sub_index}, allowed_missing=True
            )
            sub_index = {**index, **sub_index}

            cat_name, xaxis, yaxes = self.generate_category(
                case, sub_context, sub_index
            )

            final_styles = self._styles.supplement(basestyle)
            for ax_name, data, style in zip(self._yaxis, yaxes, final_styles):
                legend = self.generate_legend(sub_context, ax_name)
                plotter(backends)(legend, xpoints=xaxis, ypoints=data, style=style)

        for attr in ['title', 'xlabel', 'ylabel']:
            template = getattr(self, f'_{attr}')
            if template is None:
                continue
            text = render(template, context)
            getattr(backends, f'set_{attr}')(text)
        backends.set_xmode(self._xmode)
        backends.set_ymode(self._ymode)
        backends.set_grid(self._grid)
        if len(self._xlim) >= 2:
            backends.set_xlim(self._xlim)
        if len(self._xlim) >= 2:
            backends.set_ylim(self._ylim)

        filename = case.storagepath / render(self._filename, context)
        backends.generate(filename)

    def generate_category(self, case, context: dict, index):
        # TODO: Pick only finished results
        data = case.load_dataframe()
        if isinstance(data, pd.Series):
            data = data.to_frame().T
        for name, value in index.items():
            data = data[data[name] == value]

        # Collapse ignorable parameters
        for ignore in self._parameters_of_kind('ignore', req_arg=False):
            others = [p for p in case.parameters if p != ignore]
            data = data.groupby(by=others).first().reset_index()

        # Collapse mean parameters
        for mean in self._parameters_of_kind('mean'):
            others = [p for p in case.parameters if p != mean]
            data = data.groupby(by=others).aggregate(util.flexible_mean).reset_index()

        # Extract data
        ydata = [util.flatten(data[f].to_numpy()) for f in self._yaxis]
        if self._xaxis:
            xdata = util.flatten(data[self._xaxis].to_numpy())
        else:
            length = max(len(f) for f in ydata)
            xdata = np.arange(1, length + 1)

        if any(self._parameters_of_kind('category')):
            name = ', '.join(
                f'{k}={repr(context[k])}' for k in self._parameters_of_kind('category')
            )
        else:
            name = None

        return name, xdata, ydata

    def generate_legend(self, context: dict, yaxis: str) -> str:
        if self._legend is not None:
            return render(self._legend, {**context, 'yaxis': yaxis})
        if any(self._parameters_of_kind('category')):
            name = ', '.join(
                f'{k}={repr(context[k])}' for k in self._parameters_of_kind('category')
            )
            return f'{name} ({yaxis})'
        return yaxis
