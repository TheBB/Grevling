from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
import csv
import math
import operator
import importlib.util

from typing import List, Dict, Optional, Iterable, Any, Tuple, Literal, TYPE_CHECKING

from bidict._bidict import bidict
import numpy as np
import pandas as pd  # type: ignore

from . import util, api
from .parameters import ParameterSpace
from .render import render
from .schema import (
    PlotSchema,
    PlotModeSchema,
    PlotModeIgnoreSchema,
)

if TYPE_CHECKING:
    from . import Case


class Backends:
    backends: List[PlotBackend]

    def __init__(self, *names: str):
        self.backends = [PlotBackend.get_backend(name)() for name in names]

    def __getattr__(self, attr: str):
        def inner(*args, **kwargs):
            for backend in self.backends:
                getattr(backend, attr)(*args, **kwargs)

        return inner


class PlotBackend(ABC):
    name: str

    @staticmethod
    def get_backend(name: str):
        cls = util.find_subclass(PlotBackend, name, attr="name")
        if not cls:
            raise ImportError(f"Unknown plot backend: {name}")
        if not cls.available():
            raise ImportError(f"Additional dependencies required for {name} backend")
        return cls

    @classmethod
    @abstractmethod
    def available(cls) -> bool:
        ...

    @abstractmethod
    def generate(self, filename: Path):
        ...

    def set_title(self, title: str):
        ...

    def set_xlabel(self, title: str):
        ...

    def set_ylabel(self, title: str):
        ...

    def set_xmode(self, value: str):
        ...

    def set_ymode(self, value: str):
        ...

    def set_grid(self, value: bool):
        ...

    def set_xlim(self, value: List[float]):
        ...

    def set_ylim(self, value: List[float]):
        ...


class MockBackend(PlotBackend):
    name = "mock"
    plots: List[MockBackend] = []

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
        mode="line",
    ):
        self.objects.append(
            {
                "legend": legend,
                "x": xpoints,
                "y": ypoints,
                "mode": mode,
                **style,
            }
        )

    def add_scatter(self, *args, **kwargs):
        return self.add_line(*args, **kwargs, mode="scatter")

    def set_title(self, title: str):
        self.meta["title"] = title

    def set_xlabel(self, label: str):
        self.meta["xlabel"] = label

    def set_ylabel(self, label: str):
        self.meta["ylabel"] = label

    def set_xmode(self, value: str):
        self.meta["xmode"] = value

    def set_ymode(self, value: str):
        self.meta["ymode"] = value

    def set_grid(self, value: bool):
        self.meta["grid"] = value

    def set_xlim(self, value: List[float]):
        self.meta["xlim"] = value

    def set_ylim(self, value: List[float]):
        self.meta["ylim"] = value

    def generate(self, filename: Path):
        self.meta["filename"] = filename.name


class MatplotilbBackend(PlotBackend):
    name = "matplotlib"

    @classmethod
    def available(cls) -> bool:
        return importlib.util.find_spec("matplotlib") is not None

    def __init__(self):
        from matplotlib.figure import Figure  # type: ignore

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
            color=style["color"],
            linestyle={"dash": "dashed", "dot": "dotted"}.get(style["line"], style["line"]),
            marker={"circle": "o", "triangle": "^", "square": "s"}.get(style["marker"]),
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
        filename = filename.with_suffix(".png")
        util.log.info(f"Written: {filename}")
        self.figure.savefig(filename)


class PlotlyBackend(PlotBackend):
    name = "plotly"

    @classmethod
    def available(cls) -> bool:
        return importlib.util.find_spec("plotly") is not None

    def __init__(self):
        import plotly.graph_objects as go  # type: ignore

        self.figure = go.Figure()

    def add_line(
        self,
        legend: str,
        xpoints: List[float],
        ypoints: List[float],
        style: Dict[str, str],
        mode="lines",
    ):
        self.figure.add_scatter(x=xpoints, y=ypoints, mode=mode, name=legend)

    def add_scatter(self, *args, **kwargs):
        self.add_line(*args, **kwargs, mode="markers")

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
        if self.figure.layout.xaxis.type == "log":
            self.figure.layout.xaxis.range = [
                math.log10(value[0]),
                math.log10(value[1]),
            ]
        else:
            self.figure.layout.xaxis.range = value

    def set_ylim(self, value: List[float]):
        if self.figure.layout.yaxis.type == "log":
            self.figure.layout.yaxis.range = [
                math.log10(value[0]),
                math.log10(value[1]),
            ]
        else:
            self.figure.layout.yaxis.range = value

    def generate(self, filename: Path):
        filename = filename.with_suffix(".html")
        util.log.info(f"Written: {filename}")
        self.figure.write_html(str(filename))


class CSVBackend(PlotBackend):
    name = "csv"

    columns: List[List[float]]
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
        self.legend.extend([f"{legend} (x-axis)", legend])

    add_scatter = add_line

    def generate(self, filename: Path):
        filename = filename.with_suffix(".csv")
        util.log.info(f"Written: {filename}")
        maxlen = max(len(c) for c in self.columns)
        cols = [list(c) + [None] * (maxlen - len(c)) for c in self.columns]  # type: ignore
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.legend)
            for row in zip(*cols):
                writer.writerow(row)


class PlotStyleManager:
    _category_to_style: bidict
    _custom_styles: Dict[str, List[str]]
    _mode: Literal["line", "scatter"]
    _defaults = {
        "color": {
            "category": {
                None: ["blue", "red", "green", "magenta", "cyan", "black"],
            },
            "single": {
                None: ["blue"],
            },
        },
        "line": {
            "category": {
                "line": ["solid", "dash", "dot", "dashdot"],
                "scatter": ["none"],
            },
            "single": {
                "line": ["solid"],
                "scatter": ["none"],
            },
        },
        "marker": {
            "category": {
                None: ["circle", "triangle", "square"],
            },
            "single": {
                "line": ["none"],
                "scatter": ["circle"],
            },
        },
    }

    def __init__(self):
        self._category_to_style = bidict()
        self._custom_styles = dict()
        self._mode = "line"

    def assigned(self, category: str):
        return category in self._category_to_style

    def assign(self, category: str, style: Optional[str] = None):
        if style is None:
            candidates = list(s for s in self._defaults if s not in self._category_to_style.inverse)
            if self._mode == "scatter":
                try:
                    candidates.remove("line")
                except ValueError:
                    pass
            assert candidates
            style = candidates[0]
        assert style != "line" or self._mode != "scatter"
        self._category_to_style[category] = style

    def set_values(self, style: str, values: List[str]):
        self._custom_styles[style] = values

    def get_values(self, style: str) -> List[str]:
        # Prioritize user customizations
        if style in self._custom_styles:
            return self._custom_styles[style]

        def getter(d, k):
            return d.get(k, d.get(None, []))

        s = getter(self._defaults, style)
        s = getter(s, "category" if style in self._category_to_style.inverse else "single")
        s = getter(s, self._mode)
        return s

    def styles(self, space: ParameterSpace, *categories: str) -> Iterable[Dict[str, str]]:
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
            if style not in basestyle and self._category_to_style.get("yaxis") != style:
                basestyle[style] = self.get_values(style)[0]
        if "yaxis" in self._category_to_style:
            ystyle = self._category_to_style["yaxis"]
            for v in self.get_values(ystyle):
                yield {**basestyle, ystyle: v}
        else:
            yield basestyle


@dataclass(frozen=True)
class PlotMode:
    kind: str
    arg: Any

    @staticmethod
    def from_schema(schema: PlotModeSchema) -> PlotMode:
        return PlotMode(schema.mode, getattr(schema, "argument", None))


@dataclass
class Plot:
    parameters: Dict[str, PlotMode]
    filename: str
    fmt: List[str]
    yaxis: List[str]
    xaxis: str
    kind: Optional[Literal["scatter", "line"]]
    legend: Optional[str]
    xlabel: Optional[str]
    ylabel: Optional[str]
    title: Optional[str]
    xmode: Literal["linear", "log"]
    ymode: str
    grid: bool
    xlim: Optional[Tuple[float, float]]
    ylim: Optional[Tuple[float, float]]

    schema: PlotSchema

    @staticmethod
    def from_schema(schema: PlotSchema, paramspace: ParameterSpace) -> Plot:
        default = PlotModeIgnoreSchema()
        parameters = {name: PlotMode.from_schema(schema.parameters.get(name, default)) for name in paramspace}

        # If there is exactly one variate, and the x-axis is not given, assume that is the x-axis
        variates = [param for param, kind in parameters.items() if kind == "variate"]
        nvariate = len(variates)
        if nvariate == 1 and schema.xaxis is None:
            xaxis = next(iter(variates))
        else:
            xaxis = schema.xaxis

        return Plot(
            **schema.model_dump(exclude={"style", "parameters", "xaxis"}),
            parameters=parameters,
            xaxis=xaxis,
            schema=schema,
        )

    @cached_property
    def styles(self) -> PlotStyleManager:
        styles = PlotStyleManager()
        for key, value in self.schema.style.model_dump().items():
            if value is None:
                continue
            styles.set_values(key, value)
        for param in self._parameters_of_kind("category", req_arg=True):
            styles.assign(param, self.parameters[param].arg)
        for param in self._parameters_of_kind("category", req_arg=False):
            styles.assign(param)
        if len(self.yaxis) > 1 and not styles.assigned("yaxis"):
            styles.assign("yaxis")
        return styles

    def _parameters_of_kind(self, *kinds: str, req_arg: Optional[bool] = None):
        return [
            param
            for param, mode in self.parameters.items()
            if mode.kind in kinds
            and (
                req_arg is None
                or (req_arg is True and mode.arg is not None)
                or (req_arg is False and mode.arg is None)
            )
        ]

    def _parameters_not_of_kind(self, *kinds: str):
        return [param for param, mode in self.parameters.items() if mode.kind not in kinds]

    def _validate_kind(self, case: Case):
        types = case.type_guess()

        # Either all the axes are list type or none of them are
        list_type = types[self.yaxis[0]].is_list
        assert all(types[k].is_list == list_type for k in self.yaxis[1:])
        if self.xaxis is not None:
            assert types[self.xaxis].is_list == list_type

        # If the x-axis has list type, the effective number of variates is one higher
        nvariate = len(self._parameters_of_kind("variate"))
        eff_variates = nvariate + list_type

        # If there are more than one effective variate, the plot must be scatter
        if eff_variates > 1:
            if self.kind != "scatter" and self.kind is not None:
                util.log.warning("Line plots can have at most one variate dimension")
            self.kind = "scatter"
        elif eff_variates == 0:
            util.log.error("Plot has no effective variate dimensions")
            return False
        else:
            self.kind = "line"
        self.styles._mode = self.kind

        return True

    def generate_all(self, case: Case):
        if not self._validate_kind(case):
            return

        assert self.kind is not None

        # Pick a parameter context with 'default' values
        background = {name: param[0] for name, param in case.context_mgr.parameters.items()}

        # Collect all the fixed parameters and iterate over all those combinations
        fixed = self._parameters_of_kind("fixed")

        constants = {
            param: self.parameters[param].arg for param in self._parameters_of_kind("ignore", req_arg=True)
        }

        for index in case.parameters.subspace(*fixed):
            context = {**background, **index, **constants}
            self.generate_single(case, context, index)

    def generate_single(self, case: Case, context, index):
        # Collect all the categorized parameters and iterate over all those combinations
        categories = self._parameters_of_kind("category")
        backends = Backends(*self.fmt)
        plotter = operator.attrgetter(f"add_{self.kind}")

        sub_indices = case.parameters.subspace(*categories)
        styles = self.styles.styles(case.parameters, *categories)
        sub_context = api.Context()
        for sub_index, basestyle in zip(sub_indices, styles):
            sub_context = case.context_mgr.evaluate_context({**context, **sub_index})
            sub_index = {**index, **sub_index}

            cat_name, xaxis, yaxes = self.generate_category(case, sub_context, sub_index)

            final_styles = self.styles.supplement(basestyle)
            for ax_name, data, style in zip(self.yaxis, yaxes, final_styles):
                legend = self.generate_legend(sub_context, ax_name)
                plotter(backends)(legend, xpoints=xaxis, ypoints=data, style=style)

        for attr in ["title", "xlabel", "ylabel"]:
            template = getattr(self, attr)
            if template is None:
                continue
            text = render(template, sub_context)
            getattr(backends, f"set_{attr}")(text)
        backends.set_xmode(self.xmode)
        backends.set_ymode(self.ymode)
        backends.set_grid(self.grid)
        if self.xlim:
            backends.set_xlim(self.xlim)
        if self.ylim:
            backends.set_ylim(self.ylim)

        filename = case.storagepath / render(self.filename, sub_context)
        backends.generate(filename)

    def generate_category(self, case, context: dict, index):
        # TODO: Pick only finished results
        data = case.load_dataframe()
        if isinstance(data, pd.Series):
            data = data.to_frame().T
        for name, value in index.items():
            data = data[data[name] == value]

        # Collapse ignorable parameters
        for ignore in self._parameters_of_kind("ignore", req_arg=False):
            others = [p for p in case.parameters if p != ignore]
            data = data.groupby(by=others).first().reset_index()

        # Remove unnecessary columns
        to_keep = set(self.parameters) | set(self.yaxis)
        if self.xaxis is not None:
            to_keep.add(self.xaxis)
        to_remove = [c for c in data.columns if c not in to_keep]
        data = data.drop(columns=to_remove)

        # Collapse mean parameters
        for mean in self._parameters_of_kind("mean"):
            others = [p for p in case.parameters if p != mean]
            data = data.groupby(by=others)
            data = data.aggregate(util.flexible_mean)
            data = data.reset_index()

        # Extract data
        ydata = [util.flatten(data[f].to_numpy()) for f in self.yaxis]
        if self.xaxis:
            xdata = util.flatten(data[self.xaxis].to_numpy())
        else:
            length = max(len(f) for f in ydata)
            xdata = np.arange(1, length + 1)

        if any(self._parameters_of_kind("category")):
            name = ", ".join(f"{k}={repr(context[k])}" for k in self._parameters_of_kind("category"))
        else:
            name = None

        return name, xdata, ydata

    def generate_legend(self, context: dict, yaxis: str) -> str:
        if self.legend is not None:
            return render(self.legend, api.Context(**context, yaxis=yaxis))
        if any(self._parameters_of_kind("category")):
            name = ", ".join(f"{k}={repr(context[k])}" for k in self._parameters_of_kind("category"))
            return f"{name} ({yaxis})"
        return yaxis
