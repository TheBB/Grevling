from contextlib import contextmanager
from difflib import Differ
import multiprocessing
import operator
import os
from pathlib import Path
import pydoc
import shutil

from typing import Dict, List, Any, Iterable, Optional

from bidict import bidict
from fasteners import InterProcessLock
import numpy as np
import pandas as pd
from typing_inspect import get_origin, get_args

from .plotting import Backends
from .render import render
from .schema import load_and_validate
from .parameters import ParameterSpace
from .context import ContextManager
from .filemap import FileMap
from .capture import ResultCollector
from .script import ScriptTemplate
from . import util, api
from .runner import local as runner


__version__ = '1.2.1'


def _pandas_dtype(tp):
    if tp == int:
        return pd.Int64Dtype()
    if util.is_list_type(tp):
        return object
    return tp


def _typename(tp) -> str:
    try:
        return {int: 'integer', str: 'string', float: 'float', 'datetime64[ns]': 'datetime'}[tp]
    except KeyError:
        base = {list: 'list'}[get_origin(tp)]
        subs = ', '.join(_typename(k) for k in get_args(tp))
        return f'{base}[{subs}]'


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
            candidates = list(s for s in self._defaults if s not in self._category_to_style.inverse)
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
        s = getter(s, 'category' if style in self._category_to_style.inverse else 'single')
        s = getter(s, self._mode)
        return s

    def styles(self, space: ParameterSpace, *categories: str) -> Iterable[Dict[str, str]]:
        names, values = [], []
        for c in categories:
            style = self._category_to_style[c]
            available_values = self.get_values(style)
            assert len(available_values) >= len(space[c])
            names.append(style)
            values.append(available_values[:len(space[c])])
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
        variates = [param for param, kind in spec['parameters'].items() if kind == 'variate']
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
        list_type = util.is_list_type(types[spec['yaxis'][0]])
        assert all(util.is_list_type(types[k]) == list_type for k in spec['yaxis'][1:])
        if spec['xaxis']:
            assert util.is_list_type(types[spec['xaxis']]) == list_type

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

    def __init__(self, parameters, filename, format, yaxis, xaxis, type,
                 legend=None, xlabel=None, ylabel=None, title=None, grid=True,
                 xmode='linear', ymode='linear', xlim=[], ylim=[],style={}):
        self._parameters = {name: PlotMode.load(value) for name, value in parameters.items()}
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
            if mode.kind in kinds and (
                req_arg is None or
                req_arg is True and mode.arg is not None or
                req_arg is False and mode.arg is None
            )
        ]

    def _parameters_not_of_kind(self, *kinds: str):
        return [param for param, mode in self._parameters.items() if mode.kind not in kinds]

    def generate_all(self, case: 'Case'):
        # Collect all the fixed parameters and iterate over all those combinations
        fixed = self._parameters_of_kind('fixed')
        unfixed = set(case.parameters.keys()) - set(fixed)

        constants = {
            param: self._parameters[param].arg
            for param in self._parameters_of_kind('ignore', req_arg=True)
        }

        for index in case.parameters.subspace(*fixed):
            index = {**index, **constants}
            context = case.context_mgr.evaluate_context(index.copy(), allowed_missing=unfixed)
            self.generate_single(case, context, index)

    def generate_single(self, case: 'Case', context: dict, index):
        # Collect all the categorized parameters and iterate over all those combinations
        categories = self._parameters_of_kind('category')
        noncats = set(case.parameters.keys()) - set(self._parameters_of_kind('fixed', 'category'))
        backends = Backends(*self._format)
        plotter = operator.attrgetter(f'add_{self._type}')

        sub_indices = case.parameters.subspace(*categories)
        styles = self._styles.styles(case.parameters, *categories)
        for sub_index, basestyle in zip(sub_indices, styles):
            sub_context = case.context_mgr.evaluate_context({**context, **sub_index}, allowed_missing=noncats)
            sub_index = {**index, **sub_index}

            cat_name, xaxis, yaxes = self.generate_category(case, sub_context, sub_index)

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

    def generate_category(self, case: 'Case', context: dict, index):
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
            name = ', '.join(f'{k}={repr(context[k])}' for k in self._parameters_of_kind('category'))
        else:
            name = None

        return name, xdata, ydata

    def generate_legend(self, context: dict, yaxis: str) -> str:
        if self._legend is not None:
            return render(self._legend, {**context, 'yaxis': yaxis})
        if any(self._parameters_of_kind('category')):
            name = ', '.join(f'{k}={repr(context[k])}' for k in self._parameters_of_kind('category'))
            return f'{name} ({yaxis})'
        return yaxis


class Case:

    yamlpath: Path
    sourcepath: Path
    storagepath: Path
    dataframepath: Path

    context_mgr: ContextManager

    premap: FileMap
    postmap: FileMap
    script: ScriptTemplate
    _plots: List[Plot]

    _logdir: str
    _ignore_missing: bool

    def __init__(self, yamlpath='.', storagepath=None, yamldata=None):
        if isinstance(yamlpath, str):
            yamlpath = Path(yamlpath)
        if yamlpath.is_dir():
            for candidate in ['grevling', 'badger']:
                if (yamlpath / f'{candidate}.yaml').exists():
                    yamlpath = yamlpath / f'{candidate}.yaml'
                    break
        assert yamlpath.is_file()
        self.yamlpath = yamlpath
        self.sourcepath = yamlpath.parent

        if storagepath is None:
            storagepath = self.sourcepath / '.grevlingdata'
        storagepath.mkdir(parents=True, exist_ok=True)
        self.storagepath = storagepath

        self.dataframepath = storagepath / 'dataframe.parquet'

        with open(yamlpath, mode='r') as f:
            yamldata = f.read()
        with open(yamlpath, mode='r') as f:
            casedata = load_and_validate(yamldata, yamlpath)

        self.context_mgr = ContextManager.load(casedata)

        # Read file mappings
        self.premap = FileMap.load(casedata.get('prefiles', []), casedata.get('templates', []))
        self.postmap = FileMap.load(casedata.get('postfiles', []))

        # Read commands
        self.script = ScriptTemplate.load(casedata.get('script', []), casedata.get('containers', {}))

        # Fill in types derived from commands
        self.script.add_types(self.context_mgr.types)

        # Read settings
        settings = casedata.get('settings', {})
        self._logdir = settings.get('logdir', '${_index}')
        self._ignore_missing = settings.get('ignore-missing-files', False)

        # Construct plot objects
        self._plots = [Plot.load(spec, self.parameters, self.types) for spec in casedata.get('plots', [])]

    @property
    def parameters(self):
        return self.context_mgr.parameters

    @property
    def types(self):
        return self.context_mgr.types

    def clear_cache(self):
        shutil.rmtree(self.storagepath)
        self.storagepath.mkdir(parents=True, exist_ok=True)

    def clear_dataframe(self):
        with self.lock():
            self.dataframepath.unlink(missing_ok=True)

    def iter_instancedirs(self) -> Iterable[api.Workspace]:
        for path in self.storagepath.iterdir():
            if not (path / 'grevlingcontext.json').exists():
                continue
            yield runner.LocalWorkspace(path)

    @property
    def shape(self):
        return tuple(map(len, self._parameters.values()))

    @contextmanager
    def lock(self):
        with InterProcessLock(self.storagepath / 'lockfile'):
            yield

    def load_dataframe(self):
        if self.dataframepath.is_file():
            return pd.read_parquet(self.dataframepath, engine='pyarrow')
        data = {
            k: pd.Series([], dtype=_pandas_dtype(v))
            for k, v in self.context_mgr.types.items()
            if k != '_index'
        }
        return pd.DataFrame(index=pd.Int64Index([]), data=data)

    def save_dataframe(self, df: pd.DataFrame):
        df.to_parquet(self.dataframepath, engine='pyarrow', index=True)

    def has_data(self):
        with self.lock():
            df = self.load_dataframe()
        if df['_finished'].any():
            return True
        if any(self.iter_instancedirs()):
            return True
        return False

    def _check_decide_diff(self, diff: List[str], prev_file: Path, interactive: bool = True) -> bool:
        decision = None
        decisions = ['exit', 'diff', 'new-delete', 'new-keep', 'old']
        if interactive:
            if os.name == 'nt':
                from pyreadline import Readline
                readline = Readline()
            else:
                import readline
            readline.set_completer(util.completer(decisions))
            readline.parse_and_bind('tab: complete')
            util.log.warning("Warning: Grevlingfile has changed and data have already been stored")
            util.log.warning("Pick an option:")
            util.log.warning("  exit - quit grevling and fix the problem manually")
            util.log.warning("  diff - view a diff between old and new")
            util.log.warning("  new-delete - accept new version and delete existing data (significant changes made)")
            util.log.warning("  new-keep - accept new version and keep existing data (no significant changes made)")
            util.log.warning("  old - accept old version and exit (re-run grevling to load the changed grevlingfile)")
            while decision is None:
                decision = input('>>> ').strip().lower()
                if decision not in decisions:
                    decision = None
                    continue
                if decision == 'diff':
                    pydoc.pager(''.join(diff))
                    decision = None
                if decision == 'exit':
                    return False
                if decision == 'new-delete':
                    self.clear_cache()
                    break
                if decision == 'new-keep':
                    break
                if decision == 'old':
                    shutil.copyfile(prev_file, self.yamlpath)
                    return False
        else:
            util.log.error("Error: Grevlingfile has changed and data have already been stored")
            util.log.error("Try running 'grevling check' for more information, or delete .grevlingdata if you're sure")
            return False
        return True

    def check(self, interactive=True) -> bool:
        if self._logdir is None:
            if self._post_files:
                util.log.error("Error: logdir must be set for capture of stdout, stderr or files")
                return False

        prev_file = self.storagepath / 'grevling.yaml'
        if prev_file.exists():
            with open(self.yamlpath, 'r') as f:
                new_lines = f.readlines()
            with open(prev_file, 'r') as f:
                old_lines = f.readlines()
            diff = list(Differ().compare(old_lines, new_lines))
            if not all(line.startswith('  ') for line in diff) and self.has_data():
                if not self._check_decide_diff(diff, prev_file, interactive=interactive):
                    return False

        shutil.copyfile(self.yamlpath, prev_file)

        if interactive:
            util.log.info("Derived types:")
            for key, value in self._types.items():
                util.log.info(f"  {key}: {_typename(value)}")

        return True

    def run(self, nprocs: Optional[int] = None) -> bool:
        instances = self.context_mgr.fullspace()

        if nprocs is None:
            nsuccess = 0
            for index, namespace in enumerate(instances):
                nsuccess += self.run_single(index, namespace)
        else:
            with multiprocessing.Pool(processes=nprocs, initializer=util.initialize_process) as pool:
                nsuccess = sum(pool.starmap(self.run_single, enumerate(instances)))

        size = self.parameters.size_fullspace()
        logger = util.log.info if nsuccess == size else util.log.error
        logger(f"{nsuccess} of {size} succeeded")

        return nsuccess == size

    def capture(self):
        for index, namespace in enumerate(self._context.fullspace()):
            namespace['_index'] = index
            namespace['_logdir'] = logdir = self.storagepath / render(self._logdir, namespace)
            if not logdir.exists():
                continue
            ws = runner.LocalWorkspace(logdir)

            collector = ResultCollector(self._types)
            for key, value in namespace.items():
                collector.collect(key, value)

            namespace.update(self._constants)
            self.script.capture(collector, workspace=ws)
            collector.commit_to_file(ws, merge=True)

    def collect(self):
        with self.lock():
            data = self.load_dataframe()
            for ws in self.iter_instancedirs():
                collector = ResultCollector(self.context_mgr.types)
                collector.collect_from_file(ws)
                data = collector.commit_to_dataframe(data)
            data = data.sort_index()
            self.save_dataframe(data)

    def plot(self):
        for plot in self._plots:
            plot.generate_all(self)

    @util.with_context('instance {index}')
    def run_single(self, index, namespace, logdir=None):
        util.log.info(', '.join(f'{k}={repr(namespace[k])}' for k in self.parameters))

        namespace['_index'] = index
        if logdir is not None:
            namespace['_logdir'] = str(logdir)
        else:
            namespace['_logdir'] = logdir = self.storagepath / render(self._logdir, namespace)
        log_ws = runner.LocalWorkspace(logdir, 'LOG')
        my_ws = runner.LocalWorkspace(self.sourcepath, 'SRC')

        collector = ResultCollector(self.context_mgr.types)
        for key, value in namespace.items():
            collector.collect(key, value)
        collector.collect('_started', pd.Timestamp.now())

        with runner.TempWorkspace('WRK') as temp_ws:
            util.log.debug(f"Using SRC='{my_ws}', WRK='{temp_ws}', LOG='{log_ws}'")

            ignore_missing = self._ignore_missing
            if not self.premap.copy(namespace, my_ws, temp_ws, ignore_missing=self._ignore_missing):
                return False

            success = self.script.run(collector, namespace, temp_ws.root, log_ws)

            ignore_missing = self._ignore_missing or not success
            self.postmap.copy(namespace, temp_ws, log_ws, ignore_missing=ignore_missing)

        collector.collect('_finished', pd.Timestamp.now())
        collector.commit_to_file(log_ws)
        return success
