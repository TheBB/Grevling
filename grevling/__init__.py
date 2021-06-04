from collections.abc import Sequence
from contextlib import contextmanager
from datetime import datetime
from difflib import Differ
import inspect
from itertools import product
import json
import multiprocessing
import operator
import os
from pathlib import Path
import pydoc
import re
import shlex
import shutil
import subprocess
from tempfile import TemporaryDirectory
from time import time as osclock

from typing import Dict, List, Any, Iterable, Optional, Set

from bidict import bidict
from fasteners import InterProcessLock
import numpy as np
import pandas as pd
from simpleeval import SimpleEval, DEFAULT_FUNCTIONS, NameNotDefined
from typing_inspect import get_origin, get_args

from .plotting import Backends
from .render import render
from .schema import load_and_validate
from . import util


__version__ = '1.0.2'


@contextmanager
def time():
    start = osclock()
    yield lambda: end - start
    end = osclock()


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


def _guess_eltype(collection):
    if all(isinstance(v, str) for v in collection):
        return str
    if all(isinstance(v, int) for v in collection):
        return int
    assert all(isinstance(v, (int, float)) for v in collection)
    return float


def call_yaml(func, mapping, *args, **kwargs):
    signature = inspect.signature(func)
    mapping = {key.replace('-', '_'): value for key, value in mapping.items()}
    binding = signature.bind(*args, **kwargs, **mapping)
    return func(*binding.args, **binding.kwargs)


class Parameter(Sequence):

    @classmethod
    def load(cls, name, spec):
        if isinstance(spec, list):
            return cls(name, spec)
        subcls = util.find_subclass(cls, spec['type'], root=False, attr='__tag__')
        del spec['type']
        return call_yaml(subcls, spec, name)

    def __init__(self, name, values):
        self.name = name
        self.values = values

    def __len__(self):
        return len(self.values)

    def __getitem__(self, index):
        return self.values[index]


class UniformParameter(Parameter):

    __tag__ = 'uniform'

    def __init__(self, name, interval, num):
        super().__init__(name, np.linspace(*interval, num=num))


class GradedParameter(Parameter):

    __tag__ = 'graded'

    def __init__(self, name, interval, num, grading):
        lo, hi = interval
        step = (hi - lo) * (1 - grading) / (1 - grading ** (num - 1))
        values = [lo]
        for _ in range(num - 1):
            values.append(values[-1] + step)
            step *= grading
        super().__init__(name, np.array(values))


class ParameterSpace(dict):

    def subspace(self, *names: str) -> Iterable[Dict]:
        params = [self[name] for name in names]
        indexes = [range(len(p)) for p in params]
        for values in util.dict_product(names, params):
            yield values

    def fullspace(self) -> Iterable[Dict]:
        yield from self.subspace(*self.keys())

    def size(self, *names: str) -> int:
        return util.prod(len(self[name]) for name in names)

    def size_fullspace(self) -> int:
        return self.size(*self.keys())


class ContextManager:

    parameters: ParameterSpace
    evaluables: Dict[str, str]
    constants = Dict[str, Any]
    types: Dict[str, Any]

    def __init__(self, data: Dict):
        self.parameters = ParameterSpace()
        for name, paramspec in data.get('parameters', {}).items():
            param = Parameter.load(name, paramspec)
            self.parameters[param.name] = param

        self.evaluables = dict(data.get('evaluate', {}))
        self.constants = dict(data.get('constants', {}))

        self.types = {
            '_index': int,
            '_logdir': str,
            '_started': 'datetime64[ns]',
            '_finished': 'datetime64[ns]',
        }
        self.types.update(data.get('types', {}))

        # Guess types of parameters
        for name, param in self.parameters.items():
            if name not in self.types:
                self.types[name] = _guess_eltype(param)

        # Guess types of evaluables
        if any(name not in self.types for name in self.evaluables):
            contexts = list(self.parameters.fullspace())
            for ctx in contexts:
                self.evaluate_context(ctx, verbose=False)
            for name in self.evaluables:
                if name not in self.types:
                    values = [ctx[name] for ctx in contexts]
                    self.types[name] = _guess_eltype(values)

    def evaluate_context(self, context, verbose=True, allowed_missing=(), add_constants=True):
        evaluator = SimpleEval(functions={**DEFAULT_FUNCTIONS,
            'log': np.log,
            'log2': np.log2,
            'log10': np.log10,
            'sqrt': np.sqrt,
            'abs': np.abs,
            'ord': ord,
            'sin': np.sin,
            'cos': np.cos,
        })
        evaluator.names.update(context)
        evaluator.names.update({
            k: v for k, v in self.constants.items() if k not in context
        })
        if allowed_missing is not True:
            allowed_missing = set(allowed_missing)

        for name, code in self.evaluables.items():
            try:
                result = evaluator.eval(code) if isinstance(code, str) else code
            except NameNotDefined as error:
                if allowed_missing is True:
                    util.log.debug(f'Skipped evaluating: {name}')
                    continue
                elif error.name in allowed_missing:
                    allowed_missing.add(name)
                    util.log.debug(f'Skipped evaluating: {name}')
                    continue
                else:
                    raise
            if verbose:
                util.log.debug(f'Evaluated: {name} = {repr(result)}')
            evaluator.names[name] = context[name] = result

        if add_constants:
            for k, v in self.constants.items():
                if k not in context:
                    context[k] = v

        return context

    def subspace(self, *names: str, **kwargs) -> Iterable[Dict]:
        for values in self.parameters.subspace(*names):
            yield self.evaluate_context(values, **kwargs)

    def fullspace(self, **kwargs) -> Iterable[Dict]:
        yield from self.subspace(*self.parameters, **kwargs)


class FileMapping:

    source: str
    target: str
    template: bool
    mode: str

    @classmethod
    def load(cls, spec: dict, **kwargs):
        if isinstance(spec, str):
            return cls(spec, spec, **kwargs)
        return call_yaml(cls, spec, **kwargs)

    def __init__(self, source, target=None, template=False, mode='simple'):
        if target is None:
            target = source if mode == 'simple' else '.'
        if template:
            mode = 'simple'

        self.source = source
        self.target = target
        self.template = template
        self.mode = mode

    def iter_paths(self, context, sourcepath, targetpath):
        if self.mode == 'simple':
            yield (
                sourcepath / render(self.source, context),
                targetpath / render(self.target, context),
            )

        elif self.mode == 'glob':
            target = targetpath / render(self.target, context)
            for path in sourcepath.glob(render(self.source, context)):
                path = path.relative_to(sourcepath)
                yield (sourcepath / path, target / path)

    def copy(self, context, sourcepath, targetpath, sourcename='SRC', targetname='TGT', ignore_missing=False):
        for source, target in self.iter_paths(context, sourcepath, targetpath):
            logsrc = Path(sourcename) / source.relative_to(sourcepath)
            logtgt = Path(targetname) / target.relative_to(targetpath)

            if not source.exists():
                level = util.log.warning if ignore_missing else util.log.error
                level(f"Missing file: {logsrc}")
                if ignore_missing:
                    continue
            else:
                util.log.debug(f'{logsrc} -> {logtgt}')

            target.parent.mkdir(parents=True, exist_ok=True)
            if not self.template:
                shutil.copyfile(source, target)
                continue
            with open(source, 'r') as f:
                text = f.read()
            with open(target, 'w') as f:
                f.write(render(text, context))


class Capture:

    @classmethod
    def load(cls, spec):
        if isinstance(spec, str):
            return cls(spec)
        if spec.get('type') in ('integer', 'float'):
            pattern, tp = {
                'integer': (r'[-+]?[0-9]+', int),
                'float': (r'[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?', float),
            }[spec['type']]
            pattern = re.escape(spec['prefix']) + r'\s*[:=]?\s*(?P<' + spec['name'] + '>' + pattern + ')'
            mode = spec.get('mode', 'last')
            type_overrides = {spec['name']: tp}
            return cls(pattern, mode, type_overrides)
        return call_yaml(cls, spec)

    def __init__(self, pattern, mode='last', type_overrides=None):
        self._regex = re.compile(pattern)
        self._mode = mode
        self._type_overrides = type_overrides or {}

    def add_types(self, types: Dict[str, Any]):
        for group in self._regex.groupindex.keys():
            single = self._type_overrides.get(group, str)
            if self._mode == 'all':
                types.setdefault(group, List[single])
            else:
                types.setdefault(group, single)

    def find_in(self, collector, string):
        matches = self._regex.finditer(string)
        if self._mode == 'first':
            try:
                matches = [next(matches)]
            except StopIteration:
                pass

        elif self._mode == 'last':
            for match in matches:
                pass
            try:
                matches = [match]
            except UnboundLocalError:
                pass

        for match in matches:
            for name, value in match.groupdict().items():
                collector.collect(name, value)


class Command:

    @classmethod
    def load(cls, spec, containers={}):
        if isinstance(spec, (str, list)):
            return cls(spec)
        if 'capture-output' in spec:
            del spec['capture-output']
        return call_yaml(cls, spec, container_args=containers)

    def __init__(self, command, name=None, capture=None, capture_walltime=False,
                retry_on_fail=False, env=None, container=None, container_args={},
                allow_failure=False):
        self._command = command
        self._capture_walltime = capture_walltime
        self._retry_on_fail = retry_on_fail
        self._env = env
        self._allow_failure = allow_failure

        self._container = container
        self._container_args = container_args.get(container)

        if name is None:
            exe = shlex.split(command)[0] if isinstance(command, str) else command[0]
            self.name = Path(exe).name
        else:
            self.name = name

        self._capture = []
        if isinstance(capture, (str, dict)):
            self._capture.append(Capture.load(capture))
        elif isinstance(capture, list):
            self._capture.extend(Capture.load(c) for c in capture)

    def add_types(self, types: Dict[str, Any]):
        if self._capture_walltime:
            types[f'walltime/{self.name}'] = float
        for cap in self._capture:
            cap.add_types(types)

    @util.with_context('{self.name}')
    def run(self, collector: 'ResultCollector', context: Dict, workpath: Path, logdir: Path) -> bool:
        kwargs = {
            'cwd': workpath,
            'shell': False,
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
        }

        if self._env:
            kwargs['env'] = os.environ.copy()
            for k, v in self._env.items():
                kwargs['env'][k] = render(v, context)

        if isinstance(self._command, str):
            kwargs['shell'] = True
            command = render(self._command, context, mode='shell')
        else:
            command = [render(arg, context) for arg in self._command]

        if self._container:
            if isinstance(command, list):
                command = ' '.join(shlex.quote(c) for c in command)
            if isinstance(self._container_args, str):
                args = shlex.split(render(self._container_args, context, mode='shell'))
            elif isinstance(self._container_args, list):
                args = [render(arg, context) for arg in self._container_args]
            else:
                args = []
            command = [
                'docker', 'run', *args, f'-v{workpath}:/workdir', '--workdir', '/workdir',
                self._container, 'bash', '-c', command,
            ]
            kwargs['shell'] = False

        util.log.debug(command if isinstance(command, str) else ' '.join(shlex.quote(c) for c in command))
        with time() as duration:
            while True:
                result = subprocess.run(command, **kwargs)
                if self._retry_on_fail and result.returncode:
                    util.log.info('Failed, retrying...')
                    continue
                break
        duration = duration()

        stdout_path = logdir / f'{self.name}.stdout'
        with open(stdout_path, 'wb') as f:
            f.write(result.stdout)
        stderr_path = logdir / f'{self.name}.stderr'
        with open(stderr_path, 'wb') as f:
            f.write(result.stderr)

        self.capture(collector, stdout=result.stdout, duration=duration)

        if result.returncode:
            level = util.log.warn if self._allow_failure else util.log.error
            level(f"command returned exit status {result.returncode}")
            level(f"stdout stored in {stdout_path}")
            level(f"stderr stored in {stderr_path}")
            return self._allow_failure
        else:
            util.log.info(f"success ({util.format_seconds(duration)})")

        return True

    def capture(self, collector: 'ResultCollector', stdout=None, logdir=None, duration=None):
        if stdout is None:
            assert logdir is not None
            with open(logdir / f'{self.name}.stdout', 'rb') as f:
                stdout = f.read()
        stdout = stdout.decode()
        for capture in self._capture:
            capture.find_in(collector, stdout)
        if self._capture_walltime and duration is not None:
            collector.collect(f'walltime/{self.name}', duration)


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

        return call_yaml(cls, spec)

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


class ResultCollector(dict):

    _types: Dict[str, Any]

    def __init__(self, types):
        super().__init__()
        self._types = types

    def collect_from_file(self, path: Path):
        with open(path / 'grevlingcontext.json', 'r') as f:
            data = json.load(f)
        for key, value in data.items():
            self.collect(key, value)

    def collect(self, name: str, value: Any):
        if name not in self._types:
            return
        tp = self._types[name]
        if util.is_list_type(tp) and not isinstance(value, list):
            eltype = get_args(tp)[0]
            self.setdefault(name, []).append(eltype(value))
        elif not isinstance(tp, str) and not util.is_list_type(tp):
            self[name] = tp(value)
        else:
            self[name] = value

    def commit_to_file(self, merge=True):
        path = Path(self['_logdir']) / 'grevlingcontext.json'

        data = self
        if merge and path.exists():
            try:
                with open(path, 'r') as f:
                    existing_data = json.load(f)
            except:
                pass
            data = {**existing_data, **data}

        with open(path, 'w') as f:
            json.dump(data, f, sort_keys=True, indent=4, cls=util.JSONEncoder)

    def commit_to_dataframe(self, data):
        index = self['_index']
        data.loc[index, :] = [None] * data.shape[1]
        for key, value in self.items():
            if key == '_index':
                continue
            data.at[index, key] = value
        return data


class Case:

    yamlpath: Path
    sourcepath: Path
    storagepath: Path
    dataframepath: Path

    context_mgr: ContextManager

    _pre_files: List[FileMapping]
    _post_files: List[FileMapping]
    _commands: List[Command]
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

        self.context_mgr = ContextManager(casedata)

        # Read file mappings
        self._pre_files = [FileMapping.load(spec, template=True) for spec in casedata.get('templates', [])]
        self._pre_files.extend(FileMapping.load(spec) for spec in casedata.get('prefiles', []))
        self._post_files = [FileMapping.load(spec) for spec in casedata.get('postfiles', [])]

        # Read commands
        containers = casedata.get('containers', {})
        self._commands = [Command.load(spec, containers) for spec in casedata.get('script', [])]

        # Fill in types derived from commands
        for cmd in self._commands:
            cmd.add_types(self.context_mgr.types)

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

    def iter_instancedirs(self):
        for path in self.storagepath.iterdir():
            if not (path / 'grevlingcontext.json').exists():
                continue
            yield path

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

    def run(self, nprocs: Optional[int] = None):
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

    def capture(self):
        for index, namespace in enumerate(self._context.fullspace()):
            namespace['_index'] = index
            namespace['_logdir'] = logdir = self.storagepath / render(self._logdir, namespace)
            if not logdir.exists():
                continue

            collector = ResultCollector(self._types)
            for key, value in namespace.items():
                collector.collect(key, value)

            namespace.update(self._constants)

            for command in self._commands:
                command.capture(collector, logdir=logdir)

            collector.commit_to_file()

    def collect(self):
        with self.lock():
            data = self.load_dataframe()
            for path in self.iter_instancedirs():
                collector = ResultCollector(self.context_mgr.types)
                collector.collect_from_file(path)
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
        logdir.mkdir(parents=True, exist_ok=True)

        collector = ResultCollector(self.context_mgr.types)
        for key, value in namespace.items():
            collector.collect(key, value)
        collector.collect('_started', pd.Timestamp.now())

        with TemporaryDirectory() as workpath:
            workpath = Path(workpath)

            util.log.debug(f"Using SRC='{self.sourcepath}', WRK='{workpath}', LOG='{logdir}'")

            for filemap in self._pre_files:
                filemap.copy(namespace, self.sourcepath, workpath, sourcename='SRC', targetname='WRK')

            success = True
            for command in self._commands:
                if not command.run(collector, namespace, workpath, logdir):
                    success = False
                    break

            ignore_missing = self._ignore_missing or not success
            for filemap in self._post_files:
                filemap.copy(namespace, workpath, logdir, sourcename='WRK', targetname='LOG', ignore_missing=ignore_missing)

        collector.collect('_finished', pd.Timestamp.now())
        collector.commit_to_file()
        return success
