from contextlib import contextmanager
from difflib import Differ
import inspect
from itertools import product
from pathlib import Path
import pydoc
import re
import readline
import shlex
import shutil
import subprocess
from tempfile import TemporaryDirectory
from time import time as osclock

from typing import Dict, List, Any, Iterable

from fasteners import InterProcessLock
import numpy as np
import numpy.ma as ma
from simpleeval import SimpleEval, DEFAULT_FUNCTIONS, NameNotDefined
import treelog as log
from typing_inspect import get_origin, get_args

from badger.render import render
from badger.schema import load_and_validate
from badger.util import find_subclass, subindex_set, has_data, completer, NestedDict, dict_product


__version__ = '0.1.0'


@contextmanager
def time():
    start = osclock()
    yield lambda: end - start
    end = osclock()


def _numpy_dtype(tp):
    if tp in (int, float):
        return tp
    if isinstance(tp, NestedDict):
        return list(tp.items())
    return object


def _typename(tp) -> str:
    try:
        return {int: 'integer', str: 'string', float: 'float'}[tp]
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


class Parameter:

    @classmethod
    def load(cls, name, spec):
        if isinstance(spec, list):
            return cls(name, spec)
        subcls = find_subclass(cls, spec['type'], root=False, attr='__tag__')
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

    def make_index(self, base=None, fill=None, **kwargs):
        if base is not None:
            base = list(base)
        else:
            base = [fill] * len(self)
        for i, param in enumerate(self):
            if param in kwargs:
                base[i] = kwargs[param]
        return tuple(base)

    def subspace(self, *names) -> Iterable[Dict]:
        params = [self[name] for name in names]
        indexes = [range(len(p)) for p in params]
        for index, values in zip(dict_product(names, indexes), dict_product(names, params)):
            yield self.make_index(**index), values

    def fullspace(self) -> Iterable[Dict]:
        yield from self.subspace(*self.keys())


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

            if not sourcepath.exists():
                level = log.warning if ignore_missing else log.error
                level(f"Missing file: {logsrc}")
                if not ignore_missing:
                    return
            else:
                log.debug(logsrc, '->', logtgt)

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

    def add_types(self, types: NestedDict):
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
    def load(cls, spec):
        if isinstance(spec, (str, list)):
            return cls(spec)
        if 'capture-output' in spec:
            del spec['capture-output']
        return call_yaml(cls, spec)

    def __init__(self, command, name=None, capture=None, capture_walltime=False):
        self._command = command
        self._capture_walltime = capture_walltime

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

    def add_types(self, types: NestedDict):
        if self._capture_walltime:
            types[f'walltime/{self.name}'] = float
        for cap in self._capture:
            cap.add_types(types)

    def run(self, collector: 'ResultCollector', context: Dict, workpath: Path, logdir: Path) -> bool:
        kwargs = {
            'cwd': workpath,
            'capture_output': True,
            'shell': False,
        }

        if isinstance(self._command, str):
            kwargs['shell'] = True
            command = render(self._command, context, mode='shell')
        else:
            command = [render(arg, context) for arg in self._command]

        with log.context(self.name):
            log.debug(command if isinstance(command, str) else ' '.join(command))
            with time() as duration:
                result = subprocess.run(command, **kwargs)
            duration = duration()

            if logdir:
                stdout_path = logdir / f'{self.name}.stdout'
                with open(stdout_path, 'wb') as f:
                    f.write(result.stdout)
                stderr_path = logdir / f'{self.name}.stderr'
                with open(stderr_path, 'wb') as f:
                    f.write(result.stderr)

            stdout = result.stdout.decode()
            for capture in self._capture:
                capture.find_in(collector, stdout)
            if self._capture_walltime:
                collector.collect(f'walltime/{self.name}', duration)

            if result.returncode:
                log.error(f"Command returned exit status {result.returncode}")
                if logdir:
                    log.error(f"stdout stored in {stdout_path}")
                    log.error(f"stderr stored in {stderr_path}")
                return False
            else:
                log.info(f"Success ({duration:.3g}s)")

        return True


class Plot:

    _parameters: Dict[str, str]
    _filename: str
    _format: List[str]
    _yaxis: List[str]
    _xaxis: str
    _type: str

    @classmethod
    def load(cls, spec, parameters, types):
        # All parameters not mentioned are assumed to be fixed
        for param in parameters:
            spec['parameters'].setdefault(param, 'fixed')

        # If there is exactly one variate, and the x-axis is not given, assume that is the x-axis
        variates = [param for param, kind in spec['parameters'] if kind == 'variate']
        nvariate = len(variates)
        if nvariate == 1 and 'xaxis' not in spec:
            spec['xaxis'] = next(iter(variates))
        elif 'xaxis' not in spec:
            log.error("Plot x-axis not given, and unable to guess one")
            return None

        # If the x-axis has list type, the effective number of variates is one higher
        eff_variates = variates + bool(get_origin(types[spec['xaxis']]) == list)

        # If there are more than one effective variate, the plot must be scatter
        if eff_variates > 1:
            if spec.get('type', 'scatter') != 'scatter':
                log.warning("Line plots can have at most one variate dimension")
            spec['type'] = 'scatter'
        elif eff_variates == 0:
            log.error("Plot has no effective variate dimensions")
            return
        else:
            spec.setdefault('type', 'line')

        for k in ('format', 'yaxis'):
            if isinstance(spec[k], str):
                spec[k] = [spec[k]]

        return call_yaml(cls, spec)

    def __init__(self, parameters, filename, format, yaxis, xaxis, type):
        self._parameters = parameters
        self._filename = filename
        self._format = format
        self._yaxis = yaxis
        self._xaxis = xaxis
        self._type = type


class ResultCollector(NestedDict):

    _types: NestedDict

    def __init__(self, types):
        super().__init__()
        self._types = types

    def collect(self, name: str, value: Any):
        tp = self._types[name]
        if get_origin(tp) == list:
            eltype = get_args(tp)[0]
            self.setdefault(name, []).append(eltype(value))
        else:
            self[name] = tp(value)

    def commit(self, array):
        for key, value in self.items():
            subindex_set(array.mask, key, False)
            subindex_set(array, key, value)


class Case:

    yamlpath: Path
    sourcepath: Path
    storagepath: Path

    _parameters: ParameterSpace
    _evaluables: Dict[str, str]
    _constants: Dict[str, Any]
    _pre_files: List[FileMapping]
    _post_files: List[FileMapping]
    _commands: List[Command]
    _plots: List[Plot]
    _types: NestedDict

    def __init__(self, yamlpath='.', storagepath=None):
        if isinstance(yamlpath, str):
            yamlpath = Path(yamlpath)
        if yamlpath.is_dir():
            yamlpath = yamlpath / 'badger.yaml'
        self.yamlpath = yamlpath
        self.sourcepath = yamlpath.parent

        if storagepath is None:
            storagepath = self.sourcepath / '.badgerdata'
        storagepath.mkdir(parents=True, exist_ok=True)
        self.storagepath = storagepath

        with open(yamlpath, mode='r') as f:
            casedata = load_and_validate(f.read(), yamlpath)

        # Read parameters
        self._parameters = ParameterSpace()
        for name, paramspec in casedata.get('parameters', {}).items():
            param = Parameter.load(name, paramspec)
            self._parameters[param.name] = param

        # Read evaluables
        self._evaluables = dict(casedata.get('evaluate', {}))
        self._constants = dict(casedata.get('constants', {}))

        # Read file mappings
        self._pre_files = [FileMapping.load(spec, template=True) for spec in casedata.get('templates', [])]
        self._pre_files.extend(FileMapping.load(spec) for spec in casedata.get('prefiles', []))
        self._post_files = [FileMapping.load(spec) for spec in casedata.get('postfiles', [])]

        # Read commands
        self._commands = [Command.load(spec) for spec in casedata.get('script', [])]

        # Read types
        self._types = NestedDict(casedata.get('types', {}).items())

        # Guess types of parameters
        for name, param in self._parameters.items():
            if name not in self._types:
                self._types[name] = _guess_eltype(param)

        # Guess types of evaluables
        if any(name not in self._types for name in self._evaluables):
            contexts = list(ctx for _, ctx in self._parameters.fullspace())
            for ctx in contexts:
                self.evaluate_context(ctx, verbose=False)
            for name in self._evaluables:
                if name not in self._types:
                    values = [ctx[name] for ctx in contexts]
                    self._types[name] = _guess_eltype(values)

        # Fill in types derived from commands
        for cmd in self._commands:
            cmd.add_types(self._types)

        # Construct numpy dtype of result array
        self._dtype = self._types.map(_numpy_dtype).as_list_of_tuples()

        # Read settings
        settings = casedata.get('settings', {})
        self._logdir = settings.get('logdir', None)

        # Construct plot objects
        self._plots = [Plot.load(spec, self._parameters, self._types) for spec in casedata.get('plots', [])]

    def clear_cache(self):
        shutil.rmtree(self.storagepath)
        self.storagepath.mkdir(parents=True, exist_ok=True)

    def evaluate_context(self, context, verbose=True, allowed_missing=()):
        evaluator = SimpleEval(functions={**DEFAULT_FUNCTIONS,
            'log': np.log,
            'log2': np.log2,
            'log10': np.log10,
            'sqrt': np.sqrt,
            'abs': np.abs,
            'ord': ord,
        })
        evaluator.names.update(context)
        evaluator.names.update(self._constants)
        allowed_missing = set(allowed_missing)

        for name, code in self._evaluables.items():
            try:
                result = evaluator.eval(code) if isinstance(code, str) else code
            except NameNotDefined as error:
                if error.name in allowed_missing:
                    allowed_missing.add(name)
                    log.debug(f'Skipped evaluating: {name}')
                    continue
                else:
                    raise
            if verbose:
                log.debug(f'Evaluated: {name} = {repr(result)}')
            evaluator.names[name] = context[name] = result

    @property
    def shape(self):
        return tuple(map(len, self._parameters.values()))

    @contextmanager
    def acquire_lock(self):
        with InterProcessLock(self.storagepath / 'lockfile'):
            yield

    def commit_result(self, index, collector):
        with self.acquire_lock():
            results = self.result_array()
            collector.commit(results[index])
            np.save(self.storagepath / 'results.npy', results.data, allow_pickle=True)
            np.save(self.storagepath / 'results.mask.npy', results.mask, allow_pickle=True)

    def result_array(self):
        path = self.storagepath / 'results.npy'
        maskpath = self.storagepath / 'results.mask.npy'
        if path.is_file() and maskpath.is_file():
            return ma.array(
                np.load(path, allow_pickle=True),
                mask=np.load(maskpath),
            )
        else:
            return ma.array(
                np.zeros(self.shape, dtype=self._dtype),
                mask=np.ones(self.shape, dtype=bool)
            )

    def has_data(self):
        return has_data(self.result_array())

    def _check_decide_diff(self, diff: List[str], prev_file: Path, interactive: bool = True) -> bool:
        decision = None
        decisions = ['exit', 'diff', 'new-delete', 'new-keep', 'old']
        if interactive:
            readline.set_completer(completer(decisions))
            readline.parse_and_bind('tab: complete')
            log.warning("Warning: Badgerfile has changed and data have already been stored")
            log.warning("Pick an option:")
            log.warning("  exit - quit badger and fix the problem manually")
            log.warning("  diff - view a diff between old and new")
            log.warning("  new-delete - accept new version and delete existing data (significant changes made)")
            log.warning("  new-keep - accept new version and keep existing data (no significant changes made)")
            log.warning("  old - accept old version and exit (re-run badger to load the changed badgerfile)")
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
            log.error("Error: Badgerfile has changed and data have already been stored")
            log.error("Try running 'badger check' for more information, or delete .badgerdata if you're sure")
            return False
        return True

    def check(self, interactive=True) -> bool:
        if self._logdir is None:
            if self._post_files:
                log.error("Error: logdir must be set for capture of stdout, stderr or files")
                return False

        prev_file = self.storagepath / 'badger.yaml'
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
            log.info("Derived types:")
            for key, value in self._types.items():
                log.info(f"  {key}: {_typename(value)}")

        return True

    def run(self):
        parameters = list(self._parameters.fullspace())

        nsuccess = 0
        for index, namespace in log.iter.fraction('parameter', parameters):
            nsuccess += self.run_single(index, namespace)

        logger = log.user if nsuccess == len(parameters) else log.warning
        logger(f"{nsuccess} of {len(parameters)} succeeded")

    def run_single(self, index, namespace):
        log.user(', '.join(f'{k}={repr(v)}' for k, v in namespace.items()))
        self.evaluate_context(namespace)

        collector = ResultCollector(self._types)
        for key, value in namespace.items():
            collector.collect(key, value)

        namespace.update(self._constants)

        with TemporaryDirectory() as workpath:
            workpath = Path(workpath)

            if self._logdir:
                logdir = self.storagepath / render(self._logdir, namespace)
                logdir.mkdir(parents=True, exist_ok=True)
            else:
                logdir = None

            log.debug(f"Using SRC='{self.sourcepath}', WRK='{workpath}', LOG='{logdir}'")

            for filemap in self._pre_files:
                filemap.copy(namespace, self.sourcepath, workpath, sourcename='SRC', targetname='WRK')

            success = True
            for command in self._commands:
                if not command.run(collector, namespace, workpath, logdir):
                    self.commit_result(index, collector)
                    success = False
                    break

            if logdir:
                for filemap in self._post_files:
                    filemap.copy(namespace, workpath, logdir, sourcename='WRK', targetname='LOG', ignore_missing=not success)

        self.commit_result(index, collector)
        return success
