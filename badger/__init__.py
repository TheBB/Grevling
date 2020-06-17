from functools import reduce
import inspect
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
import shlex
import shutil
import subprocess
import re

import numpy as np
import numpy.ma as ma
from simpleeval import SimpleEval
import strictyaml as yaml
from ruamel.yaml.error import MarkedYAMLError
import treelog as log

from badger.util import find_subclass
from badger.render import render


__version__ = '0.1.0'


class Literal(yaml.ScalarValidator):

    def __init__(self, expected):
        super().__init__()
        self._expected = expected

    def validate_scalar(self, chunk):
        if self._expected != chunk.contents:
            chunk.expecting_but_found(f"when expecting {self._expected}", "found non-matching string")
        return chunk.contents


Choice = lambda *args: reduce(lambda x,y: x|y, map(Literal, args))
Scalar = lambda: yaml.Int() | yaml.Float()
FileMappingValidator = lambda: yaml.Str() | yaml.Map({'source': yaml.Str(), 'target': yaml.Str()})
RegexValidator = lambda: yaml.Map({
    'pattern': yaml.Str(),
    yaml.Optional('mode'): Choice('first', 'last', 'all'),
})

CASE_SCHEMA = yaml.Map({
    # Parameter and script validation happens separately
    yaml.Optional('parameters'): yaml.MapPattern(yaml.Str(), yaml.Any()),
    yaml.Optional('evaluate'): yaml.MapPattern(yaml.Str(), yaml.Str()),
    yaml.Optional('templates'): yaml.Seq(FileMappingValidator()),
    yaml.Optional('prefiles'): yaml.Seq(FileMappingValidator()),
    yaml.Optional('postfiles'): yaml.Seq(FileMappingValidator()),
    yaml.Optional('script'): yaml.Seq(yaml.Any()),
    yaml.Optional('settings'): yaml.Map({
        yaml.Optional('logdir'): yaml.Str(),
    }),
    yaml.Optional('types'): yaml.MapPattern(
        yaml.Str(),
        Choice('int', 'integer', 'str', 'string', 'float', 'floating', 'double'),
    ),
})

PARAM_SCHEMAS = [
    yaml.Seq(Scalar()),
    yaml.Seq(yaml.Str()),
    yaml.Map({
        'type': Literal('uniform'),
        'interval': yaml.FixedSeq([Scalar(), Scalar()]),
        'num': yaml.Int(),
    }),
    yaml.Map({
        'type': Literal('graded'),
        'interval': yaml.FixedSeq([Scalar(), Scalar()]),
        'num': yaml.Int(),
        'grading': Scalar(),
    })
]

COMMAND_SCHEMAS = [
    yaml.Str(),
    yaml.Seq(yaml.Str()),
    yaml.Map({
        'command': yaml.Str() | yaml.Seq(yaml.Str()),
        yaml.Optional('name'): yaml.Str(),
        yaml.Optional('capture-output'): yaml.Bool(),
        yaml.Optional('capture'): yaml.Str() | RegexValidator() | yaml.Seq(yaml.Str() | RegexValidator()),
    })
]

TYPES = {
    'int': int,
    'integer': int,
    'str': str,
    'string': str,
    'float': float,
    'floating': float,
    'double': float,
}


def _numpy_dtype(tp):
    if tp in (int, float):
        return tp
    return object


def validate_multiple(node, schemas, name):
    for schema in schemas:
        try:
            node.revalidate(schema)
            break
        except yaml.YAMLValidationError:
            pass
    else:
        raise yaml.YAMLValidationError(
            f"failed to find a valid schema for {name}",
            "found invalid input", node._chunk
        )


def load_and_validate(text, path):
    casedata = yaml.parser.generic_load(text, schema=CASE_SCHEMA, label=path, allow_flow_style=True)
    for name, paramspec in casedata.get('parameters', {}).items():
        validate_multiple(paramspec, PARAM_SCHEMAS, f"parameter {name}")
    for commandspec in casedata.get('script', []):
        validate_multiple(commandspec, COMMAND_SCHEMAS, "script command")
    return casedata.data


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


class FileMapping:

    @classmethod
    def load(cls, spec, **kwargs):
        if isinstance(spec, str):
            return cls(spec, spec, **kwargs)
        return call_yaml(cls, spec, **kwargs)

    def __init__(self, source, target, template=False):
        self.source = source
        self.target = target
        self.template = template

    def copy(self, context, sourcepath, targetpath):
        source = sourcepath / render(self.source, context)
        target = targetpath / render(self.target, context)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not self.template:
            shutil.copyfile(source, target)
            return
        with open(source, 'r') as f:
            text = f.read()
        with open(target, 'w') as f:
            f.write(render(text, context))


class Capture:

    @classmethod
    def load(cls, spec):
        if isinstance(spec, str):
            return cls(spec)
        return call_yaml(cls, spec)

    def __init__(self, pattern, mode='last'):
        self._regex = re.compile(pattern)
        self._mode = mode

    def find_in(self, collector, string):
        matches = self._regex.finditer(string)
        if self._mode == 'first':
            matches = [next(matches)]
        elif self._mode == 'last':
            for match in matches:
                pass
            matches = [match]

        for match in matches:
            for name, value in match.groupdict().items():
                collector.collect(name, value)


class Command:

    @classmethod
    def load(cls, spec):
        if isinstance(spec, (str, list)):
            return cls(spec)
        return call_yaml(cls, spec)

    def __init__(self, command, name=None, capture=None, capture_output=False):
        self._command = command
        self._capture_output = capture_output

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

    def run(self, collector, context, workpath, logdir):
        kwargs = {
            'cwd': workpath,
            'capture_output': True,
        }

        if isinstance(self._command, str):
            kwargs['shell'] = True
            command = render(self._command, context, mode='shell')
        else:
            command = [render(arg, context) for arg in self._command]

        result = subprocess.run(command, **kwargs)

        if logdir and (result.returncode or self._capture_output):
            stdout_path = logdir / f'{self.name}.stdout'
            with open(stdout_path, 'wb') as f:
                f.write(result.stdout)
            stderr_path = logdir / f'{self.name}.stderr'
            with open(stderr_path, 'wb') as f:
                f.write(result.stderr)

        if result.returncode:
            log.error(f"command {self.name} returned exit status {result.returncode}")
            if logdir:
                log.error(f"stdout stored in {stdout_path}")
                log.error(f"stderr stored in {stderr_path}")
            return False

        stdout = result.stdout.decode()
        for capture in self._capture:
            capture.find_in(collector, stdout)

        return True


class ResultCollector(dict):

    def __init__(self, types):
        super().__init__()
        self._types = types

    def collect(self, name, value):
        self[name] = self._types[name](value)


class Case:

    def __init__(self, yamlpath, storagepath=None):
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
        self._parameters = {}
        for name, paramspec in casedata.get('parameters', {}).items():
            param = Parameter.load(name, paramspec)
            self._parameters[param.name] = param

        # Read evaluables
        self._evaluables = dict(casedata.get('evaluate', {}))

        # Read file mappings
        self._pre_files = [FileMapping.load(spec, template=True) for spec in casedata.get('templates', [])]
        self._pre_files.extend(FileMapping.load(spec) for spec in casedata.get('prefiles', []))
        self._post_files = [FileMapping.load(spec) for spec in casedata.get('postfiles', [])]

        # Read commands
        self._commands = [Command.load(spec) for spec in casedata.get('script', [])]

        # Read types
        self._types = {key: TYPES[value] for key, value in casedata.get('types', {}).items()}
        self._dtype = [(key, _numpy_dtype(tp)) for key, tp in self._types.items()]

        # Read settings
        settings = casedata.get('settings', {})
        self._logdir = settings.get('logdir', None)

    def clear_cache(self):
        shutil.rmtree(self.storagepath)
        self.storagepath.mkdir(parents=True, exist_ok=True)

    @property
    def shape(self):
        return tuple(map(len, self._parameters.values()))

    def result_array(self):
        path = self.storagepath / 'results.npy'
        if path.is_file():
            return np.load(path, allow_pickle=True)
        else:
            return ma.array(
                np.zeros(self.shape, dtype=self._dtype),
                mask=np.ones(self.shape, dtype=bool)
            )

    def commit_result(self, array):
        np.save(self.storagepath / 'results.npy', array, allow_pickle=True)

    def check(self):
        if self._logdir is None:
            log.warning("Warning: logdir is not set; no stdout/stderr will be captured")

    def parameters(self):
        for values in product(*(param for param in self._parameters.values())):
            yield dict(zip(self._parameters, values))

    def run(self):
        self.check()

        parameters = list(self.parameters())
        results = self.result_array()

        nsuccess = 0
        for index, namespace in enumerate(log.iter.fraction('parameter', parameters)):
            collector = self.run_single(index, namespace)
            if collector is None:
                continue
            nsuccess += 1
            for key, value in collector.items():
                results.flat[index][key] = value

        self.commit_result(results)
        logger = log.info if nsuccess == len(parameters) else log.warning
        logger(f"{nsuccess} of {len(parameters)} succeeded")

    def run_single(self, index, namespace):
        # Handle all evaluables
        evaluator = SimpleEval()
        evaluator.names.update(namespace)
        for name, code in self._evaluables.items():
            result = evaluator.eval(code) if isinstance(code, str) else code
            evaluator.names[name] = namespace[name] = result

        collector = ResultCollector(self._types)

        with TemporaryDirectory() as workpath:
            workpath = Path(workpath)

            if self._logdir:
                logdir = self.storagepath / render(self._logdir, namespace)
                logdir.mkdir(parents=True, exist_ok=True)
            else:
                logdir = None

            for filemap in self._pre_files:
                filemap.copy(namespace, self.sourcepath, workpath)
            for command in self._commands:
                if not command.run(collector, namespace, workpath, logdir):
                    return False

        return collector
