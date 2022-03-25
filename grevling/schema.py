from copy import deepcopy
from functools import reduce
from pathlib import Path
import re

from typing import Any, Tuple, Dict

import goldpy as gold
import jsonschema
import jsonschema.validators
import yaml

from . import util


is_callable = lambda _, fn: callable(fn)
typechecker = jsonschema.Draft202012Validator.TYPE_CHECKER.redefine('callable', is_callable)
CustomValidator = jsonschema.validators.extend(jsonschema.Draft202012Validator, type_checker=typechecker)
CustomValidator.META_SCHEMA = {}


_null = { 'type': 'null' }
_scalar = { 'type': 'number' }
_integer = { 'type': 'integer' }
_string = { 'type': 'string' }
_bool = { 'type': 'boolean' }
_callable = { 'type': 'callable' }

def _map(x):
    return {
        'type': 'object',
        'patternProperties': { '.*': x },
    }

def _list(x):
    return {
        'type': 'array',
        'items': x,
    }

def _or(*x):
    return { 'anyOf': list(x) }

def _lit(x):
    return { 'const': x }

def _pair(x):
    return {
        'type': 'array',
        'items': x,
        'minItems': 2,
        'maxItems': 2,
    }

def _enum(*x):
    return { 'enum': list(x) }

def _obj(required=[], **kwargs):
    kwargs = {
        k.replace('_', '-'): v
        for k, v in kwargs.items()
    }
    if required is True:
        required = list(kwargs)
    return {
        'type': 'object',
        'required': required,
        'additionalProperties': False,
        'properties': kwargs,
    }


class CustomSchema:

    schema: Dict

    @classmethod
    def validate(cls, data: Any) -> bool:
        try:
            jsonschema.validate(data, cls.schema, cls=CustomValidator)
        except jsonschema.ValidationError:
            return False
        return True

    @classmethod
    def normalize(cls, data: Dict) -> Dict:
        return data


class FileMap(CustomSchema):

    schema = _or(
        _callable,
        _list(_or(
            _string,
            _obj(
                required = ['source'],
                source = _string,
                target = _string,
                mode = _enum('simple', 'glob'),
                template = _bool,
            ),
        )),
    )

    @classmethod
    def normalize(cls, data: Any) -> Any:
        if callable(data):
            return data
        return [{'source': v} if isinstance(v, str) else v for v in data]


_capture = _or(
    _string,
    _obj(
        required = ['pattern'],
        pattern = _string,
        mode = _enum('first', 'last', 'all'),
    ),
    _obj(
        required = ['type', 'name', 'prefix'],
        type = _enum('integer', 'float'),
        name = _string,
        prefix = _string,
        skip_words = _integer,
        flexible_prefix = _bool,
        mode = _enum('first', 'last', 'all'),
    )
)


_schema = {
    'type': 'object',
    'additionalProperties': False,
    'required': [],
    'properties': {
        'containers': _map(_or(_string, _list(_string))),
        'parameters': _map(_or(
            _list(_scalar),
            _list(_string),
            _obj(
                required = True,
                type = _lit('uniform'),
                interval = _pair(_scalar),
                num = _scalar,
            ),
            _obj(
                required = True,
                type = _lit('graded'),
                interval = _pair(_scalar),
                num = _scalar,
                grading = _scalar,
            ),
        )),
        'evaluate': _map(_string),
        'constants': _map(_or(_string, _null, _scalar, _bool)),
        'where': _or(_string, _list(_string)),
        'templates': FileMap.schema,
        'prefiles': FileMap.schema,
        'postfiles': FileMap.schema,
        'script': _list(_or(
            _string,
            _list(_string),
            _obj(
                command = _or(_string, _list(_string)),
                name = _string,
                capture = _or(_capture, _list(_capture)),
                capture_output = _bool,
                capture_walltime = _bool,
                retry_on_fail = _bool,
                env = _map(_string),
                container = _string,
                allow_failure = _bool,
            )
        )),
        'types': _map(_string),
        'plots': _list(_obj(
            required = ['filename', 'format', 'yaxis'],
            filename = _string,
            format = _or(_string, _list(_string)),
            parameters = _map(_or(
                _enum('fixed', 'variate', 'category', 'ignore', 'mean'),
                _obj(
                    required = ['mode', 'style'],
                    mode = _lit('category'),
                    style = _enum('color', 'line', 'marker'),
                ),
                _obj(
                    required = ['mode', 'value'],
                    mode = _lit('ignore'),
                    value = _or(_scalar, _string),
                ),
            )),
            yaxis = _or(_string, _list(_string)),
            xaxis = _string,
            ylim = _pair(_scalar),
            xlim = _pair(_scalar),
            type = _enum('scatter', 'line'),
            legend = _string,
            xlabel = _string,
            ylabel = _string,
            xmode = _enum('linear', 'log'),
            ymode = _enum('linear', 'log'),
            title = _string,
            grid = _bool,
            style = _obj(
                color = _or(_string, _list(_string)),
                line = _or(_string, _list(_string)),
                marker = _or(_string, _list(_string)),
            ),
        )),
        'settings': _obj(
            logdir = _or(_callable, _string),
            ignore_missing_files = _bool,
        ),
    },
}


def normalize(data: Dict):
    data = data.copy()
    for key, temp in [('templates', True), ('prefiles', False), ('postfiles', False)]:
        norm = FileMap.normalize(data.get(key, []))
        if not callable(norm):
            for spec in norm:
                spec.setdefault('template', temp)
        data[key] = norm
    if not callable(data['prefiles']) and data['templates']:
        data['prefiles'].extend(data.pop('templates'))
    elif data['templates']:
        raise ValueError("prefiles is function and templates is given")
    return data


def validate(data: Dict):
    jsonschema.validate(data, _schema, cls=CustomValidator)


class LibFinder(gold.LibFinder):

    def find(self, path: str):
        if path != 'grevling':
            return None
        return gold.evaluate_file(str(Path(__file__).parent / 'grevling.gold'))


def load(path: Path) -> Dict:
    if path.suffix == '.yaml':
        with open(path, 'r') as f:
            data = yaml.load(f, Loader=yaml.CLoader)
    else:
        ctx = gold.EvaluationContext()
        libfinder = LibFinder()
        ctx.append_libfinder(libfinder)
        data = gold.evaluate_file(ctx, str(path))
    validate(data)
    return normalize(data)
