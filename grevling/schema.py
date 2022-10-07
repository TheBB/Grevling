from pathlib import Path

from typing import Any, List, Dict

import goldpy as gold                   # type: ignore
import jsonschema                       # type: ignore
import jsonschema.validators            # type: ignore
import yaml


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
    def normalize(cls, data: Any) -> Any:
        return data


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


class Script(CustomSchema):

    schema = _or(
        _callable,
        _list(_or(
            _string,
            _list(_string),
            _obj(
                command = _or(_string, _list(_string)),
                name = _or(_null, _string),
                capture = _or(_capture, _list(_capture)),
                capture_output = _bool,
                capture_walltime = _bool,
                retry_on_fail = _bool,
                env = _map(_string),
                container = _or(_null, _string),
                container_args = _or(_string, _list(_string)),
                allow_failure = _bool,
                workdir = _or(_null, _string),
            )
        ))
    )

    @classmethod
    def normalize(cls, data: Any) -> List:
        result = []
        for script in data:
            if isinstance(script, (list, str)):
                result.append({'command': script})
            else:
                result.append(script)
        for script in result:
            if not isinstance(script.get('capture', []), list):
                script['capture'] = [script['capture']]
        return result


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
        'evaluate': _or(_callable, _map(_string)),
        'constants': _map(_or(_string, _null, _scalar, _bool)),
        'where': _or(_callable, _string, _list(_string)),
        'templates': FileMap.schema,
        'prefiles': FileMap.schema,
        'postfiles': FileMap.schema,
        'script': Script.schema,
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
    if isinstance(data.get('where', []), str):
        data['where'] = [data['where']]
    if not callable(data['prefiles']) and data['templates']:
        data['prefiles'].extend(data.pop('templates'))
    elif data['templates']:
        raise ValueError("prefiles is function and templates is given")
    if not callable(data.get('script', [])):
        data['script'] = Script.normalize(data.get('script', []))
        for script in data['script']:
            script.setdefault('container-args', data.get('container-args', {}).get(script.get('container', None), []))
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
