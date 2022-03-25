from copy import deepcopy
from email.policy import default
import shlex

from typing import Any, Dict, Optional, Callable, Tuple, Type, TypeVar, Union

from jsonpath_ng import parse
from mako.template import Template
from pydantic import constr

from . import api


def quote_shell(text):
    return shlex.quote(text)


def rnd(number, ndigits):
    return f'{number:.{ndigits}f}'


def sci(number, ndigits):
    return f'{number:.{ndigits}e}'


QUOTERS = {
    'shell': quote_shell,
}


class StringRenderable(api.Renderable[str]):

    template: str
    mode: Optional[str]

    def __init__(self, template: str, mode: Optional[str] = None):
        self.template = template
        self.mode = mode

    def render(self, context: api.Context) -> str:
        filters = ['str']
        imports = [
            'from numpy import sin, cos',
        ]
        if self.mode is not None:
            filters.append(f'quote_{self.mode}')
            imports.append(f'from grevling.render import quote_{self.mode}')

        template = Template(self.template, default_filters=filters, imports=imports)
        return template.render(**context, rnd=rnd, sci=sci)


T = TypeVar('T')

class JsonRenderable(api.Renderable[T]):

    data: Dict
    path: Tuple[str]
    constructor: Type[T]
    mode: Optional[str]

    def __init__(self, data: Dict, constructor: Type[T], *paths: str, mode: Optional[str] = None):
        self.paths = paths
        self.data = data
        self.constructor = constructor
        self.mode = mode

    def render(self, context: api.Context) -> T:
        def updater(value, container, key):
            if not isinstance(value, str):
                return value
            renderer = StringRenderable(value, mode=self.mode)
            container[key] = renderer.render(context)
        data = deepcopy(self.data)
        for path in self.paths:
            path = parse(path)
            data = path.update(data, updater)
        return self.constructor.load(data)


T = TypeVar('T')

class CallableRenderable(api.Renderable[T]):

    func: Callable
    validator: Callable
    constructor: Type[T]

    def __init__(self, func: Callable, constructor: Type[T], validator: Callable):
        self.func = func
        self.constructor = constructor
        self.validator = validator

    def render(self, context: api.Context) -> T:
        data = context(self.func)
        if not self.validator(data):
            raise ValueError("function validation failed")
        return self.constructor.load(data)


T = TypeVar('T')

def renderable(
    obj: Any,
    constructor: Type[T],
    validator: Callable,
    *paths: str,
    mode: Optional[str] = None
) -> api.Renderable[T]:
    if callable(obj):
        return CallableRenderable(obj, constructor, validator)
    return JsonRenderable(obj, constructor, *paths, mode=mode)


def render(text: Union[Callable, str], context: api.Context, mode: Optional[str] = None) -> str:
    if callable(text):
        return context(text)

    filters = ['str']
    imports = [
        'from numpy import sin, cos',
    ]
    if mode is not None:
        filters.append(f'quote_{mode}')
        imports.append(f'from grevling.render import quote_{mode}')

    template = Template(text, default_filters=filters, imports=imports)
    return template.render(**context, rnd=rnd, sci=sci)
