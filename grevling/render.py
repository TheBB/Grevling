from copy import deepcopy
import shlex

from typing import Any, Dict, Optional, Callable, Tuple, Type, TypeVar, Union, List

from jsonpath_ng import parse, JSONPath     # type: ignore
from mako.template import Template          # type: ignore

from . import api


T = TypeVar('T')


def quote_shell(text):
    return shlex.quote(text)


def rnd(number, ndigits):
    return f'{number:.{ndigits}f}'


def sci(number, ndigits):
    return f'{number:.{ndigits}e}'


QUOTERS = {
    'shell': quote_shell,
}


Renderable = TypeVar('Renderable', str, List[str], Dict[Any, str], None)


def render(template: Renderable, context: api.Context, mode: Optional[str] = None) -> Renderable:
    if isinstance(template, str):
        return render_str(template, context, mode)
    if isinstance(template, list):
        return [
            render_str(arg, context, mode)
            for arg in template
        ]
    if isinstance(template, dict):
        return {
            k: render_str(val, context, mode)
            for k, val in template.items()
        }
    return None


def render_str(template: str, context: api.Context, mode: Optional[str] = None):
    return StringRenderable(template, mode).render(context)


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


# class JsonRenderable(api.Renderable[T]):

#     data: Dict
#     path: Tuple[str]
#     constructor: Callable[[Any], T]
#     mode: Optional[str]

#     def __init__(self, data: Dict, constructor: Callable[[Any], T], *paths: str, mode: Optional[str] = None):
#         self.paths = paths
#         self.data = data
#         self.constructor = constructor          # type: ignore
#         self.mode = mode

#     def render(self, context: api.Context) -> T:
#         def updater(value, container, key):
#             if isinstance(container, str):
#                 return value
#             if not isinstance(value, str):
#                 return value
#             renderer = StringRenderable(value, mode=self.mode)
#             container[key] = renderer.render(context)
#         data = deepcopy(self.data)
#         for path in self.paths:
#             jpath: JSONPath = parse(path)
#             data = jpath.update(data, updater)
#         return self.constructor(data)           # type: ignore


# class CallableRenderable(api.Renderable[T]):

#     func: Callable
#     validator: Callable
#     constructor: Callable[[Any], T]

#     def __init__(self, func: Callable, constructor: Callable[[Any], T], validator: Callable):
#         self.func = func                    # type: ignore
#         self.constructor = constructor      # type: ignore
#         self.validator = validator          # type: ignore

#     def render(self, context: api.Context) -> T:
#         data = context(self.func)
#         if not self.validator(data):
#             raise ValueError("function validation failed")
#         return self.constructor(data)       # type: ignore


# def renderable(
#     obj: Any,
#     constructor: Callable[[Any], T],
#     validator: Callable,
#     *paths: str,
#     mode: Optional[str] = None
# ) -> api.Renderable[T]:
#     if callable(obj):
#         return CallableRenderable(obj, constructor, validator)
#     return JsonRenderable(obj, constructor, *paths, mode=mode)


# def render(text: Union[Callable, str], context: api.Context, mode: Optional[str] = None) -> str:
#     if callable(text):
#         return context(text)

#     filters = ['str']
#     imports = [
#         'from numpy import sin, cos',
#     ]
#     if mode is not None:
#         filters.append(f'quote_{mode}')
#         imports.append(f'from grevling.render import quote_{mode}')

#     template = Template(text, default_filters=filters, imports=imports)
#     return template.render(**context, rnd=rnd, sci=sci)
