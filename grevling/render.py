from copy import deepcopy
import shlex

from typing import Any, Dict, Optional, Callable, Tuple, Type, TypeVar, Union, List

from jsonpath_ng import parse, JSONPath     # type: ignore
from mako.template import Template          # type: ignore

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


T = TypeVar(
    'T',
    str,
    List[str],
    Dict[str, str],
    None,

    Union[str, List[str]],
    Union[str, List[str], None],
)


def render(template: T, context: api.Context, mode: Optional[str] = None) -> T:
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


def render_str(template: str, context: api.Context, mode: Optional[str] = None) -> str:
    filters = ['str']
    imports = [
        'from numpy import sin, cos',
    ]
    if mode is not None:
        filters.append(f'quote_{mode}')
        imports.append(f'from grevling.render import quote_{mode}')

    mako = Template(template, default_filters=filters, imports=imports)
    return mako.render(**context, rnd=rnd, sci=sci)
