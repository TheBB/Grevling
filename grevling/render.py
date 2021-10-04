import shlex

from typing import Optional

from mako.template import Template

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


def render(text: str, context: api.Context, mode: Optional[str] = None) -> str:
    filters = ['str']
    imports = [
        'from numpy import sin, cos',
    ]
    if mode is not None:
        filters.append(f'quote_{mode}')
        imports.append(f'from grevling.render import quote_{mode}')

    template = Template(text, default_filters=filters, imports=imports)
    return template.render(**context, rnd=rnd, sci=sci)
