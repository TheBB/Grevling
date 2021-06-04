import shlex

from mako.template import Template


def quote_shell(text):
    return shlex.quote(text)

def rnd(number, ndigits):
    return f'{number:.{ndigits}f}'

def sci(number, ndigits):
    return f'{number:.{ndigits}e}'


QUOTERS = {
    'shell': quote_shell,
}


def render(text, context, mode=None):
    filters = ['str']
    imports = [
        'from numpy import sin, cos',
    ]
    if mode is not None:
        filters.append(f'quote_{mode}')
        imports.append(f'from grevling.render import quote_{mode}')

    template = Template(text, default_filters=filters, imports=imports)
    return template.render(**context, rnd=rnd, sci=sci)
