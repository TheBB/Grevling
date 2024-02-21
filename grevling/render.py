from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, Optional, TypeVar, Union

from mako.template import Template

if TYPE_CHECKING:
    from . import api


def quote_shell(text):
    return shlex.quote(text)


def rnd(number, ndigits):
    return f"{number:.{ndigits}f}"


def sci(number, ndigits):
    return f"{number:.{ndigits}e}"


QUOTERS = {
    "shell": quote_shell,
}


T = TypeVar(
    "T",
    str,
    list[str],
    dict[str, str],
    None,
    Union[str, list[str]],
    Union[str, list[str], None],
)


def render(template: T, context: api.Context, mode: Optional[str] = None) -> T:
    if isinstance(template, str):
        return render_str(template, context, mode)
    if isinstance(template, list):
        return [render_str(arg, context, mode) for arg in template]
    if isinstance(template, dict):
        return {k: render_str(val, context, mode) for k, val in template.items()}
    return None


def render_str(template: str, context: api.Context, mode: Optional[str] = None) -> str:
    filters = ["str"]
    imports = [
        "from numpy import sin, cos",
    ]
    if mode is not None:
        filters.append(f"quote_{mode}")
        imports.append(f"from grevling.render import quote_{mode}")

    mako = Template(template, default_filters=filters, imports=imports)
    return mako.render(**context, rnd=rnd, sci=sci)
