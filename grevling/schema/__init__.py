"""Module for validating input from config files

Grevling is configured using a single config file, in either YAML (now
considered legacy) or Gold. In either case, the input is validated according to
the models in the `raw` submodule.

Many parts of this input are things that must be evaluated at a later time:
either because they are literally functions (which is possible in Gold) or
because they are templates or other code-as-text that must be evaluated when an
instance context is instantiated.

In addition to this, many settings have somewhat relaxed types for reasons of
convenience. For example, captures in a command may be either a mapping, a
string (pure regular expression), or a list of such.

To resolve this jungle of possibilities, the schemas in the `raw` module are
*refined*: converted to a lowest-common-denominator model which is as rigid as
possible. In the cases of delayed evaluables, they are converted into functions
which return validated models when called with a context as argument.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import goldpy as gold  # type: ignore
import yaml

from .. import util
from . import raw, refined
from .refined import *  # noqa: F403


def libfinder(path: str) -> Optional[dict[str, Any]]:
    """This function is called when a Gold script imports a module which
    Gold doesn't know about. We provide this to allow user scripts to import
    the 'grevling.gold' helper file.
    """
    if path != "grevling":
        return None
    retval = gold.eval_file(str(Path(__file__).parent.parent / "grevling.gold"))

    # Additional utility functions implemented in Python
    return {
        **retval,
        "legendre": util.legendre,
    }


def load(path: Path) -> refined.CaseSchema:
    """Load a Grevling configuration file and return a refined schema."""

    # We recommend new cases are written in Gold, thus we require that YAML
    # files are explicitly named as such
    if path.suffix.lower() in (".yaml", ".yml"):
        with open(path, "r") as f:
            data = yaml.load(f, Loader=yaml.CLoader)
    else:
        with open(path, "r") as f:
            src = f.read()
        resolver = gold.ImportConfig(root=str(path.parent), custom=libfinder)
        data = gold.eval(src, resolver)

    # return raw.CaseSchema.model_validate(data).refine()
    obj = raw.CaseSchema.model_validate(data)
    return obj.refine()
