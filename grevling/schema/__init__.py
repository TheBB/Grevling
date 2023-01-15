from pathlib import Path

import goldpy as gold                   # type: ignore
import yaml

from .. import util
from . import raw, refined

from .refined import *


def libfinder(path: str):
    if path != 'grevling':
        return None
    retval = gold.eval_file(str(Path(__file__).parent.parent / 'grevling.gold'))
    retval.update({
        'legendre': util.legendre
    })
    return retval


def load(path: Path) -> refined.CaseSchema:
    if path.suffix == '.yaml':
        with open(path, 'r') as f:
            data = yaml.load(f, Loader=yaml.CLoader)
    else:
        with open(path, 'r') as f:
            src = f.read()
        resolver = gold.ImportConfig(root=str(path.parent), custom=libfinder)
        data = gold.eval(src, resolver)
    return raw.CaseSchema.parse_obj(data).refine()
