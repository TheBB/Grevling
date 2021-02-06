from itertools import product

import numpy as np
import numpy.ma as ma
from typing_inspect import get_origin


def struct_as_dict(array: np.void, types: 'NestedDict') -> dict:
    retval = {}
    for k in array.dtype.fields.keys():
        if isinstance(array[k], ma.core.MaskedConstant):
            return retval
        if isinstance(array[k], (ma.mvoid, np.void)):
            subdata = struct_as_dict(array[k], types[k])
            retval[k] = subdata
        else:
            if get_origin(types[k]) == list:
                retval[k] = array[k]
            else:
                retval[k] = types[k](array[k])
    return retval


def dict_product(names, iterables):
    for values in product(*iterables):
        yield dict(zip(names, values))


def subclasses(cls, root=False):
    if root:
        yield cls
    for sub in cls.__subclasses__():
        yield sub
        yield from subclasses(sub, root=False)


def find_subclass(cls, name, root=False, attr='__tag__'):
    for sub in subclasses(cls, root=root):
        if hasattr(sub, attr) and getattr(sub, attr) == name:
            return sub
    return None


def subindex_set(target, key, value):
    *path, last = key.split('/')
    for p in path:
        target = target[p]
    target[last] = value


def has_data(array: ma.MaskedArray) -> bool:
    if array.dtype.fields is None:
        return array.count() > 0
    for k in array.dtype.fields.keys():
        if has_data(array[k]):
            return True
    return False


def completer(options):
    matches = []
    def complete(text, state):
        if state == 0:
            matches.clear()
            matches.extend(c for c in options if c.startswith(text.lower()))
        return matches[state] if state < len(matches) else None
    return complete
