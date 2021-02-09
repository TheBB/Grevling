from itertools import product

import numpy as np
import numpy.ma as ma
from typing_inspect import get_origin


def flatten(array):
    while array.dtype == object:
        array = np.array(array.tolist()).flatten()
    return array


def is_list_type(tp):
    return get_origin(tp) == list


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


def thick_index(index):
    return tuple(
        slice(i, i+1) if isinstance(i, int) else i
        for i in index
    )


def iter_axis(array, axis):
    indices = (slice(None),) * axis
    for i in range(array.shape[axis]):
        yield array[(*indices, slice(i, i+1))]


def flexible_mean(array, axis):
    d = np.sum(array, axis=axis, keepdims=True)
    if d.dtype == object:
        d = np.vectorize(lambda z: z/array.shape[axis], otypes=[object])(d)
    else:
        d = d / array.shape[axis]
    return d


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
