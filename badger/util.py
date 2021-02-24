from itertools import product
import json

import numpy as np
import pandas as pd
from typing_inspect import get_origin


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.ndarray) and obj.ndim == 1:
            return list(obj)
        if isinstance(obj, pd.Timestamp):
            return str(obj)
        return super().default(obj)


def ignore(*_, **__):
    pass


def flexible_mean(obj):
    if obj.dtype == object:
        return list(obj.apply(np.array).mean())
    return obj.mean()


def flatten(array):
    if array.dtype == object:
        array = np.array(array.tolist()).flatten()
    return array


def is_list_type(tp):
    return get_origin(tp) == list


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


def completer(options):
    matches = []
    def complete(text, state):
        if state == 0:
            matches.clear()
            matches.extend(c for c in options if c.startswith(text.lower()))
        return matches[state] if state < len(matches) else None
    return complete
