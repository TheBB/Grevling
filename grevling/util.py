import asyncio
from contextlib import contextmanager
from functools import wraps
import inspect
from itertools import product, chain
import json
import logging

from typing import List, Optional

from typing_inspect import get_origin, get_args

import numpy as np
from numpy.polynomial import Legendre
import pandas as pd
import rich.logging


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def find_context(self):
        frame = inspect.currentframe()
        while frame:
            bindings = frame.f_locals
            if '__grevling_log_context__' in bindings:
                return bindings['__grevling_log_context__']
            frame = frame.f_back
        return []

    def process(self, msg, kwargs):
        context = self.find_context()
        msg = ' · '.join(chain(context, [msg]))
        kwargs.setdefault('extra', {}).update({'markup': True})
        return msg, kwargs

    @contextmanager
    def with_context(self, ctx: str):
        context = self.find_context()
        frame = inspect.currentframe()
        frame = frame.f_back.f_back
        bindings = frame.f_locals

        bindings['__grevling_log_context__'] = [*context, ctx]
        try:
            yield
        finally:
            bindings['__grevling_log_context__'] = context


def with_context(fmt: str):
    def decorator(func: callable):
        signature = inspect.signature(func)

        def calculate_context(*args, **kwargs):
            binding = signature.bind(*args, **kwargs)
            binding.apply_defaults()
            return fmt.format(**binding.arguments)

        if asyncio.iscoroutinefunction(func):

            async def inner(*args, **kwargs):
                with log.with_context(calculate_context(*args, **kwargs)):
                    return await func(*args, **kwargs)

        else:

            def inner(*args, **kwargs):
                with log.with_context(calculate_context(*args, **kwargs)):
                    return func(*args, **kwargs)

        return wraps(func)(inner)

    return decorator


logging.basicConfig(level='INFO')
log: LoggerAdapter = LoggerAdapter(logging.getLogger(), {})


def initialize_logging(level='INFO', show_time=False):
    logging.basicConfig(
        level=level.upper(),
        format='%(message)s',
        datefmt='[%X]',
        handlers=[rich.logging.RichHandler(show_path=False, show_time=show_time)],
        force=True,
    )

    global log
    log = LoggerAdapter(logging.getLogger(), {})


def initialize_process():
    pass


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.ndarray) and obj.ndim == 1:
            return list(obj)
        if isinstance(obj, pd.Timestamp):
            return str(obj)
        return super().default(obj)


def prod(nums):
    retval = 1
    for n in nums:
        retval *= n
    return retval


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


def dict_product(names, iterables):
    for values in product(*iterables):
        yield dict(zip(names, values))


def subclasses(cls, root=False):
    if root:
        yield cls
    for sub in cls.__subclasses__():
        yield sub
        yield from subclasses(sub, root=False)


def find_subclass(cls, name, root=False, attr='__tag__', predicate=(lambda a, b: a == b)):
    for sub in subclasses(cls, root=root):
        if hasattr(sub, attr) and predicate(name, getattr(sub, attr)):
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


def format_seconds(secs: float):
    if secs < 0.1:
        return '< 0.1 s'
    if secs < 60:
        return f'{secs:.1f} s'
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f'{mins:.0f} m {secs:.0f}s'
    hours, mins = divmod(mins, 60)
    if hours < 24:
        return f'{hours:.0f} h {mins:.0f} m {secs:.0f} s'
    days, hours = divmod(hours, 24)
    return f'{days:.0f} d {hours:.0f} h {mins:.0f} m {secs:.0f} s'


def call_yaml(func, mapping, *args, **kwargs):
    signature = inspect.signature(func)
    mapping = {key.replace('-', '_'): value for key, value in mapping.items()}
    binding = signature.bind(*args, **kwargs, **mapping)
    return func(*binding.args, **binding.kwargs)


def to_queue(it):
    q = asyncio.Queue()
    for i in it:
        q.put_nowait(i)
    return q


def deprecated(info, name=None):
    def decorator(func):
        iname = name or func.__name__

        @wraps(func)
        def inner(*args, **kwargs):
            log.warn(f'{iname} is deprecated: {info}')
            return func(*args, **kwargs)

        return inner

    return decorator


def is_list_type(tp):
    return get_origin(tp) == list or get_origin(tp) == List


def unitvec(n: int, length: Optional[int] = None) -> np.ndarray:
    if length is None:
        length = n + 1
    retval = np.zeros((length,))
    retval[n] = 1
    return retval


def legendre(n: int, a: float, b: float, x: float):
    return Legendre(unitvec(n), [a, b])(x)
