from contextlib import contextmanager
from functools import wraps
import inspect
from itertools import product, chain
import json
import logging
import multiprocessing

from typing import List

import multiprocessing_logging
import numpy as np
import pandas as pd
import rich.logging
from typing_inspect import get_origin


class LoggerAdapter(logging.LoggerAdapter):

    __context: List[str]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__context = []

    def process(self, msg, kwargs):
        context = []
        procname = multiprocessing.current_process().name
        if procname != 'M':
            context.append(f'[bold magenta]{procname}[/]')
        context.extend(self.__context)
        context.append(msg)
        msg = ' · '.join(context)
        kwargs.setdefault('extra', {}).update({
            'procname': multiprocessing.current_process().name,
            'markup': True
        })
        return msg, kwargs

    def push_context(self, ctx: str):
        self.__context.append(ctx)

    def pop_context(self):
        self.__context.pop()

    @contextmanager
    def with_context(self, ctx: str):
        self.push_context(ctx)
        try:
            yield
        finally:
            self.pop_context()


def with_context(fmt: str):
    def decorator(func: callable):
        signature = inspect.signature(func)
        @wraps(func)
        def inner(*args, **kwargs):
            binding = signature.bind(*args, **kwargs)
            binding.apply_defaults()
            context = fmt.format(**binding.arguments)
            with log.with_context(context):
                return func(*args, **kwargs)
        return inner
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
    multiprocessing_logging.install_mp_handler()


def initialize_process():
    proc = multiprocessing.current_process()
    proc.name = 'W' + ':'.join(map(str, proc._identity))


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


def is_list_type(tp):
    return get_origin(tp) == list or get_origin(tp) == List


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


def format_seconds(secs: float):
    if secs < 0.1:
        return '<0.1s'
    if secs < 60:
        return f'{secs:.1f}s'
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f'{mins:.0f}m{secs:.0f}s'
    hours, mins = divmod(mins, 60)
    if hours < 24:
        return f'{hours:.0f}h{mins:.0f}m{secs:.0f}s'
    days, hours = divmod(hours, 24)
    return f'{days:.0f}d{hours:.0f}h{mins:.0f}m{secs:.0f}s'
