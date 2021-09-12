from enum import Enum
from grevling import util
import json

from typing import Optional

from . import api, util
from .render import render


class Status(Enum):

    Initialized = 'initialized'
    Started = 'started'
    Finished = 'finished'


class Instance:

    logspace: api.Workspace
    bookspace: api.Workspace
    logdir: str
    status: Status
    types: api.Types

    _context: Optional[api.Context]

    @classmethod
    def from_context(cls, context: api.Context, logdir_pattern: str,
                     logspaces: api.WorkspaceCollection, types: api.Types,
                     index: Optional[int] = None) -> 'Instance':
        if index is not None:
            context['_index'] = index
        logdir = render(logdir_pattern, context)
        logspace = logspaces.open_workspace(logdir)
        return cls(logspace, types, context)

    def __init__(self, logspace: api.Workspace, types: api.Types, context: Optional[api.Context] = None):
        self.logspace = logspace
        self.bookspace = logspace.subspace('.grevling')
        self.logdir = logspace.top_name()
        self.types = types
        self._context = context

        if context is not None:
            context['_logdir'] = self.logdir

        if self.update_status() == Status.Initialized and context is not None:
            self.write_context()

    @property
    def context(self):
        if self._context is None:
            context = {}
            with self.bookspace.open_file('context.json', 'r') as f:
                for key, value in json.load(f).items():
                    context[key] = util.coerce(self.types[key], value)
            self._context = context
        return self._context

    def __getitem__(self, key):
        return self.context[key]

    def __setitem__(self, key, value):
        self.context[key] = util.coerce(self.types[key], value)

    @property
    def index(self):
        return self.context['_index']

    def write_context(self):
        with self.bookspace.open_file('context.json', 'w') as f:
            json.dump(self.context, f, sort_keys=True, indent=4, cls=util.JSONEncoder)

    def update_status(self):
        if self.bookspace.exists('started'):
            self.status = Status.Started
        elif self.bookspace.exists('finished'):
            self.status = Status.Finished
        else:
            self.status = Status.Initialized
        return self.status
