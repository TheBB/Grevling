from enum import Enum
from grevling import util
import json

from typing import Optional

from . import api, util
from .render import render


class Status(Enum):

    Created = 'created'
    Prepared = 'prepared'
    Started = 'started'
    Finished = 'finished'
    Downloaded = 'downloaded'


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
        return cls(logspace, types, context, status=Status.Created)

    def __init__(self, logspace: api.Workspace, types: api.Types, context: Optional[api.Context] = None,
                 status = None):
        self.logspace = logspace
        self.bookspace = logspace.subspace('.grevling')
        self.logdir = logspace.top_name()
        self.types = types
        self._context = context
        self.status = status

        if context is not None:
            context['_logdir'] = self.logdir

        if context is not None and status == Status.Created:
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

    def open_workspace(self, workspaces):
        return workspaces.open_workspace(self.logdir)
