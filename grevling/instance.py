from contextlib import contextmanager
from enum import Enum
from grevling import util
import json

from typing import Optional

from . import api, util
from .capture import ResultCollector


class Status(Enum):

    Created = 'created'
    Prepared = 'prepared'
    Started = 'started'
    Finished = 'finished'
    Downloaded = 'downloaded'


class Instance:

    local: api.Workspace
    local_book: api.Workspace

    remote: Optional[api.Workspace]
    remote_book: Optional[api.Workspace]

    logdir: str
    status: Status

    _context: Optional[api.Context]
    _status: Optional[Status]

    @classmethod
    def create(cls, case, context: api.Context) -> 'Instance':
        obj = cls(case, context=context)
        obj.status = Status.Created
        obj.write_context()
        return obj

    def __init__(self, case, context: api.Context = None, logdir = None):
        self._case = case
        self._context = context

        if context:
            self.logdir = context['_logdir']
        else:
            self.logdir = logdir

        self.local = self.open_workspace(case.storage_spaces)
        self.local_book = self.local.subspace('.grevling')
        self.remote = self.remote_book = None
        self._status = None

    @property
    def status(self):
        if not self._status:
            with self.local_book.open_file('status.txt', 'r') as f:
                status = f.read()
            self._status = Status(status)
        return self._status

    @status.setter
    def status(self, value):
        with self.local_book.open_file('status.txt', 'w') as f:
            f.write(value.value)
        self._status = value

    @property
    def context(self):
        if self._context is None:
            with self.local_book.open_file('context.json', 'r') as f:
                self._context = json.load(f)
        return self._context

    @property
    def types(self):
        return self._case.types

    def __getitem__(self, key):
        return self.context[key]

    def __setitem__(self, key, value):
        self.context[key] = util.coerce(self.types[key], value)

    @contextmanager
    def bind_remote(self, spaces: api.WorkspaceCollection):
        self.remote = self.open_workspace(spaces, 'WRK')
        self.remote_book = self.remote.subspace('.grevling')
        try:
            yield
        finally:
            self.remote = self.remote_book = None

    @property
    def index(self):
        return self.context['_index']

    @property
    def script(self):
        return self._case.script.render(self.context)

    def write_context(self):
        with self.local_book.open_file('context.json', 'w') as f:
            json.dump(self.context, f, sort_keys=True, indent=4, cls=util.JSONEncoder)

    def open_workspace(self, workspaces, name=''):
        return workspaces.open_workspace(self.logdir, name)

    def prepare(self):
        assert self.remote
        assert self.status == Status.Created

        src = self._case.local_space
        util.log.debug(f"Using SRC='{src}', WRK='{self.remote}'")
        self._case.premap.copy(self.context, src, self.remote, ignore_missing=self._case._ignore_missing)

        self.status = Status.Prepared

    def download(self):
        assert self.remote
        assert self.remote_book
        assert self.status == Status.Finished

        collector = ResultCollector(self.types)
        collector.collect_from_dict(self.context)

        self.remote_book.copy_all_to(self.local_book)
        collector.collect_from_info(self.local_book)

        ignore_missing = self._case._ignore_missing or not collector['_success']
        self._case.postmap.copy(self.context, self.remote, self.local, ignore_missing=ignore_missing)

        self._case.script.capture(collector, self.local_book)
        collector.commit_to_file(self.local_book)

        self.status = Status.Downloaded

    def capture(self):
        assert self.status == Status.Downloaded

        collector = ResultCollector(self.types)
        collector.collect_from_dict(self.context)
        collector.collect_from_info(self.local_book)
        self._case.script.capture(collector, self.local_book)
        collector.commit_to_file(self.local_book)

    def cached_capture(self):
        collector = ResultCollector(self.types)
        collector.collect_from_cache(self.local_book)
        return collector
