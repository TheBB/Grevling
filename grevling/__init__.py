from __future__ import annotations

from contextlib import contextmanager
from difflib import Differ
import json
import os
from pathlib import Path
import pydoc
import shutil

from typing import List, Iterable, Optional, Any, Dict

from fasteners import InterProcessLock
import pandas as pd

from grevling.typing import TypeManager

from .api import Status
from .plotting import Plot
from .render import render
from .schema import load
from .capture import CaptureCollection
from .context import ContextProvider
from .parameters import ParameterSpace
from .filemap import FileMap
from .script import Script, ScriptTemplate
from .typing import PersistentObject
from .workflow.local import LocalWorkspaceCollection, LocalWorkspace, LocalWorkflow
from . import util, api


__version__ = '2.0.0'


class CaseState(PersistentObject):

    running: bool = False  # True if instances are currently running
    has_data: bool = False  # True if any instances have been run and downloaded
    has_captured: bool = False  # True if all finished instances have had data captured
    has_collected: bool = False  # True if data from all finished instances have been collected
    has_plotted: bool = False  # True if all plots have been generated from finished instances


class Case:

    lock: Optional[InterProcessLock]
    state: CaseState

    # Configs may be provided in pure data, in which case they don't correspond to a file
    configpath: Optional[Path]

    # Raw structured data used to initialize this case
    casedata: Dict

    sourcepath: Path
    storagepath: Path
    dataframepath: Path

    context_mgr: ContextProvider

    premap: FileMap
    postmap: FileMap
    script: ScriptTemplate
    _plots: List[Plot]

    _ignore_missing: bool

    def __init__(
        self,
        localpath: api.PathStr = '.',
        storagepath: Optional[Path] = None,
        casedata: Optional[dict] = None,
    ):
        configpath: Optional[Path] = None

        if isinstance(localpath, str):
            localpath = Path(localpath)
        if localpath.is_file():
            configpath = localpath
            localpath = configpath.parent
        elif localpath.is_dir() and casedata is None:
            for candidate in ['grevling.gold', 'grevling.yaml', 'badger.yaml']:
                if (localpath / candidate).exists():
                    configpath = localpath / candidate
                    break
        self.configpath = configpath

        self.sourcepath = localpath
        self.local_space = LocalWorkspace(self.sourcepath, 'SRC')

        if storagepath is None:
            storagepath = self.sourcepath / '.grevlingdata'
        storagepath.mkdir(parents=True, exist_ok=True)
        self.storagepath = storagepath
        self.storage_spaces = LocalWorkspaceCollection(self.storagepath)

        self.dataframepath = storagepath / 'dataframe.parquet'

        if casedata is None:
            if configpath is None:
                raise ValueError("Could not find a valid grevling configuration")
            if configpath is not None and not configpath.is_file():
                raise FileNotFoundError("Found a grevling configuration, but it's not a file")
            casedata = load(configpath)

        self.casedata = casedata
        self.context_mgr = ContextProvider.load(casedata)

        # Read file mappings
        self.premap = FileMap.load(
            casedata.get('prefiles', []), casedata.get('templates', [])
        )
        self.postmap = FileMap.load(casedata.get('postfiles', []))

        # Read commands
        self.script = ScriptTemplate.load(
            casedata.get('script', []), casedata.get('containers', {})
        )

        # Fill in types derived from commands
        self.script.add_types(self.types)

        # Read settings
        settings = casedata.get('settings', {})
        self._logdir = settings.get('logdir', '${g_index}')
        self._ignore_missing = settings.get('ignore-missing-files', False)

        # Construct plot objects
        self._plots = [
            Plot.load(spec, self.parameters, self.types)
            for spec in casedata.get('plots', [])
        ]

        self.lock = None

    def acquire_lock(self):
        assert not self.lock
        self.lock = InterProcessLock(self.storagepath / 'lockfile').__enter__()

    def release_lock(self, *args, **kwargs):
        assert self.lock
        self.lock.__exit__(*args, **kwargs)
        self.lock = None

    def __enter__(self) -> Case:
        self.acquire_lock()
        self.state = CaseState(self.storagepath / 'state.json').__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.state.__exit__(*args, **kwargs)
        self.release_lock(*args, **kwargs)

    @property
    def parameters(self) -> ParameterSpace:
        return self.context_mgr.parameters

    @property
    def types(self) -> api.Types:
        return self.context_mgr.types

    def has_data(self) -> bool:
        return self.state.has_data

    def clear_cache(self):
        self.__exit__(None, None, None)
        shutil.rmtree(self.storagepath)
        self.storagepath.mkdir(parents=True, exist_ok=True)
        self.__enter__()

    def clear_dataframe(self):
        # TODO: Py3.8: use missing_ok=True
        try:
            self.dataframepath.unlink()
        except FileNotFoundError:
            pass
        self.state.has_collected = False

    def load_dataframe(self) -> pd.DataFram:
        if self.state.has_collected:
            return pd.read_parquet(self.dataframepath, engine='pyarrow')
        data = {
            k: pd.Series([], dtype=v)
            for k, v in self.types.pandas().items()
            if k != 'g_index'
        }
        return pd.DataFrame(index=pd.Int64Index([]), data=data)

    def save_dataframe(self, df: pd.DataFrame):
        df.to_parquet(self.dataframepath, engine='pyarrow', index=True)

    def create_instances(self) -> Iterable[Instance]:
        for i, ctx in enumerate(self.context_mgr.fullspace()):
            ctx.g_index = i
            ctx.g_logdir = render(self._logdir, ctx)
            yield Instance.create(self, ctx)

    def create_instance(
        self,
        ctx: api.Context,
        logdir: Optional[Path] = None,
        index: Optional[int] = None,
    ) -> Instance:
        ctx = self.context_mgr.evaluate_context(ctx)
        if index is None:
            index = 0
        ctx.g_index = index
        if logdir is None:
            logdir = render(self._logdir, ctx)
        ctx.g_logdir = str(logdir)
        workspace = LocalWorkspace(Path(ctx.g_logdir), name='LOG')
        return Instance.create(self, ctx, local=workspace)

    def instances(self, *statuses: api.Status) -> Iterable[Instance]:
        for name in self.storage_spaces.workspace_names():
            if not self.storage_spaces.open_workspace(name).exists(
                '.grevling/status.txt'
            ):
                continue
            instance = Instance(self, logdir=name)
            if statuses and instance.status not in statuses:
                continue
            yield instance

    def capture(self):
        for instance in self.instances(Status.Downloaded):
            instance.capture()
        self.state.has_captured = True

    def collect(self):
        data = self.load_dataframe()
        for instance in self.instances(Status.Downloaded):
            collector = instance.cached_capture()
            data = collector.commit_to_dataframe(data)
        data = data.sort_index()
        self.save_dataframe(data)
        self.state.has_collected = True

    def plot(self):
        for plot in self._plots:
            plot.generate_all(self)
        self.state.has_plotted = True

    def run(self, nprocs=1) -> bool:
        nprocs = nprocs or 1
        with LocalWorkflow(nprocs=nprocs) as workflow:
            return workflow.pipeline(self).run(self.create_instances())

    def run_single(self, namespace: api.Context, logdir: Path, index: int = 0):
        instance = self.create_instance(namespace, logdir=logdir, index=index)
        with LocalWorkflow() as workflow:
            workflow.pipeline(self).run([instance])

    # Deprecated methods

    @util.deprecated("use Case.instances() instead", name='Case.iter_instancedirs')
    def iter_instancedirs(self) -> Iterable[api.Workspace]:
        for path in self.storagepath.iterdir():
            if not (path / '.grevling' / 'context.json').exists():
                continue
            yield LocalWorkspace(path)

    @property
    @util.deprecated("will be removed", name='Case.shape')
    def shape(self):
        return tuple(map(len, self._parameters.values()))


class Instance:

    local: api.Workspace
    local_book: api.Workspace

    remote: Optional[api.Workspace]
    remote_book: Optional[api.Workspace]

    logdir: str
    status: Status

    _case: Case
    _context: Optional[api.Context]
    _status: Optional[Status]

    @classmethod
    def create(cls, case: Case, context: api.Context, local=None) -> Instance:
        obj = cls(case, context=context, local=local)
        obj.status = Status.Created
        obj.write_context()
        return obj

    def __init__(
        self,
        case: Case,
        context: api.Context = None,
        logdir: Optional[str] = None,
        local: Optional[api.Workspace] = None,
    ):
        self._case = case
        self._context = context

        if context:
            self.logdir = context.g_logdir
        else:
            self.logdir = logdir

        if local is None:
            self.local = self.open_workspace(case.storage_spaces)
        else:
            self.local = local

        self.local_book = self.local.subspace('.grevling')
        self.remote = self.remote_book = None
        self._status = None

    @property
    def status(self) -> api.Status:
        if not self._status:
            with self.local_book.open_file('status.txt', 'r') as f:
                status = f.read()
            self._status = Status(status)
        return self._status

    @status.setter
    def status(self, value: api.Status):
        with self.local_book.open_file('status.txt', 'w') as f:
            f.write(value.value)
        self._status = value

    @property
    def context(self) -> api.Context:
        if self._context is None:
            with self.local_book.open_file('context.json', 'r') as f:
                self._context = json.load(f)
        return self._context

    @property
    def types(self) -> TypeManager:
        return self._case.types

    def __getitem__(self, key: str) -> Any:
        return self.context[key]

    def __setitem__(self, key: str, value: Any) -> Any:
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
    def index(self) -> int:
        return self.context.g_index

    @property
    def script(self) -> Script:
        return self._case.script.render(self.context)

    def write_context(self):
        with self.local_book.open_file('context.json', 'w') as f:
            f.write(self.context.json(sort_keys=True, indent=4))

    def open_workspace(self, workspaces, name='') -> api.Workspace:
        return workspaces.open_workspace(self.logdir, name)

    def prepare(self):
        assert self.remote
        assert self.status == Status.Created

        src = self._case.local_space
        util.log.debug(f"Using SRC='{src}', WRK='{self.remote}'")
        self._case.premap.copy(
            self.context, src, self.remote, ignore_missing=self._case._ignore_missing
        )

        self.status = Status.Prepared
        self._case.state.running = True

    def download(self):
        assert self.remote
        assert self.remote_book
        assert self.status == Status.Finished

        collector = self.types.capture_model()
        collector.update(self.context)

        bookmap = FileMap.load(
            files=[{'source': '*', 'mode': 'glob'}],
        )
        bookmap.copy(self.context, self.remote_book, self.local_book)
        collector.collect_from_info(self.local_book)

        ignore_missing = self._case._ignore_missing or not collector['g_success']
        self._case.postmap.copy(
            self.context, self.remote, self.local, ignore_missing=ignore_missing
        )

        self._case.script.capture(collector, self.local_book)
        collector = collector.validate()
        collector.commit_to_file(self.local_book)

        self.status = Status.Downloaded
        self._case.state.has_data = True
        self._case.state.has_captured = False
        self._case.state.has_collected = False
        self._case.state.has_plotted = False

    def capture(self):
        assert self.status == Status.Downloaded
        collector = self.types.capture_model()
        collector.update(self.context)
        collector.collect_from_info(self.local_book)
        self._case.script.capture(collector, self.local_book)
        collector.commit_to_file(self.local_book)

    def cached_capture(self) -> CaptureCollection:
        collector = self.types.capture_model()
        collector.collect_from_cache(self.local_book)
        return collector
