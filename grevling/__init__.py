from contextlib import contextmanager
from difflib import Differ
from enum import Enum
import json
import os
from pathlib import Path
import pydoc
import shutil

from typing import List, Iterable, Optional

from fasteners import InterProcessLock
import pandas as pd
from typing_inspect import get_origin, get_args

from .api import Status
from .capture import ResultCollector
from .plotting import Plot
from .render import render
from .schema import load_and_validate
from .context import ContextManager
from .filemap import FileMap
from .script import ScriptTemplate
from .workflow.local import LocalWorkspaceCollection, LocalWorkspace, LocalWorkflow
from . import util, api


__version__ = '1.2.1'


def _pandas_dtype(tp):
    if tp == int:
        return pd.Int64Dtype()
    if tp == bool:
        return pd.BooleanDtype()
    if util.is_list_type(tp):
        return object
    return tp


def _typename(tp) -> str:
    try:
        return {int: 'integer', str: 'string', float: 'float', 'datetime64[ns]': 'datetime'}[tp]
    except KeyError:
        base = {list: 'list'}[get_origin(tp)]
        subs = ', '.join(_typename(k) for k in get_args(tp))
        return f'{base}[{subs}]'


class Case:

    yamlpath: Path
    sourcepath: Path
    storagepath: Path
    dataframepath: Path

    context_mgr: ContextManager

    premap: FileMap
    postmap: FileMap
    script: ScriptTemplate
    _plots: List[Plot]

    _ignore_missing: bool

    def __init__(self, yamlpath='.', storagepath=None, yamldata=None):
        if isinstance(yamlpath, str):
            yamlpath = Path(yamlpath)
        if yamlpath.is_dir():
            for candidate in ['grevling', 'badger']:
                if (yamlpath / f'{candidate}.yaml').exists():
                    yamlpath = yamlpath / f'{candidate}.yaml'
                    break
        assert yamlpath.is_file()
        self.yamlpath = yamlpath
        self.sourcepath = yamlpath.parent
        self.local_space = LocalWorkspace(self.sourcepath, 'SRC')

        if storagepath is None:
            storagepath = self.sourcepath / '.grevlingdata'
        storagepath.mkdir(parents=True, exist_ok=True)
        self.storagepath = storagepath
        self.storage_spaces = LocalWorkspaceCollection(self.storagepath)

        self.dataframepath = storagepath / 'dataframe.parquet'

        with open(yamlpath, mode='r') as f:
            yamldata = f.read()
        with open(yamlpath, mode='r') as f:
            casedata = load_and_validate(yamldata, yamlpath)

        self.context_mgr = ContextManager.load(casedata)

        # Read file mappings
        self.premap = FileMap.load(casedata.get('prefiles', []), casedata.get('templates', []))
        self.postmap = FileMap.load(casedata.get('postfiles', []))

        # Read commands
        self.script = ScriptTemplate.load(casedata.get('script', []), casedata.get('containers', {}))

        # Fill in types derived from commands
        self.script.add_types(self.types)

        # Read settings
        settings = casedata.get('settings', {})
        self._logdir = settings.get('logdir', '${_index}')
        self._ignore_missing = settings.get('ignore-missing-files', False)

        # Construct plot objects
        self._plots = [Plot.load(spec, self.parameters, self.types) for spec in casedata.get('plots', [])]

    @property
    def parameters(self):
        return self.context_mgr.parameters

    @property
    def types(self):
        return self.context_mgr.types

    def clear_cache(self):
        shutil.rmtree(self.storagepath)
        self.storagepath.mkdir(parents=True, exist_ok=True)

    def clear_dataframe(self):
        with self.lock():
            self.dataframepath.unlink(missing_ok=True)

    @contextmanager
    def lock(self):
        with InterProcessLock(self.storagepath / 'lockfile'):
            yield

    def load_dataframe(self):
        if self.dataframepath.is_file():
            return pd.read_parquet(self.dataframepath, engine='pyarrow')
        data = {
            k: pd.Series([], dtype=_pandas_dtype(v))
            for k, v in self.types.items()
            if k != '_index'
        }
        return pd.DataFrame(index=pd.Int64Index([]), data=data)

    def save_dataframe(self, df: pd.DataFrame):
        df.to_parquet(
            self.dataframepath, engine='pyarrow', index=True,
            allow_truncated_timestamps=True, coerce_timestamps='us',
        )

    def has_data(self):
        with self.lock():
            for _ in self.instances(Status.Downloaded):
                return True
        return False

    def _check_decide_diff(self, diff: List[str], prev_file: Path, interactive: bool = True) -> bool:
        decision = None
        decisions = ['exit', 'diff', 'new-delete', 'new-keep', 'old']
        if interactive:
            if os.name == 'nt':
                from pyreadline import Readline
                readline = Readline()
            else:
                import readline
            readline.set_completer(util.completer(decisions))
            readline.parse_and_bind('tab: complete')
            util.log.warning("Warning: Grevlingfile has changed and data have already been stored")
            util.log.warning("Pick an option:")
            util.log.warning("  exit - quit grevling and fix the problem manually")
            util.log.warning("  diff - view a diff between old and new")
            util.log.warning("  new-delete - accept new version and delete existing data (significant changes made)")
            util.log.warning("  new-keep - accept new version and keep existing data (no significant changes made)")
            util.log.warning("  old - accept old version and exit (re-run grevling to load the changed grevlingfile)")
            while decision is None:
                decision = input('>>> ').strip().lower()
                if decision not in decisions:
                    decision = None
                    continue
                if decision == 'diff':
                    pydoc.pager(''.join(diff))
                    decision = None
                if decision == 'exit':
                    return False
                if decision == 'new-delete':
                    self.clear_cache()
                    break
                if decision == 'new-keep':
                    break
                if decision == 'old':
                    shutil.copyfile(prev_file, self.yamlpath)
                    return False
        else:
            util.log.error("Error: Grevlingfile has changed and data have already been stored")
            util.log.error("Try running 'grevling check' for more information, or delete .grevlingdata if you're sure")
            return False
        return True

    def check(self, interactive=True) -> bool:
        prev_file = self.storagepath / 'grevling.yaml'
        if prev_file.exists():
            with open(self.yamlpath, 'r') as f:
                new_lines = f.readlines()
            with open(prev_file, 'r') as f:
                old_lines = f.readlines()
            diff = list(Differ().compare(old_lines, new_lines))
            if not all(line.startswith('  ') for line in diff) and self.has_data():
                if not self._check_decide_diff(diff, prev_file, interactive=interactive):
                    return False

        shutil.copyfile(self.yamlpath, prev_file)

        if interactive:
            util.log.info("Derived types:")
            for key, value in self._types.items():
                util.log.info(f"  {key}: {_typename(value)}")

        return True

    def create_instances(self) -> Iterable['Instance']:
        for i, ctx in enumerate(self.context_mgr.fullspace()):
            ctx['_index'] = i
            ctx['_logdir'] = render(self._logdir, ctx)
            yield Instance.create(self, ctx)

    def create_instance(self, ctx: api.Context, logdir: Optional[Path] = None, index: Optional[int] = None) -> 'Instance':
        ctx = self.context_mgr.evaluate_context(ctx)
        if index is None:
            index = 0
        ctx['_index'] = index
        if logdir is None:
            logdir = render(self._logdir, ctx)
        ctx['_logdir'] = str(logdir)
        workspace = LocalWorkspace(Path(ctx['_logdir']), name='LOG')
        return Instance.create(self, ctx, local=workspace)

    def instances(self, *statuses) -> Iterable['Instance']:
        for name in self.storage_spaces.workspace_names():
            if not self.storage_spaces.open_workspace(name).exists('.grevling/status.txt'):
                continue
            instance = Instance(self, logdir=name)
            if statuses and instance.status not in statuses:
                continue
            yield instance

    def capture(self):
        with self.lock():
            for instance in self.instances(Status.Downloaded):
                instance.capture()

    def collect(self):
        with self.lock():
            data = self.load_dataframe()
            for instance in self.instances(Status.Downloaded):
                collector = instance.cached_capture()
                data = collector.commit_to_dataframe(data)
            data = data.sort_index()
            self.save_dataframe(data)

    def plot(self):
        for plot in self._plots:
            plot.generate_all(self)


    # Deprecated methods

    @util.deprecated("use LocalWorkflow.pipeline().run(case.create_instances()) instead", name='Case.run')
    def run(self, nprocs=1):
        nprocs = nprocs or 1
        with LocalWorkflow(nprocs=nprocs) as workflow:
            workflow.pipeline().run(self.create_instances())

    def run_single(self, namespace: api.Context, logdir: Path, index: int = 0):
        instance = self.create_instance(namespace, logdir=logdir, index=index)
        with LocalWorkflow() as workflow:
            workflow.pipeline().run([instance])

    @util.deprecated("use Case.instances() instead", name='Case.iter_instancedirs')
    def iter_instancedirs(self) -> Iterable[api.Workspace]:
        for path in self.storagepath.iterdir():
            print(path)
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

    _context: Optional[api.Context]
    _status: Optional[Status]

    @classmethod
    def create(cls, case, context: api.Context, local = None) -> 'Instance':
        obj = cls(case, context=context, local=local)
        obj.status = Status.Created
        obj.write_context()
        return obj

    def __init__(self, case, context: api.Context = None, logdir = None, local = None):
        self._case = case
        self._context = context

        if context:
            self.logdir = context['_logdir']
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

    def upload_script(self, in_container: bool = False):
        assert self.remote_book
        assert self.status == Status.Prepared
        script = self.script.to_bash(in_container=in_container)
        self.remote_book.write_file('grevling.sh', script)

    def download(self):
        assert self.remote
        assert self.remote_book
        assert self.status == Status.Finished

        collector = ResultCollector(self.types)
        collector.collect_from_dict(self.context)

        bookmap = FileMap.load(
            files=[{'source': '*', 'mode': 'glob'}],
        )
        bookmap.copy(self.context, self.remote_book, self.local_book)
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
