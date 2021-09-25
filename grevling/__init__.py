from contextlib import contextmanager
from difflib import Differ
import os
from pathlib import Path
import pydoc
import shutil

from typing import List, Iterable

from fasteners import InterProcessLock
import pandas as pd
from typing_inspect import get_origin, get_args

from .plotting import Plot
from .render import render
from .schema import load_and_validate
from .context import ContextManager
from .filemap import FileMap
from .script import ScriptTemplate
from . import util, api
from .runner import local as runner
from .instance import Instance


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
        self.local_space = runner.LocalWorkspace(self.sourcepath, 'SRC')

        if storagepath is None:
            storagepath = self.sourcepath / '.grevlingdata'
        storagepath.mkdir(parents=True, exist_ok=True)
        self.storagepath = storagepath
        self.storage_spaces = runner.LocalWorkspaceCollection(self.storagepath)

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

    def iter_instancedirs(self) -> Iterable[api.Workspace]:
        for path in self.storagepath.iterdir():
            print(path)
            if not (path / '.grevling' / 'context.json').exists():
                continue
            yield runner.LocalWorkspace(path)

    @property
    def shape(self):
        return tuple(map(len, self._parameters.values()))

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
        df.to_parquet(self.dataframepath, engine='pyarrow', index=True)

    def has_data(self):
        from instance import Status
        with self.lock():
            for instance in self.instances(Status.Downloaded):
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

    def create_instances(self) -> Iterable[Instance]:
        for i, ctx in enumerate(self.context_mgr.fullspace()):
            ctx['_index'] = i
            ctx['_logdir'] = render(self._logdir, ctx)
            yield Instance.create(self, ctx)

    def instances(self, *statuses) -> Iterable[Instance]:
        for name in self.storage_spaces.workspace_names():
            instance = Instance(self, logdir=name)
            if statuses and instance.status not in statuses:
                continue
            yield instance

    def capture(self):
        from .instance import Status
        with self.lock():
            for instance in self.instances(Status.Downloaded):
                instance.capture()

    def collect(self):
        from .instance import Status
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

    @util.with_context('instance {index}')
    def run_single(self, index, namespace, workspace, log_ws):
        util.log.info(', '.join(f'{k}={repr(namespace[k])}' for k in self.parameters))
        success = self.script.run(namespace, workspace.root, log_ws)
        return success
