from contextlib import contextmanager
from grevling import filemap, util
from grevling.capture import ResultCollector
from io import IOBase
from pathlib import Path
import shutil
import tempfile

from typing import Union, ContextManager, Iterable, Optional, List, Tuple

from .. import api, script
from ..capture import ResultCollector


class LocalWorkspaceCollection(api.WorkspaceCollection):

    root: Path

    def __init__(self, root: Union[str, Path], name: str = ''):
        self.root = Path(root)
        self.name = name

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def new_workspace(self, prefix: Optional[str] = None, name: str = '') -> api.Workspace:
        path = Path(tempfile.mkdtemp(prefix=prefix, dir=self.root))
        return LocalWorkspace(path, name)

    def open_workspace(self, path: str, name: str = '') -> api.Workspace:
        subpath = self.root / path
        subpath.mkdir(parents=True, exist_ok=True)
        return LocalWorkspace(subpath, name)


class LocalWorkspace(api.Workspace):

    root: Path
    name: str

    def __init__(self, root: Union[str, Path], name: str = ''):
        self.root = Path(root)
        self.name = name

    def __str__(self):
        return str(self.root)

    def destroy(self):
        shutil.rmtree(self.root)

    @contextmanager
    def open_file(self, path, mode: str = 'w') -> ContextManager[IOBase]:
        with open(self.root / path, mode) as f:
            yield f

    def write_file(self, path, source: Union[str, bytes, IOBase, Path], append: bool = False):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(source, Path):
            shutil.copyfile(source, target)
            return

        if isinstance(source, str):
            source = source.encode()

        mode = 'ab' if append else 'wb'
        with self.open_file(path, mode) as f:
            if isinstance(source, bytes):
                f.write(source)
            else:
                shutil.copyfileobj(source, f)
            return

    @contextmanager
    def read_file(self, path) -> ContextManager[IOBase]:
        with open(self.root / path, 'rb') as f:
            yield f

    def files(self) -> Iterable[Path]:
        for path in self.root.rglob('*'):
            if path.is_file():
                yield path.relative_to(self.root)

    def exists(self, path) -> bool:
        return (self.root / path).exists()

    def subspace(self, name: str) -> api.Workspace:
        path = self.root / name
        path.mkdir(exist_ok=True, parents=True)
        return LocalWorkspace(path, name=f'{self.name}/{name}')

    def top_name(self) -> str:
        return self.root.name

    def copy_all_to(self, workspace: api.Workspace):
        assert isinstance(workspace, LocalWorkspace)
        shutil.copytree(self.root, workspace.root, dirs_exist_ok=True)


class TempWorkspaceCollection(LocalWorkspaceCollection):

    tempdir: tempfile.TemporaryDirectory

    def __init__(self, name: str = ''):
        super().__init__(root='', name=name)

    def __enter__(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.__enter__())
        return self

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        self.tempdir.__exit__(*args, **kwargs)


class LocalRunner:

    workspaces: api.WorkspaceCollection
    ignore_missing: bool

    def __init__(self, ignore_missing: bool = False):
        self.prepared = []
        self.ran = []

        self.ignore_missing = ignore_missing

    def __enter__(self):
        self.workspaces = TempWorkspaceCollection().__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.workspaces.__exit__(*args, **kwargs)

    def open_workspace(self, name: str):
        return self.workspaces.open_workspace(name)

    def notify_prepared(self, instance):
        self.prepared.append(instance)

    def run_all(self):
        nsuccess = 0
        for instance in self.prepared:
            ws = self.workspaces.open_workspace(instance.logdir)
            ws_log = ws.subspace('.grevling')
            with util.log.with_context(f'Run {instance.index}'):
                success = instance.script.run(ws.root, ws_log)
            instance.success = success
            nsuccess += success
            self.notify_ran(instance)
        return nsuccess

    def notify_ran(self, instance):
        self.ran.append(instance)

    def download_all(self, workspaces: api.WorkspaceCollection, postmap):
        for instance in self.ran:
            with util.log.with_context(f'Post {instance.index}'):
                source_ws = self.workspaces.open_workspace(instance.logdir)
                target_ws = workspaces.open_workspace(instance.logdir)
                ignore_missing = self.ignore_missing or not instance.success
                postmap.copy(instance.context, source_ws, target_ws, ignore_missing=ignore_missing)

                target_log = target_ws.subspace('.grevling')
                source_log = source_ws.subspace('.grevling')
                source_log.copy_all_to(target_log)

                source_ws.destroy()
