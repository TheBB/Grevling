from contextlib import contextmanager
from io import IOBase
from pathlib import Path
import shutil
import tempfile

from typing import Union, Iterable, Optional, ContextManager

from . import Pipeline, PipeSegment, PrepareInstance, DownloadResults
from ..api import Status
from .. import util, api


class RunInstance(PipeSegment):

    def __init__(self, workspaces):
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Run')
    def apply(self, instance):
        instance.status = Status.Started
        workspace = instance.open_workspace(self.workspaces)
        instance.script.run(workspace.root, workspace.subspace('.grevling'))
        instance.status = Status.Finished
        return instance


class LocalWorkflow(api.Workflow):

    name = 'local'

    def __enter__(self):
        self.workspaces = TempWorkspaceCollection('WRK').__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.workspaces.__exit__(*args, **kwargs)

    def pipeline(self):
        return Pipeline(
            PrepareInstance(self.workspaces),
            RunInstance(self.workspaces),
            DownloadResults(self.workspaces),
        )


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

    def workspace_names(self, name: str = '') -> Iterable[str]:
        for path in self.root.iterdir():
            if path.is_dir():
                yield path.name


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

    def subspace(self, path: str, name: str = '') -> api.Workspace:
        name = name or str(path)
        path = self.root / path
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