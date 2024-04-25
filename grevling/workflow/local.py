from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import stat
import tempfile

from typing import IO, BinaryIO, Generator, TextIO, Union, Iterable, Optional, TYPE_CHECKING

from . import Pipeline, PipeSegment, PrepareInstance, DownloadResults
from ..api import Status, PathType
from .. import util, api

if TYPE_CHECKING:
    from .. import Instance, Case


class RunInstance(PipeSegment):
    name = "Run"

    def __init__(self, workspaces: api.WorkspaceCollection, ncopies: int = 1):
        super().__init__(ncopies)
        self.workspaces = workspaces

    @util.with_context("I {instance.index}")
    @util.with_context("Run")
    async def apply(self, instance: Instance) -> Instance:
        instance.status = Status.Started
        workspace = instance.open_workspace(self.workspaces)
        assert isinstance(workspace, LocalWorkspace)
        await instance.script.run(workspace.root, workspace.subspace(".grevling"))
        instance.status = Status.Finished
        return instance


class LocalWorkflow(api.Workflow):
    name = "local"
    nprocs: int

    def __init__(self, nprocs: int = 1):
        self.nprocs = nprocs

    def __enter__(self):
        self.workspaces = TempWorkspaceCollection("WRK").__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.workspaces.__exit__(*args, **kwargs)

    def pipeline(self, case: Case) -> Pipeline:
        return Pipeline(
            PrepareInstance(self.workspaces),
            RunInstance(self.workspaces, ncopies=self.nprocs),
            DownloadResults(self.workspaces, case),
        )


class LocalWorkspaceCollection(api.WorkspaceCollection):
    root: Path

    def __init__(self, root: Union[str, Path], name: str = ""):
        self.root = Path(root)
        self.name = name

    def __enter__(self) -> LocalWorkspaceCollection:
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def new_workspace(self, prefix: Optional[str] = None, name: str = "") -> LocalWorkspace:
        path = Path(tempfile.mkdtemp(prefix=prefix, dir=self.root))
        return LocalWorkspace(path, name)

    def open_workspace(self, path: str, name: str = "") -> LocalWorkspace:
        subpath = self.root / path
        subpath.mkdir(parents=True, exist_ok=True)
        return LocalWorkspace(subpath, name)

    def workspace_names(self, name: str = "") -> Iterable[str]:
        for path in self.root.iterdir():
            if path.is_dir():
                yield path.name


class LocalWorkspace(api.Workspace):
    root: Path
    name: str

    def __init__(self, root: Union[str, Path], name: str = ""):
        self.root = Path(root)
        self.name = name

    def __str__(self):
        return str(self.root)

    def destroy(self):
        shutil.rmtree(self.root)

    def to_root(self, path: Optional[Union[Path, str]]) -> Path:
        if path is None:
            return self.root
        if isinstance(path, str):
            path = Path(path)
        if path.is_absolute():
            return path
        return self.root / path

    @contextmanager
    def open_str(self, path, mode: str = "w") -> Generator[TextIO, None, None]:
        with open(self.to_root(path), mode) as f:
            yield f  # type: ignore

    @contextmanager
    def open_bytes(self, path, mode: str = "rb") -> Generator[BinaryIO, None, None]:
        with open(self.to_root(path), mode) as f:
            yield f  # type: ignore

    def write_file(self, path, source: Union[str, bytes, IO, Path], append: bool = False):
        target = self.to_root(path)
        target.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(source, Path):
            shutil.copyfile(source, target)
            return

        if isinstance(source, str):
            source = source.encode()

        mode = "ab" if append else "wb"
        with self.open_bytes(path, mode) as f:
            if isinstance(source, bytes):
                f.write(source)
            else:
                shutil.copyfileobj(source, f)
            return

    def files(self) -> Iterable[Path]:
        for path in self.root.rglob("*"):
            if path.is_file():
                yield path.relative_to(self.root)

    def exists(self, path) -> bool:
        return self.to_root(path).exists()

    def mode(self, path) -> int:
        return os.stat(self.to_root(path)).st_mode

    def type_of(self, path):
        p = self.to_root(path)
        if p.is_file():
            return PathType.File
        if p.is_dir():
            return PathType.Folder
        assert False

    def set_mode(self, path, mode: int):
        os.chmod(self.to_root(path), stat.S_IMODE(mode))

    def subspace(self, path: str, name: str = "") -> api.Workspace:
        name = name or str(path)
        subpath = self.root / path
        subpath.mkdir(exist_ok=True, parents=True)
        return LocalWorkspace(subpath, name=f"{self.name}/{name}")

    def top_name(self) -> str:
        return self.root.name

    def walk(self, path=None):
        p = self.to_root(path)
        for sub in p.iterdir():
            pathtype = self.type_of(sub)
            if pathtype == PathType.File:
                yield sub.relative_to(self.root)
            elif pathtype == PathType.Folder:
                yield from self.walk(sub)


class TempWorkspaceCollection(LocalWorkspaceCollection):
    tempdir: tempfile.TemporaryDirectory

    def __init__(self, name: str = ""):
        super().__init__(root="", name=name)

    def __enter__(self) -> LocalWorkspaceCollection:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.__enter__())
        return self

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        self.tempdir.__exit__(*args, **kwargs)
