from __future__ import annotations

import shutil
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import IO, TYPE_CHECKING, BinaryIO, Optional, TextIO, Union

from grevling import api, util
from grevling.api import PathType, Status

from . import DownloadResults, Pipeline, PipeSegment, PrepareInstance

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator
    from types import TracebackType

    from grevling import Case, Instance


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

    workspaces: TempWorkspaceCollection

    def __init__(self, nprocs: int = 1):
        self.nprocs = nprocs

    def __enter__(self) -> LocalWorkflow:
        self.workspaces = TempWorkspaceCollection("WRK").__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.workspaces.__exit__(exc_type, exc_val, exc_tb)

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

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    def new_workspace(self, prefix: Optional[str] = None, name: str = "") -> LocalWorkspace:
        path = Path(tempfile.mkdtemp(prefix=prefix, dir=self.root))
        return LocalWorkspace(path, name)

    def open_workspace(self, path: str, name: str = "") -> LocalWorkspace:
        subpath = self.root / path
        subpath.mkdir(parents=True, exist_ok=True)
        return LocalWorkspace(subpath, name)

    def destroy_workspace(self, path: str) -> None:
        subpath = self.root / path
        shutil.rmtree(subpath)

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

    def __str__(self) -> str:
        return str(self.root)

    def destroy(self) -> None:
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
    def open_str(self, path: Union[Path, str], mode: str = "w") -> Generator[TextIO, None, None]:
        with self.to_root(path).open(mode) as f:
            yield f  # type: ignore

    @contextmanager
    def open_bytes(self, path: Union[Path, str], mode: str = "rb") -> Generator[BinaryIO, None, None]:
        with self.to_root(path).open(mode) as f:
            yield f  # type: ignore

    def write_file(
        self, path: Union[Path, str], source: Union[str, bytes, IO, Path], append: bool = False
    ) -> None:
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

    def files(self) -> Iterator[Path]:
        for path in self.root.rglob("*"):
            if path.is_file():
                yield path.relative_to(self.root)

    def exists(self, path: Union[Path, str]) -> bool:
        return self.to_root(path).exists()

    def type_of(self, path: Union[Path, str]) -> PathType:
        p = self.to_root(path)
        if p.is_file():
            return PathType.File
        if p.is_dir():
            return PathType.Folder
        assert False

    def mode(self, path: Union[Path, str]) -> int:
        return self.to_root(path).stat().st_mode

    def set_mode(self, path: Union[Path, str], mode: int) -> None:
        self.to_root(path).chmod(stat.S_IMODE(mode))

    def subspace(self, path: str, name: str = "") -> api.Workspace:
        name = name or str(path)
        subpath = self.root / path
        subpath.mkdir(exist_ok=True, parents=True)
        return LocalWorkspace(subpath, name=f"{self.name}/{name}")

    def top_name(self) -> str:
        return self.root.name

    def walk(self, path: Optional[Union[Path, str]]) -> Iterator[Path]:
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

    def __enter__(self) -> TempWorkspaceCollection:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.__enter__())
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        super().__exit__(exc_type, exc_val, exc_tb)
        self.tempdir.__exit__(exc_type, exc_val, exc_tb)
