from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from fnmatch import fnmatch
import json
from pathlib import Path

from typing import IO, BinaryIO, ContextManager, Iterable, TextIO, TypeVar, Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .workflow import Pipe
    from . import Case


PathStr = Union[Path, str]


T = TypeVar("T")


class Status(Enum):
    Created = "created"
    Prepared = "prepared"
    Started = "started"
    Finished = "finished"
    Downloaded = "downloaded"


class PathType(Enum):
    Folder = "folder"
    File = "file"


class Workspace(ABC):
    name: str

    @abstractmethod
    def __str__(self) -> str:
        ...

    @abstractmethod
    def destroy(self):
        ...

    @abstractmethod
    def open_str(self, path: PathStr, mode: str = "w") -> ContextManager[TextIO]:
        ...

    @abstractmethod
    def open_bytes(self, path: PathStr) -> ContextManager[BinaryIO]:
        ...

    @abstractmethod
    def write_file(self, path: PathStr, source: Union[str, bytes, IO, Path], append: bool = False):
        ...

    @abstractmethod
    def files(self) -> Iterable[Path]:
        ...

    @abstractmethod
    def exists(self, path: PathStr) -> bool:
        ...

    @abstractmethod
    def type_of(self, path: PathStr) -> PathType:
        ...

    @abstractmethod
    def mode(self, path: PathStr) -> Optional[int]:
        ...

    @abstractmethod
    def set_mode(self, path: PathStr, mode: int):
        ...

    @abstractmethod
    def subspace(self, path: str, name: str = "") -> Workspace:
        ...

    @abstractmethod
    def top_name(self) -> str:
        ...

    @abstractmethod
    def walk(self, path: Optional[PathStr]) -> Iterable[Path]:
        ...

    def glob(self, pattern: str) -> Iterable[Path]:
        for path in self.files():
            if fnmatch(str(path), pattern):
                yield path


class WorkspaceCollection(ABC):
    @abstractmethod
    def __enter__(self) -> WorkspaceCollection:
        ...

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        ...

    @abstractmethod
    def new_workspace(self, prefix: Optional[str] = None) -> Workspace:
        ...

    @abstractmethod
    def open_workspace(self, path: str, name: str = "") -> Workspace:
        ...

    @abstractmethod
    def workspace_names(self) -> Iterable[str]:
        ...


class Workflow(ABC):
    @abstractmethod
    def __enter__(self) -> Workflow:
        ...

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        ...

    @staticmethod
    def get_workflow(name: str):
        from . import util

        cls = util.find_subclass(Workflow, name, attr="name")
        if not cls:
            raise ImportError(f"Unknown workflow, or additional dependencies required: {name}")
        return cls

    @abstractmethod
    def pipeline(self, case: Case) -> Pipe:
        ...


class Context(dict):
    def __call__(self, fn):
        return fn(**self)

    def json(self, **kwargs) -> str:
        return json.dumps(self, **kwargs)
