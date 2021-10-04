from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from fnmatch import fnmatch
from io import IOBase
from pathlib import Path

from typing import Dict, Any, ContextManager, Iterable, Union, Optional, TYPE_CHECKING

from . import util

if TYPE_CHECKING:
    from .workflow import Pipe


Context = Dict[str, Any]
Types = Dict[str, Any]
PathStr = Union[Path, str]


class Status(Enum):

    Created = 'created'
    Prepared = 'prepared'
    Started = 'started'
    Finished = 'finished'
    Downloaded = 'downloaded'


class Workspace(ABC):

    name: str

    @abstractmethod
    def __str__(self) -> str:
        ...

    @abstractmethod
    def destroy(self):
        ...

    @abstractmethod
    def open_file(self, path: PathStr, mode: str = 'w') -> ContextManager[IOBase]:
        ...

    @abstractmethod
    def write_file(self, path: PathStr, source: Union[str, bytes, IOBase, Path]):
        ...

    @abstractmethod
    def read_file(self, path: PathStr) -> ContextManager[IOBase]:
        ...

    @abstractmethod
    def files(self) -> Iterable[Path]:
        ...

    @abstractmethod
    def exists(self, path: PathStr) -> bool:
        ...

    @abstractmethod
    def subspace(self, name: str) -> Workspace:
        ...

    @abstractmethod
    def top_name(self) -> str:
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
    def open_workspace(self, path: str, name: str = '') -> Workspace:
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
        cls = util.find_subclass(Workflow, name, attr='name')
        if not cls:
            raise ImportError(
                f"Unknown workflow, or additional dependencies required: {name}"
            )
        return cls

    @abstractmethod
    def pipeline() -> Pipe:
        ...
