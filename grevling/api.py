from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from fnmatch import fnmatch
from io import IOBase
from pathlib import Path

from typing import ContextManager, Iterable, Union, Optional, TYPE_CHECKING

from pydantic import BaseModel

from . import util

if TYPE_CHECKING:
    from .workflow import Pipe
    from . import Case


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
    def pipeline(self, case: Case) -> Pipe:
        ...


class Context(BaseModel):

    class Config:
        validate_assignment = True

    def __contains__(self, key):
        return hasattr(self, key)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def update(self, mapping):
        for k, v in mapping.items():
            self[k] = v
