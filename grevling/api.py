from __future__ import annotations

import json
from abc import ABC, abstractmethod
from enum import Enum
from fnmatch import fnmatch
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    BinaryIO,
    ContextManager,
    Iterable,
    Optional,
    TextIO,
    TypeVar,
    Union,
)

import click

if TYPE_CHECKING:
    from . import Case
    from .workflow import Pipe


PathStr = Union[Path, str]


T = TypeVar("T")


class Status(Enum):
    Created = "created"
    Prepared = "prepared"
    Started = "started"
    Finished = "finished"
    Downloaded = "downloaded"


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


class Plugin:
    def __init__(self, case: Case, settings: Any) -> None:
        pass

    def commands(self, ctx: click.Context) -> list[click.Command]:
        return []
