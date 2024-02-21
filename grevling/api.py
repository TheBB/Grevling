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
    Optional,
    Protocol,
    TextIO,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from contextlib import AbstractContextManager
    from types import TracebackType

    import click
    from typing_extensions import Unpack

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
    def destroy(self) -> None:
        ...

    @abstractmethod
    def open_str(self, path: PathStr, mode: str = "w") -> AbstractContextManager[TextIO]:
        ...

    @abstractmethod
    def open_bytes(self, path: PathStr) -> AbstractContextManager[BinaryIO]:
        ...

    @abstractmethod
    def write_file(self, path: PathStr, source: Union[str, bytes, IO, Path], append: bool = False) -> None:
        ...

    @abstractmethod
    def files(self) -> Iterator[Path]:
        ...

    @abstractmethod
    def exists(self, path: PathStr) -> bool:
        ...

    @abstractmethod
    def mode(self, path: PathStr) -> Optional[int]:
        ...

    @abstractmethod
    def set_mode(self, path: PathStr, mode: int) -> None:
        ...

    @abstractmethod
    def subspace(self, path: str, name: str = "") -> Workspace:
        ...

    @abstractmethod
    def top_name(self) -> str:
        ...

    def glob(self, pattern: str) -> Iterator[Path]:
        for path in self.files():
            if fnmatch(str(path), pattern):
                yield path


class WorkspaceCollection(ABC):
    @abstractmethod
    def __enter__(self) -> WorkspaceCollection:
        ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
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


class WorkflowConstructor(Protocol):
    def __call__(self, nprocs: int = 1) -> Workflow:
        ...


class Workflow(ABC):
    @abstractmethod
    def __enter__(self) -> Workflow:
        ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        ...

    @staticmethod
    def get_workflow(name: str) -> WorkflowConstructor:
        from . import util

        cls = util.find_subclass(Workflow, name, attr="name")
        if not cls:
            raise ImportError(f"Unknown workflow, or additional dependencies required: {name}")
        return cast(WorkflowConstructor, cls)

    @abstractmethod
    def pipeline(self, case: Case) -> Pipe:
        ...


class JsonKwargs(TypedDict, total=False):
    indent: int
    sort_keys: bool


class Context(dict):
    def json(self, **kwargs: Unpack[JsonKwargs]) -> str:
        return json.dumps(self, **kwargs)


class Plugin:
    def __init__(self, case: Case, settings: Any) -> None:
        pass

    def commands(self, ctx: click.Context) -> list[click.Command]:
        return []
