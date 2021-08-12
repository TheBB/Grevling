from abc import ABC, abstractmethod
from fnmatch import fnmatch
from io import IOBase
from pathlib import Path


from typing import Dict, Any, ContextManager, Iterable, Union


Context = Dict[str, Any]


class Workspace(ContextManager['Workspace']):

    name: str

    @abstractmethod
    def __str__(self):
        ...

    @abstractmethod
    def open_file(self, path: Path, mode: str = 'w') -> ContextManager[IOBase]:
        ...

    @abstractmethod
    def write_file(self, path: Path, source: Union[bytes, IOBase, Path]):
        ...

    @abstractmethod
    def read_file(self, path: Path) -> ContextManager[IOBase]:
        ...

    @abstractmethod
    def files(self) -> Iterable[Path]:
        ...

    @abstractmethod
    def exists(self, path: Path) -> bool:
        ...

    def glob(self, pattern: str) -> Iterable[Path]:
        for path in self.files():
            if fnmatch(str(path), pattern):
                yield path
