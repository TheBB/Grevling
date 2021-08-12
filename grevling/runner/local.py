from contextlib import contextmanager
from io import IOBase
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from typing import Union, ContextManager, Iterable

from .. import api


class LocalWorkspace(api.Workspace):

    root: Path

    def __init__(self, root: Union[str, Path], name: str = ''):
        self.root = Path(root)
        self.name = name

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def __str__(self):
        return str(self.root)

    @contextmanager
    def open_file(self, path: Path, mode: str = 'w') -> ContextManager[IOBase]:
        with open(self.root / path, mode) as f:
            yield f

    def write_file(self, path: Path, source: Union[bytes, IOBase, Path]):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(source, Path):
            shutil.copyfile(source, target)
            return

        with self.open_file(path, 'wb') as f:
            if isinstance(source, bytes):
                f.write(source)
            else:
                shutil.copyfileobj(source, f)
            return

    @contextmanager
    def read_file(self, path: Path) -> ContextManager[IOBase]:
        with open(self.root / path, 'rb') as f:
            yield f

    def files(self) -> Iterable[Path]:
        for path in self.root.rglob('*'):
            if path.is_file():
                yield path.relative_to(self.root)

    def exists(self, path: Path) -> bool:
        return (self.root / path).exists()


class TempWorkspace(LocalWorkspace):

    tempdir: TemporaryDirectory

    def __init__(self, name: str):
        self.name = name

    def __enter__(self):
        self.tempdir = TemporaryDirectory()
        self.root = Path(self.tempdir.__enter__())
        return self

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        self.tempdir.__exit__(*args, **kwargs)
