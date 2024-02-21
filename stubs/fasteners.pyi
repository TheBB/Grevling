from pathlib import Path
from types import TracebackType
from typing import Optional

from typing_extensions import Self

class InterProcessLock:
    def __init__(self, path: Path) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...
