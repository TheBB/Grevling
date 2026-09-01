from pathlib import Path
from types import TracebackType
from typing import Self

class InterProcessLock:
    def __init__(self, path: Path) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...
