from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional, TypedDict

from . import api, util
from .render import render

if TYPE_CHECKING:
    from collections.abc import Iterable

    from typing_extensions import Unpack

    from .schema import FileMapSchema


class CopyKwargs(TypedDict, total=False):
    ignore_missing: bool


class SingleFileMap:
    source: str
    target: str
    template: bool
    mode: str

    @staticmethod
    def from_schema(schema: FileMapSchema) -> SingleFileMap:
        return SingleFileMap(
            source=schema.source,
            target=schema.target,
            template=schema.template,
            mode=schema.mode,
        )

    def __init__(
        self,
        source: str,
        target: Optional[str] = None,
        template: bool = False,
        mode: str = "simple",
    ):
        if Path(source).is_absolute() and target is None:
            util.log.warning("File mappings with absolute source paths should have explicit target")

        if target is None:
            target = source if mode == "simple" else "."
        if template:
            mode = "simple"

        self.source = source
        self.target = target
        self.template = template
        self.mode = mode

    def iter_paths(self, context: api.Context, source: api.Workspace) -> Iterable[tuple[Path, Path]]:
        if self.mode == "simple":
            yield (Path(self.source), Path(self.target))

        elif self.mode == "glob":
            for path in source.glob(self.source):
                yield (path, Path(self.target) / path)

    def copy(
        self,
        context: api.Context,
        source: api.Workspace,
        target: api.Workspace,
        ignore_missing: bool = False,
    ) -> bool:
        for sourcepath, targetpath in self.iter_paths(context, source):
            if not source.exists(sourcepath):
                level = util.log.warning if ignore_missing else util.log.error
                level(f"Missing file: {source.name}/{sourcepath}")
                if ignore_missing:
                    continue
                return False

            util.log.debug(f"{source.name}/{sourcepath} -> {target.name}/{targetpath}")

            if not self.template:
                with source.open_bytes(sourcepath) as f:
                    target.write_file(targetpath, f)

            else:
                with source.open_bytes(sourcepath) as f:
                    text = f.read().decode()
                target.write_file(targetpath, render(text, context).encode())

            mode = source.mode(sourcepath)
            if mode is not None:
                target.set_mode(targetpath, mode)

        return True


class FileMap:
    elements: list[SingleFileMap]

    @staticmethod
    def from_schema(schema: list[FileMapSchema]) -> FileMap:
        return FileMap([SingleFileMap.from_schema(entry) for entry in schema])

    @staticmethod
    def everything() -> FileMap:
        return FileMap([SingleFileMap(source="*", mode="glob")])

    def __init__(self, elements: list[SingleFileMap]):
        self.elements = elements

    def copy(
        self,
        context: api.Context,
        source: api.Workspace,
        target: api.Workspace,
        **kwargs: Unpack[CopyKwargs],
    ) -> bool:
        return all(mapper.copy(context, source, target, **kwargs) for mapper in self.elements)


class FileMapTemplate:
    def __init__(self, func: Callable[[api.Context], list[FileMapSchema]]):
        self.func = func

    def render(self, ctx: api.Context) -> FileMap:
        return FileMap.from_schema(self.func(ctx))
