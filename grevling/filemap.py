from __future__ import annotations

from pathlib import Path

from typing import Any, Optional, List, Dict, Iterable, Tuple

from . import util, api, schema
from .render import StringRenderable, JsonRenderable, renderable


class SingleFileMap:

    source: str
    target: str
    template: bool
    mode: str

    @classmethod
    def load(cls, spec: Dict, **kwargs) -> SingleFileMap:
        return util.call_yaml(cls, spec, **kwargs)

    def __init__(
        self,
        source: str,
        target: Optional[str] = None,
        template: bool = False,
        mode: str = 'simple',
    ):
        if Path(source).is_absolute() and target is None:
            util.log.warning('File mappings with absolute source paths should have explicit target')

        if target is None:
            target = source if mode == 'simple' else '.'
        if template:
            mode = 'simple'

        self.source = source
        self.target = target
        self.template = template
        self.mode = mode

    def iter_paths(
        self, context: api.Context, source: api.Workspace
    ) -> Iterable[Tuple[Path, Path]]:
        if self.mode == 'simple':
            yield (Path(self.source), Path(self.target))

        elif self.mode == 'glob':
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
            else:
                util.log.debug(
                    f'{source.name}/{sourcepath} -> {target.name}/{targetpath}'
                )

            if not self.template:
                with source.open_bytes(sourcepath) as f:
                    target.write_file(targetpath, f)

            else:
                with source.open_bytes(sourcepath) as f:
                    text = f.read().decode()
                target.write_file(targetpath, StringRenderable(text).render(context).encode())

            mode = source.mode(sourcepath)
            if mode is not None:
                target.set_mode(targetpath, mode)

        return True


class FileMap:

    elements: List[SingleFileMap]

    @classmethod
    def load(cls, data: List) -> FileMap:
        return cls.create(files=data)

    @classmethod
    def create(cls, *, files: List = []) -> FileMap:
        mapping = cls()
        mapping.elements = [SingleFileMap.load(spec) for spec in files]
        return mapping

    def copy(
        self,
        context: api.Context,
        source: api.Workspace,
        target: api.Workspace,
        **kwargs,
    ) -> bool:
        for mapper in self.elements:
            if not mapper.copy(context, source, target, **kwargs):
                return False
        return True


def FileMapTemplate(data: Any) -> api.Renderable[FileMap]:
    return renderable(data, FileMap.load, schema.FileMap.validate, '[*][source,target]')
