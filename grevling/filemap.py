from __future__ import annotations

from pathlib import Path

from typing import Optional, List, Dict, Iterable, Tuple

from . import util, api
from .render import render


class SingleFileMap:

    source: str
    target: str
    template: bool
    mode: str

    @classmethod
    def load(cls, spec: dict, **kwargs) -> SingleFileMap:
        if isinstance(spec, str):
            return cls(spec, spec, **kwargs)
        return util.call_yaml(cls, spec, **kwargs)

    def __init__(
        self,
        source: str,
        target: Optional[str] = None,
        template: bool = False,
        mode: str = 'simple',
    ):
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
            yield (
                Path(render(self.source, context)),
                Path(render(self.target, context)),
            )

        elif self.mode == 'glob':
            target = render(self.target, context)
            for path in source.glob(render(self.source, context)):
                yield (path, target / path)

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
                with source.read_file(sourcepath) as f:
                    target.write_file(targetpath, f)

            else:
                with source.read_file(sourcepath) as f:
                    text = f.read().decode()
                target.write_file(targetpath, render(text, context).encode())

        return True


class FileMap(list):
    @classmethod
    def load(cls, files: List[Dict] = [], templates: List[Dict] = []) -> FileMap:
        mapping = cls()
        mapping.extend(SingleFileMap.load(spec, template=True) for spec in templates)
        mapping.extend(SingleFileMap.load(spec) for spec in files)
        return mapping

    def copy(
        self,
        context: api.Context,
        source: api.Workspace,
        target: api.Workspace,
        **kwargs,
    ) -> bool:
        for mapper in self:
            if not mapper.copy(context, source, target, **kwargs):
                return False
        return True
