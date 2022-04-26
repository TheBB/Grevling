from __future__ import annotations

import json
import re

from typing import Iterable, Optional, TYPE_CHECKING, Type

from . import util, api, typing

if TYPE_CHECKING:
    from .typing import TypeManager, GType


class Capture:

    _regex: re.Pattern
    _mode: str
    _type: Optional[GType]

    @classmethod
    def load(cls, spec) -> Capture:
        if isinstance(spec, str):
            return cls(spec)
        if spec.get('type') in ('integer', 'float'):
            pattern = {
                'integer': r'[-+]?[0-9]+',
                'float': r'[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?',
            }[spec['type']]
            skip = r'(\S+\s+){' + str(spec.get('skip-words', 0)) + '}'
            if spec.get('flexible-prefix', False):
                prefix = r'\s+'.join(re.escape(p) for p in spec['prefix'].split())
            else:
                prefix = re.escape(spec['prefix'])
            pattern = (
                prefix
                + r'\s*[:=]?\s*'
                + skip
                + '(?P<'
                + spec['name']
                + '>'
                + pattern
                + ')'
            )
            mode = spec.get('mode', 'last')
            tp = typing.GType.from_string(spec['type'])
            return cls(pattern, mode, tp)
        return util.call_yaml(cls, spec)

    def __init__(
        self,
        pattern: str,
        mode: str = 'last',
        tp: Optional[GType] = None,
    ):
        self._regex = re.compile(pattern)
        self._mode = mode
        self._type = tp

    def find_in(self, collector: CaptureCollection, string: str):
        matches = self._regex.finditer(string)
        filtered: Iterable[re.Match]

        if self._mode == 'first':
            try:
                filtered = [next(matches)]
            except StopIteration:
                return
            tp = self._type

        elif self._mode == 'last':
            try:
                match = next(matches)
            except StopIteration:
                return
            for match in matches:
                pass
            filtered = [match]
            tp = self._type

        else:
            tp = typing.List(self._type) if self._type else None
            filtered = list(matches)

        for match in filtered:
            for name, value in match.groupdict().items():
                collector.collect(name, value, tp=tp)


class CaptureCollection(api.Context):

    types: TypeManager

    def __init__(self, types: TypeManager):
        self.types = types

    def collect(self, name, value, tp: Optional[GType] = None):
        gtp: GType = tp if tp is not None else self.types.get(name, typing.AnyType())
        value = gtp.coerce(value)
        if gtp.is_list:
            self.setdefault(name, []).append(value)
        else:
            self[name] = value

    def collect_from_file(self, ws: api.Workspace, filename: str):
        with ws.open_str(filename, 'r') as f:
            data = json.load(f)
        self.update(data)

    def collect_from_context(self, ws: api.Workspace):
        self.collect_from_file(ws, 'context.json')

    def collect_from_cache(self, ws: api.Workspace):
        self.collect_from_file(ws, 'captured.json')

    def collect_from_info(self, ws: api.Workspace):
        with ws.open_str('grevling.txt', 'r') as f:
            for line in f:
                key, value = line.strip().split('=', 1)
                self.collect(key, value)

    def commit_to_file(self, ws: api.Workspace):
        with ws.open_str('captured.json', 'w') as f:
            f.write(self.json())

    def commit_to_dataframe(self, data):
        index = self['g_index']
        data.loc[index, :] = [None] * data.shape[1]
        for key, value in self.items():
            if key == 'g_index':
                continue
            data.at[index, key] = value
        return data

    def commit_to_database(self, model: Type[typing.PeeweeModel]):
        kwargs = {}
        for key, value in self.items():
            if model.types[key].is_list:
                continue
            kwargs[key] = value

        inst = model.create(**kwargs)

        for key, values in self.items():
            if not model.types[key].is_list:
                continue
            rel_model = getattr(model, key).rel_model
            for index, value in enumerate(values):
                rel_inst = rel_model()
                rel_inst.instance = inst.id
                rel_inst.index = index
                rel_inst.value = value
                rel_inst.save()
