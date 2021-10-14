from __future__ import annotations

import json
import re

from typing import Optional, List, TYPE_CHECKING

from pydantic import validate_model
from pydantic.fields import SHAPE_LIST

from . import util, api

if TYPE_CHECKING:
    from .typing import TypeManager


class Capture:

    _regex: re.Pattern
    _mode: str
    _type_overrides: api.Types

    @classmethod
    def load(cls, spec) -> Capture:
        if isinstance(spec, str):
            return cls(spec)
        if spec.get('type') in ('integer', 'float'):
            pattern, tp = {
                'integer': (r'[-+]?[0-9]+', int),
                'float': (r'[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?', float),
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
            type_overrides = {spec['name']: tp}
            return cls(pattern, mode, type_overrides)
        return util.call_yaml(cls, spec)

    def __init__(
        self,
        pattern: str,
        mode: str = 'last',
        type_overrides: Optional[TypeManager] = None,
    ):
        self._regex = re.compile(pattern)
        self._mode = mode
        self._type_overrides = type_overrides or {}

    def add_types(self, types: TypeManager):
        for group in self._regex.groupindex.keys():
            single = self._type_overrides.get(group, str)
            if group in types:
                continue
            if self._mode == 'all':
                types.add(group, List[single], 'post')
            else:
                types.add(group, single, 'post')

    def find_in(self, collector: Capture, string: str):
        matches = self._regex.finditer(string)
        if self._mode == 'first':
            try:
                matches = [next(matches)]
            except StopIteration:
                return

        elif self._mode == 'last':
            try:
                match = next(matches)
            except StopIteration:
                return
            for match in matches:
                pass
            matches = [match]

        for match in matches:
            for name, value in match.groupdict().items():
                collector.collect(name, value)


class CaptureCollection(api.Context):

    def collect(self, name, value):
        if self.__fields__[name].shape == SHAPE_LIST:
            self[name].append(value)
        else:
            self[name] = value

    def collect_from_file(self, ws: api.Workspace, filename: str):
        with ws.open_file(filename, 'r') as f:
            data = json.load(f)
        self.update(data)

    def collect_from_context(self, ws: api.Workspace):
        self.collect_from_file(ws, 'context.json')

    def collect_from_cache(self, ws: api.Workspace):
        self.collect_from_file(ws, 'captured.json')

    def collect_from_info(self, ws: api.Workspace):
        with ws.open_file('grevling.txt', 'r') as f:
            for line in f:
                key, value = line.strip().split('=', 1)
                self[key] = value

    def validate(self) -> Capture:
        data, _, error = validate_model(self.__class__, self.__dict__)
        if error is not None:
            raise error
        return self.__class__(**data)

    def commit_to_file(self, ws: api.Workspace):
        with ws.open_file('captured.json', 'w') as f:
            f.write(self.json())

    def commit_to_dataframe(self, data):
        index = self['g_index']
        data.loc[index, :] = [None] * data.shape[1]
        for key, value in self.items():
            if key == 'g_index':
                continue
            data.at[index, key] = value
        return data
