from __future__ import annotations

import json
import re

from typing import Any, List, Optional

import pandas as pd

from . import util, api, typing
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
                'integer': (r'[-+]?[0-9]+', typing.Integer()),
                'float': (r'[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?', typing.Float()),
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

    def add_types(self, types: api.Types):
        for group in self._regex.groupindex.keys():
            single = self._type_overrides.get(group, typing.String())
            if self._mode == 'all':
                types.setdefault(group, typing.List(single))
            else:
                types.setdefault(group, single)

    def find_in(self, collector: ResultCollector, string: str):
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


class ResultCollector(dict):

    _types: TypeManager

    def __init__(self, types: TypeManager):
        super().__init__()
        self._types = types

    def collect_from_context(self, ws: api.Workspace):
        self.collect_from_file(ws, 'context.json')

    def collect_from_cache(self, ws: api.Workspace):
        self.collect_from_file(ws, 'captured.json')

    def collect_from_file(self, ws: api.Workspace, filename: str):
        with ws.open_file(filename, 'r') as f:
            data = json.load(f)
        for key, value in data.items():
            self.collect(key, value)

    def collect_from_info(self, ws: api.Workspace):
        with ws.open_file('grevling.txt', 'r') as f:
            for line in f:
                key, value = line.strip().split('=', 1)
                self.collect(key, value)

    def collect_from_dict(self, d: dict):
        for k, v in d.items():
            self.collect(k, v)

    def collect(self, name: str, value: Any):
        if name not in self._types:
            return
        self[name] = self._types.coerce_into(name, value, self.get(name))

    def commit_to_file(self, ws: api.Workspace):
        json_data = {
            key: self._types.to_json(key, value)
            for key, value in self.items()
        }
        with ws.open_file('captured.json', 'w') as f:
            json.dump(json_data, f, sort_keys=True, indent=4, cls=util.JSONEncoder)

    def commit_to_dataframe(self, data: pd.DataFrame):
        index = self['_index']
        data.loc[index, :] = [None] * data.shape[1]
        for key, value in self.items():
            if key == '_index':
                continue
            data.at[index, key] = value
        return data
