import json
import re

from typing import Dict, Any, List

from typing_inspect import get_args

from . import util, api


class Capture:

    @classmethod
    def load(cls, spec):
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
            pattern = prefix + r'\s*[:=]?\s*' + skip + '(?P<' + spec['name'] + '>' + pattern + ')'
            mode = spec.get('mode', 'last')
            type_overrides = {spec['name']: tp}
            return cls(pattern, mode, type_overrides)
        return util.call_yaml(cls, spec)

    def __init__(self, pattern, mode='last', type_overrides=None):
        self._regex = re.compile(pattern)
        self._mode = mode
        self._type_overrides = type_overrides or {}

    def add_types(self, types: Dict[str, Any]):
        for group in self._regex.groupindex.keys():
            single = self._type_overrides.get(group, str)
            if self._mode == 'all':
                types.setdefault(group, List[single])
            else:
                types.setdefault(group, single)

    def find_in(self, collector, string):
        matches = self._regex.finditer(string)
        if self._mode == 'first':
            try:
                matches = [next(matches)]
            except StopIteration:
                pass

        elif self._mode == 'last':
            for match in matches:
                pass
            try:
                matches = [match]
            except UnboundLocalError:
                pass

        for match in matches:
            for name, value in match.groupdict().items():
                collector.collect(name, value)


class ResultCollector(dict):

    _types: Dict[str, Any]

    def __init__(self, types):
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
        self[name] = util.coerce_into(self._types[name], value, self.get(name))

    def commit_to_file(self, ws: api.Workspace):
        with ws.open_file('captured.json', 'w') as f:
            json.dump(self, f, sort_keys=True, indent=4, cls=util.JSONEncoder)

    def commit_to_dataframe(self, data):
        index = self['_index']
        data.loc[index, :] = [None] * data.shape[1]
        for key, value in self.items():
            if key == '_index':
                continue
            data.at[index, key] = value
        return data
