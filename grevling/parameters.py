from typing import Sequence, Iterable, Dict, Any

import numpy as np

from . import api, util


class Parameter(Sequence):

    @classmethod
    def load(cls, name: str, spec: Any):
        if isinstance(spec, list):
            return cls(name, spec)
        assert isinstance(spec, dict)
        subcls = util.find_subclass(cls, spec.pop('type'), root=False, attr='__tag__')
        return util.call_yaml(subcls, spec, name)

    def __init__(self, name, values):
        self.name = name
        self.values = values

    def __len__(self):
        return len(self.values)

    def __getitem__(self, index):
        return self.values[index]


class UniformParameter(Parameter):

    __tag__ = 'uniform'

    def __init__(self, name, interval, num):
        super().__init__(name, np.linspace(*interval, num=num))


class GradedParameter(Parameter):

    __tag__ = 'graded'

    def __init__(self, name, interval, num, grading):
        lo, hi = interval
        step = (hi - lo) * (1 - grading) / (1 - grading ** (num - 1))
        values = [lo]
        for _ in range(num - 1):
            values.append(values[-1] + step)
            step *= grading
        super().__init__(name, np.array(values))


class ParameterSpace(dict):

    @classmethod
    def load(cls, data: Dict) -> 'ParameterSpace':
        return cls({
            name: Parameter.load(name, spec)
            for name, spec in data.items()
        })

    def subspace(self, *names: str) -> Iterable[Dict]:
        params = [self[name] for name in names]
        for values in util.dict_product(names, params):
            yield values

    def fullspace(self) -> Iterable[Dict]:
        yield from self.subspace(*self.keys())

    def size(self, *names: str) -> int:
        return util.prod(len(self[name]) for name in names)

    def size_fullspace(self) -> int:
        return self.size(*self.keys())
