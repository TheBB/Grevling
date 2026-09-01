from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Union, overload

import numpy as np

from . import util
from .schema import (
    GradedParameterSchema,
    ListedParameterSchema,
    ParameterSchema,
    UniformParameterSchema,
)


class Parameter(Sequence):
    name: str
    values: list[Any]

    @staticmethod
    def from_schema(name: str, schema: ParameterSchema) -> Parameter:
        if isinstance(schema, ListedParameterSchema):
            return Parameter(name, schema.values)
        if isinstance(schema, UniformParameterSchema):
            return UniformParameter(name, schema.interval, schema.num)
        if isinstance(schema, GradedParameterSchema):
            return GradedParameter(name, schema.interval, schema.num, schema.grading)
        return None

    def __init__(self, name: str, values: list):
        self.name = name
        self.values = values

    def __len__(self) -> int:
        return len(self.values)

    @overload
    def __getitem__(self, index: int) -> Any: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[Any]: ...

    def __getitem__(self, index: Union[int, slice]) -> Any:
        return self.values[index]


class UniformParameter(Parameter):
    def __init__(self, name: str, interval: tuple[float, float], num: int):
        super().__init__(name, list(np.linspace(*interval, num=num)))


class GradedParameter(Parameter):
    def __init__(self, name: str, interval: tuple[float, float], num: int, grading: float):
        lo, hi = interval
        step = (hi - lo) * (1 - grading) / (1 - grading ** (num - 1))
        values = [lo]
        for _ in range(num - 1):
            values.append(values[-1] + step)
            step *= grading
        super().__init__(name, values)


class ParameterSpace(dict):
    @classmethod
    def from_schema(cls, schema: dict[str, ParameterSchema]) -> ParameterSpace:
        return cls({name: Parameter.from_schema(name, spec) for name, spec in schema.items()})

    def subspace(self, *names: str) -> Iterable[dict]:
        params = [self[name] for name in names]
        yield from util.dict_product(names, params)

    def fullspace(self) -> Iterable[dict]:
        yield from self.subspace(*self.keys())

    def size(self, *names: str) -> int:
        return util.prod(len(self[name]) for name in names)

    def size_fullspace(self) -> int:
        return self.size(*self.keys())
