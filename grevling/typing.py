from abc import ABC, abstractmethod, abstractproperty
from datetime import datetime

from typing import Any, Dict, List as PyList

import pandas as pd

from . import util


class Type(ABC):

    names: PyList[str]
    python: Any
    pandas: Any
    json: Any

    is_list: bool

    @classmethod
    def find(cls, name: str):
        subcls = util.find_subclass(cls, name.lower(), attr='names', predicate=lambda a, b: a in b)
        if not subcls:
            raise ValueError(f"Unknown type: {name}")
        return subcls()

    @abstractmethod
    def coerce(self, value):
        ...

    @abstractmethod
    def coerce_into(self, new, existing):
        ...

    @abstractmethod
    def to_pandas(self, value) -> Any:
        ...

    @abstractmethod
    def to_json(self, value) -> Any:
        ...


class Scalar(Type):

    is_list = False

    def __eq__(self, other):
        if isinstance(other, Scalar):
            return type(self) == type(other)
        return False

    def to_string(self, value) -> str:
        return str(self.coerce(value))

    def from_string(self, value: str) -> Any:
        return self.python(value)

    def coerce(self, value):
        return self.python(value)

    def coerce_into(self, new, existing):
        return self.coerce(new)

    def to_pandas(self, value) -> Any:
        value = self.coerce(value)
        if self.pandas == object:
            return value
        return self.pandas(value)

    def to_json(self, value) -> Any:
        return self.json(self.coerce(value))

    def coerce_into(self, new, existing):
        return self.coerce(new)


class Integer(Scalar):

    names = ['int', 'integer']
    python = int
    pandas = pd.Int64Dtype()
    json = int

    def to_pandas(self, value):
        return self.coerce(value)


class Float(Scalar):

    names = ['float', 'floating', 'double']
    python = float
    pandas = float
    json = float


class String(Scalar):

    names = ['string', 'str']
    python = str
    pandas = object
    json = str


class DateTime(Scalar):

    names = []
    python = datetime
    pandas = 'datetime64[us]'
    json = str

    def coerce(self, value):
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        if isinstance(value, datetime):
            return value
        raise TypeError(f"{type(value)} not coercable to datetime")

    def to_string(self, value) -> str:
        return value.strftime('%Y-%m-%d %H:%M:%S.%f')

    def from_string(self, value) -> str:
        return datetime.strptime(value)

    def to_pandas(self, value):
        return value

    def to_json(self, value):
        return self.to_string(value)


class Boolean(Scalar):

    names = []
    python = bool
    pandas = pd.BooleanDtype()
    json = bool

    def coerce(self, value):
        if isinstance(value, str):
            return bool(int(value))
        return super().coerce(value)

    def to_string(self, value) -> str:
        return str(int(value))

    def from_string(self, value):
        return bool(int(value))

    def to_pandas(self, value):
        return self.coerce(value)


class List(Type):

    eltype: Scalar

    is_list = True

    def __init__(self, eltype: Scalar):
        self.eltype = eltype
        self.json = self.python = PyList[eltype.python]
        self.pandas = object

    def __eq__(self, other):
        if isinstance(other, List):
            return self.eltype == other.eltype
        return False

    def coerce(self, value):
        if isinstance(value, list):
            return [self.eltype.coerce(v) for v in value]
        return [self.eltype.coerce(value)]

    def coerce_into(self, new, existing):
        if isinstance(new, list) and isinstance(existing, list):
            return [*existing, *map(self.eltype.coerce, new)]
        if isinstance(existing, list):
            return [*existing, self.eltype.coerce(new)]
        return self.coerce(new)

    def to_pandas(self, value) -> Any:
        return [self.eltype.to_pandas(v) for v in value]

    def to_json(self, value) -> Any:
        return [self.eltype.to_json(v) for v in value]


class TypeManager(Dict[str, Type]):

    def coerce(self, key, value):
        return self[key].coerce(value)

    def coerce_into(self, key, new, existing):
        return self[key].coerce_into(new, existing)

    def to_pandas(self, key, value):
        return self[key].to_pandas(value)

    def to_json(self, key, value):
        return self[key].to_json(value)
