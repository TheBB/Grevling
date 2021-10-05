from __future__ import annotations

from abc import ABC, abstractclassmethod, abstractmethod
from datetime import datetime
import json
from pathlib import Path

from typing import Any, Dict, List as PyList, get_origin, get_args, Optional

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

    @classmethod
    def find_python(cls, pytype):
        for subclass in util.subclasses(cls):
            retval = subclass.fits_python(pytype)
            if retval is not None:
                return retval
        raise TypeError(f"Unknown python type: {pytype}")

    @abstractclassmethod
    def fits_python(cls, pytype):
        ...

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

    @classmethod
    def fits_python(cls, pytype):
        if hasattr(cls, 'python') and cls.python == pytype:
            return cls()
        return None

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

    @classmethod
    def fits_python(cls, pytype):
        if get_origin(pytype) != list:
            return None
        arg, = get_args(pytype)
        eltype = Type.fits_python(arg)
        if not eltype:
            return None
        return cls(eltype)

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


class Object(Type):

    types: TypeManager
    python: type
    pandas: object
    json: dict

    def __init__(self, types: TypeManager):
        self.types = types

    def __contains__(self, key):
        return key in self.types

    @classmethod
    def fits_python(cls, pytype):
        if issubclass(pytype, TypedObject):
            return pytype._type
        return None

    def coerce(self, value):
        if isinstance(value, self.python):
            return value
        if isinstance(value, dict):
            value.pop('_version', None)
            return self.python.from_rawdata({k: self.types.coerce(k, v) for k, v in value.items()})
        raise TypeError(f"Unable to coerce {type(value)} to object")

    def coerce_into(self, new, existing):
        new_data = self.coerce(new)._data
        return {**existing, **new_data}

    def to_json(self, value):
        obj = self.coerce(value)
        data = {k: self.types.to_json(k, v) for k, v in obj._data.items()}
        data['_version'] = obj._version
        return data

    def to_pandas(self, value):
        assert False


class TypedObjectMeta(type):

    def __new__(cls, name, bases, attrs):
        types = TypeManager({
            k: Type.find_python(v) for k, v in attrs.get('__annotations__', {}).items()
            if not k.startswith('_')
        })
        gtype = attrs['_type'] = Object(types)
        pytype = super().__new__(cls, name, bases, attrs)
        gtype.python = pytype
        return pytype


class TypedObject(metaclass=TypedObjectMeta):

    _type: Object
    _data: Dict
    _version: int

    @classmethod
    def upgrade_data(cls, from_version: int, data: Dict) -> Dict:
        return data

    @classmethod
    def from_rawdata(cls, data):
        obj = cls.__new__(cls)
        obj._data = data
        return obj

    def __init__(self, data: Optional[Dict] = None):
        self._data = {}
        if data is not None:
            data.pop('_version', None)
            self._data = self._type.coerce_into(data, {})

    def __getattr__(self, key):
        if key in self._type:
            try:
                return self._data[key]
            except KeyError:
                raise AttributeError(key)
        return super().__getattr__(key)

    def __setattr__(self, key, value):
        if key in self._type:
            self._data = self._type.coerce_into({key: value}, self._data)
            return
        return super().__setattr__(key, value)

    def to_json(self):
        return self._type.to_json(self)


class PersistentObject(TypedObject):

    _path: Path

    def __init__(self, path: Path):
        if path.exists():
            with open(path, 'r') as f:
                data = json.load(f)
            version = data.pop('_version')
            while version < self._version:
                data = self.upgrade_data(version, data)
                version += 1
            super().__init__(data)
        else:
            super().__init__()
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        with open(self._path, 'w') as f:
            json.dump(self.to_json(), f)
