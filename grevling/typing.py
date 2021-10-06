from __future__ import annotations

from abc import ABC, abstractclassmethod, abstractmethod
from datetime import datetime
from enum import Enum, IntEnum
import json
from pathlib import Path

from typing import Any, Dict, List as PyList, Optional, get_type_hints

import pandas as pd
from typing_inspect import get_origin, get_args

from . import util, api


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

    def coerce(self, value):
        if isinstance(value, bool):
            return str(int(value))
        return super().coerce(value)


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


class Enumeration(Scalar):

    names = []
    pandas = str
    json = str

    def __init__(self, cls):
        self.python = cls

    def coerce(self, value):
        if isinstance(value, self.python):
            return value
        if isinstance(value, str):
            return self.python[value]
        if isinstance(value, int) and issubclass(self.python, IntEnum):
            return self.python(value)
        raise ValueError(f"Unable to coerce {value} to {self.python}")

    def to_string(self, value) -> str:
        return value.name

    def from_string(self, value):
        return self.python[value]

    def to_json(self, value):
        return value.name

    def to_pandas(self, value):
        return value.name



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

    def __eq__(self, other):
        if isinstance(other, Object):
            return self.types == other.types
        return False

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
            coerced_data = {
                k: self.types.coerce(k, v) for k, v in value.items()
                if k != '_version'
            }
            if '_version' in value:
                coerced_data['_version'] = value['_version']
            return self.python.from_rawdata(coerced_data)
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
        temptype = super().__new__(cls, name, bases, attrs)
        annotations = get_type_hints(temptype)

        types = TypeManager({
            k: Type.find_python(v) for k, v in annotations.items()
            if not k.startswith('_')
        })
        gtype = attrs['_type'] = Object(types)
        pytype = super().__new__(cls, name, bases, attrs)
        gtype.python = pytype
        return pytype


class TypedObject(metaclass=TypedObjectMeta):

    _type: Object
    _data: Dict
    _version: int = 1

    @classmethod
    def upgrade_data(cls, from_version: int, data: Dict) -> Dict:
        return data

    @classmethod
    def from_rawdata(cls, data):
        obj = cls.__new__(cls)
        version = data.pop('_version', cls._version)
        while version < cls._version:
            data = cls.upgrade_data(version, data)
            version += 1
        obj._data = data
        obj._fill_in_defaults()
        return obj

    def __init__(self, data: Optional[Dict] = None, **kwargs):
        self._data = {}
        data = {**(data or {}), **kwargs}
        if data:
            version = data.pop('_version', self._version)
            while version < self._version:
                data = self.upgrade_data(version, data)
                version += 1
            self._data = self._type.coerce_into(data, {})
        self._fill_in_defaults()

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

    def _fill_in_defaults(self):
        for key, tp in self._type.types.items():
            if hasattr(self, key):
                continue
            if hasattr(self, '_defaults') and key in self._defaults:
                setattr(self, key, self._defaults[key])
            elif isinstance(tp, Object):
                setattr(self, key, tp.python())


class PersistentObject(TypedObject):

    _path: Path

    def __init__(self, path: api.PathStr):
        path = Path(path)
        if path.exists():
            with open(path, 'r') as f:
                data = json.load(f)
            super().__init__(data)
        else:
            super().__init__()
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        with open(self._path, 'w') as f:
            json.dump(self.to_json(), f)
