from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
import json
from pathlib import Path

from typing import Any, Dict, Optional, Type

import pandas as pd                 # type: ignore
import peewee
from pydantic import PrivateAttr
from pydantic.main import BaseModel

from . import api


class GType(ABC):

    pandas_type: object
    is_list: bool = False

    @staticmethod
    def from_json(data: Any) -> GType:
        if isinstance(data, str):
            return TYPES[data]
        if isinstance(data, dict) and data['type'] == 'list':
            return List(GType.from_json(data['eltype']))
        assert False

    @staticmethod
    def from_string(name: str) -> GType:
        return TYPES[name]

    @staticmethod
    def from_obj(obj: Any) -> GType:
        if isinstance(obj, bool):
            return Boolean()
        if isinstance(obj, str):
            return String()
        if isinstance(obj, int):
            return Integer()
        if isinstance(obj, float):
            return Floating()
        if isinstance(obj, datetime):
            return Datetime()
        if isinstance(obj, Sequence):
            if obj:
                return List(GType.from_obj(obj[0]))
            return List(AnyType())
        raise TypeError(f"unknown type: {type(obj)}")

    def merge_object(self, other: Any) -> GType:
        return self.merge(GType.from_obj(other))

    @abstractmethod
    def merge(self, other: GType) -> GType:
        ...

    @abstractmethod
    def sqlite(self, name: str, model: Type[peewee.Model]):
        ...

    @abstractmethod
    def to_json(self):
        ...

    def coerce(self, obj: Any):
        return obj


class AnyType(GType):

    pandas_type = object

    def merge(self, other: GType) -> GType:
        return other

    def sqlite(self, name: str, model: Type[peewee.Model]):
        assert False

    def to_json(self):
        return 'any'


class String(GType):

    pandas_type = object

    def merge(self, other: GType) -> GType:
        if isinstance(other, (String, AnyType)):
            return self
        if isinstance(other, Datetime):
            return other
        if isinstance(other, Boolean):
            return other
        raise TypeError(f"merge {self} with {other}")

    def sqlite(self, name: str, model: Type[peewee.Model]):
        model._meta.add_field(name, peewee.TextField(null=False))

    def to_json(self):
        return 'string'


class Integer(GType):

    pandas_type = pd.Int64Dtype()

    def merge(self, other: GType) -> GType:
        if isinstance(other, (Integer, AnyType)):
            return self
        if isinstance(other, Floating):
            return other
        raise TypeError(f"merge {self} with {other}")

    def coerce(self, other: Any) -> int:
        if isinstance(other, str):
            return int(other)
        raise TypeError(f"can't coerce to int: {type(other)}")

    def sqlite(self, name: str, model: Type[peewee.Model]):
        model._meta.add_field(name, peewee.IntegerField(null=False))

    def to_json(self):
        return 'integer'


class Floating(GType):

    pandas_type = float

    def merge(self, other: GType) -> GType:
        if isinstance(other, (Integer, Floating, AnyType)):
            return self
        raise TypeError(f"merge {self} with {other}")

    def coerce(self, other: Any) -> float:
        if isinstance(other, (str, int)):
            return float(other)
        raise TypeError(f"can't coerce to float: {type(other)}")

    def sqlite(self, name: str, model: Type[peewee.Model]):
        model._meta.add_field(name, peewee.FloatField(null=False))

    def to_json(self):
        return 'floating'


class Boolean(GType):

    pandas_type = pd.BooleanDtype()

    def merge(self, other: GType) -> GType:
        if isinstance(other, (Boolean, String, AnyType)):
            return self
        raise TypeError(f"merge {self} with {other}")

    def coerce(self, other: Any) -> bool:
        if isinstance(other, str):
            other = int(other)
        if isinstance(other, int):
            return bool(other)
        raise TypeError(f"can't coerce to bool: {type(other)}")

    def sqlite(self, name: str, model: Type[peewee.Model]):
        model._meta.add_field(name, peewee.BooleanField(null=False))

    def to_json(self):
        return 'boolean'


class Datetime(GType):

    pandas_type = 'datetime64[us]'

    def merge(self, other: GType) -> GType:
        if isinstance(other, (Datetime, String, AnyType)):
            return self
        raise TypeError(f"merge {self} with {other}")

    def sqlite(self, name: str, model: Type[peewee.Model]):
        model._meta.add_field(name, peewee.DateTimeField(null=False))

    def to_json(self):
        return 'datetime'


class List(GType):

    eltype: GType

    pandas_type = object
    is_list = True

    def __init__(self, eltype: GType):
        self.eltype = eltype

    def merge(self, other: GType) -> GType:
        if isinstance(other, List):
            return List(self.eltype.merge(other.eltype))
        raise TypeError(f"merge {self} with {other}")

    def coerce(self, value: Any):
        return self.eltype.coerce(value)

    def sqlite(self, name: str, model: Type[peewee.Model]):
        RelatedModel = peewee.ModelBase(f'Related_{name}', (peewee.Model,), {
            'Meta': type('Meta', (), {
                'table_name': name,
            })
        })
        RelatedModel._meta.add_field('index', peewee.ForeignKeyField(model, null=False, backref=name))
        self.eltype.sqlite('value', RelatedModel)

    def to_json(self):
        return {
            'type': 'list',
            'eltype': self.eltype.to_json(),
        }


TYPES: Dict[str, GType] = {
    'int': Integer(),
    'integer': Integer(),
    'float': Floating(),
    'floating': Floating(),
    'double': Floating(),
    'str': String(),
    'string': String(),
    'any': AnyType(),
    'boolean': Boolean(),
    'datetime': Datetime(),
}


class PeeweeModel(peewee.Model):

    types: TypeManager

    @classmethod
    def models(cls):
        yield cls
        yield from cls._meta.backrefs.values()

    @classmethod
    def related(cls):
        for field, model in cls._meta.backrefs.items():
            yield field.backref, model


class TypeManager(Dict[str, GType]):

    @staticmethod
    def from_json(data: Dict) -> TypeManager:
        return TypeManager({k: GType.from_json(v) for k, v in data.items()})

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self['g_index'] = Integer()
        self['g_logdir'] = String()
        self['g_started'] = Datetime()
        self['g_finished'] = Datetime()
        self['g_success'] = Boolean()

    def __getitem__(self, key: str) -> GType:
        if key.startswith('g_walltime'):
            return Floating()
        return super().__getitem__(key)

    def get(self, key, default):
        if key.startswith('g_walltime'):
            return Floating()
        return super().get(key, default)

    def merge(self, data: Dict):
        for k, t in data.items():
            self[k] = self.get(k, AnyType()).merge_object(t)

    def pandas(self) -> Dict:
        return {k: t.pandas_type for k, t in self.items()}

    def sqlite_model(self) -> Type[peewee.Model]:
        DynamicModel = peewee.ModelBase('GrevlingModel', (PeeweeModel,), {
            'types': self,
            'Meta': type('Meta', (), {
                'table_name': 'data',
            }),
        })
        for k, t in self.items():
            t.sqlite(k, DynamicModel)
        return DynamicModel

    def fill_string(self, data: Dict[str, str]):
        for name, typename in data.items():
            self[name] = GType.from_string(typename)

    def fill_obj(self, data: Dict[str, str]):
        for name, typename in data.items():
            self[name] = GType.from_obj(typename)

    def to_json(self) -> Dict:
        return {k: v.to_json() for k, v in self.items()}


class PersistentObject(BaseModel):

    _path: Path = PrivateAttr()

    def __init__(self, path: api.PathStr):
        path = Path(path)
        if path.exists():
            with open(path, 'r') as f:
                data = json.load(f)
            super().__init__(**data)
        else:
            super().__init__()
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        with open(self._path, 'w') as f:
            f.write(self.json())
