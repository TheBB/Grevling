from __future__ import annotations

from datetime import datetime
from enum import Enum
import json
from pathlib import Path

from typing import Any, Dict, Optional, Type

from dataclasses import dataclass
import pandas as pd
from pydantic import create_model, PrivateAttr
from pydantic.main import BaseModel

from . import util, api, capture


TYPES: Dict[str, Type] = {
    'int': int,
    'integer': int,
    'float': float,
    'floating': float,
    'double': float,
    'str': str,
    'string': str,
}


PANDAS = {
    int: pd.Int64Dtype(),
    float: float,
    str: object,
    datetime: 'datetime64[us]',
    bool: pd.BooleanDtype(),
}


def find_type(name: str) -> Type:
    try:
        return TYPES[name.lower()]
    except KeyError:
        raise ValueError(f"Unknown type {name}")


class Stage(Enum):
    pre = 'pre'
    post = 'post'


@dataclass
class Field:
    tp: Type
    default: Optional[Any]
    stage: Stage


class TypeManager(Dict[str, Field]):

    _context_model = None
    _capture_model = None

    def add(self, name: str, tp: Type, stage: str):
        default = [] if util.is_list_type(tp) else None
        self[name] = Field(tp, default, Stage(stage))

    def context_model(self) -> Type[api.Context]:
        if self._context_model is None:
            kwargs = {k: (v.tp, v.default) for k, v in self.items() if v.stage == Stage.pre}
            self._context_model = create_model(
                'GrevlingContext',
                __base__=api.Context,
                **kwargs,
            )
        return self._context_model

    def capture_model(self) -> api.Capture:
        if self._capture_model is None:
            kwargs = {k: (v.tp, v.default) for k, v in self.items()}
            self._capture_model = create_model(
                'GrevlingCapture',
                __base__=capture.CaptureCollection,
                **kwargs,
            )
        return self._capture_model()

    def pandas(self) -> Dict:
        retval = {}
        for name, value in self.items():
            try:
                retval[name] = PANDAS[value.tp]
                continue
            except KeyError:
                pass

            if util.is_list_type(value.tp):
                retval[name] = object
                continue
        return retval


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
