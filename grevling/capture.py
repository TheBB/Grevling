from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from . import api, typing, util
from .schema import RegexCaptureSchema, SimpleCaptureSchema

if TYPE_CHECKING:
    from collections.abc import Iterable

    import pandas as pd

    from .typing import GType, TypeManager


class Capture:
    _regex: re.Pattern
    _mode: str
    _type: Optional[GType]

    @staticmethod
    def from_schema(schema: Union[RegexCaptureSchema, SimpleCaptureSchema]) -> Capture:
        if isinstance(schema, RegexCaptureSchema):
            return Capture(
                pattern=schema.pattern,
                mode=schema.mode,
            )

        pattern = {
            "integer": r"[-+]?[0-9]+",
            "float": r"[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?",
        }[schema.kind]
        skip = r"(\S+\s+){" + str(schema.skip_words) + "}"
        if schema.flexible_prefix:
            prefix = r"\s+".join(re.escape(p) for p in schema.prefix.split())
        else:
            prefix = re.escape(schema.prefix)
        pattern = prefix + r"\s*[:=]?\s*" + skip + "(?P<" + schema.name + ">" + pattern + ")"
        mode = schema.mode
        tp = typing.GType.from_string(schema.kind)
        return Capture(pattern, mode, tp)

    @classmethod
    def load(cls, spec: Any) -> Capture:
        if isinstance(spec, str):
            return cls(spec)
        if spec.get("type") in ("integer", "float"):
            pattern = {
                "integer": r"[-+]?[0-9]+",
                "float": r"[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?",
            }[spec["type"]]
            skip = r"(\S+\s+){" + str(spec.get("skip-words", 0)) + "}"
            if spec.get("flexible-prefix", False):
                prefix = r"\s+".join(re.escape(p) for p in spec["prefix"].split())
            else:
                prefix = re.escape(spec["prefix"])
            pattern = prefix + r"\s*[:=]?\s*" + skip + "(?P<" + spec["name"] + ">" + pattern + ")"
            mode = spec.get("mode", "last")
            tp = typing.GType.from_string(spec["type"])
            return cls(pattern, mode, tp)
        return cast(Capture, util.call_yaml(cls, spec))

    def __init__(
        self,
        pattern: str,
        mode: str = "last",
        tp: Optional[GType] = None,
    ) -> None:
        self._regex = re.compile(pattern)
        self._mode = mode
        self._type = tp

    def find_in(self, collector: CaptureCollection, string: str) -> None:
        matches = self._regex.finditer(string)
        filtered: Iterable[re.Match]

        if self._mode == "first":
            try:
                filtered = [next(matches)]
            except StopIteration:
                return
            tp = self._type

        elif self._mode == "last":
            try:
                match = next(matches)
            except StopIteration:
                return
            for match in matches:
                pass
            filtered = [match]
            tp = self._type

        else:
            tp = typing.List(self._type) if self._type else None
            filtered = list(matches)

        for match in filtered:
            for name, value in match.groupdict().items():
                collector.collect(name, value, tp=tp)


class CaptureCollection(api.Context):
    types: TypeManager

    def __init__(self, types: TypeManager):
        self.types = types

    def collect(self, name: str, value: Any, tp: Optional[GType] = None) -> None:
        gtp: GType = tp if tp is not None else self.types.get(name, typing.AnyType())
        value = gtp.coerce(value)
        if gtp.is_list:
            self.setdefault(name, []).append(value)
        else:
            self[name] = value

    def collect_from_file(self, ws: api.Workspace, filename: str) -> None:
        with ws.open_str(filename, "r") as f:
            data = json.load(f)
        self.update(data)

    def collect_from_context(self, ws: api.Workspace) -> None:
        self.collect_from_file(ws, "context.json")

    def collect_from_cache(self, ws: api.Workspace) -> None:
        self.collect_from_file(ws, "captured.json")

    def collect_from_info(self, ws: api.Workspace) -> None:
        with ws.open_str("grevling.txt", "r") as f:
            for line in f:
                key, value = line.strip().split("=", 1)
                self.collect(key, value)

    def commit_to_file(self, ws: api.Workspace) -> None:
        with ws.open_str("captured.json", "w") as f:
            f.write(self.json())

    def commit_to_dataframe(self, data: pd.DataFrame) -> pd.DataFrame:
        index = self["g_index"]
        data.loc[index, :] = [None] * data.shape[1]
        for key, value in self.items():
            if key == "g_index":
                continue
            data.at[index, key] = value
        return data
