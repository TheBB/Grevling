from __future__ import annotations

from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated

from .. import api

Scalar = Union[int, float]
Constant = Union[str, None, Scalar, bool]


class ListedParameterSchema(BaseModel):
    kind: Literal["listed"]
    values: Union[
        List[Scalar],
        List[str],
    ]


class UniformParameterSchema(BaseModel):
    kind: Literal["uniform"]
    interval: Tuple[Scalar, Scalar]
    num: int


class GradedParameterSchema(BaseModel):
    kind: Literal["graded"]
    interval: Tuple[Scalar, Scalar]
    num: int
    grading: Scalar


ParameterSchema = Annotated[
    Union[
        ListedParameterSchema,
        UniformParameterSchema,
        GradedParameterSchema,
    ],
    Field(discriminator="kind"),
]


class FileMapSchema(BaseModel):
    source: str
    target: Optional[str]
    mode: Literal["simple", "glob"]
    template: bool


class SimpleCaptureSchema(BaseModel):
    capture_type: Literal["simple"]
    kind: Literal["integer", "float"]
    name: str
    prefix: str
    skip_words: int
    flexible_prefix: bool
    mode: Literal["first", "last", "all"]


class RegexCaptureSchema(BaseModel):
    capture_type: Literal["regex"]
    pattern: str
    mode: Literal["first", "last", "all"]


CaptureSchema = Annotated[
    Union[
        SimpleCaptureSchema,
        RegexCaptureSchema,
    ],
    Field(discriminator="capture_type"),
]


class CommandSchema(BaseModel):
    command: Optional[Union[str, List[str]]]
    name: Optional[str]
    capture: List[CaptureSchema]
    allow_failure: bool
    retry_on_fail: bool
    env: Dict[str, str]
    container: Optional[str]
    container_args: Union[str, List[str]]
    workdir: Optional[str]


class PlotModeFixedSchema(BaseModel):
    mode: Literal["fixed"] = "fixed"


class PlotModeVariateSchema(BaseModel):
    mode: Literal["variate"] = "variate"


class PlotModeCategorySchema(BaseModel):
    mode: Literal["category"] = "category"
    argument: Optional[Literal["color", "line", "marker"]] = None


class PlotModeIgnoreSchema(BaseModel):
    mode: Literal["ignore"] = "ignore"
    argument: Optional[Union[Scalar, str]] = None


class PlotModeMeanSchema(BaseModel):
    mode: Literal["mean"] = "mean"


PlotModeSchema = Annotated[
    Union[
        PlotModeFixedSchema,
        PlotModeVariateSchema,
        PlotModeCategorySchema,
        PlotModeIgnoreSchema,
        PlotModeMeanSchema,
    ],
    Field(discriminator="mode"),
]


class PlotStyleSchema(BaseModel):
    color: Optional[List[str]]
    line: Optional[List[str]]
    marker: Optional[List[str]]


class PlotSchema(BaseModel):
    filename: str
    fmt: List[str]
    parameters: Dict[str, PlotModeSchema]
    xaxis: Optional[str]
    yaxis: List[str]
    kind: Optional[Literal["scatter", "line"]]
    grid: bool
    xmode: Literal["linear", "log"]
    ymode: Literal["linear", "log"]
    xlim: Optional[Tuple[Scalar, Scalar]]
    ylim: Optional[Tuple[Scalar, Scalar]]
    title: Optional[str]
    xlabel: Optional[str]
    ylabel: Optional[str]
    legend: Optional[str]
    style: PlotStyleSchema


class SettingsSchema(BaseModel):
    storagedir: str
    logdir: Callable[[api.Context], str]
    ignore_missing_files: bool


class CaseSchema(BaseModel):
    parameters: Dict[str, ParameterSchema]
    script: Callable[[api.Context], List[CommandSchema]]
    constants: Dict[str, Constant]
    evaluate: Callable[[api.Context], Dict[str, Any]]
    where: Callable[[api.Context], bool]
    prefiles: Callable[[api.Context], list[FileMapSchema]]
    postfiles: Callable[[api.Context], list[FileMapSchema]]
    types: Dict[str, str]
    settings: SettingsSchema
    plots: List[PlotSchema]
