from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

from grevling import api

Scalar = int | float
Constant = str | None | Scalar | bool


class ListedParameterSchema(BaseModel):
    kind: Literal["listed"]
    values: list[Scalar] | list[str]


class UniformParameterSchema(BaseModel):
    kind: Literal["uniform"]
    interval: tuple[Scalar, Scalar]
    num: int


class GradedParameterSchema(BaseModel):
    kind: Literal["graded"]
    interval: tuple[Scalar, Scalar]
    num: int
    grading: Scalar


ParameterSchema = Annotated[
    ListedParameterSchema | UniformParameterSchema | GradedParameterSchema,
    Field(discriminator="kind"),
]


class FileMapSchema(BaseModel):
    source: str
    target: str | None
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
    SimpleCaptureSchema | RegexCaptureSchema,
    Field(discriminator="capture_type"),
]


class CommandSchema(BaseModel):
    command: str | list[str] | None
    name: str | None
    capture: list[CaptureSchema]
    allow_failure: bool
    retry_on_fail: bool
    env: dict[str, str]
    container: str | None
    container_args: str | list[str]
    workdir: str | None


class PlotModeFixedSchema(BaseModel):
    mode: Literal["fixed"] = "fixed"


class PlotModeVariateSchema(BaseModel):
    mode: Literal["variate"] = "variate"


class PlotModeCategorySchema(BaseModel):
    mode: Literal["category"] = "category"
    argument: Literal["color", "line", "marker"] | None = None


class PlotModeIgnoreSchema(BaseModel):
    mode: Literal["ignore"] = "ignore"
    argument: Scalar | str | None = None


class PlotModeMeanSchema(BaseModel):
    mode: Literal["mean"] = "mean"


PlotModeSchema = Annotated[
    PlotModeFixedSchema
    | PlotModeVariateSchema
    | PlotModeCategorySchema
    | PlotModeIgnoreSchema
    | PlotModeMeanSchema,
    Field(discriminator="mode"),
]


class PlotStyleSchema(BaseModel):
    color: list[str] | None
    line: list[str] | None
    marker: list[str] | None


class PlotSchema(BaseModel):
    filename: str
    fmt: list[str]
    parameters: dict[str, PlotModeSchema]
    xaxis: str | None
    yaxis: list[str]
    kind: Literal["scatter", "line"] | None
    grid: bool
    xmode: Literal["linear", "log"]
    ymode: Literal["linear", "log"]
    xlim: tuple[Scalar, Scalar] | None
    ylim: tuple[Scalar, Scalar] | None
    title: str | None
    xlabel: str | None
    ylabel: str | None
    legend: str | None
    style: PlotStyleSchema


class SettingsSchema(BaseModel):
    storagedir: str
    logdir: Callable[[api.Context], str]
    ignore_missing_files: bool


class PluginSchema(BaseModel):
    name: str
    settings: Any


class CaseSchema(BaseModel):
    parameters: dict[str, ParameterSchema]
    script: Callable[[api.Context], list[CommandSchema]]
    constants: dict[str, Constant]
    evaluate: Callable[[api.Context], dict[str, Any]]
    where: Callable[[api.Context], bool]
    prefiles: Callable[[api.Context], list[FileMapSchema]]
    postfiles: Callable[[api.Context], list[FileMapSchema]]
    types: dict[str, str]
    settings: SettingsSchema
    plots: list[PlotSchema]
    plugins: list[PluginSchema]
