from __future__ import annotations

from functools import partial
from pathlib import Path

from typing import Any, List, Dict, Optional, Union, Literal, Callable, Tuple

from pydantic import BaseModel, Field

from .. import util, api
from ..render import render
from . import refined


Scalar = Union[int, float]


Constant = Union[
    str,
    None,
    Scalar,
    bool,
]


class RegexCaptureSchema(BaseModel):
    capture_type: Literal['regex'] = 'regex'
    pattern: str
    mode: Literal['first', 'last', 'all'] = 'last'

    @staticmethod
    def from_str(pattern: str) -> RegexCaptureSchema:
        return RegexCaptureSchema(pattern=pattern)


class SimpleCaptureSchema(BaseModel):
    capture_type: Literal['simple'] = 'simple'
    kind: Literal['integer', 'float'] = Field(alias='type')
    name: str
    prefix: str
    skip_words: int = 0
    flexible_prefix: bool = False
    mode: Literal['first', 'last', 'all'] = 'last'


class TemplateSchema(BaseModel):
    source: str
    target: Optional[str]
    mode: Literal['simple', 'glob'] = 'simple'
    template: bool = True

    @staticmethod
    def from_any(source: Union[str, TemplateSchema]) -> TemplateSchema:
        if isinstance(source, str):
            return TemplateSchema.parse_obj({'source': source})
        return source

    def to_filemap(self) -> FileMapSchema:
        return FileMapSchema(
            source=self.source,
            target=self.target,
            mode=self.mode,
            template=self.template,
        )


class FileMapSchema(BaseModel):
    source: str
    target: Optional[str]
    mode: Literal['simple', 'glob'] = 'simple'
    template: bool = False

    @staticmethod
    def from_any(source: Union[Dict, str, FileMapSchema]) -> FileMapSchema:
        if isinstance(source, FileMapSchema):
            return source
        if isinstance(source, str):
            return FileMapSchema.parse_obj({'source': source})
        return FileMapSchema.parse_obj(source)

    def render(self, context: api.Context) -> FileMapSchema:
        return self.copy(update={
            'source': render(self.source, context),
            'target': render(self.target, context) if self.target else None,
        })

    def refine(self) -> refined.FileMapSchema:
        return refined.FileMapSchema.parse_obj(self.dict())


class CommandSchema(BaseModel):
    command: Optional[Union[str, List[str]]]
    name: Optional[str]

    p_capture: Union[
        str,
        SimpleCaptureSchema,
        RegexCaptureSchema,
        List[
            Union[
                str,
                SimpleCaptureSchema,
                RegexCaptureSchema,
            ]
        ],
    ] = Field(alias='capture', default=[])

    capture_output: bool = True
    capture_walltime: bool = True
    retry_on_fail: bool = False
    env: Dict[str, str] = {}
    container: Optional[str]
    container_args: Union[str, List[str]] = []
    allow_failure: bool = False
    workdir: Optional[str]

    @staticmethod
    def from_any(source: Union[str, List[str], CommandSchema, Dict]) -> CommandSchema:
        if isinstance(source, CommandSchema):
            return source
        if isinstance(source, Dict):
            return CommandSchema.parse_obj(source)
        return CommandSchema.parse_obj({'command': source})

    def render(self, context: api.Context):
        cmd_render_mode = 'shell' if isinstance(self.command, str) else None
        cargs_render_mode = 'shell' if isinstance(self.container_args, str) else None

        return self.copy(update={
            'command': render(self.command, context, mode=cmd_render_mode),
            'container_args': render(self.container_args, context, mode=cargs_render_mode),
            'workdir': render(self.workdir, context),
            'env': render(self.env, context),
        })

    @property
    def capture(self) -> List[Dict]:
        raw_captures = self.p_capture if isinstance(self.p_capture, list) else [self.p_capture]
        return [
            RegexCaptureSchema.from_str(pattern).dict() if isinstance(pattern, str) else pattern.dict()
            for pattern in raw_captures
        ]

    def refine(self) -> refined.CommandSchema:
        return refined.CommandSchema.parse_obj({
            **self.dict(),
            'capture': self.capture,
        })


class UniformParameterSchema(BaseModel):
    kind: Literal['uniform'] = Field(alias='type')
    interval: Tuple[Scalar, Scalar]
    num: int

    class Config:
        smart_union = True

    def refine(self) -> refined.UniformParameterSchema:
        return refined.UniformParameterSchema.parse_obj(self.dict())


class GradedParameterSchema(BaseModel):
    kind: Literal['graded'] = Field(alias='type')
    interval: Tuple[Scalar, Scalar]
    num: int
    grading: Scalar

    class Config:
        smart_union = True

    def refine(self) -> refined.GradedParameterSchema:
        return refined.GradedParameterSchema.parse_obj(self.dict())


ParameterSchema = Union[
    List[Scalar],
    List[str],
    UniformParameterSchema,
    GradedParameterSchema,
]


class PlotCategorySchema(BaseModel):
    mode: Literal['category']
    argument: Optional[Literal['color', 'line', 'marker']] = Field(alias='style')


class PlotIgnoreSchema(BaseModel):
    mode: Literal['ignore']
    argument: Optional[Union[Scalar, str]] = Field(alias='value')

    class Config:
        smart_union = True


PlotModeSchema = Union[
    Literal['fixed', 'variate', 'category', 'ignore', 'mean'],
    PlotCategorySchema,
    PlotIgnoreSchema,
]


class PlotStyleSchema(BaseModel):
    color: Optional[Union[str, List[str]]] = None
    line: Optional[Union[str, List[str]]] = None
    marker: Optional[Union[str, List[str]]] = None

    def refine(self) -> refined.PlotStyleSchema:
        fix = lambda x: [x] if isinstance(x, str) else x
        return refined.PlotStyleSchema.parse_obj({
            'color': fix(self.color),
            'line': fix(self.line),
            'marker': fix(self.marker),
        })


class PlotSchema(BaseModel):

    class Config:
        smart_union = True

    filename: str
    p_fmt: Union[str, List[str]] = Field(alias='format')

    xaxis: Optional[str]
    p_yaxis: Union[str, List[str]] = Field(alias='yaxis')

    ylim: Optional[Tuple[Scalar, Scalar]]
    xlim: Optional[Tuple[Scalar, Scalar]]
    kind: Literal['scatter', 'line'] = Field(alias='type', default='line')
    legend: Optional[str]
    xlabel: Optional[str]
    ylabel: Optional[str]
    xmode: Literal['linear', 'log'] = 'linear'
    ymode: Literal['linear', 'log'] = 'linear'
    title: Optional[str]
    grid: bool = True

    parameters: Dict[str, PlotModeSchema] = {}

    style: PlotStyleSchema = PlotStyleSchema()

    @property
    def fmt(self) -> List[str]:
        return self.p_fmt if isinstance(self.p_fmt, list) else [self.p_fmt]

    @property
    def yaxis(self) -> List[str]:
        return self.p_yaxis if isinstance(self.p_yaxis, list) else [self.p_yaxis]

    def refine(self) -> refined.PlotSchema:
        parameters = {
            name: {'mode': value} if isinstance(value, str) else value
            for name, value in self.parameters.items()
        }
        return refined.PlotSchema.parse_obj({
            **self.dict(),
            'fmt': self.fmt,
            'yaxis': self.yaxis,
            'parameters': parameters,
            'style': self.style.refine(),
        })


class Settings(BaseModel):
    p_logdir: Union[Callable, str] = Field(alias='logdir', default='${g_index}')
    ignore_missing_files: bool = Field(alias='ignore-missing-files', default=False)

    @property
    def logdir(self) -> Callable[[api.Context], str]:
        if isinstance(self.p_logdir, str):
            def renderer(ctx: api.Context) -> str:
                return render(self.p_logdir, ctx)
            return renderer
        elif callable(self.p_logdir):
            return lambda ctx: self.p_logdir(**ctx)

    def refine(self) -> refined.SettingsSchema:
        return refined.SettingsSchema.parse_obj({
            **self.dict(),
            'logdir': self.logdir,
        })


class CaseSchema(BaseModel):

    class Config:
        smart_union = True

    parameters: Dict[str, ParameterSchema] = {}

    p_script: Union[
        Callable,
        List[
            Union[
                str,
                List[str],
                CommandSchema,
            ]
        ],
    ] = Field(alias='script', default=[])

    p_containers: Dict[str, Union[str, List[str]]] = Field(alias='containers', default={})
    p_evaluate: Union[Callable, Dict[str, str]] = Field(alias='evaluate', default={})
    constants: Dict[str, Constant] = {}
    p_where: Union[Callable, str, List[str]] = Field(alias='where', default=[])
    types: Dict[str, str] = {}

    p_templates: Union[Callable, List[Union[str, TemplateSchema]]] = Field(alias='templates', default=[])
    p_prefiles: Union[Callable, List[Union[str, FileMapSchema]]] = Field(alias='prefiles', default=[])
    p_postfiles: Union[Callable, List[Union[str, FileMapSchema]]] = Field(alias='postfiles', default=[])

    plots: List[PlotSchema] = []

    settings: Settings = Settings()

    def refine(self) -> refined.CaseSchema:
        parameters = {}
        for name, schema in self.parameters.items():
            if isinstance(schema, list):
                parameters[name] = {
                    'kind': 'listed',
                    'values': schema,
                }
            else:
                parameters[name] = schema.refine()

        obj = self.dict()
        obj.update({
            'parameters': parameters,
            'script': self.script,
            'evaluate': self.evaluate,
            'where': self.where,
            'prefiles': self.prefiles,
            'postfiles': self.postfiles,
            'settings': self.settings.refine(),
            'plots': [plot.refine() for plot in self.plots],
        })

        return refined.CaseSchema.parse_obj(obj)

    @property
    def script(self) -> Callable[[api.Context], List[refined.CommandSchema]]:
        if isinstance(self.p_script, list):
            return lambda ctx: [
                CommandSchema.from_any(schema).render(ctx).refine()
                for schema in self.p_script
            ]
        return lambda ctx: [
            CommandSchema.from_any(schema).refine()
            for schema in self.p_script(**ctx)
        ]

    @property
    def evaluate(self) -> Callable[[api.Context], Dict[str, Any]]:
        if isinstance(self.p_evaluate, dict):
            return partial(util.evaluate, evaluables=self.p_evaluate)
        return lambda ctx: self.p_evaluate(**ctx)

    @property
    def where(self) -> Callable[[api.Context], bool]:
        if isinstance(self.p_where, str):
            return partial(util.all_truthy, conditions=[self.p_where])
        if isinstance(self.p_where, list):
            return partial(util.all_truthy, conditions=self.p_where)
        return lambda ctx: self.p_where(**ctx)

    @staticmethod
    def _filemap(schemas, schema_converter):
        if isinstance(schemas, list):
            return lambda ctx: [
                schema_converter(schema).render(ctx).refine() for schema in schemas
            ]
        return lambda ctx: [
            schema_converter(schema).refine() for schema in schemas(**ctx)
        ]

    def _templates_callable(self) -> Callable[[api.Context], List[FileMapSchema]]:
        return CaseSchema._filemap(
            self.p_templates,
            lambda schema: TemplateSchema.from_any(schema).to_filemap()
        )

    def _prefiles_callable(self) -> Callable[[api.Context], List[FileMapSchema]]:
        return CaseSchema._filemap(
            self.p_prefiles,
            lambda schema: FileMapSchema.from_any(schema)
        )

    @property
    def prefiles(self) -> Callable[[api.Context], List[FileMapSchema]]:
        prefiles = self._prefiles_callable()
        templates = self._templates_callable()
        return lambda ctx: [*prefiles(ctx), *templates(ctx)]

    @property
    def postfiles(self) -> Callable[[api.Context], List[FileMapSchema]]:
        return CaseSchema._filemap(
            self.p_postfiles,
            lambda schema: FileMapSchema.from_any(schema)
        )
