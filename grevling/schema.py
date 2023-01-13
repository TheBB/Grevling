from __future__ import annotations

from functools import partial
from pathlib import Path

from typing import Any, List, Dict, Optional, Union, Literal, Callable, Tuple

from pydantic import BaseModel, Field

import goldpy as gold                   # type: ignore
import jsonschema                       # type: ignore
import jsonschema.validators            # type: ignore
import yaml

from . import util, api
from .render import render


class RegexCapture(BaseModel):
    pattern: str
    mode: Literal['first', 'last', 'all'] = 'last'

    @staticmethod
    def from_str(pattern: str) -> RegexCapture:
        return RegexCapture(pattern=pattern)


class SimpleCapture(BaseModel):
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


class CommandSchema(BaseModel):
    command: Optional[Union[str, List[str]]]
    name: Optional[str]

    p_capture: Union[
        str,
        SimpleCapture,
        RegexCapture,
        List[
            Union[
                str,
                SimpleCapture,
                RegexCapture,
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
    def capture(self) -> List[Union[SimpleCapture, RegexCapture]]:
        raw_captures = self.p_capture if isinstance(self.p_capture, list) else [self.p_capture]
        return [
            RegexCapture.from_str(pattern) if isinstance(pattern, str) else pattern
            for pattern in raw_captures
        ]


Scalar = Union[int, float]


class UniformParameterSchema(BaseModel):
    kind: Literal['uniform'] = Field(alias='type')
    interval: Tuple[Scalar, Scalar]
    num: int

    class Config:
        smart_union = True


class GradedParameterSchema(BaseModel):
    kind: Literal['graded'] = Field(alias='type')
    interval: Tuple[Scalar, Scalar]
    num: int
    grading: Scalar

    class Config:
        smart_union = True


class PlotCategory(BaseModel):
    mode: Literal['category']
    style: Literal['color', 'line', 'marker']


class PlotIgnore(BaseModel):
    mode: Literal['ignore']
    value: Union[Scalar, str]

    class Config:
        smart_union = True


class PlotStyle(BaseModel):
    color: Optional[Union[str, List[str]]]
    line: Optional[Union[str, List[str]]]
    marker: Optional[Union[str, List[str]]]


PlotModeSchema = Union[
    Literal['fixed', 'variate', 'category', 'ignore', 'mean'],
    PlotCategory,
    PlotIgnore,
]


class PlotStyle(BaseModel):
    color: Optional[Union[str, List[str]]]
    line: Optional[Union[str, List[str]]]
    marker: Optional[Union[str, List[str]]]


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

    style: PlotStyle = PlotStyle()

    @property
    def fmt(self) -> List[str]:
        return self.p_fmt if isinstance(self.p_fmt, list) else [self.p_fmt]

    @property
    def yaxis(self) -> List[str]:
        return self.p_yaxis if isinstance(self.p_yaxis, list) else [self.p_yaxis]


class Settings(BaseModel):
    p_logdir: Union[Callable, str] = Field(alias='logdir', default='${g_index}')
    ignore_missing_files: bool = Field(alias='ignore-missing-files', default=False)

    @property
    def logdir(self) -> Callable[[api.Context], str]:
        if isinstance(self.p_logdir, str):
            return lambda ctx: render(self.p_logdir, ctx)
        return lambda ctx: self.p_logdir(**ctx)


ParameterSchema = Union[
    List[Scalar],
    List[str],
    UniformParameterSchema,
    GradedParameterSchema,
]


Constant = Union[
    str,
    None,
    Scalar,
    bool,
]


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

    @property
    def script(self) -> Callable[[api.Context], List[CommandSchema]]:
        if isinstance(self.p_script, list):
            return lambda ctx: [
                CommandSchema.from_any(schema).render(ctx)
                for schema in self.p_script
            ]
        return lambda ctx: [
            CommandSchema.from_any(schema)
            for schema in self.p_script(**ctx)
        ]

    @property
    def evaluate(self) -> Callable[[api.Context], Dict]:
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

    def _templates_callable(self) -> Callable[[api.Context], List[FileMapSchema]]:
        if isinstance(self.p_templates, list):
            return lambda ctx: [
                TemplateSchema.from_any(schema).to_filemap().render(ctx)
                for schema in self.p_templates
            ]
        return lambda ctx: [
            TemplateSchema.from_any(schema).to_filemap()
            for schema in self.p_templates(**ctx)
        ]

    def _prefiles_callable(self) -> Callable[[api.Context], List[FileMapSchema]]:
        if isinstance(self.p_prefiles, list):
            return lambda ctx: [
                FileMapSchema.from_any(schema).render(ctx)
                for schema in self.p_prefiles
            ]
        return lambda ctx: [
            FileMapSchema.from_any(schema)
            for schema in self.p_prefiles(**ctx)
        ]

    @property
    def prefiles(self) -> Callable[[api.Context], List[FileMapSchema]]:
        prefiles = self._prefiles_callable()
        templates = self._templates_callable()
        return lambda ctx: [*prefiles(ctx), *templates(ctx)]

    @property
    def postfiles(self) -> Callable[[api.Context], List[FileMapSchema]]:
        if isinstance(self.p_postfiles, list):
            return lambda ctx: [
                FileMapSchema.from_any(schema).render(ctx)
                for schema in self.p_postfiles
            ]
        return lambda ctx: [
            FileMapSchema.from_any(schema)
            for schema in self.p_postfiles(**ctx)
        ]


def libfinder(path: str):
    if path != 'grevling':
        return None
    retval = gold.eval_file(str(Path(__file__).parent / 'grevling.gold'))
    retval.update({
        'legendre': util.legendre
    })
    return retval


def load(path: Path) -> CaseSchema:
    if path.suffix == '.yaml':
        with open(path, 'r') as f:
            data = yaml.load(f, Loader=yaml.CLoader)
    else:
        with open(path, 'r') as f:
            src = f.read()
        resolver = gold.ImportConfig(root=str(path.parent), custom=libfinder)
        data = gold.eval(src, resolver)
    l = CaseSchema.parse_obj(data)
    return l
    # validate(data)
    # return normalize(data)
