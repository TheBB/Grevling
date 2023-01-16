from typing import Union, List, Literal, Tuple, Dict, Callable, Optional, Any
from typing_extensions import Annotated

from pydantic import BaseModel, Field

from .. import api


Scalar = Union[int, float]
Constant = Union[str, None, Scalar, bool]


class ListedParameterSchema(BaseModel, allow_mutation=False, smart_union=True):
    kind: Literal['listed']
    values: Union[
        List[Scalar],
        List[str],
    ]


class UniformParameterSchema(BaseModel, allow_mutation=False, smart_union=True):
    kind: Literal['uniform']
    interval: Tuple[Scalar, Scalar]
    num: int


class GradedParameterSchema(BaseModel, allow_mutation=False, smart_union=True):
    kind: Literal['graded']
    interval: Tuple[Scalar, Scalar]
    num: int
    grading: Scalar


ParameterSchema = Annotated[
    Union[
        ListedParameterSchema,
        UniformParameterSchema,
        GradedParameterSchema,
    ],
    Field(discriminator='kind'),
]


class FileMapSchema(BaseModel, allow_mutation=False):
    source: str
    target: Optional[str]
    mode: Literal['simple', 'glob']
    template: bool


class SimpleCaptureSchema(BaseModel, allow_mutation=False):
    capture_type: Literal['simple']
    kind: Literal['integer', 'float']
    name: str
    prefix: str
    skip_words: int
    flexible_prefix: bool
    mode: Literal['first', 'last', 'all']


class RegexCaptureSchema(BaseModel, allow_mutation=False):
    capture_type: Literal['regex']
    pattern: str
    mode: Literal['first', 'last', 'all']


CaptureSchema = Annotated[
    Union[
        SimpleCaptureSchema,
        RegexCaptureSchema,
    ],
    Field(discriminator='capture_type'),
]


class CommandSchema(BaseModel, allow_mutation=False):
    command: Optional[Union[str, List[str]]]
    name: Optional[str]
    capture: List[CaptureSchema]
    allow_failure: bool
    retry_on_fail: bool
    env: Dict[str, str]
    container: Optional[str]
    container_args: Union[str, List[str]]
    workdir: Optional[str]


class PlotModeFixedSchema(BaseModel, allow_mutation=False):
    mode: Literal['fixed'] = 'fixed'


class PlotModeVariateSchema(BaseModel, allow_mutation=False):
    mode: Literal['variate'] = 'variate'


class PlotModeCategorySchema(BaseModel, allow_mutation=False):
    mode: Literal['category'] = 'category'
    argument: Optional[Literal['color', 'line', 'marker']]


class PlotModeIgnoreSchema(BaseModel, allow_mutation=False):
    mode: Literal['ignore'] = 'ignore'
    argument: Optional[Union[Scalar, str]]


class PlotModeMeanSchema(BaseModel, allow_mutation=False):
    mode: Literal['mean'] = 'mean'


PlotModeSchema = Annotated[
    Union[
        PlotModeFixedSchema,
        PlotModeVariateSchema,
        PlotModeCategorySchema,
        PlotModeIgnoreSchema,
        PlotModeMeanSchema,
    ],
    Field(discriminator='mode'),
]


class PlotStyleSchema(BaseModel, allow_mutation=False):
    color: Optional[List[str]]
    line: Optional[List[str]]
    marker: Optional[List[str]]


class PlotSchema(BaseModel, allow_mutation=False, smart_union=True):
    filename: str
    fmt: List[str]
    parameters: Dict[str, PlotModeSchema]
    xaxis: Optional[str]
    yaxis: List[str]
    kind: Optional[Literal['scatter', 'line']]
    grid: bool
    xmode: Literal['linear', 'log']
    ymode: Literal['linear', 'log']
    xlim: Optional[Tuple[Scalar, Scalar]]
    ylim: Optional[Tuple[Scalar, Scalar]]
    title: Optional[str]
    xlabel: Optional[str]
    ylabel: Optional[str]
    legend: Optional[str]
    style: PlotStyleSchema


class SettingsSchema(BaseModel, allow_mutation=False):
    logdir: Callable[[api.Context], str]
    ignore_missing_files: bool


class CaseSchema(BaseModel, allow_mutation=False, smart_union=True):
    parameters: Dict[str, ParameterSchema]
    script: Callable[[api.Context], List[CommandSchema]]
    constants: Dict[str, Constant]
    evaluate: Callable[[api.Context], Dict[str, Any]]
    where: Callable[[api.Context], bool]
    prefiles: Callable[[api.Context], FileMapSchema]
    postfiles: Callable[[api.Context], FileMapSchema]
    types: Dict[str, str]
    settings: SettingsSchema
    plots: List[PlotSchema]
