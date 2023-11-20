from __future__ import annotations

from typing import Any, Callable, Dict, Iterator, List, Optional, TypedDict

from typing_extensions import Unpack

from . import api
from .parameters import ParameterSpace
from .schema import CaseSchema, Constant


class EvaluateKwargs(TypedDict, total=False):
    verbose: bool
    add_constants: bool


class ContextProvider:
    parameters: ParameterSpace

    evaluables: Callable[[api.Context], Dict[str, Any]]

    constants: Dict[str, Constant]
    templates: Dict[str, Any]

    cond_func: Optional[Callable]
    cond_dep: List[str]

    @classmethod
    def from_schema(cls, schema: CaseSchema) -> ContextProvider:
        return cls(schema)

    def __init__(self, schema: CaseSchema):
        self.parameters = ParameterSpace.from_schema(schema.parameters)
        self.constants = schema.constants
        self.evaluables = schema.evaluate
        self.cond_func = schema.where

    def evaluate_context(self, context: dict[str, Any], **kwargs: Unpack[EvaluateKwargs]) -> api.Context:
        return self.evaluate(context, **kwargs)

    def evaluate(self, context: dict[str, Any], **kwargs: Unpack[EvaluateKwargs]) -> api.Context:
        return self.raw_evaluate(context, **kwargs)

    def raw_evaluate(
        self,
        context: dict[str, Any],
        verbose: bool = True,
        add_constants: bool = True,
    ) -> api.Context:
        context = api.Context({**self.constants, **context})
        context.update(self.evaluables(context))

        if add_constants:
            for k, v in self.constants.items():
                if k not in context:
                    context[k] = v

        return api.Context(context)

    def _subspace(
        self,
        *names: str,
        context: Optional[dict[str, Any]] = None,
        **kwargs: Unpack[EvaluateKwargs],
    ) -> Iterator[api.Context]:
        if context is None:
            context = {}
        for values in self.parameters.subspace(*names):
            ctx = self.evaluate({**context, **values}, **kwargs)
            if not self.cond_func and not self.cond_dep:
                yield ctx
                continue
            if self.cond_func and not self.cond_func(ctx):
                continue
            yield ctx
            continue

    def subspace(
        self,
        *names: str,
        context: Optional[dict[str, Any]] = None,
        **kwargs: Unpack[EvaluateKwargs],
    ) -> Iterator[api.Context]:
        for i, ctx in enumerate(self._subspace(*names, context=context, **kwargs)):
            ctx["g_index"] = i
            yield ctx

    def fullspace(
        self,
        context: Optional[dict[str, Any]] = None,
        **kwargs: Unpack[EvaluateKwargs],
    ) -> Iterator[api.Context]:
        yield from self.subspace(*self.parameters, context=context, **kwargs)
