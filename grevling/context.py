from __future__ import annotations

from typing import Callable, Dict, Any, Optional, Iterable, List

from asteval import Interpreter                 # type: ignore

from .schema import CaseSchema, Constant
from .parameters import ParameterSpace
from . import util, api


class ContextProvider:

    parameters: ParameterSpace

    evaluables: Callable[[api.Context], Dict]

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

        # conditions = schema.where
        # if callable(conditions):
        #     self.cond_func = conditions
        #     self.cond_dep = []
        # elif isinstance(conditions, list):
        #     self.cond_func = None
        #     self.cond_dep = conditions
        # else:
        #     self.cond_func = None
        #     self.cond_dep = [conditions]

    def evaluate_context(self, *args, **kwargs) -> api.Context:
        return self.evaluate(*args, **kwargs)

    def evaluate(self, *args, **kwargs) -> api.Context:
        return api.Context(self.raw_evaluate(*args, **kwargs))

    def raw_evaluate(
        self,
        context,
        verbose: bool = True,
        add_constants: bool = True,
    ) -> api.Context:

        context = {**self.constants, **context}
        context.update(self.evaluables(context))

        if add_constants:
            for k, v in self.constants.items():
                if k not in context:
                    context[k] = v

        return api.Context(context)

    def _subspace(self, *names: str, context = None, **kwargs) -> Iterable[api.Context]:
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

    def subspace(self, *args, **kwargs) -> Iterable[api.Context]:
        for i, ctx in enumerate(self._subspace(*args, **kwargs)):
            ctx['g_index'] = i
            yield ctx

    def fullspace(self, context = None, **kwargs) -> Iterable[api.Context]:
        yield from self.subspace(*self.parameters, context=context, **kwargs)
