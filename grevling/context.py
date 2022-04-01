from __future__ import annotations

from typing import Callable, Dict, Any, Optional, Iterable, List

from asteval import Interpreter                 # type: ignore

from .parameters import ParameterSpace
from . import util, api


class ContextProvider:

    parameters: ParameterSpace

    eval_func: Optional[Callable]
    eval_dep: Dict[str, str]

    constants: Dict[str, Any]
    templates: Dict[str, Any]

    cond_func: Optional[Callable]
    cond_dep: List[str]

    @classmethod
    def load(cls, spec: Dict) -> ContextProvider:
        return cls(spec)

    def __init__(self, data: Dict):
        self.parameters = ParameterSpace.load(data.get('parameters', {}))
        self.constants = dict(data.get('constants', {}))

        evaluables = data.get('evaluate', {})
        if callable(evaluables):
            self.eval_func = evaluables
            self.eval_dep = {}
        else:
            self.eval_func = None
            self.eval_dep = evaluables

        conditions = data.get('where', [])
        if callable(conditions):
            self.cond_func = conditions
            self.cond_dep = []
        else:
            self.cond_func = None
            self.cond_dep = conditions

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
        if self.eval_func:
            context = {**context, **self.eval_func(**context)}

        evaluator = Interpreter()
        evaluator.symtable.update({
            'legendre': util.legendre,
        })
        evaluator.symtable.update(context)
        evaluator.symtable.update(
            {k: v for k, v in self.constants.items() if k not in context}
        )

        for name, code in self.eval_dep.items():
            if not isinstance(code, str):
                result = code
            else:
                result = evaluator.eval(code, show_errors=False)
                if evaluator.error:
                    raise ValueError(f"Errors occurred evaluating '{name}'")
            if verbose:
                util.log.debug(f'Evaluated: {name} = {repr(result)}')
            evaluator.symtable[name] = context[name] = result

        if add_constants:
            for k, v in self.constants.items():
                if k not in context:
                    context[k] = v

        return api.Context(context)

    def subspace(self, *names: str, **kwargs) -> Iterable[api.Context]:
        for values in self.parameters.subspace(*names):
            context = self.evaluate(values, **kwargs)
            if not self.cond_func and not self.cond_dep:
                yield context
                continue
            if self.cond_func and not self.cond_func(**context):
                continue
            if not self.cond_dep:
                yield context
                continue
            evaluator = Interpreter()
            evaluator.symtable.update(context)
            for condition in self.cond_dep:
                if not evaluator.eval(condition):
                    break
            else:
                yield context

    def fullspace(self, **kwargs) -> Iterable[api.Context]:
        yield from self.subspace(*self.parameters, **kwargs)
