from __future__ import annotations

from datetime import datetime
from operator import methodcaller

from typing import Dict, Any, Sequence, Iterable, Type, List

from asteval import Interpreter

from .parameters import ParameterSpace
# from .typing import TypeManager, Stage, find_type
from . import util, api


def _guess_eltype(collection: Sequence) -> Type:
    if all(isinstance(v, str) for v in collection):
        return str
    if all(isinstance(v, int) for v in collection):
        return int
    assert all(isinstance(v, (int, float)) for v in collection)
    return float


class ContextProvider:

    parameters: ParameterSpace
    evaluables: Dict[str, str]
    constants: Dict[str, Any]
    templates: Dict[str, Any]
    conditions: List[str]

    @classmethod
    def load(cls, spec: Dict) -> ContextProvider:
        return cls(spec)

    def __init__(self, data: Dict):
        self.parameters = ParameterSpace.load(data.get('parameters', {}))
        self.evaluables = dict(data.get('evaluate', {}))
        self.constants = dict(data.get('constants', {}))

        conditions = data.get('where', [])
        if isinstance(conditions, str):
            conditions = [conditions]
        self.conditions = conditions

    def evaluate_context(self, *args, **kwargs) -> api.Context:
        return self.evaluate(*args, **kwargs)

    def evaluate(self, *args, **kwargs) -> api.Context:
        return api.Context(self.raw_evaluate(*args, **kwargs))

    def raw_evaluate(
        self,
        context,
        verbose: bool = True,
        allowed_missing: bool = False,
        add_constants: bool = True,
    ) -> Dict[str, Any]:
        evaluator = Interpreter()
        evaluator.symtable.update({
            'legendre': util.legendre,
        })
        evaluator.symtable.update(context)
        evaluator.symtable.update(
            {k: v for k, v in self.constants.items() if k not in context}
        )

        for name, code in self.evaluables.items():
            if not isinstance(code, str):
                result = code
            else:
                result = evaluator.eval(code, show_errors=False)
                only_nameerror = set(tp for tp, _ in map(methodcaller('get_error'), evaluator.error)) == {'NameError'}
                if evaluator.error and only_nameerror and allowed_missing:
                    util.log.debug(f'Skipped evaluating: {name}')
                    continue
                elif evaluator.error:
                    raise ValueError(f"Errors occurred evaluating '{name}'")
            if verbose:
                util.log.debug(f'Evaluated: {name} = {repr(result)}')
            evaluator.symtable[name] = context[name] = result

        if add_constants:
            for k, v in self.constants.items():
                if k not in context:
                    context[k] = v

        return context

    def subspace(self, *names: str, **kwargs) -> Iterable[Dict]:
        for values in self.parameters.subspace(*names):
            context = self.evaluate(values, **kwargs)
            if not self.conditions:
                yield context
                continue
            evaluator = Interpreter()
            evaluator.symtable.update(context)
            for condition in self.conditions:
                if not evaluator.eval(condition):
                    break
            else:
                yield context

    def fullspace(self, **kwargs) -> Iterable[Dict]:
        yield from self.subspace(*self.parameters, **kwargs)
