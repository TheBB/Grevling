from __future__ import annotations

from typing import Dict, Any, Sequence, Iterable, Type

import numpy as np
from simpleeval import SimpleEval, DEFAULT_FUNCTIONS, NameNotDefined

from .parameters import ParameterSpace
from . import util, api


BUILTINS = {
    **DEFAULT_FUNCTIONS,
    'log': np.log,
    'log2': np.log2,
    'log10': np.log10,
    'sqrt': np.sqrt,
    'abs': np.abs,
    'ord': ord,
    'sin': np.sin,
    'cos': np.cos,
}


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
    types: api.Types

    @classmethod
    def load(cls, spec: Dict) -> ContextProvider:
        return cls(spec)

    def __init__(self, data: Dict):
        self.parameters = ParameterSpace.load(data.get('parameters', {}))
        self.evaluables = dict(data.get('evaluate', {}))
        self.constants = dict(data.get('constants', {}))

        self.types = {
            '_index': int,
            '_logdir': str,
            '_started': 'datetime64[ns]',
            '_finished': 'datetime64[ns]',
            '_success': bool,
        }
        self.types.update(data.get('types', {}))

        # Guess types of parameters
        for name, param in self.parameters.items():
            if name not in self.types:
                self.types[name] = _guess_eltype(param)

        # Guess types of evaluables
        if any(name not in self.types for name in self.evaluables):
            contexts = list(self.parameters.fullspace())
            for ctx in contexts:
                self.evaluate_context(ctx, verbose=False)
            for name in self.evaluables:
                if name not in self.types:
                    values = [ctx[name] for ctx in contexts]
                    self.types[name] = _guess_eltype(values)

    def evaluate_context(self, *args, **kwargs) -> api.Context:
        return self.evaluate(*args, **kwargs)

    def evaluate(
        self,
        context,
        verbose: bool = True,
        allowed_missing: bool = False,
        add_constants: bool = True,
    ) -> api.Context:
        evaluator = SimpleEval(functions=BUILTINS)
        evaluator.names.update(context)
        evaluator.names.update(
            {k: v for k, v in self.constants.items() if k not in context}
        )

        if allowed_missing is False:
            allowed_missing = set()
        elif allowed_missing is not True:
            allowed_missing = set(allowed_missing)

        for name, code in self.evaluables.items():
            try:
                result = evaluator.eval(code) if isinstance(code, str) else code
            except NameNotDefined as error:
                if allowed_missing is True:
                    util.log.debug(f'Skipped evaluating: {name}')
                    continue
                elif error.name in allowed_missing:
                    allowed_missing.add(name)
                    util.log.debug(f'Skipped evaluating: {name}')
                    continue
                else:
                    raise
            if verbose:
                util.log.debug(f'Evaluated: {name} = {repr(result)}')
            evaluator.names[name] = context[name] = result

        if add_constants:
            for k, v in self.constants.items():
                if k not in context:
                    context[k] = v

        return context

    def subspace(self, *names: str, **kwargs) -> Iterable[Dict]:
        for values in self.parameters.subspace(*names):
            yield self.evaluate(values, **kwargs)

    def fullspace(self, **kwargs) -> Iterable[Dict]:
        yield from self.subspace(*self.parameters, **kwargs)
