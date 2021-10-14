from __future__ import annotations

from datetime import datetime

from typing import Dict, Any, Sequence, Iterable, Type

import numpy as np
from simpleeval import SimpleEval, DEFAULT_FUNCTIONS, NameNotDefined

from .parameters import ParameterSpace
from .typing import TypeManager, Stage, find_type
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
    types: TypeManager

    @classmethod
    def load(cls, spec: Dict) -> ContextProvider:
        return cls(spec)

    def __init__(self, data: Dict):
        self.parameters = ParameterSpace.load(data.get('parameters', {}))
        self.evaluables = dict(data.get('evaluate', {}))
        self.constants = dict(data.get('constants', {}))

        self.types = TypeManager()
        self.types.add('g_index', int, 'pre')
        self.types.add('g_logdir', str, 'pre')
        self.types.add('g_started', datetime, 'post')
        self.types.add('g_finished', datetime, 'post')
        self.types.add('g_success', bool, 'post')

        for k, v in data.get('types', {}).items():
            self.types.add(k, find_type(v), 'post')

        for k, v in self.constants.items():
            self.types.add(k, type(v), 'pre')

        # Guess types of parameters
        for name, param in self.parameters.items():
            if name not in self.types:
                self.types.add(name, _guess_eltype(param), 'pre')
            else:
                self.types[name].stage = Stage.pre

        # Guess types of evaluables
        if any(name not in self.types for name in self.evaluables):
            contexts = list(self.parameters.fullspace())
            for ctx in contexts:
                self.raw_evaluate(ctx, verbose=False)
            for name in self.evaluables:
                if name not in self.types:
                    values = [ctx[name] for ctx in contexts]
                    self.types.add(name, _guess_eltype(values), 'pre')
                else:
                    self.types[name].stage = Stage.pre

    def evaluate_context(self, *args, **kwargs) -> api.Context:
        return self.evaluate(*args, **kwargs)

    def evaluate(self, *args, **kwargs) -> api.Context:
        model = self.types.context_model()
        return model(**self.raw_evaluate(*args, **kwargs))

    def raw_evaluate(
        self,
        context,
        verbose: bool = True,
        allowed_missing: bool = False,
        add_constants: bool = True,
    ) -> Dict[str, Any]:
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
