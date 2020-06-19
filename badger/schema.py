from functools import reduce

import strictyaml as yaml
from strictyaml import (
    ScalarValidator, Optional, Any, Bool, Int, Float, Str, Map, MapPattern, Seq, FixedSeq
)


class Literal(yaml.ScalarValidator):
    """Validator that only matches a literal string."""

    def __init__(self, expected):
        super().__init__()
        self._expected = expected

    def validate_scalar(self, chunk):
        if self._expected != chunk.contents:
            chunk.expecting_but_found(f"when expecting {self._expected}", "found non-matching string")
        return chunk.contents


def Choice(*args):
    """Validator that matches a choice of several literal strings."""
    either = lambda x, y: x | y
    return reduce(either, map(Literal, args))


def Scalar():
    """Validator that matches integers and floats."""
    return Int() | Float()


def FileMapping():
    """Validator that matches a file mapping: a string or a mapping with
    source and target.
    """
    return Str() | Map({
        'source': Str(),
        'target': Str(),
    })


def Regex():
    """Validator that matches a regex: a mapping with pattern and optional
    mode.
    """
    return Map({
        'pattern': Str(),
        Optional('mode'): Choice('first', 'last', 'all'),
    })


CASE_SCHEMA = yaml.Map({
    Optional('parameters'): MapPattern(Str(), Any()),
    Optional('evaluate'): MapPattern(Str(), Str()),
    Optional('templates'): Seq(FileMapping()),
    Optional('prefiles'): Seq(FileMapping()),
    Optional('postfiles'): Seq(FileMapping()),
    Optional('script'): Seq(Any()),
    Optional('settings'): Map({
        Optional('logdir'): Str(),
    }),
    Optional('types'): MapPattern(
        Str(),
        Choice('int', 'integer', 'str', 'string', 'float', 'floating', 'double'),
    ),
})

PARAM_SCHEMAS = [
    Seq(Scalar()),
    Seq(yaml.Str()),
    Map({
        'type': Literal('uniform'),
        'interval': FixedSeq([Scalar(), Scalar()]),
        'num': Int(),
    }),
    Map({
        'type': Literal('graded'),
        'interval': FixedSeq([Scalar(), Scalar()]),
        'num': Int(),
        'grading': Scalar(),
    })
]

COMMAND_SCHEMAS = [
    Str(),
    Seq(Str()),
    Map({
        'command': Str() | Seq(yaml.Str()),
        Optional('name'): Str(),
        Optional('capture'): Str() | Regex() | Seq(Str() | Regex()),
        Optional('capture-output'): Bool(),
        Optional('capture-walltime'): Bool(),
    })
]


def validate_multiple(node, schemas, name):
    for schema in schemas:
        try:
            node.revalidate(schema)
            break
        except yaml.YAMLValidationError:
            pass
    else:
        raise yaml.YAMLValidationError(
            f"failed to find a valid schema for {name}",
            "found invalid input", node._chunk
        )


def load_and_validate(text, path):
    casedata = yaml.parser.generic_load(text, schema=CASE_SCHEMA, label=path, allow_flow_style=True)
    for name, paramspec in casedata.get('parameters', {}).items():
        validate_multiple(paramspec, PARAM_SCHEMAS, f"parameter {name}")
    for commandspec in casedata.get('script', []):
        validate_multiple(commandspec, COMMAND_SCHEMAS, "script command")
    return casedata.data
