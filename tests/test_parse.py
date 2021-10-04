from pathlib import Path
import re

from typing import List

import numpy as np

from grevling import Case


DATADIR = Path(__file__).parent / 'data'


def test_parse():
    case = Case(DATADIR / 'valid' / 'diverse.yaml')

    for name, param in case.parameters.items():
        assert param.name == name
    assert case.parameters['alpha'].values == [1, 2]
    assert case.parameters['bravo'].values == [1.0, 2.0]
    assert case.parameters['charlie'].values == [3, 4.5]
    np.testing.assert_allclose(
        case.parameters['delta'].values,
        [0.0, 0.25, 0.5, 0.75, 1.0],
        atol=1e-6,
        rtol=1e-6,
    )
    np.testing.assert_allclose(
        case.parameters['echo'].values,
        [0.0, 0.186289, 0.409836, 0.678092, 1.0],
        atol=1e-6,
        rtol=1e-6,
    )
    assert case.parameters['foxtrot'].values == ['a', 'b', 'c']

    assert case.context_mgr.evaluables == {
        'dblalpha': '2 * alpha',
    }

    assert case.context_mgr.constants == {
        'int': 14,
        'float': 14.0,
    }

    assert case.premap[0].source == 'somefile'
    assert case.premap[0].target == 'somefile'
    assert case.premap[0].mode == 'simple'
    assert case.premap[0].template

    assert case.premap[1].source == 'from'
    assert case.premap[1].target == 'to'
    assert case.premap[1].mode == 'simple'
    assert case.premap[1].template

    assert case.premap[2].source == 'q'
    assert case.premap[2].target == 'q'
    assert case.premap[2].mode == 'simple'
    assert case.premap[2].template

    assert case.premap[3].source == 'a'
    assert case.premap[3].target == 'b'
    assert case.premap[3].mode == 'simple'
    assert not case.premap[3].template

    assert case.premap[4].source == 'r'
    assert case.premap[4].target == 's'
    assert case.premap[4].mode == 'simple'
    assert not case.premap[4].template

    assert case.postmap[0].source == 'c'
    assert case.postmap[0].target == 'd'
    assert case.postmap[0].mode == 'simple'
    assert not case.postmap[0].template

    assert case.postmap[1].source == 'm'
    assert case.postmap[1].target == '.'
    assert case.postmap[1].mode == 'glob'
    assert not case.postmap[1].template

    assert case.script.commands[0].command == 'string command here'
    assert case.script.commands[0].name == 'string'
    assert case.script.commands[0].captures == []

    assert case.script.commands[1].command == ['list', 'command', 'here']
    assert case.script.commands[1].name == 'list'
    assert case.script.commands[1].captures == []

    assert case.script.commands[2].command == '/usr/bin/nontrivial-name with args'
    assert case.script.commands[2].name == 'nontrivial-name'
    assert case.script.commands[2].captures == []

    assert case.script.commands[3].command == [
        '/usr/bin/nontrivial-name-2',
        'with',
        'args',
        'as',
        'list',
    ]
    assert case.script.commands[3].name == 'nontrivial-name-2'
    assert case.script.commands[3].captures == []

    assert case.script.commands[4].command == 'run this thing'
    assert case.script.commands[4].name == 'somecommand'
    assert case.script.commands[4].captures[0]._regex.pattern == 'oneregex (?P<one>.*)'
    assert case.script.commands[4].captures[0]._mode == 'last'

    assert case.script.commands[5].command == '/some/nontrivial-stuff'
    assert case.script.commands[5].name == 'nontrivial-stuff'
    assert (
        case.script.commands[5].captures[0]._regex.pattern == 'multiregex (?P<multi>.*)'
    )
    assert case.script.commands[5].captures[0]._mode == 'all'
    assert (
        case.script.commands[5].captures[1]._regex.pattern == 'firstregex (?P<first>.*)'
    )
    assert case.script.commands[5].captures[1]._mode == 'first'
    assert (
        case.script.commands[5].captures[2]._regex.pattern == 'lastregex (?P<last>.*)'
    )
    assert case.script.commands[5].captures[2]._mode == 'last'
    assert (
        case.script.commands[5]
        .captures[3]
        ._regex.pattern.startswith(re.escape('someint'))
    )
    assert case.script.commands[5].captures[3]._mode == 'last'
    assert (
        case.script.commands[5]
        .captures[4]
        ._regex.pattern.startswith(re.escape('here is a prefix'))
    )
    assert case.script.commands[5].captures[4]._mode == 'all'

    assert case.types == {
        '_index': int,
        '_logdir': str,
        '_started': 'datetime64[ns]',
        '_finished': 'datetime64[ns]',
        '_success': bool,
        'alpha': int,
        'bravo': float,
        'charlie': float,
        'delta': float,
        'echo': float,
        'foxtrot': str,
        'dblalpha': int,
        'one': int,
        'first': str,
        'multi': List[str],
        'last': float,
        'someint': int,
        'somefloat': List[float],
        'walltime/string': float,
        'walltime/list': float,
        'walltime/nontrivial-name': float,
        'walltime/nontrivial-name-2': float,
        'walltime/somecommand': float,
        'walltime/nontrivial-stuff': float,
    }

    assert case._logdir == 'loop-de-loop'
