from datetime import datetime
from pathlib import Path
import re

from typing import List

import numpy as np
import pytest

from grevling import Case
# from grevling.typing import Field, Stage


DATADIR = Path(__file__).parent / 'data'


@pytest.mark.parametrize('suffix', ['.yaml', '.gold'])
def test_parse(suffix):
    case = Case(DATADIR / 'valid' / f'diverse{suffix}')

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

    assert case._logdir == 'loop-de-loop'
