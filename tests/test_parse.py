from pathlib import Path
import re

from typing import List

import numpy as np

from badger import Case


DATADIR = Path(__file__).parent / 'data'

def test_parse():
    case = Case(DATADIR / 'valid' / 'diverse.yaml')

    for name, param in case._parameters.items():
        assert param.name == name
    assert case._parameters['alpha'].values == [1, 2]
    assert case._parameters['bravo'].values == [1.0, 2.0]
    assert case._parameters['charlie'].values == [3, 4.5]
    np.testing.assert_allclose(case._parameters['delta'].values, [0.0, 0.25, 0.5, 0.75, 1.0], atol=1e-6, rtol=1e-6)
    np.testing.assert_allclose(case._parameters['echo'].values, [0.0, 0.186289, 0.409836, 0.678092, 1.0], atol=1e-6, rtol=1e-6)
    assert case._parameters['foxtrot'].values == ['a', 'b', 'c']

    assert case._evaluables == {
        'dblalpha': '2 * alpha',
    }

    assert case._constants == {
        'int': 14,
        'float': 14.0,
    }

    assert case._pre_files[0].source == 'somefile'
    assert case._pre_files[0].target == 'somefile'
    assert case._pre_files[0].mode == 'simple'
    assert case._pre_files[0].template

    assert case._pre_files[1].source == 'from'
    assert case._pre_files[1].target == 'to'
    assert case._pre_files[1].mode == 'simple'
    assert case._pre_files[1].template

    assert case._pre_files[2].source == 'q'
    assert case._pre_files[2].target == 'q'
    assert case._pre_files[2].mode == 'simple'
    assert case._pre_files[2].template

    assert case._pre_files[3].source == 'a'
    assert case._pre_files[3].target == 'b'
    assert case._pre_files[3].mode == 'simple'
    assert not case._pre_files[3].template

    assert case._pre_files[4].source == 'r'
    assert case._pre_files[4].target == 's'
    assert case._pre_files[4].mode == 'simple'
    assert not case._pre_files[4].template

    assert case._post_files[0].source == 'c'
    assert case._post_files[0].target == 'd'
    assert case._post_files[0].mode == 'simple'
    assert not case._post_files[0].template

    assert case._post_files[1].source == 'm'
    assert case._post_files[1].target == '.'
    assert case._post_files[1].mode == 'glob'
    assert not case._post_files[1].template

    assert case._commands[0]._command == 'string command here'
    assert case._commands[0].name == 'string'
    assert case._commands[0]._capture == []

    assert case._commands[1]._command == ['list', 'command', 'here']
    assert case._commands[1].name == 'list'
    assert case._commands[1]._capture == []

    assert case._commands[2]._command == '/usr/bin/nontrivial-name with args'
    assert case._commands[2].name == 'nontrivial-name'
    assert case._commands[2]._capture == []

    assert case._commands[3]._command == ['/usr/bin/nontrivial-name', 'with', 'args', 'as', 'list']
    assert case._commands[3].name == 'nontrivial-name'
    assert case._commands[3]._capture == []

    assert case._commands[4]._command == 'run this thing'
    assert case._commands[4].name == 'somecommand'
    assert case._commands[4]._capture[0]._regex.pattern == 'oneregex (?P<one>.*)'
    assert case._commands[4]._capture[0]._mode == 'last'

    assert case._commands[5]._command == '/some/nontrivial-stuff'
    assert case._commands[5].name == 'nontrivial-stuff'
    assert case._commands[5]._capture[0]._regex.pattern == 'multiregex (?P<multi>.*)'
    assert case._commands[5]._capture[0]._mode == 'all'
    assert case._commands[5]._capture[1]._regex.pattern == 'firstregex (?P<first>.*)'
    assert case._commands[5]._capture[1]._mode == 'first'
    assert case._commands[5]._capture[2]._regex.pattern == 'lastregex (?P<last>.*)'
    assert case._commands[5]._capture[2]._mode == 'last'
    assert case._commands[5]._capture[3]._regex.pattern.startswith(re.escape('someint'))
    assert case._commands[5]._capture[3]._mode == 'last'
    assert case._commands[5]._capture[4]._regex.pattern.startswith(re.escape('here is a prefix'))
    assert case._commands[5]._capture[4]._mode == 'all'

    assert case._types == {
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
    }

    assert case._logdir == 'loop-de-loop'
