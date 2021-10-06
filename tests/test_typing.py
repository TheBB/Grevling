from __future__ import annotations

from datetime import datetime
from enum import Enum, IntEnum
from pathlib import Path

import pytest

from grevling.typing import (
    Enumeration,
    Type,
    Integer,
    String,
    Boolean,
    Float,
    DateTime,
    List,
    TypedObject,
    PersistentObject,
)


def test_find():
    assert Type.find('int') == Integer()
    assert Type.find('integer') == Integer()
    assert Type.find('float') == Float()
    assert Type.find('floating') == Float()
    assert Type.find('double') == Float()
    assert Type.find('str') == String()
    assert Type.find('string') == String()


def test_coerce():
    assert Integer().coerce(1) == 1
    assert Integer().coerce('1') == 1
    assert Integer().coerce(False) == 0
    assert Integer().coerce(True) == 1

    assert Float().coerce(1) == 1.0
    assert Float().coerce('1.1') == 1.1

    assert String().coerce(1) == '1'
    assert String().coerce(1.2) == '1.2'
    assert String().coerce(False) == '0'
    assert String().coerce(True) == '1'
    assert String().coerce('zomg') == 'zomg'

    assert Boolean().coerce(False) == False
    assert Boolean().coerce(True) == True
    assert Boolean().coerce(0) == False
    assert Boolean().coerce(1) == True
    assert Boolean().coerce('0') == False
    assert Boolean().coerce('1') == True

    dt = datetime(2021, 10, 5, 14, 19, 22, 348723)
    assert DateTime().coerce(dt) == dt
    assert DateTime().coerce('2021-10-05 14:19:22.348723') == dt

    assert List(Integer()).coerce([1]) == [1]
    assert List(Integer()).coerce(1) == [1]
    assert List(Integer()).coerce('1') == [1]
    assert List(Integer()).coerce(['1']) == [1]
    assert List(Integer()).coerce(['1', False, 3]) == [1, 0, 3]

    class SEnum(Enum):
        A = 'a'
        B = 'b'

    assert Enumeration(SEnum).coerce('A') == SEnum.A

    class IEnum(IntEnum):
        A = 1
        B = 2

    assert Enumeration(IEnum).coerce('A') == IEnum.A
    assert Enumeration(IEnum).coerce(2) == IEnum.B


def test_coerce_into():
    assert Integer().coerce_into(1, 0) == 1
    assert Integer().coerce_into('1', None) == 1
    assert Integer().coerce_into(False, object()) == 0
    assert Integer().coerce_into(True, 'haha') == 1

    assert Float().coerce_into(1, None) == 1.0
    assert Float().coerce_into('1.1', 1.0) == 1.1

    assert String().coerce_into(1, []) == '1'
    assert String().coerce_into(1.2, set()) == '1.2'
    assert String().coerce_into(False, {}) == '0'
    assert String().coerce_into(True, None) == '1'
    assert String().coerce_into('zomg', 'aaa') == 'zomg'

    assert Boolean().coerce_into(False, None) == False
    assert Boolean().coerce_into(True, None) == True
    assert Boolean().coerce_into(0, None) == False
    assert Boolean().coerce_into(1, None) == True
    assert Boolean().coerce_into('0', None) == False
    assert Boolean().coerce_into('1', None) == True

    dt = datetime(2021, 10, 5, 14, 19, 22, 348723)
    assert DateTime().coerce_into(dt, None) == dt
    assert DateTime().coerce_into('2021-10-05 14:19:22.348723', None) == dt

    assert List(Integer()).coerce_into([1], []) == [1]
    assert List(Integer()).coerce_into([1], None) == [1]
    assert List(Integer()).coerce_into([1], [2, 3]) == [2, 3, 1]

    class SEnum(Enum):
        A = 'a'
        B = 'b'

    assert Enumeration(SEnum).coerce_into('A', None) == SEnum.A

    class IEnum(IntEnum):
        A = 1
        B = 2

    assert Enumeration(IEnum).coerce_into('A', None) == IEnum.A
    assert Enumeration(IEnum).coerce_into(2, None) == IEnum.B


def test_object():
    class InnerObj(TypedObject):
        myint: int
        mystr: str
        mybool: bool

    obj = InnerObj()
    obj.myint = '1'
    obj.mystr = 5
    obj.mybool = '0'

    assert obj.myint == 1
    assert obj.mystr == '5'
    assert obj.mybool == False

    assert obj.to_json() == {
        '_version': 1,
        'myint': 1,
        'mystr': '5',
        'mybool': False,
    }

    obj = InnerObj({'myint': 1, 'mystr': '5'})
    assert obj.myint == 1
    assert obj.mystr == '5'
    assert hasattr(obj, 'mystr')
    assert not hasattr(obj, 'mybool')
    with pytest.raises(AttributeError):
        obj.mybool


class InnerObj1(TypedObject):

    _version = 2
    myint: int

    @classmethod
    def upgrade_data(cls, from_version, data):
        assert from_version == 1
        data['myint'] += 15
        return data


class OuterObj1(TypedObject):
    inner: InnerObj1


class InnerObj2(TypedObject):

    _version = 2
    myint: int
    mystr: str

    @classmethod
    def upgrade_data(cls, from_version, data):
        assert from_version == 1
        data['mystr'] = 'lol'
        return data


class OuterObj2(TypedObject):
    inner: InnerObj2


def test_upgrade():

    obj = InnerObj1({'myint': 1, '_version': 1})
    assert obj.myint == 16
    assert obj.to_json() == {
        'myint': 16,
        '_version': 2,
    }

    obj = OuterObj1({'inner': {'myint': 1, '_version': 1}})
    assert obj.inner.myint == 16

    obj = InnerObj2(myint=1, _version=1)
    assert obj.myint == 1
    assert obj.mystr == 'lol'

    obj = OuterObj2(inner=dict(myint=1, _version=1))
    assert obj.inner.myint == 1
    assert obj.inner.mystr == 'lol'


class InnerObj3(TypedObject):
    myint: int


class OuterObj3(PersistentObject):
    inner: InnerObj3


class InnerObj4(TypedObject):
    _version = 2
    myint: int

    @classmethod
    def upgrade_data(cls, from_version, data):
        assert from_version == 1
        data['myint'] += 15
        return data


class OuterObj4(PersistentObject):
    inner: InnerObj4


def test_persistent():

    path = Path(__file__).parent / 'temp.json'
    try:
        path.unlink()
    except FileNotFoundError:
        pass

    with OuterObj3(path) as obj:
        obj.inner = InnerObj3()
        obj.inner.myint = 4

    with OuterObj3(path) as obj:
        assert obj.inner.myint == 4

    with OuterObj4(path) as obj:
        assert obj.inner.myint == 19

    path.unlink()


class InnerObj5(TypedObject):
    myint: int
    mystr: str
    _defaults = {'myint': 1}


class OuterObj5(TypedObject):
    inner: InnerObj5


class InnerObj6(TypedObject):
    a: bool
    b: bool
    _defaults = {'a': False, 'b': False}


def test_defaults():

    obj = InnerObj5()
    assert obj.myint == 1
    assert not hasattr(obj, 'mystr')

    obj = InnerObj5({'myint': 2})
    assert obj.myint == 2
    assert not hasattr(obj, 'mystr')

    obj = InnerObj5({'mystr': 'b'})
    assert obj.myint == 1
    assert obj.mystr == 'b'

    obj = OuterObj5()
    assert obj.inner.myint == 1
    assert not hasattr(obj.inner, 'mystr')

    obj = OuterObj5({'inner': {'myint': 2}})
    assert obj.inner.myint == 2
    assert not hasattr(obj.inner, 'mystr')

    obj = OuterObj5({'inner': {'mystr': 'b'}})
    assert obj.inner.myint == 1
    assert obj.inner.mystr == 'b'

    obj = InnerObj6()
    obj.a = True
    assert obj.a
    assert not obj.b
    obj.b = True
    assert obj.a
    assert obj.b