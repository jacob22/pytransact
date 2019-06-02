# -*- coding: utf-8 -*-

# Copyright 2019 Open End AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from importlib import reload
import bson, decimal
from io import StringIO
from pytransact import custombson


def test_encode_decode():

    class Foo(object):
        def __init__(self, foo):
            self.foo = foo

    custombson.register(Foo)

    class Bar:
        def __init__(self, bar):
            self.bar = bar

    custombson.register(Bar)

    data = {'foo': Foo(Foo('foo')), 'bar' : Bar(Bar('bar'))}
    son = bson.BSON.encode(data)
    decoded = son.decode()

    assert isinstance(decoded['foo'], Foo)
    assert isinstance(decoded['foo'].foo, Foo)
    assert decoded['foo'].foo.foo == 'foo'
    assert isinstance(decoded['bar'], Bar)
    assert isinstance(decoded['bar'].bar, Bar)
    assert decoded['bar'].bar.bar == 'bar'

def test_decimal():
    d = decimal.Decimal('3.14')
    data = {'decimal': d}
    son = bson.BSON.encode(data)
    decoded = son.decode()
    assert decoded == data

def test_set():
    s = set([1, 2])
    data = {'data': s}
    son = bson.BSON.encode(data)
    decoded = son.decode()
    assert decoded == data

def test_str():
    s = 'räksmörgås'.encode('latin-1')
    data = {'data': s}
    son = bson.BSON.encode(data)
    decoded = son.decode()
    assert decoded == data
    assert not isinstance(decoded['data'], str)

class Test_uuid_subtype_hack(object):
    def setup_method(self, method):
        self._save_extension = custombson.Extension

    def teardown_method(self, method):
        reload(custombson)
        assert custombson._elements_to_dict.__name__ == '_elements_to_dict'
        custombson.Extension = self._save_extension

    def test_get_binary(self, monkeypatch):
        def fourargs(a,b,c,d):
            calls.append((a,b,c,d))
        def fiveargs(a,b,c,d,e):
            calls.append((a,b,c,d,e))
        def sixargs(a,b,c,d,e,f):
            calls.append((a,b,c,d,e,f))

        calls = []
        reload(bson)
        monkeypatch.setattr(bson, '_get_binary', fourargs)
        reload(custombson)
        assert custombson._bson_get_binary.__name__ =='fourargs'
        custombson._bson_get_binary(1,2,3,4,5,6)
        assert calls == [(1,2,3,4)]

        calls = []
        reload(bson)
        monkeypatch.setattr(bson, '_get_binary', fiveargs)
        reload(custombson)
        assert custombson._bson_get_binary.__name__ =='fiveargs'
        custombson._bson_get_binary(1,2,3,4,5,6)
        assert calls == [(1,2,3,4,5)]

        calls = []
        reload(bson)
        monkeypatch.setattr(bson, '_get_binary', sixargs)
        reload(custombson)
        assert custombson._bson_get_binary.__name__ =='sixargs'
        custombson._bson_get_binary(1,2,3,4,5,6)
        assert calls == [(1,2,3,4,5,6)]
