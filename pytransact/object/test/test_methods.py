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

import py.test
from pytransact.object.method import *
from pytransact.object.to import TO
from pytransact.object.attribute import Int
from pytransact.object.attribute import Serializable
from pytransact.contextbroker import ContextBroker
from pytransact.exceptions import LocalisedError, AttrErrorList
from pytransact.object.attribute import String as metaTestString
from pytransact.patterns import extends
from pytransact.testsupport import FakeContext


def setup_module(module):
    ContextBroker().pushContext(FakeContext())

def teardown_module(module):
    ContextBroker().popContext()


class fooTO(TO):
    @method(Int())
    def fooMethod(toi, fooArg=Int()):
        return fooArg

def test_misstype():
    def fun():
        class tTO(TO):
            @method(None)
            def tMethod(toi, fooArg):
                pass

    py.test.raises(SyntaxError, fun)

def test_noreturntype():
    class tTO(TO):
        @method(None)
        def tMethod(toi, fooArg=Int()):
            pass

def test_noargs():
    class tTO(TO):
        @method(None)
        def tMethod(toi):
            pass

def test_call():
    toi = fooTO()

    assert toi.fooMethod([47]) == [47]

def test_call_more():
    calls = []
    retVal = object()
    class Foo(TO):
        @method(Int)
        def fofunc(toi, int1=Int()):
            calls.append((toi, int1,))
            return retVal
    toi = Foo()
    result = toi.fofunc([55])

    assert calls == [ (toi, [55],) ]
    assert result is retVal

def test_call_param_by_name():
    calls = []
    retVal = object()
    class Foo(TO):
        @method(Int)
        def fofunc(toi, int1=Int(), int2=Int(), int3=Int()):
            calls.append((toi, int1, int2, int3))
            return retVal
    toi = Foo()
    result = toi.fofunc(int3=[3, 33], int1=[1, 11])

    assert calls == [ (toi, [1, 11], [], [3, 33]) ]
    assert result is retVal

def test_call_unbound_method():
    py.test.skip('Unbound method are not supported yet.')
    class Foo(TO):
        @method(Int)
        def fofunc(toi, int1=Int()):
            calls.append((toi, int1,))
            return retVal
    toi = Foo()

    calls = []
    retVal = object()
    func = Foo.fofunc
    result = func(toi, [89])
    assert calls == [ (toi, [89],) ]
    assert result is retVal

def test_call_even_more():
    calls = []
    retVal = object()
    class Foo(TO):
        @method(Int)
        def fofunc(toi, int1=Int()):
            calls.append((toi, int1,))
            return retVal
    toi1 = Foo()
    toi2 = Foo()
    func1 = toi1.fofunc
    func2 = toi2.fofunc
    result = func1([45])

    assert calls == [ (toi1, [45]) ]
    assert result is retVal

def test_extend_method():
    calls = []
    class Foo(TO):
        @method(None)
        def bar(toi, arg1=Int()):
            calls.append(('origbar', toi, arg1))
    origbar = Foo.bar
    class __extend__(Foo):
        @extends
        def bar(toi, arg1, origfunc):
            calls.append(('extendbar', toi, arg1, origfunc,))
    ff = Foo()
    assert isinstance(ff.bar, ExternalMethod)

    ff.bar([234])
    assert len(calls) == 1
    barname, toi, arg_, func_ = calls[0]
    assert barname == 'extendbar'
    assert toi is ff
    assert arg_ == [234]

    calls = []
    func_(ff, [987])
    assert len(calls) == 1
    barname, toi, arg_ = calls[0]
    assert barname == 'origbar'
    assert toi is ff
    assert arg_ == [987]


def test_double_extend():
    calls = []
    class Foo(TO):
        @method(None)
        def bar(toi, arg1=Int()):
            calls.append(('origbar', toi, arg1))
    origbar = Foo.bar
    class __extend__(Foo):
        @extends
        def bar(toi, arg1, origfunc):
            calls.append(('extendbar1', toi, arg1, origfunc,))
    class __extend__(Foo):
        @extends
        def bar(toi, arg1, origfunc):
            calls.append(('extendbar2', toi, arg1, origfunc,))

    ff = Foo()
    assert isinstance(ff.bar, ExternalMethod)

    ff.bar([234])
    assert len(calls) == 1
    barname, toi, arg_, func_ = calls[0]
    assert barname == 'extendbar2'
    assert toi is ff
    assert arg_ == [234]

    calls = []
    func_(ff, [987])
    assert len(calls) == 1
    barname, toi, arg_, func_ = calls[0]
    assert barname == 'extendbar1'
    assert toi is ff
    assert arg_ == [987]

    calls = []
    func_(ff, [666])
    assert len(calls) == 1
    barname, toi, arg_ = calls[0]
    assert barname == 'origbar'
    assert toi is ff
    assert arg_ == [666]

def test_serializable():
    from pytransact import query as Query
    from pytransact.object.restriction import Quantity

    calls = []
    class Foo(TO):
        @method(None)
        def bar(toi, arg=Serializable(Quantity(1))):
            calls.append((toi, arg[0]))

    foo = Foo()
    for val in (1, [1, 2, 3], Query.Query('Foo')):
        foo.bar([val])
        x = calls.pop()
        assert x[0] is foo
        assert x[1] is val

class TestBlmMethod(object):
    pass

class TestExternalMethod(object):

    class fakeMetaString(metaTestString):
        _xlatKey = 'useFullTranslatedNameDummy'

    def test_doCall_raise_LocalisedError_check_xlatKey(self):
        def duuh():
            raise LocalisedError

        em = ExternalMethod('my_duuh', duuh)
        em.params = [ self.fakeMetaString() ]
        err = py.test.raises(AttrErrorList, em.doCall, [[None]], _client=False)

        assert      err._excinfo[0]           == AttrErrorList
        assert type(err._excinfo[1].args[0])  == AttrValueError
        assert      err._excinfo[1].t['name'] == self.fakeMetaString._xlatKey
