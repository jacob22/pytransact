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

from pytransact.patterns import extends, Extendable
import py

class Test_extend(object):

    def setup_method(self, method):
        py.test.skip('skip extend tests for now, we do not use the '
                     'extend functionality')

    def test_simple(self):
        class Foo(Extendable):
            bar = 27
        class __extend__(Foo):
            baz = 42
        assert Foo.baz == 42

    def test_override(self):
        class Foo(Extendable):
            bar = 27
        class __extend__(Foo):
            bar = 42
        assert Foo.bar == 42

    def test_extend_many(self):
        class Foo(Extendable):
            bar = 27
        class Cux(Extendable):
            thumb = 2.54
        class __extend__(Foo, Cux):
            bar = 42
        assert Foo.bar == 42
        assert Cux.bar == 42

    def test_extend_method(self):
        calls = []
        class Foo(Extendable):
            def bar(self, *args, **kw):
                pass
        origbar = Foo.bar
        class __extend__(Foo):
            @extends
            def bar(*args, **kw):
                calls.append(('extends.bar', args, kw))
        args = 1, 2
        kw = { 'cox' : 1, 'cuz' : 2 }
        ff = Foo()
        ff.bar(*args, **kw)
        assert len(calls) == 1
        func, args_, kw_ = calls[0]
        assert func == 'extends.bar'
        assert args_ == (ff, ) + args + (origbar,)
        assert kw_ == kw

        calls = []
        Foo.bar(ff, *args, **kw)
        assert len(calls) == 1
        func, args_, kw_ = calls[0]
        assert func == 'extends.bar'
        assert args_ == (ff, ) + args + (origbar,)
        assert kw_ == kw

    def test_extend_method_on_many(self):
        calls = []
        class Foo(Extendable):
            def bar(self, *args, **kw):
                pass
        class Cuz(Extendable):
            def bar(self, *args, **kw):
                pass
        origFoobar = Foo.bar
        origCuzbar = Cuz.bar
        class __extend__(Foo, Cuz):
            @extends
            def bar(*args, **kw):
                calls.append(('extends.bar', args, kw))
        args = 1, 2
        kw = { 'cox' : 1, 'cez' : 2 }
        ff = Foo()
        ff.bar(*args, **kw)
        assert len(calls) == 1
        func, args_, kw_ = calls[0]
        assert func == 'extends.bar'
        assert args_ == (ff, ) + args + (origFoobar,)

        calls = []
        cc = Cuz()
        cc.bar(*args, **kw)
        assert len(calls) == 1
        func, args_, kw_ = calls[0]
        assert func == 'extends.bar'
        assert args_ == (cc, ) + args + (origCuzbar,)

    def test_inherit(self):
        """ Testing extend functionality
        """
        calls = []
        class Foo(Extendable):
            def bar(self, *args, **kw):
                pass
        origbar = Foo.bar
        class __extend__(Foo):
            @extends
            def bar(*args, **kw):
                calls.append(('extends.bar', args, kw))
        class Bar(Foo):
            pass
        bb = Bar()
        bb.bar()

        assert len(calls) == 1
        assert calls[0][1] == (bb, origbar,)

    def test_extend_base_class(self):
        """ Testing extend functionality
        """
        calls = []
        class Foo(Extendable):
            def bar(self, *args, **kw):
                pass
        origbar = Foo.bar
        class Bar(Foo):
            pass
        bb = Bar()
        class __extend__(Foo):
            @extends
            def bar(*args, **kw):
                calls.append(('extends.bar', args, kw))
        bb.bar()

        assert len(calls) == 1
        assert calls[0][1] == (bb, origbar,)

    def test_extendwith(self):
        """ Testing extends functionality, when the extended attribute
            knows how to extend itself
        """
        extendedbar = object()
        class Bar(object):
            def __extendwith__(self, newvalue):
                calls.append(('extendwith', newvalue))
                return extendedbar
        class Foo(Extendable):
            cuz = Bar()
        origcuz = Foo.cuz

        calls = []
        class __extend__(Foo):
            @extends
            def cuz(*args, **kw):
                calls.append(('orignewcuz', args, kw))

        assert Foo.cuz is extendedbar
        assert len(calls) == 1
        assert calls[0][0] == 'extendwith'
        ff = calls[0][1]
        calls = []
        ff()
        assert calls == [('orignewcuz', (origcuz,), {})]
