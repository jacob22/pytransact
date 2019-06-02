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

from .. import metaproxy
from future.utils import with_metaclass
class TestMetaproxy(object):
    def test_simple(self):
        class D(metaproxy.Proxy, dict):
            pass


        d = D()

        assert not isinstance(d, dict)
        assert isinstance(d, D)

        d['foo'] = 'bar'

        assert d['foo'] == 'bar'
        del d['foo']

        assert 'foo' not in d

    def test_specialmethods(self):
        class A(object):
            def a(self):
                return self

            @staticmethod
            def b():
                return 'static'

            @classmethod
            def c(cls):
                return cls

        class B(with_metaclass(metaproxy.mProxy,A)):
            def d(self):
                return self

            @staticmethod
            def e():
                return 'STATIC'

            @classmethod
            def f(cls):
                return cls

            @classmethod
            def g(cls):
                return cls()

        b = B()

        assert not isinstance(b,A)
        assert isinstance(b,B)

        assert b.a()
        assert isinstance(b.a(), A)
        assert not isinstance(b.a(), B)
        assert isinstance(b.a().__class__(), B)

        assert b.b() == 'static'
        assert B.b() == 'static'

        assert issubclass(b.c(), A)
        assert issubclass(B.c(), A)

        assert isinstance(b.d(), A)

        assert b.e() == 'STATIC'
        assert B.e() == 'STATIC'

        assert issubclass(b.f(), A)
        assert issubclass(B.f(), A)

        assert isinstance(b.g(), B)

    def test_special_builtin(self):
        import itertools
        class I(with_metaclass(metaproxy.mProxy,itertools.chain)):
            pass

        i = I()

        assert isinstance(i.from_iterable([1,2,3]), itertools.chain)
        #assert isinstance(i.from_iterable([1,2,3]), I)
        #XXX be ware of c-modules which blindly create clones
        # of a given class.
