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

from pytransact.object.model import *
from pytransact.query import Query

from blm import fundamental

class User(fundamental.AccessHolder):
    class name(LimitedString()):
        pass

class Other(TO):
    class name(LimitedString()):
        pass

    class related(Relation()):
        related = 'Related.other'

class OtherWeak(TO):
    class related(Relation(Weak())):
        related = 'Related.weak'

class RestTest(TO):
    class name(LimitedString(Quantity(1))):
        pass

class Related(TO):
    class name(LimitedString()):
        pass

    class other(Relation()):
        related = Other.related

    class weak(Relation()):
        related = OtherWeak.related

class Test(TO):
    class name(LimitedString()):
        pass

    class extra(LimitedString()):
        def on_index(attr, val, toi):
            return val

    class readonly(LimitedString(ReadOnly())):
        pass

    class computed(LimitedString()):
        def on_computation(attr, self):
            return ['foo']

    class unchangeable(LimitedString(Unchangeable())):
        pass

    class weakref(ToiRef(Weak())):
        pass

    class reorder(LimitedString(ReorderOnly())):
        pass

    class unique(LimitedString(Unique())):
        pass

    class simpleToiType(ToiRef(ToiType('Test', name='test'))):
        pass

    class toiref(ToiRef()):
        pass

    class toirefmap(ToiRefMap()):
        pass

    class complexToiType(ToiRef(ToiType('Test',
                                        toiref=Query(Other, name='test')))):
        pass

    class blob(Blob()):
        pass

    @method(LimitedString())
    def simple(toi, arg=LimitedString()):
        return toi.name + arg

    @method(None)
    def add(toi, arg=LimitedString()):
        toi.extra = toi.extra + arg

    def canWrite(self, user, attrName):
        return False


@method(None)
def broken():
    raise cBlmError('broken')

@method(LimitedString())
def simple(arg=LimitedString()):
    return ['foo'] + arg

@method(None)
def write(toi=ToiRef(ToiType(Test)),
          name=LimitedString()):
    toi[0](name=name)

