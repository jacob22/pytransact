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

from pytransact.exceptions import ClientError
from pytransact.testsupport import loadBLMs, BLMTests
from py.test import skip, raises

def setup_module(mod):
    import blm, blm.fundamental
    mod.blm = blm

def teardown_module(mod):
    blm.clear()

class TestStructure(BLMTests):

    def setup_method(self, method):
        super(TestStructure, self).setup_method(method)
        import blm, blm.fundamental
        loadBLMs([('fooBlm', fooBlm),
                  ('barBlm', barBlm),
                  ('zooBlm', zooBlm)])

    def teardown_method(self, method):
        super(TestStructure, self).teardown_method(method)
        blm.clear()

    def test_undefInheritedRelation(self):
        foo = blm.fooBlm.Foo()
        bar = blm.barBlm.Bar()
        zoo = blm.zooBlm.Zoo()

        assert bar.recipRel.related == blm.fooBlm.Foo.relBase

    def test_undefInheritedRelationAssignment(self):
        foo = blm.fooBlm.Foo()
        bar = blm.barBlm.Bar()
        zoo = blm.zooBlm.Zoo()

        bar(recipRel=[foo, zoo])

        assert bar.recipRel == [ foo, zoo ]
        assert foo.relBase == [ bar ]
        assert zoo.relBase == [ bar ]

    def test_assignmentInSubclass(self):
        foo = blm.fooBlm.Foo()
        zoo = blm.zooBlm.Zoo()

        zoo(zoostr=['foo'])

        raises(AttributeError,
               foo,
               zoostr=['foo'])

    def test_overloadAttr(self):
        foo = blm.fooBlm.Foo()
        zoo = blm.zooBlm.Zoo()

        foo(foostr=['foo', 'bar'])
        zoo(foostr=['foo'])

        raises(ClientError,
               zoo,
               foostr=['foo', 'bar'])

    def test_extendAttr(self):
        skip('We do not know if this is how it should work')
        foo = blm.fooBlm.Foo()
        zoo = blm.zooBlm.Zoo()

        assert foo.extendInSubclass == []
        assert zoo.extendInSubclass == ['morga']

    def test_extendMethodsInToc(self):
        foo = blm.fooBlm.Foo()
        zoo = blm.zooBlm.Zoo()

        assert foo.extAttr == [ 'moo' ]
        assert zoo.extAttr == [ 'moo' ]
        foo(foostr=['foo'])
        zoo(foostr=['goo'])
        assert foo.extAttr == [ 'moo' ]
        assert zoo.extAttr == [ 'goo' ]

    def test_extendAfterInherit(self):
        foo = blm.fooBlm.Foo()
        zoo = blm.zooBlm.Zoo()

        assert blm.zooBlm.Zoo.extAttr._basetoc == 'fooBlm.Foo'
        assert blm.zooBlm.Zoo.extAttr._toc is blm.zooBlm.Zoo
        assert blm.zooBlm.Zoo._attributes['extAttr']

    def test_extendFailsAfterInherit(self):
        skip('We want to fix this, but not right now')

        # Reason: Zoo already exists when nextAttr is extended, and
        # Zoo.nextAttr is not updated at that time.
        foo = blm.fooBlm.Foo()
        zoo = blm.zooBlm.Zoo()

        assert foo.nextAttr == []
        assert zoo.nextAttr == []
        foo(nextAttr=['foo'])
        zoo(nextAttr=['goo'])
        assert foo.nextAttr == [ 'foo' ]
        assert zoo.nextAttr == [ 'koko' ]

    def test_override_attr_in_baseclass(self):
        skip('We want to fix this, but not right now')
        assert len(blm.zooBlm.SubClass.state.values) == 4
        assert len(blm.zooBlm.SubClass.inactiveStates) == 2
        for state in blm.zooBlm.SubClass.inactiveStates:
            assert state in blm.zooBlm.SubClass.state.values


fooBlm = """
import pytransact.object.model as M

class Foo(M.TO):

    class relBase(M.Relation()):
        pass

    class foostr(M.LimitedString()):
        pass

    class extendInSubclass(M.LimitedString()):
        pass
"""

barBlm = """
import pytransact.object.model as M
from blm import fooBlm

class Bar(M.TO):

    class recipRel(M.Relation()):
        related = fooBlm.Foo.relBase
"""

zooBlm = """
import pytransact.object.model as M
from blm import fooBlm

class Zoo(fooBlm.Foo):

    class zoostr(M.LimitedString()):
        pass

    class foostr(M.LimitedString(M.QuantityMax(1))):
        pass

class __extend__(Zoo.extendInSubclass):
    def on_create(attr, value, self):
        return ['morga']

class __extend__(fooBlm.Foo):

    class extAttr(M.LimitedString()):
        pass

    class nextAttr(M.LimitedString()):
        pass

    def on_create(self):
        self.extAttr=['moo']

class __extend__(Zoo.nextAttr):

    def on_update(attr, value, self):
        return ['koko']

class __extend__(Zoo):

    def on_update(self, newAttrValues):
        self._update(newAttrValues)
        self.extAttr=['goo']

class BaseClass(M.TO):
    class state(M.Enum()):
        values = ('New', 'Finished')

    inactiveStates = (state.Finished)

class SubClass(BaseClass):
    pass

class __extend__(BaseClass):
    class state(M.Enum()):
        values = ('New', 'Assigned', 'Discarded', 'Finished')

    inactiveStates = (state.Discarded, state.Finished)
"""
