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

from py.test import raises, skip
import py

from pytransact import contextbroker
from pytransact.blmsupport import BlmAttributeError
from pytransact.exceptions import ClientError
from pytransact.testsupport import loadBLMs, BLMTests

def setup_module(mod):
    import blm, blm.fundamental
    mod.blm = blm

def teardown_module(mod):
    blm.clear()

aBlm = """
import pytransact.object.model as M1

class A(M1.TO):
    pass

"""

bBlm = """
import pytransact.object.model as M
import blm.a

class B(M.TO):

    class a(M.Relation()):
        related = 'a.A.b'

"""

cBlm = """
import pytransact.object.model as M
import blm.a

class __extend__(blm.a.A):

    class b(M.Relation()):
        related = blm.b.B.a

"""


dBlm = """
import pytransact.object.model as M
import blm.b

class __extend__(blm.b.B):

    class a(M.ToiRef()):
        pass

"""

eBlm = """
import pytransact.object.model as M

class E(M.TO):

    class a(M.Relation()):
        related='E.b'

    class b(M.Relation()):
        related='E.c'

    class c(M.Relation()):
        related='E.a'

"""


class TestRelation(object):

    def setup_method(self, method):
        import blm.fundamental

    def teardown_method(self, method):
        import blm
        blm.clear()

    def test_extended_relation(self):
        loadBLMs([('a', aBlm), ('b', bBlm), ('c', cBlm)])

        import blm, blm.a, blm.b, blm.c

        assert blm.a.A.b.related is blm.b.B.a
        assert blm.b.B.a.related is blm.a.A.b

    def test_broken_relation(self):
        py.test.raises(BlmAttributeError,
                       loadBLMs, [('a', aBlm), ('b', bBlm),
                                  ('d', dBlm), ('c', cBlm)])

    def test_triangle_relation(self):
        py.test.raises(BlmAttributeError,
                       loadBLMs, [('e', eBlm)])


class TestParentRelationRestriction(object):

    def setup_method(self, method):
        import blm.fundamental

    def teardown_method(self, method):
        import blm
        blm.clear()

    parentBlm = """
import pytransact.object.model as M

class RelatedChild(M.TO):
    class name(M.LimitedString()):
        pass
    class parent1(M.Relation(M.Parent())):
        pass
    class parent2(M.Relation(M.Parent())):
        pass

"""

    def test_double_parent_attributes_raises(self):
        """Creating TOCs with multiple Parents should raise error
        """
        skip('Multiple Parent() restrictions do not raise error')
        raises(Exception, loadBLMs, [('parent', self.parentBlm)])

class TestRelationRestrictions_etc(BLMTests):

    def setup_method(self, method):
        import blm.fundamental
        loadBLMs([('relBlm', relBlm)])
        super(TestRelationRestrictions_etc, self).setup_method(method)

        import blm

        self.parent = blm.relBlm.Parent()
        self.child  = blm.relBlm.Child (parent=[self.parent])
        self.queen  = blm.relBlm.Parent()
        self.prince = blm.relBlm.Child (parent=[self.queen])
        self.nurse  = blm.relBlm.Nurse ()
        self.baby   = blm.relBlm.Orphan(parent=[self.nurse])
        self.orphan = blm.relBlm.Orphan()
        self.lisa   = blm.relBlm.Baby()
        self.mom    = blm.relBlm.Mom (children=[self.lisa])
        self.commit()

    def teardown_method(self, method):
        super(TestRelationRestrictions_etc, self).teardown_method(method)
        import blm
        blm.clear()

    def test_change_relations_new_parent(self):
        """Changing parent, simple test for common situations.
        """
        py.test.skip('FIXME!')
        self.prince(parent=[self.parent])
        self.commit()
        assert len(self.parent.children) == 2
        assert self.prince in self.parent.children and \
               self.child  in self.parent.children
        assert self.queen.children == []

        self.child(parent=[self.queen])
        assert self.queen .children == [self.child]
        assert self.parent.children == [self.prince]
        assert self.queen .children == self.queen .children.value
        assert self.parent.children == self.parent.children.value

        self.nurse(children=[self.orphan])
        assert self.orphan.parent[0] == self.nurse

    def test_change_relations_add_child(self):
        py.test.raises(ClientError, self.parent,
                       children=[self.child, self.prince])

    def test_change_relations_remove_child(self):
        py.test.raises(ClientError, self.parent, children=[])

    def test_change_relations_orphan_add(self):
        py.test.raises(ClientError, self.orphan, parent=[self.nurse])
        py.test.raises(ClientError, self.orphan, parent=[self.queen])

    def test_change_relations_orphan_remove(self):
        py.test.raises(ClientError, self.lisa, parent=[])

relBlm = """
import pytransact.object.model as M

class Parent(M.TO):

    class children(M.Relation()):
        related = 'Child.parent'

class Child(M.TO):

    class parent(M.Relation(M.Quantity(1))):
        related = Parent.children

class Nurse(M.TO):

    class children(M.Relation(M.QuantityMax(1))):
        related = 'Orphan.parent'

class Mom(M.TO):

    class children(M.Relation(M.Quantity(1))):
        related = 'Baby.parent'

class Orphan(M.TO):

    class parent(M.Relation(M.QuantityMax(1))):
        pass

class Baby(M.TO):

    class parent(M.Relation(M.QuantityMax(1))):
        pass

"""
