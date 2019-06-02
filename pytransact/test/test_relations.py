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

from py.test import skip
from pytransact.testsupport import loadBLMs, BLMTests
import blm

def setup_module(module):
    from blm import fundamental
    loadBLMs([('fake', fake)])


def teardown_module(module):
    blm.clear()


class TestSimpleRelation(BLMTests):

    def setup_method(self, method):
        super(TestSimpleRelation, self).setup_method(method)
        self.parent = [ blm.fake.SimpleParent() ]

    def test_create(self):
        self.child = [blm.fake.BaseToc(parent=self.parent)]
        assert self.child[0].parent == self.parent
        assert self.parent[0].children == self.child

    def test_delete(self):
        self.child = [blm.fake.BaseToc(parent=self.parent)]
        assert self.child[0].parent == self.parent
        assert self.parent[0].children == self.child

        self.child[0]._delete()
        skip("CommitProcessor doesn't handle add/remove in same commit()")
        # These asserts do not work, since CommitProcessor doesn't handle
        # removing/adding the same value from a relation in the same commit
        assert self.parent[0].children == []


class TestMultiRelations(BLMTests):

    def setup_method(self, method):
        super(TestMultiRelations, self).setup_method(method)
        self.parent = [ blm.fake.FakeToc() ]

    def test_blms_loaded(self):
        pass

    def test_create(self):
        self.sub1 = [blm.fake.SubToc1(parent=self.parent)]
        self.sub2 = [blm.fake.SubToc2(parent=self.parent)]
        assert self.sub1 == self.parent[0].subs1.value 
        assert self.sub2 == self.parent[0].subs2.value

    def test_update(self):
        otherParent = [ blm.fake.FakeToc() ]
        self.sub1 = [blm.fake.SubToc1(parent=otherParent)]
        self.sub2 = [blm.fake.SubToc2(parent=otherParent)]
        assert otherParent == self.sub1[0].parent.value
        assert otherParent == self.sub2[0].parent.value
        self.sub1[0](parent = self.parent)
        self.sub2[0](parent = self.parent)
        assert self.sub1 == self.parent[0].subs1.value
        assert self.sub2 == self.parent[0].subs2.value

        skip("CommitProcessor doesn't handle add/remove in same commit()")
        # These asserts do not work, since CommitProcessor doesn't handle
        # removing/adding the same value from a relation in the same commit
        assert [] == self.parent[0].subs1.value
        assert [] == self.parent[0].subs2.value

    def test_delete(self):
        self.sub1 = [blm.fake.SubToc1(parent=self.parent)]
        self.sub2 = [blm.fake.SubToc2(parent=self.parent)]
        self.sub1[0]._delete()
        self.sub2[0]._delete()
        skip("CommitProcessor doesn't handle add/remove in same commit()")
        # These asserts do not work, since CommitProcessor doesn't handle
        # removing/adding the same value from a relation in the same commit
        assert [] == self.parent[0].subs1.value
        assert [] == self.parent[0].subs2.value


fake = """
import pytransact.object.model as M

class BaseToc(M.TO):

    class parent(M.Relation(M.Quantity(1), M.Parent())):
        pass

class SimpleParent(M.TO):

    class children(M.Relation(M.Weak())):
        related = 'blm.fake.BaseToc.parent'

class SubToc1(BaseToc):

    class foo(M.LimitedString()):
        pass

class SubToc2(BaseToc):

    class bar(M.LimitedString()):
        pass

class FakeToc(M.TO):

    class subs1(M.Relation(M.Weak())):
        related = 'blm.fake.SubToc1.parent'

    class subs2(M.Relation(M.Weak())):
        related = 'blm.fake.SubToc2.parent'
"""
