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

import bson, copy
import py.test as pt
from pytransact.object.model import *
from pytransact.exceptions import *
from pytransact.contextbroker import ContextBroker
from pytransact.testsupport import FakeContext

cb = ContextBroker()

def setup_module(module):
    # Erase leftovers from previous tests...
    module.cb.contextDict.clear()
    fakeContext = FakeContext()
    module.cb.pushContext(fakeContext)

def teardown_module(module):
    module.cb.contextDict.clear()

def test_tocSimple():
    "Tests a simple toc declaration"

    class testTO(TO):
        pass

def test_hasId():
    "Tests that a TO has an id attribute that is a tuple containing one int."

    class spam(TO): pass

    to = spam()
    assert hasattr(to, 'id')
    assert to.id[0]
    assert to['id']

def test_AttributeMainNotUsable():
    "Tests that an attribute must be derived to be used"

    def spam():
        class testTO(TO):
            class attr(Attribute()):
                pass

    pt.raises(SyntaxError, spam)

def test_AttrClassname():
    "Tests that the attribute class name gets set properly"

    class spam(TO):
        class attr(LimitedString()):
            pass

    assert spam.attr.__class__.__name__ ==  'spam.attr'

def test_AttributeCollection():
    "Tests that the attributes gets collected in the relvant tocs"

    class spamTO(TO):
        class spamAttr(LimitedString()):
            pass

    class fooTO(TO):
        class fooAttr(Blob()):
            pass

    assert (set(spamTO._attributes.keys()) ==
            set(['allowRead', 'spamAttr']) and
            set(fooTO._attributes.keys()) ==
            set(['allowRead', 'fooAttr']))

def test_SubClassGetsAttributes():
    "Tests that subclasses get copies instead of original attr objects"

    class BaseToc(TO):

        class foo(Int()):
            pass

    class SubToc(BaseToc):
        pass

    # Check that subtocs have gotten copies, not original attribute objects
    assert BaseToc.foo is not SubToc.foo
    assert 'foo' in SubToc._attributes
    assert SubToc._attributes['foo'] is SubToc.foo

    assert BaseToc.foo._toc is BaseToc
    assert SubToc.foo._toc is SubToc
    assert BaseToc.foo._basetoc == SubToc.foo._basetoc == 'test_to.BaseToc'

def test_OnlyInheritOneAttributeType():
    "Tests that only one given attribute type is inherited"

    def spam():
        class spamTO(TO):
            class spamAttr(LimitedString(), Blob()):
                pass

    pt.raises(SyntaxError, spam)

def test_AttrDefaultValueListType():
    "Tests that a default value list is of the correct type"


    def spam():
        class spamTO(TO):
            class spamAttr(Int()):
                default = 'apa'

    pt.raises(AttrValueError, spam)

def test_AttrDefaultValue1():
    "Tests that a default value gets assigned to an attribute"

    class spamTO(TO):
        class spamAttr(LimitedString()):
            default = [ 'ab' ]

    toi = spamTO()
    assert toi.spamAttr[:] == [ 'ab' ]

def test_AttrDefaultValue2():
    "Tests that a default single value gets assigned to an attribute"

    class spamTO(TO):
        class spamAttr(LimitedString()):
            default = 'ab'

    toi = spamTO()
    assert toi.spamAttr[:] == [ 'ab' ]

def test_AttrAssignment1():
    "Tests that a value can be assigned and retrieved from an attribute"

    class spamTO(TO):
        class spamAttr(LimitedString()):
            pass

    toi = spamTO()
    toi.spamAttr = [ 'spam', 'soss' ]

    assert toi.spamAttr[:] == [ 'spam', 'soss' ]

def test_AttrAssignment2():
    "Tests that a single value can be assigned and retrieved from an attribute"

    class spamTO(TO):
        class spamAttr(LimitedString()):
            pass

    toi = spamTO()
    toi(spamAttr = 'spam')

    assert toi.spamAttr[:] == [ 'spam' ]

def test_AttrAssignment3():
    "Tests that a single value can be assigned and retrieved from an attribute"

    class spamTO(TO):
        class spamAttr(LimitedString()):
            pass

    toi = spamTO()
    toi.spamAttr = 'spam'

    assert toi.spamAttr[:] ==  [ 'spam' ]

def test_AttrValueAssignmentTypechecking1():
    "Tests that value type checking works on assignment"

    class spamTO(TO):
        class spamAttr(Int()):
            pass

    toi = spamTO()
    pt.raises(AttrValueError, lambda: toi(spamAttr = [ 'spam', 3 ]))

def test_AttrValueAssignmentTypechecking2():
    "Tests that value type checking works on assignment"

    class spamTO(TO):
        class spamAttr(Int()):
            pass

    def spam():
        toi = spamTO()
        toi.spamAttr = [ 'spam', 3 ]

    pt.raises(AttrValueError, spam)

def test_AttrValueInListAssignment():
    "Tests that you can modify an individual attribute in a list"

    class spamTO(TO):
        class spamAttr(LimitedString()):
            default = [ 'spam', 'foo' ]

    toi = spamTO()
    toi.spamAttr[1] = 'bar'

    assert  toi.spamAttr[:] == [ 'spam', 'bar' ]

def test_AttrValueInListAssignmentTypeChecking():
    "Tests type checking on individual attribute assignment"

    class spamTO(TO):
        class spamAttr(Int()):
            default = [ 1, 2 ]

    toi = spamTO()

    def spam():
        toi.spamAttr[1] = 'spam'

    pt.raises(IntValueError, spam)

def test_normalize():
    class spamTO(TO):
        class attr(Int()): pass
        class mapattr(IntMap()): pass

    toi = spamTO(attr=1, mapattr={'foo': 1})
    assert toi.attr == [1]
    assert toi.mapattr == {'foo': 1}


def test_nonExistantIniAttr():
    """
    Tests that passing a non-existant attribute when creating a Toi raises
    the correct exception.
    """
    pt.skip('this tests depends on the behavior of FakeContext, clean up')

    class spamTO(TO):
        class spamAttr(Int()): pass

    def spam():
        toi = spamTO(badAttr=1)

    pt.raises(AttrNameError, spam)

def test_equality():

    class spam(TO): pass

    to = spam()
    other = spam()
    assert to == to
    assert to.id[0] == to
    assert to != other

def test_deepcopy():
    class spam(TO): pass

    to = spam()
    assert copy.deepcopy(to) is to

def test_clear():
    class spamTO(TO):
        class spamAttr(Int()): pass

    toi = spamTO(spamAttr=[1])
    assert toi.spamAttr == [1] # sanity
    toi._clear()
    assert toi._attrData == {}
    assert toi._orgAttrData == {}
    assert toi.spamAttr == []

    toi._delete()
    assert toi._deleted
    toi._clear()
    assert toi._deleted # still deleted

def test___getstate__():
    class spam(TO): pass
    to = spam()
    state = to.__getstate__()
    assert state == {'id': to.id[0], 'toc': to._fullname}

    to._delete()
    state = to.__getstate__()
    assert state == {'id': to.id[0], 'toc': to._fullname, 'deleted': True}

def test___setstate__():
    class spam(TO): pass
    to = spam.__new__(spam)
    toid = bson.objectid.ObjectId()
    to.__setstate__({'id': toid, 'toc': to._fullname})
    assert to.id == (toid,)
    assert not to._deleted

    to = spam.__new__(spam)
    to.__setstate__({'id': toid, 'toc': to._fullname, 'deleted': True})
    assert to._deleted

def test_phantom():
    class spam(TO): pass

    toid = bson.objectid.ObjectId()
    toi = spam._create(toid)

    assert toi._phantom

def test_delete():
    class spam(TO): pass
    toid = bson.objectid.ObjectId()
    toi = spam._create(toid)

    can_delete = False
    class cb(object):
        def canDelete(self, toi):
            return can_delete
        def deleteToi(self, toi):
            toi._deleted = True

    toi._cb = cb()
    pt.raises(ClientError, toi._delete)
    assert toi._deleted == False

    can_delete = True
    toi._delete()
    assert toi._deleted == True
