#!/usr/bin/env py.test
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

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

import decimal, gridfs
from io import StringIO
from bson.binary import Binary
from bson.objectid import ObjectId

from py.test import raises, skip

from pytransact.contextbroker import ContextBroker
from pytransact.exceptions import *
from pytransact.object.to import TO
from pytransact.object.attribute import *
from pytransact.object.restriction import Restriction
from pytransact.testsupport import DBTests, FakeContext, loadBLM

import blm


def teardown_module(module):
    blm.clear()


class my_unicode(str):
    pass


class TestAttribute(object):

    BASIC_VALUE_TESTS = (
        (Bool, (True, False, None)),
        (Blob, ('good float', False, None)),
        (Decimal, (decimal.Decimal('0.0'), 'bad decimal', DecimalValueError)),
        (DecimalMap, (('somekey', decimal.Decimal('1.0')), ('somekey', 'bad val'), DecimalValueError)),
        (DecimalMap, (('somekey', '1.0'), (None, 1), StringValueError)),
        (DecimalMap, (('somekey', 1), 42, TypeError)),
        (Float, (1.0, 'bad float', FloatValueError)),
        (Int, (1, 'bad int', IntValueError)),
        (Int, ('1', False, None)),
        (Int, (1, False, None)),
        (IntMap, (('somekey', 1), ('somekey', 'bad val'), IntValueError)),
        (IntMap, (('somekey', '1'), (None, 1), StringValueError)),
        (IntMap, (('somekey', 1), 42, TypeError)),
        (String, ('good string', False, None)),
        (String, (my_unicode('foo'), False, None)),
        (LimitedString, ('god limited string', False, None)),
        (Timespan, (1, 'bad timespan', TimespanValueError)),
        (TimespanMap, (('somekey', 1), ('somekey', 'bad val'), TimespanValueError)),
        (TimespanMap, (('somekey', '1'), (None, 1), StringValueError)),
        (TimespanMap, (('somekey', 1), 42, TypeError)),
        (Timestamp, (1, 'bad timestamp', TimestampValueError)),
        (TimestampMap, (('somekey', 1), ('somekey', 'bad val'), TimestampValueError)),
        (TimestampMap, (('somekey', '1'), (None, 1), StringValueError)),
        (TimestampMap, (('somekey', 1), 42, TypeError)),
    )

    cb = ContextBroker()

    def setup_method(self, method):
        # Erase leftovers from previous tests...
        self.cb.contextDict.clear()
        self.context = FakeContext()
        self.cb.pushContext(self.context)

    def teardown_method(self, method):
        self.cb.contextDict.clear()

    def test_AttributeInherited(self):
        "Tests that Attribute has to be derived to be used"

        def spam():
            class dummyAttr(Attribute()):
                spam

        raises(SyntaxError, spam)

    def test_ModifierInheritance(self):
        "Tests that attributes only can inherit Modifiers"

        def spam():

            class foo(object):
                pass

            class bar(Int(foo())):
                pass

        raises(TypeError, spam)

    def test_AttributeSimple(self):
        "Tests a simple attribute declaration"

        class spam(Attribute):
            pass

    def test_ModifierValidity(self):
        "Tests that a modifier has to be applicable to be inherited"

        def spam():

            class dummyRest(Restriction):
                pass

            class dummyAttr(Int()):
                pass

            class foo(dummyAttr(dummyRest())):
                pass

        raises(TypeError, spam)

    def test_ToiDeleted(self):
        class FakeToi(object):
            _deleted = True
            _fullname = 'FakeToi'
            id = [27]
            attr = Attribute._instantiate('attr')
            _xlatKey = 'FakeToi'

        toi = FakeToi()
        raises(ToiDeletedError, setattr, toi, 'attr', ['bar'])
        attr = toi.attr
        raises(ToiDeletedError, getattr, attr, 'value')

    def check_createOk(self, AttrClass):
        "Tests that attributes can be constructed and instantiated."

        class AttrDeriv(AttrClass()):
            pass

        AttrDeriv._instantiate('attr')

    def check_typeOk(self, attr, valueOk):
        "Tests that attribute instances accept a correct value."
        return attr.coerceValueList((valueOk,))

    def _badValue_reRaise(self, attr, badVal):
        """
        Feeds an incorrect value to the typechecking of an Attribute, extracts the
        underlaying type error and re-raises that error.
        """
        try:
            attr.coerceValueList((badVal,))
        except CapsAttributeError as e:
            realE = e.error.args[0][1]
            raise realE.__class__(realE)

    def check_type_notOk(self, attr, valueNotOk, error):
        "Tests that attribute type checks raises on incorrect values."
        raises(error, self._badValue_reRaise, attr, valueNotOk)

    def check_equals(self, AttrDeriv, value):
        class FakeToi(object):
            _deleted = False

            def __init__(self, **attrData):
                self._attrData = attrData

        attr1 = AttrDeriv._instantiate('attr1')
        attr2 = AttrDeriv._instantiate('attr2')
        attr3 = AttrDeriv._instantiate('attr3')

        toi = FakeToi(attr1=[value], attr2=[value], attr3=[value, value])
        attr1.toi = attr2.toi = attr3.toi = toi

        assert attr1 == attr2
        assert not (attr1 != attr2)
        assert attr1 != attr3
        assert not (attr1 == attr3)

    def test_Attribute_basics(self):
        "Yields tests that perform sanity tests on Attribute's."

        for AttrClass, (valueOk, valueNotOk, excType) in self.BASIC_VALUE_TESTS:
            yield self.check_createOk, AttrClass

            class AttrDeriv(AttrClass()):
                pass

            attr = AttrDeriv._instantiate('attr')
            attr2 = AttrDeriv._instantiate('attr2')

            yield self.check_typeOk, attr, valueOk
            yield self.check_equals, AttrDeriv, valueOk

            if valueNotOk:
                yield self.check_type_notOk, attr, valueNotOk, excType

    def test_SingleInheritance(self):
        "Test that an attribute only can inherit ONE base attribute type"

        def spam():
            class dummyAttr(Bool(), Bool()):
                pass

        raises(SyntaxError, spam)

    def test_Enum(self):
        "Tests the Enum attribute"

        class spam(Enum()):
            values = ('foo', 'bar')
            pass

    def helpValueError(self, classOb, val, err):

        class foo(object):
            ob = type.__call__(classOb(), 'bar')

        bar = foo()

        try:
            # if err is None:
            #     import pdb; pdb.set_trace()
            bar.ob.coerceValueList(val)
        except AttrValueError as l:
            
            return isinstance(l.args[3].args[0][1], err)

        assert not err
        return True

    def test_EnumTypechecking(self):
        "Tests the Enum attribute value typechecking"

        class spam(Enum()):
            values = ('foo', 'bar')

        y = type.__call__(spam, 'spam')

        assert self.helpValueError(spam, ('spam',), EnumValueError)
        assert self.helpValueError(spam, (y.foo,), None)

    def test_EnumIterator(self):
        "Tests the Enum iterator functionality"

        class spam(Enum()):
            values = ('foo', 'bar', 'moo', 'guu')

        y = type.__call__(spam, 'spam')

        rList = []
        for i in y:
            rList.append(i)

        assert rList == list(spam.values)

    def test_ToiRef(self):
        "Tests the ToiRef attribute"
        class FakeToc(TO):
            _deleted = False
            class spam(ToiRef()):
                pass

        toi = FakeToc(ObjectId())
        ref = FakeToc(ObjectId())
        toi.spam = [ref]
        assert toi.spam == [ref]

        id = ObjectId()
        self.context.requestAttribute = lambda toi, attr: {'spam': [id]}[attr.name]
        assert len(ref.spam) == 1
        assert ref.spam[0].id[0] == id

    def test_defaultEnum(self):
        "Tests the enum defaults"

        class spam(Enum()):
            values = ('foo', 'bar')
            default = 'bar'

        y = type.__call__(spam, 'spam')
        assert y.default[0] == y.bar

    def test_enumSubclassWithDefault(self):

        class spam(Enum()):
            values = ('foo', 'bar')

        class spam2(spam()):
            default = 'bar'

        y = type.__call__(spam2, 'spam')
        assert y.default[0] == y.bar

    def test_ToiRefTypechecking(self):
        "Tests the ToiRef attribute value typechecking"

        class spam(ToiRef()):
            pass

        assert self.helpValueError(spam, ('spam',), ToiRefValueError)
        assert self.helpValueError(spam, (object(),), ToiRefValueError)
        assert self.helpValueError(spam, (42,), ToiRefValueError)
        assert self.helpValueError(spam, (str(ObjectId()),), ToiRefValueError)
        assert self.helpValueError(spam, (ObjectId(),), ToiRefValueError)
        assert self.helpValueError(spam, (TO(),), None)

    def test_ToiRefMapTypechecking(self):
        "Tests the ToiRef attribute value typechecking"

        class spam(ToiRefMap()):
            pass

        assert self.helpValueError(spam, (('foo', 'spam'),), ToiRefValueError)
        assert self.helpValueError(spam, (('foo', object()),), ToiRefValueError)
        assert self.helpValueError(spam, (('foo', 42),), ToiRefValueError)
        assert self.helpValueError(spam, (('foo', str(ObjectId())),), ToiRefValueError)
        assert self.helpValueError(spam, (('foo', ObjectId()),), ToiRefValueError)
        assert self.helpValueError(spam, (('foo', TO()),), None)

    def test_Map_get(self):
        class FakeToc(TO):
            _deleted = False
            class spam(IntMap()):
                pass

        toi = FakeToc(ObjectId())
        self.context.requestAttribute = lambda toi, attr: {'spam': {'foo': 2}}[attr.name]

        assert toi.spam.get('foo') == 2
        assert toi.spam.get('bar', 3) == 3

    def test_Map_dictlike(self):
        class FakeToc(TO):
            _deleted = False
            class spam(IntMap()):
                pass

        toi = FakeToc(ObjectId())
        self.context.requestAttribute = lambda toi, attr: {'spam': {'foo': 2, 'bar': 3}}[attr.name]

        d = {'foo': 2, 'bar': 3}
        assert list(toi.spam.keys()) == list(d.keys())
        assert list(toi.spam.items()) == list(d.items())
        assert list(toi.spam.values()) == list(d.values())

    def test_Relation(self):
        "Tests the Relation attribute"

        class spam(Relation()):
            pass

    def test_validateValues(self):
        class FakeToc(TO):
            _deleted = False
            class spam(Int(Range(1, 10))):
                pass

        toi = FakeToc(ObjectId())
        raises(AttrValueError, toi.spam.validateValues, value=[27])

    def test_validateValues_MapAttr(self):
        class FakeToc(TO):
            _deleted = False
            class spam(IntMap(Range(1, 10))):
                pass

        toi = FakeToc(ObjectId())
        raises(AttrValueError, toi.spam.validateValues, value={'1': 27})
        toi.spam.validateValues(value={'1': 10})

    def test_empty(self):
        class FakeToc(TO):
            class int(Int()):
                default = [1]
            class intmap(IntMap()):
                default = {'foo': 1}

        assert FakeToc.int.empty == []
        assert FakeToc.intmap.empty == {}

    def test_on_computation(self):
        class FakeToc(TO):
            class int(Int()):
                def on_computation(attr, toi):
                    return [27]

        toi = FakeToc()
        assert toi.int == [27]

    def test_attribute_copying(self):
        class FakeToc(TO):
            class int(Int()):
                pass
            class indirect(Int()):
                def on_computation(attr, toi):
                    return toi.int

        toi1 = FakeToc()
        toi2 = FakeToc()

        toi1.int = [27]
        toi2.int = toi1.int

        assert toi1.int == toi2.int == [27]

        toi2.int = [0]
        # this tests that attribute unpacking in Attribute.__set__
        # unpacks all levels of attributes, not just the first
        toi2.int = toi1.indirect
        assert toi2.int == [27]

    def test_add(self):
        class FakeToc(TO):
            class int(Int()):
                pass
        toi = FakeToc()

        toi.int.add(1)
        assert toi.int == [1]

        toi.int.add(1)
        assert toi.int == [1]

        toi.int.add(2)
        assert toi.int == [1, 2]

    def test_discard(self):
        class FakeToc(TO):
            class int(Int()):
                pass
        toi = FakeToc()

        toi.int = [1, 2]
        toi.int.discard(2)
        assert toi.int == [1]

        toi.int.discard(3)
        assert toi.int == [1]

class Test_Serializable(object):

    def test_serializable(self):
        from pytransact import query as Query
        attr = Serializable._instantiate('foo')

        for val in (1, [1, 2, 3], Query.Query('Foo')):
            v = attr.coerceValue(val)
            assert v is val


class TestBlobVal(DBTests):

    def setup_method(self, method):
        super(TestBlobVal, self).setup_method(method)
        class FakeContext(object):
            database = self.database
        ContextBroker().pushContext(FakeContext())

    def teardown_method(self, method):
        super(TestBlobVal, self).teardown_method(method)
        ContextBroker().popContext()

    def test_simple(self):
        val = BlobVal('foo', filename='foo.txt', content_type='text/plain',
                      transfer_encoding='7bit')
        assert val.read() == b'foo'
        assert val.read() == b''
        val.seek(0)
        assert val.read() == b'foo'

        assert val.filename == 'foo.txt'
        assert val.content_type == 'text/plain'
        assert val.length == 3
        assert val.transfer_encoding == '7bit'
        assert val.references == set()

    def test_init_with_filelike(self):
        data = StringIO('foo')
        val = BlobVal(data, filename='foo.txt', content_type='text/plain')

        assert val.read() == 'foo'
        assert val.length == 3

    def test_init_with_filelike_no_seek(self):
        from io import StringIO
        data = BytesIO(b'foo')
        def tell():
            raise IOError
        data.tell = tell
        val = BlobVal(data, filename='foo.txt', content_type='text/plain')

        assert val.read() == b'foo'
        assert val.length == 3

    def test_init_with_gridfs(self):
        val = BlobVal('foo', filename='foo.txt', content_type='text/plain')
        val.large_blob = 2
        val.__getstate__()
        self.sync()
        gridout = val.value

        val = BlobVal(gridout)
        assert val.filename == 'foo.txt'
        assert val.content_type == 'text/plain'
        assert val.read() == b'foo'

    def test___getstate__(self):
        val = BlobVal('foo', filename='foo.txt', content_type='text/plain',
                      transfer_encoding='7bit')
        state = val.__getstate__()
        assert state == {'value': Binary(b'foo'), 'filename': 'foo.txt',
                         'content_type': 'text/plain',
                         'transfer_encoding': '7bit',
                         'references': set()}

    def test___getstate__with_filelike(self):
        data = BytesIO(b'foo')
        val = BlobVal(data, filename='foo.txt', content_type='text/plain')
        state = val.__getstate__()
        assert state == {'value': Binary(b'foo'), 'filename': 'foo.txt',
                         'content_type': 'text/plain',
                         'references': set()}

    def test___getstate__when_large(self, monkeypatch):
        monkeypatch.setattr(BlobVal, 'large_blob', 10)
        data = BytesIO(b'x' * (BlobVal.large_blob + 1))
        val = BlobVal(data, filename='foo.txt', content_type='text/plain')
        val.references.add(27)
        state = val.__getstate__()
        self.sync()
        value = state.pop('gridfs')
        assert state == {}
        assert isinstance(value, ObjectId)
        gridfsfile = gridfs.GridFS(self.database, 'blobvals').get(value)
        assert gridfsfile.read() == data.getvalue()
        assert gridfsfile.metadata['references'] == {27}

    def test___setstate__(self):
        val = BlobVal.__new__(BlobVal)
        val.__setstate__({'value': Binary(b'foo'), 'filename': 'foo.txt',
                          'content_type': 'text/plain',
                          'transfer_encoding': '7bit'})
        assert val.read() == b'foo'
        assert val.filename == 'foo.txt'
        assert val.content_type == 'text/plain'
        assert val.length == 3
        assert val.transfer_encoding == '7bit'

    def test___setstate__with_gridfs(self):
        gridfsid = gridfs.GridFS(self.database, 'blobvals').put(
                b'foo', filename='foo.txt', content_type='text/plain')
        self.sync()
        val = BlobVal.__new__(BlobVal)
        val.__setstate__({'gridfs': gridfsid})
        assert val.read() == b'foo'
        assert val.filename == 'foo.txt'
        assert val.content_type == 'text/plain'
        assert val.length == 3
        assert val.transfer_encoding is None

    def test___setstate__with_gridfs_gone(self):
        fileid = ObjectId()
        val1 = BlobVal.__new__(BlobVal)
        val1.__setstate__({'gridfs': fileid})
        raises(IOError, val1.read)

        val2 = BlobVal.__new__(BlobVal)
        val2.__setstate__({'gridfs': fileid})

        assert val1 == val2

    def test_no_file_duplication(self, monkeypatch):
        monkeypatch.setattr(BlobVal, 'large_blob', 10)
        data = BytesIO(b'x' * (BlobVal.large_blob + 1))
        val = BlobVal(data, filename='foo.txt', content_type='text/plain')

        son = bson.BSON.encode({'blob': val})
        _id = val.value._id
        self.sync()
        decoded1 = son.decode()
        son = bson.BSON.encode(decoded1)
        self.sync()
        decoded2 = son.decode()

        assert _id == decoded1['blob'].value._id == decoded2['blob'].value._id
        assert val == decoded1['blob'] == decoded2['blob']

    def test_unicode(self):
        data = 'räksmörgås'
        val = BlobVal(data, filename='foo.txt', content_type='text/plain')

        son = bson.BSON.encode({'blob': val})
        decoded = son.decode()
        stored = decoded['blob'].read()
        assert stored.decode('utf-8') == data

    def test_addref_delref(self):
        val = BlobVal('foo')
        val.large_blob = 2

        son = bson.BSON.encode({'blob': val})

        ref1 = ObjectId()
        ref2 = ObjectId()
        val.addref(ref1)
        val.addref(ref2)

        self.sync()

        gridfile = gridfs.GridFS(self.database, 'blobvals').get(val.value._id)
        assert gridfile.metadata['references'] == {ref1, ref2}

        val.delref(ref1)
        val.delref(ref1)

        self.sync()

        gridfile = gridfs.GridFS(self.database, 'blobvals').get(val.value._id)
        assert gridfile.read() == b'foo'
        assert gridfile.metadata['references'] == {ref2}

        val.delref(ref2)

        self.sync()

        raises(Exception, gridfs.GridFS(self.database, 'blobvals').get, val.value._id)

    def test___hash__(self):
        val1 = BlobVal(b'foo')
        val2 = BlobVal(BytesIO(b'foo'))
        val3 = BlobVal(b'foo')
        val3.large_blob = 2
        val3.__getstate__()
        self.sync()
        val4 = BlobVal(val3.value)

        assert val1.__hash__() == val2.__hash__()
        assert val1.__hash__() == val3.__hash__()
        assert val1.__hash__() == val4.__hash__()

    def test_comparison(self):
        assert BlobVal('foo') == BlobVal('foo')
        assert BlobVal('foo') != BlobVal('bar')
        assert BlobVal('foo', 'text/plain') == BlobVal('foo', 'text/plain')

        # not too sure about these two
        assert BlobVal('foo') == BlobVal('foo', 'text/plain')
        assert BlobVal('foo', 'text/plain') == BlobVal('foo', 'text/html')


class Test_EnumVal(object):

    def test_enumval(self):
        # just save the value in the db, for easier querying
        val = EnumVal('foo')
        data = {'data': val}
        son = bson.BSON.encode(data)
        decoded = son.decode()
        assert not isinstance(decoded['data'], EnumVal)
        assert decoded == {'data': 'foo'}



class TestBlmAttribute(object):

    blmsource = """
from pytransact.object.model import *

class AccessHolder(TO):
    pass

calls = []

class blmAttr(Int()):
    def on_computation(attr):
        calls.append('blmAttr')
        return blmAttrValue
"""

    def teardown_method(self, method):
        blm.clear()

    def test_getValue(self):
        loadBLM('fundamental', self.blmsource)

        blm.fundamental.blmAttrValue = [42]

        v = blm.fundamental.blmAttr.value
        assert isinstance(blm.fundamental.blmAttr, Attribute)
        assert v == [ 42 ]
        assert blm.fundamental.calls == [ 'blmAttr' ]
