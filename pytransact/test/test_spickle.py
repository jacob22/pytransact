# -*- coding: utf-8-*-

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

#
# Unit tests for spickle
#

from pytransact.spickle import *
from io import StringIO
import struct
#
# class Test_encodelong:
#     def test_1octet(self):
#         assert encode_long(0) == ''
#         for i in range(1,128):
#             assert encode_long(i) == chr(i), i
#         for i in range(1,129):
#             assert encode_long(-i) == chr(-i & 0xFF), -i
#
#     def test_2octet(self):
#         for i in range(128,32768,11):
#             assert encode_long(i) ==  chr((i&0xff00)>>8) + chr(i&0xff)
#         i = 32767
#         assert encode_long(i) ==  chr((i&0xff00)>>8) + chr(i&0xff)
#         for i in range(129,32769,11):
#             assert encode_long(-i) ==  chr((-i&0xff00)>>8) + chr(-i&0xff), i
#         i = 32768
#         assert encode_long(-i) ==  chr((-i&0xff00)>>8) + chr(-i&0xff), i

class Test_dump_simple:
    def test_bool(self):
        false = dumps(False)
        assert false == BOOL_FALSE + STOP, false
        true = dumps(True)
        assert true == BOOL_TRUE + STOP, true

    def test_float(self):
        fobj = 1.0
        flt = dumps(fobj)
        assert flt == FLOAT + struct.pack('!d', fobj) + STOP, flt

    def test_int1(self):
        # 1-octet
        i = dumps(0)
        assert i == INT1 + b'\0' + STOP, i
        i = dumps(1)
        assert i == INT1 + b'\x01' + STOP, i
        i = dumps(-1)
        assert i == INT1 + b'\xFF' + STOP, i
        i = dumps(-128)
        assert i == INT1 + b'\x80' + STOP, i
        
    def test_int2(self):
        # 2-octet
        i = dumps(128)
        assert i == INT2 + b'\0\x80' + STOP, i # Yes, really '\x80' is -128
        i = dumps(-129)
        assert i == INT2 + b'\xff\x7f' + STOP, i
        
    def test_int4(self):
        # 4-octet
        i = dumps(1<<24)
        assert i == INT4 + b'\x01\0\0\0' + STOP, i
        i = dumps(-1<<24)
        assert i == INT4 + b'\xff\0\0\0' + STOP, i

    # def test_long1(self):
    #     l = 1
    #     le = encode_long(l)
    #     assert dumps(l) == LONG1 + bytes([len(le)]) + le + STOP, l
    #     l = -1
    #     le = encode_long(l)
    #     assert dumps(l) == LONG1 + bytes([len(le)]) + le + STOP, l
    #     l = 1 << (255*8-2)
    #     le = encode_long(l)
    #     assert dumps(l) == LONG1 + bytes([len(le)]) + le + STOP, l
	#
    # def test_long4(self):
    #     l = 1 << (256*8)
    #     le = encode_long(l)
    #     assert dumps(l) == LONG4 + struct.pack('!I', len(le)) + le + STOP, l

    def test_none(self):
        assert dumps(None) == NONE + STOP

    def test_bytes1(self):
        s = b'foo bar baz'
        assert dumps(s) == STRING1 + bytes([len(s)]) + s + STOP

    def test_bytes4(self):
        s = b'foo ' * 256
        assert dumps(s) == STRING4 + struct.pack('!I', len(s)) + s + STOP

    def test_unicode1(self):
        u = 'flygande bäckasiner söka hwila på mjuka tufvor qxz'
        s = u.encode('utf-8')
        assert dumps(u) == UNICODE1 + bytes([len(s)]) + s + STOP
        
    def test_unicode4(self):
        u = 'åäö ' * 256
        s = u.encode('utf-8')
        assert dumps(u) == UNICODE4 + struct.pack('!I', len(s)) + s + STOP

    def test_string_memo(self):
        f = BytesIO()
        dumper = Pickler(f)
        s = b'foo'
        dumper.dump(s)
        p1 = f.getvalue()
        f.truncate(0)
        f.seek(0)
        assert p1 == STRING1 + bytes([len(s)]) + s + STOP
        #import pdb;pdb.set_trace()
        dumper.dump(s)
        p2 = f.getvalue()
        f.truncate(0)
        assert p2 == dumps(0)[:-1] + GET + STOP

    def test_complex(self):
        c = 1+2j
        assert dumps(c) == (dumps(2.0)[:-1] + dumps(1.0)[:-1] + COMPLEX + STOP)


class Test_dict:
    def test_dict0(self):
        assert dumps({}) == EMPTY_DICT + STOP

    def test_dict1(self):
        assert dumps({ 'foo': 'bar'}) == (
            EMPTY_DICT +
            dumps('foo')[:-1] + dumps('bar')[:-1] +
            SETITEM + STOP)

    def test_dict2(self):
        d = dumps({ 'foo': 'bar', 'apa' : 'bepa' })
        # Order of items in dump isn't guaranteed
        s1 = dumps('foo')[:-1] + dumps('bar')[:-1]
        s2 = dumps('apa')[:-1] + dumps('bepa')[:-1]
        h = EMPTY_DICT + MARK
        f = UPDATE + STOP
        r1 = h + s1 + s2 + f
        r2 = h + s2 + s1 + f
        assert d == r1 or d == r2

    def test_recursive(self):
        d = {}
        d['foo'] = d

        assert dumps(d) == (
            EMPTY_DICT +
            dumps('foo')[:-1] + dumps(0)[:-1] + GET +
            SETITEM + STOP)

    def test_load_empty_update(self):
        assert loads(EMPTY_DICT + MARK + UPDATE + STOP) == {}

    def test_load_setitem(self):
        d = loads(EMPTY_DICT + INT1 + b'\1' + INT1 + b'\2' + SETITEM + STOP)
        assert d == { 1:2 }

    def test_load_update(self):
        d = loads(EMPTY_DICT + MARK + INT1 + b'\1' + INT1 + b'\2' + UPDATE
                  + STOP)
        assert d == { 1:2 }

    def test_load_memo(self):
        assert loads(EMPTY_DICT + POP + INT1 + b'\0' + GET + STOP) == {}

class Test_list:
    def test_list0(self):
        assert dumps([]) == EMPTY_LIST + STOP

    def test_list1(self):
        assert dumps(['apa']) == (
            EMPTY_LIST +
            dumps('apa')[:-1] +
            APPEND + STOP)

    def test_list2(self):
        assert dumps(['foo', 'bar']) == (
            EMPTY_LIST + MARK +
            dumps('foo')[:-1] +
            dumps('bar')[:-1] +
            EXTEND + STOP)

    def test_recursive(self):
        l = []
        l.append(l)

        assert dumps(l) == (
            EMPTY_LIST + dumps(0)[:-1] + GET + APPEND + STOP)

    def test_empty_extend(self):
        assert loads(EMPTY_LIST + MARK + EXTEND + STOP) == []

    def test_append(self):
        assert loads(EMPTY_LIST + NONE + APPEND + STOP) == [None]

    def test_extend(self):
        assert loads(EMPTY_LIST + MARK + NONE + EXTEND + STOP) == [None]

    def test_load_memo(self):
        assert loads(EMPTY_LIST + POP + INT1 + b'\0' + GET + STOP) == []

class Test_tuple:
    def test_tuple0(self):
        assert dumps(()) == EMPTY_TUPLE + STOP

    def test_tuple1(self):
        assert dumps(('foo',)) == (MARK + dumps('foo')[:-1] + TUPLE + STOP)

    def test_recursive(self):
        t = ([],)
        t[0].append(t)

        assert dumps(t) == (
            MARK + EMPTY_LIST + MARK + dumps(0)[:-1] + GET + TUPLE + APPEND +
            POP_MARK + dumps(1)[:-1] + GET + STOP)

    def test_load_empty(self):
        assert loads(MARK + TUPLE + STOP) == ()

    def test_load(self):
        assert loads(MARK + NONE + TUPLE + STOP) == (None,)

    def test_load_memo(self):
        assert loads(EMPTY_TUPLE + POP + INT1 + b'\0' + GET + STOP) == ()
        assert loads(MARK + TUPLE + POP + INT1 + b'\0' + GET + STOP) == ()

class Test_ext:
    class Foo(object):
        def __init__(self, value):
            self.value = value

    @classmethod
    def pickler(self, obj):
        return 42

    @classmethod
    def unpickler(self, code, data):
        assert code == self.Foo.__name__
        return self.Foo(data)

    class Bar:
        pass

    def setup_class(self):
        Extension.register(self.Foo, pickler=self.pickler,
                           unpickler=self.unpickler)
        Extension.register(self.Bar, None, pickler=self.pickler)

    def test_extension(self):
        p = Extension.getpickler(type(self.Foo(42)))
        assert p
        assert p(None) == (self.Foo.__name__, 42)
        u = Extension.getunpickler(self.Foo.__name__)
        assert u
        assert type(u(self.Foo.__name__, None)) == self.Foo
    
    def test_code(self):
        assert dumps(self.Foo(42)) == (
            dumps(42)[:-1] + dumps(self.Foo.__name__)[:-1] + EXTENSION + STOP)

    def test_nocode(self):
        assert dumps(self.Bar()) == (dumps(42)[:-1] + PUT + STOP)

    def test_load(self):
        obj = loads(dumps(42)[:-1] + dumps(self.Foo.__name__)[:-1] +
                    EXTENSION + STOP)
        assert type(obj) is self.Foo
        assert obj.value == 42


class Test_load_simple:
    def test_bool(self):
        assert loads(BOOL_TRUE + STOP) is True
        assert loads(BOOL_FALSE + STOP) is False

    def test_float(self):
        "May fail if you have strange floats"
        flt = 2.25
        assert loads(FLOAT + struct.pack('!d', 2.25) + STOP) == 2.25

    def test_int1(self):
        assert loads(INT1 + b'\0' + STOP) == 0
        assert loads(INT1 + b'\xff' + STOP) == -1

    def test_int2(self):
        assert loads(INT2 + b'\1\2' + STOP) == (1 << 8) + 2
        assert loads(INT2 + b'\xff'*2 + STOP) == -1

    def test_int4(self):
        assert loads(INT4 + b'\1\2\3\4' + STOP) == (1<<24)+(2<<16)+(3<<8)+4
        assert loads(INT4 + b'\xff'*4 + STOP) == -1

    # def test_long1(self):
    #     assert loads(LONG1 + b'\0' + STOP) == 0
    #     assert loads(dumps(1<<64)) == 1<<64
    #
    # def test_long4(self):
    #     assert loads(LONG4 + b'\0'*4 + STOP) == 0
    #     assert loads(dumps(1<<256*8)) == 1<<256*8

    def test_none(self):
        assert loads(NONE + STOP) is None

    def test_string1(self):
        #import pdb; pdb.set_trace()
        assert loads(STRING1 + b'\0' + STOP) == b''
        assert loads(STRING1 + b'\x01a' + STOP) == b'a'

    def test_string4(self):
        str = b'abc '*256
        assert loads(STRING4 + b'\0'*4 + STOP) == b''
        assert loads(STRING4 + struct.pack('!I', len(str))
                     + str + STOP) == str

    def test_unicode1(self):
        str = 'flygande bäckasiner söka hwila på mjuka tufvor qxz'
        enc = str.encode('utf-8')
        assert loads(UNICODE1 + b'\0' + STOP) == ''
        assert loads(UNICODE1 + bytes([len(enc)]) + enc + STOP) == str

    def test_unicode4(self):
        str = 'flygande bäckasiner söka hwila på mjuka tufvor qxz'
        enc = str.encode('utf-8')
        assert loads(UNICODE4 + b'\0'*4 + STOP) == ''
        assert loads(UNICODE4 + struct.pack('!I', len(enc))
                     + enc + STOP) == str

    def test_empty_dict(self):
        r = loads(EMPTY_DICT + STOP)
        assert type(r) is type({})
        assert r == {}

    def test_empty_list(self):
        r = loads(EMPTY_LIST + STOP)
        assert type(r) is type([])
        assert r == []

    def test_empty_tuple(self):
        r = loads(EMPTY_TUPLE + STOP)
        assert type(r) is type(())
        assert r == ()

    def test_string_memo(self):
        str = b'foo'
        d = loads(EMPTY_LIST + STRING1 + bytes([len(str)]) + str + APPEND +
                  INT1 + b'\1' + GET + APPEND + STOP)
        assert d[0] is d[1]
        assert d[0] == str

    def test_complex(self):
        "May fail if you have strange floats"
        assert loads(INT1 + b'\2' + INT1 + b'\1' + COMPLEX + STOP) == 1+2j

    def test_pop_mark(self):
        assert loads(NONE + MARK + INT1 + b'\1' + POP_MARK + STOP) is None

    def test_pop(self):
        assert loads(INT1 + b'\1' + INT1 + b'\2' + POP + STOP) == 1

    def test_dup(self):
        assert loads(EMPTY_LIST + MARK + NONE + DUP + EXTEND + STOP) == [
            None, None]

    def test_put_get(self):
        assert loads(MARK + INT1 + b'\1' + PUT + POP_MARK + INT1 + b'\0' +
                     GET + STOP) == 1

class Test_all:
    def test_all(self):
        obj = [ True, False, 1.0, 4711, 1<<48, None, 'Apa',
                'flygande bäckasiner söka hwila på mjuka tufvor qxz',
                { 'apa' : 'bepa' }, [1,2,3], (4,5,6), ([],)]

        assert type(obj[-1]) is type(())
        assert type(obj[-1][0]) is type([])
        obj[-1][0].append(obj)
        obj.append(obj)
        d = {}
        d['foo'] = d
        obj.append(d)

        
        f = BytesIO()
        dumper = Pickler(f)
        dumper.dump(obj)
        d = f.getvalue()
        m = list(dumper.memo.values())
        m.sort()
        l = loads(d)
        assert l[:-3] == obj[:-3]
        assert l[-3][0][0] is l
        assert l[-2] is l
        assert l[-1]['foo'] is l[-1]
