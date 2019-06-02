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

"""
Safe pickler

 A pickler that will only pickle known types

 The safe pickle protocol is similar, but not identical to the
 standard pickle protocol. It is stack based, with memoization to
 deal with recursive objects, and for efficiency.

 The list of supported types is fixed (although it may be extended
 if more types are added to python). There is however an extention
 mechanism, which will allow adding picklers and unpicklers for
 and unsupported type.
"""
import sys
if sys.version < '3':
    text_type = unicode
    binary_type = str
else:
    text_type =str
    binary_type = bytes
    
from types import *
from itertools import islice
from io import BytesIO
import struct, binascii

_supported = (bool, complex, dict, float, int,
              list, type(None), bytes, tuple,
              str, set)

#
#   Stack Data
#Position
#
#    TOP: 0    <= x
#      1: 1    <= y
#      2: 2
#    ...
#    n-2: n
#    n-1: MARK
# BOTTOM: []
#
# sending an 'e' gives the list [0, 1, 2, ..., n] as x
# 
# N.B. all multibyte args are network byte order (bigendian)

BOOL_TRUE   = b'1'  # Push Boolean True
BOOL_FALSE  = b'0'  # Push Boolean False
FLOAT       = b'F'  # Push float, arg is 8-byte IEEE double
INT1        = b'I'  # Push 1-byte signed int
INT2        = b'J'  # Push 2-byte signed int
INT4        = b'K'  # Push 4-byte signed int
LONG1       = b'L'  # Push long < 256 bytes (note that ints > 2^31 on 64-bit
                   # machines also end up here)
LONG4       = b'M'  # Push long < 4G bytes. Longer longs are not supported.
NONE        = b'N'  # Push None

# The following types are memoized
STRING1     = b'S'  # Push string < 256 bytes
STRING4     = b'T'  # Push string < 4G bytes. Longer not supported
UNICODE1    = b'U'  # Push utf-8 encoded unicode < 256 bytes (!= characters)
UNICODE4    = b'V'  # Push unicode < 4G bytes
EMPTY_DICT  = b'}'  # Push empty dict
EMPTY_LIST  = b']'  # Push empty list
EMPTY_TUPLE = b')'  # Push empty tuple

MARK        = b'('  # Push special mark object
POP_MARK    = b'!'  # Pop until mark and discard
POP         = b'^'  # Pop 1 item and discard
DUP         = b'2'  # Duplicate top object

COMPLEX     = b'c'  # Pop 2 items and push complex (x+jy)
SETITEM     = b's'  # Pop 2 items, set as key+value in dict on (new) top
UPDATE      = b'u'  # Pop pairs until mark, update dict on (new) top
APPEND      = b'a'  # Pop 1 item, append to list on (new) top
EXTEND      = b'e'  # Pop until mark, extend list on (new) top
TUPLE       = b't'  # Pop until mark, make tuple
EXTENSION   = b'x'  # Pop 2 items, push result from registed unpickler[x](y)
                   # and memoize result
MKSET       = b'%'  # Make set from list
GET         = b'g'  # Push object from memo, id from stack
PUT         = b'p'  # memoize top of stack (without poping it)

STOP        = b'.'  # Marks end of pickle

class PickleError(Exception):
    pass

class PicklingError(PickleError):
    pass

class UnpicklingError(PickleError):
    pass

class ExtensionType(object):

    _empty = object()

    def __init__(self):
        self.extByType = {}
        self.extByTag = {}

    @staticmethod
    def _cantpickle(obj):
        raise PicklingError("No pickler registered for object: %r" % (obj,))

    @staticmethod
    def _cantunpickle(code, data):
        raise UnpicklingError("No unpickler registered for code %r" % (code,))

    def register(self, typeobj, code=_empty, pickler=_cantpickle,
                 unpickler=_cantunpickle):
        """
        Register a new type for extension pickling.

        typeobj   - type object to handle (or class object for old classes)
        code      - a string identifier for the object type,
                    defaults to typeobj.__name__
                    if None, then just pickle whatever pickler returns
                    as-is. (i.e. whatever pickler returns will be the
                    result of unpickling.)
        pickler   - gets called with object as it's single object during
                    pickling
        unpickler - gets called with code tag and whatever pickler returned
                    during unpickling
        """
        if typeobj in self.extByType:
            raise PickleError("Type %r already registered" % (typeobj,))
        if code is self._empty:
            code = typeobj.__name__
        if code in self.extByTag:
            raise PickleError("Typecode %r already registered" % (code,))

        self.extByType[typeobj] = lambda x: (code, pickler(x))
        if code is not None:
            self.extByTag[code] = unpickler

    def setpickler(self, typeobj, pickler):
        """
        Set pickler of already registered extension type
        """
        if typeobj not in self.extByType:
            raise PickleError("Type %r not registered" % (typeobj,))

        self._extByType = pickler

    def setunpickler(self, code, unpickler):
        """
        Set unpickler of already registered extension type
        """
        if code not in self.extByTag:
            raise PickleError("Type code %r not registered" % (code,))

        self._extByTag = unpickler

    def getpickler(self, typeobj):
        """
        Get pickler for type
        """
        return self.extByType.get(typeobj)

    def getunpickler(self, code):
        """
        Get unpickler for object
        """
        return self.extByTag.get(code)

Extension = ExtensionType()

class _Empty:
    pass

def stateExtension(typeobj, code=Extension._empty, registry=Extension):
    """
    Decorator for registering an object as an extension for spickle.

    The object is pickled and unpickled using the objects __getstate__
    and __setstate__ methods, respectively. If either method does not
    exist, direct manipulation on __dict__ will be used instead (as for
    pickle).

    The object is created using __new__ if it is a new-style class and
    by assigning __class__ if it is an oldstyle class. If this breaks,
    you get to keep both pieces. This function is only to make conversion
    from pickle easier.

        typeobj   - type object to handle (or class object for old classes)
        code      - a string identifier for the object type,
                    defaults to typeobj.__name__
    """
    #if not hasattr(typeobj, '__getstate__'):
    #    raise TypeError("Object must have '__getstate__' method.")
    if hasattr(typeobj, '__new__'):
        def unpickler(code, state):
            inst = typeobj.__new__(typeobj)
            if hasattr(inst, '__setstate__'):
                inst.__setstate__(state)
            else:
                inst.__dict__.update(state)
            return inst
    else:
        def unpickler(code, state):
            inst = _Empty()
            inst.__class__ = typeobj
            if hasattr(inst, '__setstate__'):
                inst.__setstate__(state)
            else:
                inst.__dict__.update(state)
            return inst
    def pickler(inst):
        if hasattr(inst, '__getstate__'):
            return inst.__getstate__()
        else:
            return inst.__dict__
    registry.register(typeobj, code, pickler=pickler, unpickler=unpickler)
    return typeobj

# since decimal.Decimal relies on the __reduce__ mechanism when using cPickle,
# we have to supply an explicit mechanism to handle it since spickle doesn't
# support this out of the box
import decimal
def _decimalPickler(inst):
    return inst.__reduce__()[1]

def _decimalUnpickler(code, state):
    return decimal.Decimal(*state)

Extension.register(decimal.Decimal, pickler=_decimalPickler,
                   unpickler=_decimalUnpickler)

_Pdispatch = {}
def saver(objtype):
    def save(func):
        _Pdispatch[objtype] = func
        return func
    return save

class Pickler(object):
    _BATCHSIZE = 1000

    dispatch = _Pdispatch
        
    def __init__(self, file):
        self.write = file.write
        self.memo = {}

    
    def dump(self, obj):
        "Write pickled representation to file"
        self.save(obj)
        self.write(STOP)

    def memoize(self, obj):
        """
        Store object in the memo. The objects are implicitly memoized,
        so there is no need to tell the receiver that an object may
        repeat.
        """
        assert id(obj) not in self.memo
        memo_len = len(self.memo)
        self.memo[id(obj)] = memo_len, obj

    def save(self, obj):
        # Check memo
        x = self.memo.get(id(obj))
        if x:
            self.get(x[0])
            return

        objtype = type(obj)

        f = self.dispatch.get(objtype)
        if f:
            f(self, obj)
            return

        # Check extension
        if isinstance(objtype , object):
            objtype = obj.__class__
        f = Extension.getpickler(objtype)
        if f:
            code, nobj = f(obj)
            self.save(nobj)
            if code is None:
                oid = id(nobj)
                if oid not in self.memo:
                    self.memoize(nobj)
                    self.write(PUT)
                self.memo[id(obj)] = self.memo[oid]
            else:
                self.save(code)
                self.write(EXTENSION)
                self.memoize(obj)
            return

        raise PicklingError("Unpicklable object: %r (%r)" % (type(obj), obj,))

    def get(self, objid):
        """
        Write memo retrieval instructions
        """
        self.save_int(objid)
        self.write(GET)

    @saver(bool)
    def save_bool(self, obj):
        if obj:
            self.write(BOOL_TRUE)
        else:
            self.write(BOOL_FALSE)

    @saver(int)
    def save_int(self, obj, pack=struct.pack):
        """
        Write an integer
        """
        neg = 0
        if obj < 0:
            neg = -1
        obj ^= neg # Apply 1's complement (for byte sizing)
        if obj <= 0x7f:
            obj ^= neg
            self.write(INT1 + bytes([obj&0xff]))
        elif obj <= 0x7fff:
            obj ^= neg
            self.write((INT2 + pack('!h', obj)))
        elif obj <= 0x7fffffff:
            obj ^= neg
            self.write((INT4 + pack('!i', obj)))
        else:
            data = encode_long(obj)
            n = len(data)
            if n < 256:
                self.write(LONG1 + bytes([n]) + data)
            else:
                self.write(LONG4 + pack('!i', n) + data)

    @saver(float)
    def save_float(self, obj, pack=struct.pack):
        self.write((FLOAT + pack('!d', obj)))

    @saver(type(None))
    def save_none(self, obj):
        self.write(NONE)

    @saver(bytes)
    def save_string(self, obj, pack=struct.pack):
        self.memoize(obj)
        l = len(obj)
        if l < 256:
            self.write(STRING1 + bytes([l]) + obj)
        else:
            self.write(STRING4 + pack('!I', l) + obj)

    @saver(str)
    def save_unicode(self, obj, pack=struct.pack):
        self.memoize(obj)
        s = obj.encode('utf-8')
        l = len(s)
        if l < 256:
            self.write(UNICODE1 + bytes([l]) + s)
        else:
            self.write(UNICODE4 + pack('!I', l) + s)

    @saver(dict)
    def save_dict(self, obj):
        save = self.save
        write = self.write

        write(EMPTY_DICT)
        self.memoize(obj)

        items = iter(obj.items())
        while True:
            batch = list(islice(items, self._BATCHSIZE))
            if not batch:
                break
            if len(batch) == 1:
                (k,v), = batch
                save(k)
                save(v)
                write(SETITEM)
            else:
                write(MARK)
                for k,v in batch:
                    save(k)
                    save(v)
                write(UPDATE)

    @saver(list)
    def save_list(self, obj):
        save = self.save
        write = self.write

        write(EMPTY_LIST)
        self.memoize(obj)

        vals = iter(obj)
        while True:
            batch = list(islice(vals, self._BATCHSIZE))
            if not batch:
                break
            if len(batch) == 1:
                save(batch[0])
                write(APPEND)
            else:
                write(MARK)
                for x in batch:
                    save(x)
                write(EXTEND)

    @saver(tuple)
    def save_tuple(self, obj):
        write = self.write
        save = self.save

        if len(obj) == 0:
            write(EMPTY_TUPLE)
            self.memoize(obj)
            return

        write(MARK)
        for element in obj:
            save(element)

        x = self.memo.get(id(obj))
        if x:
            # obj has appeared in memo while saving, so tuple is recursive
            # and has already been saved. Clear to mark and get it from
            # memo instead.
            write(POP_MARK)
            self.get(x[0])
            return

        self.write(TUPLE)
        self.memoize(obj)

    @saver(complex)
    def save_complex(self, obj):
        save = self.save
        save(obj.imag)
        save(obj.real)
        self.write(COMPLEX)

    @saver(set)
    def save_set(self, obj):
        self.save(list(obj))
        self.write(MKSET)

_Udispatch = {}
def loader(code):
    def load(func):
        _Udispatch[code] = func
        return func
    return load

def memoized(func):
    def memo(self):
        func(self)
        self.memo.append(self.stack[-1])
    return memo

class Unpickler(object):
    dispatch = _Udispatch
    
    class _Stop(Exception):
        def __init__(self, value):
            self.value = value
    
    def __init__(self, file):
        self.read = file.read
        self.memo = []

    def load(self):
        self.mark = object()
        self.stack = []
        self.markstack = [] # remember where we push marks
        self.push = self.stack.append
        self.pop = self.stack.pop
        read = self.read
        dispatch = self.dispatch
        try:
            while True:
                dispatch[read(1)](self)
        except self._Stop as e:
            return e.value
        except KeyError:
            raise UnpicklingError("This does not appear to be a spickle")

    @loader('')
    def load_eof(self):
        raise EOFError("Stack contents: %r" % (self.stack,))
    
    @loader(STOP)
    def load_stop(self):
        raise self._Stop(self.pop())

    @loader(BOOL_TRUE)
    def load_true(self):
        self.push(True)

    @loader(BOOL_FALSE)
    def load_false(self):
        self.push(False)

    @loader(FLOAT)
    def load_floate(self, unpack=struct.unpack):
        self.push(unpack('!d', self.read(8))[0])

    @loader(INT1)
    def load_int1(self, unpack=struct.unpack):
        self.push(unpack('!b', self.read(1))[0])

    @loader(INT2)
    def load_int2(self, unpack=struct.unpack):
        self.push(unpack('!h', self.read(2))[0])

    @loader(INT4)
    def load_int4(self, unpack=struct.unpack):
        self.push(unpack('!i', self.read(4))[0])
    
    @loader(LONG1)
    def load_long1(self):
        n = ord(self.read(1))
        self.push(decode_long(self.read(n)))

    @loader(LONG4)
    def load_long4(self, unpack=struct.unpack):
        n = unpack('!I', self.read(4))[0]
        self.push(decode_long(self.read(n)))

    @loader(NONE)
    def load_none(self):
        self.push(None)

    @loader(STRING1)
    @memoized
    def load_string1(self):
        n = ord(self.read(1))
        self.push(self.read(n))

    @loader(STRING4)
    @memoized
    def load_string4(self, unpack=struct.unpack):
        n = unpack('!I', self.read(4))[0]
        self.push(self.read(n))

    @loader(UNICODE1)
    @memoized
    def load_unicode1(self):
        n = ord(self.read(1))
        self.push(str(self.read(n), 'utf-8'))

    @loader(UNICODE4)
    @memoized
    def load_unicode4(self, unpack=struct.unpack):
        n = unpack('!I', self.read(4))[0]
        self.push(str(self.read(n), 'utf-8'))

    @loader(EMPTY_DICT)
    @memoized
    def load_empty_dict(self):
        self.push({})
        
    @loader(EMPTY_LIST)
    @memoized
    def load_empty_list(self):
        self.push([])
        
    @loader(EMPTY_TUPLE)
    @memoized
    def load_empty_tuple(self):
        self.push(())

    @loader(MARK)
    def load_mark(self):
        self.markstack.append(len(self.stack))
        self.push(self.mark)

    @loader(POP_MARK)
    def pop_mark(self):
        ssize = len(self.stack)
        mindex = self.markstack.pop()
        while self.markstack and (mindex >= ssize or
                                  self.stack[mindex] is not self.mark):
            # In case something unaware poped the mark
            mindex = self.markstack.pop()
        if self.stack[mindex] is self.mark:
            rv = self.stack[mindex+1:]
            del self.stack[mindex:]
            return rv
        else:
            raise UnpicklingError("Mark not found (Stack: %r)" % (self.stack,))

    @loader(POP)
    def load_pop(self):
        if self.pop() is self.mark:
            self.markstack.pop()

    @loader(DUP)
    def load_dup(self):
        obj = self.stack[-1]
        if obj is self.mark:
            self.markstack.append(len(self.stack))
        self.push(obj)

    @loader(COMPLEX)
    def load_complex(self):
        x = self.pop()
        y = self.pop()
        self.push(x + 1j*y)

    @loader(SETITEM)
    def load_setitem(self):
        v = self.pop()
        k = self.pop()
        self.stack[-1][k] = v

    @loader(UPDATE)
    def load_update(self):
        data = iter(self.pop_mark())
        dct = self.stack[-1]
        try:
            while True:
                k = next(data)
                v = next(data)
                dct[k] = v
        except StopIteration:
            pass

    @loader(APPEND)
    def load_append(self):
        v = self.pop()
        self.stack[-1].append(v)

    @loader(EXTEND)
    def load_extend(self):
        data = self.pop_mark()
        self.stack[-1].extend(data)

    @loader(TUPLE)
    @memoized
    def load_tuple(self):
        self.push(tuple(self.pop_mark()))

    @loader(EXTENSION)
    @memoized
    def load_extension(self):
        code = self.pop()
        data = self.pop()
        f = Extension.getunpickler(code)
        if f is None:
            raise UnpicklingError("No unpickler registered for '%s'" % (code,))
        self.push(f(code, data))

    @loader(GET)
    def load_get(self):
        i = self.pop()
        self.push(self.memo[i])

    @loader(PUT)
    @memoized
    def load_put(self):
        pass

    @loader(MKSET)
    def load_set(self):
        self.push(set(self.pop()))

def encode_long(x):
    """
    Encode long to 2's complement big-endian binary string.
    """
    if x == 0:
        return ''
    if x > 0:
        ashex = '%X' % (x,)
        nibbles = len(ashex)
        if nibbles & 1:
            ashex = '0' + ashex
        elif int(ashex[0], 16) >= 8:
            # high bit set, add byte of \0 to keep it from turning negative
            ashex = '00' + ashex
    else:
        # Build 2's complement (1 << nbits) - (-x).
        ashex = '%X' % (-x,)
        nibbles = len(ashex)
        x += 3 << (nibbles * 4)  # 3 guarantees bit set in extra nibble
        ashex = ('%X' % (x,))[1:] # Chop off extra nibble
        if int(ashex[0], 16) < 8:
            # high bit not set, add 'f' to keep it from turning positive
            ashex = 'f' + ashex
        nibbles = len(ashex)
        if nibbles & 1: # Make even bytes
            ashex = 'f' + ashex

    return binascii.unhexlify(ashex)


def decode_long(data):
    """
    decode a 2's complement big-endian binary string
    """
    nbytes = len(data)
    if nbytes == 0:
        return 0
    n = int(binascii.hexlify(data), 16)
    if data[0] >= 0x80:
        n -= 1 << (nbytes * 8)
    return n

def dumps(obj, ignored=None, ign2=None):
    f = BytesIO()
    p = Pickler(f)
    try:
        p.dump(obj)
    except PicklingError as e:
        raise PicklingError(e.message)
    return f.getvalue()

def loads(strin):
    f = BytesIO(strin)
    u = Unpickler(f)
    try:
        return u.load()
    except UnpicklingError:
        print('Error unpickling %r' % (strin,))
        raise

def dump(obj, file):
    p = Pickler(file)
    p.dump(obj)

def load(file):
    u = Unpickler(file)
    return u.load()
