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

needs_replacing = set()
try:
    from bson import _cbson
    needs_replacing.add(_cbson)
except ImportError:
    pass
from importlib import reload
try:
    from pymongo import _cmessage
    needs_replacing.add(_cmessage)
except ImportError:
    pass

import importlib

if needs_replacing:
    class Finder(object):
        def find_module(self, fullname, path=None):
            if fullname in ('bson._cbson', 'pymongo._cmessage'):
                raise ImportError

    import sys, bson, pymongo, pymongo.message
    sys.meta_path.append(Finder())

    for name, module in list(sys.modules.items()):
        if module in needs_replacing:
            del sys.modules[name]

    for module, attr in ((bson, '_cbson'), (pymongo, '_cmessage'),
                         (pymongo.message, '_cmessage')):
        try:
            delattr(module, attr)
        except AttributeError:
            pass

    del attr, name, module, needs_replacing

    importlib.reload(bson)
    importlib.reload(pymongo.message)

    assert not bson.has_c()
    assert not pymongo.has_c()


import types, functools
import bson, bson.son, contextlib, struct
from bson.binary import Binary, OLD_UUID_SUBTYPE
from gridfs import GridFS
from collections import OrderedDict
from bson import _elements_to_dict, DBRef
from pytransact import spickle

max_bson_element_size = 2 ** 18

Extension = spickle.ExtensionType()

def register(typeobj, code=spickle.ExtensionType._empty):
    spickle.stateExtension(typeobj, code, Extension)


_bson_element_to_bson = bson._element_to_bson
if _bson_element_to_bson.__globals__ == globals():
    # bson._element_to_bson already patched, reload bson to get original
    reload(bson)
    _bson_element_to_bson = bson._element_to_bson

def _element_to_bson(key, value, check_keys, uuid_subtype):
    objtype = type(value)
    if isinstance(objtype,object): #objtype == types.InstanceType:
        objtype = value.__class__
    encoder = Extension.getpickler(objtype)
    if not encoder and hasattr(value, '__getstate__'):
        for _type in type(value).__mro__:
            encoder = Extension.getpickler(_type)
            if encoder:
                break
    if encoder:
        code, value = encoder(value)
        if isinstance(value, dict):
            assert '__customtype__' not in value
            value = value.copy()
            value['__customtype__'] =  code

    if isinstance(value, (list, tuple)):
        name = bson._make_c_string(key)
        as_dict = OrderedDict((str(k), v) for (k, v) in enumerate(value))
        return bson.BSONARR + name + bson._dict_to_bson(as_dict, check_keys,
                                                        uuid_subtype, False)

    return _bson_element_to_bson(key, value, check_keys, uuid_subtype)

bson._element_to_bson = _element_to_bson

# if _elements_to_dict.func_code.co_argcount == 4:
#     # old pymongo didn't take compile_re
#     _elements_to_dict = functools.wraps(_elements_to_dict)(
#         lambda a,b,c,d,e,fn=_elements_to_dict : fn(a,b,c,d))

# if _elements_to_dict.func_code.co_argcount == 3:
#     # older pymongo didn't take uuid_subtype
#     _elements_to_dict = functools.wraps(_elements_to_dict)(
#         lambda a,b,c,d,e,fn=_elements_to_dict : fn(a,b,c))


def _get_object(data, position, as_class, tz_aware,
                uuid_subtype=OLD_UUID_SUBTYPE, compile_re=True):
    obj_size = struct.unpack("<i", data[position:position + 4])[0]
    encoded = data[position + 4:position + obj_size - 1]
    object = _elements_to_dict(encoded, as_class, tz_aware, uuid_subtype,
        compile_re)
    position += obj_size
    if "$ref" in object:
        return (DBRef(object.pop("$ref"), object.pop("$id"),
                      object.pop("$db", None), object), position)
    if '__customtype__' in object:
        code = object.pop('__customtype__')
        unpickler = Extension.getunpickler(code)
        if not unpickler:
            raise ValueError("Unknown custom type: %r" % code)
        return unpickler(code, object), position

    return object, position


_bson_get_object = bson._get_object
def _get_object(data, position, obj_end, opts, dummy=None):
    if 'dummy' in _bson_get_object.__code__.co_varnames:
        obj, position = _bson_get_object(data, position, obj_end, opts, dummy)
    else:
        obj, position = _bson_get_object(data, position, obj_end, opts)
    if '__customtype__' in obj:
        code = obj.pop('__customtype__')
        unpickler = Extension.getunpickler(code)
        if not unpickler:
            raise ValueError("Unknown custom type: %r" % code)
        obj = unpickler(code, obj)
    return obj, position


_bson_get_binary = bson._get_binary
if _bson_get_binary.__globals__ == globals():
    # bson._get_binary already patched, reload bson to get old _get_binary
    reload(bson)
    _bson_get_binary = bson._get_binary

if _bson_get_binary.__code__.co_argcount == 5:
    # old pymongo didn't take compile_re
    _bson_get_binary = functools.wraps(_bson_get_binary)(
        lambda a,b,c,d,e,f,fn=_bson_get_binary : fn(a,b,c,d,e))

if _bson_get_binary.__code__.co_argcount == 4:
    # older pymongo didn't take uuid_subtype
    _bson_get_binary = functools.wraps(_bson_get_binary)(
        lambda a,b,c,d,e,f,fn=_bson_get_binary : fn(a,b,c,d))

def _get_binary(data, position, as_class, tz_aware,
                uuid_subtype=OLD_UUID_SUBTYPE,compile_re=True):
    value, position = _bson_get_binary(data, position, as_class, tz_aware,
                                       uuid_subtype, compile_re)
    try:
        if value.subtype == 0:
            return str(value), position
    except AttributeError:
        pass
    return value, position

try:
    bson._element_getter[bson.BSONOBJ] = bson._get_object = _get_object
    bson._element_getter[bson.BSONBIN] = bson._get_binary = _get_binary
except AttributeError:
    bson._ELEMENT_GETTER[bson.BSONOBJ] = bson._get_object = _get_object
    bson._ELEMENT_GETTER[bson.BSONBIN] = bson._get_binary = _get_binary


import decimal
def _decimalPickler(obj):
    return {'value': obj.as_tuple()}

def _decimalUnpickler(code, obj):
    return decimal.Decimal(obj['value'])

Extension.register(decimal.Decimal, pickler=_decimalPickler,
                   unpickler=_decimalUnpickler)


def _setPickler(obj):
    return {'value': list(obj)}

def _setUnpickler(code, obj):
    return set(obj['value'])

Extension.register(set, pickler=_setPickler, unpickler=_setUnpickler)


def _strPickler(obj):
    try:
        return obj #.decode('utf-8')
    except UnicodeDecodeError:
        return bson.binary.Binary(obj)

Extension.register(str, pickler=_strPickler)
