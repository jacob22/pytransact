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

import bson, collections, decimal, gridfs, os, hashlib, itertools
from copy import copy, deepcopy
#from types import NoneType
from io import BytesIO

from pytransact import (contextbroker, custombson, exceptions, metaproxy,
                        patterns)
from pytransact.queryops import *
from pytransact.object.property import *
from pytransact.object.restriction import *

class mAttribute(patterns.mExtendable):

    # Don't make this anything but staticmethod or things will break bigtime!
    @staticmethod
    def __new__(cls, name, bases, bdict):
        """
        Control that at only one Attribute class is inherited.

        Arguments: cls - the base class
                   name - the attribute name
                   bases - base classes
                   bdict - start context
        Returns:   Attribute class object
        """
        if name == '__extend__':
            return super(mAttribute, cls).__new__(cls, name, bases, bdict)

        attrBase = False
        for b in bases:
            if isinstance(b, mAttribute):
                if attrBase:
                    # You can only inherit one Attribute base class
                    raise SyntaxError('You can only inherit one Attribute base class.')
                attrBase = True

        ob = super(mAttribute, cls).__new__(cls, name, bases, bdict)

        default = bdict.get('default')
        if default:
            if not isinstance(default, (list, tuple, set, frozenset, dict)):
                default = [ default ]

            if issubclass(ob, Enum):
                ob.coerceValueList(default, name)
            else:
                default = ob.coerceValueList(default, name)

            ob.default = default
        elif not hasattr(ob, 'default'):
            ob.default = []

        ob.name = name

        return ob

    def __call__(cls, *arg, **kw):
        """
        Handle inherit or instansiation of an Attribute object.

        Arguments: arg, kw - inherit/instansiation arguments
        Returns:   new temporary subclass of the original one
        """
        if arg and arg[0] == '__extend__':
            for name, fun in arg[2].items():
                if callable(fun):
                    setattr(cls, name, fun)
            return cls

        def iterKeywords():
            for k, ob in kw.items():
                if isinstance(ob, (list, tuple, set, frozenset)):
                    for part in ob:
                        yield k, part
                else:
                    yield k, ob

        # The Attribute base class may not be used outright
        if cls is Attribute:
            # You must use a derived class of Attribute
            raise SyntaxError('You must use a derived class of Attribute.')

        # Restriction/property definitions for this subpart
        # of the attribute

        preRest = []
        postRest = []
        hintRest = []
        preProp = []
        postProp = []
        hintProp = []

        # Verify and sort restrictions and properties
        for k, ob in iterKeywords():
            # All 'inherit arguments' must be modifiers of some kind
            if not isinstance(ob, (Restriction, Property)):
                # All arguments must be a modifier
                raise TypeError('All arguments must be a modifier.')

            # The attribute defines which modifier is applicable
            if not isinstance(ob, cls._validModifiers):
                # Modifier not applicable to this attribute type
                raise TypeError('The modifier %r can not be applied to this type.' % [ob])
            # Sort modifiers by use and type
            if k == 'pre':
                if isinstance(ob, Restriction):
                    preRest.append(ob)
                else:
                    preProp.append(ob)

            elif k == 'post':
                if isinstance(ob, Restriction):
                    postRest.append(ob)
                else:
                    postProp.append(ob)

            elif k == 'hint':
                if isinstance(ob, Restriction):
                    hintRest.append(ob)
                else:
                    hintProp.append(ob)

            else:
                # Unrecognised restriction
                raise TypeError("Unknown restriction '%s'. Restrictions must be "
                                "of types 'pre', 'post', 'hint' or unspecified." % [k])

        # Unspecified modifiers become both pre and post, but not hint
        for ob in arg:
            if not isinstance(ob, cls._validModifiers):
                #  Modifier not applicable to this attribute type
                raise TypeError('The modifier %r can not be applied to this type.' % [ob])
            if isinstance(ob, Restriction):
                preRest.append(ob)
                postRest.append(ob)

            elif isinstance(ob, Property):
                preProp.append(ob)
                postProp.append(ob)

            else:
                # Attribute argument must be a Property or a Restriction
                raise TypeError('The parameter %r is not a modifier' % [ob])

        # Here comes the tricky bit. Instead of returning the Attribute
        # class object, return a unique per-attribute created class
        # which contains the sum of all modifiers througout the
        # inheritance chain.
        #
        # This special object inherits both the original Attribute
        # class as well as a special 'Modified' marker so that the
        # TO later can be certain there's no funny stuff mixed into
        # its list of attributes.

        return type('meta' + cls.__name__, (cls,), {
            'preRestrictions': cls.preRestrictions + tuple(preRest),
            'postRestrictions': cls.postRestrictions + tuple(postRest),
            'hintRestrictions': cls.hintRestrictions + tuple(hintRest),
            'preProperties': cls.preProperties + tuple(preProp),
            'postProperties': cls.postProperties + tuple(postProp),
            'hintProperties': cls.hintProperties + tuple(hintProp) })

    _instantiate = type.__call__

class MapValue(tuple):

    def __eq__(self, other):
        return tuple(self) == tuple(other)


class Attribute(object,metaclass=mAttribute):
    """
    The base Attribute class, mimicks a sequence object

    To begin with attributes must be derived from the this Attribute
    class, it's not possible to use Attribute() directly.

    Attributes are not meant to be instanciated as such. Instead when
    declaring an attribute in a toc, you declare it as subclass of one
    of the standard Attribute-subclasssed attribute. Then you supply a
    list of restrictions (or none) as arguments to the Attribute
    declaration.

    E.g class myAttr(LimitedString(QuantityMax(1)))

    You may specify restrictions as pre, post or hint (both pre and
    post is default when unspecified) by simply providing the
    restrictions as a keyword tuple.
    """

    _validModifiers = ()	# Derived classes list of acceped modifiers
    _validQueryOps = ()		# List of acceped query operators

    preRestrictions = ()	# Instance list of pre restrictions
    postRestrictions = ()	#	- " -	   post    -"-
    hintRestrictions = ()	#	- " -	   hint    -"-
    preProperties = ()		#	- " -	   pre properties
    postProperties = ()		#	- " -	   post   -"-
    hintProperties = ()		#	- " -	   hint   -"-
    _location = None

    def __init__(self, name):
        """
        Initialise the object.

        Arguments: name - the attribute name
        Returns:   None
        """
        self.name = name
        self.toi = None

    def __repr__(self):
        if self.toi is None:
            return '<unbound %s at %#x>' % (self.__class__.__name__, id(self))
        return '<%s(%s): %r>' % (self.__class__.__name__, id(self), self.value)

    def __get__(self, toi, _type=None):
        """
        Implement 'Declarator' property 'get' method.

        Arguments: toi - the toi object to get data from
        Returns:   Special copy of this object with the approiate
                   toi filled in for attribute retrieval purposes
        """
        if toi is None:
            return self

        if self.toi is not None:
            if self.toi._deleted:
                raise exceptions.ToiDeletedError(self.toi._fullname,
                                                 self.toi.id[0])
            return self

        x = self.__class__._instantiate(self.name)
        x.__dict__.update(self.__dict__)
        x.toi = toi
        return x

    def __set__(self, toi, val):
        """
        Implement 'Declarator' property 'set' method.

        Arguments: toi - the toi object to put data in
                   val - the value to set (must be a sequence object)
        Returns:   None
        """
        if toi is None:
            raise exceptions.cLookupError("Trying to access non-instantiated attribute '%s'"%(self.name))

        if toi._deleted:
            raise exceptions.ToiDeletedError(toi._fullname, toi.id[0])

        if not contextbroker.ContextBroker().context.canWrite(toi, self.name):
            raise exceptions.cAttrPermError(self.name, toi._fullname, toi.id[0])

        while isinstance(val, Attribute):
            val = val.value	# Obtain the attribute value, rather than
            			# the object.

        if not isinstance(val, (list, tuple, set, frozenset, dict)):
            val = [ val ]

        val = self.coerceValueList(val)
        myval = getattr(toi, self.name).value
        if myval == val: # no-op
            return
        if self.name in toi._orgAttrData: # Old saved value
            if toi._orgAttrData[self.name] == val: # implicit rollback
                del toi._orgAttrData[self.name]
        else: # New modification, store original value
            toi._orgAttrData[self.name] = myval

        toi._attrData[self.name] = val
        toi._register()

    @property
    def empty(self):
        return self.default.__class__()

    @property
    def value(self):
        """
        The entire value list.
        """
        # BLM attribute call
        if self.computed and self.toi is None:
            return self.on_computation()

        ######## self.toi MUST exist from this point

        if self.toi is None:
            raise LookupError('Tried to use an attribute with no toi: %s' % [self._xlatKey])

        if self.toi._deleted:
            raise exceptions.ToiDeletedError(self.toi._xlatKey, self.toi.id[0])

        if self.computed:
            return self.on_computation(self.toi)

        try:
            val = self.toi._attrData[self.name]
        except KeyError:
            val = self.toi._requestAttribute(self)

            # Turn objectid results into TOIrefs, if necessary
            if isinstance(self, ToiRef) and val:
                tt = self.getrest(ToiType, post=True)
                from . import to
                toc = to.TO
                if tt:
                    toc = tt.validToiType
                val = [ toc._create(v) for v in val ]
            self.toi._attrData[self.name] = val

        return val

    @property
    def oldvalue(self):
        """
        The old value list.
        """
        try:
            val = self.toi._orgAttrData[self.name]
            if val is None:
                self.toi._requestAttribute(self)
                val = self.toi._orgAttrData[self.name]
        except KeyError:
            deleted = self.toi._deleted
            self.toi._deleted = False
            try:
                val = self.value
            finally:
                self.toi._deleted = deleted

        return val

    def validateValues(self, pre=False, value=None):
        """
        Validate the attribute values against the restrictions.

        Arguments: pre - validate pre restrictions, else post validate,
                   value - the value to validate, or our own if None
        Returns:   None (can raise AttrValueError exception
        """
        if pre:
            restList = self.preRestrictions
        else:
            restList = self.postRestrictions
        if value is None:
            value = self.value

        for rest in restList:
            try:
                rest.validateValueList(value)
            except (RestrictionError, RestrictionErrorList) as e:
                if self.toi is not None:
                    raise exceptions.AttrValueError(self._xlatKey, self.toi._xlatKey,
                                         self.toi.id[0], e)
                else:
                    raise exceptions.AttrValueError(self._xlatKey, None, None, e)

    def getprop(self, property, pre=False, post=False, hint=False):
        """
        Investigates the presence of a certain property.

        Arguments: property - the property to look for
                   pre (bool) - Search in pre-defs only
                   post (bool) - Search in post-defs only
                   hint (bool) - Search in hint-defs only
        Returns:   Property or None
        """
        # pre = post == False => search in both
        if pre == False and post == False and hint == False:
            pre = post = True

        if pre:
            for prop in self.preProperties:
                if isinstance(prop, property):
                    return prop

        if post:
            for prop in self.postProperties:
                if isinstance(prop, property):
                    return prop

        if hint:
            for prop in self.hintProperties:
                if isinstance(prop, property):
                    return prop

        return None

    def getrest(self, restriction, pre=False, post=False, hint=False):
        """
        Investigates the presence of a certain restriction.

        Arguments: restriction - the restriction to look for
                   pre (bool) - Search in pre-defs only
                   post (bool) - Search in post-defs only
                   hint (bool) - Search in hint-defs only
        Returns:   Restriction or None
        """
        # pre = post == False => search in both
        if pre == False and post == False and hint == False:
            pre = post = True

        if pre:
            for rest in self.preRestrictions:
                if isinstance(rest, restriction):
                    return rest

        if post:
            for rest in self.postRestrictions:
                if isinstance(rest, restriction):
                    return rest

        if hint:
            for rest in self.hintRestrictions:
                if isinstance(rest, restriction):
                    return rest

        return None

    @property
    def computed(self):
        return hasattr(self, 'on_computation')

    def __getitem__(self, idx):
        """
        Return a specified value in the sequence list.

        Arguments: idx - slice object
        Returns:   Specified value
        """
        return self.value[idx]

    def __setitem__(self, idx, val):
        """
        Set a value in a specific position.

        Arguments: idx - index
                   val - value
        Returns:   None
        """
        d = self.value[:]
        if len(d) < idx:
            raise exceptions.cIndexError('Attribute assignment index out of range')

        val = self.coerceValue(val)
        if d[idx] == val:
            return
        d[idx] = val
        self.__set__(self.toi, d)

    def __delitem__(self, idx):
        """
        Delete a value in a specific position.

        Arguments: idx - index
        Returns:   None
        """
        d = self.value[:]
        del d[idx]
        self.__set__(self.toi, d)

    def __bool__(self):
        """
        Returns True when this object is considered to be non zero.

        An attribute that's not connected to a TOI is always non
        zero. An attribute connected to a TOI will return True iff
        __len__ is non zero.

        Arguments: None
        Returns: True/False
        """
        if self.toi:
            return bool(len(self))
        return True

    def __len__(self):
        """
        Return the length of the value.

        Arguments: None
        Returns:   the length as an integer.
        """
        return len(self.value)

    def add(self, val):
        """
        Append a value to the end of the value list.

        Arguments: val - value
        Returns:   None
        """
        d = self.value[:]
        val = self.coerceValue(val)
        if val in d:
            return
        d.append(val)
        self.__set__(self.toi, d)

    def append(self, val):
        """
        Append a value to the end of the value list.

        Arguments: val - value
        Returns:   None
        """
        d = self.value[:]
        val = self.coerceValue(val)
        d.append(val)
        self.__set__(self.toi, d)

    def extend(self, val):
        """
        Extend a list of values to the end of the value list.

        Arguments: val - value
        Returns:   None
        """
        d = self.value[:]
        val = self.coerceValueList(val)
        d.extend(val)
        self.__set__(self.toi, d)

    def discard(self, val):
        try:
            self.remove(val)
        except ValueError:
            pass

    def remove(self, val):
        """
        Remove a value in a specific position.

        Arguments: val - value
        Returns:   None
        """
        d = self.value[:]
        val = self.coerceValue(val)
        d.remove(val) # Raises exception if not found
        self.__set__(self.toi, d)

    def get(self, key, default=None):
        """
        Get value for key, or default if key does not exist.

        Arguments: key - key
                   default - default to return (default: None)
        Returns:   value or default
        """
        return self.value.get(key, default)

    def __add__(self, val):
        """
        Add the value of self to another and return it.
        """
        return self.value + val

    def __radd__(self, val):
        """
        Add another to the value of self and return it.
        """
        return val + self.value

    def __eq__(self, other):
        """
        Compare this object with another one.

        Arguments: other - the object to compare to.
        Returns:   Boolean
        """
        if self is other:
            return True

        if self.toi is None:
            return False

        if isinstance(other, Attribute):
            if other.toi is not None:
                return self.value == other.value
            else:
                return False
        else:
            return self.value == other

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return id(self)

    @classmethod
    def coerceValueList(cls, vlist, attrName=None):
        """
        Coerce a list of values to the type of attribute, raise
        exception on fail.

        Arguments: vlist - list of values
                   attrName - name of responsible for the coercing.
        Returns:   None, or list of tracebacks
        """
        if attrName is None:
            attrName = cls.name

        rList = []
        eList = []
        for i in range(len(vlist)):
            try:
                rList.append(cls.coerceValue(vlist[i]))
            except exceptions.ValueTypeError as e:
                eList.append((i, e))

        if eList:
            err = exceptions.ValueErrorList(*eList)
            if hasattr(cls, '_toc'):
                aErr = exceptions.AttrValueError(cls.name,
                                                 cls._toc._fullname,
                                                 cls.toi and cls.toi.id[0],
                                                 err)
            else:
                aErr = exceptions.AttrValueError(cls.name,
                                                 cls.__module__,
                                                 None,
                                                 err)
            raise aErr

        return rList

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        raise NotImplementedError


def _MapDictUnpickler(code, state):
    return dict(state)

# for unpickling existing MapDict data in db
custombson.Extension.register(object(), 'MapDict', unpickler=_MapDictUnpickler)

class MapAttribute(Attribute):
    """
    This class is the base class for all mapping attributes
    """
    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, InMap, NoneOfMap,
                      Readable)

    default = {}

    def keys(self):
        return list(self.value.keys())

    def values(self):
        return list(self.value.values())

    def items(self):
        return list(self.value.items())

    def validateValues(self, pre=False, value=None):
        if value is None:
            value = self.value
        value = list(value.values())
        super(MapAttribute, self).validateValues(pre, value)

    @classmethod
    def coerceValueList(cls, vlist, attrName=None):
        """
        Coerce a list of values to the type of attribute, raise
        exception on fail.

        Arguments: vlist - list of values
                   attrName - name of responsible for the coercing.
        Returns:   None, or list of tracebacks
        """
        if attrName is None:
            attrName = cls.name

        rList = {}
        eList = []

        for itemfunc in ('iteritems', 'items'):
            try:
                items = getattr(vlist, itemfunc)()
                break
            except AttributeError:
                pass
        else:
            items = iter(vlist)


        for i, (k, v) in enumerate(items):
            try:
                k, v = cls.coerceValue((k,v))
                rList[k] = v
            except exceptions.ValueTypeError as e:
                eList.append((i, e))

        if eList:
            err = exceptions.ValueErrorList(*eList)
            if hasattr(cls, '_toc'):
                aErr = exceptions.AttrValueError(cls.name,
                                                 cls._toc._fullname,
                                                 cls.toi and cls.toi.id[0],
                                                 err)
            else:
                aErr = exceptions.AttrValueError(cls.name,
                                                 cls.__module__,
                                                 None,
                                                 err)
            raise aErr

        return rList

class BaseInt(Attribute):
    """
    This class is the base class for all INT table based attributes
    """

class BaseIntMap(MapAttribute):
    """
    This class is the base class for all INTMAP table based attributes
    """

class BaseFloat(Attribute):
    """
    This class is the base class for all FLOAT table based attributes
    """

class Bool(BaseInt):
    """
    This class implements a boolean type attribute.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Unchangeable)

    _validQueryOps = (Empty, NotEmpty, Exact, In, NotIn, NoneOf, Readable)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            return bool(val)
        except:
            raise exceptions.BoolValueError(val)


class LostFile(gridfs.GridOut):

    _id = None # override GridOut's _id property

    def __init__(self, id):
        self._id = id

    def read(self):
        raise IOError

    def __getattr__(self, attr):
        raise AttributeError


class BlobVal(object):
    "Blob value"

    large_blob = 2**16 # 64k
    transfer_encoding = None # for __setstate__

    def __init__(self, value, content_type=None, filename=None,
                 transfer_encoding=None):
        if isinstance(value, gridfs.GridOut):
            self.value = value
            return

        if isinstance(value, str):
            value = value.encode('utf-8')

        try:
            pos = value.tell()
            value.seek(0, os.SEEK_END)
            self.value = value
            self.length = value.tell()
        except AttributeError:
            self.value = BytesIO(value)
            self.length = len(value)
        except IOError:
            self.value = BytesIO(value.read())
            self.length = len(self.value.getvalue())
        finally:
            try:
                value.seek(pos)
            except (AttributeError, NameError):
                pass

        self.content_type = content_type
        self.filename = filename
        self.transfer_encoding = transfer_encoding
        self.references = set()

    @classmethod
    def gridfs(self, database=None):
        coll = self.get_collection(database)
        return gridfs.GridFS(coll.database, coll.name)

    @staticmethod
    def get_collection(database=None):
        if not database:
            database = contextbroker.ContextBroker().database
        return database.blobvals

    def getvalue(self):
        pos = self.value.tell()
        try:
            self.value.seek(0)
            return self.value.read()
        finally:
            self.value.seek(pos)

    def addref(self, ref):
        self.references.add(ref)
        if isinstance(self.value, gridfs.GridOut):
            self.value._coll.files.update(
                {'_id': self.value._id},
                # sets are Bsoned as dicts with a key 'value' (see Bson.py)
                {'$addToSet': {'metadata.references.value': ref}})

    def delref(self, ref):
        self.references.discard(ref)
        if isinstance(self.value, gridfs.GridOut):
            self.value._coll.files.update(
                {'_id': self.value._id},
                # sets are Bsoned as dicts with a key 'value' (see Bson.py)
                {'$pull': {'metadata.references.value': ref}})
            doc = self.value._coll.files.find_and_modify(
                {'_id': self.value._id, 'metadata.references.value': []},
                remove=True)
            if doc:
                self.value._coll.chunks.remove({'files_id': self.value._id})

    def __getstate__(self):
        if isinstance(self.value, gridfs.GridOut):
            return {'gridfs': self.value._id}

        if self.length > self.large_blob:
            try:
                pos = self.value.tell()
                self.value.seek(0)
            except IOError:
                pos = 0

            gridfile = self.gridfs().new_file(filename=self.filename,
                                              content_type=self.content_type,
                                              metadata={'references': self.references})
            #import pdb; pdb.set_trace()
            gridfile.write(self.value)
            gridfile.close()
            collection = self.get_collection()
            self.value = gridfs.GridOut(collection,
                                        file_document=gridfile._file)
            self.value._coll = collection
            self.value.seek(pos)
            return {'gridfs': gridfile._id}
        else:
            value = bson.binary.Binary(self.getvalue())
            self.value = BytesIO(value)

        state = {'value': value, 'filename': self.filename,
                 'content_type': self.content_type,
                 'references': self.references}
        if self.transfer_encoding:
            state['transfer_encoding'] = self.transfer_encoding
        return state

    def __setstate__(self, state):
        try:
            self.value = self.gridfs().get(state['gridfs'])
            self.value._coll = self.get_collection()
        except gridfs.NoFile:
            self.value = LostFile(state['gridfs'])
        except KeyError:
            self.__dict__.update(state)
            self.length = len(self.value)
            self.value = BytesIO(self.value)

    def __getattr__(self, attr):
        try:
            return getattr(self.value, attr)
        except AttributeError as e:
            try:
                return self.value.metadata[attr]
            except (KeyError, TypeError):
                raise e

    def __getitem__(self, item):
        return self.value[item]

    def __deepcopy__(self, memo):
        return BlobVal(deepcopy(self.value, memo),
                       self.contentType, self.filename)

    def __str__(self):
        return self.value

    def __repr__(self):
        return '<BlobVal at %#x>' % -id(self)

    def __eq__(self, other):
        if isinstance(other, BlobVal):
            gridfiles = len([_f for _f in (isinstance(self.value, gridfs.GridOut),
                                          isinstance(other.value, gridfs.GridOut)) if _f])
            if gridfiles == 1:
                return False
            elif gridfiles == 0:
                return self.getvalue() == other.getvalue()
            elif gridfiles == 2:
                return self.value._id == other.value._id
        return False

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        try:
            return hash(self.md5)
        except AttributeError:
            self.md5 = hashlib.md5(self.getvalue()).hexdigest()
            return hash(self.md5)


custombson.register(BlobVal)

class Blob(Attribute):
    """
    This class implements a blob type attribute.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Size, Unchangeable)

    _validQueryOps = (Empty, NotEmpty, Readable)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        if not isinstance(val, BlobVal):
            val = BlobVal(val)
        return val

class BlobMap(MapAttribute):
    """
    this class implements a blob mapping type attribute.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Distinct, Size, Unchangeable)

    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, Readable)

    @classmethod
    def coerceValue(cls, value):
        try:
            name, val = value
            if type(name) is not str:
                '' + name
                name = str(name, 'latin-1')
            if val is None:
                return name, val
            return MapValue((name, str(val)))
        except:
            raise exceptions.BlobValueError(value)

class Decimal(Attribute):
    """
    This class implements a Decimal type attribute.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Selection, Unchangeable, Unique)

    _validQueryOps = (Between, Empty, NotEmpty, Exact, Greater, GreaterEq,
                      In, NotIn, Less, LessEq, NoneOf, Readable)

    precision = 20

    def __init__(self, *args, **kw):
        """
        Handle precision, if given.

        Arguments: Various
        Returns:   None
        """
        super(Decimal, self).__init__(*args, **kw)

        if not hasattr(self.__class__, 'quantifier'):
            self.__class__.quantifier = decimal.Decimal("0."+(self.precision-1)*"0"+"1")

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            if not hasattr(cls, 'quantifier'):
                cls.quantifier = decimal.Decimal("0."+(cls.precision-1)*"0"+"1")
            return decimal.Decimal(val).quantize(cls.quantifier)
        except Exception as e:
            raise exceptions.DecimalValueError(val)

class DecimalMap(BaseIntMap):
    """
    This class implements a map of limitedstring to decimal.
    """
    _validModifiers = (Distinct, ReorderOnly, ReadOnly,
                       Quantity, Selection, Range,
                       Unchangeable,   # For the whole attribute
                       Unique) # For values

    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, InMap, NoneOfMap,
                      Readable)
    precision = 20

    def __init__(self, *args, **kw):
        """
        Handle precision, if given.

        Arguments: Various
        Returns:   None
        """
        super(DecimalMap, self).__init__(*args, **kw)

        if not hasattr(self.__class__, 'quantifier'):
            self.__class__.quantifier = decimal.Decimal("0."+(self.precision-1)*"0"+"1")

    @classmethod
    def coerceValue(cls, value):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            name, val = value
            if type(name) is not str:
                '' + name
                name = str(name, 'latin-1')
        except Exception as e:
            raise exceptions.StringValueError(value)
        try:
            if not hasattr(cls, 'quantifier'):
                cls.quantifier = decimal.Decimal("0."+(cls.precision-1)*"0"+"1")
            val = decimal.Decimal(val).quantize(cls.quantifier)
        except Exception as e:
            raise exceptions.DecimalValueError(val)
        return MapValue((name, val))

class EnumVal(object):
    "Enum value"

    def __init__(self, name, index=None):
        """
        Arguments: name  - internal enum name. Must be unique in this attr
                   index - (ignored) index in the list. Readjusted by BL
        """
        self.name = name
        self.index = index

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<EnumVal %s at %#x>' % (self.name, -id(self))

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        elif not isinstance(other, EnumVal):
            return False
        return self.index == other.index and self.name == other.name

    def __ne__(self, other):
        return not self == other

    def __cmp__(self, other):
        return cmp(self.index, other.index)

custombson.Extension.register(EnumVal, pickler=str)


class mEnum(mAttribute):

    @staticmethod
    def __new__(cls, name, bases, bdict):
        values = bdict.get('values', None)
        if values is not None:
            if not isinstance(values, (list, tuple, set, frozenset)):
                values = [ values ]
            nValues = []
            for i, v in enumerate(values):
                if not isinstance(v, EnumVal):
                    nVal = EnumVal(v, i)
                else:
                    nVal = v
                    nVal.index = i
                nValues.append(nVal)
                bdict[nVal.name] = nVal

            bdict['values'] = nValues
        return super(cls, mEnum).__new__(cls, name, bases, bdict)

class Enum(Attribute,metaclass=mEnum):
    """
    Specifies an enumerated type attribute. The 'values' attribute must contain
    a list of strings or EnumVal objects.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Unchangeable, Unique)

    _validQueryOps = (Between, Empty, NotEmpty, Exact, Greater, GreaterEq,
                      In, NotIn, Less, LessEq, NoneOf, Readable)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        if isinstance(val, str):
            for v in cls.values:
                if val == v.name:
                    return v

        if isinstance(val, EnumVal) and val in cls.values:
            return val

        raise exceptions.EnumValueError(val)

    @classmethod
    def __iter__(cls):
        """
        Enum values are iterable.
        """
        for v in cls.values:
            yield v
        raise StopIteration()


class Float(BaseFloat):
    """
    This class implements a float type attribute.
    """
    _validModifiers = (Presentation, Quantity, Range, ReadOnly,
                       ReorderOnly, Selection, Unchangeable, Unique)

    _validQueryOps = (Between, Empty, NotEmpty, Exact, Greater, GreaterEq,
                      In, NotIn, Less, LessEq, NoneOf, Readable)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            return float(val)
        except:
            raise exceptions.FloatValueError(val)

class Int(BaseInt):
    """
    This class implements an integer type attribute.
    """
    _validModifiers = (Presentation, Quantity, Range, ReadOnly,
                       ReorderOnly, Selection, Unchangeable, Unique)

    _validQueryOps = (Between, Empty, NotEmpty, Exact, Greater, GreaterEq,
                      In, NotIn, Less, LessEq, NoneOf, Readable)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            return int(val) # long()?
        except:
            raise exceptions.IntValueError(val)

class IntMap(BaseIntMap):
    """
    This class implements a map of limitedstring to int.
    """
    _validModifiers = (Distinct, ReorderOnly, ReadOnly,
                       Quantity, Selection, Range,
                       Unchangeable,   # For the whole attribute
                       Unique) # For values

    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, InMap, NoneOfMap,
                      Readable)

    @classmethod
    def coerceValue(cls, value):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            name, val = value
            if type(name) is not str:
                '' + name
                name = str(name, 'latin-1')
        except Exception as e:
            raise exceptions.StringValueError(value)
        try:
            val = int(val)
        except ValueError:
            raise exceptions.IntValueError(value)
        return MapValue((name, val))


class String(Attribute):
    """
    This class implements a string type attribute.
    """
    _validModifiers = (MessageID, Presentation, ReorderOnly,
                       ReadOnly, Quantity, Regexp, Selection, Size,
                       Unchangeable, Unique)

    _validQueryOps = (Empty, NotEmpty, Exact, Ilike, NotIlike,
                      In, NotIn, Like, NotLike, NoneOf, Readable, RegEx,
                      Between, Greater, GreaterEq, Less, LessEq)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        if type(val) is str:
            return val
        if isinstance(val, str):
            return str(val)  # unicode subclass
        try:
            '' + val
            return str(val, 'latin-1')
        except Exception as e:
            raise exceptions.StringValueError(val)

class LimitedString(String):
    """
    This class implements a string type attribute.
    """

class StringMap(MapAttribute):
    """
    This class implements a string mapping type attribute.
    """
    _validModifiers = (Distinct, ReorderOnly, ReadOnly,
                       Quantity, Regexp, Selection, Size,
                       Unchangeable,                       # For the whole attribute
                       Unique) # For values

    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, InMap, NoneOfMap,
                      LikeMap, NotLikeMap, IlikeMap, NotIlikeMap, Readable)

    @classmethod
    def coerceValue(cls, value):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            name, val = value
            if type(name) is not str:
                '' + name
                name = str(name, 'latin-1')
            #if type(val) not in (unicode, NoneType):
            #    '' + val
            #    val = unicode(val, 'latin-1')
            return MapValue((name, val))
        except Exception as e:
            raise exceptions.StringValueError(value)

class Timespan(BaseInt):
    """
    This class implements a timespan type attribute.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Range, Resolution, Selection, Unchangeable,
                       Unique, Presentation)

    _validQueryOps = (Between, Empty, NotEmpty, Exact, Greater, GreaterEq,
                      In, NotIn, Less, LessEq, NoneOf, Readable)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            return int(val)
        except:
            raise exceptions.TimespanValueError(val)

class TimespanMap(BaseIntMap):
    """
    This class implements a timespan mapping type attribute.
    """
    _validModifiers = (Distinct, ReorderOnly, ReadOnly,
                       Quantity, Range, Resolution, Selection,
                       Unchangeable, Unique)

    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, InMap, NoneOfMap,
                      # Between, Exact, Greater, GreaterEq, Less, LessEq,
                      Readable)

    @classmethod
    def coerceValue(cls, value):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            name, val = value
            if type(name) is not str:
                '' + name
                name = str(name, 'latin-1')
        except Exception as e:
            raise exceptions.StringValueError(value)
        try:
            val = int(val)
        except ValueError:
            raise exceptions.TimespanValueError(value)
        return MapValue((name, val))

class Timestamp(BaseInt):
    """
    This class implements a timestamp type attribute.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Range, Resolution, Selection, Unchangeable,
                       Unique, Presentation)

    _validQueryOps = (Between, Empty, NotEmpty, Exact, Greater, GreaterEq,
                      In, NotIn, Less, LessEq, NoneOf, Readable)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            return int(val)
        except:
            raise exceptions.TimestampValueError(val)

class TimestampMap(BaseIntMap):
    """
    This class implements a timestamp mapping type attribute.
    """
    _validModifiers = (Distinct, ReorderOnly, ReadOnly,
                       Quantity, Range, Resolution, Selection,
                       Unchangeable, Unique)

    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, InMap, NoneOfMap,
                      # Between, Exact, Greater, GreaterEq, Less, LessEq,
                      Readable)

    @classmethod
    def coerceValue(cls, value):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        try:
            name, val = value
            if type(name) is not str:
                '' + name
                name = str(name, 'latin-1')
        except Exception as e:
            raise exceptions.StringValueError(value)
        try:
            val = int(val)
        except ValueError:
            raise exceptions.TimestampValueError(value)
        return MapValue((name, val))

class ToiRef(Attribute):
    """
    This class implements a TOI reference type attribute.
    """
    _validModifiers = (Parent, ReorderOnly, ReadOnly,
                       Quantity, Selection, ToiType, Unchangeable, Unique,
                       Weak, Presentation)

    _validQueryOps = (Empty, NotEmpty, Exact, Fulltext,
                      In, NotIn, NoneOf, Readable, SubQuery)

    @classmethod
    def coerceValue(cls, val):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: val - the value to coerce
        Returns:   None
        """
        from . import to
        if not isinstance(val, to.TO):
            raise exceptions.ToiRefValueError(val)

        return val

class ToiRefMap(MapAttribute):
    """
    This class implements a TOI reference mapping type attribute.
    """
    _validModifiers = (ReorderOnly, ReadOnly, Quantity,
                       Unchangeable, Distinct, ToiType, Weak)

    _validQueryOps = (Empty, NotEmpty, HasKey, LacksKey, InMap, NoneOfMap,
                      Readable)

    @classmethod
    def coerceValue(cls, value):
        """
        Coerce a value to the type of the attribute, raise
        exception on fail.

        Arguments: value - the value to coerce
        Returns:   None
        """
        from . import to
        try:
            name, val = value
            if type(name) is not str:
                '' + name
                name = str(name, 'latin-1')
            if not isinstance(val, (type(None), to.TO)):
                raise ToiRefValueError(val)
            return MapValue((name, val))
        except Exception as e:
            raise exceptions.ToiRefValueError(value)


class mRelation(mAttribute):

    @staticmethod
    def __new__(cls, name, bases, bdict):
        # Only do this check for classes inheriting Relation. Relation
        # itself will have ToiRef as its first base
        if bases[0] != ToiRef:
            if 'related' in bdict and not isinstance(bdict['related'],
                                                     str):
                bdict['related'] = '%s.%s'%(bdict['related']._toc._fullname,
                                            bdict['related'].name)
        return super(cls, mRelation).__new__(cls, name, bases, bdict)


class Relation(ToiRef,metaclass=mRelation):
    """
    This class implements the Relation type attribute.
    """
    related = None

# for now to be used only as a parameter type for blm methods
class Serializable(Attribute):
    """
    This class implement an attribute accepting any data.

    Although not enforced it is meant to be used to specify spickable data
    for parameters to a blm method, that's the motivation for the name.
    """

    _validModifiers = (Quantity) # what else?

    _validQueryOps = ()

    @classmethod
    def coerceValue(cls, value):
        return value


# This is intended to be the canonical cleanValue. All other copies should go away and point here

def cleanValue(value):
    """
    Replace all TO objects in a value lists for the
    corresponding toi id.

    Arguments: value - the value object, list or dict to clean.
    Returns:   The cleaned value
    """
    from pytransact import blm

    if isinstance(value, Attribute):
        value = value.value

    if isinstance(value, blm.getTocByFullname('TO')):
        return value.id[0]
    if isinstance(value, EnumVal):
        return value.name
    if isinstance(value, list):
        return [ cleanValue(val) for val in value ]
    if isinstance(value, tuple):
        return tuple([ cleanValue(val) for val in value ])
    if isinstance(value, set):
        return set([ cleanValue(val) for val in value ])
    if isinstance(value, dict):
        return dict([ (key, cleanValue(val))
                      for key, val in value.items() ])
    return value
