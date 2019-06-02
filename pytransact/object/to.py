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
This module contains the Task Object definition for CAPS BLMs
"""

import logging
log = logging.getLogger('eutaxia.TO')
from pytransact import patterns, custombson
from pytransact.exceptions import *
from pytransact.object.restriction import ToiType
from pytransact.object.attribute import Attribute, ToiRef
from pytransact.contextbroker import ContextBroker
#import new
from bson.objectid import ObjectId
from pytransact import blm


class mTO(patterns.mExtendable, metaclass = patterns.mExtendable ):
    """
    TO metaclass
    """

    def _query(cls, **queryAttrs):
        """
        Create a query based on this TOC.

        Arguments: queryAttrs - query keyword arguments
        Returns:   Service query object
        """
        return cls._cb.createQuery(cls, queryAttrs)

    def _getArgData(cls, kw):
        """
        Convert a keyword argument dict to a dict that can be given as
        argData to a commit operation.
        """
        argData = {}
        for attrName, val in kw.items():
            if isinstance(val, Attribute):
                val = val.value
            if type(val) not in (list, tuple, set, frozenset, dict):
                val = [ val ]
            else:
                # coerce to correct type; list or dict
                try:
                    val = cls._attributes[attrName].default.__class__(val)
                except KeyError:
                    raise AttrNameError(attrName, cls._xlatKey, None)
            argData[attrName] = val
        return argData

    def __call__(cls, id=None, **kw):
        """
        Create a new TOI based on this TOC.

        Arguments: id - the id number (if specified)
                   kw - attribute value pairs
        Returns:   The new toi
        """
        #import pdb; pdb.set_trace()
        toi = cls._cb.getToi(id)

        if toi and not toi._phantom:
            # Toi ID number already used
            raise cRuntimeError('Toi ID %s already used' % id)

        argData = cls._getArgData(kw)
        toi = cls._cb.createToi(cls, id, argData)
        return toi

    def _create(cls, toid, kw={}):
        """
        Creates a TOI object based on an existing ID number (with
        data already in the database).

        Arguments: toid - the id number
                   kw - attribute keyword arguments
        Returns:   The new toi
        """
        if isinstance(toid, TO):
            if not isinstance(toid, cls):
                # Trying to coerce a TOI (%(tocname)s) to an incorrect TO.
                raise cValueError(
                    'Trying to coerce a TOI (%(toname)s) to an '
                    'incorrect TO (%(actual)s).' % {'toname': toid._xlatKey,
                                                    'actual': cls.__name__,})
            toid = toid.id[0]

        toid = ObjectId(toid)

        toi = cls._cb.getToi(toid)

        if not toi:
            toi = type.__call__(cls, toid, **kw)
            cls._cb.addToi(toi)
        else:
            try:
                for attrName, val in kw.items():
                    if attrName not in toi._orgAttrData:
                        toi._attrData[attrName] = \
                            toi._attributes[attrName].coerceValueList(val)
            except KeyError:
                raise AttrNameError(attrName, toi._xlatKey, toi.__id)


        if toi.__class__ != cls and issubclass(cls, toi.__class__):
            toi.__class__ = cls		# METAMORPHOSIS!!!

        return toi

    def __new__(cls, name, bases, namespace):
        """
        Handle instansiation of new toc classes.

        Very special as it goes through the namespace and for all
        derived Attribute classes found creates an object instance
        of that class which it replaces the original class with.

        This means that all TOIs later will share the same attribute
        instances throughout, meaning that the actual attribute data
        has to be stored in the TOI rather than the attributes.

        It also puts the Methods and Attributes in object attributes
        for easy reference/introspection purposes.

        Arguments: name - toc class name
                   bases - base objects
                   namespace - class namespace
        Returns:   New toi object
        """

        methods = {}
        attrs = {}

        cname = name
        if name == '__extend__':
            cname = bases[0].__name__

        # Go through all members, handle attributes and methods
        try:
            del namespace['__extend__']
        except KeyError:
            pass

        for obName, ob in list(namespace.items()):

            # Attributes is a new style object subclass of Attribute
            if (isinstance(ob, Attribute) or
                (isinstance(ob, type) and issubclass(ob, Attribute))):

                if isinstance(ob, Attribute):
                    oc = ob.__class__
                    oc = new.classobj(oc.__name__, oc.__bases__, dict(oc.__dict__))
                else:
                    oc = ob
                # Replace the class object with a per name object instance
                # of the class. This means that the object will be shared
                # for all instances of this particular toc, so the attribute
                # data must be stored with the toi.
                namespace[obName] = attrs[obName] = oc._instantiate(obName)

                # Since python can't cope, fix the class name here
                attrs[obName].__class__.__name__ = '%s.%s'%(cname, obName)

                # Pad the class with info of which blm it was extended from,
                # if any.
                attrs[obName]._overridden = None
                if name == '__extend__':
                    attrs[obName]._extended = namespace['__module__']

                    # Jot down its origin
                    if cname == 'TO':
                        attrs[obName]._basetoc = cname
                    else:
                        attrs[obName]._basetoc = bases[0]._fullname
                        if obName in bases[0]._attributes:
                            attrs[obName]._overridden = namespace['__module__']

                else:
                    attrs[obName]._extended = None

                    # Jot down its origin
                    if cname == 'TO':
                        attrs[obName]._basetoc = cname
                    else:
                        attrs[obName]._basetoc = '%s.%s'%(
                            namespace['__module__'].rsplit('.',1)[-1], cname)
                        if obName in bases[0]._attributes:
                            attrs[obName]._overridden = namespace['__module__']


                attrs[obName]._xlatKey = 'blm.%s.%s'%(attrs[obName]._basetoc, obName)
            # Methods
            from pytransact.object.method import ExternalMethod
            if isinstance(ob, ExternalMethod):
                methods[obName] = ob
                if name == '__extend__':
                    ob._extended = namespace['__module__']

                    # Jot down its origin
                    if cname == 'TO':
                        ob._basetoc = cname
                    else:
                        ob._basetoc = bases[0]._fullname
                        if obName in bases[0]._methods:
                            ob._overridden = namespace['__module__']
                    ob._xlatKey = 'blm.%s.%s' % (ob._basetoc, obName)
                else:
                    ob._extended = None

                    # Jot down its origin
                    if cname == 'TO':
                        ob._basetoc = cname
                    else:
                        ob._basetoc = '%s.%s'%(namespace['__module__'].rsplit('.',1)[-1], cname)
                        ob._xlatKey = 'blm.%s.%s'%(ob._basetoc, obName)

        # Instantiate the object
        ob = super(mTO, cls).__new__(cls, name, bases, namespace)
        if not ob:
            ob = bases[0]
        else:
            blmName = namespace['__module__'].rsplit('.', 1)[-1]
            if cname == 'TO':
                ob._fullname = 'TO'
            else:
                ob._fullname = '%s.%s'%(blmName, cname)
            ob._blmname = blmName
            ob._xlatKey = 'blm.%s'%(ob._fullname,)

        # Annotate the attributes
        for aname, attr in attrs.items():
            attr._toc = ob

        # Update list of methods and attributes with inherited data

        parentAttributes = ob._attributes
        ## ob._attributes = ob._attributes.copy()
        ob._attributes = {}
        for attrName, parentAttr in parentAttributes.items():
            if attrName in attrs:
                continue
            attr = parentAttr.__class__._instantiate(attrName)
            attr.__dict__.update(parentAttr.__dict__)
            attr._toc = ob
            ob._attributes[attrName] = attr
            setattr(ob, attrName, attr)

        ob._attributes.update(attrs)
        ob._methods = ob._methods.copy()
        ob._methods.update(methods)

        return ob

class TO(object, metaclass=mTO):
    """
    TO base class.
    """
    _methods = {}		# Blm methods
    _attributes = {}		# Attribute objects
    _cb = ContextBroker()	# Singleton!
    _fullname = None		# The TO full name

    def __init__(self, id=None, **kw):
        """
        Initialise the toi.

        Arguments: id - the id number, if specified
                   kw - attribute value pairs
        Returns:   None
        """
        self._attrData = {}	# Contains all member Attribute's data
        			# on the format name: value sequence
        self._orgAttrData = {}	# For rollback
        self._deleted = False	# Delete marker
        self._phantom = True    # The TOI is not known to exist, either in DB
                                # or having been properly created in current
                                # context

        if not id:
            self.id = (self._cb.newId(),)
        else:
            self.id = (ObjectId(id),)

        # Add a __name__ if missing. Needed in testing, if nothing else
        if not hasattr(self, '__name__'):
            self.__name__ = '%s:%s'%(self.__class__.__name__, self.id[0])

        try:
            for attrName, val in kw.items():
                self._attrData[attrName] = self._attributes[attrName].coerceValueList(val)
        except KeyError:
            raise AttrNameError(attrName, self._xlatKey, self.id)

    def __getstate__(self):
        state = {'id': self.id[0], 'toc': self.__class__._fullname}
        if self._deleted:
            state['deleted'] = True
        return state

    def __setstate__(self, state):
        self.id = state['id'],
        # as in __init__:
        self._attrData = {}
        self._orgAttrData = {}
        self._deleted = state.get('deleted', False)
        if not hasattr(self, '__name__'):
            self.__name__ = '%s:%s'%(self.__class__.__name__, self.id[0])

    def __deepcopy__(self, memo):
        return self

    def __call__(self, **kw):
        """
        Update the object with new data.

        Arguments: kw - attribute name value pairs
        Returns:   None
        """
        argData = self.__class__._getArgData(kw)
        self._cb.changeToi(self, argData)

    def _update(self, kw):
        """
        Convenience call for BLM only!!!! to update attribute values.

        Arguments: kw - attribute data
        Returns:   None
        """
        for attrName, value in kw.items():
            setattr(self, attrName, value)

    def _clear(self):
        self._attrData = {}
        self._orgAttrData = {}

    class allowRead(ToiRef(ToiType('fundamental.AccessHolder'))):
        "List of AccessHolders that are allowed to read this object"

    def canRead(self, user):
        return bool(set(user._privileges) & set(self.allowRead))

    def canWrite(self, user, attr):
        return self.canRead(user)

    def canDelete(self, user):
        return self.canWrite(user, 'id')

    def __eq__(self, other):
        try:
            if self.id != other.id:
                return False
            elif isinstance(other, TO):
                return True
        except AttributeError:
            pass
        try:
            return self.id[0] == ObjectId(other)
        except TypeError:
            return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.id[0])

    def __str__(self):
        return '<%s %s>' % (self._fullname, self.id[0])

    def __repr__(self):
        return str(self)

    @property
    def _modified(self):
        """
        The modified attribute names.
        """
        return set(self._orgAttrData)

    def __getitem__(self, item):
        """
        Returns an attribute based on the name.

        Arguments: item - attribute name
        Returns:   The attribute
        """
        if item in self._attributes:
            return getattr(self, item)

        if item == 'id':
            return self.id

        raise AttrNameError(item, self._xlatKey, self.id)

    def __iter__(self):
        """
        Iterate over the attribute list.

        Arguments: None
        Returns:   generator
        """
        for attrName in self._attributes:
            yield getattr(self, attrName)

    def _requestAttribute(self, attr):
        """
        Request an attribute value list. Also sets it in this object.

        Arguments: attr - the attribute to request
        Returns:   Attribute values
        """
        val = self._cb.requestAttribute(self, attr)
        return val

    def _preload(self, attrNames):
        """
        Issue a pre-request for a list of attributes, to speed up later
        access.
        """
        aNames = []
        for attr in attrNames:
            try:
                aname = attr.name # Work with attribute objects too!
            except:
                aname = attr
            if aname not in self._attributes and aname != 'id':
                raise AttrNameError(attrName, self._xlatKey, self.id)
            # Filter out values that are already fetched
            if (aname == 'id' or
                aname in self._attrData and
                (aname not in self._orgAttrData or
                 self._orgAttrData[aname] is not None)):
                pass
            else:
                aNames.append(aname)
        self._cb.preloadAttributes(self, aNames)

    def _register(self):
        """
        Register for commit.

        Arguments: None
        Returns:   None
        """
        self._cb.register(self)

    def _delete(self):
        """
        Mark this TOI as deleted.

        Arguments: None
        Returns:   None
        """
        if not self._cb.canDelete(self):
            raise cAttrPermError('_delete', self._fullname, self.id[0])
        self._cb.deleteToi(self)


def unpickle_to(code, state):
    toc = blm.getTocByFullname(state['toc'])
    toi = toc._create(state['id'])
    toi._phantom = False
    return toi

custombson.Extension.register(TO, pickler=TO.__getstate__, unpickler=unpickle_to)
