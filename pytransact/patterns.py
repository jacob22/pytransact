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
Contains GOF pattern base structures
"""
import copy, types
from future.utils import with_metaclass

class mSingleton(type):
    """
    Metaclass for the Singleton class.

    Will hold the singleton and return it rather than a new object
    on instansiation of the class.
    """

    def __call__(cls, *args, **kw):
        """
        Do the singleton magic on instansiation.

        Arguments: cls - the class to instansiate
                   args, kw - various arguments
        Returns:   The singleton
        """
        if cls.__instance__ is None:
            cls.__instance__ = type.__call__(cls, *args, **kw)
        return cls.__instance__


class Singleton(object, metaclass = mSingleton):
    """
    Singleton object mix-in class
    """
    __instance__ = None


class mExtendable(type):
    """
    Metaclass for Extendable object type.

    A type with a syntax trick: 'class __extend__(t)' actually extends
    the definition of 't' instead of creating a new subclass.
    """

    def __new__(cls, name, bases, ddict):
        """
        Create the new object, extending the namespace with the contents
        of the objects in the initial dict

        Arguments: cls - the class object to create
                   name - the name of the object
                   bases - the bases of the object
                   ddict - the data dict.
        Returns:   The new object
        """
        if name == '__extend__':
            for key, value in list(ddict.items()):
                if key in ('__module__', '__doc__'):
                    continue

                for cls in bases:
                    newVal = value
                    if isinstance(value, _ExtendedMethod):
                        oldfunc = getattr(cls, key) # Raises AttributeError
                        newVal = copy.copy(value)
                        newVal.oldfunc = oldfunc
                        if hasattr(oldfunc, '__extendwith__'):
                            newVal = oldfunc.__extendwith__(newVal)
                    setattr(cls, key, newVal)
            return None
        return super(mExtendable, cls).__new__(cls, name, bases, ddict)


class Extendable(object,metaclass = mExtendable):
    """
    Extendable classes, stolen from pypy.
    """


class _ExtendedMethod(object):
    """
    Wrapper object for methods needing access to their previous version.
    Only the 'top' implementation is wrapped in an _ExtendedMethod wrapper
    """
    oldfunc = None

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kw):
        # Move old func to start arg list
        return self.func(*(args + (self.oldfunc,)), **kw)

    def __get__(self, ob, _type=None):
        if self.oldfunc is None:
            raise RuntimeError('Trying to access non-existent extended function from %s:%s'%(type(ob),  self.func.__name__))

        return types.MethodType(self, ob, _type)


def extends(f):
    """
    Decorator that tags a function definition as extending a previous one.
    The mExtendable.__new__ method will detect this and store away a copy
    of the previous version, defined 'extended' in the function
    code.
    """
    return _ExtendedMethod(f)
