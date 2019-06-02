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
This module contains all BLM top level objects in a BLM.
"""

from pytransact.contextbroker import ContextBroker
from copy import copy
from pytransact.exceptions import *

def method(rtype):
    """
    Decorator designating and specifying external methods return type

    Arguments: func - the function object
               rtype - the return type (attribute object with restrictions)
    Returns:   ExternMethod object representing the method
    """

    def decorator(func):
        argcount = func.__code__.co_argcount
        argnames = func.__code__.co_varnames[:argcount]
        ndefaults = 0
        if func.__defaults__:
            ndefaults = len(func.__defaults__)

        argNames = func.__code__.co_varnames[(argcount - ndefaults):]

        if ndefaults < (argcount - 1):
            raise cSyntaxError(
                'Type declarations missing from arguments %(args)r in the BLM '
                'method %(func)s().' % {
                    'args': list(reversed(argnames))[ndefaults:],
                    'func': func.__name__,})
        params = []
        if func.__defaults__:
            params = [ arg._instantiate(name) for arg, name in
                         zip(func.__defaults__, argNames)]

        func.__defaults__ = None
        m = ExternalMethod(func.__name__, func)
        if rtype:
            m.rtype = rtype._instantiate('result')
        m.params = params

        return m

    return decorator

class ExternalMethod(object):
    """
    The external (toc/blm) method class.
    """

    def __init__(self, name, method):
        """
        Initialise the object
        
        Arguments: name - the name of the method
                   method - the actual code to run
        Returns:   
        """
        self.method = method	# The method
        self.name = name	# The name
        self.__name__ = name    # helps test debugging
        self.rtype = None	# The return type (attribute ob)
        self.domain = None	# The method domain: blm.toc or just blm
        self.params = None	# The attribute argument types (attribute obs)
        self.toi = None		# Toi for individualised method calls

    def __extendwith__(self, newvalue):
        newvalue.oldfunc = self.method
        self.method = newvalue
        return self

    def __call__(self, *args, **kw):
        kw['_client'] = False
        return self.doCall(*args, **kw)

    def clientInvocation(self, *args, **kw):
        kw['_client'] = True
        return self.doCall(*args, **kw)

    def doCall(self, *args, **kw):
        """
        Call the method from the client.
        
        Arguments: args - method argument list
                   kw - keyword argument list
        Returns:   Method call results
        """
        args = list(args)

        for param in self.params[len(args):]:
            args.append(kw.pop(param.name, []))

        if not set(kw) <= {'_client'}:
            raise TypeError('Invalid keyword arguments: %s' % kw)

        if len(args) > len(self.params):
            err = cTypeError('%(func)s() takes exactly %(needed)d arguments '
                             '(%(given)d given)',
                             nt={'func': self.name,
                                 'needed': len(self.params),
                                 'given': len(args)})

            if kw['_client']:
                raise ClientError(err)
            else:
                raise err

        elist = []
        for i in range(len(self.params)):
            attr = self.params[i]
            try:
                v = attr.coerceValueList(args[i], str(i))
                attr.validateValues(False, v)
            except LocalisedError as e:
                if not hasattr(attr, '_toc') and hasattr(attr, '_xlatKey'):
                    e.t['name'] = attr._xlatKey
                elist.append(e)
                continue
            args[i] = v
        if elist:
            if kw['_client']:
                raise cAttrErrorList(*elist)
            else:
                raise AttrErrorList(*elist)

        # Exceptions in the implementation won't be wrapped in ClientError
        if self.toi:
            aList = [self.toi] + args
            return self.method(*aList)
        else:
            return self.method(*args)
        
    def __get__(self, toi, _type=None):
        """
        Get the method object.

        Returns an individual copy of the method per use, ensuring
        that each method call will be run for in that particular
        toi's context.
        
        Arguments: toi - toi specification to use.
        Returns:   Object
        """
        if toi is None:
            return self

        if self.toi is not None:
            if self.toi._deleted:
                # Trying call a method on a deleted toi
                raise cLookupError(
                    'Trying call a method on a deleted Toi '
                    '(%(toname)s %(toid)d).' % {
                        'toname': toi._xlatKey,
                        'toid': toi.id[0]})
            return self

        x = copy(self)
        x.toi = toi
        return x
