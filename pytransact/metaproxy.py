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
# Proxy metaclass
#

import types, itertools
from future.utils import with_metaclass

WrapperDescriptorType = type(dict.__init__)
MethodDescriptorType = type(dict.__contains__)
ClassMethodWrapperType = type(itertools.chain.__dict__['from_iterable'])
assert ClassMethodWrapperType != WrapperDescriptorType != MethodDescriptorType

class bProxy(object):
    """
    Replacement base class for proxying classes. Defines methods used to get
    at the true methods.
    """

    def __getattribute__(self, attr):
        if attr == '__class__':
            return type(self)
        if attr in ('__new__',):
            return object.__getattribute__(self, attr)
        return getattr(object.__getattribute__(self, "__o"), attr)
    def __delattr__(self, attr):
        delattr(object.__getattribute__(self, "__o"), attr)
    def __setattr__(self, attr, val):
        setattr(object.__getattribute__(self, "__o"), attr, val)

    def __new__(*a, **kw):
        cls = a[0]
        realcls = getattr(cls, "__c")
            
        realobj = super(realcls, realcls).__new__(realcls, *a[1:], **kw)

        obj = object.__new__(cls)
        object.__setattr__(obj, "__o", realobj)

        return obj    

class mProxy(type):
    """
    Create a proxy class for a given class. This results in objects
    which behave like objects of the proxied class, but for which
    isinstance(proxied, obj) will be false. Proxy classes are always
    new-style classes, and can (re)define attributes and methods.
    These will not be visible from the proxied class, only from the
    proxy.
    """
    @staticmethod
    def __method(name):
        def fn(*a, **kw):
            self = object.__getattribute__(a[0], '__o')
            return getattr(self, name)(*a[1:], **kw)
        fn.__name__ = name
        return fn

    @staticmethod
    def __classmethod(name):
        def fn(*a, **kw):
            cls = object.__getattribute__(a[0], '__c')
            return getattr(cls, name)(*a[1:], **kw)
        fn.__name__ = name
        return fn
    
    def __new__(cls, name, bases, namespace):
        if len(bases) > 1:
            bases = tuple([b for b in bases if b is not Proxy])
        realcls = type.__new__(cls, "%s(proxied)" % name, bases, namespace)

        override = set(fn for fn in dir(bProxy)
                       if getattr(bProxy, fn) != getattr(bProxy.__base__, fn, object()))

        ns = { "__c" : realcls,
               "__module__" : namespace.get("__module__", "<dynamic>"),
               }
        
        for fn in dir(realcls):
            func = getattr(realcls, fn)
            if fn not in override and callable(func):
                if isinstance(func, types.MethodType):
                    if func.__self__:
                        ns[fn] = classmethod(cls.__classmethod(fn))
                    else:
                        ns[fn] = cls.__method(fn)
                elif isinstance(func, (MethodDescriptorType, WrapperDescriptorType)):
                    ns[fn] = cls.__method(fn)
                elif isinstance(func, types.FunctionType):
                    ns[fn] = staticmethod(func)
                else:
                    ns[fn] = func
            
        newcls = type.__new__(cls, name, (bProxy,), ns)
        type.__setattr__(realcls, '__r', newcls)
        def __new__(cls, *args, **kw):
            # make a new proxy instead of realcls
            return newcls.__new__(newcls, *args, **kw)
        type.__setattr__(realcls, '__new__', staticmethod(__new__))
        return newcls

    def __call__(cls, *args, **kw):
        if not issubclass(cls, bProxy):
            cls = object.__getattribute__(cls, '__r')

        return type.__call__(cls, *args, **kw)

class Proxy(with_metaclass(mProxy,object)):
    pass
