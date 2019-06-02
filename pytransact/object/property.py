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
This module contains all property objects definitions for TO data
model.
"""
from pytransact import custombson
from future.utils import with_metaclass

class mProperty(type):
    """
    Property metaclass, for inerhitance detection purposes only
    """

    def __init__(cls, name, bases, namespace):
        """
        Implement singleton behaviour.

        Arguments: metaclass junk
        Returns:   None
        """
        super(mProperty, cls).__init__(name, bases, namespace)
        cls.instance = None

    def __call__(cls, *args, **kw):
        """
        Someone tries to inherit or create an object of this class
        """

        if cls is Property:
            raise SyntaxError('You must use a derived class of Property')

        # Now do singleton magic
        if cls.instance is None:
            cls.instance = super(mProperty, cls).__call__(*args, **kw)

        return cls.instance


class Property(with_metaclass(mProperty,object)):
    """
    This class implements the basic property functionalities.
    """

    def getstate(self, *vars):
        """
        Return a dictionary containing the state-relevant variables.

        Arguments: vars - the list of variables to return in the dict
        Returns:   A dictionary containing the state-relevant variables.
        """
        rval = {}
        for v in vars:
            rval[v] = getattr(self, v)

        return rval


class Parent(Property):
    """
    This class implements the parent type property for a TOI value.
    """
custombson.register(Parent)


class MessageID(Property):
    """
    This class implements the message ID type property for a TOI value.
    """
custombson.register(MessageID)


class Presentation(Property):
    """
    This class implements the presentation type property for a TOI value.
    """
custombson.register(Presentation)


class ReorderOnly(Property):
    """
    This class implements the reorder only type property for a TOI value.
    """
custombson.register(ReorderOnly)


class ReadOnly(Property):
    """
    This class implements the readonly type property for a TOI value.
    Always counts as a 'pre' property.
    """
custombson.register(ReadOnly)


class Unchangeable(Property):
    """
    This class implements the unchangeable type property for a TOI value.
    """
custombson.register(Unchangeable)


class Unique(Property):
    """
    This class implements the unique type property for a TOI value.
    The property works best in combination with a QuantityRestriction.
    Note that this property doesn't check for uniqueness of the entire
    value list, it simply verifies that every single value of the list
    is unique.
    """
custombson.register(Unique)


class Weak(Property):
    """
    This class implements the Weak type property for a TOI value.
    """
custombson.register(Weak)
