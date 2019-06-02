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
This module contains all restriction objects definitions for CAPS BLMs
"""
from future.utils import with_metaclass
from pytransact import spickle, custombson
from pytransact.exceptions import RestrictionError, RestrictionErrorList, \
                              QuantityMinError, QuantityMaxError, \
                              RangeLowError, RangeHighError, \
                              RegexpError, ResolutionError, \
                              SelectionError, SizeShortError, SizeLongError, \
                              ToiTypeError, QualificationError
import re

class mRestriction(type):
    """
    Restriction metaclass, for inerhitance detection purposes only
    """

    def __call__(cls, *args, **kw):
        """
        Someone tries to inherit or create an object of this class
        """

        if cls is Restriction:
            raise SyntaxError('You must use a derived class of Restriction')

        return type.__call__(cls, *args, **kw)


class Restriction(with_metaclass(mRestriction,object)):
    """
    This class implements the basic restriction functionalities.
    """

    def validateValue(self, val):
        """
        Validate that a given value conforms to the restriction.

        This method has to be overridden in a subclass.

        Arguments: val - the value to validate
        Returns:   Error list [type, spec]
                   If no errors: None
        """
        raise NotImplementedError

    def validateValueList(self, values):
        """
        Validate that a given list of values conforms to the restriction.
        Raises a RestrictionError with a list of found exceptions

        Arguments: values - the list of values to validate.
        """
        elist = []
        for n, val in zip(range(len(values)), values):
            try:
                self.validateValue(val)
            except RestrictionError as e:
                elist.append((n, e))

        if elist:
            raise RestrictionErrorList(*elist)

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

class Quantity(Restriction):
    """
    This class implements the Quantity type restriction for a TOI value.
    """

    obj = object()

    def __init__(self, min, max=obj):
        """
        Initialize the instance

        Arguments: min - mininum required amount of values in the value list
                   max - maximum allowed amount of values in the value list
        Returns:   None
        """
        self.min = min                          # Min amount of values
        if max is self.obj:
            max = min
        self.max = max                          # Max amount of values

        if max is not None and max < min:
            raise AttributeError('Max must not be less than min')

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object.
        """
        return self.__class__.__name__ + "(%r, %r)"%(self.min, self.max)

    def validateValueList(self, values):
        """
        Validate that a given list of values conforms to the restriction.

        Arguments: values - the list of values to validate.
        Returns:   List of failures: [[type, specific, index],]
                   If no errors: []
        """
        if len(values) < self.min:
            raise QuantityMinError(len(values), self.min)
        if self.max is not None and len(values) > self.max:
            raise QuantityMaxError(len(values), self.max)

custombson.register(Quantity)


def QuantityMax(max):
    return Quantity(0, max)

def QuantityMin(min):
    return Quantity(min, None)


class Distinct(Restriction):
    """
    This class implements the distinct values restriction for a TOI value.
    For Mapping type attributes, this restriction applies to keys, not values.
    """

    def validateValueList(self, values):
        """
        Validate that a given list of values conforms to the restriction.

        Arguments: values - the list of values to validate.
        Returns:   List of failures: [[type, specific, index],]
                   If no errors: []
        """
        if values and type(values[0]) in (list, tuple): # Mapping attribute
            values = [key for key,val in values]
        checked = set()
        for val in values:
            if val in checked:
                raise NonDistinctError(val)
            checked.add(val)

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object.
        """
        return self.__class__.__name__ + '()'


class Range(Restriction):
    """
    This class implements the Range type restriction for a TOI value.
    """

    def __init__(self, low=None, high=None):
        """
        Initialize the instance.

        Arguments: low - the minimum allowed value
                   high - the maximum allowed value
        Returns:   None
        """
        self.low = low                  # Lowest allowed value
        self.high = high                # Highest allowed value

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object
        """
        return self.__class__.__name__ + "(%s, %s)"%(self.low, self.high)

    def validateValue(self, val):
        """
        Validate that a given value conforms to the restriction.

        Arguments: val - the value to validate
        Returns:   Error list [type, spec]
                   If no errors: None
        """
        # Use this order to get right comparision operator
        if self.low is not None and val < self.low:
            raise RangeLowError(val, self.low)
        if self.high is not None and val > self.high:
            raise RangeHighError(val, self.high)

custombson.register(Range)


def RangeMin(value):
    return Range(value)

def RangeMax(value):
    return Range(high=value)

class Regexp(Restriction):
    """
    This class implements the Regexp type restriction for a TOI value.

    Please note that the regexp must match the entire string in
    order to succeed.
    """

    def __init__(self, regexp):
        """
        Initialize the instance

        Arguments: regexp - the regular expression to match by
        Returns:   None
        """
        self.regexp = regexp
        self.re = re.compile(regexp, re.U)

    def __getinitargs__(self):
        """
        Return a tuple containing the state-relevant variables.

        Arguments: None
        Returns:   A tuple containing the state-relevant variables.
        """
        return (self.regexp,)

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object.
        """
        return self.__class__.__name__ + "(%r)"%(self.regexp,)

    def validateValue(self, val):
        """
        Validate that a given value conforms to the restriction.

        Arguments: val - the value to validate
        Returns:   Error list [type, spec]
                   If no errors: None
        """
        # Has to match the entire value
        if not self.re.match(val):
            raise RegexpError

    def __getstate__(self):
        return self.regexp

    def __setstate__(self, state):
        self.regexp = state
        self.re = re.compile(state, re.U)

custombson.register(Regexp)


class Resolution(Restriction):
    """
    This class implements the Resolution type restriction for a TOI value.

    Please note that the regexp must match the entire string in
    order to succeed.
    """

    minute = 60
    hour = minute * 60
    day = hour * 24
    week = day * 7
    month = day * 30
    year = day * 365

    def __init__(self, resolution):
        """
        Initialize the instance

        Arguments: regexp - the regular expression to match by
        Returns:   None
        """
        self.resolution = resolution

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object.
        """
        return self.__class__.__name__ + "(%r)"%(self.resolution,)

    def validateValue(self, val):
        """
        Validate that a given value conforms to the restriction.

        Arguments: val - the value to validate
        Returns:   Error list [type, spec]
                   If no errors: None
        """
        if bool(val % self.resolution):
            raise ResolutionError

custombson.register(Resolution)


class Selection(Restriction):
    """
    This class implements the Selection type restriction for a TOI value.
    """

    def __init__(self, values):
        """
        Initialize the instance

        Arguments: values - list of valid values
        Returns:   None
        """
        self.validValues = values                  # List of valid values

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object.
        """
        return self.__class__.__name__ + "(%r)" % (self.validValues,)

    def validateValue(self, val):
        """
        Validate that a given value conforms to the restriction.

        Arguments: val - the value to validate
        Returns:   Error list [type, spec]
                   If no errors: None
        """
        if val not in self.validValues:
            raise SelectionError(val)

custombson.register(Selection)


class Size(Restriction):
    """
    This class implements the Size type restriction for a TOI value.
    """

    def __init__(self, min, max=None):
        """
        Initialize the instance

        Arguments: min - mininum size of a specific value
                   max - maximum size of a specific value
        Returns:   None
        """
        self.min = min                          # Min size
        self.max = max                          # Max size

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object.
        """
        return self.__class__.__name__ + "(%d, %d)"%(self.min, self.max)

    def validateValue(self, val):
        """
        Validate that a given value conforms to the restriction.

        Arguments: val - the value to validate
        Returns:   Error list [type, spec]
                   If no errors: None
        """
        # Use this order to get right comparision operator
        if self.min > len(val):
            raise SizeShortError(len(val), self.min)
        if self.max and self.max < len(val):
            raise SizeLongError(len(val), self.max)

custombson.register(Size)


class ToiType(Restriction):
    """
    This class implements the ToiType type restriction for a TOI value.
    """

    def __init__(self, validToiType, **qual):
        """
        Initialize the instance.

        Arguments: validToiType - the valid TOI type ('BLM:TOC' string)
                   qual - qualifications (query conditions) on this toi type
        Returns:   None
        """
        self.validToiType = validToiType                # Valid TOI spec
        self.qualification = qual

    def __repr__(self):
        """
        Return an informal string representation of the object.

        Arguments:   None
        Returns:     A string representation of the object.
        """
        return self.__class__.__name__ + "(%s)"%(self.validToiType,)

    def validateValue(self, val):
        """
        Validate that a given value conforms to the restriction.

        Arguments: val - the value to validate
        Returns:   Error list [type, spec]
                   If no errors: None
        """
        if not isinstance(val, self.validToiType):
            raise ToiTypeError(val.__class__._fullname + ':' + str(id(val.__class__)),
                               self.validToiType._fullname + ':' + str(id(self.validToiType)))

    def expandQual(self):
        return self.qualification

custombson.register(ToiType)
