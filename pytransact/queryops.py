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

"Query operators and associated helpers."

import re, time
from pytransact import custombson, textindexing


# Matches a number of non escaped asterisks
replaceAsterisk = re.compile(r'(^|[^\\])\\\*+')

# Matches a non escaped question mark
replaceQuestionmark = re.compile(r'(^|[^\\])\\\?')

# Matches an escaped character
replaceBackslash = re.compile(r'\\\\(..?)')

def makeRe(p, flags=0):
    """
    Create a 'glob' regexp. Supported expressions are
    * - match anything
    ? - match exactly one character and
    \n - escape the 'n' character

    Arguments: p - the pattern to turn into a regexp
               args, kw - re extra arguments
    Returns:   Compiled regexp
    """

    p = re.escape(p)

    if p.endswith(r'\*'):
        p = p[:-2]
    else:
        p = p + '$'

    if p.startswith(r'\*'):
        p = p[2:]
    else:
        p = '^' + p

    p = replaceAsterisk.sub(r'\1.*', p)
    p = replaceQuestionmark.sub(r'\1.', p)
    p = replaceBackslash.sub(r'\1', p)

    return re.compile(p, flags | re.U)


def getValue(valOb):
    """
    Return the value (for toi attributes it could be the object) of an
    attribute.

    Arguments: The value object
    Returns:   The value
    """
    if hasattr(valOb, 'value'):
        return valOb.value
    return valOb


class Operator(object):
    """
    Operator base class
    """

    def __init__(self, value):
        """
        Initialise the object.
        This default implementation is intended for the single-valued
        operators, and ensures that self.value contains a single value.

        Arguments: args - operator arguments
        Returns:   None
        """
        self.value = getValue(value)
        if type(self.value) not in (list,tuple,set,frozenset):
            self.value = [self.value]
        if len(self.value) != 1:
            raise ValueError('This operator takes a single argument')

    def __str__(self):
        if type(self.value) in (set,frozenset,tuple,list):
            return "%s(%s)" % (self.__class__.__name__,
                                 ','.join([repr(v) for v in self.value]))
        else:
            return "%s(%r)" % (self.__class__.__name__, self.value)

    def __repr__(self):
        if type(self.value) in (set,frozenset):
            return "%s([%s])" % (self.__class__.__name__,
                                 ', '.join([repr(v) for v in self.value]))
        elif type(self.value) is tuple:
            return "%s(%s)" % (self.__class__.__name__,
                                 ', '.join([repr(v) for v in self.value]))
        else:
            return "%s(%r)" % (self.__class__.__name__, self.value)

    def __eq__(self, other):
        """
        Operator equality.
        """
        if type(other) is type(self):
            return other.value == self.value
        return False

    def matches(self, value):
        """
        Determine if a given value matches the stored value for
        the operator in question.

        Arguments: value - the value to match
        Returns:   bool
        """
        raise NotImplementedError

    def copy(self, attr, translator=None):
        """
        Return a copy of this object, with all TOIref values translated
        using the provided translation dict.

        Attributes: attr - the Attribute object this condition is connected to
                    translator - dict mapping old to new values, OR a callable
                                 providing the same functionality
        """
        if translator is None:
            translator = lambda a,x: x
        if type(self.value) in (list, tuple, set, frozenset):
            return self.__class__([translator(attr, v) for v in self.value])
        else:
            return self.__class__(translator(attr, self.value))


class MapOperator(Operator):
    """
    Operator base class
    """

    def __init__(self, key, value=None):
        """
        Initialise the object.
        This default implementation is intended for the single-valued
        operators, and ensures that self.value contains a single value.

        Arguments: args - operator arguments
        Returns:   None
        """
        if value is None:
            key, value = key
        if type(key) not in (str, str):
            raise ValueError('The key provided is not a string')
        self.key = key

        value = getValue(value)
        if type(value) in (list,tuple,set,frozenset):
            value = value[0]
        self.value = key, value

    def __setstate__(self, state):
        self.key = state['key']
        self.value = tuple(state['value'])

    def copy(self, attr, translator=None):
        """
        Return a copy of this object, with all TOIref values translated
        using the provided translation dict.

        Attributes: attr - the Attribute object this condition is connected to
                    translator - dict mapping old to new values, OR a callable
                                 providing the same functionality
        """
        if translator is None:
            translator = lambda a,x: x
        key, value = self.value
        if type(value) in (list, tuple, set, frozenset):
            return self.__class__(key, [translator(attr, v) for v in value])
        else:
            return self.__class__(key, translator(attr, value))


class Between(Operator):
    """
    True if any value in the attribute is between the two specified
    values, inclusively. For single-element lists, this is the same as
    GreaterEq and LesserEq combined.

    Arguments: Two values; lower and upper bound of the value
    """

    def __init__(self, lower, upper=None):
        """
        Initialise the object

        Arguments: args - operator arguments
        Returns:   None
        """
        self.lower = getValue(lower)
        self.upper = getValue(upper)
        if upper is None:
            self.lower, self.upper = lower[0], lower[1]
        self.value = (self.lower, self.upper)

    def __setstate__(self, state):
        self.lower = state['lower']
        self.upper = state['upper']
        self.value = self.lower, self.upper

    def matches(self, value):
        lower = self.lower
        if isinstance(lower, Now):
            lower = lower.evaluate()
        upper = self.upper
        if isinstance(upper, Now):
            upper = upper.evaluate()

        for v in value:
            if v >= lower and v <= upper:
                return True

        return False

    def mongo(self):
        return {'$elemMatch': {'$gte': self.lower, '$lte': self.upper}}

custombson.register(Between)


class Empty(Operator):
    """
    True if the attribute has no value.

    Arguments: None.
    """

    def __init__(self):
        self.value = ()

    def __setstate__(self, state):
        self.value = ()

    def matches(self, value):
        return not value

    def copy(self, a, _=None):
        return self

    def mongo(self):
        return {'$empty': True}

custombson.register(Empty)


class NotEmpty(Operator):
    """
    True if the attribute has any value.

    Arguments: None.
    """

    def __init__(self):
        self.value = ()

    def __setstate__(self, state):
        self.value = ()

    def matches(self, value):
        return bool(value)

    def copy(self, a, _=None):
        return self

    def mongo(self):
        return {'$exists': True, '$ne': []}

custombson.register(NotEmpty)


class Exact(Operator):
    """
    True if the attribute the attribute contains the same values as
    the argument irrespective of order.

    Arguments: List of values.
    """

    def __init__(self, value):
        self.value = set(getValue(value))

    def matches(self, value):
        return self.value == set(value)

    def mongo(self):
        return {'$all' : list(self.value), '$size' : len(self.value)}

custombson.register(Exact)


class Fulltext(Operator):
    """
    True if the toi's fulltext representation matches the provided
    expression, according to the rules defined by the search engine.

    Arguments: A text search expression
    """
    def __init__(self, value, tocName=None):
        if isinstance(value, (list, tuple)):
            value, tocName = value
        self.value = (value, tocName)
        self.expression = value
        self.tocName = tocName
        self.results = None # Will get filled in by text engine

    def __setstate__(self, state):
        self.value = tuple(state['value'])
        self.expression = state['expression']
        self.tocName = state['tocName']
        self.results = None

    def matches(self, value):
        if self.results is None:
            return False
        return bool(set(self.results).intersection(value))

    def mongo(self):
        return {'$fulltext': sorted(textindexing.getTerms([self.expression]))}

custombson.register(Fulltext)


class Greater(Operator):
    """
    True if any value in the attribute is greater than the argument.

    Arguments: Single value
    """

    def matches(self, value):
        val = self.value[0]
        if isinstance(val, Now):
            val = val.evaluate()

        for v in value:
            if v > val:
                return True

        return False

    def mongo(self):
        return {'$gt': self.value[0]}

custombson.register(Greater)


class GreaterEq(Operator):
    """
    True if any value in the attribute is greater than or equal to
    the argument.

    Arguments: Single argument
    """

    def matches(self, value):
        val = self.value[0]
        if isinstance(val, Now):
            val = val.evaluate()

        for v in value:
            if v >= val:
                return True

        return False

    def mongo(self):
        return {'$gte': self.value[0]}

custombson.register(GreaterEq)


class HasKey(Operator):
    """
    True if any key in the attribute matches the argument.

    Arguments: Single string
    """

    def __init__(self, value):
        super(HasKey, self).__init__(value)
        self.key = self.value[0]
        if not isinstance(self.key, str):
            raise ValueError("Map keys must be strings.")

    def matches(self, value):
        key = self.value[0]
        for k,v in value:
            if k == key:
                return True
        return False

    def mongo(self):
        return {'.' : ( self.key, { '$exists' : True } ) }

custombson.register(HasKey)


class LacksKey(Operator):
    """
    True if no keys in the attribute matches the argument.

    Arguments: Single string.
    """

    def __init__(self, value):
        super(LacksKey, self).__init__(value)
        self.key = self.value[0]
        if not isinstance(self.key, str):
            raise ValueError("Map keys must be strings.")

    def matches(self, value):
        key = self.value[0]

        for k,v in value:
            if k == key:
                return False
        return True

    def mongo(self):
        return {'.' : ( self.key, { '$exists' : False } ) }

custombson.register(LacksKey)


class Ilike(Operator):
    """
    True if any value in the attribute is case-insensitively like the argument.

    Arguments: Single string.
    """

    def matches(self, value):

        r = makeRe(self.value[0], re.I)
        for v in value:
            if r.search(v):
                return True

        return False

    def mongo(self):
        return {'$regex': makeRe(self.value[0], re.I)}

custombson.register(Ilike)


class IlikeMap(MapOperator):
    """
    True if any keyed value in the attribute is like the argument.

    Arguments: key, single string pattern.
    """

    def matches(self, value):
        key, val = self.value
        r = makeRe(val, re.I)
        for k,v in value:
            if k != key:
                continue
            if r.search(v):
                return True
        return False

    def mongo(self):
        return {'.' : (self.key, {'$regex': makeRe(self.value[1], re.I)})}

custombson.register(IlikeMap)


class NotIlike(Operator):
    """
    True if all values in the attribute are case-insensitively unlike the argument.

    Arguments: Single string.
    """

    def matches(self, value):

        r = makeRe(self.value[0], re.I)
        for v in value:
            if r.search(v):
                return False

        return True

    def mongo(self):
        return {'$not': makeRe(self.value[0], re.I)}

custombson.register(NotIlike)


class NotIlikeMap(MapOperator):
    """
    True if any keyed value in the attribute is unlike the argument.

    Arguments: key, single string pattern.
    """

    def matches(self, value):
        key, val = self.value
        r = makeRe(val, re.I)
        for k,v in value:
            if k != key:
                continue
            if not r.search(v):
                return True
        return False

    def mongo(self):
        return {'.' : (self.key, {'$not': makeRe(self.value[1], re.I)})}

custombson.register(NotIlikeMap)


class In(Operator):
    """
    True if any value in the attribute is equal to any value in the
    argument.

    Arguments: List of values
    """

    def __init__(self, value):
        value = getValue(value)
        if not isinstance(value, (list, tuple, set, frozenset)):
            self.value = set([ value ])
        else:
            self.value = set(value)

    def matches(self, value):
        return bool(self.value.intersection(value))

    def mongo(self):
        return { '$in': list(self.value) }

custombson.register(In)


class InMap(Operator):
    """
    True if any keyed value in the attribute is equal to any value in the
    argument.

    Arguments: key, list of values
    """

    def __init__(self, key, value=None):
        if value is None:
            key, value = key
        if type(key) not in (str, str):
            raise ValueError('The key provided is not a string')
        self.key = key

        value = getValue(value)
        if type(value) not in (list, tuple, set, frozenset):
            value = set([ value ])
        else:
            value = set(value)
        self.value = key, value

    def __setstate__(self, state):
        self.key = state['key']
        self.value = tuple(state['value'])

    def matches(self, value):
        for key, val in value:
            if key != self.key:
                continue
            if val in self.value[1]:
                return True
        return False

    def mongo(self):
        return {'.': (self.key, { '$in': list(self.value[1]) })}

custombson.register(InMap)


class NotIn(Operator):
    """
    True if any value in the attribute is unequal to all values in the
    argument.

    Arguments: List of values
    """

    def __init__(self, value):
        value = getValue(value)
        if not isinstance(value, (list, tuple, set, frozenset)):
            self.value = set([ value ])
        else:
            self.value = set(value)

    def matches(self, value):
        return bool(set(value).difference(self.value))

    def mongo(self):
        return { '$nin': list(self.value) }

custombson.register(NotIn)


class Less(Operator):
    """
    True if any value in the attribute is less than the argument.

    Arguments: Single value
    """

    def matches(self, value):

        for v in value:
            if v < self.value[0]:
                return True

        return False

    def mongo(self):
        return {'$lt': self.value[0]}

custombson.register(Less)


class LessEq(Operator):
    """
    True if any value in the attribute is lesser than or equal to the argument.

    Arguments: Single value
    """

    def matches(self, value):

        for v in value:
            if v <= self.value[0]:
                return True

        return False

    def mongo(self):
        return {'$lte': self.value[0]}

custombson.register(LessEq)


class Like(Operator):
    """
    True if any value in the attribute is like the argument.

    Arguments: Single string.
    """

    def matches(self, value):

        r = makeRe(self.value[0])
        for v in value:
            if r.search(v):
                return True

        return False

    def mongo(self):
        return {'$regex': makeRe(self.value[0])}

custombson.register(Like)


class LikeMap(MapOperator):
    """
    True if any keyed value in the attribute is unlike the argument.

    Arguments: key, single string pattern.
    """

    def matches(self, value):
        key, val = self.value
        r = makeRe(val)
        for k,v in value:
            if k != key:
                continue
            if r.search(v):
                return True
        return False

    def mongo(self):
        return {'.' : (self.key, {'$regex': makeRe(self.value[1])})}

custombson.register(LikeMap)


class NotLike(Operator):
    """
    True if any value in the attribute is unlike the argument.

    Arguments: Single string.
    """

    def matches(self, value):

        r = makeRe(self.value[0])
        for v in value:
            if not r.search(v):
                return True

        return False

    def mongo(self):
        return {'$not': makeRe(self.value[0])}

custombson.register(NotLike)


class NotLikeMap(MapOperator):
    """
    True if any keyed value in the attribute is unlike the argument.

    Arguments: key, single string pattern.
    """

    def matches(self, value):
        key, val = self.value
        r = makeRe(val)
        for k,v in value:
            if k != key:
                continue
            if not r.search(v):
                return True
        return False

    def mongo(self):
        return {'.' : (self.key, {'$not': makeRe(self.value[1])})}

custombson.register(NotLikeMap)


class NoneOf(Operator):
    """
    True if no value in the attribute is equal to any of the values
    in the argument

    Arguments: List of values.
    """

    def __init__(self, value):
        value = getValue(value)
        if not isinstance(value, (list, tuple, set, frozenset)):
            self.value = set([ value ])
        else:
            self.value = set(value)

    def matches(self, value):
        return not self.value.intersection(value)

    def mongo(self):
        return { '$nor' : list(self.value) }

custombson.register(NoneOf)


class NoneOfMap(Operator):
    """
    True if no keyed value in the attribute is equal to any value in the
    argument.

    Arguments: key, list of values
    """

    def __init__(self, key, value=None):
        if value is None:
            key, value = key
        if type(key) not in (str, str):
            raise ValueError('The key provided is not a string')
        self.key = key

        value = getValue(value)
        if type(value) not in (list, tuple, set, frozenset):
            value = set([ value ])
        else:
            value = set(value)
        self.value = key, value

    def __setstate__(self, state):
        self.key = state['key']
        self.value = tuple(state['value'])

    def matches(self, value):
        result = False
        for key, val in value:
            if key != self.key:
                continue
            result = True
            if val in self.value[1]:
                return False
        return result

    def mongo(self):
        return {'.': (self.key, { '$nor' : list(self.value[1]) })}

custombson.register(NoneOfMap)


class Readable(Operator):
    """
    Only matches readable objects.

    Arguments: None.
    """

    def __init__(self):
        self.value = ()

    def __setstate__(self, state):
        self.value = ()

    def matches(self, value):
        return True

    def copy(self, a, _=None):
        return self

custombson.register(Readable)


class RegEx(Operator):
    """
    True if any value in the attribute matches the regular expression.

    Arguments: Compiled regular expression OR
               regular expression string and flags.
    """

    def __init__(self, regex, flags=0):
        if isinstance(regex, str):
            self.value = re.compile(regex, flags | re.U)
        else:
            assert regex.flags & re.U
            self.value = regex

    def __deepcopy__(self, memo):
        return RegEx(self.value)

    def matches(self, value):
        for v in value:
            if self.value.search(v):
                return True
        return False

    def mongo(self):
        return {'$regex': self.value}

custombson.register(RegEx)


class SubQuery(Operator):
    """
    Mixin class for Query indicating it is an operator.
    """
    pass

custombson.register(SubQuery)


class Now(object):
    """
    Now time marker

    Arguments: delta - interval from now in seconds.
                       Positive values are in the future.
               resolution - how precisely the values must match, in
                            seconds.
    """

    def __init__(self, delta=0, resolution=1):
        self.delta = delta
        self.resolution = resolution

    def __eq__(self, other):
        return (self.__class__ is other.__class__ and
                self.delta == other.delta and
                self.resolution == other.resolution)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        if self.resolution == 1:
            return '%s(%s)' % (self.__class__.__name__, self.delta)
        else:
            return '%s(%s, %s)' % (self.__class__.__name__,
                                  self.delta, self.resolution)

    def evaluate(self, when=None):
        if when is None:
            when = time.time()
        when = int(when + self.delta)
        # Midpoint of resolution
        when = when - when % self.resolution
        return when

    def copy(self, a, _=None):
        return Now(self.delta, self.resolution)

custombson.register(Now)
