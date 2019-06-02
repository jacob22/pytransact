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
Exceptions intended for transmission between processes
"""

from pytransact import spickle, custombson

# Ensure importability
#try:
import builtins
# except ImportError:
#     import __builtin__ as builtins

if '_st' not in builtins.__dict__:
    builtins.__dict__['_st'] = lambda x: x

class ClientError(Exception):
    """
    Used to wrap another exception so that it is sent to the client in
    unmodified form. All other exceptions should be replaced with a
    general exception, and the 'real' exception should be printed on the
    console.
    """
# spickle.stateExtension(ClientError) # not spicklable since it's for
                                      # internal use
custombson.register(ClientError)


class LocalisedError(Exception):
    """
    Base error class for all errors that are possible to translate into
    a localised, human-readable form.
    """
    message = ''        # defaults to 'Exceptions.<exception name>'
    t = {}              # Translatable parameters
    nt = {}             # Non-translatable parameters

    def xlat(self, tfunc, raiseError=False):
        """
        Perform string/value translation.

        Arguments: tfunc - translation function
        Returns:   Translated string
        """
        if not self.message:
            self.message = 'Exceptions.'+self.__class__.__name__

        x = dict((i, tfunc(v)) for i, v in self.t.items())
        x.update(self.nt)

        try:
            return tfunc(self.message) % x
        except KeyError:
            # Error message is borken!
            if raiseError:
                raise
            return "Message '%s' (originally '%s') failed does not work " \
                   "with dict %s" % (tfunc(self.message), self.message, str(x))

    def __bytes__(self):
        return str(self.message).encode("utf-8")

    def __str__(self):
        msg = self.xlat(lambda x: x)
        if not isinstance(msg, str):
            msg = msg.decode("ascii", "ignore")
        return msg

    def __getstate__(self):
        result = {}
        for param in ('t', 'nt', 'args', 'message'):
            if getattr(self, param) != getattr(self.__class__, param):
                result[param] = getattr(self, param)
        return result

    def __setstate__(self, state):
        for param in ('t', 'nt', 'args', 'message'):
            if param in state:
                setattr(self, param, state[param])
spickle.stateExtension(LocalisedError)
custombson.register(LocalisedError)

class MessageError(LocalisedError):
    """
    Base class for the 'miscellaneous' errors that may turn up. When created,
    MessageErrors should be initialised with a translation-tagged format string
    and two dictionaries. The first contains translatable parameters and the
    second contains untranslatable parameters that should be inserted verbatim.
    """
    def __init__(self, mess, t={}, nt={}):
        LocalisedError.__init__(self, mess)
        self.message = mess
        self.t = t
        self.nt = nt
spickle.stateExtension(MessageError)
custombson.register(MessageError)

class BlError(MessageError, RuntimeError): pass
spickle.stateExtension(BlError)
custombson.register(BlError)
class BlmError(MessageError, RuntimeError): pass
spickle.stateExtension(BlmError)
custombson.register(BlmError)
def cBlmError(*args, **kw): return ClientError(BlmError(*args, **kw))

class PermissionError(MessageError, RuntimeError): pass
spickle.stateExtension(PermissionError)
custombson.register(PermissionError)
def cPermissionError(*args, **kw): return ClientError(PermissionError(*args, **kw))

class cValueError(MessageError, ValueError): pass
spickle.stateExtension(cValueError)
custombson.register(cValueError)
class cSyntaxError(MessageError, SyntaxError): pass
spickle.stateExtension(cSyntaxError)
custombson.register(cSyntaxError)
class cRuntimeError(MessageError, RuntimeError): pass
spickle.stateExtension(cRuntimeError)
custombson.register(cRuntimeError)
class cLookupError(MessageError, LookupError): pass
spickle.stateExtension(cLookupError)
custombson.register(cLookupError)
class cIndexError(MessageError, IndexError): pass
spickle.stateExtension(cIndexError)
custombson.register(cIndexError)
class cTypeError(MessageError, TypeError): pass
spickle.stateExtension(cTypeError)
custombson.register(cTypeError)

class NonAsciiMailChars(MessageError, ValueError):
    """
    The message adress of contains non-ascii characters.
    """
    def __init__(self, field, address):
        MessageError.__init__(self, _st(''
                        'Exceptions.cNonAsciiMailChars %(field)s %(address)s'),
                        nt={'field': field, 'address': address,})
spickle.stateExtension(NonAsciiMailChars)
custombson.register(NonAsciiMailChars)
def cNonAsciiMailChars(*args, **kw): return ClientError(NonAsciiMailChars(*args, **kw))

class MailNoUtf8(MessageError, ValueError):
    """
    The message adress of contains non-ascii characters.
    """
    def __init__(self):
        MessageError.__init__(self, _st('Exceptions.MailNoUtf8'))
spickle.stateExtension(MailNoUtf8)
custombson.register(MailNoUtf8)
def cMailNoUtf8(*args, **kw): return ClientError(MailNoUtf8(*args, **kw))

#
# TOI errors
#

class CapsToiError(LocalisedError):
    """
    Base exception for all exceptions related to attributes. The first element
    in self.args should be the toc full name and the second the TOI id if
    available. Subclasses may specify further arguments.
    """
    message = builtins._st("Exceptions.CapsToiError")
    def __init__(self, tocname, toid, *args):
        if not tocname.startswith('blm.'):
            tocname = 'blm.%s'%(tocname,)
        self.tocname, self.toid = tocname, toid
        args = [tocname, toid] + list(args)
        LocalisedError.__init__(self, *args)
        self.t = { 'toc'  : self.tocname }
        self.nt = { 'toid' : self.toid }
spickle.stateExtension(CapsToiError)
custombson.register(CapsToiError)

class ToiNonexistantError(CapsToiError):
    message = builtins._st("Exceptions.ToiNonexistantError '%(toc)s(%(toid)s)'")
spickle.stateExtension(ToiNonexistantError)
custombson.register(ToiNonexistantError)
def cToiNonexistantError(*args): return ClientError(ToiNonexistantError(*args))

class ToiDeletedError(ToiNonexistantError):
    message = builtins._st("Exceptions.ToiDeletedError '%(toc)s(%(toid)s)'")

    def __init__(self, tocname, toid, *args):
        self.t = {'toc': tocname, 'toid': toid}
        ToiNonexistantError.__init__(self, tocname, toid, *args)

spickle.stateExtension(ToiDeletedError)
custombson.register(ToiDeletedError)
def cToiDeletedError(*args): return ClientError(ToiDeletedError(*args))

#
# Attribute errors
#
class AttrErrorList(LocalisedError):
    """
    Wraps a list of attribute errors.
    """
    message = builtins._st("Exceptions.AttrErrorList %(errors)s")
    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]
        LocalisedError.__init__(self, *args)
        self.t = {}
        self.nt = {}
        if len(args):
            self.t.update(args[0].t)
            self.nt.update(args[0].nt)

    def xlat(self, tfunc):
        self.nt['errors'] = '\n'.join([e.xlat(tfunc) for e in self.args])
        return LocalisedError.xlat(self, tfunc)
spickle.stateExtension(AttrErrorList)
custombson.register(AttrErrorList)
def cAttrErrorList(*args): return ClientError(AttrErrorList(*args))

class CapsAttributeError(LocalisedError):
    """
    Base exception for all exceptions related to attributes. The first
    element in self.args should be the attribute name, toc or blm name
    and toi ID. Subclasses may specify further arguments.
    """
    message = builtins._st("Exceptions.CapsAttributeError")
    def __init__(self, name, tocname, toid, *args):
        self.error = (args and args[0]) or None
        name = str(name) # Might be an int for method params
        self.name, self.tocname, self.toid = name, tocname, toid
        args = [name, tocname, toid] + list(args)
        LocalisedError.__init__(self, *args)
        self.t = { 'name' : self.name, 'toc': self.tocname  }
        self.nt = { 'toid' : self.toid }
spickle.stateExtension(CapsAttributeError)
custombson.register(CapsAttributeError)

class AttrNameError(CapsAttributeError, AttributeError):
    """
    The attribute wasn't found (probably a typo).
    """
    message = builtins._st("Exceptions.AttrNameError %(name)s: '%(toc)s(%(toid)s)'")
spickle.stateExtension(AttrNameError)
custombson.register(AttrNameError)
def cAttrNameError(*args): return ClientError(AttrNameError(*args))

class AttrPermError(CapsAttributeError):
    """
    Permission was denied for the attempted action.
    """
    message = builtins._st("Exceptions.AttrPermError %(name)s: '%(toc)s(%(toid)s)'")
spickle.stateExtension(AttrPermError)
custombson.register(AttrPermError)
def cAttrPermError(*args): return ClientError(AttrPermError(*args))

class AttrValueError(CapsAttributeError):
    """
    The value for this attribute was incorrect. The last element in
    self.args should be the error.
    """
    message = builtins._st("Exceptions.AttrValueError %(name)s: %(error)s")

    def xlat(self, tfunc):
        """
        Translate the contained error and run normal translation.
        """
        if self.args:
            # The last element in self.args should be the error.
            error = self.args[-1]
            if isinstance(error, LocalisedError):
                err = error.xlat(tfunc)
            else:
                err = repr(error)
            self.nt['error'] = err
        return CapsAttributeError.xlat(self, tfunc)
spickle.stateExtension(AttrValueError)
custombson.register(AttrValueError)

def cAttrValueError(*args): return ClientError(AttrValueError(*args))

# Thrown by calling coerceValueList
class ValueErrorList(LocalisedError):
    """
    The value for this attribute was incorrect. self.args is a list of
    tuples (i, e), where i is the index in the value list and e is the
    error raised for that value.
    """
    message = builtins._st("Exceptions.ValueErrorList")

    def xlat(self, tfunc):
        return ', '.join(['%d: %s' % (i, x.xlat(tfunc))
                          for (i, x) in self.args])

    def __str__(self):
        return ' [' + ', '.join(['%d %s' % (i, repr(x))
                                 for (i, x) in self.args]) + ']'
    __repr__ = __str__
spickle.stateExtension(ValueErrorList)
custombson.register(ValueErrorList)

#
# The following errors are all thrown by calling coerceValue with an
# incorrect argument. self.args is the value that was tried.
#
class ValueTypeError(LocalisedError):
    """
    The type for this value was incorrect. The first element in
    self.args should be the (incorrect) value.
    """
    message = builtins._st("Exceptions.ValueTypeError")
    def __init__(self, value):
        LocalisedError.__init__(self, value)
        self.nt = { 'value' : value }
spickle.stateExtension(ValueTypeError)
custombson.register(ValueTypeError)

class BoolValueError(ValueTypeError):
    message = builtins._st("Exceptions.BoolValueError")
spickle.stateExtension(BoolValueError)
custombson.register(BoolValueError)

class BlobValueError(ValueTypeError):
    message = builtins._st("Exceptions.BlobValueError")
spickle.stateExtension(BlobValueError)
custombson.register(BlobValueError)

class DecimalValueError(ValueTypeError):
    message = builtins._st("Exceptions.DecimalValueError")
spickle.stateExtension(DecimalValueError)
custombson.register(DecimalValueError)

class EnumValueError(ValueTypeError):
    message = builtins._st("Exceptions.EnumValueError")
spickle.stateExtension(EnumValueError)
custombson.register(EnumValueError)

class FloatValueError(ValueTypeError):
    message = builtins._st("Exceptions.FloatValueError")
spickle.stateExtension(FloatValueError)
custombson.register(FloatValueError)

class IntValueError(ValueTypeError):
    message = builtins._st("Exceptions.IntValueError")
spickle.stateExtension(IntValueError)
custombson.register(IntValueError)

class StringValueError(ValueTypeError):
    message = builtins._st("Exceptions.StringValueError")
spickle.stateExtension(StringValueError)
custombson.register(StringValueError)

class TimespanValueError(ValueTypeError):
    message = builtins._st("Exceptions.TimespanValueError")
spickle.stateExtension(TimespanValueError)
custombson.register(TimespanValueError)

class TimestampValueError(ValueTypeError):
    message = builtins._st("Exceptions.TimestampValueError")
spickle.stateExtension(TimestampValueError)
custombson.register(TimestampValueError)

class ToiRefValueError(ValueTypeError):
    message = builtins._st("Illegal ToiRef value: %(value)r")
spickle.stateExtension(ToiRefValueError)
custombson.register(ToiRefValueError)

class ToiRefMapValueError(ValueTypeError):
    message = builtins._st("Illegal ToiRefMap value: %(value)r")
spickle.stateExtension(ToiRefMapValueError)
custombson.register(ToiRefMapValueError)

#
# Restriction errors.
#

# Specialisation to separate between type and restriction errors
class RestrictionErrorList(ValueErrorList):
    message = builtins._st("Exceptions.RestrictionErrorList")
spickle.stateExtension(RestrictionErrorList)
custombson.register(RestrictionErrorList)

class RestrictionError(LocalisedError):
    def __init__(self, *args):
        LocalisedError.__init__(self, *args)
        self.nt = dict([(str(i), args[i]) for i in range(len(args))])

    def __str__(self):
        return '<%s %r>' % (self.__class__.__name__, self.args)
    __repr__ = __str__


class QuantityMinError(RestrictionError):
    # FIXME: You are *not* allowed to create this error type without specifying
    # the minimum quantity required. Will break spickling logic.
    def __init__(self, *args):
        RestrictionError.__init__(self, *args)
        if args[0] > 1:
            self.message = _st("Exceptions.QuantityMinError %(0)d given %(1)d expected")
        else:
            self.message = _st("Exceptions.QuantityMinError not empty")
spickle.stateExtension(QuantityMinError)
custombson.register(QuantityMinError)

class QuantityMaxError(RestrictionError):
    message = builtins._st("Exceptions.QuantityMaxError")
spickle.stateExtension(QuantityMaxError)
custombson.register(QuantityMaxError)

class NonDistinctError(RestrictionError):
    message = builtins._st("Exceptions.NonDistinctError")
spickle.stateExtension(NonDistinctError)
custombson.register(NonDistinctError)

class RangeLowError(RestrictionError):
    message = builtins._st("Exceptions.RangeLowError")
spickle.stateExtension(RangeLowError)
custombson.register(RangeLowError)

class RangeHighError(RestrictionError):
    message = builtins._st("Exceptions.RangeHighError")
spickle.stateExtension(RangeHighError)
custombson.register(RangeHighError)

class RegexpError(RestrictionError):
    message = builtins._st("Exceptions.RegexpError")
spickle.stateExtension(RegexpError)
custombson.register(RegexpError)

class ResolutionError(RestrictionError):
    message = builtins._st("Exceptions.ResolutionError")
spickle.stateExtension(ResolutionError)
custombson.register(ResolutionError)

class SelectionError(RestrictionError):
    message = builtins._st("Exceptions.SelectionError")
spickle.stateExtension(SelectionError)
custombson.register(SelectionError)

class SizeShortError(RestrictionError):
    message = builtins._st("Exceptions.SizeShortError")
spickle.stateExtension(SizeShortError)
custombson.register(SizeShortError)

class SizeLongError(RestrictionError):
    message = builtins._st("Exceptions.SizeLongError")
spickle.stateExtension(SizeLongError)
custombson.register(SizeLongError)

class QualificationError(RestrictionError):
    message = builtins._st("Exceptions.QualificationError")
spickle.stateExtension(QualificationError)
custombson.register(QualificationError)

class ToiTypeError(RestrictionError):
    message = builtins._st("Exceptions.ToiTypeError")
spickle.stateExtension(ToiTypeError)
custombson.register(ToiTypeError)

class RelationError(ToiTypeError):
    message = builtins._st("Exceptions.RelationError")
spickle.stateExtension(RelationError)
custombson.register(RelationError)

class UniqueError(RestrictionError):
    message = builtins._st("Exceptions.UniqueError")
spickle.stateExtension(UniqueError)
custombson.register(UniqueError)
