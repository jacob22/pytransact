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

from py.test import raises
from pytransact.object.restriction import *

def test_RestrictionInherit():
    "Test inhibition of base Restriction class usage"

    def spam():
        r = Restriction()

    raises(SyntaxError, spam)

def test_Quantity():
    "Tests the Quantity restriction"

    r = Quantity(1, 3)

    r.validateValueList([1, 2])
    raises(QuantityMinError, r.validateValueList, [])
    raises(QuantityMaxError, r.validateValueList, [1, 2, 3, 4])

    # Not sure how the forever BLM got here, but it shouldn't work!
    r = Quantity(0)
    raises(QuantityMaxError, r.validateValueList, [1])

    r = Quantity(1)
    r.validateValueList([1])
    raises(QuantityMinError, r.validateValueList, [])
    raises(QuantityMaxError, r.validateValueList, [1, 2])

    r = QuantityMin(1)
    raises(QuantityMinError, r.validateValueList, [])
    r.validateValueList([1, 2, 3, 4, 5, 6])

    r = QuantityMax(2)
    r.validateValueList([])
    raises(QuantityMaxError, r.validateValueList, [1, 2, 3])

def test_Range():
    "Tests the Range restriction"

    r = Range(1, 3)

    r.validateValue(2)
    raises(RangeLowError, r.validateValue, -1)
    raises(RangeHighError, r.validateValue, 99)

    r = Range(1)

    raises(RangeLowError, r.validateValue, -1)
    r.validateValue(2)

    r = Range(None, 3)

    r.validateValue(-1)
    raises(RangeHighError, r.validateValue, 99)

    r = Range(0)
    raises(RangeLowError, r.validateValue, -1)

    r = Range(None, 0)
    raises(RangeHighError, r.validateValue, 1)

def test_Regexp():
    "Tests the Regexp restriction"

    r = Regexp('abc*')

    r.validateValue('ab9')
    raises(RegexpError, r.validateValue, 'xa')

def test_Resolution():
    "Tests the Restriction restriction"

    r = Resolution(Resolution.day)

    r.validateValue(24*60*60*3)
    raises(ResolutionError, r.validateValue, 99)

def test_Selection():
    "Tests the Selection restriction"

    r = Selection([1, 3, 9])

    r.validateValue(1)
    r.validateValue(3)
    r.validateValue(9)
    raises(SelectionError, r.validateValue, 2)

def test_Size():
    "Tests the Size restriction"

    r = Size(2, 9)

    r.validateValue('abc')
    raises(SizeShortError, r.validateValue, 'a')
    raises(SizeLongError, r.validateValue, 'abcdefghij')

    r = Size(2)
    r.validateValue('abc')
    raises(SizeShortError, r.validateValue, 'a')
    r.validateValue('abcdefghij')

def test_ToiType():
    "Tests the ToiType restriction"

    class ValidToc(object):
        _fullname = 'valid.toc'
    class InvalidToc(object):
        _fullname = 'invalid.toc'

    r = ToiType(ValidToc)

    r.validateValue(ValidToc())
    raises(ToiTypeError, r.validateValue, InvalidToc())
