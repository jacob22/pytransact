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

import py.test
from pytransact.object.property import *

def test_PropertyInherit():
    "Test inhibition of base Property class usage"

    def spam():
        r = Property()

    py.test.raises(SyntaxError, spam)

def test_Singleton():
    "Test that a property is a singleton"

    assert ReadOnly() is ReadOnly() is ReadOnly()

def test_Parent():
    "Tests Parent property"

    p = Parent()

def test_Presentation():
    "Tests Presentation property"

    p = Presentation()

def test_ReorderOnly():
    "Tests ReorderOnly property"

    p = ReorderOnly()

def test_ReadOnly():
    "Tests ReadOnly property"

    p = ReadOnly()

def test_Unchangeable():
    "Tests Unchangeable property"

    p = Unchangeable()

def test_Unique():
    "Tests Unique property"

    p = Unique()

def test_Weak():
    "Tests Weak property"

    p = Weak()
