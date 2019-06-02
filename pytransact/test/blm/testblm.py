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

from pytransact.object.model import *
from pytransact.exceptions import cBlmError
from pytransact.query import Query

class Test(TO):
    class name(LimitedString()):
        def on_index(attr, val, toi):
            return val

    class attr1(LimitedString()):
        pass

    class attr2(Blob()):
        pass

    class attr3(StringMap()):
        pass

    class attr4(ToiRef()):
        pass

    class attr5(ToiRefMap()):
        pass

    @method(Serializable)
    def the_method(self):
        return [{'foo': 'bar'}]

    @staticmethod
    def sort_name_key(p1):
        return p1['name'][0]

    @staticmethod
    def sort_attr1_key(p1):
        return p1['attr1'][0]

    @staticmethod
    def sort_attr2_key(p1):
        return p1['attr2'][0].getvalue()

@method(Serializable)
def the_method(k1=LimitedString(), k2=LimitedString()):
    if k1[0] == 'redirect':
        return [{'__oe_redirect': 'http://foo.bar.baz/'}]
    return [{'k1': k1, 'k2': k2, '__oe_headers': [('X-Foo',  'Foo')]}]

@method(Serializable)
def client_error():
    raise cBlmError('foo')

@method(Serializable)
def unknown_error():
    raise RuntimeError('foo')



class Defaults(TO):

    class no_default(Int()):
        pass

    class empty_default(Int()):
        default = []

    class has_default(Int()):
        default = [42]


class Base(TO):
    pass

class Sub(Base):
    pass
