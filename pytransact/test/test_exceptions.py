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
from pytransact import spickle, exceptions


class TestExceptions(object):

    def test___bytes__(self):
        e = exceptions.LocalisedError()
        e.message = "foo"
        assert bytes(e) == b"foo"

        e = exceptions.LocalisedError()
        e.message = "\xc5"
        assert bytes(e) == b'\xc3\x85'

        # e = exceptions.LocalisedError()
        # e.message = "xx\xc5"
        # assert bytes(e) == b"xx"


    def test___str__(self):
        e = exceptions.LocalisedError()
        e.message = b"foo"
        assert str(e) == "foo"

        e = exceptions.LocalisedError()
        e.message = "\xc5"
        assert str(e) == "\xc5"

        # e = exceptions.LocalisedError()
        # e.message = "xx\xc5"
        # assert str(e) == "xx"

    def test_spickle_MessageError(self):
        x = exceptions.MessageError('foo', t={'a':'apa'},
                                    nt={'b':'bepa'})
        y = spickle.loads(spickle.dumps(x))

        assert x.__class__ is y.__class__
        assert x.message == y.message
        assert x.args == y.args

        assert x.t == y.t
        assert x.nt == y.nt

    def test_spickle_AttrValueError(self):
        x = exceptions.AttrValueError('foo', 'foo.bar', 27,
                                      exceptions.QuantityMinError(1))
        y = spickle.loads(spickle.dumps(x))

        assert x.__class__ is y.__class__
        assert x.message == y.message
        assert x.args[:3] == y.args[:3]
        assert type(x.args[3]) == type(y.args[3])
        assert x.args[3].__dict__ == y.args[3].__dict__

        assert x.t == y.t
        assert x.nt == y.nt

        # Just to make sure the translation code still functions
        y.xlat(lambda x: x)


class TestLocalisedError(object):

    def test_xlat_simple_with_keyword(self):
        x = exceptions.LocalisedError()
        x.message = "Dummy text failed with %(tagword)s"
        x.t  = {}
        x.nt = { 'tagword': 'hello!' }

        assert x.xlat(lambda x: x) == "Dummy text failed with hello!"

    def test_xlat_raise_error(self):
        x = exceptions.LocalisedError()
        x.message = "Dummy text failed with %(tagword)s"
        x.t  = {}
        x.nt = {}

        raises(KeyError, x.xlat, lambda x: x, raiseError=True)

    def test_xlat_simple_NO_raise_error(self):
        x = exceptions.LocalisedError()
        x.message = "Dummy text failed with %(tagword)s"
        x.t  = {}
        x.nt = {}

        assert len(x.xlat(lambda x: x)) > 0  # some descriptive fault msg
