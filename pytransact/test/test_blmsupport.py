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

from pytransact.testsupport import loadBLMs, BLMTests
import blm 
import bson

def setup_module(module):
    from blm import fundamental


def teardown_module(module):
    blm.clear()


class TestBson(BLMTests):

    def test_bson(self):
        loadBLMs([('fooBlm', fooBlm)])

        to = blm.fooBlm.FooToc()

        son = bson.BSON.encode({'toi': to})
        decoded = son.decode()

        assert decoded['toi'].__class__ == blm.fooBlm.FooToc
        assert decoded['toi'].id == to.id


class TestRelations(object):

    def teardown_method(self, method):
        blm.clear()

    def test_annotateBlmForRelations(self):
        loadBLMs([('fundamental', baseBlm),
                  ('fooBlm', fooBlm),
                  ('barBlm', barBlm)])

        assert blm.barBlm.Foo.subParts.related is blm.fooBlm.MIMEPart.superPart

baseBlm = """
import pytransact.object.model as M

class AccessHolder(M.TO):
    pass
"""

fooBlm = """
import pytransact.object.model as M

class blmAttr(M.String()):
    pass

class MIMEPart(M.TO):

    class superPart(M.Relation()):
        related = 'MIMEPart.subParts'

    class subParts(M.Relation()):
        related = 'MIMEPart.superPart'

class FooToc(M.TO):

    class usingBlmAttr(blmAttr()):
        pass
"""

barBlm = """
import pytransact.object.model as M
from blm import fooBlm

class Foo(fooBlm.MIMEPart):
    pass
"""
