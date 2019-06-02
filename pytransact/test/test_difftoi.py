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

import bson
from pytransact import difftoi
from pytransact.testsupport import Fake


def test_matchOld():
    toc = Fake(_fullname='blm.toc', _attributes=['foo', 'bar'])
    toi = Fake(foo=['bar'], baz=['qux'])
    toiDiff = difftoi.DiffTOI()
    toiDiff.setAttrDiff(toc, 42, toi.__dict__, {'foo': ['apa']})

    assert not toiDiff.diffsOld(toi)

    toi.foo = ['apa']
    assert toiDiff.diffsOld(toi) == {'foo': (['apa'], ['bar'])}


def test_setToi():
    class TO(Fake):
        _fullname = 'blm.TO'
        _attributes = 'attr1', 'attr2', 'attr3', 'attr4'
        def __getattr__(self, attrName):
            return Fake(value=self._attrData.get(attrName, []),
                        default={} if attrName == 'attr4' else [])
        @property
        def _modified(self):
            return set(self._orgAttrData)

    toi = TO(id=[42],
             _attrData={'attr1': ['changed'],
                        'attr2': ['new'],
                        'attr3': ['same'],
                        'attr4': {'foo': 'bar'}},
             _orgAttrData={'attr1': ['old'],
                           'attr2': [],
                           'attr4': {}})
    toiDiff = difftoi.DiffTOI()
    toiDiff.setToi(toi)

    assert toiDiff.toid == 42
    assert toiDiff.toc_fullname == 'blm.TO'
    assert toiDiff.diffAttrs == {'attr1': ['changed'],
                                 'attr2': ['new'],
                                 'attr4': {'foo': 'bar'}}
    assert toiDiff.orgAttrs == {'attr1': ['old'],
                                'attr2': [],
                                'attr4': {}}


def test_serializable():
    dt = difftoi.DiffTOI()
    dt.toid = toid = bson.objectid.ObjectId()
    dt.toc = object()
    dt.toc_fullname = 'TO'
    dt.diffAttrs = {'foo': [1], 'bar': [2]}
    data = {'difftoi': dt}
    son = bson.BSON.encode(data)
    decoded = son.decode()
    assert decoded == data

