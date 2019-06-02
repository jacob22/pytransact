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

from pytransact import iterate


def test_chunks():
    result = iterate.chunks(range(10), 2)
    assert list(result) == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]

    result = iterate.chunks(range(10), 3)
    assert list(result) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    result = iterate.chunks([], 27)
    assert list(result) == []

    numbers = list(range(10))
    def iterator():
        while numbers:
            yield numbers.pop(0)

    chunks = iterate.chunks(iterator(), 2)
    assert next(chunks) == [0, 1]
    assert numbers == list(range(2, 10))


def test_progress():
    reported = []
    for x in iterate.progress(range(10), reported.append, 3):
        pass

    assert reported == [3, 6, 9, 10]


def test_uniq():
    L = [1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 7, 6, 7, 8, 1, 9, 9]
    r = list(iterate.uniq(L))
    assert r == [1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_walk():
    obj = {'key1': [({'bar': 'baz'}, {'apa'}, 'bepa'), 'cepa'],
           'key2': 'foo'}

    expected = {'baz', 'apa', 'bepa', 'cepa', 'foo'}
    result = set(iterate.walk(obj))
    #import pdb; pdb.set_trace()
    assert result == expected
