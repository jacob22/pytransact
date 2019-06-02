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

import collections
from itertools import islice


def chunks(iterable, chunksize):
    iterable = iter(iterable)
    chunk = True
    while chunk:
        chunk = list(islice(iterable, chunksize))
        if chunk:
            yield chunk


def progress(iterable, cb, interval=1):
    n = last = None
    for n, element in enumerate(iterable, 1):
        yield element
        if not n % interval:
            cb(n)
            last = n

    if n != last:
        cb(n)


def uniq(iterable):
    seen = set()
    for elem in iterable:
        if elem not in seen:
            yield elem
            seen.add(elem)


def walk(obj):
    if isinstance(obj, str):
        yield obj
        return
    if hasattr(obj, 'values'):
        obj = iter(obj.values())
    if isinstance(obj, collections.Iterable):
        for value in obj:
            for v in walk(value):
                yield v
        return
    yield obj

