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

import difflib
from future.moves.itertools import filterfalse


def apply_opcodes(old, opcodes):
    result = list(old)
    offset = 0
    for idx1, idx2, data in opcodes:
        result[idx1+offset: idx2+offset] = data
        offset += len(data) - (idx2 - idx1)
    return result


def diff_opcodes(old, new):
    matcher = difflib.SequenceMatcher(None, old, new)
    opcodes = matcher.get_opcodes()
    result = []
    for opcode in opcodes:
        if opcode[0] == 'equal':
            continue
        result.append((opcode[1], opcode[2],
                       new[opcode[3]:opcode[4]]))
    return result


def difference(orig, new):
    '''
    Calculate difference between two iterables.

    Will return two iterables, one with added elements and one with
    removed elements.

    Arguments: orig - original iterable
               new  - new iterable
    Returns:   added, removed
    '''
    try:
        added = set(new).difference(orig)
    except TypeError:
        added = filterfalse(orig.__contains__, new)

    try:
        removed = set(orig).difference(new)
    except TypeError:
        removed = filterfalse(new.__contains__, orig)

    return added, removed
