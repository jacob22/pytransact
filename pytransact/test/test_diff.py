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

from pytransact import diff


def test_difference():
    L = [1, 2, 3, 4, 5]
    M = [4, 5, 6, 7, 8]

    added, removed = diff.difference(L, M)
    assert set(added) == {6, 7, 8}
    assert set(removed) == {1, 2, 3}


    unhashables = [{}, [], set()]

    L = unhashables[:2] + [1]
    M = unhashables[1:] + [2]

    added, removed = diff.difference(L, M)
    assert list(added) == [unhashables[2], 2]
    assert list(removed) == [unhashables[0], 1]


def test_diff_opcodes():
    oldseq = []
    newseq = [1,32,5]
    opcodes = diff.diff_opcodes(oldseq,newseq)
    assert len(opcodes) == 1
    assert opcodes[0] == (0,0,newseq) # insert

    oldseq = [1,4,5]
    newseq = [1,5]
    opcodes = diff.diff_opcodes(oldseq,newseq)
    assert len(opcodes) == 1
    assert opcodes[0] == (1,2,[]) # delete

    oldseq = [1,4,5]
    newseq = [1,3,5]
    opcodes = diff.diff_opcodes(oldseq,newseq)
    assert opcodes[0] == (1,2,[3]) # replace


def test_apply_opcodes():
    oldseq = []
    newseq = [1,32,5]
    opcodes = diff.diff_opcodes(oldseq, newseq)
    result = diff.apply_opcodes(oldseq, opcodes)
    assert result == newseq

    oldseq = [1,4,5]
    newseq = [1,5]
    opcodes = diff.diff_opcodes(oldseq, newseq)
    result = diff.apply_opcodes(oldseq, opcodes)
    assert result == newseq

    oldseq = [1,4,5]
    newseq = [1,3,5]
    opcodes = diff.diff_opcodes(oldseq, newseq)
    result = diff.apply_opcodes(oldseq, opcodes)
    assert result == newseq

    oldseq = [1,2,6,2,7,278,568,2,54]
    newseq = [568, 278, 1, 54, 2, 7, 2, 2, 6]

    opcodes = diff.diff_opcodes(oldseq, newseq)
    result = diff.apply_opcodes(oldseq, opcodes)
    assert result == newseq

    oldseq = [1,2,6,2,7,278,568,2,54]
    newseq = [0,1,2,6,2,568,14,2,54,66]

    opcodes = diff.diff_opcodes(oldseq, newseq)
    result = diff.apply_opcodes(oldseq, opcodes)
    assert result == newseq
