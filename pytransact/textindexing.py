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

"""
Code common for textindexing.
"""

import re
split_re = re.compile(r'\W', re.U)

def getTerms(attrValue):
    terms = set()
    for v in attrValue:
        terms.update([_f for _f in split_re.split(v.lower()) if _f])
    return terms


def indexDataForToi(toi):
    terms = set()
    for attr in toi:
        if hasattr(attr, 'on_index'):
            on_index = getattr(attr, 'on_index')
            indexData = on_index(attr.value, toi)
            terms.update(getTerms(indexData))

    toid = toi.id
    if hasattr(toi, 'on_index'):
        on_index = getattr(toi, 'on_index')
        toid, terms = on_index(terms)
    return toid[0], sorted(terms)
