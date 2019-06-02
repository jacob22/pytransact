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
Runtime information for BLM methods.
"""
import contextlib
import pytransact.object.attribute
from pytransact.contextbroker import ContextBroker


def addIndexData(indexData):
    return ContextBroker().context.addIndexData(indexData)


def cleanValue(value):
    return pytransact.object.attribute.cleanValue(value)


def getClientUser():
    return ContextBroker().context.getUser()


def setUser(user):
    return ContextBroker().context.setUser(user)


class cache:

    def __getattr__(self, attr):
        return getattr(ContextBroker().context.__cache__, attr)

    def __getitem__(self, item):
        return ContextBroker().context.__cache__.__getitem__(item)

    def __setitem__(self, item, value):
        return ContextBroker().context.__cache__.__setitem__(item, value)

    @contextlib.contextmanager
    def set(self, d={}, **kw):
        d = dict(d)
        d.update(kw)
        self.update(d)
        yield self
        for k in d:
            self.pop(k, None)


cache = cache()


@contextlib.contextmanager
def setuid(user=None):
    context = ContextBroker().context
    current = context.user
    context.setUser(user)
    try:
        yield
    finally:
        context.setUser(current)
