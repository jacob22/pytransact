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

import py.test, sys, threading

from pytransact.contextbroker import ContextBroker
from pytransact.testsupport import CallCollector


class TestContextBroker(object):

    def setup_method(self, method):
        ContextBroker().contextDict.clear()

    def teardown_method(self, method):
        ContextBroker().contextDict.clear()

    def test_singleton(self):
        "Enshures that the ContextBroker is a singleton."
        assert ContextBroker() is ContextBroker()

    def test_startsEmpty(self):
        py.test.raises(LookupError, lambda: ContextBroker().context)

    def test_pushContext(self):
        cb = ContextBroker()
        ctx = object()
        cb.pushContext(ctx)
        assert cb.context is ctx

    def test_popContext_returnValOk(self):
        cb = ContextBroker()
        ctx = object()
        cb.pushContext(ctx)

        assert cb.context is ctx
        assert cb.popContext() is ctx

    def test_effectOfPopContextOk(self):
        cb = ContextBroker()
        py.test.raises(IndexError, cb.popContext)
        ctx1 = object()
        ctx2 = object()
        cb.pushContext(ctx1)
        cb.pushContext(ctx2)

        assert cb.context is ctx2
        assert cb.popContext() is ctx2
        assert cb.context is ctx1

    def test_popEmpty(self):
        cb = ContextBroker()
        py.test.raises(IndexError, lambda: cb.popContext())

    def test_otherThread_startsEmpty(self):
        cb = ContextBroker()
        ctx = object()
        cb.pushContext(ctx)

        def getContext():
            return cb.context

        py.test.raises(LookupError, runInThread, 1.0, getContext)

    def test_otherThread_pushContext(self):
        cb = ContextBroker()
        ctx = object()

        def check_pushContext():
            cb.pushContext(ctx)
            assert cb.context is ctx
            assert cb.popContext() is ctx

        runInThread(1.0, check_pushContext)

    def test_otherThread_effectOfPopContextOk(self):
        runInThread(1.0, self.test_effectOfPopContextOk)

    def test_forwardCalls(self):
        cb = ContextBroker()

        fwdCalls = {
            'getToi': 1, 'addToi': 1, 'createToi': 3, 'changeToi': 2,
            'deleteToi': 1, 'register': 1, 'runQuery': 1, 'requestAttribute': 2,
            'callMethod': 1 , 'newId': 0,
        }

        callCollector = CallCollector()
        cb.pushContext(callCollector)

        expected = []
        for call, argN in fwdCalls.items():
            args = tuple(object() for i in range(argN))
            retVal = getattr(cb, call)(*args)
            expected.append((call, args, {}, retVal))

        assert callCollector.calls == expected


def runInThread(timeout, function, *args, **kw):

    thread = RunInThread(function, *args, **kw)
    thread.start()
    thread.join(timeout)
    if thread.isAlive():
        raise TimeoutError("function %s timed out." % function)
    if thread.exc[0] is not None:
        raise thread.exc[0](thread.exc[1]).with_traceback(thread.exc[2])
    else:
        return thread.result

class TimeoutError(Exception):
    pass


class RunInThread(threading.Thread):

    def __init__(self, fun, *args, **kw):
        super(RunInThread, self).__init__()
        self.fun = fun
        self.args = args
        self.kw = kw
        self.result = None
        self.exc = None, None, None

    def run(self):
        try:
            self.result = self.fun(*self.args, **self.kw)
        except:
            self.exc = sys.exc_info()
