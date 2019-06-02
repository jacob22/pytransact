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

import importlib, os, py, pymongo, sys, time
from pytransact import blmsupport, commit, context, contextbroker, mongo
from . import blm
from types import ModuleType

# db helpers

dbname = 'pytest'
dburi = os.environ.get('PYTRANSACT_DBURI')


def use_unique_database():
    global dbname
    dbname = 'pytest_%d' % os.getpid()

    connection = connect()
    assert dbname not in connection.database_names()

    def unconfigure():
        connection.drop_database(dbname)
        connection.close()

    return unconfigure


def connect():
    return mongo.connect(host=dburi)
    # connection = None

    # if dburi:
    #     uri_info = pymongo.uri_parser.parse_uri(dburi)
    #     if 'replicaset' in uri_info['options']:
    #         connection = mongo.connect(
    #             host=dburi,
    #             read_preference=pymongo.ReadPreference.SECONDARY)
    # if connection is None:
    #     connection = mongo.connect(host=dburi)
    # return connection


def clean_db(database, _dbname=None):
    assert database.name == _dbname or dbname
    for collection in database.collection_names():
        if not collection.startswith('system'):
            database.drop_collection(collection)
    sync(database)


def sync(database):
    if len(database.client.nodes) > 1:
        err = database.command('getLastError', w=len(database.client.nodes))
        assert not err['err'], err


# contexts

def _createToi(toc, toid, kw):
    toi = toc._create(toid)
    for attr in toc._attributes.values():
        if attr.default:
            kw.setdefault(attr.name, attr.default)
    if kw:
        toi._update(kw)
    else:
        toi._register()
    return toi


class FakeContext(context.ReadonlyContext):

    def __init__(self):
        super(FakeContext, self).__init__(None)

    def createToi(self, toc, toid, kw):
        return _createToi(toc, toid, kw)

    def requestAttribute(self, toi, attr):
        return []

    def register(self, toi):
        pass

    def deleteToi(self, toi):
        toi._deleted = True

    def changeToi(self, toi, kw):
        toi._update(kw)


class RuntimeContext(context.ReadonlyContext):

    def createToi(self, toc, toid, kw):
        return _createToi(toc, toid, kw)

    def changeToi(self, toi, attrData):
        # this is toi(foo=['bar'])
        toi._update(attrData)
        mongo.save_toi(self.database, toi.id[0], toi.__class__, toi._attrData)

    def deleteToi(self, toi):
        mongo.remove_tois(self.database, [toi.id[0]])

    def register(self, toi):
        mongo.save_toi(self.database, toi.id[0], toi.__class__, toi._attrData)

    def runCommit(self, operations, **kw):
        pass


# test baseclasses

class DBTests(object):

    def setup_method(self, method):
        self._connection = None
        self._database = None

    def teardown_method(self, method):
        if self._database:
            clean_db(self._database)
        if self._connection:
            self._connection.close()

    @property
    def connection(self):
        if not self._connection:
            self._connection = connect()
        return self._connection

    @property
    def database(self):
        if not self._database:
            self._database = self.connection[dbname]
        return self._database

    def sync(self):
        sync(self.database)


class ContextTests(DBTests):

    cb = contextbroker.ContextBroker()
    ContextClass = RuntimeContext

    def setup_method(self, method):
        super(ContextTests, self).setup_method(method)
        self.pushnewctx()

    def teardown_method(self, method):
        while True:
            try:
                self.cb.context
            except LookupError:
                break
            else:
                self.cb.popContext()
        super(ContextTests, self).teardown_method(method)

    def pushnewctx(self, ContextClass=None, oldctx=None, user=None):
        ContextClass = ContextClass or self.ContextClass

        if oldctx:
            self.ctx = ContextClass.clone(oldctx)
        else:
            self.ctx = ContextClass(self.database, user=user)
        try:

            self.ctx.setMayChange(True)
        except AttributeError:
            pass

        self.cb.pushContext(self.ctx)
        return self.ctx

    def fakeToiData(self, toid, attrData):
        toi, = blm.TO._query(id=toid).run()
        toc = toi.__class__
        mongo.save_toi(self.database, toid, toc, attrData)


class BLMTests(ContextTests):

    ContextClass = commit.CommitContext

    def new_context(self):
        self.pushnewctx(oldctx=self.cb.popContext())

    def commit(self):
        self.ctx.runCommit([], interested=None)
        self.ctx.clearTois()
        self.sync()
        self.new_context()


# misc

def _loadBLM(name, source):
    filename = "blm.{name}".format(name=name)

    mod = ModuleType(name)
    exec(compile(source, filename, 'exec'), vars(mod))
    setattr(blm, name, mod)
    blm.__blms__[name] = sys.modules[filename] = mod
    return mod

def loadBLM(name, source):
    mod = _loadBLM(name, source)
    blmsupport.setupBlm(mod)
    blmsupport.setupTocs(list(blm.__blms__.values()))

def loadBLMs(blms):
    mods = []
    for name, source in blms:
        #import pdb; pdb.set_trace()
        mods.append(_loadBLM(name, source))
    
    for mod in mods:
        blmsupport.setupBlm(mod)
    
    blmsupport.setupTocs(list(blm.__blms__.values()))


class CallCollector(object):

    def __init__(self):
        self.calls = []

    def __getattr__(self, attrName):
        def fn(*args, **kw):
            retVal = object()
            self.calls.append((attrName, args, kw, retVal))
            return retVal
        return fn


class Fake(object):

    def __init__(self, **kw):
        self.__dict__.update(kw)


class Time(object):

    def __init__(self, now=None):
        if now is None:
            now = int(time.time())
        self.now = now
        self._time = time.time
        self._strftime = time.strftime
        time.time = self.time
        time.strftime = self.strftime

    def time(self):
        return self.now

    __call__ = time  # old interface

    def strftime(self, format, t=None):
        if t is None:
            t = time.localtime(self.now)
        return self._strftime(format, t)

    def step(self):
        self.now += 1

    def restore(self):
        if time.time == self.time:
            time.time = self._time
        if time.strftime == self.strftime:
            time.strftime = self._strftime

    def __iadd__(self, v):
        self.now += v
        return self

    def __eq__(self, other):
        return self.now == other

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.restore()
