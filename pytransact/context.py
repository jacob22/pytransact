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

import collections, copy, functools
import pymongo, bson.objectid

from pytransact import contextbroker, query as Query
from pytransact.object.model import Attribute
from pytransact import mongo


def getTocByFullname(tocName):
    from . import blm
    parts = tocName.split('.')
    if parts[0] == 'blm':
        parts.pop(0)
    return getattr(getattr(blm, parts[0]), parts[1])


class ServiceQuery(Query.Query):

    def run(self):
        return contextbroker.ContextBroker().runQuery(self)

    def push(self, **conds):
        for attrName, cond in conds.items():
            if isinstance(cond, Attribute):
                conds[attrName] = cond.value
        super(ServiceQuery, self).push(**conds)

def isSuperUser(user):
    if not user:
        return True
    return user.super[0]


class ReadonlyContext(object):

    def __init__(self, database, user=None):
        self.database = database
        self.__instances__ = {}
        self.__cache__ = {}  # used by runtime.cache
        self._query_cache = {}
        self._preload = collections.defaultdict(set)
        self.setUser(user)

    def __enter__(self):
        contextbroker.ContextBroker().pushContext(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert contextbroker.ContextBroker().context == self
        contextbroker.ContextBroker().popContext()

    @classmethod
    def clone(cls, ctx=None, **kw):
        "Create a fresh context based on ctx"
        if ctx is None:
            from pytransact.contextbroker import ContextBroker
            ctx = ContextBroker().context
        return cls(kw.get('database', ctx.database), kw.get('user', ctx.user))

    def createQuery(self, toc, kw):
        return ServiceQuery(toc, **kw)

    def getToi(self, id):
        """
        Return an existing toi, or None

        Arguments: id - the toi ID
        Returns:   Toi
        """
        return self.__instances__.get(id)

    def addToi(self, toi):
        """
        Add a toi.

        Arguments: toi - the toi to add
        Returns:   None
        """
        toid = toi.id[0]
        if toid in self.__instances__:
            # Toi already added
            raise cRuntimeError(
                'Trying to create a toi with an already '
                'existing id: (%s %d)' % (
                    toi._xlatKey, toid))
        self.__instances__[toid] = toi

    def getUser(self):
        return self.user

    def setUser(self, user):
        if user is not None:
            if user not in self.__instances__:
                with self:
                    user = user.__class__._create(user)
        self.user = user

    def runQuery(self, query):
        if not isSuperUser(self.user):
            query = query.copy()
            privileges = self.user._privileges.value
            op = Query.In(privileges)
            if len(query):
                for cg in query:
                    cg[query.toc.allowRead] = [op]
            else:
                query.pushDict({query.toc.allowRead: [op]})
        mongo_query = query.mongo()
        key = Query.freeze(mongo_query)
        try:
            attrList, result = self._query_cache[key]
        except KeyError:
            attrList = set()
        else:
            if attrList >= set(query.attrList):
                return result

        results = []
        fields = set(query.attrList) | attrList
        for doc in mongo.run_query(self.database, mongo_query, projection=fields):
            toc = getTocByFullname(doc.pop('_toc'))
            id = doc.pop('_id')
            for attrName in fields:
                if attrName not in doc and attrName in toc._attributes:
                    doc[attrName] = copy.copy(toc._attributes[attrName].default)
            toi = toc._create(id, kw=doc)
            toi._phantom = False
            results.append(toi)

        self._query_cache[key] = fields, results
        return results

    def requestAttribute(self, toi, attr):
        _id = bson.objectid.ObjectId(toi.id[0])
        attrNames = self._preload.pop(_id, set())
        attrNames.add(attr.name)
        attrNames -= set(toi._attrData)
        try:
            dbob = next(mongo.run_query(self.database, {'_id': _id},
                                   projection=attrNames))
        except StopIteration:
            # New toi
            return []

        toi._phantom = False
        tocName = dbob['_toc']
        if toi._fullname != tocName:
            toi.__class__ = getTocByFullname(tocName)
        for attrName in attrNames:
            try:
                data = dbob[attrName]
            except KeyError:
                data = copy.copy(toi._attributes[attrName].default)
            toi._attrData[attrName] = data
        return toi._attrData[attr.name]

    def preloadAttributes(self, toi, attrNames):
        self._preload[toi.id[0]].update(attrNames)

    def clearTois(self):
        for toi in self.__instances__.values():
            toi._clear()

    def canRead(self, toi):
        if isSuperUser(self.user):
            return True
        return toi.canRead(self.user)

    def canWrite(self, toi, attrName):
        if isSuperUser(self.user):
            return True
        return toi.canWrite(self.user, attrName)

    def canDelete(self, toi):
        if isSuperUser(self.user):
            return True
        return toi.canDelete(self.user)

    def runAfterCommit(self, func, *args, **kw):
        raise NotImplementedError

    def newId(self):
        return bson.objectid.ObjectId()


def maybe_with_context(contextClass=ReadonlyContext):
    def wrapper(func):
        @functools.wraps(func)
        def _(*args, **kw):
            database = kw.pop('database', None)
            try:
                context = contextbroker.ContextBroker().context
                if not isinstance(context, contextClass):
                    return contextClass.clone(context)
                return func(*args, **kw)
            except LookupError:
                if database is None:
                    raise ValueError('Missing both database and context')
                with contextClass(database):
                    return func(*args, **kw)
        return _
    return wrapper
