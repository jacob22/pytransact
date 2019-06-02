from __future__ import print_function

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
This module contains the logic for processing commits.

Intended usage for the MongoDB version:
- create a CommitContext()
- push it on the context stack
- do stuff
- pop it off the context stack
- call commit on the context to commit changes to the db.
  + May raise an execption if data has changed in the DB,
    if so, the commit will fail, and you need to deal with it.
"""

import logging
from functools import reduce
log = logging.getLogger('pytransact.commit')

from bson.objectid import ObjectId
from pymongo.errors import BulkWriteError, OperationFailure
import pymongo, gridfs, bson, bson.errors
import pymongo.operations
import collections
import itertools
import sys
import copy
import time
import traceback

from pytransact import context as Context
from pytransact import mongo

from pytransact.difftoi import DiffTOI

from pytransact import custombson, diff, iterate
from pytransact import textindexing, query, queryops
import pytransact.object.property
import pytransact.object.restriction
import pytransact.object.attribute as Attribute
import pytransact.object.to as TO
from pytransact.object.attribute import cleanValue

DEBUG_CP = False
DEBUG_CPCT = False

from pytransact.exceptions import *

from pytransact import contextbroker

from pytransact import blm

CB = contextbroker.ContextBroker()

MaxRecursionDepth = 3
NoRestrictions = False

CREATE_ACTION = 'on_create'               # Call on create
UPDATE_ACTION = 'on_update'               # Call on update
DELETE_ACTION = 'on_delete'               # Call on delete
INDEX_ACTION = 'on_index'                 # Call on index
COMPUTED_ACTION = 'on_computation'        # No call, used with computed actions


class Timeout(RuntimeError):
    pass


class CommitConflict(RuntimeError):
    pass

class ToisLocked(RuntimeError):
    pass

class Commit(object):
    @classmethod
    def wait_for_commit(cls, database, interested, onfail=None, timeout=120):
        end = time.time() + timeout
        for wait in itertools.count(0, 0.1):
            doc = mongo.find_one(database.commits,
                                 {'interested': interested, 'state': 'done'})
            if doc:
                commit = cls.fromdoc(database, doc)
                commit.delete(database)
                return commit.results, commit.error
            now = time.time()
            if now > end:
                raise onfail or Timeout()
            time.sleep(wait if now + wait < end else end - now)

    @staticmethod
    def handlers_running(database):
        assert database.read_preference == pymongo.ReadPreference.PRIMARY
        return mongo.find_one(
            database.commits, {
                'state': { '$nin': ['done', 'failed'] },
                'handled_by': {'$exists': True}},
            projection=['_id'])

    @staticmethod
    def unhandle_handled(database, id, handler):
        "unhandle commit handled by this handler"
        mongo.update_one(database.commits,
                         {'_id': id, 'handled_by': handler},
                         {'$unset': {'handled_by': True}})

    @classmethod
    def handle(cls, database, handler):
        "find and handle commit stored in db"
        doc = mongo.find_and_modify(database.commits,
                                    {'handled_by': None,
                                     'state': { '$nin': ['done', 'failed'] }},
                                    {'$set': {'handled_by': handler}})
        if not doc:
            return None

        doc['handled_by'] = handler

        return cls.fromdoc(database, doc)

    @staticmethod
    def count(database):
        return mongo.find(database.commits).count()

    @classmethod
    def fromdoc(cls, database, doc):
        griddata = doc.pop('_griddata', None)
        if griddata:
            alldoc = bson.BSON(griddata.getvalue()).decode()
            alldoc.update(doc)
        else:
            alldoc = doc
        new = cls(**alldoc)
        new._griddata = griddata
        return new

    @classmethod
    def fromquery(cls, database, query):
        doc = mongo.find_one(database.commits, query)
        if not doc:
            return None
        return cls.fromdoc(database, doc)

    def __init__(self, _id=None, **kw):
        if _id is None:
            _id = ObjectId()
        self._id = _id
        self.user = kw.get('user')
        self.interested = kw.get('interested')
        self.handled_by = kw.get('handled_by')
        self.error = kw.get('error')
        self.traceback = kw.get('traceback')
        self.state = kw.get('state', 'new')
        self.generation = kw.get('generation', 0)

        # These go in a GridFS-file on save
        self.operations = kw.get('operations', [])
        self.newTois = kw.get('newTois', [])
        self.changedTois = kw.get('changedTois', [])
        self.deletedTois = kw.get('deletedTois', [])
        self.addedBlobVals = kw.get('addedBlobVals', {})
        self.deletedBlobVals = kw.get('deletedBlobVals', {})
        self.indexData = kw.get('indexData', [])
        self.results = kw.get('results', [])

        self._griddata = None

    def get_doc(self):
        doc = {
            '_id': self._id,
            'user': self.user,
            'interested': self.interested,
            'error': self.error,
            'traceback': self.traceback,
            'state': self.state,
            'generation': self.generation
            }
        if self.handled_by:
            # XXX Must not exist if it is not handled
            doc['handled_by'] = self.handled_by


        griddata = {
            'operations': self.operations,
            'newTois': self.newTois,
            'changedTois': self.changedTois,
            'deletedTois': self.deletedTois,
            'addedBlobVals': self.addedBlobVals,
            'deletedBlobVals': self.deletedBlobVals,
            'indexData': self.indexData,
            'results': self.results,
            }

        if self._griddata:
            self._griddata.delref(self._id)
            self._griddata = None

        if [_f for _f in list(griddata.values()) if _f]:
            # Not all empty
            griddata = Attribute.BlobVal(bson.BSON.encode(griddata))
            griddata.addref(self._id)
            self._griddata = doc['_griddata'] = griddata

        return doc

    def save(self, database):
        mongo.replace_one(database.commits, {'_id': self._id},
                          self.get_doc(), upsert=True)

    def delete(self, database):
        log.debug('Deleting commit %s', self._id)
        for op in self.operations:
            for blobVal in op.blobVals():
                blobVal.delref(self._id)
        for blobVals in (list(self.addedBlobVals.values()) +
                         list(self.deletedBlobVals.values())):
            for blobVal in blobVals:
                blobVal.delref(self._id)
        for value in iterate.walk(self.results):
            if isinstance(value, Attribute.BlobVal):
                value.delref(self._id)
        if self._griddata:
            self._griddata.delref(self._id)

        mongo.remove(database.commits, {'_id': self._id})

    def unhandle(self, database, handler):
        "unhandle this commit, for later processing"
        self.handled_by = None
        mongo.replace_one(database.commits,
                          {'_id': self._id, 'handled_by': handler},
                          self.get_doc(), upsert=True)

    def done(self, database):
        if not self.interested:
            self.delete(database)
            return

        toAddRef = set()
        cid = self._id
        for value in iterate.walk(self.results):
            if isinstance(value, Attribute.BlobVal):
                toAddRef.add(value)

        for op in self.operations:
            for blobVal in op.blobVals():
                if blobVal not in toAddRef:
                    blobVal.delref(cid)

        for blobVals in (list(self.addedBlobVals.values()) +
                         list(self.deletedBlobVals.values())):
            for blobVal in blobVals:
                if blobVal not in toAddRef:
                    blobVal.delref(cid)

        for blobVal in toAddRef:
            blobVal.addref(cid)

        mongo.replace_one(database.commits,
                          {'_id': cid},
                          {
                              '_id': cid,
                              'handled_by': self.interested,
                              'interested': self.interested,
                              'state': 'done',
                              'results': self.results,
                              'error': self.error
                          },
                          upsert=True)


# Backwards compatibility
wait_for_commit = Commit.wait_for_commit

class CommitContext(Context.ReadonlyContext):
    """
    A read/write runtime context for commits.
    """

    def __init__(self, database, user=None):
        """
        Initialise the object.

        Arguments: session - the client session object running the operation
        Returns:   None
        """
        super(CommitContext, self).__init__(database, user)
        self.id = ObjectId()
        self._clear()

    def _clear(self):
        self.deletedTois = {}
        self.newTois = {}
        self.changedTois = {}
        self.addedBlobVals = {}  # { toid: [blobval, ... ] }
        self.deletedBlobVals = {}  # { toid: [blobval, ... ] }
        self._mayChange = False
        self.processUsedIds = set()
        self._commitHooks = []
        self.indexData = []

    def canWrite(self, toi, attrName):
        return (toi.id[0] in self.newTois or
                super(CommitContext, self).canWrite(toi, attrName))

    def createToi(self, toc, toid, kw):
        """
        Create a toi (and run the on create actions)

        Arguments: toc - the TO
                   toid - the id number
                   kw - attribute keyword data
        Returns:   The new toi
        """
        if not self._mayChange:
            raise cRuntimeError('Tried to change Toi in a read only context.')

        if toid in self.newTois:
            # Attempted to create the same toi twice.
            raise cRuntimeError('Tried to recreate the same Toi twice.')

        if toid in self.deletedTois:
            # Attempted to recreate deleted toi
            raise cRuntimeError('Tried to recreate a deleted Toi.')

        op = CreateToi(toc._fullname, toid, kw)
        oldChange = self._mayChange
        #import pdb;pdb.set_trace()
        toi = op.operate(self)
        self.setMayChange(oldChange)

        return toi

    def changeToi(self, toi, kw):
        """
        Change a toi (and run the on update actions)

        Arguments: toi - the toi to create
                   kw - Attribute name value pairs
        Returns:   None
        """
        if not self._mayChange:
            raise cRuntimeError('Tried to change Toi in a read only context.')

        op = ChangeToi(toi, kw)
        oldChange = self._mayChange
        op.operate(self)
        self.setMayChange(oldChange)

    def deleteToi(self, toi):
        """
        Delete a toi (and run the on delete actions)

        Arguments: toi - the toi to delete
        Returns:   None
        """
        if not self._mayChange:
            raise cRuntimeError('Tried to change Toi in a read only context.')

        op = DeleteToi(toi)
        oldChange = self._mayChange
        op.operate(self)
        self.setMayChange(oldChange)

    def register(self, toi):
        """
        Mark a toi as changed. Doesn't run any actions.

        Arguments: toi - the changed toi
        Returns:   None
        """
        if not self._mayChange:
            raise cRuntimeError('Tried to change Toi in a read only context.')
        toid = toi.id[0]

        if toi._deleted:
            if toid in self.deletedTois:
                return
            if toid in self.newTois:
                del self.newTois[toid]
            else:
                if toid in self.changedTois:
                    del self.changedTois[toid]
                self.deletedTois[toid] = toi
        elif toid not in self.newTois and toid not in self.changedTois:
            self.changedTois[toid] = toi

    def setMayChange(self, mayChange):
        """
        Set the may change flag.

        Arguments: mayChange - the flag value
        Returns:   None
        """
        self._mayChange = mayChange

    def runQuery(self, query):
        """
        Create and return a query result.

        Arguments: query - the search query
        Returns:   Query results
        """
        return self._runComplexQuery(copy.deepcopy(query))

    def _runComplexQuery(self, q):
        """
        Create and return a query result for a query that may
        contain subqueries.

        Arguments: query - the search query
        Returns:   Query results
        """
        for cg in q:
            for condList in cg.values():
                for i in range(len(condList)):
                    cond = condList[i]
                    if isinstance(cond, query.Query):
                        # Replace subquery with result
                        value = self._runComplexQuery(cond)
                        cond = queryops.In(value)
                        condList[i] = cond

        return self._runSimpleQuery(q)

    def _runSimpleQuery(self, query):
        """
        Create and return a query result for a query without subqueries

        Arguments: query - the search query
        Returns:   Query results
        """

        class ToiProxy(object):
            def __init__(self, toi):
                self.id = toi.id
            def __eq__(self, other):
                try:
                    otherid = other.id
                except AttributeError:
                    try:
                        otherid = ObjectId(other),
                    except TypeError:
                        return NotImplemented
                return self.id == otherid
            def __hash__(self):
                return hash(self.id[0])

        def attrGetter(toi, attr):
            if attr == 'id':
                # XXX This is so query(id=toi) and
                # XXX query(id=ObjectId(toid)) works
                # XXX as toi != toid
                # XXX however query(toiref=toid) doesn't work
                return [ToiProxy(toi)]
            return toi[attr.name].value

        result = super(CommitContext, self).runQuery(query)

        # Remove deleted TOs from result
        result = [ ob for ob in result if not ob._deleted ]

        # SubTOI check in new objects
        for toi in self.newTois.values():
            if isinstance(toi, query.toc):
                if query.matches(toi, attrGetter):
                    result.append(toi)

        # SubTOI check in changed objects
        mAttrs = set()
        for cg in query:
            for attr in cg.keys():
                if attr != 'id':
                    mAttrs.add(attr.name)
                else:
                    mAttrs.add(attr)

        for toi in self.changedTois.values():
            if isinstance(toi, query.toc):
                if not mAttrs.intersection(toi._modified):
                    continue
                match = query.matches(toi, attrGetter)
                if match and toi not in result:
                    result.append(toi)
                elif not match and toi in result:
                    result.remove(toi)

        return result

    def requestAttribute(self, toi, attr, filter=True):
        """
        Perform an attribute request.

        Arguments: toi - the requesting object
                   attr - the requested attribute
        Returns:   The requested value
        """
        # New object - invent something to return
        if toi.id[0] in self.newTois:
            return getattr(toi, attr.name).value
        if toi._deleted:
            raise cRuntimeError('Tried to access deleted Toi.')

        rval = super(CommitContext, self).requestAttribute(toi, attr)
        # Eliminate weak referenced deleted tois
        if filter and attr.getprop(pytransact.object.property.Weak):
            rval = [ val for val in rval if not val._deleted ]

        return rval

    def preloadAttributes(self, toi, attrNames):
        """
        Perform an *asynchronous* attribute request.

        Arguments: toi -  the requesting object
                   attrNames - the requested attribute names
        Returns:   None
        """
        if toi.id[0] in self.newTois:
            return
        return super(CommitContext, self).preloadAttributes(toi, attrNames)

    def validateAttrValues(self, toi, attr, value=None, pre=True):
        """
        Validate an attribute value.

        Arguments: toi   - the TOI to receive the new value, or None if new
                   attr  - the attribute to validate against
                   value - the value to validate, or None for the value in the
                           attribute object
                   pre   - pre validate on true, otherwise post
        Returns:   value
        Raises:    ValueError on invalid value
        """
        if NoRestrictions:
            if not value:
                value = attr.value
            return value

        post = not(pre)
        aName = attr.name
        if toi is not None:
            toid = toi.id[0]
        else:
            toid = None

        if attr.computed:
            raise cAttrPermError(attr._xlatKey, attr._toc._xlatKey, toid,
                                 'Cannot change computed attribute.')

        if pre and attr.getprop(pytransact.object.property.ReadOnly):
            raise cAttrPermError(attr._xlatKey, attr._toc._xlatKey, toid,
                                 'Access denied, read only attribute.')

        if attr.getprop(pytransact.object.property.Unchangeable, pre=pre, post=post):
            if toid and toid not in self.newTois and aName in toi._modified:
                raise cAttrPermError(attr._xlatKey, attr._toc._xlatKey, toid,
                                     'Access denied, unchangeable attribute.')

        if value is None:
            value = attr.value

        if attr.getprop(pytransact.object.property.Weak):
            if value and type(value[0]) in (list, tuple):
                value = [(key,val) for key,val in value if not val._deleted]
            else:
                value = [val for val in value if not val._deleted]

        # Validate attribute restrictions
        try:
            attr.validateValues(pre, value)
        except AttrValueError as err:
            raise ClientError(err)

        # Validate ReorderOnly property
        if (toid and toid not in self.newTois and
            attr.getprop(pytransact.object.property.ReorderOnly, pre=pre, post=post)):
            newVal = set(value)
            oldVal = set(attr.oldvalue)
            if oldVal != newVal:
                raise cAttrPermError(attr._xlatKey, attr._toc._xlatKey, toid,
                                     'Access denied, reorder only attribute')

        # Test unique
        if attr.getprop(pytransact.object.property.Unique, pre=pre, post=post) and value:
            if toid is None:
                toc = attr._toc
            else:
                toc = toi.__class__
            while toc != TO.TO:
                if aName not in toc.__bases__[0]._attributes:
                    break
                toc = toc.__bases__[0]
            q = toc._query(**{aName: value})
            # check two contexts in order to ensure that attrval is
            # unique both for:
            # - existing tois in db we may not have read access to
            # - newly created tois in this context
            for ctx in self, Context.ReadonlyContext(self.database):
                with ctx:
                    data = ctx.runQuery(q)
                if data and (len(data) > 1 or data[0].id[0] != toid):
                    raise cAttrValueError(attr._xlatKey, attr._toc._xlatKey, toid,
                                          UniqueError(value))

        if (not isinstance(attr, Attribute.ToiRef) and
            not isinstance(attr, Attribute.ToiRefMap)):
            return value

        # Test toi type
        rest = attr.getrest(Attribute.ToiType, pre=pre, post=post)
        qual = {}
        if rest:
            restToc = rest.validToiType
            qual = rest.expandQual()
        elif (isinstance(attr, Attribute.Relation) and
              attr.related is not None):
            restToc = attr.related._toc
        else:
            restToc = TO.TO

        # Uniqify the value list
        if isinstance(attr, Attribute.ToiRefMap):
            try:
                try:
                    testValue = set([_f for _f in list(value.values()) if _f])
                except AttributeError:
                    testValue = set(val for key,val in value if val is not None)
            except Exception:
                raise cAttrValueError(attr._xlatKey, attr._toc._xlatKey, toid,
                                      ToiRefMapValueError('<broken object>'))
        else:
            testValue = set(value)

        # Check for existance as specified
        for val in testValue:

            if not isinstance(val, restToc):
                raise cAttrValueError(
                    attr._xlatKey, attr._toc._xlatKey, toid,
                    ToiTypeError(val._fullname,
                                 restToc._xlatKey))

            if val._deleted:
                raise cAttrValueError(
                    attr._xlatKey, attr._toc._xlatKey, toid,
                    ToiDeletedError(val._fullname,
                                    val.id[0]))

            if not pre and val._phantom:
                # See if toi exists in database. Fetch all referenced
                # tois in attribute to avoid multiple roundtrips.
                restToc._query(id=testValue).run()
                if val._phantom:
                    raise cAttrValueError(
                        attr._xlatKey, attr._toc._xlatKey, toid,
                        ToiNonexistantError(val._fullname,
                                            val.id[0]))


        # Run toiType qualifications, if any
        # Simple qualifications can be run directly, while complex ones need
        # IFC treatment.
        # NB: We don't really have any complex qualifications any
        # more, this code could probably be simplified. /micke 2013-02-12
        if len(testValue) and qual:
            q = restToc._query(**qual)
            cg = q[0]

            complexQuery = False
            for a, cond in cg:
                if isinstance(cond, query.Query):
                    complexQuery = True
                    break
            if not complexQuery:
                def attrGetter(toi, a):
                    if a == 'id':
                        return toi.id
                    return toi[a.name].value

                for v in testValue:
                    v._preload(list(cg.keys()))
                    if not q.matches(v, attrGetter):
                        raise cAttrValueError(
                            attr._xlatKey, attr._toc._xlatKey, toid,
                            QualificationError())
            else:
                cg.setdefault('id', []).append(queryops.In(testValue))
                data = self.runQuery(q)
                if len(data) != len(testValue):
                    raise cAttrValueError(
                        attr._xlatKey, attr._toc._xlatKey, toid,
                        QualificationError())

        return value

    def findRelatedAttr(self, toi, val, attr):
        assert attr._toc is type(toi)
        # Find the unbound attr
        attr = attr._toc._attributes[attr.name]
        for relattr in val._attributes.values():
            if (isinstance(relattr, Attribute.Relation) and
                relattr.related is not None):
                if (relattr.related.name == attr.name and
                    isinstance(toi, relattr.related._toc)):
                    # bound relattr
                    return getattr(val, relattr.name)
        else:
            raise cAttrValueError(attr._xlatKey, toi._xlatKey, toi.id[0],
                                  RelationError(val.id[0]))

    def updateRelations(self, toi, attr):
        """
        Update a relation attribute, modifying the other end of the relation
        to make sure a link exists in both places.

        Arguments: toi   - the TOI to be checked.
                   attr  - the attribute to validate
        Returns:   None
        """
        if not isinstance(attr, Attribute.Relation):
            return

        try:
            value = attr.value
        except ToiDeletedError:
            value = []
        toid = toi.id[0]

        log.debug('Processing %s %s %s', toi, attr.name, list(toi._orgAttrData.keys()))

        if toid in self.newTois:
            added = value
            removed = []
        else:
            log.debug('  DATA %s %s', attr.oldvalue, value)
            added, removed = list(map(list, diff.difference(attr.oldvalue, value)))

        log.debug('  Added, removed %s %s', added, removed)

        if attr.related is not None:

            log.debug('  Related is known: %s', attr.related.name)

            relAttrName = attr.related.name

            for val in added:
                log.debug('  Adding %s to %s', val.id, attr.name)
                relAttr = val[relAttrName]
                if toi not in relAttr.value:
                    attrVal = relAttr[:]
                    attrVal.append(toi)
                    setattr(val, relAttrName, self.validateAttrValues(
                            val, relAttr, value=attrVal, pre=False))

            for val in removed:
                if val._deleted:
                    # Ok, since the other side goes away no matter what.
                    log.debug('    Object %s deleted, proceeding.', val.id)
                    continue
                log.debug('    Removing %s from %s', val.id, attr.name)
                relAttr = val[relAttrName]
                if toi in self.requestAttribute(relAttr.toi, relAttr, filter=False):
                    attrVal = relAttr[:]
                    attrVal.remove(toi)
                    setattr(val, relAttrName, self.validateAttrValues(
                            val, relAttr, value=attrVal, pre=False))
        else:
            log.debug('  Related is unknown')
            for val in added:
                log.debug('    Adding %s to %s', val.id, attr.name)
                relAttr = self.findRelatedAttr(toi, val, attr)
                if toi not in relAttr.value:
                    attrVal = relAttr[:]
                    attrVal.append(toi)
                    setattr(val, relAttr.name, self.validateAttrValues(
                            val, relAttr, value=attrVal, pre=False))

            for val in removed:
                log.debug('    Removing %s from %s', val.id, attr.name)
                if val._deleted:
                    # Ok, since the other side goes away no matter what.
                    continue
                relAttr = self.findRelatedAttr(toi, val, attr)
                if toi in self.requestAttribute(relAttr.toi, relAttr, filter=False):
                    attrVal = relAttr[:]
                    attrVal.remove(toi)
                    setattr(val, relAttr.name, self.validateAttrValues(
                            val, relAttr, value=attrVal, pre=False))

    def updateBlobs(self, toi, attr):
        if not isinstance(attr, Attribute.Blob):
            return

        try:
            value = attr.value
        except ToiDeletedError:
            value = []
        toid = toi.id[0]

        if toid in self.newTois:
            added = value
            removed = []
        else:
            added, removed = list(map(list, diff.difference(attr.oldvalue, value)))

        for val in added:
            self.addedBlobVals.setdefault(str(toid), []).append(val)

        for val in removed:
            self.deletedBlobVals.setdefault(str(toid), []).append(val)


    def runAfterCommit(self, func, *args, **kw):
        self._commitHooks.append((func, args, kw))

    def runCommit(self, operations, interested=None, processCommits=True,
                  generation=0):
        log.debug('Running commit %s', operations)
        results = []
        try:
            for op in operations:
                op.checkPermissions(self)
                val = op.operate(self)
                results.append(cleanValue(val))
        except ClientError as error:
            #import pdb;pdb.set_trace()
            error = error.args[0]
            commit = self.createCommit(operations, [], error=error,
                                      interested=interested)
        else:
            self.setMayChange(True)

            log.debug('Updating relations')

            # Handle relation updates for old objects
            for toi in list(self.changedTois.values()):
                for attrName in list(toi._orgAttrData.keys()):
                    self.updateRelations(toi, toi[attrName])
                    self.updateBlobs(toi, toi[attrName])

            # Handle relation updates for new objects
            for toi in list(self.newTois.values()):
                for attrName in list(toi._attrData.keys()):
                    self.updateRelations(toi, toi[attrName])
                    self.updateBlobs(toi, toi[attrName])

            for toi in list(self.deletedTois.values()):
                for attrName in list(toi._attributes.keys()):
                    self.updateRelations(toi, toi[attrName])
                    self.updateBlobs(toi, toi[attrName])

            log.debug('Updating text index')
            # Create text index data
            self.updateTextIndex()

            log.debug('Updating done')

            self.setMayChange(False)

            commit = self.createCommit(operations, results,
                                       interested=interested,
                                       generation=generation)

        if processCommits:
            self.processCommits(commit)
        return commit

    def rerunCommit(self, commit):
        if commit.generation > 5:
            raise Timeout('Generation count too large.')
        generation = commit.generation + 1
        ops = commit.operations
        user = commit.user
        interested = commit.interested
        commit.delete(self.database)
        with CommitContext(self.database, user) as ctx:
            return ctx.runCommit(ops, interested=interested, processCommits=False,
                                 generation=generation)

    def set_read_preference(self, read_preference):
        if self.database.read_preference != read_preference:
            self.database = self.database.client.get_database(
                self.database.name, read_preference=read_preference)

    def processCommits(self, commit):
        successful = []
        lockedCommits = collections.defaultdict(int)
        with Context.ReadonlyContext(self.database):
            self.set_read_preference(pymongo.ReadPreference.PRIMARY)
            while commit:
                try:
                    commitId = commit._id
                    log.debug('Processing commit %s', commitId)
                    self.commit(commit)
                except ToisLocked as exc:
                    commit.unhandle(self.database, self.id)
                    lockedCommits[commitId] += 1
                    if lockedCommits[commitId] > 3:
                        raise Timeout('Failed to lock TOIs for commit too many '
                                      'times: %s', commitId)
                    if commit.handlers_running(self.database):
                        break
                except CommitConflict as exc:
                    log.info('CommitConflict ID: %s, TOI: %s, DIFF: %s',
                             commitId, *exc.args)
                    # replacing commit doc with new one - must have new id
                    # finally clause will unlock it otherwise
                    commit = self.rerunCommit(commit)
                    continue
                except Exception as exc:
                    log.error('Failed to process commit', exc_info=True)
                    commit.state = 'failed'
                    commit.traceback = traceback.format_exc()
                    commit.error = BlError('commit failure')
                    commit.save(self.database)
                    # XXX We should roll back changes made, or we might
                    # leave the database in an inconsistent state.
                else:
                    successful.append(commit)
                finally:
                    commit.unhandle_handled(self.database, commitId, self.id)

                commit = commit.handle(self.database, self.id)

            self.notifyChanges(successful)

    def createCommit(self, operations, results, error=None, interested=None,
                     generation=0):
        """
        Prepare information about a commit for storing in the database.

        `operations` indicate the commit operations used in the
        commit, `results` the result of the commit and the optional
        argument `interested` is a free form identifier that can be
        used to identify which code (e.g. client and link) requested
        the commit.
        """
        log.debug('Creating commit %s, %s, %s', operations, results, error)

        commitId = ObjectId()
        for op in operations:
            for blobVal in op.blobVals():
                blobVal.addref(commitId)
        for blobVals in list(self.addedBlobVals.values()) + list(self.deletedBlobVals.values()):
            for blobVal in blobVals:
                blobVal.addref(commitId)

        commit = Commit(commitId, handled_by=self.id, operations=operations,
                        user=self.user, interested=interested,
                        generation=generation)

        if error:
            commit.error = error
        else:
            commit.deletedTois = list(self.deletedTois.keys())
            commit.addedBlobVals = self.addedBlobVals
            commit.deletedBlobVals = self.deletedBlobVals
            commit.indexData = self.indexData
            commit.results = results
            for toid, toi in list(self.changedTois.items()):
                diff = DiffTOI()
                diff.setToi(toi)
                commit.changedTois.append(diff)
            for toid, toi in list(self.newTois.items()):
                diff = DiffTOI()
                diff.setToi(toi)
                commit.newTois.append(diff)

        return commit

        # Make sure we can BSON-encode the commit
        # This is to reduce the chance of generating broken
        # data due to attempting to save broken (generally
        # too large) bson to a toi.
        # XXX This is expensive, see if we can estimate size
        # and only call BSON.encode in edge cases.
        # fido spends 5 seconds here initializing a full chart of accounts
        max_bson_size = 16777216 # conn.max_bson_size is unreliable, as it's 0 when primary is unknown
        if len(bson.BSON.encode(doc)) > max_bson_size:
            raise bson.InvalidDocument('Commit document too large')
        return doc

    def updateTextIndex(self):
        indexDataByOwner = {}
        tois = set(self.newTois.values()) | set(self.changedTois.values())
        for toi in tois:
            # ownerToid not necessarily toi.id, a toi may produce
            # index data for e.g. parent
            ownerToid, terms = textindexing.indexDataForToi(toi)
            document = {'toid': toi.id[0], 'data': terms}
            indexDataByOwner.setdefault(ownerToid, []).append(document)
        self.indexData = list(indexDataByOwner.items())

    def saveIndexData(self, indexData):
        return []
        to_dict = lambda _docs: dict(('_terms.%s' % d['toid'], d) for d in _docs if d['data'])
        dbops = []
        for ownerToid, documents in indexData:
            newByToid = {}
            for doc in documents:
                toid = doc['toid']
                if toid in self.newTois and not doc['data']:
                    continue
                newByToid['_terms.%s' % toid] = doc
            #newByToid = to_dict(documents)

            #old = mongo.find_one(self.database.tois, {'_id': ownerToid},
            #                     projection=['_terms']).get('_terms', [])
            #oldByToid = to_dict(old)
            #oldByToid.update(newByToid)
            #indexData = oldByToid.values()
            #mongo.update(self.database.tois, {'_id': ownerToid},
            #          {'$set': {'_terms': indexData}})
            if newByToid:
                dbops.append(pymongo.operations.UpdateOne(
                    {'_id': ownerToid}, {'$set': newByToid}))
        return dbops

    def _lockTois(self, toids):
        assert self.database.read_preference == pymongo.ReadPreference.PRIMARY
        status = mongo.update_many(
            self.database.tois,
            {'_id': {'$in': list(toids)}, '_handled_by': None},
            {'$set': {'_handled_by': self.id}})

        affected = status.modified_count
        if affected is None:
            affected = status.matched_count

        if affected == len(toids):
            affected = mongo.find(
                self.database.tois,
                {'_id': {'$in': list(toids)}, '_handled_by': self.id}).count()

        if affected != len(toids):
            # Locked or deleted?
            if mongo.find(self.database.tois,
                          {'_id': {'$in': list(toids)}}).count() != len(toids):
                log.debug('Some TOIs deleted.')
                raise CommitConflict(None, None)
            log.debug('Some TOIs already locked.')
            raise ToisLocked('Some TOIs already locked.', toids)

    def _unlockTois(self):
        mongo.update_many(self.database.tois, {'_handled_by': self.id},
                          {'$unset': {'_handled_by': True}})

    def commit(self, commit):
        "Commit pending changes to database"
        self.set_read_preference(pymongo.ReadPreference.PRIMARY)

        deletedTois = commit.deletedTois
        changedTois = commit.changedTois
        newTois = commit.newTois
        indexData = commit.indexData
        interested = commit.interested
        result = commit.results
        affectedToids = set(deletedTois) | set(diff.toid for diff in changedTois)

        try:
            self._lockTois(affectedToids)

            # xxx assumes that toirefs pointing to removed tois are
            # represented here with empty difftois
            for toiDiff in changedTois:
                # This line MUST be run in a different context than self,
                # since self will modify the data of the TOI loaded.
                # This is why processCommits() runs with a ReadonlyContext.
                # XXX Related to the above; it would be nice to have a
                # nicer interface to commit
                # ctx.processCommits(ctx.createCommit([], [])) (which
                # in practice is what is needed now) is bit of a mouth
                # full...
                toi = mongo.load_toi(self.database, toiDiff.toid,
                                     set(toiDiff.orgAttrs))
                diff = toiDiff.diffsOld(toi)
                if diff:
                    raise CommitConflict(toi, diff)

            self.database.reset_error_history()

            dbops = []

            for toiDiff in newTois:
                toc = blm.getTocByFullname(toiDiff.toc_fullname)
                dbops.append(mongo.bulk_save_toi(toiDiff.toid, toc, toiDiff.diffAttrs))

            for toiDiff in changedTois:
                if toiDiff.diffAttrs:
                    doc = {'$set': toiDiff.diffAttrs}
                    spec = {'_id': toiDiff.toid}
                    dbops.append(pymongo.operations.UpdateOne(spec, doc))

            dbops.append(mongo.bulk_remove_tois(deletedTois))

            log.debug('Bulk writing %d operations', len(dbops))
            try:
                result = mongo.bulk_write(self.database.tois, dbops,
                                          ordered=False)
            except BulkWriteError as exc:
                log.exception('Bulk write error: %s, ops: %s', exc.details, dbops)
                raise

            log.debug('Bulk write result: '
                      'acknowledged: %s, '
                      '%d deleted, '
                      '%d inserted, '
                      '%d matched, '
                      '%d modified, '
                      '%d upserted',
                      result.acknowledged,
                      result.deleted_count,
                      result.inserted_count,
                      result.matched_count,
                      result.modified_count,
                      result.upserted_count,
            )

            dbops = self.saveIndexData(indexData)
            if dbops:
                result = mongo.bulk_write(self.database.tois, dbops, ordered=False)

            for toid, blobVals in commit.addedBlobVals.items():
                toid = ObjectId(toid)
                for blobVal in blobVals:
                    blobVal.addref(toid)

            for toid, blobVals in commit.deletedBlobVals.items():
                toid = ObjectId(toid)
                for blobVal in blobVals:
                    blobVal.delref(toid)

            error = self.database.previous_error()
            if error:
                raise OperationFailure(error.get('err'), error.get('code'))

            commit.done(self.database)
        finally:
            # xxx unlock commit object?
            self._unlockTois()

    link_old_age = 3600

    def notifyChanges(self, commits):
        "Notify subscriptions of changes"
        new = set()
        changed = set()
        deleted = set()
        allowRead = set()
        outdatedBy = None
        for commit in commits:
            # it does not really matter which of the commit ids we pick
            # to take responsibility for the outdatedBy flag - as long
            # as it's something unique (e.g. an ObjectId)
            outdatedBy = commit._id
            new.update(diff.toid for diff in commit.newTois)
            changed.update(diff.toid for diff in commit.changedTois)
            deleted.update(commit.deletedTois)

            for toi in commit.newTois:
                allowRead.update(toi.diffAttrs.get('allowRead', []))
            for diff in commit.changedTois:
                _allowRead = (diff.diffAttrs.get('allowRead', []) +
                              diff.orgAttrs.get('allowRead', []))
                if not _allowRead:
                    _allowRead = blm.TO._query(id=diff.toid).run()[0].allowRead
                allowRead.update(_allowRead)
        allowRead = [toi.id[0] for toi in allowRead]
        allAffectedTois = new | changed | deleted

        self.database.links.update(
            {'type': 'LinkRequest',
             'params.toid': {'$in': list(changed|deleted)}},
            {'$set': {'outdatedBy': outdatedBy}}, multi=True)

        self.database.links.update(
            {'type': 'LinkSortedQuery',
             'ancient': False,
             '$or': [{'allowRead': {'$in': allowRead}},
                     {'state.query.id': {'$in': list(deleted)}}]},

            {'$set': {'outdatedBy': outdatedBy},
             '$addToSet': {'outdatedToids': {'$each': list(allAffectedTois)}}},
            multi=True)

        self.database.links.update(
            {'type': {'$nin': ['LinkRequest', 'LinkSortedQuery']}},
            {'$set': {'outdatedBy': outdatedBy}}, multi=True)

        self.database.links.update(
            {'type': 'LinkSortedQuery',
             'timestamp': {'$lt': time.time() - self.link_old_age},
             'outdatedBy': {'$ne': None},
             'ancient': False},
            {'$set': {'outdatedToids': [], 'ancient': True}}, multi=True)

def concat_values(x,y):
    try:
        return x + list(y.values())
    except AttributeError:
        return x + y

class OperateBase(object):
    """
    Action operation base class
    """

    def __eq__(self, o):
        return type(self) == type(o) and self.__dict__ == o.__dict__

    def __ne__(self, other):
        return not self == other

    def blobVals(self):
        raise NotImplementedError


class CreateToi(OperateBase):
    """
    Create TOI operation.
    """

    def __init__(self, tocName, toid, attrData=None):
        """
        Initialise the object.

        Arguments: tocName - full toc specification of the new TOI
                   toid - the assigned toi ID
                   attrData - attribute data for the new TOI
        Returns:   None
        """
        self.tocName = tocName
        self.toid = toid
        self.attrData = attrData

    def blobVals(self):
        for value in reduce(concat_values, list(self.attrData.values()), []):
            if isinstance(value, Attribute.BlobVal):
                yield value

    def checkPermissions(self, context):
        pass

    def operate(self, context):
        """
        Perform the create TOI operation.

        Arguments: context - The context to run the operation against.
        Returns:   The newly created TOI
        """
        if DEBUG_CP:
            print('Running',self, file=sys.stderr)
        # Obtain the toc
        toc = blm.getTocByFullname(self.tocName)
        self.tocName = toc._fullname

        # Fill out missing attributes
        for attrName, value in list(self.attrData.items()):
            attr = toc._attributes[attrName]

            if attr.computed or attr.getprop(pytransact.object.property.ReadOnly, pre=True):
                # xxx why should we accept data just because it's empty? remove the if statement
                # and always raise
                if self.attrData.get(attr.name):
                    # Access denied, read only attribute
                    raise cRuntimeError("Tried to access the read only "
                                        "attribute '%(name)s'." %
                                        {'name': attr._xlatKey,})
                del self.attrData[attr.name]
                continue

            if (isinstance(attr, Attribute.ToiRef) or
                isinstance(attr, Attribute.ToiRefMap)):
                rest = attr.getrest(pytransact.object.restriction.ToiType)
                if rest:
                    restToc = rest.validToiType
                elif (isinstance(attr, Attribute.Relation) and
                      attr.related is not None):
                    restToc = attr.related._toc
                else:
                    restToc = TO.TO

                try:
                    if isinstance(attr, Attribute.ToiRef):
                        value = [ restToc._create(toid) for toid in value ]
                    else: # ToiRefMap
                        if not hasattr(value,'items'):
                            value = dict(value)
                        for name, toid in value.items():
                            value[name] = (toid and restToc._create(toid)) or None
                except ToiNonexistantError as e:
                    raise ClientError(e)

            self.attrData[attrName] = attr.coerceValueList(value)

        # Fill in empty attributes
        elist = []
        for attr in toc._attributes.values():
            if attr.computed:
                continue

            if attr.getprop(pytransact.object.property.ReadOnly, pre=True):
                default = attr.default or []
                self.attrData[attr.name] = list(default)
            else:
                if not self.attrData.get(attr.name):
                    default = attr.default # or []
                    self.attrData[attr.name] = attr.coerceValueList(default)
                # Pre validate data
                try:
                    self.attrData[attr.name] = context.validateAttrValues(
                        None, attr, self.attrData[attr.name], pre=True)
                except ClientError as e:
                    #print 'Pre: Got', e, 'for', attr.name
                    elist.append(e.args[0])
                except LocalisedError as e:
                    #print 'Pre: Got', e, 'for', attr.name
                    elist.append(e)
        if elist:
            raise cAttrErrorList(elist)

        # Run create actions
        context.setMayChange(True)
        for attrName, value in self.attrData.items():
            act = getattr(toc._attributes[attrName], CREATE_ACTION, None)
            if act:
                self.attrData[attrName] = act(value, None)

        # Actually create the TOI instance
        #import pdb;pdb.set_trace()
        toi = toc._create(self.toid, self.attrData)
        toi._orgAttrData = dict((k, toc._attributes[k].default.__class__()) for k in self.attrData)
        self.toid = toi.id[0]

        context.newTois[toi.id[0]] = toi
        toi._phantom = False

        # Run TOC create action
        act = getattr(toi, CREATE_ACTION, None)
        if act is not None:
            act()

        # Post validate data
        for attrName in self.attrData:
            setattr(toi, attrName, context.validateAttrValues(
                toi, toi[attrName], pre=False))

        for attrName in self.attrData:
            context.updateRelations(toi, toi[attrName])

        context.setMayChange(False)

        # Return the new toi
        return toi

    def __str__(self):
        """
        Debug information.
        """
        return '<CreateToi %r %r %r>' % (
            self.tocName, self.toid, self.attrData)

custombson.register(CreateToi)


class ChangeToi(OperateBase):
    """
    Change TOI operation.
    """

    def __init__(self, toi, attrData):
        """
        Initialise the object.

        Arguments: toi - the TOI to change
                   attrData - the dict of the attributes to change.
        Returns:   None
        """
        self.toi = toi
        self.attrData = attrData

    def blobVals(self):
        for value in reduce(concat_values, list(self.attrData.values()), []):
            if isinstance(value, Attribute.BlobVal):
                yield value

    def checkPermissions(self, context):
        """
        Perform the access verification for the user.

        Arguments: context - context to perform operation in
        Returns:   None
        """
        # Check permissions
        for attrName in self.attrData:
            if not context.canWrite(self.toi, attrName):
                raise cAttrPermError(attrName, self.toi._xlatKey, self.toi.id[0])

    def operate(self, context):
        """
        Perform the change TOI operation.

        Arguments: context - context to perform operation in
        Returns:   The changed TOI
        """
        if DEBUG_CP:
            print('Running',self, file=sys.stderr)
        # Reload toi, self.toi may be from a different context(!)
        # this will ensure we notice tois that have been deleted
        # from under our feet.
        tois = blm.TO._query(id=self.toi.id[0]).run()
        if not tois or tois[0]._deleted:
            raise cToiDeletedError(self.toi._fullname, self.toi.id[0])
        toi = tois[0]
        self.tocName = toi._fullname

        elist = []
        # Pre validate data
        for attrName, value in list(self.attrData.items()):
            attr = toi[attrName]

            if (isinstance(attr, Attribute.ToiRef) or
                isinstance(attr, Attribute.ToiRefMap)):
                rest = attr.getrest(pytransact.object.restriction.ToiType)
                if rest:
                    restToc = rest.validToiType
                elif (isinstance(attr, Attribute.Relation) and
                      attr.related is not None):
                    restToc = attr.related._toc
                else:
                    restToc = TO.TO

                try:
                    if isinstance(attr, Attribute.ToiRef):
                        value = [ restToc._create(toid) for toid in value ]
                        # Check that toirefs exist
                        [v.allowRead.value for v in value]
                    else: # ToiRefMap
                        if not hasattr(value, 'iteritems'):
                            value = dict(value)
                        for name, toid in value.items():
                            value[name] = (toid and restToc._create(toid)) or None
                            if toid is not None:
                                toid.allowRead.value # Check that toirefs exist
                except ToiNonexistantError as e:
                    raise ClientError(e)
                self.attrData[attrName] = value

            try:
                self.attrData[attrName] = context.validateAttrValues(
                    toi, attr, value, pre=True)
            except ClientError as e:
                elist.append(e.args[0])
            except LocalisedError as e:
                elist.append(e)
        if elist:
            raise cAttrErrorList(elist)

        # Run update actions
        context.setMayChange(True)
        for attrName, value in list(self.attrData.items()):
            attr = toi[attrName]
            act = getattr(attr, UPDATE_ACTION, None)
            if act:
                self.attrData[attrName] = act(value, toi)

        # Run TOC update action
        act = getattr(toi, UPDATE_ACTION, None)
        if act:
            # Make sure a 'modified' entry for this TOI exists.
            act(self.attrData)
        else:
            for attrName, value in self.attrData.items():
                setattr(toi, attrName, value)

        # Post validate
        for attrName in self.attrData:
            setattr(toi, attrName, context.validateAttrValues(
                toi, toi[attrName], pre=False))

        for attrName, value in self.attrData.items():
            context.updateRelations(toi, toi[attrName])

        context.setMayChange(False)

        # Return the toi
        return toi

    def __str__(self):
        return '<ChangeToi %r %r>' % (self.toi.id[0], self.attrData)

    __repr__ = __str__


custombson.register(ChangeToi)


class DeleteToi(OperateBase):
    """
    Delete TOI operation.
    """

    def __init__(self, toi):
        """
        Initialise the object.

        Arguments: toid - the TOI to delete.
        Returns:   None
        """
        self.toi = toi

    def blobVals(self):
        return []

    def checkPermissions(self, context):
        """
        Perform the access verification for the user.

        Arguments: context - context to perform operation in
        Returns:   None
        """
        return context.canDelete(self.toi)

    def operate(self, context):
        """
        Perform the delete TOI operation.

        Arguments: context - context to perform operation in
        Returns:   None
        """

        if DEBUG_CP:
            print('Running',self, file=sys.stderr)
        # Reload toi, self.toi may be from a different context(!)
        # this will ensure we notice tois that have been deleted
        # from under our feet.
        #import pdb;pdb.set_trace()
        tois = blm.TO._query(id=self.toi.id[0]).run()
        if not tois or tois[0]._deleted:
            return # Already gone
        toi = tois[0]
        self.tocName = toi._fullname

        # Run toc actions
        context.setMayChange(True)
        act = getattr(toi, DELETE_ACTION, None)
        if act is not None:
            act()

        # Check that relations are empty. If any relation isn't
        # the delete will fail.
        for attr in toi:
            if isinstance(attr, Attribute.Relation):
                if attr.related is not None:
                    relattr = attr.related
                    if not relattr.getprop(pytransact.object.property.Weak):
                        for ref in attr.value:
                            if not ref._deleted and toi in ref[relattr.name]:
                                raise cRuntimeError(
                                    "The Toi (%(to)s %(toi)s) could not be "
                                    "removed beacuse the Toi (%(refto)s "
                                    "%(ref)s) has a relation to it." % {
                                        'to': toi._xlatKey,
                                        'refto':ref._xlatKey,
                                        'toi':toi.id[0],
                                        'ref':ref.id[0] })
                else:
                    for ref in attr.value:
                        if not ref._deleted:
                            relattr = context.findRelatedAttr(toi, ref, attr)
                            if (not relattr.getprop(pytransact.object.property.Weak) and
                                toi in ref[relattr.name]):
                                raise cRuntimeError(
                                    "The Toi (%(to)s %(toi)s) could not be "
                                    "removed beacuse the Toi (%(refto)s "
                                    "%(ref)s) has a relation to it." % {
                                        'to': toi._xlatKey,
                                        'refto':ref._xlatKey,
                                        'toi':toi.id[0],
                                        'ref':ref.id[0] })

        # Actually mark for deletion. Ugly, but done this way to avoid
        # recursive operation
        toi._deleted = True
        context.register(toi)

    def __str__(self):
        return '<DeleteToi %s>' % (self.toi.id[0],)

custombson.register(DeleteToi)


class CallToi(OperateBase):
    """
    Call TOI operation.
    """

    def __init__(self, toid, methodName, args):
        """
        Initialise the object.

        Arguments: toid - the ID of the TOI to change
                   methodName - the name of the method
                   args - the method arguments
        Returns:   None
        """
        self.toid = toid
        self.methodName = methodName
        self.args = args

    def blobVals(self):
        for arg in self.args:
            for v in arg:
                if isinstance(v, Attribute.BlobVal):
                    yield v

    def checkPermissions(self, context):
        toi = blm.TO._query(id=self.toid).run()[0]
        if self.methodName not in toi._methods:
            raise cAttrNameError(self.methodName,toi._xlatKey,self.toid)

        # Check permissions
        if not context.canWrite(toi, self.methodName):
            raise cAttrPermError(self.methodName,toi._xlatKey,self.toid)

    def operate(self, context):
        """
        Perform the change TOI operation.

        Arguments: context - context to perform operation in
        Returns:   Result of the operation
        """
        if DEBUG_CP:
            print('Running',self, file=sys.stderr)
        toi = blm.TO._query(id=self.toid).run()[0]

        # Check that the method exists
        if self.methodName not in toi._methods:
            raise cAttrNameError(self.methodName,toi._xlatKey,self.toid)

        method = getattr(toi, self.methodName)

        if DEBUG_CPCT:
            print('Calling method for', self.toid, self.methodName, file=sys.stderr)

        # Build argument list
        if len(self.args) < len(method.params):
            self.args.extend([[]]*(len(method.params)-len(self.args)))

        elif len(self.args) > len(method.params):
            raise cTypeError("The method %(method)s() takes exactly "
                             "%(nparams)d arguments, %(nargs)d given." %
                             {'method' : self.methodName,
                              'nparams' : len(method.params),
                              'nargs' : len(self.args), })

        attrList = []
        for i in range(len(method.params)):
            value = self.args[i]
            attr = method.params[i]
            if isinstance(attr, Attribute.ToiRef):
                rest = attr.getrest(pytransact.object.restriction.ToiType)
                if rest:
                    restToc = rest.validToiType
                else:
                    restToc = TO.TO
                value = [ restToc._create(toid) for toid in value ]
                try:
                    # Check that toirefs exist
                    [v.allowRead.value for v in value]
                except ToiNonexistantError as e:
                    raise ClientError(e)

            attrList.append(value)

        # Run call code
        context.setMayChange(True)
        result = method.clientInvocation(*attrList)
        context.setMayChange(False)

        # Validate return data
        if method.rtype is not None:
            method.rtype.validateValues(False, result)

        return result

    def __str__(self):
        return 'CallToi(%r, %r, %r)'%(self.toid, self.methodName, self.args)

custombson.register(CallToi)


class CallBlm(OperateBase):
    """
    Call BLM method operation
    """

    def __init__(self, blmName, methodName, args):
        """
        Initialise the object.

        Arguments: blmName - the name of the BLM
                   methodName - the name of the method
                   args - the method arguments
        Returns:   None
        """
        self.blmName = blmName
        self.methodName = methodName
        self.args = args

    def blobVals(self):
        for arg in self.args:
            for v in arg:
                if isinstance(v, Attribute.BlobVal):
                    yield v

    def checkPermissions(self, context):
        mod = getattr(blm, self.blmName)
        try:
            mod._methods[self.methodName]
        except KeyError:
            raise cAttrNameError(self.methodName, self.blmName, None)

    def operate(self, context):
        """
        Perform the call BLM method operation.

        Arguments: context - context to perform operation in
        Returns:   Result of the operation
        """
        if DEBUG_CP:
            print('Running',self, file=sys.stderr)
        # Check that the method exists
        mod = getattr(blm, self.blmName)
        try:
            method = mod._methods[self.methodName]
        except KeyError:
            raise cAttrNameError(self.methodName, self.blmName, None)

        # Build argument list
        if len(self.args) < len(method.params):
            self.args.extend([[]]*(len(method.params)-len(self.args)))

        elif len(self.args) > len(method.params):
            raise cTypeError("The method %(method)s() takes exactly "
                             "%(nparams)d arguments, %(nargs)d given." %
                             {'method': self.methodName,
                              'nparams': len(method.params),
                              'nargs': len(self.args), })

        attrList = []
        for i in range(len(method.params)):
            value = self.args[i]
            attr = method.params[i]

            if type(value) not in (list, tuple):
                raise cTypeError("Wrong parameter type for %(method)s "
                                 "%(param)s: %(val)s" %
                                 {'method': self.methodName,
                                  'param': repr(attr),
                                  'val': repr(value)})

            if isinstance(attr, Attribute.ToiRef):
                rest = attr.getrest(pytransact.object.restriction.ToiType)
                if rest:
                    restToc = rest.validToiType
                else:
                    restToc = TO.TO
                # Check that toirefs exist
                def normalizetoid(toi):
                    if hasattr(toi, 'id'):
                        return toi.id[0]
                    return ObjectId(toi)
                value = list(map(normalizetoid, value))
                tois = dict((t.id[0], t) for t in restToc._query(id=value).run())
                try:
                    value = [ tois[toid] for toid in value ]
                except KeyError as e:
                    raise cToiNonexistantError(restToc._fullname, e.args[0])

            attrList.append(value)

        # Run call code
        context.setMayChange(True)
        result = method.clientInvocation(*attrList)
        context.setMayChange(False)

        # Validate return data
        if method.rtype is not None:
            method.rtype.validateValues(False, result)

        return result

    def __str__(self):
        return 'CallBlm(%r, %r, %r)'%(self.blmName, self.methodName, self.args)

custombson.register(CallBlm)
