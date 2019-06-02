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

import bson, logging, os, gridfs, py, pymongo
from bson.objectid import ObjectId

from pymongo.errors import OperationFailure
from pytransact.difftoi import DiffTOI
from pytransact import commit, mongo
from pytransact.contextbroker import ContextBroker
from pytransact.exceptions import *
from pytransact.object.attribute import BlobVal
from pytransact.testsupport import ContextTests, Fake, RuntimeContext, Time
import blm

def setup_module(mod):
    from blm import fundamental
    mod.blm = blm
    blm.addBlmPath(os.path.join(os.path.dirname(__file__), 'blm'))
    from blm import testcommit

    logging.basicConfig()
    commit.log.setLevel(logging.DEBUG)

def teardown_module(mod):
    blm.removeBlmPath(os.path.join(os.path.dirname(__file__), 'blm'))
    blm.clear()


class BaseCommitContextTests(ContextTests):

    def setup_method(self, method):
        super(BaseCommitContextTests, self).setup_method(method)
        with RuntimeContext(self.database):
            self.user = blm.fundamental.AccessHolder(super=[True])
        self.sync()

    def newcontext(self, user=None):
        if user is None:
            user = self.user
        ctx = commit.CommitContext(self.database, user)
        ctx.setMayChange(True)
        ContextBroker().pushContext(ctx)
        return ctx

    def commit(self):
        ctx = ContextBroker().context
        ctx.runCommit([], interested=None)
        ContextBroker().popContext()
        self.sync()
        self.newcontext()

    def set_primary(self):
        self._database = self.database.client.get_database(
            self.database.name, read_preference=pymongo.ReadPreference.PRIMARY)

    def find(self, query, collection=None):
        collection = collection or self.database.tois
        return mongo.find(collection, query)

    def find_one(self, query, collection=None):
        collection = collection or self.database.tois
        return mongo.find_one(collection, query)


class TestCommitContext(BaseCommitContextTests):

    def test_wait_for_commit(self):
        self._commit('interested')
        result, error = commit.wait_for_commit(self.database, 'interested',
                                                        timeout=1)
        assert result
        assert not error

    def test_wait_for_commit_timeout(self):
        py.test.raises(commit.Timeout, commit.wait_for_commit,
                       self.database, 'interested', timeout=0.1)

        class MyException(Exception): pass

        py.test.raises(MyException, commit.wait_for_commit,
                       self.database, 'interested', onfail=MyException, timeout=0.1)


    def test_simple(self):
        cctx = commit.CommitContext(self.database)

    def test_createToi(self):
        cctx = self.newcontext()
        toi = cctx.createToi(blm.testcommit.Test, cctx.newId(),
                             {'name': ['test']})

        assert toi.name == ['test']
        assert toi.__class__._query(name='test').run()[0] is toi
        assert self.find({'_toc': 'testcommit.Test'}).count() == 0

        ContextBroker().popContext()
        cctx.runCommit([])
        assert self.find({'_toc': 'testcommit.Test'}).count() == 1

    def test_canWrite_new_toi(self):
        user = blm.fundamental.AccessHolder()
        cctx = self.newcontext(user=user)
        toi = cctx.createToi(blm.testcommit.Test, cctx.newId(),
                             {'name': ['test']})
        self.sync()
        assert toi.name == ['test']
        assert toi.__class__._query(name='test').run()[0] is toi
        assert self.find({'_toc': 'testcommit.Test'}).count() == 0

        ContextBroker().popContext()
        cctx.runCommit([])
        assert self.find({'_toc': 'testcommit.Test'}).count() == 1

    def test_changeToi(self):
        toi = blm.testcommit.Test(name=['test'])
        self.sync()

        cctx = self.newcontext()
        # New context, so we have to look it up again
        toi = blm.testcommit.Test._query().run()[0]

        toi(extra=['fOo'])

        assert toi.extra == ['fOo']
        assert toi.__class__._query(extra='fOo').run()[0] is toi
        assert toi.__class__._query(extra=None).run() == []
        dbtoi, = list(self.database.tois.find({'_toc': 'testcommit.Test'}))
        assert dbtoi.get('extra',[]) == []

        ContextBroker().popContext()
        cctx.runCommit([])

        dbtoi, = list(self.database.tois.find({'_toc': 'testcommit.Test'}))
        assert dbtoi['extra'] == ['fOo']

    def test_changeToi_with_nop_change(self):
        toi = blm.testcommit.Test(name=['test'])
        self.sync()

        cctx = self.newcontext()
        # New context, so we have to look it up again
        toi = blm.testcommit.Test._query(_attrList=['name']).run()[0]

        toi(name=['fOo'])

        assert toi.name == ['fOo']
        assert toi.__class__._query(name='fOo').run()[0] is toi
        assert toi.__class__._query(name=None).run() == []
        dbtoi, = list(self.database.tois.find({'_toc': 'testcommit.Test'}))
        assert dbtoi.get('extra',[]) == []

        toi(name=['test'])  # Restore to original value

        ContextBroker().popContext()
        commit = cctx.runCommit([])
        assert commit.state != 'failed'

        dbtoi, = list(self.database.tois.find({'_toc': 'testcommit.Test'}))
        assert dbtoi['name'] == ['test']

    def test_deleteToi(self):
        toi = blm.testcommit.Test(name=['text'])
        print(toi, toi.__class__)
        self.sync()

        cctx = self.newcontext()
        toi, = blm.testcommit.Test._query().run()
        toi._delete()
        print(toi, toi.__class__)
        assert toi.__class__._query().run() == []
        self.sync()
        assert self.find({'_toc': 'testcommit.Test'}).count() == 1

        ContextBroker().popContext()
        cctx.runCommit([])

        assert self.find({'_toc': 'testcommit.Test'}).count() == 0

    def test_runQuery_simple(self):
        # This is actually already tested by the queries
        # in the above *Toi tests, but we make an explicit test
        # anyway
        blm.testcommit.Test(name=['text'])
        self.sync()
        cctx = self.newcontext()

        toi, = blm.testcommit.Test._query(name='text').run()
        name = toi.name[0]
        assert name == 'text'

    def test_runQuery_subQuery(self):
        foo = blm.testcommit.Test(name=['foo'])
        blm.testcommit.Test(name=['text'], toiref=[foo])
        self.commit()
        cctx = self.newcontext()
        q = blm.testcommit.Test._query(
            toiref=blm.testcommit.Test._query(name='foo'))
        toi, = q.run()
        assert toi.name == ['text']

    def test_requestAttribute(self):
        cctx = self.newcontext()

        toi = blm.fundamental.AccessHolder._query().run()[0]
        attrVal = cctx.requestAttribute(toi, blm.fundamental.AccessHolder.super)
        assert attrVal == [True]

        toi = blm.testcommit.Test(name=['text'])
        attrVal = cctx.requestAttribute(toi, blm.testcommit.Test.name)
        assert attrVal == ['text']

    def test_requestAttribute_with_toi_deleted(self):
        cctx = self.newcontext()

        toi = blm.testcommit.Test(name=['foo'])
        toi._delete()

        py.test.raises(RuntimeError, cctx.requestAttribute, toi, None)

    def test_preloadAttributes(self):
        py.test.skip('Not really useful, remove it?')

    def test_validateAttrValues_simple(self):
        cctx = self.newcontext()

        toi1 = blm.testcommit.RestTest(name=['test'])
        value = ['foo']
        rval = cctx.validateAttrValues(toi1, toi1.name, value)
        assert rval == value
        py.test.raises(ClientError, cctx.validateAttrValues, toi1,
                       toi1.name, [])
        py.test.raises(ClientError, cctx.validateAttrValues, toi1,
                       toi1.name, ['foo', 'bar'])

    def test_validateAttrValues_readonly(self):
        toi1 = blm.testcommit.Test(name=['test'])
        cctx = self.newcontext()

        py.test.raises(ClientError, cctx.validateAttrValues,
                       None, blm.testcommit.Test.readonly, ['foo'])

        toi1 = blm.testcommit.Test._query(name='test').run()[0]
        py.test.raises(ClientError, cctx.validateAttrValues,
                       toi1, blm.testcommit.Test.readonly, ['foo'])

    def test_validateAttrValues_computed(self):
        toi1 = blm.testcommit.Test(name=['test'])
        cctx = self.newcontext()

        py.test.raises(ClientError, cctx.validateAttrValues,
                       None, blm.testcommit.Test.computed, ['foo'])

        toi1 = blm.testcommit.Test._query(name='test').run()[0]
        py.test.raises(ClientError, cctx.validateAttrValues,
                       toi1, blm.testcommit.Test.computed, ['foo'])

    def test_validateAttrValues_unchangeable(self):
        toi1 = blm.testcommit.Test(name=['test'])
        cctx = self.newcontext()

        value = ['foo']
        rval = cctx.validateAttrValues(None, blm.testcommit.Test.unchangeable,
                                       ['foo'])
        assert value == rval

        toi1 = blm.testcommit.Test._query(name='test').run()[0]
        # XXX unchangeable is tested against what (if any) change
        # has been made in the toi!
        toi1.unchangeable = ['foo']
        py.test.raises(ClientError, cctx.validateAttrValues,
                       toi1, blm.testcommit.Test.unchangeable, None)

    def test_validateAttrValues_weakref(self):
        # Check that deleted tois are dropped
        cctx = self.newcontext()

        toi1 = blm.testcommit.Test(name=['toi1'])
        toi2 = blm.testcommit.Test(name=['toi2'])
        toi3 = blm.testcommit.Test(name=['toi3'])
        toi3._delete()

        value = [toi1, toi2, toi3]
        rval = cctx.validateAttrValues(toi1, blm.testcommit.Test.weakref,
                                       value)
        assert rval == [toi1, toi2]

    def test_validateAttrValues_reorder(self):
        toi1 = blm.testcommit.Test(name=['toi1'], reorder=['a','b','c'])
        self.sync()

        cctx = self.newcontext()
        toi1, = blm.testcommit.Test._query(name='toi1').run()

        py.test.raises(ClientError, cctx.validateAttrValues,
                       toi1, toi1.reorder, ['a'])

        value = ['c','b','a']
        rval = cctx.validateAttrValues(toi1, toi1.reorder, value)
        assert value == rval

    def test_validateAttrValues_unique(self):
        cctx = self.newcontext()

        toi1 = blm.testcommit.Test(name=['toi1'],
                                   unique=['toi1'])
        py.test.raises(ClientError, cctx.validateAttrValues, None,
                       toi1.unique, ['toi1'])

        value = ['toi1']
        rval = cctx.validateAttrValues(toi1, toi1.unique, value)
        assert rval == value

    def test_validateAttrValues_simple_toitype(self):
        cctx = self.newcontext()

        toi1 = blm.testcommit.Test(name=['toi1'])
        py.test.raises(ClientError, cctx.validateAttrValues, None,
                       toi1.simpleToiType, [toi1])
        toi1.name = ['test']
        value = [toi1]
        rval = cctx.validateAttrValues(None, toi1.simpleToiType, value)
        assert value == rval

    def test_validateAttrValues_toiref_exists(self):
        cctx = self.newcontext()

        toi1 = blm.testcommit.Test(name=['toi1'])
        phantom = blm.testcommit.Test._create(ObjectId())
        value = [phantom]
        rval = cctx.validateAttrValues(None, toi1.toiref, value, pre=True)
        assert rval == value

        # do not accept phantom tois in database
        py.test.raises(ClientError, cctx.validateAttrValues, None,
                       toi1.toiref, value, pre=False)

    def test_validateAttrValues_complex_toitype(self):
        cctx = self.newcontext()

        toi1 = blm.testcommit.Test(name=['toi1'])
        toi2 = blm.testcommit.Other(name=['toi2'])
        toi3 = blm.testcommit.Test(name=['toi3'],
                                   toiref = [toi2])

        py.test.raises(ClientError, cctx.validateAttrValues, None,
                       toi1.complexToiType, [ toi1 ])

        toi1.complexToiType = [toi3]
        py.test.raises(ClientError, cctx.validateAttrValues, None,
                       toi1.complexToiType, [ toi3 ])

        q = blm.testcommit.Test._query(toiref =
                                       blm.testcommit.Other._query(name='test'),
                                       id = [toi3])

        toi2.name = ['test']
        value = [toi3]
        rval = cctx.validateAttrValues(None, toi1.complexToiType, value)
        assert rval == value

    def test_validateAttrValues_toirefmap(self):
        cctx = self.newcontext()

        toi1 = blm.testcommit.Test(name=['toi1'])
        toi2 = blm.testcommit.Test(name=['toi2'],
                                   toirefmap={'toi1': toi1})

        value = {'foo': toi2}
        rval = cctx.validateAttrValues(None, toi1.toirefmap, value)
        assert rval == value

    def test_findRelatedAttr(self):
        cctx = self.newcontext()

        toi1 = blm.testcommit.Other(name=['other'])
        toi2 = blm.testcommit.Related()

        rval = cctx.findRelatedAttr(toi1, toi2, toi1.related)
        assert rval == toi2.other

    def test_updateRelations(self):
        toi1 = blm.testcommit.Other(name=['other'])
        toi2 = blm.testcommit.Related(name=['related'],
                                      other=[toi1])
        toi1.related = [toi2] # Simple commit context doesn't fix this
        self.sync()

        cctx = self.newcontext()
        toi1 = blm.testcommit.Other._query().run()[0]

        toi2 = blm.testcommit.Related._query(name=['related']).run()[0]

        toi3 = blm.testcommit.Related(name=['releated3'],
                                            other=[toi1])

        self.commit()
        toi1 = blm.testcommit.Other._query(id=toi1.id).run()[0]
        assert toi1.related == [toi2, toi3]

        toi2 = blm.testcommit.Related._query(id=toi2.id).run()[0]
        toi2.other = []
        self.commit()

        toi1 = blm.testcommit.Other._query(id=toi1.id).run()[0]
        assert toi1.related == [toi3]

        toi2 = blm.testcommit.Related._query(id=toi2.id).run()[0]
        toi2._orgAttrData['other'] = [] # pretend it changed in DB

        toi2.other = [toi1]
        self.commit()

        toi1 = blm.testcommit.Other._query(id=toi1.id).run()[0]
        assert toi1.related == [toi3, toi2]

        toi2 = blm.testcommit.Related._query(id=toi2.id).run()[0]
        toi2._orgAttrData['other'] = [toi1]
        toi2.other = []
        toi2._delete()
        self.commit()

        toi1 = blm.testcommit.Other._query(id=toi1.id).run()[0]
        assert toi1.related == [toi3]

    def test_commitRelations(self):
        self.newcontext()
        toi1 = blm.testcommit.Other(name=['other'])
        toi2 = blm.testcommit.Related(name=['related'], other=[toi1])
        self.commit()

        toi1, = blm.testcommit.Other._query(id=toi1.id).run()
        assert toi1.related == [toi2]

        toi2, = blm.testcommit.Related._query(id=toi2.id).run()
        toi2.other = []
        toi2._delete()
        self.commit()

        toi1, = blm.testcommit.Other._query(id=toi1.id).run()
        assert toi1.related == []

        self.newcontext()
        toi1 = blm.testcommit.OtherWeak()
        toi2 = blm.testcommit.Related(name=['related'], weak=[toi1])
        self.commit()

        toi1, = blm.testcommit.OtherWeak._query(id=toi1.id).run()
        assert toi1.related == [toi2]

        toi2, = blm.testcommit.Related._query(id=toi2.id).run()
        toi2._delete()
        self.commit()

        toi1, = blm.testcommit.OtherWeak._query(id=toi1.id).run()
        assert toi1.related == []

    def test_updateBlobs(self):
        self.newcontext()
        val1 = BlobVal('foo')
        val1.large_blob = 2
        toi = blm.testcommit.Test(blob=[val1])
        self.commit()

        assert val1.references == {toi.id[0]}

        self.newcontext()

        ref = ObjectId()
        val1.addref(ref)
        self.sync()

        toi, = blm.testcommit.Test._query().run()

        val1 = toi.blob[0]
        val2 = BlobVal('foo')
        val2.large_blob = 2
        toi.blob = [val2]
        self.commit()
        self.sync()

        assert val1.references == {ref}
        assert val2.references == {toi.id[0]}

        self.newcontext()

        toi, = blm.testcommit.Test._query().run()
        val2 = toi.blob[0]

        toi._delete()
        self.commit()
        self.sync()

        assert val2.references == set()

        py.test.raises(Exception, gridfs.GridFS(self.database, 'blobvals').get, val2.value._id)


    def test_runAfterCommit(self):
        py.test.xfail("post-commit hooks not supported")
        callbackCalled = []
        def callback(tid, *args, **kw):
            callbackCalled.append((tid, args, kw))

        class Op(commit.OperateBase):
            def checkPermissions(self, context):
                pass
            def operate(self, context):
                context.runAfterCommit(callback, 42, foo='bar')

        cctx = commit.CommitContext(self.database)
        ContextBroker().pushContext(cctx)
        cctx.setMayChange(True)

        results = cctx.runCommit([Op()])

        assert callbackCalled == [(None, (42,), {'foo':'bar'})]

    def test_runAfterCommitFailing(self):
        py.test.xfail("post-commit hooks not supported")
        callbackCalled = []
        def callback(tid, *args, **kw):
            callbackCalled.append((tid, args, kw))
            raise RuntimeError('error')
        def callback2(tid, *args, **kw):
            callbackCalled.append((tid, args, kw))

        class Op(commit.OperateBase):
            def checkPermissions(self, context):
                pass
            def operate(self, context):
                context.runAfterCommit(callback, 42, foo='bar')
                context.runAfterCommit(callback2, 43)

        cctx = commit.CommitContext(self.database)
        ContextBroker().pushContext(cctx)
        cctx.setMayChange(True)

        results = cctx.runCommit([Op()])

        assert callbackCalled == [(None, (42,), {'foo':'bar'}),
                                  (None, (43,), {})]

    def test_notifyChanges_being_called(self):
        result = []
        def notifyChanges(commits):
            result.append([c._id for c in commits])

        cctx = self.newcontext()
        cctx.id = None # cheat - pretend that this commit is unhandled
        commit1 = cctx.createCommit([], [])
        commit1.save(self.database)
        ContextBroker().popContext()

        cctx = self.newcontext()
        commit2 = cctx.createCommit([], [])
        ContextBroker().popContext()

        cctx.notifyChanges = notifyChanges
        cctx.processCommits(commit2)

        expected = [[commit2._id, commit1._id]]
        assert result == expected

    def test_runCommit(self):
        op = commit.CallBlm('testcommit', 'simple', [['bar']])
        cctx = self.newcontext()
        cctx.runCommit([op])

    def test_runCommit_error(self):
        op = commit.CallBlm('testcommit', 'broken', [])
        cctx = self.newcontext()
        cmt = cctx.runCommit([op])
        assert cmt.error

    def test_runCommit_createCommit_fail(self, monkeypatch):
        def createCommit(*args, **kw):
            kw['args'] = args
            kw['_id'] = ObjectId()
            return kw

        op = commit.CallBlm('testcommit', 'simple', [['bar']])
        cctx = self.newcontext()

        monkeypatch.setattr(cctx, 'createCommit', createCommit)

        op = commit.CallBlm('testcommit', 'broken', [])
        cctx = self.newcontext()
        cmt = cctx.runCommit([op], processCommits=False)
        assert cmt.error.message == 'broken'

    def test_runCommit_error_with_interest(self):
        op = commit.CallBlm('testcommit', 'broken', [])
        cctx = self.newcontext()
        interested = ObjectId()
        cmt = cctx.runCommit([op], interested=interested)
        assert cmt.error
        self.sync()
        stored = mongo.find_one(self.database.commits, {'_id': cmt._id})
        assert type(cmt.error) == type(stored['error'])
        assert cmt.error.args == stored['error'].args

    def test_createCommit(self):
        cctx = self.newcontext()
        toi1 = blm.testcommit.Test(name=['foo'])
        toi2 = blm.testcommit.Test(name=['bar'])
        toi3 = blm.testcommit.Test(name=['baz'])
        toi1._orgAttrData = { 'name': ['apa'] }

        cctx.changedTois = { toi1.id[0]: toi1 }
        cctx.newTois = { toi2.id[0]: toi2 }
        cctx.deletedTois = {toi3.id[0]: toi3}
        cctx.indexData = [(toi1.id[0], {'toid': toi1.id[0],
                                        'data': ['foo', 'bar']})]
        bval1 = BlobVal('x')
        bval2 = BlobVal('y')
        bval3 = BlobVal('z')

        cctx.addedBlobVals = { str(toi1.id[0]): [bval1]}
        cctx.deletedBlobVals = { str(toi2.id[0]): [bval2]}

        ops = [commit.CallBlm('foo', 'bar', [[bval3]])]

        cmt = cctx.createCommit(ops, [['result']], interested='interested')

        assert cmt.user == cctx.user != None
        assert set(cmt.deletedTois) == set([toi3.id[0]])
        diff = DiffTOI()
        diff.setAttrDiff(toi2.__class__, toi2.id[0],
                         {'name': []}, {'name': ['bar']})
        assert cmt.newTois == [diff]
        diff = DiffTOI()
        diff.setAttrDiff(toi1.__class__, toi1.id[0],
                         {'name': ['apa']}, {'name': ['foo']})
        assert cmt.changedTois == [diff]
        assert cmt.indexData == [(toi1.id[0], {'toid': toi1.id[0],
                                                  'data': ['foo', 'bar']})]
        assert cmt.addedBlobVals == { str(toi1.id[0]): [bval1]}
        assert cmt.deletedBlobVals == { str(toi2.id[0]): [bval2]}
        assert cmt.operations == ops
        assert cmt.results == [['result']]
        assert cmt.interested == 'interested'
        assert cmt.error is None

        assert bval1.references == {cmt._id}
        assert bval2.references == {cmt._id}
        assert bval3.references == {cmt._id}

    def test_createCommit_error(self):
        cctx = self.newcontext()
        toi1 = blm.testcommit.Test(name=['foo'])
        toi2 = blm.testcommit.Test(name=['bar'])
        toi3 = blm.testcommit.Test(name=['baz'])
        toi1._orgAttrData = { 'name': ['apa'] }

        cctx.changedTois = { toi1.id[0]: toi1 }
        cctx.newTois = { toi2.id[0]: toi2 }
        cctx.deletedTois = {toi3.id[0]: toi3}
        cctx.indexData = [(toi1.id[0], {'toid': toi1.id[0],
                                        'data': ['foo', 'bar']})]

        ops = []
        error = ClientError()
        commit = cctx.createCommit(ops, [['result']], error=error)
        assert commit.newTois == []
        assert commit.changedTois == []
        assert commit.deletedTois == []
        assert commit.indexData == []
        assert commit.results == []
        assert commit.error is error

    def test_createCommit_bad_doc(self):
        cctx = self.newcontext()
        toi1 = blm.testcommit.Test(name=['foo'*8*1024*1024]) # 24MB
        cctx.newTois = { toi1.id[0]: toi1 }

        ops = [commit.CallBlm('foo', 'bar', [])]

        cmt = cctx.createCommit(ops, [['result']], interested='interested')
        # Will fail when commit is saved


    def _commit(self, interested=None, _id=ObjectId(), operations=[],
                result=[['result'], 42], error=None):
        toiToDelete = blm.testcommit.Test(name=['baz'])
        toiToChange = blm.testcommit.Test(name=['foo'])
        cctx = self.newcontext()
        toiToCreate = blm.testcommit.Test(name=['bar'])

        new = DiffTOI()
        new.setToi(toiToCreate)

        changed = DiffTOI()
        changed.setAttrDiff(toiToChange.__class__, toiToChange.id[0],
                            toiToChange._attrData, {'name': ['apa']})
        doc = {'_id': _id,
               'newTois': [new],
               'changedTois': [changed],
               'deletedTois': [toiToDelete.id[0]],
               'operations': operations,
               'addedBlobVals': {},
               'deletedBlobVals': {},
               'results': result,
               'error': error,
               'indexData': [],
               'handled_by': cctx.id,
               'user': cctx.user,
               'interested': interested}
        commitId = mongo.insert(self.database.commits, doc)
        assert commitId
        self.sync()
        ContextBroker().popContext()

        assert self.find_one({'_id': toiToChange.id[0]})['name'] == ['foo']
        assert not self.find_one({'_id': toiToCreate.id[0]})
        assert self.find_one({'_id': toiToDelete.id[0]})
        cctx.commit(commit.Commit.fromquery(self.database, {'_id': commitId}))
        self.sync()

        assert self.find_one({'_id': toiToChange.id[0]})['name'] == ['apa']
        assert self.find_one({'_id': toiToCreate.id[0]})
        assert not self.find_one({'_id': toiToDelete.id[0]})
        return commitId

    def test_commit_without_interest_successful(self):
        commitId = self._commit()
        assert not mongo.find_one(self.database.commits, {'_id': commitId})

    def test_commit_with_interest_successful(self):
        commitId = self._commit(interested=[1, 2])
        commit = mongo.find_one(self.database.commits, {'_id': commitId})
        assert commit['handled_by'] == commit['interested'] == [1, 2]
        assert commit['state'] == 'done'
        assert commit['results'] == [['result'], 42]

    def test_commit_with_interest_error(self):
        commitId = self._commit(interested=[1, 2], result=[None], error='error')
        commit = mongo.find_one(self.database.commits, {'_id': commitId})
        assert commit['handled_by'] == commit['interested'] == [1, 2]
        assert commit['state'] == 'done'
        assert commit['results'] == [None]
        assert commit['error'] == 'error'

    def test_commit_blobval_reference_handling(self):
        commitId = ObjectId()
        bv1 = BlobVal('foo')
        bv2 = BlobVal('bar')
        bv3 = BlobVal('baz')
        bv1.large_blob = bv2.large_blob = bv3.large_blob = 2
        bv1.addref(commitId)
        bv3.addref(commitId)

        op = commit.CallToi(ObjectId(), 'foo', [[bv1, bv3]])
        result = [[bv2, bv3, 'baz']]
        commitId = self._commit(interested=[1, 2], _id=commitId, operations=[op],
                                result=result)
        cmt = mongo.find_one(self.database.commits, {'_id': commitId})

        py.test.raises(gridfs.NoFile, bv1.gridfs(self.database).get, bv1.value._id)
        assert cmt['results'][0][0].references == {commitId}
        assert cmt['results'][0][1].references == {commitId}

    def test_commit_with_mongodb_error(self):
        self.set_primary()
        self.database.previous_error = lambda : {'err': 'ohnoes!', 'code': 123}
        err = py.test.raises(OperationFailure, self._commit)
        assert err.value.args[0] == 'ohnoes!'
        assert err.value.code == 123

    def test_commit_already_in_progress(self):
        toi = blm.testcommit.Test(name=['foo'])
        mongo.find_and_modify(self.database.tois, {'_id': toi.id[0]},
                           {'$set': {'_handled_by': ObjectId()}})
        self.sync()
        cctx = self.newcontext()
        toi, = blm.testcommit.Test._query(name=['foo']).run()
        toi(name=['bar'])
        py.test.raises(commit.ToisLocked, cctx.commit,
                       cctx.createCommit([], []))

    def test_rerun_conflicting_commit(self):
        toi = blm.testcommit.Test(name=['foo'])
        self.sync()
        assert self.find_one({'_id': toi.id[0]})
        cctx = self.newcontext()
        op = commit.CallToi(toi.id[0], 'add', [['bar']])
        commitId = cctx.runCommit([op], processCommits=False)
        ContextBroker().popContext()
        toi.extra = ['conflict']
        self.sync()

        _rerunCommit = cctx.rerunCommit
        def rerunCommit(*args, **kw):
            db_toi_data = self.find_one({'_id': toi.id[0]})
            assert db_toi_data.get('_terms', []) == []
            return _rerunCommit(*args, **kw)
        cctx.rerunCommit = rerunCommit

        cctx.processCommits(commitId)
        self.sync()

        cctx = self.newcontext()
        toi, = blm.testcommit.Test._query().run()
        assert toi.extra == ['conflict', 'bar']

        db_toi_data = self.find_one({'_id': toi.id[0]})
        py.test.skip('full text index disabled for now')
        assert db_toi_data['_terms'] == [{'toid': toi.id[0],
                                          'data': ['bar', 'conflict']}]

    def test_rerun_locked_tois_commit_self(self):
        toi = blm.testcommit.Test(name=['foo'])
        cctx = self.newcontext()
        op = commit.CallToi(toi.id[0], 'add', [['bar']])
        commitId = cctx.runCommit([op], processCommits=False)

        # toi is locked by a commit already, abuse the fact that
        # locked toi check does not care about who has locked it, so
        # using commit context's own ID which will be removed by
        # unlocking
        mongo.find_and_modify(self.database.tois, {'_id': toi.id[0]},
                           {'$set': {'_handled_by': cctx.id}})

        cctx.processCommits(commitId)
        self.sync()

        cctx = self.newcontext()
        toi, = blm.testcommit.Test._query().run()
        assert toi.extra == ['bar']

    def test_mark_failed_commits(self):
        cctx = self.newcontext()
        def commit(commit):
            raise RuntimeError('everything broke')
        cctx.commit = commit
        commit = cctx.runCommit([])
        commit = self.find_one({'_id': commit._id}, self.database.commits)
        assert commit['state'] == 'failed'
        assert 'everything broke' in commit['traceback']
        error = commit['error']
        assert isinstance(error, BlError)

    def test_failure_timeout_on_toislocked(self):
        cctx = self.newcontext()
        def ct(c):
            raise commit.ToisLocked()
        cctx.commit = ct
        py.test.raises(commit.Timeout, cctx.runCommit, [])

    def test_failure_timeout_on_conflict(self):
        cctx = self.newcontext()
        def ct(c):
            raise commit.CommitConflict('toi', 'diff')
        cctx.commit = ct
        py.test.raises(commit.Timeout, cctx.runCommit, [])

    def test_saveIndexData(self):
        py.test.skip('full text index disabled for now')
        cctx = self.newcontext()
        toid = ObjectId()
        child1 = ObjectId()
        child2 = ObjectId()
        mongo.insert(self.database.tois, {'_id': toid})

        get_stored = lambda : sorted(self.find_one({'_id': toid})['_terms'])

        indexData = [(toid, [{'toid': toid, 'data': ['foo']}])]
        expect = [{'toid': toid, 'data': ['foo']}]
        cctx.saveIndexData(indexData)
        assert get_stored() == expect

        indexData = [(toid, [{'toid': toid, 'data': ['foo']},
                             {'toid': child1, 'data': ['bar']}])]
        expect = sorted([{'toid': toid, 'data': ['foo']},
                         {'toid': child1, 'data': ['bar']}])
        cctx.saveIndexData(indexData)
        assert get_stored() == expect

        indexData = [(toid, [{'toid': child1, 'data': ['bar', 'baz']},
                             {'toid': child2, 'data': ['qux']}])]
        expect = sorted([{'toid': toid, 'data': ['foo']},
                         {'toid': child1, 'data': ['bar', 'baz']},
                         {'toid': child2, 'data': ['qux']}])
        cctx.saveIndexData(indexData)
        assert get_stored() == expect


class TestNotifyChanges(BaseCommitContextTests):

    def setup_method(self, method):
        super(TestNotifyChanges, self).setup_method(method)
        self.time = Time()

    def teardown_method(self, method):
        super(TestNotifyChanges, self).teardown_method(method)
        self.time.restore()

    def test_requests(self):
        toid1 = ObjectId()
        toid2 = ObjectId()
        toid3 = ObjectId()

        link1 = mongo.insert(self.database.links,
                          {'type': 'LinkRequest', 'params': { 'toid': toid1}})
        link2 = mongo.insert(self.database.links,
                          {'type': 'LinkRequest', 'params': { 'toid': toid2}})
        link3 = mongo.insert(self.database.links,
                          {'type': 'LinkRequest', 'params': { 'toid': toid3}})

        cctx = self.newcontext()
        diff = DiffTOI()
        diff.toid = toid1
        diff.diffAttrs['allowRead'] = [Fake(id=[ObjectId()])]
        cid1, cid2 = ObjectId(), ObjectId()
        commits = [commit.Commit.fromdoc(self.database,
                                         {'_id': cid1,
                                          'changedTois': [diff],
                                          'deletedTois': [],
                                          'newTois': []}),
                   commit.Commit.fromdoc(self.database,
                                         {'_id': cid2,
                                          'changedTois': [],
                                          'deletedTois': [toid2],
                                          'newTois': []})]
        cctx.notifyChanges(commits)
        self.sync()

        outdated = self.find({'outdatedBy': {'$ne': None}}, self.database.links)
        assert outdated.count() == 2
        outdated = list(outdated)
        assert {link['_id'] for link in outdated} == {link1, link2}
        assert outdated[0]['outdatedBy'] == cid2
        assert outdated[1]['outdatedBy'] == cid2

    def test_sorted_query_by_allow_read(self):
        user1 = blm.fundamental.AccessHolder()
        user2 = blm.fundamental.AccessHolder()
        toi1 = blm.testcommit.Test(name=['test'], allowRead=[user1])
        toi2 = blm.testcommit.Test(name=['test'], allowRead=[user2])
        toi3 = blm.testcommit.Test(name=['test'])
        self.commit()

        link1 = mongo.insert(self.database.links,
                          {'type': 'LinkSortedQuery', 'allowRead': user1.id,
                           'timestamp': self.time.now, 'ancient': False})
        link2 = mongo.insert(self.database.links,
                          {'type': 'LinkSortedQuery', 'allowRead': user2.id,
                           'timestamp': self.time.now, 'ancient': False})
        link3 = mongo.insert(self.database.links,
                          {'type': 'LinkSortedQuery',
                           'state': {'query': [{'id': toi3.id[0]}]},
                           'allowRead': [ObjectId()],
                           'timestamp': self.time.now, 'ancient': False})
        link4 = mongo.insert(self.database.links,
                          {'type': 'LinkSortedQuery',
                           'ancient': False,
                           'allowRead': user1.id,
                           'outdatedToids': [ObjectId()],
                           'timestamp': self.time.now -
                           commit.CommitContext.link_old_age - 1})

        cctx = self.newcontext()
        diff = DiffTOI()
        diff.toid = toi1.id[0]
        cid = ObjectId()
        commits = [commit.Commit.fromdoc(self.database,
                                         {'_id': cid,
                                          'changedTois': [diff],
                                          'deletedTois': toi3.id,
                                          'newTois': []})]
        cctx.notifyChanges(commits)
        self.sync()

        outdated = self.find({'outdatedBy': {'$ne': None}}, self.database.links)
        outdated = dict((link['_id'], link) for link in outdated)
        assert len(outdated) == 3
        assert set(outdated) == {link1, link3, link4}

        assert outdated[link1]['outdatedBy'] == cid
        assert outdated[link3]['outdatedBy'] == cid
        assert outdated[link4]['outdatedBy'] == cid
        assert set(outdated[link1]['outdatedToids']) == {toi1.id[0], toi3.id[0]}
        assert set(outdated[link3]['outdatedToids']) == {toi1.id[0], toi3.id[0]}
        assert len(outdated[link4]['outdatedToids']) == 0

class TestChangeToi(BaseCommitContextTests):

    def test_simple(self):
        toi = blm.testcommit.Test(name=['foo'])
        self.commit()
        with self.newcontext() as cctx:
            toi = blm.testcommit.Test._query(id=toi.id).run()[0]
            op = commit.ChangeToi(toi, {'name': ['bar']})
            op.operate(cctx)

            assert toi.name == ['bar']

    def test_commit_deleted(self):
        toi = blm.testcommit.Test()
        self.commit()

        ctx1 = commit.CommitContext(self.database)
        ctx1.setMayChange(True)
        with ctx1:
            toi1 = blm.testcommit.Test._query(id=toi.id).run()[0]
            op = commit.ChangeToi(toi1, {'name': ['bar']})

            interested = ObjectId()
            c = ctx1.runCommit([op], interested=interested, processCommits=False)

        toi._delete()
        self.commit()

        ctx1.processCommits(c)

        result, error = commit.wait_for_commit(self.database, interested)
        assert error


class TestDeleteToi(BaseCommitContextTests):

    def test_simple(self):
        toi = blm.testcommit.Test(name=['foo'])
        self.commit()
        with self.newcontext() as cctx:
            toi = blm.testcommit.Test._query(id=toi.id).run()[0]
            op = commit.DeleteToi(toi)
            op.operate(cctx)

            assert toi._deleted

    def test_commit_deleted(self):
        toi = blm.testcommit.Test()
        self.commit()

        ctx1 = self.newcontext()
        with ctx1:
            toi1 = blm.testcommit.Test._query(id=toi.id).run()[0]
            op = commit.DeleteToi(toi1)

        toi._delete()
        self.commit()

        interested = ObjectId()
        ctx1.runCommit([op], interested=interested)

        result, error = commit.wait_for_commit(self.database, interested)
        assert not error # It was already gone, so we are ok.


class TestOperations(BaseCommitContextTests):

    def test_CreateToi(self):
        toid = ObjectId()
        op = commit.CreateToi('testcommit.Test', toid, {'name': ['test']})
        cctx = self.newcontext()
        toi = op.operate(cctx)
        assert not toi._phantom

    def test_CallToi_simple(self):
        toi = blm.testcommit.Test(name=['test'])
        self.sync()
        cctx = self.newcontext()

        op = commit.CallToi(toi.id[0], 'simple', [['bar']])
        commitDoc = cctx.runCommit([op])
        result = commitDoc.results

        assert result == [['test', 'bar']]

    def test_CallBlm_simple(self):
        cctx = self.newcontext()

        op = commit.CallBlm('testcommit', 'simple', [['bar']])
        commitDoc = cctx.runCommit([op])
        result = commitDoc.results

        assert result == [['foo', 'bar']]

    def test_CallBlm_write(self):
        user = blm.fundamental.AccessHolder()
        toi = blm.testcommit.Test(name=['foo'], allowRead=[user])
        self.commit()
        self.sync()
        cctx = self.newcontext(user=user)

        op = commit.CallBlm('testcommit', 'write', [[toi], ['bar']])
        cmt = cctx.runCommit([op])
        # xxx find a better way of testing this
        assert 'AttrPermError' in repr(cmt.error)

    def test_CallBlm_non_existant_toiref(self):
        op = commit.CallBlm('testcommit', 'write', [[ObjectId()], ['bar']])
        cctx = self.newcontext()
        print(py.test.raises(ClientError, op.operate, cctx))

    def test_BlobVals(self):
        val = BlobVal('foo')
        op = commit.CallToi(ObjectId(), 'foo', [[val], ['apa']])
        assert set(op.blobVals()) == {val}

        op = commit.CallBlm('theblm', 'foo', [[val], ['apa']])
        assert set(op.blobVals()) == {val}

    def test_serialization(self):
        op = commit.DeleteToi(None)
        data = {'op': op}
        son = bson.BSON.encode(data)
        decoded = son.decode()
        assert decoded == data

class TestCommitObject(BaseCommitContextTests):
    def test_attributes(self):
        c = commit.Commit()
        assert isinstance(c._id, ObjectId)
        assert c.user == None
        assert c.interested == None
        assert c.handled_by == None
        assert c.operations == []
        assert c.newTois == []
        assert c.changedTois == []
        assert c.deletedTois == []
        assert c.addedBlobVals == {}
        assert c.deletedBlobVals == {}
        assert c.indexData == []
        assert c.results == []
        assert c.error == None
        assert c.traceback == None
        assert c.state == 'new'
        assert c.generation == 0

    def test_get_doc(self):
        c = commit.Commit()
        result = c.get_doc()
        expected = {
            '_id': c._id,
            'user': None,
            'interested': None,
            'error': None,
            'traceback': None,
            'state': 'new',
            'generation': 0
            }
        assert result == expected

    def test_save(self):
        c = commit.Commit()
        c.save(self.database)
        self.sync()
        result = self.find_one({'_id': c._id}, self.database.commits)
        expect = c.get_doc()
        assert result == expect

    def test_save_gridfs(self):
        resstr = 'random string'*24*2**10
        c1 = commit.Commit(results = [resstr])
        c1.save(self.database)
        self.sync()
        result = self.find_one({'_id': c1._id}, self.database.commits)
        gridfile = result['_griddata']
        assert isinstance(gridfile, BlobVal)
        assert isinstance(gridfile.value, gridfs.GridOut)
        c2 = commit.Commit.fromdoc(self.database, result)
        assert c2.results == [resstr]

    def test_delete(self):
        class Op(object):
            def __init__(self, blobs):
                self.blobs = blobs

            def blobVals(self):
                return self.blobs

        c = commit.Commit()
        added = BlobVal('added')
        deleted = BlobVal('deleted')
        opval = BlobVal('op')
        added.addref(c._id)
        deleted.addref(c._id)
        opval.addref(c._id)
        result = BlobVal('result')
        result.addref(c._id)

        ops = [ Op([opval]) ]

        c.results = [ result ]
        c.addedBlobVals = { 'a' : [added] }
        c.deletedBlobVals = { 'b' : [deleted] }
        c.save(self.database)
        self.sync()

        assert self.find_one({'_id': c._id}, self.database.commits)
        c.operations = ops # Not BSONable
        c.delete(self.database)
        self.sync()
        assert not self.find_one({'_id': c._id}, self.database.commits)
        assert added.references == set()
        assert deleted.references == set()
        assert ops[0].blobs[0].references == set()
        assert result.references == set()

    def test_delete_gridfs(self):
        c = commit.Commit(results=['foo bar baz'*24*2**10])
        doc = c.get_doc()
        blobval = doc['_griddata']
        c.save(self.database)
        self.sync()
        assert self.find_one({'_id': c._id}, self.database.commits)
        assert self.find_one({'metadata.references.value': c._id},
                             self.database.blobvals.files)

        c.delete(self.database)
        self.sync()

        assert not self.find_one({'_id': c._id}, self.database.commits)
        assert not self.find_one({'metadata.references.value': c._id},
                                 self.database.blobvals.files)
        # Make sure that ALL generated blobvals get decref()'d
        assert blobval.references == set()


    def test_unhandle(self):
        handler = ObjectId()
        c = commit.Commit(handled_by=handler)
        c.unhandle(self.database, handler)
        self.sync()

        cdoc = self.find_one({'_id': c._id}, self.database.commits)
        assert 'handled_by' not in cdoc

    def test_unhandle_handled(self):
        handler = ObjectId()
        c1 = commit.Commit(handled_by=handler)
        c1.save(self.database)
        c1.unhandle_handled(self.database, c1._id, handler)
        self.sync()
        assert self.find_one({'_id': c1._id}, self.database.commits)

        c2 = commit.Commit(handled_by=handler)
        # c1 is intentional. Ensure c2 isn't saved in any form
        c1.unhandle_handled(self.database, c2._id, handler)
        c2.unhandle_handled(self.database, c2._id, handler)
        self.sync()
        assert not self.find_one({'_id': c2._id}, self.database.commits)

    def test_handlers_running(self):
        self.set_primary()
        handler = ObjectId()
        c = commit.Commit(handled_by=handler)
        c.save(self.database)

        assert c.handlers_running(self.database)
        c.unhandle(self.database, handler)

        assert not c.handlers_running(self.database)

    def test_handle(self):
        handler = ObjectId()
        c1 = commit.Commit()
        c1.save(self.database)
        expected = c1.get_doc()
        expected['handled_by'] = handler

        c2 = commit.Commit.handle(self.database, handler)
        assert c2.get_doc() == expected

        c3 = commit.Commit.handle(self.database, handler)
        assert c3 is None

    def test_done(self):
        c = commit.Commit(interested='interested')
        result = BlobVal('result')
        c.results = [result]
        c.newTois = {'a': 'b'}
        c.done(self.database)
        self.sync()

        assert result.references == {c._id}
        result = self.find_one({'_id': c._id}, self.database.commits)
        expect = {'_id': c._id,
                  'error': None,
                  'handled_by': 'interested',
                  'interested': 'interested',
                  'results': c.results,
                  'state': 'done'}
        assert result == expect

    def test_done_not_interested(self):
        c = commit.Commit()
        result = BlobVal('result')
        c.results = [result]
        c.newTois = {'a': 'b'}
        c.done(self.database)

        assert result.references == set()
        result = self.find_one({'_id': c._id}, self.database.commits)
        assert not result

    def test_fromdoc(self):
        _id = ObjectId()
        c = commit.Commit.fromdoc(self.database, {'_id': _id,
                                                  'results': ['foo']})
        assert c._id == _id
        assert c.results == ['foo']

    def test_fromquery(self):
        handler = ObjectId()
        c1 = commit.Commit(handled_by=handler)
        c1.save(self.database)
        self.sync()
        c2 = commit.Commit.fromquery(self.database, {'handled_by': handler})
        assert c1._id == c2._id
