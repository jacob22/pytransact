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

import py
import os
from bson.objectid import ObjectId

from pytransact import commit, contextbroker, exceptions, mongo
from pytransact import link as Link
from pytransact import query as Query
from pytransact.object.attribute import BlobVal
from pytransact.testsupport import ContextTests, Time
import blm
from blm import fundamental

blmpath = os.path.join(os.path.dirname(__file__), 'blm')


def setup_module(mod):
    import blm.fundamental
    blm.addBlmPath(blmpath)


def teardown_module(mod):
    blm.removeBlmPath(blmpath)
    blm.clear()


class LinkTests(ContextTests):

    def setup_method(self, method):
        super(LinkTests, self).setup_method(method)
        self.time = Time()
        self.clientId = mongo.insert(self.database.clients, {'updates': []})
        self.linkId = 27
        self.user = blm.fundamental.AccessHolder()

    def teardown_method(self, method):
        super(LinkTests, self).teardown_method(method)
        self.time.restore()

    def _getLinkData(self):
        self.sync()
        return mongo.find_one(self.database.links,
                              {'client': self.clientId,
                               'link': self.linkId})

    def _getResult(self):
        result = mongo.find_and_modify(self.database.clients,
                                    {'_id': self.clientId},
                                    {'$set': {'updates': []}})
        if not result['updates']:
            return None
        return result['updates'][0]['args']

    def _getState(self):
        self.sync()
        link = mongo.find_one(self.database.links,
                              {'client': self.clientId,
                               'link': self.linkId})
        if link:
            return link['state']

    def _getParams(self):
        self.sync()
        link = mongo.find_one(self.database.links,
                              {'client': self.clientId,
                               'link': self.linkId})
        if link:
            return link['params']

    def uptodate(self):
        self.sync()
        link = mongo.find_one(self.database.links, {'client': self.clientId,
                                                    'link': self.linkId})
        return link and not link.get('outdatedBy')

    def outdate(self, toids=[]):
        mongo.update_one(self.database.links,
                         {'client': self.clientId, 'link': self.linkId},
                         {'$set': {'outdatedBy': ObjectId()},
                          '$addToSet': {'outdatedToids': {'$each': toids}}})
        self.sync()


class LinkTransient(Link.Link):

    def _run(self, params, state=None):
        assert state is None
        self.update({'foo': 'bar'})


class LinkPersistent(Link.Link):

    state = {'gurka': 'tomat'}
    result = {'foo': 'bar'}

    def _run(self, params, state=None):
        self._state = state
        self.save(params, self.state)
        self.update(self.result, persistent=True)


class TestLinkPersistence(LinkTests):

    def setup_method(self, method):
        super(TestLinkPersistence, self).setup_method(method)
        self.factory = Link.LinkFactory()
        self.factory.Transient = LinkTransient
        self.factory.Persistent = LinkPersistent
        self.pushnewctx(commit.CommitContext, user=self.user)

    def test_transient(self):
        link = self.factory.create('Transient', self.clientId, self.linkId,
                            params=dict(toid='apa'))
        link.run()
        self.sync()
        # no persistent links
        assert not list(self.database.links.find({'client': self.clientId,
                                            'link': self.linkId}))

        # but one (transient) update for the client
        clientdata, = list(self.database.clients.find({'_id': self.clientId}))

        assert clientdata['updates'] == [{'type': 'update',
                                          'id': self.linkId,
                                          'args': {'foo': 'bar'}}]

    def test_persistent_new(self):
        assert self.database.links.find({'client': self.clientId,
                                   'link': self.linkId}).count() == 0
        link = self.factory.create('Persistent', self.clientId, self.linkId,
                            params={'apa': 'bepa'})
        link.run()
        self.sync()

        assert link._state is None
        linkdata, = list(self.database.links.find({'client': self.clientId,
                                             'link': self.linkId}))
        clientdata, = list(self.database.clients.find({'_id': self.clientId}))

        linkdata.pop('_id') # don't care
        assert linkdata == {'type': 'LinkPersistent',
                            'client': self.clientId,
                            'link': self.linkId,
                            'allowRead': [self.user.id[0]],
                            'params': {'apa': 'bepa'},
                            'state': link.state,
                            'timestamp': self.time,
                            'outdatedBy': None,
                            'ancient': False}

        assert clientdata['updates'] == [{'type': 'update',
                                          'id': self.linkId,
                                          'args': {'foo': 'bar'}}]

    def test_persistent_client_update(self):
        # create an existing link
        link = self.factory.create('Persistent', self.clientId, self.linkId,
                            params=dict(toid='27', attrs=['foo', 'bar']))
        link.run()
        self.sync()

        self.database.clients.find_and_modify({'_id': self.clientId},
                                        {'$set': {'updates': []}})

        self.time.step()

        # now update it
        link = self.factory.create('Persistent', self.clientId, self.linkId,
                            params=dict(toid='27', attrs=['bar', 'baz']))
        link.run()
        self.sync()

        assert link._state == {'gurka': 'tomat'}
        linkdata, = list(self.database.links.find({'client': self.clientId,
                                             'link': self.linkId}))
        clientdata, = list(self.database.clients.find({'_id': self.clientId}))

        linkdata.pop('_id') # don't care
        assert linkdata == {'type': 'LinkPersistent',
                            'client': self.clientId,
                            'link': self.linkId,
                            'allowRead': [self.user.id[0]],
                            'params': dict(toid='27', attrs=['bar', 'baz']),
                            'state': link.state,
                            'timestamp': self.time,
                            'outdatedBy': None,
                            'ancient': False}

        assert clientdata['updates'] == [{'type': 'update',
                                          'id': self.linkId,
                                          'args': {'foo': 'bar'}}]

    def test_persistent_db_update(self, monkeypatch):
        # create an existing link
        link = self.factory.create('Persistent', self.clientId, self.linkId,
                            params=dict(toid='27', attrs=['foo', 'bar']))
        link.run()
        mongo.update_one(self.database.clients, {'_id': self.clientId},
                         {'$set': {'updates': []}})
        self.sync()

        # no new data, but uptodate should be set
        monkeypatch.setattr(LinkPersistent, 'result', None)
        link = self.factory.create('Persistent', self.clientId, self.linkId)
        link.run()
        self.sync()

        clientdata = mongo.find_one(self.database.clients, {'_id': self.clientId})
        assert clientdata['updates'] == []
        linkdata = mongo.find_one(self.database.links, {'client': self.clientId,
                                               'link': self.linkId})
        assert linkdata['outdatedBy'] == None
        monkeypatch.undo()

        monkeypatch.setattr(LinkPersistent, 'state', {'sallad': 'paprika'})

        # now update it
        link = self.factory.create('Persistent', self.clientId, self.linkId)
        link.run()
        self.sync()
        assert link._state == {'gurka': 'tomat'}

        linkdata, = list(self.database.links.find({'client': self.clientId,
                                             'link': self.linkId}))
        clientdata, = list(self.database.clients.find({'_id': self.clientId}))

        linkdata.pop('_id') # don't care
        assert linkdata == {'type': 'LinkPersistent',
                            'client': self.clientId,
                            'link': self.linkId,
                            'state': {'sallad': 'paprika'},
                            'outdatedBy': None,
                            'allowRead': [self.user.id[0]],
                            'timestamp': self.time,
                            'ancient': False,
                            # same params as first time:
                            'params': dict(toid='27', attrs=['foo', 'bar'])}

        assert clientdata['updates'] == [{'type': 'update',
                                          'id': self.linkId,
                                          'args': {'foo': 'bar'}}]

    def test_remove(self):
        link = self.factory.create('Persistent', self.clientId, self.linkId,
                            params={'apa': 'bepa'})
        link.run()
        self.sync()

        # sanity checks
        assert mongo.find_one(self.database.links, {'link': self.linkId})
        client = mongo.find_one(self.database.clients, {'_id': self.clientId})

        link = self.factory.create('Persistent', self.clientId, self.linkId)
        link.remove()
        self.sync()

        assert not mongo.find_one(self.database.links, {'link': self.linkId})
        client = mongo.find_one(self.database.clients, {'_id': self.clientId})

    def test_iterate_links(self, monkeypatch):
        # create a few links
        link0 = self.factory.create('Persistent', self.clientId, self.linkId,
                                    params=dict(toid='27',
                                                attrs=['foo', 'bar']))
        link0.run()
        link1 = self.factory.create('Persistent', self.clientId, self.linkId+1,
                                    params=dict(toid='28',
                                                attrs=['apa', 'bepa']))
        link1.run()
        link2 = self.factory.create('Persistent', self.clientId, self.linkId+2,
                                    params=dict(toid='29',
                                                attrs=['gurka', 'tomat']))
        link2.run()

        self.sync()

        monkeypatch.setattr(Link, 'LinkPersistent', LinkPersistent,
                            raising=False)

        links = set((l.clientId, l.linkId) for l in self.factory.iter())
        expected = set((l.clientId, l.linkId) for l in [link0, link1, link2])
        assert links == expected

        otherclient = ObjectId()
        link3 = self.factory.create('Persistent', otherclient, self.linkId,
                             params=(dict(toid='30', attrs=['foo', 'bar'])))
        link3.run()
        self.sync()

        links = set((l.clientId, l.linkId) for l in self.factory.iter(
                {'client': otherclient}))
        expected = set((l.clientId, l.linkId) for l in [link3])
        assert links == expected


class TestLink(LinkTests):

    def setup_method(self, method):
        super(TestLink, self).setup_method(method)
        self.ops = []
        self.results = [[42]]
        self.calls = []
        self.error = None
        def runCommit(ops, interested=None):
            self.sync()
            state = self._getState()
            # link must have been saved before we process commit
            assert state == 'processing'
            self.ops = ops
            self.interested = interested
            cmt = commit.Commit(handled_by=interested, state='done',
                                interested=interested, results=self.results,
                                error=self.error)
            cmt.done(self.database)

        self.ctx.runCommit = runCommit

    def test_CallMethod_blm(self):
        link = Link.LinkCallMethod(self.clientId, self.linkId,
                                   {'blmName':'testBlm',
                                    'methodName':'testMethod',
                                    'args':[0,1,2]})
        val = BlobVal('42')
        val.large_blob = 1
        self.results = [[{'foo': val}]]
        link.run()
        self.sync()
        op, = self.ops
        assert isinstance(op, commit.CallBlm)
        assert op.args == [0,1,2]
        assert op.blmName == 'testBlm'
        assert op.methodName == 'testMethod'
        assert self.interested == (self.clientId, self.linkId)

        result = self._getResult()
        assert result == {'error': None, 'result': [{'foo': val}]}
        assert result['result'][0]['foo'].references == {self.clientId}

        # commits are removed after result has been produced
        assert mongo.find(self.database.links,
                       {'client': link.clientId, 'link': link.linkId},
                          ).count() == 0

    def test_CallMethod_blm_error(self):
        self.results = []
        self.error = 'error'
        link = Link.LinkCallMethod(self.clientId, self.linkId,
                                   {'blmName':'testBlm',
                                    'methodName':'testMethod',
                                    'args':[0,1,2]})
        link.run()
        op, = self.ops
        assert isinstance(op, commit.CallBlm)
        assert op.args == [0,1,2]
        assert op.blmName == 'testBlm'
        assert op.methodName == 'testMethod'
        assert self.interested == (self.clientId, self.linkId)

        result = self._getResult()
        assert result == {'error': 'error', 'result': []}
        self.sync()

        assert mongo.find(self.database.links, {'client': link.clientId,
                                       'link': link.linkId}).count() == 0

    def test_CallMethod_toi(self):
        oid = ObjectId()
        link = Link.LinkCallMethod(self.clientId, self.linkId,
                                   {'toid': str(oid),
                                    'methodName':'testMethod',
                                    'args':[0,1,2]})
        link.run()
        op, = self.ops
        assert isinstance(op, commit.CallToi)
        assert op.args == [0,1,2]
        assert op.toid == oid
        assert op.methodName == 'testMethod'
        assert self.interested == (self.clientId, self.linkId)

        self.sync()
        result = self._getResult()
        assert result == {'error': None, 'result': [42]}

        assert mongo.find(self.database.links, {'client': link.clientId,
                                       'link': link.linkId},
                       ).count() == 0


class TestLinkWithData(LinkTests):

    def test_Request(self):
        from blm import testblm
        oid = ObjectId()
        link = Link.LinkRequest(self.clientId, self.linkId,
                                {'toid': str(oid),
                                 'attrList' : ['attr1', 'attr2']})
        link.run()
        x = self._getResult()
        assert isinstance(x['error'], exceptions.ToiNonexistantError)
        assert not self.uptodate()

        oid = ObjectId()
        toi = self.ctx.createToi(blm.testblm.Test, oid,
                                 {'name':['test_Request']})
        self.sync()
        link = Link.LinkRequest(self.clientId, self.linkId,
                                {'toid': str(oid),
                                 'attrList' : ['attr1', 'attr2']})
        link.run()
        x = self._getResult()
        assert x['error'] is None
        assert x['toiDiff'].diffAttrs == {'attr1': [], 'attr2': [] }
        s = self._getState()
        assert s is None
        assert not self.uptodate()

        oid = ObjectId()
        toi = self.ctx.createToi(blm.testblm.Test, oid,
                                 {'name':['test_Request'], 'attr1':['A1']})
        self.sync()
        link = Link.LinkRequest(self.clientId, self.linkId,
                                {'toid': str(oid),
                                 'attrList' : ['attr1', 'attr2']})
        link.run()
        x = self._getResult()
        assert x['error'] is None
        assert x['toiDiff'].diffAttrs == {'attr1': ['A1'], 'attr2': [] }
        s = self._getState()
        assert s is None
        assert not self.uptodate()

        # subscription
        link = Link.LinkRequest(self.clientId, self.linkId,
                                {'toid': str(oid),
                                 'attrList' : ['attr1', 'attr2'],
                                 'subscription': True})
        link.run()
        self.sync()

        x = self._getResult()
        assert x['error'] is None
        assert x['toiDiff'].diffAttrs == {'attr1': ['A1'], 'attr2': [] }

        s = self._getState()
        assert s == {'attr1': ['A1'], 'attr2': [] }
        assert self.uptodate()

        p = self._getParams()
        assert p['toid'] == oid

        link = Link.LinkRequest(self.clientId, self.linkId)
        link.run()
        x = self._getResult()
        assert x is None

        s = self._getState()
        assert s == {'attr1': ['A1'], 'attr2': [] }
        assert self.uptodate()

        # update
        self.outdate()
        self.sync()

        toi.attr2 = ['A2']

        link = Link.LinkRequest(self.clientId, self.linkId)
        link.run()
        self.sync()
        x = self._getResult()
        assert x['toiDiff'].diffAttrs == {'attr2': [BlobVal('A2')]}
        s = self._getState()
        assert s == {'attr1': ['A1'], 'attr2': [BlobVal('A2')] }
        assert s['attr2'][0].references == {self.clientId, link.link['_id']}
        assert self.uptodate()

    def test_Query(self):
        from blm import testblm
        query = blm.testblm.Test._query(name='test_Query')

        # empty result
        link = Link.LinkQuery(self.clientId, self.linkId, {'criteria': query})
        link.run()
        x = self._getResult()
        assert x['add'] == {}
        assert x['del'] == {}
        assert x['error'] is None
        s = self._getState()
        assert s is None
        assert not self.uptodate()

        # non empty result
        oid1 = ObjectId()
        toi1 = self.ctx.createToi(blm.testblm.Test, oid1,
                                  {'name':['test_Query'],
                                   'attr1':['A1']})
        self.sync()
        self.pushnewctx()
        link = Link.LinkQuery(self.clientId, self.linkId, {'criteria' : query})
        link.run()
        x = self._getResult()
        assert x['add'] == { str(oid1): 'testblm.Test' }
        assert x['del'] == {}
        assert x['error'] is None
        s = self._getState()
        assert s is None
        assert not self.uptodate()

        # subscription
        link = Link.LinkQuery(self.clientId, self.linkId,
                              {'criteria' : query, 'subscription': True})
        link.run()
        self.sync()
        x = self._getResult()
        assert x['add'] == { str(oid1): 'testblm.Test' }
        assert x['del'] == {}
        assert x['error'] is None

        s = self._getState()
        assert s == { str(oid1): 'testblm.Test' }
        assert self.uptodate()


        # update, new toi
        self.outdate()
        oid2 = ObjectId()
        toi2 = self.ctx.createToi(blm.testblm.Test, oid2,
                                  {'name':['test_Query'],
                                   'attr1':['A2']})
        self.sync()
        self.pushnewctx()
        link = Link.LinkQuery(self.clientId, self.linkId)
        link.run()
        self.sync()

        x = self._getResult()
        assert x['add'] == { str(oid2): 'testblm.Test' }
        assert x['del'] == {}
        assert x['error'] is None

        s = self._getState()
        assert s == { str(oid1): 'testblm.Test',
                      str(oid2): 'testblm.Test',}
        assert self.uptodate()

        # update, remove toi
        self.outdate()
        self.ctx.deleteToi(toi1)
        self.sync()
        self.pushnewctx()
        link = Link.LinkQuery(self.clientId, self.linkId)
        link.run()
        self.sync()
        x = self._getResult()
        assert x['add'] == {}
        assert x['del'] == { str(oid1): 'testblm.Test' }
        assert x['error'] is None

        s = self._getState()
        assert s == { str(oid2): 'testblm.Test'}
        assert self.uptodate()

    def test_SortedQuery_basic(self, monkeypatch):
        monkeypatch.setattr(BlobVal, 'large_blob', 1)

        from blm import testblm
        query = blm.testblm.Test._query(name='test_SortedQuery')
        query.clear()

        # empty result
        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria' : query,
                                     'attrList' : ['attr1', 'attr2']})
        link.run()
        x = self._getResult()
        assert x['diffops'] == []
        assert x['error'] is None
        assert x['toiDiffs'] == {}
        s = self._getState()
        assert s is None
        assert not self.uptodate()

        # non empty result
        self.outdate()
        oid = ObjectId()
        toi = self.ctx.createToi(blm.testblm.Test, oid,
                                 {'name':['test_SortedQuery'], 'attr1':['A1']})
        self.sync()
        self.pushnewctx()

        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria' : query,
                                     'attrList' : ['attr1', 'attr2']})
        link.run()
        x = self._getResult()
        assert x['diffops'] == [[0, 0, [toi]]]
        assert x['error'] is None
        assert len(x['toiDiffs']) == 1
        td = x['toiDiffs'][str(oid)]
        assert td.diffAttrs == {'attr1': ['A1'], 'attr2': [] }
        s = self._getState()
        assert s is None
        assert not self.uptodate()

        # subscription
        self.outdate()
        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria' : query,
                                     'attrList' : ['attr1', 'attr2'],
                                     'subscription' : True,
                                     'sorting': 'attr1'})
        link.run()
        self.sync()
        x = self._getResult()
        assert x['diffops'] == [[0, 0, [toi]]]
        assert x['error'] is None
        assert len(x['toiDiffs']) == 1
        td = x['toiDiffs'][str(oid)]
        assert td.diffAttrs == {'attr1': ['A1'], 'attr2': [] }

        s = self._getState()
        expected = { 'query' : [ str(oid) ],
                     'tois' : { str(oid) : { 'attr1' : ['A1'], 'attr2' : [] }},
                     'order': [ str(oid) ],
                     }
        assert s == expected
        assert self.uptodate()

        # no change
        self.outdate()
        link = Link.LinkSortedQuery(self.clientId, self.linkId)
        link.run()
        self.sync()
        x = self._getResult()
        assert x is None
        assert self.uptodate()

        # update of toi
        self.outdate()
        val = BlobVal('A2')
        toi.attr2 = [val]
        self.pushnewctx()
        link = Link.LinkSortedQuery(self.clientId, self.linkId)
        link.run()
        self.sync()
        x = self._getResult()
        assert x['diffops'] == []
        assert x['error'] is None
        assert len(x['toiDiffs']) == 1
        td = x['toiDiffs'][str(oid)]
        assert td.diffAttrs == {'attr2': [val] }
        assert td.diffAttrs['attr2'][0].references == {self.clientId, link.link['_id']}
        assert self.uptodate()
        assert self.database.blobvals.files.find({'metadata.references.value': [self.clientId]}).count() == 0

        # update of query: new toi
        oid2 = ObjectId()
        val = BlobVal('B2')
        toi2 = self.ctx.createToi(blm.testblm.Test, oid2,
                                  {'attr1':['B1'], 'attr2': [val]})
        self.outdate([oid2])
        self.sync()
        link = Link.LinkSortedQuery(self.clientId, self.linkId)
        link.run()
        x = self._getResult()
        assert x['diffops'] == [[1, 1, [toi2]]]
        assert x['error'] is None
        assert len(x['toiDiffs']) == 1
        td = x['toiDiffs'][str(oid2)]
        assert td.diffAttrs == {'attr1': ['B1'], 'attr2': [val] }
        assert self.uptodate()
        # client still references outdated blobval A2
        assert self.database.blobvals.files.find({'metadata.references.value': [self.clientId]}).count() == 1

        # update: remove toi
        self.ctx.deleteToi(toi2)
        self.outdate([oid2])
        self.sync()
        self.pushnewctx()
        link = Link.LinkSortedQuery(self.clientId, self.linkId)
        link.run()
        x = self._getResult()
        assert x['diffops'] == [[1, 2, []]]
        assert x['error'] is None
        assert len(x['toiDiffs']) == 0
        assert self.uptodate()
        # client still references outdated blobvals A2, B2; JsLink._msg_poll will dereference
        assert self.database.blobvals.files.find({'metadata.references.value': [self.clientId]}).count() == 2

    def test_SortedQuery_rerun_optimization(self, monkeypatch):
        from blm import testblm

        oid1 = ObjectId()
        toi1 = self.ctx.createToi(blm.testblm.Test, oid1, {'name':['foobar']})
        oid2 = ObjectId()
        toi2 = self.ctx.createToi(blm.testblm.Test, oid2, {'name':['gazonk']})
        oid3 = ObjectId()
        toi3 = self.ctx.createToi(blm.testblm.Test, oid3, {'name':['zonka']})
        self.sync()

        query = blm.testblm.Test._query(name=Query.Like('foo*'))

        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria': query,
                                     'attrList': ['name'],
                                     'subscription': True,
                                     'sorting': 'name'})
        link.run()
        r = self._getResult()
        assert list(r['toiDiffs'].keys()) == [str(oid1)] # sanity

        self.ctx.changeToi(toi2, {'name': ['foooo']})
        mongo.update_one(self.database.links,
                         {'client': self.clientId, 'link': self.linkId},
                         {'$set': {'outdatedBy': ObjectId(),
                                   'outdatedToids': [oid2]}})
        self.sync()

        link = Link.LinkFactory().create(None, self.clientId, self.linkId)

        runQuery = self.ctx.runQuery
        queries = []
        def _runQuery(query):
            queries.append(query)
            return runQuery(query)
        monkeypatch.setattr(self.ctx, 'runQuery', _runQuery)
        link.run()
        r = self._getResult()
        assert r['diffops'] == [[1, 1, [toi2]]]
        assert r['toiDiffs'][str(oid2)].diffAttrs == {'name': ['foooo']}

        assert queries == [blm.testblm.Test._query(id=[oid2])] # whitebox

    def test_SortedQuery_rerun_optimization_on_id(self, monkeypatch):
        from blm import testblm

        oid1 = ObjectId()
        toi1 = self.ctx.createToi(blm.testblm.Test, oid1, {'name':['foobar']})
        self.sync()

        query = blm.testblm.Test._query(id=Query.In([str(oid1)]))

        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria': query,
                                     'attrList': ['name'],
                                     'subscription': True,
                                     'sorting': 'name'})
        link.run()
        r = self._getResult()
        assert list(r['toiDiffs'].keys()) == [str(oid1)] # sanity

        self.ctx.changeToi(toi1, {'name': ['foooo']})
        mongo.update_one(self.database.links,
                         {'client': self.clientId, 'link': self.linkId},
                         {'$set': {'outdatedBy': ObjectId(),
                                   'outdatedToids': [oid1]}})
        self.sync()

        link = Link.LinkFactory().create(None, self.clientId, self.linkId)

        runQuery = self.ctx.runQuery
        queries = []
        def _runQuery(query):
            queries.append(query)
            return runQuery(query)
        monkeypatch.setattr(self.ctx, 'runQuery', _runQuery)
        link.run()
        r = self._getResult()
        assert r['diffops'] == []
        assert r['toiDiffs'][str(oid1)].diffAttrs == {'name': ['foooo']}

        assert queries == [blm.testblm.Test._query(id=[oid1])] # whitebox

    def test_SortedQuery_fulltext_update(self):
        py.test.skip('full text index disabled for now')
        from blm import testblm
        with commit.CommitContext(self.database, user=self.user) as ctx:
            ctx.setMayChange(True)
            oid1 = ObjectId()
            toi1 = ctx.createToi(blm.testblm.Test, oid1, {'name':['foo']})
            ctx.runCommit([])

        self.sync()

        query = blm.testblm.Test._query(id=Query.Fulltext('bar'))

        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria': query,
                                     'attrList': ['name'],
                                     'subscription': True,
                                     'sorting': 'name'})
        link.run()
        self.sync()
        x = self._getResult()
        assert x['diffops'] == []
        toi1, = blm.testblm.Test._query(id=oid1).run()
        with commit.CommitContext(self.database) as ctx:
            ctx.setMayChange(True)
            ctx.changeToi(toi1, {'name': ['foo bar']})
            ctx.runCommit([])
        mongo.update_one(self.database.links,
                         {'client': self.clientId, 'link': self.linkId},
                         {'$set': {'outdatedBy': ObjectId(),
                                   'outdatedToids': [oid1]}})
        self.sync()

        link = Link.LinkFactory().create(None, self.clientId, self.linkId)
        link.run()
        self.sync()
        x = self._getResult()
        assert x['diffops'] == [[0, 0, [toi1]]]

    def test_SortedQuery_updateParameters(self):
        from blm import testblm
        query = blm.testblm.Test._query(name='test_SortedQuery')

        oid1 = ObjectId()
        toi1 = self.ctx.createToi(blm.testblm.Test, oid1,
                                  {'name':['test_SortedQuery'],
                                   'attr1':['A1'], 'attr2':['B1']})
        oid2 = ObjectId()
        toi2 = self.ctx.createToi(blm.testblm.Test, oid2,
                                  {'name':['test_SortedQuery'],
                                   'attr1':['B1'], 'attr2':['A2']})
        self.sync()

        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria': query,
                                     'attrList': ['attr1', 'attr2'],
                                     'subscription' : True,
                                     'sorting': 'attr1'})
        link.run()
        x = self._getResult()
        assert x['diffops'] == [[0, 0, [toi1, toi2]]]

        self.outdate()
        link = Link.LinkSortedQuery(self.clientId, self.linkId)
        link.updateParameters(params={'sorting': 'attr2'})
        x = self._getResult()
        self.sync()
        assert x['diffops'] == [[0, 0, [toi2]], [1, 2, []]]
        assert self.uptodate()

        # Make sure we always get an update, even if the result does not change.
        # The client expects it.
        self.outdate()
        link = Link.LinkSortedQuery(self.clientId, self.linkId)
        link.updateParameters(params={'sorting': 'attr2'})
        x = self._getResult()
        self.sync()
        assert x['diffops'] == []
        assert self.uptodate()
        assert self._getLinkData()['outdatedToids'] == []

    def test_SortedQuery_timing_error(self, monkeypatch):
        # this test is nearly identical to
        # test_SortedQuery_rerun_optimization but sneakily modifies
        # the link's uptodate state while the link is running
        #
        # (it is possible that this is now a strict superset of
        # test_SortedQuery_rerun_optimization and if so we might want
        #  to consider merging them)
        from blm import testblm

        oid1 = ObjectId()
        toi1 = self.ctx.createToi(blm.testblm.Test, oid1, {'name':['foobar']})
        oid2 = ObjectId()
        toi2 = self.ctx.createToi(blm.testblm.Test, oid2, {'name':['gazonk']})
        self.sync()

        query = blm.testblm.Test._query(name=Query.Like('foo*'))

        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria': query,
                                     'attrList': ['name'],
                                     'subscription': True,
                                     'sorting': 'name'})
        link.run()
        r = self._getResult()
        assert list(r['toiDiffs'].keys()) == [str(oid1)] # sanity

        cid1 = ObjectId()
        self.ctx.changeToi(toi2, {'name': ['foooo']})
        mongo.update_one(self.database.links,
                         {'client': self.clientId, 'link': self.linkId},
                         {'$set': {'outdatedBy': cid1,
                                   'outdatedToids': [oid2]}})
        self.sync()

        link = Link.LinkFactory().create(None, self.clientId, self.linkId)

        runQuery = self.ctx.runQuery
        queries = []
        cid2 = ObjectId()
        def _runQuery(query):
            queries.append(query)
            result = list(runQuery(query))
            mongo.update_one(self.database.links,
                             {'client': self.clientId, 'link': self.linkId},
                             {'$set': {'outdatedBy': cid2},
                              '$addToSet': {'outdatedToids': oid1}})
            return result

        monkeypatch.setattr(self.ctx, 'runQuery', _runQuery)
        link.run()
        r = self._getResult()
        assert r['diffops'] == [[1, 1, [toi2]]]
        assert r['toiDiffs'][str(oid2)].diffAttrs == {'name': ['foooo']}

        assert queries == [blm.testblm.Test._query(id=[oid2])] # whitebox

        self.sync()

        assert not self.uptodate()
        assert self._getLinkData()['outdatedBy'] == cid2
        assert set(self._getLinkData()['outdatedToids']) == {oid1, oid2}

        monkeypatch.undo()

        link = Link.LinkFactory().create(None, self.clientId, self.linkId)
        link.run()
        self.sync()
        assert self.uptodate()
        assert self._getLinkData()['outdatedBy'] == None
        assert self._getLinkData()['outdatedToids'] == []

    def test_SortedQuery_do_not_optimize_ancients(self, monkeypatch):
        from blm import testblm

        oid1 = ObjectId()
        toi1 = self.ctx.createToi(blm.testblm.Test, oid1, {'name':['foobar']})
        oid2 = ObjectId()
        toi2 = self.ctx.createToi(blm.testblm.Test, oid2, {'name':['gazonk']})
        oid3 = ObjectId()
        toi3 = self.ctx.createToi(blm.testblm.Test, oid3, {'name':['zonka']})
        self.sync()

        query = blm.testblm.Test._query(name=Query.Like('foo*'))

        link = Link.LinkSortedQuery(self.clientId, self.linkId,
                                    {'criteria': query,
                                     'attrList': ['name'],
                                     'subscription': True,
                                     'sorting': 'name'})
        link.run()
        r = self._getResult()
        assert list(r['toiDiffs'].keys()) == [str(oid1)] # sanity

        self.ctx._query_cache.clear()  # xxx we didn't need to do this in py2
        self.ctx.changeToi(toi2, {'name': ['foooo']})
        mongo.update_one(self.database.links,
                         {'client': self.clientId, 'link': self.linkId},
                         {'$set': {'outdatedBy': ObjectId(),
                                   'outdatedToids': [],
                                   'ancient': True}})
        self.sync()

        link = Link.LinkFactory().create(None, self.clientId, self.linkId)

        runQuery = self.ctx.runQuery
        queries = []
        def _runQuery(query):
            queries.append(query)
            return runQuery(query)
        monkeypatch.setattr(self.ctx, 'runQuery', _runQuery)
        link.run()
        r = self._getResult()
        assert r['diffops'] == [[1, 1, [toi2]]]
        assert r['toiDiffs'][str(oid2)].diffAttrs == {'name': ['foooo']}

        assert queries == [query] # whitebox
