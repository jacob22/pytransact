# Copyright (c) Open End AB 2011-2013, All rights reserved
import collections, logging, pymongo, time
from bson.objectid import ObjectId
from pytransact import (blm, commit, diff, difftoi, exceptions,
                        iterate, mongo, query)
from pytransact.commit import concat_values
from pytransact.contextbroker import ContextBroker
from pytransact.object import attribute as Attribute
from functools import reduce

log = logging.getLogger(__name__)
opt_log = logging.getLogger('sortedquery.optimization')


class Link(object):

    _links = collections.defaultdict(int) # for debugging
    _link = object()

    def __init__(self, clientId, linkId, params=None):
        self._links[(clientId, linkId)] += 1 # for debugging
        self.clientId = clientId
        self.linkId = linkId
        self.params = params
        log.debug('Creating %s', self)

    def __del__(self):
        # for debugging only
        key = self.clientId, self.linkId
        self._links[key] -= 1
        if self._links[key] <= 0:
            del self._links[key]

    def __str__(self):
        return '%s: %s, %s %s' % (self.__class__.__name__,
                                  self.clientId, self.linkId,
                                  self._links[self.clientId, self.linkId])

    @property
    def context(self):
        return ContextBroker().context

    @property
    def database(self):
        return self.context.database

    def _getLink(self):
        if self._link is Link._link:
            self._link = mongo.find_one(self.database.links,
                                        {'client': self.clientId,
                                         'link': self.linkId,
                                         'type': self.__class__.__name__})
        if not self._link:
            self._link = {'_id': ObjectId()}
        return self._link

    def _setLink(self, link):
        self._link = link
    link = property(_getLink, _setLink)

    def run(self):
        log.debug('Running %s', self)
        state = self.link.get('state')
        if self.params is None:
            self.params = self.link.get('params')
        self._run(self.params, state)

    def update(self, data, persistent=False, updatelink=lambda x: None):
        if data is not None:
            update = {'type': 'update',
                      'id': self.linkId,
                      'args': data }
            document = {'$push': {'updates': update}}
            log.debug('Updating %s: %r', self, update)
            mongo.update_one(self.database.clients, {'_id': self.clientId},
                             document)

        if persistent:
            outdatedBy = self.link.get('outdatedBy', None)
            spec = {'client': self.clientId,
                    'link': self.linkId,
                    'outdatedBy': outdatedBy}
            doc = {'$set': {'outdatedBy': None,
                            'timestamp': time.time(),
                            'ancient': False}}
            updatelink(doc)
            mongo.update_one(self.database.links, spec, doc)

    def save(self, params, state):
        log.debug('Saving %s: %r', self, state)

        user = self.context.user
        if user:
            allowRead = [toi.id[0] for toi in user._privileges]
        else:
            allowRead = []

        spec = {'_id': self.link['_id'],
                'client': self.clientId,
                'link': self.linkId}
        document = {'$set':{'params': params,
                            'state': state,
                            'allowRead': allowRead,
                            'type': self.__class__.__name__,
                            'timestamp': time.time(),
                            'ancient': False
                            }
                    }

        mongo.update_one(self.database.links, spec, document, upsert=True)
        self._link = Link._link

    def remove(self):
        log.debug('Removing %s', self)
        mongo.delete_one(self.database.links,
                         {'client': self.clientId, 'link': self.linkId})


class LinkCallMethod(Link):

    def _run(self, params, state):
        me = self.clientId, self.linkId
        if state is None:
            toid = params.get('toid')
            blmName = params.get('blmName')
            if not (bool(toid) ^ bool(blmName)): # XOR
                raise RuntimeError("Only one of toid or blmName "
                                   "may be supplied.")

            if toid is not None and not isinstance(toid, blm.TO):
                toid = ObjectId(toid)

            methodName = params['methodName']
            args = params['args']
            self.save(None, 'processing') # arg only used in tests

            if toid:
                op = commit.CallToi(toid, methodName, args)
            else:
                op = commit.CallBlm(blmName, methodName, args)
            self.context.runCommit([op], interested=me)

        done = commit.Commit.fromquery(self.database,
                                       {'handled_by': me, 'state': 'done'})

        if done:
            error = done.error
            if error is None:
                result = done.results[0]
            else:
                result = []

            for obj in iterate.walk(result):
                if isinstance(obj, Attribute.BlobVal):
                    obj.addref(self.clientId)

            self.update({'result': result, 'error': error})
            done.delete(self.database)
            self.remove()


class LinkRequest(Link):

    def __str__(self):
        params = self.params or {}
        toid = params.get('toid')
        attrList = params.get('attrList')
        return '%s: %s, %s %s %s %s' % (
            self.__class__.__name__, self.clientId, self.linkId,
            self._links[self.clientId, self.linkId], toid, attrList)

    def _run(self, params, state):
        toid = params['toid'] = ObjectId(params.get('toid'))
        query = blm.TO._query(id=toid)
        query.attrList = attrList = params['attrList']
        toi = query.run()
        if not toi:
            self.update({'error' : exceptions.ToiNonexistantError(
                        'TO', toid)})
            return

        persistent = params.get('subscription', False)

        toi, = toi
        diffOb = difftoi.DiffTOI()
        attrData = dict([(x, getattr(toi, x).value) for x in attrList])

        diffOb.setDiff(toi.__class__, str(toi.id[0]),
                       state or {}, {}, attrData, {})

        for value in reduce(concat_values, iter(diffOb.diffAttrs.values()), []):
            if isinstance(value, Attribute.BlobVal):
                value.addref(self.clientId)
                if persistent:
                    value.addref(self.link['_id'])

        for value in reduce(concat_values, iter(diffOb.orgAttrs.values()), []):
            if isinstance(value, Attribute.BlobVal):
                value.addref(self.clientId)
                value.delref(self.link['_id'])

        # if this is the first update (state is None) or there's new data...
        update = None
        if state is None or diffOb.diffAttrs:
            update = {'toiDiff': diffOb, 'error': None}
        self.update(update, persistent=persistent)
        if persistent:
            self.save(params, attrData)


class LinkQuery(Link):

    def _run(self, params, state):
        tois = ContextBroker().runQuery(params['criteria'])
        result = dict([(str(t.id[0]), t._fullname) for t in tois])
        update = None
        if result != state:
            state = state or {}
            added = dict(item for item in list(result.items())
                         if item not in list(state.items()))
            deleted = dict(item for item in list(state.items())
                           if item not in list(result.items()))
            update = { 'add' : added,
                       'del' : deleted,
                       'relevance' : None,
                       'error' : None }
        persistent = params.get('subscription')
        self.update(update, persistent=persistent)
        if persistent:
            self.save(params, result)


class LinkSortedQuery(Link):

    def __str__(self):
        query = (self.params or {}).get('criteria')
        return '%s: %s, %s %s %s' % (self.__class__.__name__,
                                     self.clientId, self.linkId,
                                     self._links[self.clientId, self.linkId],
                                     query)

    def updateParameters(self, params):
        log.debug('Updating %s', self)
        state = self.link['state']
        # this should effectively force an update, as the recalculated result
        # will never contain this parameter
        state['update'] = True
        params = dict(list(self.link['params'].items()) + list(params.items()))
        self._run(params, state)

    def _run(self, params, state):
        criteria = params['criteria']
        sorting = params.get('sorting')
        clientAttrs = set(params.get('attrList', []))
        neededAttrs = set(clientAttrs)

        if sorting:
            sortingAttrs = getattr(criteria.toc, 'sort_%s_attrs' % sorting, [])
            neededAttrs.update(sortingAttrs)

        for attrName in neededAttrs.copy():
            attr = getattr(criteria.toc, attrName)
            extra = getattr(attr, 'extraAttrs', [])
            neededAttrs.update(extra)

        criteria.attrList = neededAttrs

        changedIds =  self.link.get('outdatedToids', [])
        optimize = bool(state and
                        not criteria.hasSubQuery() and
                        not criteria.hasFulltext() and
                        (changedIds or 'update' in state))
        start = time.time()
        if optimize:
            log.debug('Using optimized sorted query recalculation for %s', self)
            tois = set()
            for toid, attrData in state['tois'].items():
                toi = criteria.toc._create(toid, kw=attrData) # xxx
                tois.add(toi)

            if changedIds:
                changedToisQuery = query.Query(criteria.toc, id=changedIds)
                changedToisQuery.attrList = neededAttrs
                changedTois = ContextBroker().runQuery(changedToisQuery)
            else:
                changedTois = []

            def getter(toi, attr):
                if attr == 'id':
                    return list(map(str, toi.id))
                value = getattr(toi, attr.name).value
                if isinstance(attr, Attribute.ToiRef):
                    # Code fighting code:
                    # The conds for toiref attributes will contain
                    # sets of unicode strings representing toids.
                    # The .matches() of respective operators mostly
                    # use set matching operators, so we need to spit
                    # out objects with the same hash as what's in the
                    # conds - thus TOIs themselves are no good.
                    value = [str(toi.id[0]) for toi in value]
                return value

            unhandled = set(changedIds)
            for changedToi in changedTois:
                unhandled.discard(changedToi.id[0])
                if criteria.matches(changedToi, getter):
                    tois.add(changedToi)
                else:
                    tois.discard(changedToi)
            # toids left in unhandled have been removed
            tois = [toi for toi in tois if toi.id[0] not in unhandled]
        else:
            tois = ContextBroker().runQuery(criteria)
        end = time.time()
        _state = state or {}
        opt_log.debug('%s %f %d %d %s %s', optimize, end - start,
                      len(_state.get('tois', [])), len(tois),
                      bool(self.link.get('outdatedBy')), self)

        if sorting:
            sorter = getattr(criteria.toc, 'sort_'+sorting+'_key')
            #import pdb;pdb.set_trace()
            tois.sort(key=sorter)

        persistent = params.get('subscription')

        toiDiffs = {}
        state = state or { 'query': [], 'tois' : {} }
        result = { 'query' : tois, 'tois' : {}, 'order': [] }
        newBlobVals = set()
        for toi in tois:
            diffOb = difftoi.DiffTOI()
            clientData = dict((x, getattr(toi, x).value) for x in clientAttrs)
            neededData = dict((x, getattr(toi, x).value) for x in neededAttrs)
            toid = str(toi.id[0])
            result['tois'][toid] = neededData
            result['order'].append(toid)

            diffOb.setDiff(toi.__class__, toid,
                           state['tois'].get(toid,{}), {}, clientData, {})
            if diffOb.diffAttrs:
                toiDiffs[toid] = diffOb

                for value in reduce(concat_values,
                                    iter(diffOb.diffAttrs.values()), []):
                    if isinstance(value, Attribute.BlobVal):
                        value.addref(self.clientId)
                        newBlobVals.add(value)

                for value in reduce(concat_values,
                                    iter(diffOb.orgAttrs.values()), []):
                    if isinstance(value, Attribute.BlobVal):
                        value.addref(self.clientId)

        oldBlobVals = set()
        for toid, attrData in state['tois'].items():
            for value in reduce(concat_values, iter(attrData.values()), []):
                if isinstance(value, Attribute.BlobVal):
                    oldBlobVals.add(value)

        for blobVal in newBlobVals - oldBlobVals:
            blobVal.addref(self.link['_id'])

        for blobVal in oldBlobVals - newBlobVals:
            blobVal.delref(self.link['_id'])

        update = None
        if state != result:
            log.debug('Updating %s', self)
            diffops = diff.diff_opcodes(state['query'], tois)
            update = { 'diffops' : diffops,
                       'toiDiffs' : toiDiffs,
                       'error': None }
        def updatelink(doc):
            doc.setdefault('$set', {})['outdatedToids'] = []
        self.update(update, persistent, updatelink)
        if persistent:
            self.save(params, result)


class LinkFactory(object):
    CallMethod = LinkCallMethod
    Request = LinkRequest
    Query = LinkQuery
    SortedQuery = LinkSortedQuery

    @property
    def _links(self):
        return ContextBroker().context.database.links

    def create(self, _name, _clientid, _linkid, **kw):
        if _name:
            return getattr(self, _name)(_clientid, _linkid, **kw)

        doc = mongo.find_one(self._links, {'client': _clientid,
                                           'link': _linkid})
        if doc:
            link = globals()[doc['type']](_clientid, _linkid, **kw)
            link.link = doc
            return link

    def iter(self, query={}):
        for doc in mongo.find(self._links, query):
            cls = globals()[doc['type']]
            link = cls(doc['client'], doc['link'], params=doc.get('params'))
            link.link = doc
            yield link
