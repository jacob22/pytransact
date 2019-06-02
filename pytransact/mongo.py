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

import bson
import collections
import contextlib
import copy
import functools
import logging
import pymongo
import pymongo.errors
import pymongo.operations
import pymongo.uri_parser
import pytransact.custombson  # Make sure bson._cbson is unloaded
import time

log = logging.getLogger('pytransact.mongo')


def connect(*args, **kw):
    kw.setdefault('read_preference', pymongo.ReadPreference.NEAREST)
    kw.setdefault('w', 'majority')
    kw.setdefault('fsync', True)
    return pymongo.MongoClient(*args, **kw)


def retry(func):
    @functools.wraps(func)
    def retry(*args, **kw):
        for wait in (0.0, 0.1, 0.5, 1, 2, 5, 5, 5, 5):
            try:
                return func(*args, **kw)
            except pymongo.errors.AutoReconnect as exc:
                log.error('AutoReconnect cought, retrying.')
                time.sleep(wait)
        raise exc
    return retry


@retry
def bulk_write(collection, *args, **kw):
    return collection.bulk_write(*args, **kw)


@retry
def delete_one(collection, spec):
    log.debug('delete_one: %s', spec)
    return collection.delete_one(spec)


@retry
def delete_many(collection, spec):
    log.debug('delete_many: %s', spec)
    return collection.delete_many(spec)


@retry
def find(collection, query={}, *args, **kw):
    log.debug('find: %s', query)
    return collection.find(query, *args, **kw)


@retry
def find_and_modify(collection, query={}, *args, **kw):
    log.debug('find_and_modify: %s', query)
    return collection.find_and_modify(query, *args, **kw)


@retry
def find_one(collection, query={}, *args, **kw):
    log.debug('find_one: %s', query)
    return collection.find_one(query, *args, **kw)


@retry
def insert(collection, doc_or_docs, *args, **kw):
    log.debug('insert: %s', doc_or_docs)
    return collection.insert(doc_or_docs, *args, **kw)


@retry
def replace_one(collection, spec, document, *args, **kw):
    log.debug('replace_one: %s -> %s', spec, document)
    return collection.replace_one(spec, document, *args, **kw)


@retry
def update(collection, spec={}, document={}, *args, **kw):
    log.debug('update: %s -> %s', spec, document)
    return collection.update(spec, document, *args, **kw)


@retry
def update_many(collection, filter={}, update={}, upsert=False):
    log.debug('update_many: %s -> %s', filter, update)
    return collection.update_many(filter, update, upsert)


@retry
def update_one(collection, filter={}, update={}, upsert=False):
    log.debug('update_one: %s -> %s', filter, update)
    return collection.update_one(filter, update, upsert)


@retry
def remove(collection, spec, *args, **kw):
    log.debug('remove: %s', spec)
    return collection.remove(spec, *args, **kw)


_always_fetch = frozenset(['_id', '_toc'])
def run_query(database, query, projection=set(), collection='tois'):
    log.debug('Running query: %r (%s)', query, projection)
    return find(database[collection], query, projection=list(projection | _always_fetch))


def load_toi(database, toid, attributes, collection='tois'):
    from . import blm
    doc = find_one(database[collection], {'_id': toid},
                   projection=list(attributes | _always_fetch))

    # consider merging this code with the one in context.py that's identical
    toc = blm.getTocByFullname(doc.pop('_toc'))
    id = doc.pop('_id')
    for attrName in attributes:
        if attrName not in doc and attrName in toc._attributes:
            doc[attrName] = copy.copy(toc._attributes[attrName].default)
    toi = toc._create(id, kw=doc)
    return toi


def save_toi(database, toid, toc, attrData, collection='tois'):
    data = copy.copy(attrData)
    data['_toc'] = toc._fullname
    data['_bases'] = [t._fullname for t in toc.__mro__[:-1]] # exclude object
    spec = {'_id': toid}
    document = {'$set': data}

    log.debug('Saving %r: %r', spec, document)
    result = update_one(database[collection], spec, document, upsert=True)
    log.debug('  Result: %r', result)
    return result


def bulk_save_toi(toid, toc, attrData):
    data = copy.copy(attrData)
    data['_toc'] = toc._fullname
    data['_bases'] = [t._fullname for t in toc.__mro__[:-1]] # exclude object
    spec = {'_id': toid}
    document = {'$set': data}
    operation = pymongo.operations.UpdateOne(spec, document, upsert=True)
    log.debug('Saving TOI: %s', operation)
    return operation


def remove_tois(database, tois, collection='tois'):
    toids = []
    for toi in tois:
        try:
            id = toi.id[0]
        except AttributeError:
            id = toi
        assert isinstance(id, bson.objectid.ObjectId)
        toids.append(id)

    log.debug('Removing TOIS: %s', toids)
    remove(database[collection], {'_id': {'$in': toids}})


def bulk_remove_tois(tois):
    toids = []
    for toi in tois:
        try:
            id = toi.id[0]
        except AttributeError:
            id = toi
        assert isinstance(id, bson.objectid.ObjectId)
        toids.append(id)

    log.debug('Removing TOIS: %s', toids)
    return pymongo.operations.DeleteMany({'_id': {'$in': toids}})


# database maintenance and configuration

@retry
def ensure_indexes(database):
    index_map = [
        (database.tois, '_toc _bases allowRead.id'),
        (database.clients, 'timestamp'),
        (database.blobvals.files, 'metadata.references.value'),
        ]
    for collection, indexes in index_map:
        for index in indexes.split():
            log.info('Ensuring index in %s: %s', collection.name, index)
            collection.ensure_index(index)
