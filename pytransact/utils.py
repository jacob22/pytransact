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

import collections
import functools
import pymongo
import socket
import time

from . import mongo

# used to compare ip addresses - it is content agnostic :)
from os.path import commonprefix

def score_node(host, port):
    peers = []
    try:
        addresses = socket.getaddrinfo(host, port)
    except IOError:
        return []

    for family, socktype, proto, _, sockaddr in addresses:
        try:
            sock = socket.socket(family, socktype, proto)
            try:
                sock.bind((sockaddr[0], 0)) # Is this address local?
            except IOError:
                pass # Not local
            else:
                # Yep, local. Make sure it sorts early
                peers.append((10, (host, port)))
                continue
            sock.connect(sockaddr)
        except IOError:
            continue
        peerip = sock.getpeername()[0]
        sockip = sock.getsockname()[0]
        common = commonprefix([peerip, sockip])
        score = float(len(common)) / float(len(peerip))
        peers.append((score, (host, port)))
    return peers


def shuffle_is_sort(hosts, cache={}):
    """
    A ReplicaSetConnection should try to use the same secondary all
    the time, and preferably localhost, or at least the "closest"
    node.
    """
    cache_key = tuple(hosts)
    if cache_key in cache:
        return cache[cache_key]
    else:
        cache.clear()
    peers = set()
    for host, port in hosts:
        peers.update(score_node(host, port))
    peers = sorted(peers, reverse=True)
    cache[cache_key] = [peer[1] for peer in peers]
    return cache[cache_key]


def is_localhost_primary(connection):
    if not isinstance(connection, (pymongo.MongoReplicaSetClient,
                                   pymongo.MongoClient)):
        return False

    connection.local.foo.find().count()

    if connection.primary is None:
        return False
    for score, peer in score_node(*connection.primary):
        if score >= 1:
            return True
    return False


def update_bases(database, toc):
    """Update tois _bases to reflect actual hierarchy"""

    query = {'_bases': {'$in': [toc._fullname]} }
    document = {'$set': {'_bases':  [t._fullname for t in toc.__mro__[:-1]] }}
    mongo.update_many(database.tois, query, document)
    err = database.command('getLastError',
                           read_preference=pymongo.ReadPreference.PRIMARY)
    assert not err['err'], err


def initiate_default_values(database, toc, *attrNames, **kw):
    """Set the attributes' default value to TOIs that lack data for
    attributes in `attrNames'. Use when adding a new attribute with a
    default value to a TOC that already has existing TOIs in the
    database."""
    from pytransact.queryops import Empty
    collection = database[kw.get('collection', 'tois')]

    for attrName in attrNames:
        attr = toc._attributes[attrName]
        default = attr.default
        if not default:
            continue # is this right?
        query = {'_bases': {'$in': [toc._fullname]},
                 attrName: {'$in': [None, [], {}]}}
        document = {'$set': {attrName: default}}
        mongo.update_many(collection, query, document)
        err = database.command('getLastError')
        assert not err['err'], err


class count_db_calls(collections.defaultdict):
    """Context manager for counting DB calls, for debugging/performance
    evaluation purposes."""

    def __init__(self, loglevel=None):
        super(count_db_calls, self).__init__(int)
        self.orig = {}
        self.loglevel = loglevel

    def __getattr__(self, attr):
       return self[attr]

    def __enter__(self):
        self.replace('bulk_write')
        self.replace('find')
        self.replace('find_and_modify')
        self.replace('find_one')
        self.replace('insert')
        self.replace('update')
        self.replace('update_many')
        self.replace('update_one')
        self.replace('remove')
        self.start = time.time()
        if self.loglevel is not None:
            loglevel, self.loglevel = self.loglevel, mongo.log.level
            mongo.log.setLevel(loglevel)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop = time.time()
        self.restore()

    def _copy(self):
        copy = dict(self)
        copy['time'] = self.stop - self.start
        return copy

    def __str__(self):
        return str(self._copy())

    def __repr__(self):
        return repr(self._copy())

    def replace(self, funcname):
        self.orig[funcname] = func = getattr(mongo, funcname)
        @functools.wraps(func)
        def wrapper(*args, **kw):
            self[funcname] += 1
            return func(*args, **kw)
        setattr(mongo, funcname, wrapper)

    def restore(self):
        while self.orig:
            funcname, func = self.orig.popitem()
            setattr(mongo, funcname, func)
        if self.loglevel is not None:
            mongo.log.setLevel(self.loglevel)
