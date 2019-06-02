# -*- coding: utf-8 -*-

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
import pymongo
import pytransact.testsupport
from pytransact.testsupport import DBTests, Fake

from .. import mongo


class TestToi(DBTests):

    def test_save_toi(self):
        class TO(object):
            _fullname = 'TO'
        class Foo(TO):
            _fullname = 'test.Foo'
        class Bar(Foo):
            _fullname = 'test.Bar'
        class Baz(Bar):
            _fullname = 'test.Baz'

        toid = bson.objectid.ObjectId()
        mongo.save_toi(self.database, toid, Baz, {})
        self.sync()

        bases = ['test.Baz', 'test.Bar', 'test.Foo', 'TO']
        assert self.database.tois.find_one({'_id': toid})['_bases'] == bases

    def test_remove_tois(self):
        toid1 = mongo.insert(self.database.tois, {})
        toid2 = mongo.insert(self.database.tois, {})

        mongo.remove_tois(self.database, [toid1, toid2])
        self.sync()

        assert not mongo.find_one(self.database.tois, {'_id': toid1})
        assert not mongo.find_one(self.database.tois, {'_id': toid2})


class TestMaintenance(DBTests):

    def test_ensure_indexes(self):
        mongo.ensure_indexes(self.database)

        # whitebox wrt mongodb's automatic index names
        assert '_toc_1' in self.database.tois.index_information()
        assert '_bases_1' in self.database.tois.index_information()
        assert 'allowRead.id_1' in self.database.tois.index_information()

        assert 'timestamp_1' in self.database.clients.index_information()
        assert 'metadata.references.value_1' in \
            self.database.blobvals.files.index_information()

    # Also consider porting obsolete attribute/toc cleanup method from Eutaxia


class TestBasics(DBTests):

    def test_insert_and_find(self):
        coll = self.database.foo
        doc_id = mongo.insert(coll, {'foo': 'bar'})
        cursor = mongo.find(coll, {'foo': 'bar'})
        assert cursor.count() == 1
        assert next(cursor) == {'_id': doc_id, 'foo': 'bar'}
