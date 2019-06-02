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

import os
from bson.objectid import ObjectId
import socket, pymongo, pymongo.helpers
from .. import mongo, utils, testsupport
import blm

def setup_module(mod):
    from blm import fundamental
    mod.blm = blm
    blm.addBlmPath(os.path.join(os.path.dirname(__file__), 'blm'))
    from blm import testblm


def teardown_module(mod):
    blm.removeBlmPath(os.path.join(os.path.dirname(__file__), 'blm'))
    blm.clear()


def test_score_node():
    for score in utils.score_node('localhost', 22):
        assert score[0] == 10
    assert score # at least one match
    score = None

    for score in utils.score_node('google.com', 80):
        assert 0 <= score[0] < 1
    assert score

    assert utils.score_node('no.such.address', -1) == []


def test_shuffle_is_sort(monkeypatch):
    hosts = [('host1', 27017), ('host2', 27017),
             ('badname', 27017), ('local', 27017)]
    def getaddrinfo(hostname, port):
        if hostname == 'badname':
            raise IOError
        ipno = {'host1': '1.2.3.4',
                'host2': '2.3.4.5',
                'local': '9.9.9.9',
                }[hostname]
        return [(None, None, None, None, (ipno, port))]
    class Socket(object):
        def __init__(self, family, socktype, proto):
            pass
        def connect(self, sockaddr):
            self.sockaddr = sockaddr
        def getpeername(self):
            return self.sockaddr
        def getsockname(self):
            return ('2.3.4.5', 12345)
        def bind(self, xxx_todo_changeme):
            (host, port) = xxx_todo_changeme
            if host != '9.9.9.9':
                raise IOError

    monkeypatch.setattr(socket, 'getaddrinfo', getaddrinfo)
    monkeypatch.setattr(socket, 'socket', Socket)

    res = utils.shuffle_is_sort(hosts)
    assert res == [('local', 27017), ('host2', 27017), ('host1', 27017)]

    monkeypatch.undo()

    res = utils.shuffle_is_sort(hosts)
    assert res == [('local', 27017), ('host2', 27017), ('host1', 27017)]


def test_count_db_calls():
    class FakeDatabase(object):
        pass
    class FakeCollection(object):
        def find(self, *args, **kw):
            return {}
        find_and_modify = find
        find_one = find
        insert = find
        update = find
        remove = find

    db = FakeCollection()
    db.coll = coll = FakeCollection()

    find = mongo.find

    time = testsupport.Time()

    with utils.count_db_calls() as c:
        assert c.start == time
        assert mongo.find.__name__ == 'find'
        assert c.find == 0
        mongo.find(coll)
        assert c.find == 1
        mongo.find(coll)
        assert c.find == 2

        mongo.find_and_modify(coll)
        assert c.find_and_modify == 1
        time.step()

    assert c.stop == time
    assert mongo.find is find

    # don't explode:
    str(c)
    repr(c)

    assert eval(repr(c))['time'] == 1 # repr includes timing info


class Test_update_bases(testsupport.BLMTests):

    def setup_method(self, method):
        super(Test_update_bases, self).setup_method(method)
        self.toid1, self.toid2 = [ObjectId() for x in range(2)]
        mongo.save_toi(self.database, self.toid1, blm.testblm.Sub, {})
        mongo.save_toi(self.database, self.toid2, blm.testblm.Sub, {})
        mongo.update_one(self.database.tois, {'_id': self.toid2},
                         { '$set' : { '_bases' : [ 'testblm.Sub', 'TO' ] } } )
        self.sync()

    def test_simple(self):
        tois = blm.testblm.Sub._query().run()
        assert len(tois) == 2
        base = blm.testblm.Base._query().run()
        assert len(base) == 1
        utils.update_bases(self.database, blm.testblm.Sub)
        self.sync()
        self.pushnewctx()
        tois = blm.testblm.Sub._query().run()
        assert len(tois) == 2
        base = blm.testblm.Base._query().run()
        assert len(base) == 2


class Test_initiate_default_values(testsupport.BLMTests):

    def setup_method(self, method):
        super(Test_initiate_default_values, self).setup_method(method)
        self.toid1, self.toid2 = [ObjectId() for x in list(range(2))]
        mongo.save_toi(self.database, self.toid1, blm.testblm.Defaults, {})
        mongo.save_toi(self.database, self.toid2, blm.testblm.Defaults, {
                'no_default': [1],
                'empty_default': [2],
                'has_default': [3],
                })
        self.sync()

    def test_data(self):
        utils.initiate_default_values(
            self.database, blm.testblm.Defaults,
            'no_default', 'empty_default', 'has_default')
        self.sync()

        toi, = blm.testblm.Defaults._query(no_default=None).run()
        assert toi.id[0] == self.toid1

        toi, = blm.testblm.Defaults._query(empty_default=None).run()
        assert toi.id[0] == self.toid1

        assert not blm.testblm.Defaults._query(has_default=None).run()

        toi, = blm.testblm.Defaults._query(empty_default=None).run()
        assert toi.id[0] == self.toid1

        toi, = blm.testblm.Defaults._query(has_default=42).run()
        assert toi.id[0] == self.toid1

        toi, = blm.testblm.Defaults._query(has_default=3).run()
        assert toi.id[0] == self.toid2

        toi1, = blm.TO._query(id=self.toid1).run()
        toi2, = blm.TO._query(id=self.toid2).run()

        assert toi1.no_default == []
        assert toi1.empty_default == []
        assert toi1.has_default == [42]

        assert toi2.no_default == [1]
        assert toi2.empty_default == [2]
        assert toi2.has_default == [3]
