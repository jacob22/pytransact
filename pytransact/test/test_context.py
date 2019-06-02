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

import py.test
import os
from bson.objectid import ObjectId
from pytransact import commit, context
from pytransact.contextbroker import ContextBroker
from pytransact.testsupport import ContextTests

blmdir = os.path.join(os.path.dirname(__file__), 'blm')
import blm
def setup_module(module):
    global blm
    from pytransact import blm
    blm.addBlmPath(blmdir)
    from blm import fundamental, testcommit

def teardown_module(module):
    blm.removeBlmPath(blmdir)
    blm.clear()


class FakeUser(object):
    @classmethod
    def _create(cls, user):
        return user


class TestContext(ContextTests):

    def setup_method(self, method):
        super(TestContext, self).setup_method(method)
        self.user = blm.fundamental.AccessHolder()

    def test_createQuery(self):
        query = self.ctx.createQuery(blm.testcommit.Test, {'name': 'test'})
        assert isinstance(query, context.ServiceQuery)

    def test_query_invisible(self):
        toi = blm.testcommit.Test(name=['test'])
        self.sync()

        cctx = self.pushnewctx(user=self.user)
        r = blm.testcommit.Test._query(name='test').run()
        assert r == []

        q = blm.testcommit.Test._query()
        q.clear()
        r = q.run()
        assert r == []

    def test_query_visible(self):
        toi = blm.testcommit.Test(name=['test'], allowRead=[self.user])
        self.sync()

        cctx = self.pushnewctx(user=self.user)
        r = blm.testcommit.Test._query(name='test').run()
        assert r == [toi]
        assert not r[0]._phantom

    def test_query_with_data_fetching(self):
        blm.testcommit.Test(name=['test'], reorder=['foo', 'bar'], unique=['baz'])
        self.sync()

        cctx = self.pushnewctx()
        query = blm.testcommit.Test._query(name='test')
        query.attrList = {'name', 'reorder', 'toirefmap'}
        toi, = query.run()
        assert toi._attrData == {'name': ['test'], 'reorder': ['foo', 'bar'],
                                 'toirefmap': {}}
        assert not toi._phantom

        query = blm.testcommit.Test._query(name='test')
        query.attrList = {'unique'}
        toi, = query.run()
        assert toi._attrData == {'name': ['test'], 'reorder': ['foo', 'bar'],
                                 'toirefmap': {},
                                 'unique': ['baz']}

        # test that we don't explode on non existing attributes in attrlist
        query = blm.testcommit.Test._query(name='test')
        query.attrList = {'doesnotexist'}
        toi, = query.run()  # don't explode
        assert toi._attrData == {'name': ['test'], 'reorder': ['foo', 'bar'],
                                 'toirefmap': {},
                                 'unique': ['baz']}

    def test_clearTois(self):
        toi = blm.testcommit.Test(name=['test'])
        assert 'name' in toi._attrData
        self.ctx.clearTois()
        assert toi._attrData == {}

    def test_clone(self):
        clone = self.ctx.clone()
        assert id(clone) != id(self.ctx)
        assert type(clone) == type(self.ctx)
        assert clone.user == self.ctx.user
        assert clone.database == self.ctx.database

        class OtherContext(context.ReadonlyContext):
            pass

        clone = OtherContext.clone()
        assert type(clone) == OtherContext
        assert clone.user == self.ctx.user
        assert clone.database == self.ctx.database

        clone = OtherContext.clone(self.ctx)
        assert type(clone) == OtherContext
        assert clone.user == self.ctx.user
        assert clone.database == self.ctx.database

        clone = OtherContext.clone(self.ctx, user=self.user)
        assert type(clone) == OtherContext
        assert clone.user == self.user
        assert clone.database == self.ctx.database

    def test_requestAttribute(self):
        toi = blm.testcommit.Test(name=['foo'], reorder=['bar'], unique=['baz'])
        id = toi.id[0]
        self.sync()
        ctx = self.pushnewctx()

        toi = blm.testcommit.Test._create(id)
        assert toi._phantom  # toi is not known yet

        assert toi.name == ['foo']
        assert toi.toirefmap == {}  # default for maps is a dict, not a list
        assert not toi._phantom  # toi is known

        # toi not in the db and not newly created
        toi = blm.testcommit.Test._create(ObjectId())
        assert toi._phantom  # toi is not known yet
        assert toi.name == []
        assert toi._phantom  # toi is still not known

    def test_requestAttribute_copy_default(self):
        toi1 = blm.testcommit.Test()
        toi2 = blm.testcommit.Test()
        id1, id2 = toi1.id[0], toi2.id[0]
        self.sync()
        ctx = self.pushnewctx()

        toi1, = blm.testcommit.Test._query(id=id1).run()
        name = toi1.name.value
        name.append('foo')
        toi1.name = name
        assert toi1.name == ['foo']  # sanity

        toi2, = blm.testcommit.Test._query(id=id2).run()
        # if we are not careful with *copying* the default value above
        # we may end up with toi2.name == ['foo']
        assert toi2.name == []

    def test_preload(self):
        toi = blm.testcommit.Test(name=['foo'], reorder=['bar'], unique=['baz'])
        id = toi.id[0]
        self.sync()
        ctx = self.pushnewctx()

        toi, = blm.testcommit.Test._query(id=id).run()
        assert not toi._attrData

        toi._preload(['reorder', 'unique'])
        assert not toi._attrData

        assert toi.name.value # load
        assert toi._attrData == {
            'name': ['foo'],
            'reorder': ['bar'],
            'unique': ['baz'],
            }

    def test_setUser(self):
        # Make sure contexts have user objects that are reliable from
        # within the context itself:
        # context.user should give you a TO which is equivalent to the
        # one you'd get from a blm.User query
        # Specifically, we do not want any stale data from an outdated
        # context to linger in the object.
        # Thus, we make sure to always create a fresh, context specific
        # copy of the user TO in setUser().
        user = blm.testcommit.User(name=['foo'])
        user.allowRead = [user]
        self.sync()

        ctx = self.pushnewctx(ContextClass=commit.CommitContext, user=user)
        user = ctx.user

        user.name = ['not commited!']

        with self.pushnewctx(user=user) as newctx:
            assert newctx.user.id == user.id
            assert newctx.user is not user
            assert newctx.user.name == ['foo']
            assert newctx.user.name != ['not commited!']
            assert newctx.user in newctx.__instances__


class TestMaybeWithContext(object):

    def test_with_no_context(self):
        py.test.raises(Exception, lambda: ContextBroker().context)  # sanity
        database = object()
        @context.maybe_with_context()
        def foo(arg):
            assert isinstance(ContextBroker().context, context.ReadonlyContext)
            assert ContextBroker().context.database is database
            return arg
        obj = object()
        assert foo(obj, database=database) is obj

    def test_with_factory(self):
        class MyContext(context.ReadonlyContext):
            pass
        @context.maybe_with_context(MyContext)
        def foo():
            assert isinstance(ContextBroker().context, MyContext)
        foo(database=object())

    def test_with_correct_context_class(self):
        @context.maybe_with_context()
        def foo():
            return ContextBroker().context

        with context.ReadonlyContext(object()) as ctx:
            assert foo() is ctx

        class MyContext(context.ReadonlyContext):
            pass

        with MyContext(object()) as ctx:
            assert foo() is ctx

        class WeirdContext(object):
            database = object()
            user = FakeUser()
            def __enter__(self):
                ContextBroker().pushContext(self)
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                assert ContextBroker().context == self
                ContextBroker().popContext()

        with WeirdContext():
            ctx = foo()
            assert isinstance(ctx, context.ReadonlyContext)
            assert ctx.database is WeirdContext.database

    def test_with_custom_context_class(self):
        class MyContext(context.ReadonlyContext):
            pass

        database = object()
        user = FakeUser()

        @context.maybe_with_context(MyContext)
        def foo():
            return ContextBroker().context

        with context.ReadonlyContext(database, user):
            ctx = foo()
            assert isinstance(ctx, MyContext)
            assert ctx.database is database
            assert ctx.user is user

    def test_no_database(self):
        @context.maybe_with_context()
        def foo():
            return ContextBroker().context

        py.test.raises(ValueError, foo)
