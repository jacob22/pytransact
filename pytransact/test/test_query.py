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

import copy, os, re
import py.test
import pytransact.query as q
from pytransact.object.model import *
from pytransact.testsupport import BLMTests

import blm

blmpath = os.path.join(os.path.dirname(__file__), 'blm')


def setup_module(mod):
    blm.addBlmPath(blmpath)


def teardown_module(mod):
    blm.removeBlmPath(blmpath)
    blm.clear()


def test_Less():
    "Test Less operator"
    
    assert q.Less(42).matches([41, 99])
    assert not q.Less(42).matches([42])
    assert not q.Less(42).matches([88, 998])

def test_Greater():
    "Test Greater operator"
    
    assert q.Greater(42).matches([41, 43])
    assert not q.Greater(42).matches([39, 40])

def test_LessEq():
    "Test LessEq operator"
    
    assert q.LessEq(42).matches([42, 43])
    assert q.LessEq(42).matches([42])
    assert not q.LessEq(42).matches([49, 43])

def test_GreaterEq():
    "Test GreaterEq operator"
    
    assert q.GreaterEq(42).matches([39, 42])
    assert not q.GreaterEq(42).matches([39, 41])

def test_Between():
    "Test Between operator"
    
    assert q.Between(30, 40).matches([29, 30, 42])
    assert q.Between(30, 40).matches([29, 40, 42])
    assert q.Between(30, 40).matches([29, 37, 42])
    assert not q.Between(30, 40).matches([29, 42])

def test_In():
    "Test In operator"
    
    assert q.In([41, 42]).matches([1, 2, 41])
    assert q.In([41, 42]).matches([1, 2, 42])
    assert not q.In([41, 42]).matches([1, 2, 43])

def test_NotIn():
    "Test NotIn operator"
    
    assert q.NotIn([41, 42]).matches([1, 2, 3, 41])
    assert q.NotIn([41, 42]).matches([1, 42, 3])
    assert not q.NotIn([41, 42]).matches([41, 42, 41])

def test_InSingleValue():
    "Test In operator for single values"
    
    assert q.In(42).matches([42])
    assert not q.In(42).matches([41])

def test_NotInSingleValue():
    "Test In operator for single values"
    
    assert q.NotIn(42).matches([41, 42])
    assert not q.NotIn(42).matches([42])

class TestGlob(object):
    "Test the 'glob'-like regexp replacer"

    def test_asteriskLA(self):
        "Test asterisk left anchored glob"

        r = q.makeRe('foo*')
        assert r.search('foobar')
        assert r.search('foofoo')
        assert r.search('foo')
        assert not r.search('foxo')
        assert not r.search('barfoo')

    def test_asteriskRA(self):
        "Test asterisk right anchored glob"

        r = q.makeRe('*foo')
        assert not r.search('foobar')
        assert r.search('foofoo')
        assert r.search('foo')
        assert not r.search('foxo')
        assert r.search('barfoo')

    def test_asteriskBoth(self):
        "Test asterisk none anchored glob"

        r = q.makeRe('*foo*')
        assert r.search('foobar')
        assert r.search('foofoo')
        assert r.search('foo')
        assert not r.search('foxo')
        assert r.search('barfoo')

    def test_asteriskMiddle(self):
        "Test asterisk both anchored glob"

        r = q.makeRe('foo*bar')
        assert r.search('foobar')
        assert not r.search('foofoo')
        assert not r.search('foo')
        assert not r.search('foxo')
        assert not r.search('barfoo')
        assert r.search('foomoobar')
        assert not r.search('ofofoomoobar')
        assert not r.search('foomoobarofo')

    def test_Question(self):
        "Test question glob"

        r = q.makeRe('foo?bar')
        assert r.search('foombar')
        assert not r.search('fooxmbar')
        assert not r.search('foobar')

    def test_EscapeAst(self):
        "Test escape asterisk glob"

        r = q.makeRe(r'foo\*bar')
        assert r.search('foo*bar')
        assert not r.search('fooxbar')
        assert not r.search('foobar')

    def test_EscapeQuestion(self):
        "Test escape question glob"

        r = q.makeRe(r'foo\?bar')
        assert r.search('foo?bar')
        assert not r.search('fooxbar')
        assert not r.search('foobar')

    def test_EscapeEscape(self):
        "Test escape escape glob"

        r = q.makeRe(r'foo\\bar')
        assert r.search(r'foo\bar')
        assert not r.search('fooxbar')
        assert not r.search('foobar')

    def test_EscapeEscapeEscape(self):
        "Test escape escape escape glob"

        r = q.makeRe(r'foo\\\tbar')
        assert r.search(r'foo\tbar')
        assert not r.search('fooxbar')
        assert not r.search('foobar')

    def test_EscapeOther(self):
        "Test escape anything glob"

        r = q.makeRe(r'foo\tbar')
        assert r.search(r'footbar')
        assert not r.search('fooxbar')
        assert not r.search('foobar')

def test_Like():
    "Test Like operator"
    
    assert q.Like('foo*').matches(['foobar', 'xyzzy'])
    assert not q.Like('foo*').matches(['barfoo', 'xyzzy'])

def test_NotLike():
    "Test NotLike operator"
    
    assert q.NotLike('foo*').matches(['foobar', 'xyzzy'])
    assert not q.NotLike('foo*').matches(['foobar', 'foofoo'])

def test_Ilike():
    "Test Ilike operator"
    
    assert q.Ilike('foo*').matches(['FoOBar', 'xyzzy'])
    assert not q.Ilike('fOo*').matches(['barFoo', 'xFoObar'])

def test_NotIlike():
    "Test NotIlike operator"

    assert q.NotIlike('foO*').matches(['fobar', 'oof'])
    assert not q.NotIlike('fOo*').matches(['fOoBar', 'xyzzy'])
    assert not q.NotIlike('foO*').matches(['fOObAr', 'foOfoo'])

def test_Empty():
    "Test Empty operator"
    
    assert q.Empty().matches([])
    assert not q.Empty().matches([1, 2])

def test_NotEmpty():
    "Test NotEmpty operator"
    
    assert not q.NotEmpty().matches([])
    assert q.NotEmpty().matches([1, 2])

def test_NoneOf():
    "Test NoneOf operator"
    
    assert q.NoneOf([1, 2, 3]).matches([4, 5, 6])
    assert not q.NoneOf([1, 2, 3]).matches([4, 2, 6])

def test_Exact():
    "Test Exact operator"
    
    assert q.Exact([3, 2, 1]).matches([1, 3, 2])
    assert q.Exact([3, 2, 1, 1]).matches([1, 3, 2])
    assert not q.Exact([3, 2, 1]).matches([1, 3, 7])

def test_Fulltext():
    op = q.Fulltext('foo and bar', 'fake.Toc')
    assert op.expression == 'foo and bar'
    assert op.tocName == 'fake.Toc'
    assert op.value == ('foo and bar', 'fake.Toc')

    # A non evaluated fulltext condition matches nothing (yet)
    assert not op.matches([])
    assert not op.matches([1])

    # this is normally set by the TSE
    op.results = [1, 2, 3]
    assert op.matches([1])
    assert not op.matches([27])

def test_Readable():
    "Test Redable operator"
    
    assert q.Readable().matches([1, 3])
    assert q.Readable().matches(False)

def test_RegEx():
    regex = re.compile('foo.*', re.U)
    assert q.RegEx(regex).matches(['foobar', 'xyzzy'])
    assert not q.RegEx(regex).matches(['FoOBar', 'xyzzy'])

    assert q.RegEx('foobar', re.I | re.U).matches(['foobar', 'xyzzy'])
    assert q.RegEx('foobar', re.I | re.U).matches(['FoOBar', 'xyzzy'])

    copy.deepcopy(q.RegEx('foo')) # don't explode

def test_OperatorTypeEnforcement():
    "Test that attributes verify operator types"

    class myToc(TO):
        class myAttr(Bool()):
            pass

    y = q.Query(myToc, myAttr=q.Empty())

    def spam():
        y = q.Query(myToc, myAttr=q.Greater(23))

    py.test.raises(TypeError, spam)

def test_lateValidation():
    """
    Test that you can populate a Query using strings and run the
    validation afterwards
    """

    y = q.Query('myToc', myStr="foo", myBool=True)
    assert y.toc is None
    assert y.tocName == 'myToc'
    for attr in list(y[0].keys()):
        assert isinstance(attr, str)
    y[0].customAttribute = 'testing'
    
    class myToc(TO):
        class myStr(LimitedString()):
            pass
        class myBool(Bool()):
            pass

    py.test.raises(RuntimeError, y.validate) # Couldn't find TOC
    y.toc = myToc
    y.validate()
    assert y.toc == myToc
    for attr in list(y[0].keys()):
        assert isinstance(attr, Attribute)
    assert y[0].customAttribute == 'testing'
    
def test_defaultOperator():
    class myToc(TO):
        class myStr(LimitedString()):
            pass
        class myMap(StringMap()):
            pass

    y = q.Query(myToc, myStr="foo", myMap=('foo','bar'))
    
    y.toc = myToc
    y.validate()
    assert y.toc == myToc
    for attr in list(y[0].keys()):
        assert isinstance(attr, Attribute)
    assert isinstance(y[0][myToc.myMap][0], q.InMap)
    assert isinstance(y[0][myToc.myStr][0], q.In)
    

def test_ConditionGroup_behaves_dict_like():
    cg = q.ConditionGroup()
    cg['k1'] = 'v1'
    cg['k2'] = 'v2'
    assert cg['k1'] == 'v1'
    assert cg['k2'] == 'v2'

    assert cg

    assert isinstance(list(cg.keys()), list)
    assert sorted(cg.keys()) == ['k1', 'k2']
    assert sorted(cg.keys()) == ['k1', 'k2']

    assert isinstance(list(cg.values()), list)
    assert sorted(cg.values()) == ['v1', 'v2']
    assert sorted(cg.values()) == ['v1', 'v2']

    assert isinstance(list(cg.items()), list)
    assert sorted(cg.items()) == [('k1', 'v1'), ('k2', 'v2')]
    assert sorted(cg.items()) == [('k1', 'v1'), ('k2', 'v2')]

    assert cg.setdefault('k3', 'v3') == 'v3'
    assert cg['k3'] == 'v3'
    assert cg.setdefault('k3', 'ignore') == 'v3'
    assert cg['k3'] == 'v3'

    cg.clear()
    assert list(cg.keys()) == []
    assert not cg # empty, considered false

def test_Query_behaves_list_like():
    query = q.Query('some.Toc')

    assert len(query) == 1 # one empty cond group
    list(query) # don't explode

    query.append('blah')
    assert query[-1] == 'blah'

    query[:] = []
    assert len(query) == 0

def test_Query_copy():
    query = q.Query('some.Toc')
    query.push(foo='bar')
    query.attrList = {'foo'}

    copy = query.copy()

    assert copy == query
    assert copy.attrList == query.attrList

def test_Query_hasSubQuery():
    query = q.Query('some.Toc')
    assert not query.hasSubQuery()

    query.push(foo='bar')
    assert not query.hasSubQuery()

    query.push(baz=q.Query('some.OtherToc'))
    assert query.hasSubQuery()

def test_Query_hasFulltext():
    query = q.Query('some.Toc')
    assert not query.hasFulltext()

    query.push(foo='bar')
    assert not query.hasFulltext()

    query.push(id=q.Fulltext('some.OtherToc'))
    assert query.hasFulltext()

def test_Query_attrList():
    query = q.Query('some.Toc', foo='bar', _attrList=['foo', 'bar'])
    assert query.attrList == {'foo', 'bar'}

    query = q.Query('some.Toc', foo='bar')
    assert query.attrList == set()


class uolist(set):  # unordered list

    def __eq__(self, other):
        return set(other).__eq__(self)

class comp_re(object):

    def __init__(self, pattern, flags=0):
        self.re = re.compile(pattern, flags|re.U)

    def __eq__(self, other):
        return self.re.pattern == other.pattern and self.re.flags == other.flags


def repr_re(regex): # convenient when debugging
    return '/%s/%d' % (regex.pattern, regex.flags)


class TestMongoTranslation(object):

    def setup_method(self, method):
        class myToc(TO):
            class foo(Int()):
                pass
            class bar(ToiRef()):
                pass
            class baz(Timestamp()):
                pass
            class map(IntMap()):
                pass
            class str(String()):
                pass
            class bool(Bool()):
                pass

        self.toc = myToc

    def test_empty(self):
        query = q.Query(self.toc)
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]}}

    def test_empty_TO(self):
        query = q.Query(TO)
        mongo = query.mongo()
        assert mongo == {}

    def test_simple(self):
        query = q.Query(self.toc, foo=[27, 42])
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'foo': {'$in': uolist([27, 42])}
                         }

    def test_coercevalue(self):
        query = q.Query(self.toc, foo=['27', '42'])
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'foo': {'$in': uolist([27, 42])}
                         }
        

    def test_condgroups(self):
        query = q.Query(self.toc)
        query.clear()
        query.push(foo=[27, 42])
        query.push(foo=[66, 666])
        mongo = query.mongo()

        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         '$or': [{'foo': {'$in': uolist([27, 42])}},
                                 {'foo': {'$in': uolist([66, 666])}}]
                         }

    def test_special_case_op(self):
        query = q.Query(self.toc)
        query.clear()
        query.pushDict({'foo': [q.In([27, 42]), q.Empty()]})
        mongo = query.mongo()

        # xxx Empty overrides In
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'foo': {'$in': [None, [], {}]}}

    def test_id(self):
        from bson.objectid import ObjectId
        oid = ObjectId()
        query = q.Query(self.toc, id=oid)
        mongo = query.mongo()
        # id -> _id
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         '_id': {'$in': [oid]}
                         }

    def test_id_empty(self):
        # this will of course never find any tois, but the query is
        # possible to ask
        query = q.Query(self.toc, id=None)
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         '_id': {'$in': [None, [], {}]}}

    def test_Now_bson(self):
        now = q.Now(-10)
        now.evaluate = lambda when=None: 100
        query = q.Query(self.toc, baz=q.LessEq(now))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'baz': {'$lte': 100}}

    def test_Now_between_bson(self):
        now = q.Now(-10)
        now.evaluate = lambda when=None: 100
        then = q.Now(-50)
        then.evaluate = lambda when=None: 50
        query = q.Query(self.toc, baz=q.Between(then,now))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'baz': { '$elemMatch' : { '$gte' : 50, '$lte': 100 }}}

    def test_bool_empty(self):
        from bson.objectid import ObjectId
        oid = ObjectId()
        query = q.Query(self.toc, bool=None)
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'bool': {'$in': [None, [], {}]}
                         }

    def test_toiref(self):
        from bson.objectid import ObjectId
        oid = ObjectId()
        query = q.Query(self.toc, bar=oid)
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'bar.id': {'$in': [oid]}
                         }

        # again, with an op value that is (well, looks like) a toi
        class FakeToi(object):
            def __init__(self, id):
                self.id = tuple(id)
        op = list(query[0].items())[0][1][0] # fishing... oh, well
        op.value = set([FakeToi(id=op.value)])
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'bar.id': {'$in': [oid]}
                         }

    def test_toiref_empty(self):
        from bson.objectid import ObjectId
        oid = ObjectId()
        query = q.Query(self.toc, bar=None)
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'bar': {'$in': [None, [], {}]}
                         }

    def test_toiref_exact(self):
        from bson.objectid import ObjectId
        oid = ObjectId()
        query = q.Query(self.toc, bar=q.Exact([oid]))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'bar.id': {'$all' : [oid], '$size' : 1}
                         }

    def test_toiref_noneof(self):
        from bson.objectid import ObjectId
        oid = ObjectId()
        query = q.Query(self.toc, bar=q.NoneOf([str(oid)]))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         '$nor' : [{ 'bar.id': oid }]
                         }

    def test_fulltext(self):
        query = q.Query(self.toc, id=q.Fulltext('foo Bar'))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         '_terms.data': {'$all' : ['bar', 'foo']}
                         }

    def test_nor(self):
        query = q.Query(self.toc, foo=q.NoneOf([1,2]))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         '$nor': [ { 'foo' : 1 }, { 'foo' : 2} ] }

    def test_HasKey(self):
        query = q.Query(self.toc, map=q.HasKey('foo'))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'map.foo': { '$exists' : True }}

    def test_LacksKey(self):
        query = q.Query(self.toc, map=q.LacksKey('foo'))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'map.foo': { '$exists' : False }}

    def test_NoneOfMap(self):
        query = q.Query(self.toc, map=q.NoneOfMap('apa', [27, 42]))
        mongo = query.mongo()
        assert (mongo == {'_bases': {'$in': [self.toc._fullname]},
                          '$nor': [{'map.apa': 42}, {'map.apa': 27}] }
                or
                mongo == {'_bases': {'$in': [self.toc._fullname]},
                          '$nor': [{'map.apa': 27}, {'map.apa': 42}] })

    def test_comparison_mangling(self):
        query = q.Query(self.toc, foo=q.GreaterEq(1))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'foo': {'$gte': 1}}

        query = q.Query(self.toc, str=q.GreaterEq('aaa'))
        mongo = query.mongo()
        assert mongo == {'_bases': {'$in': [self.toc._fullname]},
                         'str': {'$gte': 'aaa'}}


class TestMongoOperatorTranslation(object):

    def test_Between(self):
        op = q.Between(27, 42)
        mongo = op.mongo()
        assert mongo == {'$elemMatch': {'$gte': 27, '$lte': 42}}

    def test_Empty(self):
        op = q.Empty()
        mongo = op.mongo()
        assert mongo == {'$empty': True}

    def test_NotEmpty(self):
        op = q.NotEmpty()
        mongo = op.mongo()
        assert mongo == {'$exists': True, '$ne': []}

    def test_Exact(self):
        op = q.Exact(['foo'])
        mongo = op.mongo()
        assert mongo == {'$all': ['foo'], '$size' : 1}

    def test_Fulltext(self):
        op = q.Fulltext('foo Bar!')
        mongo = op.mongo()
        assert mongo == {'$fulltext': ['bar', 'foo']}

    def test_Greater(self):
        op = q.Greater(27)
        mongo = op.mongo()
        assert mongo == {'$gt': 27}

    def test_GreaterEq(self):
        op = q.GreaterEq(27)
        mongo = op.mongo()
        assert mongo == {'$gte': 27}

    def test_HasKey(self):
        op = q.HasKey('abc')
        mongo = op.mongo()
        assert mongo == {'.': ('abc', { '$exists' : True })}

    def test_LacksKey(self):
        op = q.LacksKey('abc')
        mongo = op.mongo()
        assert mongo == {'.': ('abc', { '$exists' : False })}

    def test_Ilike(self):
        op = q.Ilike('foo*bar')
        mongo = op.mongo()
        assert mongo == {'$regex': comp_re('^foo.*bar$', re.I)}

    def test_IlikeMap(self):
        op = q.IlikeMap(('apa', 'foo*bar'))
        mongo = op.mongo()
        assert mongo == {'.': ('apa', {'$regex': comp_re('^foo.*bar$', re.I)})}

    def test_NotIlike(self):
        op = q.NotIlike('foo*bar')
        mongo = op.mongo()
        assert mongo == {'$not': comp_re('^foo.*bar$', re.I)}

    def test_NotIlikeMap(self):
        op = q.NotIlikeMap('apa', 'foo*bar')
        mongo = op.mongo()
        assert mongo == {'.': ('apa', {'$not': comp_re('^foo.*bar$', re.I)})}

    def test_In(self):
        op = q.In([27, 42])
        mongo = op.mongo()
        assert mongo == {'$in': uolist([27, 42])}

    def test_InMap(self):
        op = q.InMap('apa', [27, 42])
        mongo = op.mongo()
        assert mongo == {'.': ('apa', {'$in': uolist([27, 42])})}

    def test_NotIn(self):
        op = q.NotIn([27, 42])
        mongo = op.mongo()
        assert mongo == {'$nin': uolist([27, 42])}

    def test_Less(self):
        op = q.Less(27)
        mongo = op.mongo()
        assert mongo == {'$lt': 27}

    def test_LessEq(self):
        op = q.LessEq(27)
        mongo = op.mongo()
        assert mongo == {'$lte': 27}

    def test_Like(self):
        op = q.Like('foo*bar')
        mongo = op.mongo()
        assert mongo == {'$regex': comp_re('^foo.*bar$')}

    def test_LikeMap(self):
        op = q.LikeMap('apa', 'foo*bar')
        mongo = op.mongo()
        assert mongo == {'.': ('apa', {'$regex': comp_re('^foo.*bar$')})}

    def test_NotLike(self):
        op = q.NotLike('foo*bar')
        mongo = op.mongo()
        assert mongo == {'$not': comp_re('^foo.*bar$')}

    def test_NotLikeMap(self):
        op = q.NotLikeMap('apa', 'foo*bar')
        mongo = op.mongo()
        assert mongo == {'.': ('apa', {'$not': comp_re('^foo.*bar$')})}

    def test_NoneOf(self):
        op = q.NoneOf([27, 42])
        mongo = op.mongo()
        assert sorted(mongo['$nor']) == sorted([27, 42])
        assert list(mongo.keys()) == ['$nor']

    def test_NoneOfMap(self):
        op = q.NoneOfMap('apa', [27, 42])
        mongo = op.mongo()
        assert list(mongo.keys()) == ['.']
        key, mop = mongo['.']
        assert key == 'apa'
        assert sorted(mop['$nor']) == sorted([27, 42])
        assert list(mop.keys()) == ['$nor']

    def test_Readable(self):
        py.test.skip('Not implemented')

    def test_RegEx(self):
        op = q.RegEx('foo.*bar')
        mongo = op.mongo()
        assert mongo == {'$regex': comp_re('foo.*bar')}

        op = q.RegEx(re.compile('foo.*bar', re.I | re.U))
        mongo = op.mongo()
        assert mongo == {'$regex': comp_re('foo.*bar', re.I | re.U)}


class TestFreezeQuery(object):

    def test_freeze(self):
        r = q.freeze([[], 1])
        assert r == ((), 1)

        r = q.freeze({'foo': 'bar',
                      'baz': 'qux'})
        assert r == frozenset([
            ('foo', 'bar'),
            ('baz', 'qux'),
        ])

        r = q.freeze({'foo': {'bar': 'baz'}})
        assert r == frozenset([
            ('foo', frozenset([('bar', 'baz')])),
        ])

        r = q.freeze({'foo': {'bar': [1, 2, 3]}})
        assert r == frozenset([
            ('foo', frozenset([('bar', (1, 2, 3))])),
        ])

    def check_hashable(self, query, expect):
        hashable = q.freeze(query.mongo())
        {hashable: 1}  # don't explode
        assert hashable == expect

    def test_empty(self):
        query = q.Query('Foo')
        self.check_hashable(
            query,
            frozenset([('_bases', frozenset([('$in', ('Foo',))]))])
        )


class TestWithDB(BLMTests):

    def test_Empty(self):
        from blm import fundamental, testblm
        toi = blm.testblm.Test()
        self.commit()

        assert blm.testblm.Test._query(attr1=q.Empty()).run()
        assert blm.testblm.Test._query(attr3=q.Empty()).run()
        assert blm.testblm.Test._query(attr4=q.Empty()).run()
        assert blm.testblm.Test._query(attr5=q.Empty()).run()

        toi, = blm.testblm.Test._query().run()
        toi(attr1=['foo'], attr3={'foo': 'bar'}, attr4=[toi], attr5={'foo': toi})
        self.commit()

        assert not blm.testblm.Test._query(attr1=q.Empty()).run()
        assert not blm.testblm.Test._query(attr3=q.Empty()).run()
        assert not blm.testblm.Test._query(attr4=q.Empty()).run()
        assert not blm.testblm.Test._query(attr5=q.Empty()).run()

        toi, = blm.testblm.Test._query().run()
        toi(attr1=[], attr3={}, attr4=[], attr5={})
        self.commit()

        assert blm.testblm.Test._query(attr1=q.Empty()).run()
        assert blm.testblm.Test._query(attr3=q.Empty()).run()
        assert blm.testblm.Test._query(attr4=q.Empty()).run()
        assert blm.testblm.Test._query(attr5=q.Empty()).run()
