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
This module contains the inter-process query object definitions.
"""

__all__ = ('ConditionGroup', 'Query', 'Operator', 'Between',
           'Empty', 'NotEmpty', 'Exact', 'Greater', 'GreaterEq', 'HasKey',
           'LacksKey', 'Ilike', 'NotIlike', 'In', 'NotIn', 'InMap', 'Less',
           'LessEq', 'In', 'NotIn', 'Like', 'NotLike','Ilike', 'NotIlike',
           'LikeMap', 'NotLikeMap', 'IlikeMap', 'NotIlikeMap',
           'NoneOf', 'NoneOfMap', 'Readable', 'Now', 'Fulltext', 'RegEx')

from pytransact import exceptions, custombson, textindexing
from pytransact.queryops import *
import re
import time
from pytransact import blm


def freeze(obj):
    if isinstance(obj, dict):
        r = set()
        for k, v in obj.items():
            r.add((k, freeze(v)))
        return frozenset(r)
    elif isinstance(obj, list):
        return tuple(freeze(o) for o in obj)
    return obj


class ConditionGroup(object):
    """
    Represents a group of conditions, specified through an attribute
    object as index and an Operator containing the comparison value.

    A TO instance matches a condition group if it matches all
    Operators in the group.

    This class exists only so that it will possible to add custom
    attributes to it (not possible for std dicts). This functionality
    is needed for the subscription registry in the IFC to work - a
    ConditionGroup may be looking at different TOCs yet have the same
    attribute-value contents.
    """

    def __init__(self, *args, **kw):
        self.conds = dict(*args, **kw)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def __len__(self):
        return len(self.conds)

    def __setitem__(self, item, value):
        self.conds[item] = value

    def __getitem__(self, item):
        return self.conds[item]

    def __repr__(self):
        return 'ConditionGroup(%s)' % str(list(self.conds.items()))

    def copy(self):
        copy = ConditionGroup()
        copy.conds = self.conds.copy()
        return copy

    def clear(self):
        self.conds.clear()

    def items(self):
        return list(self.conds.items())

    def iteritems(self):
        return iter(self.conds.items())

    def keys(self):
        return list(self.conds.keys())

    def iterkeys(self):
        return iter(self.conds.keys())

    def values(self):
        return list(self.conds.values())

    def itervalues(self):
        return iter(self.conds.values())

    def setdefault(self, item, value):
        return self.conds.setdefault(item, value)

    @property
    def id(self):
        raise RuntimeError('The query has not been run')

    def __iter__(self):
        """
        Return an iterator over all the attribute, Operator pairs in this
        ConditionGroup.

        Arguments: None
        Returns:   The iterator.
        """
        for attr, v in list(self.conds.items()):
            for val in v:
                yield attr, val
        raise StopIteration()

    def matches(self, toi, getter, when=None):
        """
        Deterine if the condition group matches a given toi.

        Arguments: toi - data to match (NB! Could be any type of object)
                   getter - attribute value extractor function
                   when - time reference for use in time-based conds
        Returns:   Bool
        """
        if not self:
            return True
        res = True
        for attr, clist in self.items():
            for cond in clist:
                if isinstance(cond, Query):
                    # Fugly but works - the IFC needs this stuff
                    if hasattr(cond, "results") and cond.results is not None:
                        res = res and set(getter(toi, attr)).intersection(cond.results)
                    else:
                        raise RuntimeError(
                            "FIXME! URGENT! The code that called this *MUST* "
                            "look at the query and run any subqueries "
                            "beforehand or it can't get correct results!")
                else:
                    res = res and cond.matches(getter(toi, attr))
                
                # Speedup
                if not res:
                    break
        
        return res

    def url(self):
        return ','.join(['%s:%s' % (str(attr), str(cond))
                         for attr,cond in self])

    def _iterconds(self):
        for clist in self.values():
            for cond in clist:
                yield cond

    def hasSubQuery(self):
        for cond in self._iterconds():
            if isinstance(cond, Query):
                return True
        return False

    def hasFulltext(self):
        for cond in self._iterconds():
            if isinstance(cond, Fulltext):
                return True
        return False

custombson.register(ConditionGroup)


class Query(SubQuery):
    """
    Represents a query tree to be sent to the IFC. A query tree is
    intended to be applied to all instances of a a single TOC.
    """

    def __init__(self, toc, _attrList=(), **conds):
        """
        Initialise the object.

        Arguments: tocName - the name of the objective TOC
                   _attrList - attributes to fetch from the db
                   conds - initial conds to add
        Returns:   None
        """
        if isinstance(toc, str):
            try:
                from . import blm
                self.toc = eval('blm.'+toc)
                self.tocName = self.toc._fullname
            except:
                self.toc = None # Must validate this TOC later.
                self.tocName = toc
        else:
            self.toc = toc
            self.tocName = toc._fullname
        self.results = None
        self.valid = None
        self.cgs = []
        self.attrList = set(_attrList)
        self.push(**conds)

    def append(self, o):
        self.cgs.append(o)

    def __iter__(self):
        return iter(self.cgs)

    def __len__(self):
        return len(self.cgs)

    def __getitem__(self, item):
        return self.cgs[item]

    def __setitem__(self, item, value):
        self.cgs[item] = value

    def __getstate__(self):
        """
        Returns a dictionary representation of this object

        Arguments: None
        Returns:   A dict containing the internal state for this object
        """
        if self.toc is None:
            cgData = self[:]
        else:
            cgData = []
            for cg in self:
                newcg = {}
                for attr, condList in cg.items():
                    if attr == 'id':
                        newcg['id'] = condList
                    else:
                        newcg[attr.name] = condList
                cgData.append(newcg)
        return { 'condgroups' : cgData,
                 'tocName'    : self.tocName,
                 'attrList'   : self.attrList }

    def __setstate__(self, state):
        """
        Sets the state of this object from the given dictionary

        Arguments: A dictionary to read state from
        Returns:   None
        """
        self.results = None
        self.valid = None
        self.toc = self.tocName = None
        self.cgs = []
        try:
            from . import blm
            self.toc = blm.getTocByFullname(state['tocName'])
            self.tocName = self.toc._fullname

            cgList = []
            for cg in state['condgroups']:
                cgnew = ConditionGroup()
                for aname, condList in cg.items():
                    if aname == 'id':
                        cgnew[aname] = condList
                    else:
                        cgnew[getattr(self.toc, aname)] = condList
                cgList.append(cgnew)

            self.cgs[:] = cgList

        except:
            self.tocName = state['tocName']
            self.cgs[:] = [ConditionGroup((aname, condList)
                                          for aname, condList in cg.items())
                           for cg in state['condgroups'] ]
        self.attrList = state.get('attrList', set())

    def __repr__(self):
        return "<Query %s [%s]>" % (self.tocName,
                                    ', '.join([repr(cg) for cg in self]))

    __str__ = __repr__

    def url(self):
        return "search:%s{%s}" % (self.tocName,
                                  '|'.join([cg.url() for cg in self]))

    def fromUrl(self, url):
        raise NotImplementedError

    def intersection(self, other):
        """
        Builds a new query that will match the intersection of the results
        from this query and the other.
        """
        def getName(attr):
            if type(attr) is str: return attr
            return attr.name

        self.validate()
        other.validate()
        if other.tocName != self.tocName:
            if issubclass(self.toc, other.toc):
                toc = self.toc
            elif issubclass(other.toc, self.toc):
                toc = other.toc
            else:
                raise ValueError("The queries must operate on the same TOC")
        else:
            toc = self.toc
        newQ = Query(toc)
        newQ.clear()
        for cg1 in self:
            for cg2 in other:
                newcg = dict([(getName(attr), conds)
                              for attr,conds in cg1.items()])
                for attr,cond in cg2:
                    conds = newcg.setdefault(getName(attr),[])
                    if cond not in conds:
                        conds.append(cond)
                newQ.pushDict(newcg)
        return newQ

    def union(self, other):
        """
        Builds a new query that will match the union of the results
        from this query and the other.
        """
        def getName(attr):
            if type(attr) is str: return attr
            return attr.name

        self.validate()
        other.validate()
        if other.tocName != self.tocName:
            raise ValueError("The queries must operate on the same TOC")
        cgs = []
        for cg in self:
            cgs.append(dict([(getName(attr), conds)
                             for attr,conds in cg.items()]))
        for cg in other:
            newcg = dict([(getName(attr), conds)
                          for attr,conds in cg.items()])
            if newcg not in cgs:
                cgs.append(newcg)
        newQ = Query(self.toc)
        newQ.clear()
        for cg in cgs:
            newQ.pushDict(cg)
        return newQ

    def __eq__(self, other):
        """
        Performs a comparison by the other object's values. QueryCondGroups
        are matched independent of order

        NB! If this function breaks, EVERYTHING breaks!
        """
        if self is other:
            return True

        try:
            if self.tocName != other.tocName:
                return False
            if len(self) != len(other):
                return False
        except AttributeError:
            return False # Not a Query

        def eqCg(one, two):
            if set(one.keys()) == set(two.keys()):
                for key, val in one.items():
                    if val != two[key]:
                        return False
                else:
                    return True
            return False

        cgDone = [False]*len(other)
        for cg in self:
            for i, otherCG in enumerate(other):
                if not cgDone[i] and eqCg(cg, otherCG):
                    cgDone[i] = True
                    break
            else:
                return False

        return True

    def __ne__(self, other):
        return not self == other

    @property
    def attributes(self):
        """
        Returns the attributes tested by this condition group.

        Arguments: None
        Returns:   The list of attributes
        """
        result = set()
        for cg in self:
            result.update(list(cg.keys()))

        return result

    def matches(self, toi, getter, when=None):
        """
        Deterine if the query matches a given toi.

        Arguments: toi - data to match (NB! Could be any type of object)
                   getter - attribute value extractor function
                   when - time reference for use in time-based conds
        Returns:   Bool
        """
        if not self:
            return True

        if not when:
            when = time.time()

        for cg in self:
            res = cg.matches(toi, getter, when)

            if res:
                return res

        return res

    def pushDict(self, cDict):
        """
        Push a pre-processed condgroup 'dict' to the 'stack'.

        Arguments: cDict - dictionary of attribute conditions
        Returns:   None
        """
        def translate(attr, value):
            op = In
            from pytransact.object.attribute import MapAttribute
            if isinstance(attr, MapAttribute):
                op = InMap

            if value is None:
                cond = Empty()

            elif isinstance(value, (Operator, Query)): # NB! Query IS a list!!!
                cond = value

            elif isinstance(value, (list, tuple, set, frozenset)):
                cond = op(tuple(value))

            else:
                cond =  op([value])

            if (hasattr(attr, '_validQueryOps') and
                not isinstance(cond, attr._validQueryOps)):
                raise TypeError('Invalid operator "%s" for attribute "%s"' %
                                (cond.__class__.__name__, attr))

            return cond

        cg = ConditionGroup()

        for attr, value in cDict.items():

            if self.toc and attr != 'id':
                if isinstance(attr, str):

                    attr = getattr(self.toc, attr)
                else:
                    attr = getattr(self.toc, attr.name)

            vlist=[]

            if value and isinstance(value, (list, tuple, set, frozenset)):
                # Special-case code for when we're appending a condList
                for v in value:
                    if isinstance(v, (Operator, Query)):
                        # One operator means this counts as a list of Operators
                        for val in value:
                            vlist.append(translate(attr, val))
                        break
                else: # Ordinary list expression
                    vlist.append(translate(attr, value))
            else:
                vlist.append(translate(attr, value))

            cg[attr] = vlist

        self.append(cg)

    def push(self, **conds):
        """
        Push a new cond group on to the 'stack'.

        Arguments: conds - set of initial conditions to add
        Returns:   None
        """
        self.pushDict(conds)

    def clear(self):
        """
        Remove *all* condition groups from this query. Presumably, we're going
        to add some later. If not, the query probably won't run properly.

        Arguments: None
        Returns:   None
        """
        self.cgs[:] = []

    def validate(self, module=None):
        """
        Do all that funky validation. Raises appropriate error on failure.
        """
        if module:
            try:
                self.toc = eval('module.' + self.tocName)
            except:
                pass

        if not self.toc:
            try:
                self.toc = eval('blm.' + self.tocName)
            except:
                pass

        if not self.toc:
            raise RuntimeError('Could not locate TOC %s for this Query.' % (
                self.tocName))

        self.tocName = self.toc._fullname
        for cg in self:
            # Replaces the keys _in-place_. Values, CGs &c keep their object
            # identity and any non-standard attributes set in them.
            oldcg = cg.copy()
            cg.clear()
            for attr, clist in oldcg.items():
                if attr != 'id':
                    if isinstance(attr, str):
                        attr = getattr(self.toc, attr)
                    else:
                        attr = getattr(self.toc, attr.name)

                cg[attr] = clist
                for c in clist:
                    if (hasattr(attr, '_validQueryOps') and
                        not isinstance(c, attr._validQueryOps)):
                        raise TypeError('Invalid operator "%s" for attribute "%s"' %
                                        (c.__class__.__name__, attr))
                    if isinstance(c, Query):
                        c.validate(module)

    def copy(self, translator=None):
        """
        Create and return a copy of this Query, with all TOIref values
        translated using the provided translation dict.

        Arguments: translator - optional callable for translating condition
                                values. The callable will get attribute as
                                first parameter, and value as the second.
        Returns: A copy of the Query
        """
        newcgs = []
        for cg in self:
            newcg = ConditionGroup()
            newcgs.append(newcg)
            for attr, vlist in cg.items():
                nvlist = []
                for v in vlist:
                    if isinstance(v, Query):
                        nvlist.append(v.copy(translator))
                    else:
                        nvlist.append(v.copy(attr, translator))
                newcg[attr] = nvlist
        newq = Query(self.toc or self.tocName)
        newq[:] = newcgs
        newq.attrList = set(self.attrList)
        return newq

    def hasSubQuery(self):
        return any(cg.hasSubQuery() for cg in self.cgs)

    def hasFulltext(self):
        return any(cg.hasFulltext() for cg in self.cgs)

    _valueops=set(['$gt', '$lt', '$gte', '$lte', '$all', '$ne',
                   '$in', '$nin', '$nor'])
    def mongo(self):
        from pytransact.object.attribute import ToiRef
        from bson.objectid import ObjectId, InvalidId

        def _filter_mop(attr, mop):
            # some special cases that don't fit in elswhere:
            # - toiref attrnames are converted to the dot-notation in order
            #   to find our toiref representation objects
            # - toiref have tois converted to objectids
            # - id (which, btw, is a str rather than an Attribute) is
            #   renamed to _id (as that's how it's stored in mongodb)
            #   or _terms, depending on operator used
            # - resolve Query.Now()
            # - reformat nor to match mongodb expectations
            # - translate $empty to {'$in': [None, [], {}]}
            # - expand map attribute names
            def maybeObjectId(oid):
                if oid is None:
                    return oid
                try:
                    return ObjectId(oid)
                except (InvalidId, TypeError):
                    return oid

            def tryCoerceValue(attr, val):
                try:
                    return attr.coerceValue(val)
                except exceptions.ValueTypeError:
                    return val

            def fixSpecial(attrName, mop):
                if not isinstance(mop, dict):
                    return attrName, mop
                if '.' in mop:
                    key, mop = mop['.']
                    attrName = '%s.%s' % ( attrName, key)

                if '$empty' in mop:
                    del mop['$empty']
                    if attrName.endswith('.id'):
                        attrName = attrName[:-3]
                    mop['$in'] = [None, [], {}]

                if '$nor' in mop:
                    norlist = []
                    norop = mop.copy()
                    del norop['$nor']
                    for expr in mop['$nor']:
                        op = norop.copy()
                        op[attrName] = expr
                        norlist.append(op)
                    return '$nor', norlist

                return attrName, mop

            if isinstance(mop, dict) and (isinstance(
                attr, ToiRef) or attr == 'id'):
                for key, value in mop.items():
                    if key in self._valueops:
                        mop[key][:] = [t.id[0] if hasattr(t, 'id') else
                                       maybeObjectId(t)
                                       for t in value]
            if attr == 'id':
                if mop is None:
                    return '_id', None
                if '$fulltext' in mop:
                    mop['$all'] = mop['$fulltext']
                    del mop['$fulltext']
                    return '_terms.data', mop
                else:
                    return fixSpecial('_id', mop)

            if isinstance(mop, dict):
                for key, value in mop.items():
                    if isinstance(value, Now):
                        value = value.evaluate()
                    if key == '$elemMatch':
                        for k2, v2 in value.items():
                            if isinstance(v2, Now):
                                v2 = v2.evaluate()
                            v2 = tryCoerceValue(attr, v2)
                            value[k2] = v2
                    elif key in self._valueops:
                        if (not isinstance(value, str) and
                            hasattr(value, '__len__')):
                            value = [tryCoerceValue(attr, v) for v in value]
                        else:
                            value = tryCoerceValue(attr, value)
                    mop[key] = value

            if isinstance(attr, ToiRef):
                return fixSpecial('%s.id' % (attr.name,), mop)

            return fixSpecial(attr.name, mop)

        mongo = {}
        if self.tocName != 'TO':
            mongo['_bases'] = {'$in': [self.tocName]}
        cglist = []
        for cg in self:
            mongocg = {}
            cglist.append(mongocg)
            for attr, ops in cg.items():
                for op in ops:
                    attrName, mop = _filter_mop(attr, op.mongo())
                    if isinstance(mop, dict):
                        mongocg.setdefault(attrName, {}).update(mop)
                    elif attrName == '$nor':
                        mongocg.setdefault('$nor', []).extend(mop)
                    else:
                        mongocg[attrName] = mop
                        break
        if len(cglist) > 1:
            mongo['$or'] = cglist
        elif cglist:
            mongo.update(cglist[0])
        return mongo

custombson.register(Query)
