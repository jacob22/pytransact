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
This module collects, completes and verifies BLMs
"""

from pytransact.object.to import TO
import pytransact.object.attribute as Attribute
from pytransact.query import Query
from pytransact.object.restriction import ToiType
from pytransact.object.property import Presentation
from pytransact.object.method import ExternalMethod
from . import blm


class BlmAttributeError(AttributeError):

    pass


relationData = {}

def setupBlm(mod):
    """
    Set up a BLM

    Arguments: mod - the blm module to set up
    Returns:   None
    """
    mod._tocs = {}
    mod._attributes = {}
    mod._methods = {}

    # Add the module itself to the module namespace
    # for self-reference purposes in relations etc.
    mod.__dict__[mod.__name__] = mod

    # Walk the list of objects, find BLM objects
    mlist = list(mod.__dict__.keys())
    for moditem in mlist:
        try:
            moditemOb = mod.__dict__[moditem]
        except:
            continue

        if hasattr(moditemOb, '__extend__'):
            try:
                del moditemOb.__dict__['__extend__']
                del moditemOb._tocs['__extend__']
            except:
                pass
            continue

        # Sort TOCs
        if (isinstance(moditemOb, type) and
            issubclass(moditemOb, TO) and
            moditemOb is not TO):
            mod._tocs[moditem] = moditemOb

            presProp = False		# Presentation property detection

            # Complete relation declarations
            for attr, attrOb in moditemOb._attributes.items():
                attrOb._location = moditemOb._fullname
                if (isinstance(attrOb, Attribute.Relation)):
                    r = getattr(attrOb, 'related', None)
                    if (r is not None and
                        isinstance(r, str)):
                        # related should be a string by this time.
                        # See Bl.Object.Attribute.mAttribute.__new__
                        spam, bName, tName, aName = \
                              ('...' + attrOb.related).rsplit('.', 3)

                        try:
                            pbName, ptName = attrOb._basetoc.split('.', 1)

                            if bName:
                                bOb = getattr(blm, bName)
                            else:
                                bOb = getattr(blm, pbName)

                            if tName:
                                tOb = getattr(bOb, tName)
                            else:
                                tOb = getattr(bOb, ptName)
                                
                            attrOb.related = getattr(tOb, aName)

                        except AttributeError as e:
                            if not bName:
                                bName = mod.__name__
                            raise BlmAttributeError("Attribute '%s' not defined in '%s.%s.%s'"%(r, bName, tName, aName))

                        if not bName:
                            bName = mod.__name__
                        if not tName:
                            tName = moditem
                        relationData['%s.%s.%s'%(mod.__name__, moditem,
                                                 attrOb.name)] = \
                        '%s.%s.%s'%(bName, tName, aName)

                    else:
                        # Might not have existed before, does now with
                        # determined value None
                        if r is None:
                            setattr(attrOb, 'related', None)
                            relationData['%s.%s.%s'%(mod.__name__, moditem,
                                                     attrOb.name)] = None
                        else:                            
                            relationData['%s.%s.%s'%(mod.__name__, moditem,
                                                     attrOb.name)] = '%s.%s'%(
                                r._toc._fullname, r.name)

                    if attrOb.related is not None:
                        if not isinstance(attrOb.related, Attribute.Relation):
                            raise BlmAttributeError("Attribute '%s' is not a relation or None" % r)
                        recipAttr = attrOb.related.related
                        attrObName = '%s.%s'%(attrOb._basetoc,
                                              attrOb.name)
                        if recipAttr is not None:
                            if isinstance(recipAttr, Attribute.Relation):
                                recipAttrName = '%s.%s'%(recipAttr._basetoc,
                                                         recipAttr.name)
                                if recipAttrName != attrObName:
                                    raise BlmAttributeError('Attribute "%s"\'s related attribute does not relate back (%s)'% (attrObName, recipAttrName))
                            
                            
                        

                # Make sure any queries in qualifications have been validated,
                # so that TOC and Attribute checking is done.
                if (isinstance(attrOb, Attribute.ToiRef) or
                    isinstance(attrOb, Attribute.ToiRefMap)):
                    for resttype in ('pre', 'post', 'hint'):
                        rest = attrOb.getrest(ToiType, **{resttype:True})
                        if not rest:
                            continue
                        try:
                            quals = rest.expandQual()
                            q = Query(rest.validToiType, **quals)
                            q.validate(mod) # Will validate subqueries too
                        except Exception as e:
                            raise RuntimeError('Illegal restriction:\n'
                                '     validToiType: %r\n'
                                '     Qualification: %r\n'
                                '     Attribute Object: %r\n'
                                '     Error string: %s' % (
                                rest.validToiType, rest.qualification,
                                attrOb, str(e)))
                        rest.validToiType = q.toc

                # Fill in the xlatKey param in EnumVals
                if isinstance(attrOb, Attribute.Enum):
                    for v in attrOb.values:
                        v._xlatKey = '%s.%s'%(attrOb._xlatKey, v.name)

                # Check that the Presentation() property is only
                # applied once, if ever
                if attrOb.getprop(Presentation):
                    if presProp:
                        raise RuntimeError('Double Presentation() tagging, in "%s.%s.%s" and "%s.%s.%s"'%(mod.__name__, moditem, presProp.name, mod.__name__, moditem, attrOb.name))
                    else:
                        presProp = attrOb

            # Complete method return/restriction type declarations
            for method, methodOb in moditemOb._methods.items():
                methodOb._location = moditemOb._fullname
                if isinstance(methodOb.rtype, Attribute.ToiRef):
                    r = methodOb.rtype.getrest(ToiType, post=True)
                    if isinstance(r.validToiType, str):
                        r.validToiType = getattr(mod, r.validToiType)
                for p in methodOb.params:
                    if isinstance(p, Attribute.ToiRef):
                        r = p.getrest(ToiType, post=True)
                        if (r is not None and
                            isinstance(r.validToiType, str)):
                            r.validToiType = getattr(mod, r.validToiType)
                    p._xlatKey = '%s.%s'%(methodOb._xlatKey, p.name)
                if methodOb.rtype:
                    methodOb.rtype._xlatKey = methodOb._xlatKey
                        
        if isinstance(moditemOb, Query):
            moditemOb.validate(mod)
        
        # Sort Attributes & Instansiate blm ones
        if (isinstance(moditemOb, type) and
            issubclass(moditemOb, Attribute.Attribute) and
            not moditemOb.__name__.startswith('_') and
            moditemOb.__module__ == mod.__name__):
            mod._attributes[moditem] = moditemOb._instantiate(moditem)
            mod._attributes[moditem]._location = mod.__name__
            mod._attributes[moditem]._xlatKey = '%s.%s'%(mod.__name__, moditem)
            setattr(mod, moditem, mod._attributes[moditem])

        # Fill in the xlatKey param in EnumVals
        attrOb = getattr(mod, moditem, None)
        if isinstance(attrOb, Attribute.Enum):
            for v in attrOb.values:
                v._xlatKey = '%s.%s'%(attrOb._xlatKey, v.name)

        # Sort Methods
        if (isinstance(moditemOb, ExternalMethod) and
            not moditem.startswith('_')):
            mod._methods[moditem] = moditemOb
            moditemOb._location = mod.__name__
            moditemOb._xlatKey = '%s.%s'%(mod.__name__, moditem)
            if moditemOb.rtype:
                moditemOb.rtype._xlatKey = '%s.%s'%(mod.__name__, moditem)
            for p in moditemOb.params:
                p._xlatKey = '%s.%s'%(moditemOb._xlatKey, p.name)


def setupTocs(blms):
    fixedTocs = set()

    def fixToc(toc):
        if toc in fixedTocs:
            return
        if not issubclass(toc.__bases__[0], TO):
            fixedTocs.add(toc)
            return
        parentToc = toc.__bases__[0]
        if parentToc not in fixedTocs:
            fixToc(parentToc)
        missingAttrs = [attrName for attrName in parentToc._attributes
                        if attrName not in toc._attributes]
        
        for attrName in missingAttrs:
            parentAttr = parentToc._attributes[attrName]
            attr = parentAttr.__class__._instantiate(attrName)
            if hasattr(attr, 'related'):
                attr.related = parentAttr.related
            attr._xlatKey = parentAttr._xlatKey
            attr._extended = parentAttr._extended
            attr._overridden = parentAttr._overridden
            attr._basetoc = parentAttr._basetoc
            toc._attributes[attrName] = attr
            attr._toc = toc
            setattr(toc, attrName, attr)
        
        missingMethods = [methName for methName in parentToc._methods
                          if methName not in toc._methods]

        for methName in missingMethods:
            toc._methods[methName] = parentToc._methods[methName]

        fixedTocs.add(toc)
        
    for blm in blms:
        for toc in blm._tocs.values():
            fixToc(toc)
