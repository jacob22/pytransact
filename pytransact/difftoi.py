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
Contains definitions for all object types used by the BL (and) Client
"""
from pytransact import spickle, custombson

class DiffTOI(object):
    """
    This object defines the base class for the object type used to
    send TOI updates between the client and the BL.
    """

    def __init__(self, orgToi=None, newToi=None):
        """
        Initialise the instance.

        Arguments: optional: orgToi and newToi as in extractDiff
        Returns:   None
        """
        self.diffAttrs = {}
        self.orgAttrs = {}
        self.toid = 0
        self.toc_fullname = None
        self.toc = None

        if (newToi is not None and not issubclass(newToi.toc, orgToi.toc)):
            raise RuntimeError("Differing base TOCs")

        if orgToi is not None:
            if newToi is not None:
                self.setAttrDiff(newToi.toc, newToi.id,
                                 orgToi._attrData, newToi._attrData)
            else:
                self.setData(orgToi.toc, orgToi.id, orgToi._attrData)

    def __eq__(self, o):
        try:
            return (self.toid == o.toid and
                    self.toc_fullname == o.toc_fullname and
                    self.diffAttrs == o.diffAttrs and
                    self.orgAttrs == o.orgAttrs)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return '<DiffTOI %s(%s) attrs: %r>' % (self.toc_fullname,
                                               self.toid,
                                               self.diffAttrs)

    def __getstate__(self):
        """
	Returns a dictionary representation of this object

        Arguments: None
        Returns:   A dict containing the internal state for this object
        """
        d = self.__dict__.copy()
        if 'toc' in d:
            del d['toc']
        return d

    def forget(self, attrName):
        """
        Delete cached info about an attribute
        """
        if attrName in self.diffAttrs:
            del self.diffAttrs[attrName]

    def update(self, other):
        """
        Update cached info based on new data

        Arguments: other - another DiffToi
        Returns:   None
        """
        self.toc_fullname = other.toc_fullname
        self.toid = other.toid
        if other.toc:
            self.toc = other.toc
        self.diffAttrs.update(other.diffAttrs)

    def setData(self, toc, toid, oldAttr, oldAccess={}):
        """
        Copy data from a TO for transmission
        """
        self.toid = toid
        self.toc = toc
        self.toc_fullname = toc._fullname
        self.diffAttrs = {}
        self.orgAttrs = {}

        for attrName in toc._attributes:
            attr = oldAttr.get(attrName)
            if attr is not None:
                self.diffAttrs[attrName] = attr

    def setDiff(self, toc, toid, oldAttr, oldAccess, newAttr, newAccess):
        """
        Set diff status from ToiRequest dicts
        """
        self.toid = toid
        self.toc_fullname = toc._fullname
        self.diffAttrs = {}
        self.orgAttrs = {}

        for attrName in toc._attributes:
            newValue = newAttr.get(attrName)
            oldValue = oldAttr.get(attrName)

            if newValue is not None:
                if oldValue != newValue:
                    self.diffAttrs[attrName] = newValue
                    if oldValue is None:
                        oldValue = getattr(toc, attrName).empty
                    self.orgAttrs[attrName] = oldValue

    def setAttrDiff(self, toc, toid, oldAttr, newAttr):
        self.setDiff(toc, toid, oldAttr, {}, newAttr, {})

    def setToi(self, toi):
        new = dict((attr, getattr(toi, attr).value) for attr in toi._modified)
        self.setAttrDiff(toi.__class__, toi.id[0], toi._orgAttrData, new)

    def diffsOld(self, oldToi):
        diffs = {}
        for name, val in self.orgAttrs.items():
            old = getattr(oldToi, name)
            if val != old:
                diffs[name] = (old, val)
        return diffs

spickle.stateExtension(DiffTOI)
custombson.register(DiffTOI)
