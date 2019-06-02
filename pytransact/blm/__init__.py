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

import sys, os, imp, importlib.machinery

__path__ = [os.path.dirname(__file__)]
__blms__ = {}

def getTocByFullname(fullName):
    #import pdb; pdb.set_trace()
    if fullName == 'TO' or fullName.endswith('Object.TO.TO'):
        from pytransact.object.to import TO
        return TO
    blm, toc = fullName.split('.')
    #if blm in __blms__:
    return __blms__[blm]._tocs[toc]
    #return None


def addBlmPath(path):
    sys.path_importer_cache.pop(path,None)
    __path__.append(path)


def removeBlmPath(path):
    sys.path_importer_cache.pop(path,None)
    __path__.remove(path)


def clear():
    blm = sys.modules['blm']  # this module
    for mod in list(sys.modules.keys()):
        if mod.startswith('blm.') or mod.startswith('pytransact.blm.'):
            modname = mod.split('.')[-1]
            blm.__dict__.pop(modname, None)
            del sys.modules[mod]

    blm.__blms__.clear()
    TO = getTocByFullname('TO')
    ttype = TO.allowRead.preRestrictions[0].validToiType
    try:
        TO.allowRead.preRestrictions[0].validToiType = ttype._fullname
    except AttributeError:
        pass


class _BLMImporter(object):
    "Handle import of blm modules"

    def __init__(self, path):
        if path not in __path__:
            raise ImportError

    class blmloader(object):
        pending_loads = set()

        def __init__(self, fp, path, descr):
            self.fp = fp
            self.path = path
            self.descr = descr

        def load_module(self, name):
            self.pending_loads.add(self)

            if name in sys.modules:
                return sys.modules[name]

            try:
                mod = imp.load_module(name, self.fp, self.path, self.descr)
            finally:
                if self.fp:
                    self.fp.close()
                self.fp = None

            sys.modules[name] = mod
            _, blmName = name.rsplit('.',-1)
            mod.__loader__ = self
            mod.blmName = blmName
            __blms__[blmName] = mod

            self.pending_loads.remove(self)
            if not self.pending_loads:
                # No more loads pending, resolve

                # XXX blm.blmName must exists
                import blm
                setattr(blm, blmName, mod)

                from pytransact import blmsupport

                for blmName, blm in __blms__.items():
                    blmsupport.setupBlm(blm)
                blmsupport.setupTocs(iter(__blms__.values()))
            return mod

    def find_module(self, fullname, path=None):
        if not fullname.startswith('blm.'):
            # Not a blm import
            return None

        _, blm_name = fullname.rsplit('.',1)
        found = imp.find_module(blm_name, path or __path__)

        # Hack, ensure TO exists
        global TO
        from pytransact.object.to import TO

        return self.blmloader(*found)


class BlmLoader(importlib.abc.Loader):

    def __init__(self, modname, filename):
        self.modname = modname
        self.filename = filename

    def create_module(self, spec):
        # reuse old python 2 importer  (TODO: clean up)
        old_imp = _BLMImporter(os.path.dirname(self.filename))
        modname = 'blm.{modname}'.format(modname=self.modname)
        loader = old_imp.find_module(modname)
        return loader.load_module(modname)

    def exec_module(self, spec):
        pass

    def load_module(self, spec):
        pass


class BlmImporter:

    def find_spec(self, fullname, path, target=None):

        if path is not None:
            for p in path:
                if p in __path__:
                    modname = fullname.split('.')[-1]
                    filename = os.path.join(p, '{modname}.py'.format(
                        modname=modname))
                    if os.path.exists(filename):
                        loader = BlmLoader(modname, filename)
                        spec = importlib.machinery.ModuleSpec(fullname, loader)
                        return spec

sys.meta_path.insert(0, BlmImporter())

# sys.path_hooks.append(_BLMImporter)
# for p in __path__:
#     sys.path_importer_cache.pop(p,None)
