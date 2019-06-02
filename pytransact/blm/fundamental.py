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

from pytransact.object.to import TO
from pytransact.object.attribute import Bool, ToiRef
from pytransact.object.restriction import Quantity


class AccessHolder(TO):

    _privilege_attrs = []  # maybe use a property instead?

    class _privileges(ToiRef()):

        def on_computation(attr, self):
            result = [self]
            for attribute in self._privilege_attrs:
                for v in self[attribute]:
                    result.append(v)
            return result

    class super(Bool(Quantity(1))):
        default = [False]
