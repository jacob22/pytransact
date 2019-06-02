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
Contains the singleton ContextBroker used by other modules to find their
RuntimeContext.
"""
import collections, _thread
from pytransact.patterns import Singleton

class ContextBroker(Singleton):
    """
    Do context brokering.
    """

    def __init__(self):
        """
        Initialise the object.
        """
        self.contextDict = collections.defaultdict(list)

    def pushContext(self, context):
        """
        Push a context onto the context stack

        Arguments: context - the context to register for the calling thread
        """
        self.contextDict[_thread.get_ident()].append(context)

    def popContext(self):
        """
        Pop a context off the context stack

        Returns:   The context object.
        """
        ident = _thread.get_ident()
        try:
            return self.contextDict[ident].pop()
        finally:
            if not self.contextDict[ident]:
                del self.contextDict[ident]

    @property
    def context(self):
        """
        Retrieve context.

        Returns:   Current context
        Raises:    LookupError when there is no current context.
        """
        return self.contextDict[_thread.get_ident()][-1]

    def __getattr__(self, attr):
        return getattr(self.context, attr)
