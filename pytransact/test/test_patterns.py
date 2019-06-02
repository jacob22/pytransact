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

from pytransact import patterns

def setup_module(mod):
    pass
    #

def teardown_module(mod):
    pass
    #
    
class TestPatterns():
    """ testing patterns
    """
    def testSingleton(self):
        instance1 = patterns.Singleton()
        instance2 = patterns.Singleton()
        assert instance1 == instance2
    
    def testextendable(self):
        instance1 = patterns.Extendable()
        instance2 = patterns.Extendable()
        assert instance2 == instance2
        
    # def testextends(self):
    #     obj=patterns.Singleton()
    #     print(type(obj), obj)
    #     instance1 = patterns.extends(obj)
    #     instance2 = patterns.extends(obj)
    #     print(type(instance1), instance1)
    #     print(type(instance2), instance2)
    #     assert instance1 == instance2
        
