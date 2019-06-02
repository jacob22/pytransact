#! -*- coding: utf-8 -*-

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

from pytransact import textindexing


class Toi(set):

    id = 42,


class Attribute(object):

    def __init__(self, name, data):
        self.value = data


class IndexedAttribute(Attribute):

    def on_index(self, value, toi):
        return value


def test_index_attrs():
    toi = Toi()
    toi.add(IndexedAttribute('name', ['Some User (blärg)\N{interrobang} 你好，你怎麼']))
    toi.add(Attribute('unindexed', ['Ignore This!']))

    result = textindexing.indexDataForToi(toi)
    assert result == (42, ['blärg', 'some', 'user', '你好', '你怎麼'])


def test_index_toi():
    toi = Toi()
    def on_index(words):
        return toi.id, set(['foo', 'bar'])
    toi.on_index = on_index
    result = textindexing.indexDataForToi(toi)
    assert result == (42, ['bar', 'foo'])

    toi.add(IndexedAttribute('name', ['Some User']))
    result = textindexing.indexDataForToi(toi)
    assert result == (42, ['bar', 'foo'])

    otherid = 27,
    def on_index(words):
        return otherid, set(['foo', 'bar'])
    toi.on_index = on_index
    result = textindexing.indexDataForToi(toi)
    assert result == (27, ['bar', 'foo'])

    def on_index(words):
        words.update(['foo', 'bar'])
        return toi.id, words
    toi.on_index = on_index
    result = textindexing.indexDataForToi(toi)
    assert result == (42, ['bar', 'foo', 'some', 'user'])
