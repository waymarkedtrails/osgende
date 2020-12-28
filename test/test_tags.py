# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende.
# Copyright (C) 2020 Sarah Hoffmann

import unittest
from collections import OrderedDict

from osgende.common.tags import TagStore

class TestTagStore(unittest.TestCase):

    def test_firstof(self):
        t = TagStore({'one': 'foo', 'two': 'bar'})
        self.assertEqual('foo', t.firstof('one', 'two'))
        self.assertEqual('bar', t.firstof('three', 'two'))
        self.assertEqual('baz', t.firstof('three', 'siz', default='baz'))
        self.assertIsNone(t.firstof('three', 'siz'))

    def test_get_booleans(self):
        t = TagStore(dict(a='true', b='True', c='no', d='noo', e='yes', f='false'))
        self.assertEqual(dict(a=True, b=True, c=False, e=True, f=False),
                         t.get_booleans())

    def test_get_wikipedia_url(self):
        self.assertEqual('https://en.wikipedia.org/wiki/Something',
                         TagStore(dict(wikipedia='Something')).get_wikipedia_url())
        self.assertEqual('https://de.wikipedia.org/wiki/Etwas',
                         TagStore(dict(wikipedia='de:Etwas')).get_wikipedia_url())
        self.assertEqual('https://de.wikipedia.org/wiki/Etw%20as',
                         TagStore(dict(wikipedia='de:Etw as')).get_wikipedia_url())
        self.assertEqual('https://est.wikipedia.org/wiki/Nada',
                         TagStore({'wikipedia:est': 'Nada'}).get_wikipedia_url())

        self.assertIsNone(TagStore(dict(name='wikipedia')).get_wikipedia_url())
        self.assertIsNone(TagStore(dict(wikipedia='http://en.wikipedia.org/wiki/Something')).get_wikipedia_url())
        self.assertIsNone(TagStore(dict(wikipedia='xxxx:Entry')).get_wikipedia_url())

        self.assertIsNone(TagStore({'wikipedia:x': 'Nada'}).get_wikipedia_url())
        self.assertIsNone(TagStore({'wikipedia:1234': 'Nada'}).get_wikipedia_url())

    def test_get_wikipedia_tags(self):
        self.assertEqual(dict(en='Something'),
                         TagStore(dict(wikipedia='Something')).get_wikipedia_tags())
        self.assertEqual(dict(de='Etwas'),
                         TagStore(dict(wikipedia='de:Etwas')).get_wikipedia_tags())
        self.assertEqual(dict(de='Etw as'),
                         TagStore(dict(wikipedia='de:Etw as')).get_wikipedia_tags())
        self.assertEqual(dict(est='Nada'),
                         TagStore({'wikipedia:est': 'Nada'}).get_wikipedia_tags())

        self.assertEqual({}, TagStore(dict(name='wikipedia')).get_wikipedia_tags())
        self.assertEqual({}, TagStore(dict(wikipedia='http://en.wikipedia.org/wiki/Something')).get_wikipedia_tags())
        self.assertEqual({}, TagStore(dict(wikipedia='xxxx:Entry')).get_wikipedia_tags())

        self.assertEqual({}, TagStore({'wikipedia:x': 'Nada'}).get_wikipedia_tags())
        self.assertEqual({}, TagStore({'wikipedia:1234': 'Nada'}).get_wikipedia_tags())

    def test_get_url(self):
        self.assertEqual('http://foo.bar',
                         TagStore({'url' : 'http://foo.bar',
                                   'website' : 'https://foo.bar'}).get_url())
        self.assertEqual('http://google.com',
                         TagStore({'website' : 'http://google.com'}).get_url())
        self.assertEqual('ftp://google.com',
                         TagStore({'website' : 'ftp://google.com'}).get_url(schemes=('ftp','http')))
        self.assertEqual('http://myspace.de/me%27me%22',
                         TagStore({'url' : 'http://myspace.de/me\'me"'}).get_url())
        self.assertIsNone(TagStore({'url' : 'ftp://foo.bar'}).get_url())
        self.assertIsNone(TagStore({'url' : 'foo.bar'}).get_url())

    def test_get_image_url(self):
        self.assertEqual('http://foo.bar',
                         TagStore({'image' : 'http://foo.bar'}).get_url(keys=['image']))
        self.assertIsNone(TagStore({'image' : 'foo.bar'}).get_url(keys=['image']))

    def test_get_length(self):
        with self.assertRaises(ValueError):
            TagStore({}).get_length(unit='xx')

        self.assertIsNone(TagStore({'name' : 'A'}).get_length('ele'))
        self.assertIsNone(TagStore({'dist' : '100xx'}).get_length('dist'))

        self.assertEqual(10, TagStore({'dist' : '10'}).get_length('dist'))
        self.assertEqual(10, TagStore({'dist' : '10km'}).get_length('dist'))
        self.assertEqual(10.3, TagStore({'dist' : '10.3 km'}).get_length('dist'))
        self.assertEqual(10.3, TagStore({'dist' : '10,3'}).get_length('dist'))
        self.assertEqual(0.1, TagStore({'dist' : '100m'}).get_length('dist'))
        self.assertEqual(0.1, TagStore({'dist' : '100 m'}).get_length('dist'))

    def test_get_length_convert(self):
        self.assertAlmostEqual(10000,
            TagStore({'ele': '10km'}).get_length('ele', unit='m'))
        self.assertAlmostEqual(3.048,
            TagStore({'ele': '10'}).get_length('ele', unit='m', default='ft'))

    def test_make_localized(self):
        tags = OrderedDict((('name', 'B'), ('name:en', 'A')))
        self.assertDictEqual({'name' : 'A'},
                             TagStore.make_localized(tags, ('en', 'de')))

        tags = OrderedDict((('name:en', 'B'), ('name', 'A')))
        self.assertDictEqual({'name' : 'B'},
                             TagStore.make_localized(tags, ('en', 'de')))

        tags = OrderedDict((('name:en', 'B'), ('name:de', 'A')))
        self.assertDictEqual({'name' : 'B'},
                             TagStore.make_localized(tags, ('en', 'de')))

        tags = OrderedDict((('name:en', 'B'), ('name:de', 'A')))
        self.assertDictEqual({'name' : 'A'},
                             TagStore.make_localized(tags, ('de', 'en')))

        tags = OrderedDict((('name', 'B'), ('name:fr', 'A')))
        self.assertDictEqual({'name' : 'B', 'name:fr' : 'A'},
                             TagStore.make_localized(tags, ('en', 'de')))

    def test_get_prefixed(self):
        tags = TagStore({'name': 'A', 'name:fr': 'B', 'ref': 'C', 'name:XXX': 'D'})
        self.assertDictEqual(dict(fr='B', XXX='D'), tags.get_prefixed('name:'))

        tags = TagStore({'name': 'A', 'name:fr': 'B', 'ref': 'C', 'name:XXX': 'D'})
        self.assertDictEqual({}, tags.get_prefixed('addr:'))
