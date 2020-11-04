# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende.
# Copyright (C) 2020 Sarah Hoffmann

import unittest

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
