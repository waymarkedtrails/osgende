# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2020 Sarah Hoffmann

import unittest

from osgende.mapdb import _Tables

class TestTablesDict(unittest.TestCase):

    def setUp(self):
        self.tables = _Tables()
        self.tables.add('foo', 23)
        self.tables.add('bar', 45)

    def test_get_as_dict(self):
        self.assertEqual(23, self.tables['foo'])
        self.assertEqual(45, self.tables['bar'])

    def test_get_as_dict_fail(self):
        with self.assertRaises(KeyError):
            self.tables['baz']

    def test_in(self):
        self.assertTrue('foo' in self.tables)
        self.assertFalse('nothing' in self.tables)

    def test_get_as_attribute(self):
        self.assertEqual(23, self.tables.foo)
        self.assertEqual(45, self.tables.bar)

    def test_get_as_attribute_fail(self):
        with self.assertRaises(AttributeError):
            self.tables.baz

    def test_has_attribute(self):
        self.assertTrue(hasattr(self.tables, 'foo'))
        self.assertFalse(hasattr(self.tables, 'foot'))

    def test_len(self):
        self.assertEqual(2, len(self.tables))
