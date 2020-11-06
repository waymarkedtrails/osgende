# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende.
# Copyright (C) 2020 Sarah Hoffmann

import unittest
import tempfile

from osgende.common.nodestore import NodeStore, NodeStorePoint

class TestNodeStorePoint(unittest.TestCase):

    def test_wkb(self):
        self.assertEqual('0101000020e610000000000000000024400000000000000840',
                         NodeStorePoint(10.0, 3.0).wkb())
        self.assertEqual('0101000020230f000000000000000051c00000000000002840',
                         NodeStorePoint(-68.0, 12.0).wkb(3875))

    def test_mercator(self):
        merc = NodeStorePoint(20.2, 45.5).to_mercator()
        self.assertAlmostEqual(merc.x, 2248653.7140241)
        self.assertAlmostEqual(merc.y, 5700582.7324697)

class TestNodeStore(unittest.TestCase):

    def test_create_reload(self):
        with tempfile.TemporaryDirectory() as storage_dir:
            store = NodeStore(storage_dir + '/test.store')

            for i in range(25500, 26000):
                store[i] = NodeStorePoint(1, i/1000.0)

            store.close()
            del store

            store = NodeStore(storage_dir + '/test.store')

            for i in range(25500, 26000):
                self.assertEqual(store[i].y, i/1000.0)

            with self.assertRaises(KeyError):
                store[1000]

            with self.assertRaises(KeyError):
                store[0]

            for i in range(100055500, 100056000):
                store[i] = NodeStorePoint(i/10000000.0, 1)

            with self.assertRaises(KeyError):
                store[16001]

            store.close()
            del store

            store = NodeStore(storage_dir + '/test.store')

            for i in range(25500, 26000):
                self.assertEqual(store[i].y, i/1000.0)

            for i in range(100055500, 100056000):
                self.assertEqual(store[i], NodeStorePoint(i/10000000.0, 1))

            store.close()
