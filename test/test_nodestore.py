# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende.
# Copyright (C) 2024 Sarah Hoffmann

import pytest

from osgende.common.nodestore import NodeStore, NodeStorePoint

def test_point_wkb():
    assert '0101000020e610000000000000000024400000000000000840'\
             == NodeStorePoint(10.0, 3.0).wkb()
    assert '0101000020230f000000000000000051c00000000000002840'\
             == NodeStorePoint(-68.0, 12.0).wkb(3875)


def test_point_mercator():
    merc = NodeStorePoint(20.2, 45.5).to_mercator()
    assert merc.x == pytest.approx(2248653.7140241)
    assert merc.y == pytest.approx(5700582.7324697)


def test_create_reload(tmpdir):
    store = NodeStore(str(tmpdir / 'test.store'))

    for i in range(25500, 26000):
        store[i] = NodeStorePoint(1, i/1000.0)

    store.close()
    del store

    store = NodeStore(str(tmpdir / 'test.store'))

    for i in range(25500, 26000):
        assert store[i].y == i/1000.0

    with pytest.raises(KeyError):
        store[1000]

    with pytest.raises(KeyError):
        store[0]

    for i in range(100055500, 100056000):
        store[i] = NodeStorePoint(i/10000000.0, 1)

    with pytest.raises(KeyError):
        store[16001]

    store.close()
    del store

    store = NodeStore(str(tmpdir / '/test.store'))

    for i in range(25500, 26000):
        assert store[i].y == i/1000.0

    for i in range(100055500, 100056000):
        assert store[i] == NodeStorePoint(i/10000000.0, 1)

    store.close()
