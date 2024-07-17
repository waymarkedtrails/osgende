# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Tests for PlainWayTable without modified tags.
"""
import pytest

from osgende.lines import PlainWayTable
from db_compare import Line


EXPECT_W101 = { 'id' : 101, 'tags' : { 'name' : 'first' },
                'nodes' : [1, 2], 'geom' : Line(1, 2) }
EXPECT_W103 = { 'id' : 103, 'tags' : { 'name' : 'second' },
                'nodes' : [34, 1, 36], 'geom' : Line(34, 1, 36) }

@pytest.fixture
def test_table(db):
    table = db.add_table(PlainWayTable(db.db.metadata, "test",
                                       db.db.osmdata.way, db.db.osmdata))

    db.import_data("""\
        w101 Tname=first Nn1,n2
        w103 Tname=second Nn34,n1,n36
    """,
    { 1: (1.0, 2.1), 2: (1.0, 2.2), 3: (1.0, 2.3),
          34: (0.9, 2.1), 36: (1.2, 2.5),
          101: (4.5, -34.1), 103: (4.51, -34.111)})

    return table

def test_create(test_table):
    test_table.has_data(EXPECT_W101, EXPECT_W103)

def test_update_add_way(db, test_table):
    db.update_data("""w3 Tx=y Nn101,n103""")
    test_table.has_changes('A3')
    test_table.has_data(EXPECT_W101, EXPECT_W103,
        {'id': 3, 'tags': { 'x': 'y'}, 'nodes': [101, 103], 'geom': Line(101, 103)}
        )

def test_update_delete_way(db, test_table):
    db.update_data("""w101 v2 dD""")
    test_table.has_changes('D101')
    test_table.has_data(EXPECT_W103)

def test_update_change_tags(db, test_table):
    db.update_data("""w101 v2 Tname=new Nn1,n2""")
    test_table.has_changes('M101')
    new_w101 = dict(EXPECT_W101)
    new_w101.update({'tags' : {"name" : "new"}})
    test_table.has_data(new_w101, EXPECT_W103)

def test_update_add_node(db, test_table):
    db.update_data("""w101 v2 Tname=first Nn1,n2,n3""")
    test_table.has_changes('M101')
    test_table.has_data(EXPECT_W103,
        { 'id' : 101, 'tags' : { 'name' : 'first' },
                'nodes' : [1, 2, 3], 'geom' : Line(1, 2, 3) })

def test_delete_node_invalidate_way(db, test_table):
    db.update_data("""w101 v2 Tname=first Nn1""")
    test_table.has_changes('D101')
    test_table.has_data(EXPECT_W103)

def test_move_node(db, test_table):
    db.update_data("""n2 v2 x0.9 y2.1""")
    test_table.has_changes('M101')
    test_table.has_data(EXPECT_W103,
        { 'id' : 101, 'tags' : { 'name' : 'first' },
                'nodes' : [1, 2], 'geom' : Line(1, (0.9, 2.1)) })
