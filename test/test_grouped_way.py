# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Tests for grouped-way table
"""

import pytest
import sqlalchemy as sa

from osgende.lines import GroupedWayTable

@pytest.fixture
def table(db):
    return db.add_table(GroupedWayTable(db.db.metadata, 'test',
                                        db.db.osmdata.way, ('tags', )))

def H(d):
    res = []
    for k, v in d.items():
        res.extend([{'id': k, 'child': x} for x in v])

    return res


def test_import_create(db, table):
    db.import_data("""
     w5 Ttype=foo Nn1,n2,n3
     w6 Ttype=foo Nn4,n5
     w7 Ttype=foo Nn5,n6
    """)
    table.has_data(*H({6 : [6, 7]}))


def test_import_circular(db, table):
    db.import_data("""
     w5 Ttype=foo Nn1,n2,n3
     w6 Ttype=foo Nn3,n4,n5
     w7 Ttype=foo Nn5,n6,n1
    """)
    table.has_data(*H({5 : [5, 6, 7]}))


def test_import_touching_with_different_tags(db, table):
    db.import_data("""
     w5 Ttype=foo,name=x Nn1,n2,n3,n4
     w6 Ttype=foo Nn4,n5
     w7 Ttype=foo Nn5,n6
    """)
    table.has_data(*H({6 : [6, 7]}))


def test_import_crossing_ways(db, table):
    db.import_data("""
     w1 Ttype=red Nn100,n102,n103
     w2 Ttype=red Nn101,n102,n104
    """)
    table.has_data(*H({1 : [1, 2]}))


def test_import_crossing_ways_with_different_tags(db, table):
    db.import_data("""
     w1 Ttype=red Nn100,n102,n103
     w2 Ttype=blue Nn101,n102,n104
    """)
    table.has_data()


def test_import_crossing_ways_without_touching(db, table):
    db.import_data("""
     w1 Ttype=red Nn100,n103
     w2 Ttype=red Nn101,n104
    """)
    table.has_data()


def test_update_add_single_independent_way(db, table):
    db.import_data("""
     w6 Ttype=foo Nn4,n5
     w7 Ttype=foo Nn5,n6
    """)
    db.update_data("w5 Ttype=foo Nn1,n2,n3")
    table.has_changes()
    table.has_data(*H({6 : [6, 7]}))


def test_update_add_grouped_independent_way(db, table):
    db.import_data("""
     w5 Ttype=foo Nn1,n2,n3
    """)
    db.update_data("""
     w6 Ttype=foo Nn4,n5
     w7 Ttype=foo Nn5,n6
    """)
    table.has_changes('A6')
    table.has_data(*H({6 : [6, 7]}))


def test_update_add_to_single_way_matching(db, table):
    db.import_data("""
     w5 Ttype=foo Nn1,n2,n3
    """)
    db.update_data("w6 Ttype=foo Nn3,n6,n5")
    table.has_changes('A6')
    table.has_data(*H({6 : [5, 6]}))


def test_update_add_to_single_way_not_matching(db, table):
    db.import_data("""
     w5 Ttype=foo Nn1,n2,n3
    """)
    db.update_data("w6 Ttype=bar Nn3,n6,n5")
    table.has_changes()
    table.has_data(*H({}))


def test_update_add_to_grouped_way_end_matching(db, table):
    db.import_data("""
     w6 Ttype=foo Nn4,n5
     w7 Ttype=foo Nn5,n6
    """)
    db.update_data("w5 Ttype=foo Nn6,n3,n2")
    table.has_changes('M6')
    table.has_data(*H({6 : [6, 7, 5]}))


def test_update_add_to_grouped_way_middle_matching(db, table):
    db.import_data("""
     w6 Ttype=foo Nn4,n5,n6
     w7 Ttype=foo Nn3,n6
    """)
    db.update_data("w5 Ttype=foo Nn5,n2")
    table.has_changes('M6')
    table.has_data(*H({6 : [6, 7, 5]}))


def test_update_add_to_grouped_way_connecting(db, table):
    db.import_data("""
     w6 Ttype=foo Nn4,n5
     w7 Ttype=foo Nn3,n6
    """)
    db.update_data("w5 Ttype=foo Nn5,n6")
    table.has_changes('A5')
    table.has_data(*H({5 : [6, 7, 5]}))


def test_update_add_to_grouped_way_connecting_grouped(db, table):
    db.import_data("""
     w6 Ttype=foo Nn4,n5,n6
     w7 Ttype=foo Nn3,n6
     w10 Ttype=foo Nn100,n102
     w11 Ttype=foo Nn101,n102
    """)
    db.update_data("w5 Ttype=foo Nn5,n100")
    table.has_changes('M6', 'D10')
    table.has_data(*H({6 : [6, 7, 5, 10, 11]}))


def test_update_delete_from_grouped_end_still_grouped(db, table):
    db.import_data("""
     w6 Ttype=foo Nn1,n2
     w7 Ttype=foo Nn2,n3
     w9 Ttype=foo Nn3,n6
    """)
    db.update_data("w9 v2 dD")
    table.has_changes('M6')
    table.has_data(*H({6 : [6, 7]}))


def test_update_delete_from_grouped_end_still_grouped_name_change(db, table):
    db.import_data("""
     w6 Ttype=foo Nn1,n2
     w7 Ttype=foo Nn2,n3
     w9 Ttype=foo Nn3,n6
    """)
    db.update_data("w6 v2 dD")
    table.has_changes('D6', 'A9')
    table.has_data(*H({9 : [7, 9]}))


def test_update_delete_from_grouped_end_singled(db, table):
    db.import_data("""
     w6 Ttype=foo Nn1,n2
     w7 Ttype=foo Nn2,n3
    """)
    db.update_data("w7 v2 dD")
    table.has_changes('D6')
    table.has_data()


def test_update_delete_from_grouped_middle_singled(db, table):
    db.import_data("""
     w6 Ttype=foo Nn1,n2
     w7 Ttype=foo Nn2,n3
     w9 Ttype=foo Nn3,n6
    """)
    db.update_data("w7 v2 dD")
    table.has_changes('D6')
    table.has_data()


def test_update_delete_from_grouped_middle_split(db, table):
    db.import_data("""
     w6 Ttype=foo Nn1,n2
     w7 Ttype=foo Nn2,n3
     w9 Ttype=foo Nn3,n6
     w10 Ttype=foo Nn5,n6
     w11 Ttype=foo Nn5,n4
    """)
    db.update_data("w9 v2 dD")
    table.has_changes('M6', 'A10')
    table.has_data(*H({6 : [6, 7], 10 : [10, 11]}))
