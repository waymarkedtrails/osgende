# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Tests for RelationWaysTable with geometries enabled.
"""
import pytest

from osgende.lines import RelationWayTable

from db_compare import Line

NODES = { 1: (1.0, 2.1), 2: (1.1, 2.0), 3: (1.15, 1.9),
          4: (1.2, 1.886), 5: (1.0, 1.7),
          10: (10.342, -4.3), 11: (10.344, -4.332),
          111: (1.0, 2.1)}

@pytest.fixture
def test_table(db):
    return db.add_table(RelationWayTable(db.db.metadata, "test",
                                         db.db.osmdata.way,
                                         db.db.osmdata.relation, db.db.osmdata))

def test_create_single(db, test_table):
    db.import_data("""\
        w1 Nn1,n3
        w2 Nn3,n5
        r1 Mw1@,w2@
        """, NODES)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 3], 'rels' : [1], 'geom' : Line(1, 3) },
        { 'id' : 2, 'nodes' : [3, 5], 'rels' : [1], 'geom' : Line(3, 5) }
        )

def test_create_one_point_way(db, test_table):
    db.import_data("""\
        w1 Nn1,n3
        w2 Nn5,n5
        r1 Mw1@,w2@
        """, NODES)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 3], 'rels' : [1], 'geom' : Line(1, 3) },
        { 'id' : 2, 'nodes' : [5, 5], 'rels' : [1], 'geom' : Line(5, 5) }
        )

def test_create_overlapping_rels(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n3
        w2 Nn3,n4,n5
        w3 Nn10,n11
        r1 Mw1@,w2@
        r2 Mw2@,w3@
        """, NODES)
    test_table.has_data(
        { 'id': 1, 'nodes': [1, 2, 3], 'rels': [1], 'geom': Line(1, 2, 3) },
        { 'id': 2, 'nodes': [3, 4, 5], 'rels': [1, 2], 'geom': Line(3, 4, 5) },
        { 'id': 3, 'nodes': [10, 11], 'rels': [2], 'geom': Line(10, 11) }
        )

def test_create_rel_without_way(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n3
        w2 Nn3,n4,n5
        r1 Mw1@,w2@,w3@
        """, NODES)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1], 'geom': Line(1, 2, 3) },
        { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1], 'geom': Line(3, 4 ,5) }
        )

def test_create_way_without_rel(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n3
        w2 Nn3,n4,n5
        r1 Mw1@
        """, NODES)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1], 'geom': Line(1, 2, 3) }
        )

def test_create_way_with_duplicate_node(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n2,n4,n5
        r1 Mw1@
        """, NODES)
    test_table.has_data(
        { 'id': 1, 'nodes': [1, 2, 2, 4, 5], 'rels': [1],
          'geom': Line(1, 2, 2, 4, 5) }
        )

def test_create_way_with_duplicate_location(db, test_table):
    db.import_data("""\
        w1 Nn2,n1,n111,n4,n5
        r1 Mw1@
        """, NODES)
    test_table.has_data(
        { 'id': 1, 'nodes': [2, 1, 111, 4, 5], 'rels': [1],
          'geom': Line(2, 1, 111, 4, 5) }
        )


class TestSimpleRelationWaysUpdateSimpleWayChanges:

    @pytest.fixture(autouse=True)
    def setup(self, db, test_table):
        db.import_data("""\
            w1 Nn1,n2,n3
            w2 Nn3,n4,n5
            r1 Mw1@,w2@
            """, NODES)

    def test_update_move_node(self, db, test_table):
        db.update_data("n3 x1.0 y2.0")
        test_table.has_changes('M1', 'M2')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1],
              'geom' : Line(1, 2, (1.0, 2.0)) },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1],
              'geom' : Line((1.0, 2.0), 4, 5) }
            )

    def test_update_way_tags_only(self, db, test_table):
        db.update_data("w1 v2 Tfoo=bar Nn1,n2,n3")
        test_table.has_changes()
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1], 'geom' : Line(1, 2, 3) },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1], 'geom' : Line(3, 4, 5) }
            )

    def test_update_add_node_to_way(self, db, test_table):
        db.update_data("w1 v2 Nn1,n10,n2,n3")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 10, 2, 3], 'rels' : [1],
              'geom' : Line(1, 10, 2, 3) },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1],
              'geom' : Line(3, 4, 5) }
            )

    def test_update_remove_node_from_way(self, db, test_table):
        db.update_data("w2 v2 Nn3,n4")
        test_table.has_changes('M2')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1], 'geom' : Line(1, 2, 3) },
            { 'id' : 2, 'nodes' : [3, 4], 'rels' : [1], 'geom' : Line(3, 4) }
            )

    def test_update_shorten_way_to_one_node(self, db, test_table):
        db.update_data("w2 v2 Nn3")
        test_table.has_changes('D2')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1], 'geom' : Line(1, 2, 3) }
            )


class TestSimpleRelationWaysUpdateSimpleRelationChanges:

    @pytest.fixture(autouse=True)
    def setup(self, db, test_table):
        db.import_data("""\
            w1 Nn1,n2,n3
            w2 Nn3,n4,n5
            r1 Mw1@,w2@
            r2 Mw1@,w2@
            """, NODES)
        self.test_table = test_table

    def is_unchanged(self):
        self.test_table.has_changes()
        self.test_table.has_data(
            { 'id': 1, 'nodes': [1, 2, 3], 'rels': [1, 2], 'geom': Line(1, 2, 3) },
            { 'id': 2, 'nodes': [3, 4, 5], 'rels': [1, 2], 'geom': Line(3, 4, 5) }
            )

    def test_update_add_relation(self, db, test_table):
        db.update_data("r20 Mw1@")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1, 2, 20], 'geom': Line(1, 2, 3) },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2], 'geom': Line(3, 4, 5) }
            )

    def test_update_delete_relation(self, db, test_table):
        db.update_data("r2 v2 dD")
        test_table.has_changes('M1', 'M2')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1], 'geom': Line(1, 2, 3) },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1], 'geom': Line(3, 4, 5) }
            )

    def test_update_add_way(self, db, test_table):
        db.update_data("""\
                w3 v1 Nn10,n11
                r1 v2 Mw1@,w3@,w2@
                """)
        test_table.has_changes('A3')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1, 2], 'geom': Line(1, 2, 3) },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2], 'geom': Line(3, 4, 5) },
            { 'id' : 3, 'nodes' : [10, 11], 'rels' : [1], 'geom': Line(10, 11) }
            )

    def test_update_remove_way(self, db, test_table):
        db.update_data("r2 v2 Mw2@")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1], 'geom': Line(1, 2, 3) },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2], 'geom': Line(3, 4, 5) }
            )

    def test_update_relation_tags(self, db, test_table):
        db.update_data("r2 v2 Tname=foo Mw1@,w2@")
        self.is_unchanged()

    def test_update_relation_role(self, db, test_table):
        db.update_data("r2 v2 Mw1@foo,w2@bar")
        self.is_unchanged()

    def test_update_add_node_member(self, db, test_table):
        db.update_data("r2 v2 Mw1@,w2@,n2@")
        self.is_unchanged()

    def test_update_add_relation_member(self, db, test_table):
        db.update_data("r2 v2 Mr3@,w1@,w2@")
        self.is_unchanged()
