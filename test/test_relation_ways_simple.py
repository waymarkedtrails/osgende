# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Tests for RelationWaysTable
"""
import pytest

from osgende.lines import RelationWayTable

@pytest.fixture
def test_table(db):
    return db.add_table(RelationWayTable(db.db.metadata, "test",
                                         db.db.osmdata.way,
                                         db.db.osmdata.relation))


def test_create_single(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n3
        w2 Nn3,n4,n5
        r1 Mw1@,w2@
        """)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
        { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1] }
        )


def test_create_overlapping_rels(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n3
        w2 Nn3,n4,n5
        w3 Nn10,n11
        r1 Mw1@,w2@
        r2 Mw2@,w3@
        """)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
        { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2] },
        { 'id' : 3, 'nodes' : [10, 11], 'rels' : [2] }
        )


def test_create_rel_without_way(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n3
        w2 Nn3,n4,n5
        r1 Mw1@,w2@,w3@
        """)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
        { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1] }
        )


def test_create_way_without_rel(db, test_table):
    db.import_data("""\
        w1 Nn1,n2,n3
        w2 Nn3,n4,n5
        r1 Mw1@
        """)
    test_table.has_data(
        { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] }
        )


class TestSimpleRelationWaysUpdateSimpleWayChanges:

    @pytest.fixture(autouse=True)
    def setup(self, db, test_table):
        db.import_data("""\
            n3 x10.0 y10.0
            w1 Nn1,n2,n3
            w2 Nn3,n4,n5
            r1 Mw1@,w2@
            """)


    def test_update_move_node(self, db, test_table):
        db.update_data("n3 x10.1 y10.1")
        test_table.has_changes()
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1] }
            )

    def test_update_way_tags_only(self, db, test_table):
        db.update_data("w1 v2 Tfoo=bar Nn1,n2,n3")
        test_table.has_changes()
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1] }
            )

    def test_update_add_node_to_way(self, db, test_table):
        db.update_data("w1 v2 Nn1,n23,n2,n3")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 23, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1] }
            )

    def test_update_remove_node_from_way(self, db, test_table):
        db.update_data("w2 v2 Nn3,n4")
        test_table.has_changes('M2')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'nodes' : [3, 4], 'rels' : [1] }
            )

    def test_update_shorten_way_to_one_node(self, db, test_table):
        db.update_data("w2 v2 Nn3")
        test_table.has_changes('M2')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'nodes' : [3], 'rels' : [1] }
            )


class TestSimpleRelationWaysUpdateSimpleRelationChanges:

    @pytest.fixture(autouse=True)
    def setup(self, db, test_table):
        db.import_data("""\
            n3 x10.0 y10.0
            w1 Nn1,n2,n3
            w2 Nn3,n4,n5
            r1 Mw1@,w2@
            r2 Mw1@,w2@
            """)

    def is_unchanged(self, test_table):
        test_table.has_changes()
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1, 2] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2] }
            )

    def test_update_add_relation(self, db, test_table):
        db.update_data("r20 Mw1@")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1, 2, 20] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2] }
            )

    def test_update_delete_relation(self, db, test_table):
        db.update_data("r2 v2 dD")
        test_table.has_changes('M1', 'M2')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1] }
            )

    def test_update_add_way(self, db, test_table):
        db.update_data("""\
                w3 v1 Nn10,n11
                r1 v2 Mw1@,w3@,w2@
                """)
        test_table.has_changes('A3')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1, 2] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2] },
            { 'id' : 3, 'nodes' : [10, 11], 'rels' : [1] }
            )

    def test_update_remove_way(self, db, test_table):
        db.update_data("r2 v2 Mw2@")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2] }
            )
    def test_update_remove_all_ways(self, db, test_table):
        db.update_data("r2 v2 Mw2@\nr1 v2 Mw2@")
        test_table.has_changes('D1')
        test_table.has_data(
            { 'id' : 2, 'nodes' : [3, 4, 5], 'rels' : [1, 2] }
            )

    def test_update_relation_tags(self, db, test_table):
        db.update_data("r2 v2 Tname=foo Mw1@,w2@")
        self.is_unchanged(test_table)

    def test_update_relation_role(self, db, test_table):
        db.update_data("r2 v2 Mw1@foo,w2@bar")
        self.is_unchanged(test_table)

    def test_update_add_node_member(self, db, test_table):
        db.update_data("r2 v2 Mw1@,w2@,n2@")
        self.is_unchanged(test_table)

    def test_update_add_relation_member(self, db, test_table):
        db.update_data("r2 v2 Mr3@,w1@,w2@")
        self.is_unchanged(test_table)

    def test_double_update(self, db, test_table):
        db.update_data("r20 Mw1@")
        test_table.has_changes('M1')
        db.update_data("r2 v2 Mw1@\nr1 v2 Mw1@")
        test_table.has_changes('D2')
