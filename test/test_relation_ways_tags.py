# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Tests for RelationWaysTable
"""
import pytest

import sqlalchemy as sa

from osgende.lines import RelationWayTable

class RelationWayWithTags(RelationWayTable):

    def __init__(self, db):
        super().__init__(db.metadata, "test", db.osmdata.way,
                         db.osmdata.relation)

    def add_columns(self, table):
        table.append_column(sa.Column('name', sa.Text))

    def transform_tags(self, oid, tags):
        if 'name' not in tags:
            return None

        return {'name' : tags['name'].upper()}


@pytest.fixture
def test_table(db):
    return db.add_table(RelationWayWithTags(db.db))


def test_create_single(db, test_table):
    db.import_data("""\
        w1 Tname=foo Nn1,n2,n3
        w2 Tname=bar,name2=yy Nn3,n4,n5
        w10 Nn10,n11
        r1 Mw1@,w2@,w10@
        """)
    test_table.has_data(
        { 'id' : 1, 'name' : 'FOO', 'nodes' : [1, 2, 3], 'rels' : [1] },
        { 'id' : 2, 'name' : 'BAR', 'nodes' : [3, 4, 5], 'rels' : [1] }
        )

def test_create_overlapping_rels(db, test_table):
    db.import_data("""\
        w1 Tname=1 Nn1,n2,n3
        w2 Tname=2 Nn3,n4,n5
        w3 Tname=3 Nn10,n11
        w4 Tfo=ba Nn23,n12
        r1 Mw1@,w2@,w4@
        r2 Mw4@,w2@,w3@
        """)
    test_table.has_data(
        { 'id' : 1, 'name' : '1', 'nodes' : [1, 2, 3], 'rels' : [1] },
        { 'id' : 2, 'name' : '2', 'nodes' : [3, 4, 5], 'rels' : [1, 2] },
        { 'id' : 3, 'name' : '3', 'nodes' : [10, 11], 'rels' : [2] }
        )

def test_create_ignore_unused_way(db, test_table):
    db.import_data("""\
        w1 Tname=a Nn1,n2,n3
        w2 Tname=b Nn3,n4,n5
        w12 Tname=c Nn200,n201,n202
        r1 Mw1@,w2@,w3@
        """)
    test_table.has_data(
        { 'id' : 1, 'name' : 'A', 'nodes' : [1, 2, 3], 'rels' : [1] },
        { 'id' : 2, 'name' : 'B', 'nodes' : [3, 4, 5], 'rels' : [1] }
        )


class TestSimpleRelationWaysUpdateSimpleWayChanges:

    @pytest.fixture(autouse=True)
    def setup(self, db, test_table):
        db.import_data("""\
            n3 x10.0 y10.0
            w1 Tname=w1 Nn1,n2,n3
            w2 Tname=w2 Nn3,n4,n5
            w3 Nn99,n98
            r1 Mw1@,w2@,w3@
            """)

        self.test_table = test_table

    def is_unchanged(self):
        self.test_table.has_changes()
        self.test_table.has_data(
            { 'id' : 1, 'name' : 'W1', 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'name' : 'W2', 'nodes' : [3, 4, 5], 'rels' : [1] }
            )

    def test_update_move_node(self, db, test_table):
        db.update_data("n3 x10.1 y10.1")
        self.is_unchanged()

    def test_update_unaffected_way_tags_only(self, db, test_table):
        db.update_data("w1 v2 Tfoo=bar,name=w1 Nn1,n2,n3")
        self.is_unchanged()

    def test_update_way_tags_without_changed_output(self, db, test_table):
        db.update_data("w1 v2 Tname=W1 Nn1,n2,n3")
        self.is_unchanged()

    def test_update_add_node_to_way(self, db, test_table):
        db.update_data("w1 v2 Tname=wnew Nn1,n23,n2,n3")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'name' : 'WNEW', 'nodes' : [1, 23, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'name' : 'W2', 'nodes' : [3, 4, 5], 'rels' : [1] }
            )

    def test_update_remove_node_from_way(self, db, test_table):
        db.update_data("w2 v2 Tname=W2,x=y Nn3,n4")
        test_table.has_changes('M2')
        test_table.has_data(
            { 'id' : 1, 'name' : 'W1', 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'name' : 'W2', 'nodes' : [3, 4], 'rels' : [1] }
            )

    def test_update_remove_relevant_tag(self, db, test_table):
        db.update_data("w2 v2 Nn3,n4,n5")
        test_table.has_changes('D2')
        test_table.has_data(
            { 'id' : 1, 'name' : 'W1', 'nodes' : [1, 2, 3], 'rels' : [1] }
            )

    def test_update_add_relevant_tag(self, db, test_table):
        db.update_data("w3 v2 Tname=w3 Nn99,n98")
        test_table.has_changes('A3')
        test_table.has_data(
            { 'id' : 1, 'name' : 'W1', 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'name' : 'W2', 'nodes' : [3, 4, 5], 'rels' : [1] },
            { 'id' : 3, 'name' : 'W3', 'nodes' : [99, 98], 'rels' : [1] }
            )


class TestSimpleRelationWaysUpdateSimpleRelationChanges:

    @pytest.fixture(autouse=True)
    def setup(self, db, test_table):
        db.import_data("""\
            n3 x10.0 y10.0
            w1 Tname=hin Nn1,n2,n3
            w2 Tname=Her Nn3,n4,n5
            w44 Tnam=incomplete Nn44,n55,n66
            r1 Mw1@,w2@
            r2 Mw1@,w2@,w44@
            """)
        self.test_table = test_table

    def is_unchanged(self):
        self.test_table.has_changes()
        self.test_table.has_data(
            { 'id' : 1, 'name' : 'HIN', 'nodes' : [1, 2, 3], 'rels' : [1, 2] },
            { 'id' : 2, 'name' : 'HER', 'nodes' : [3, 4, 5], 'rels' : [1, 2] }
            )

    def test_update_add_relation(self, db, test_table):
        db.update_data("r20 Mw1@")
        test_table.has_changes('M1')
        test_table.has_data(
            { 'id' : 1, 'name' : 'HIN', 'nodes' : [1, 2, 3], 'rels' : [1, 2, 20] },
            { 'id' : 2, 'name' : 'HER', 'nodes' : [3, 4, 5], 'rels' : [1, 2] }
            )

    def test_update_delete_relation(self, db, test_table):
        db.update_data("r2 v2 dD")
        test_table.has_changes('M1', 'M2')
        test_table.has_data(
            { 'id' : 1, 'name' : 'HIN', 'nodes' : [1, 2, 3], 'rels' : [1] },
            { 'id' : 2, 'name' : 'HER', 'nodes' : [3, 4, 5], 'rels' : [1] }
            )

    def test_update_add_way_without_relevant_tag(self, db, test_table):
        db.update_data("""\
                w3 v1 Nn10,n11
                r1 v2 Mw1@,w3@,w2@
                """)
        self.is_unchanged()

    def test_update_remove_way_without_relevant_tags(self, db, test_table):
        db.update_data("r2 v2 Mw1@,w2@")
        self.is_unchanged()

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
