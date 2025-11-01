# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Test for relation hierarchy table
"""
import pytest

import sqlalchemy as sa

from osgende.relations import RelationHierarchy

@pytest.fixture
def rel_table(db):
    return db.add_table(RelationHierarchy(db.db.metadata, "test",
                                          db.db.osmdata.relation))


def test_flat_relations(db, rel_table):
    db.import_data("""
        r1 Mn1@,n3@
        r2 Mw23@,w3@
        """)
    rel_table.has_data()


def test_simple_hierarchy(db, rel_table):
    db.import_data("""
        r1 Mr2@
        r2 Mr3@
        r3 Mw3@
        """)
    rel_table.has_data({ 'parent' : 1, 'child' : 2, 'depth' : 2 },
                       { 'parent' : 1, 'child' : 3, 'depth' : 3 },
                       { 'parent' : 2, 'child' : 3, 'depth' : 2 })


def test_ciruclar_dependancy(db, rel_table):
    db.import_data("""
        r1 Mr2@
        r2 Mr1@
        """)
    rel_table.has_data({ 'parent' : 1, 'child' : 2, 'depth' : 2 },
                       { 'parent' : 2, 'child' : 1, 'depth' : 2 })


def test_self_contained(db, rel_table):
    db.import_data("""
        r1 Mr2@,r1@
        r2 Mw1@
        """)
    rel_table.has_data({ 'parent' : 1, 'child' : 2, 'depth' : 2 })


class TestHierarchyTableWithChanges:

    @pytest.fixture(autouse=True)
    def setup(self, db):
        self.hier_table = db.add_table(
            RelationHierarchy(db.db.metadata, "test",
                              db.db.osmdata.relation,
                              track_changes=True))

        db.import_data("""
            r1 Mr2@
            r2 Mr3@
            r3 Mw3@
            r100 Mw34@
            """)

    def has_data(self, *content):
        as_array = [{'parent': c[0], 'child': c[1], 'depth': c[2]} for c in content]
        self.hier_table.has_data(*as_array)

    def has_changes(self, *content):
        as_array = [{'action': c[0], 'parent': c[1], 'child': c[2]} for c in content]
        self.hier_table._contains(self.hier_table.table.change, as_array)

    def test_add_simple_parent(self, db):
        db.update_data("r55 Mr2@,r100@")

        self.has_data(
            (1, 2, 2),
            (1, 3, 3),
            (2, 3, 2),
            (55, 2, 2),
            (55, 3, 3),
            (55, 100, 2))

        self.has_changes(
            ('A', 55, 2),
            ('A', 55, 3),
            ('A', 55, 100))

        db.update_data("r56 Mw45@")

        self.has_changes()

    def test_delete_parent(self, db):
        db.update_data("r2 Mw44@")

        self.has_changes(
            ('D', 1, 3),
            ('D', 2, 3))
