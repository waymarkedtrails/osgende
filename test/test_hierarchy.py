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

