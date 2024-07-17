# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Tests for TransformedTable used together with a Filtered View.
"""
import pytest

import sqlalchemy as sa

from osgende.generic import TransformedTable, FilteredTable

class TransformedTestTable(TransformedTable):

    def __init__(self, db, base):
        super().__init__(db.metadata, "test", base)

    def add_columns(self, table, src):
        table.append_column(sa.Column('a', sa.Integer))
        table.append_column(sa.Column('b', sa.Integer))

    def transform(self, obj):
        t = obj.tags
        if 'ignore' in t:
            return None

        return { 'a' : int(t['foo']) if 'foo' in t else None,
                 'b' : int(t.get('bar', 0)) }


@pytest.fixture(params=[True, False])
def test_table(request, db):
    filtered = db.add_table(FilteredTable(db.db.metadata, "filter", db.db.osmdata.node,
                                          sa.literal_column("tags ? 'include'"),
                                          view_only=request.param))
    table = db.add_table(TransformedTestTable(db.db, filtered.table))

    db.import_data("""
        n1 Tfoo=4,name=what x3 y2
        n2 Tinclude=yes,foo=3,go=go x1 y2
        n3 Tinclude=yes,foo=100,bar=4 x0 y0
        n5 Tinclude=yes,bar=49,FOO=4 x0 y0
        n6 Tinclude=yes,foo=100,bar=4,ignore=1 x0 y0
        n10 Tinclude=yes,xxx=zzz x0 y0
        """)

    return table


def test_create(db, test_table):
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_delete_data(db, test_table):
    db.update_data("n5 v2 dD x0 y0")
    test_table.has_changes('D5')
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_delete_filtered_data(db, test_table):
    db.update_data("n1 v2 dD x0 y0")
    test_table.has_changes()
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_delete_unrelated_data(db, test_table):
    db.update_data("n6 v2 dD x0 y0")
    test_table.has_changes()
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_ignore_data(db, test_table):
    db.update_data("n2 v2 Tinclude=yes,ignore=1,foo=3,go=go x1 y2")
    test_table.has_changes('D2')
    test_table.has_data(
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_unignore_data(db, test_table):
    db.update_data("n6 v2 Tinclude=yes,foo=100,bar=4 x0 y0")
    test_table.has_changes('A6')
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 6, 'a': 100, 'b': 4},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_filter_data(db, test_table):
    db.update_data("n2 v2 Tfoo=3,go=go x1 y2")
    test_table.has_changes('D2')
    test_table.has_data(
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_unfilter_data(db, test_table):
    db.update_data("n1 Tinclude=yes,foo=4,name=what x3 y2")
    test_table.has_changes('A1')
    test_table.has_data(
            {'id': 1, 'a': 4, 'b': 0},
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_delete_modify_relevant_data(db, test_table):
    db.update_data("n3 Tinclude=yes,foo=99,bar=4 x0 y0")
    test_table.has_changes('M3')
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 99, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_delete_modify_irrelevant_data(db, test_table):
    db.update_data("n10 Tinclude=yes,xxx=zzz,kk=gg x0 y0")
    test_table.has_changes()
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            )

def test_add_new_data(db, test_table):
    db.update_data("n99 Tinclude=yes,foo=5,bar=5 x0 y0")
    test_table.has_changes('A99')
    test_table.has_data(
            {'id': 2, 'a': 3, 'b': 0},
            {'id': 3, 'a': 100, 'b': 4},
            {'id': 5, 'a': None, 'b': 49},
            {'id': 10, 'a': None, 'b': 0},
            {'id': 99, 'a': 5, 'b': 5},
            )
