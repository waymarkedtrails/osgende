# This file is part of Osgende
# Copyright (C) 2018 Sarah Hoffmann
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
"""
Tests for TransformedTable used together with a Filtered View.
"""

import unittest
import sqlalchemy as sa

from osgende.generic import TransformedTable, FilteredTable

from table_test_fixture import TableTestFixture

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


class TestTransformedTableViewFilteredTable(TableTestFixture):
    view_only = True

    baseimport = """
        n1 Tfoo=4,name=what x3 y2
        n2 Tinclude=yes,foo=3,go=go x1 y2
        n3 Tinclude=yes,foo=100,bar=4 x0 y0
        n5 Tinclude=yes,bar=49,FOO=4 x0 y0
        n6 Tinclude=yes,foo=100,bar=4,ignore=1 x0 y0
        n10 Tinclude=yes,xxx=zzz x0 y0
        """

    def create_tables(self, db):
        filtered = FilteredTable(db.metadata, "filter", db.osmdata.node,
                                 sa.text("tags ? 'include'"),
                                 view_only=self.view_only)
        test = TransformedTestTable(db, filtered)
        return (filtered, test)

    def test_create(self):
        self.import_data(self.baseimport)
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_delete_data(self):
        self.import_data(self.baseimport)
        self.update_data("n5 v2 dD x0 y0")
        self.has_changes("test_changeset", ['D5'])
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_delete_filtered_data(self):
        self.import_data(self.baseimport)
        self.update_data("n1 v2 dD x0 y0")
        self.has_changes("test_changeset", [])
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_delete_unrelated_data(self):
        self.import_data(self.baseimport)
        self.update_data("n6 v2 dD x0 y0")
        self.has_changes("test_changeset", [])
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_ignore_data(self):
        self.import_data(self.baseimport)
        self.update_data("n2 v2 Tinclude=yes,ignore=1,foo=3,go=go x1 y2")
        self.has_changes("test_changeset", ['D2'])
        self.table_equals("test", [
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_unignore_data(self):
        self.import_data(self.baseimport)
        self.update_data("n6 v2 Tinclude=yes,foo=100,bar=4 x0 y0")
        self.has_changes("test_changeset", ['A6'])
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 6, 'a': 100, 'b': 4},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_filter_data(self):
        self.import_data(self.baseimport)
        self.update_data("n2 v2 Tfoo=3,go=go x1 y2")
        self.has_changes("test_changeset", ['D2'])
        self.table_equals("test", [
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_unfilter_data(self):
        self.import_data(self.baseimport)
        self.update_data("n1 Tinclude=yes,foo=4,name=what x3 y2")
        self.has_changes("test_changeset", ['A1'])
        self.table_equals("test", [
                {'id': 1, 'a': 4, 'b': 0},
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_delete_modify_relevant_data(self):
        self.import_data(self.baseimport)
        self.update_data("n3 Tinclude=yes,foo=99,bar=4 x0 y0")
        self.has_changes("test_changeset", ['M3'])
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 99, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_delete_modify_irrelevant_data(self):
        self.import_data(self.baseimport)
        self.update_data("n10 Tinclude=yes,xxx=zzz,kk=gg x0 y0")
        self.has_changes("test_changeset", [])
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                ])

    def test_add_new_data(self):
        self.import_data(self.baseimport)
        self.update_data("n99 Tinclude=yes,foo=5,bar=5 x0 y0")
        self.has_changes("test_changeset", ['A99'])
        self.table_equals("test", [
                {'id': 2, 'a': 3, 'b': 0},
                {'id': 3, 'a': 100, 'b': 4},
                {'id': 5, 'a': None, 'b': 49},
                {'id': 10, 'a': None, 'b': 0},
                {'id': 99, 'a': 5, 'b': 5},
                ])


class TestTransformedTableFullFilteredTable(TestTransformedTableViewFilteredTable):
    view_only = False

