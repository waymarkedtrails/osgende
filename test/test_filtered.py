# This file is part of Osgende
# Copyright (C) 2017 Sarah Hoffmann
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
Tests for FilteredTable
"""

import unittest
import sqlalchemy as sqla

from osgende.generic import FilteredTable

from table_test_fixture import TableTestFixture

class TestFilteredTable(TableTestFixture):

    basedata1 = """\
      r1 Tname=house,foo=bar Mn23@,w4@forward
      r2 Ttype=multipolygon,building=yes Mw2@,w3@,w5@
      """

    basedata2 = """\
      r10 Ttype=nothing Mw7@,w8@,w9@
      r11 Tfoo=foo,source=gogo Mr11@
      """

    basedata3 = """\
      r2 Tfoo=x,building=yes Mw2@,w3@,w5@
      """

    r1_expect = {'id' : 1, 'tags': { 'foo' : 'bar', 'name' : 'house' },
                 'members' : [{ 'id' : 23, 'type' : 'N', 'role' : ''},
                              { 'id' : 4, 'type' : 'W', 'role' : 'forward'}]}
    r2_expect = {'id' : 2, 'tags' : { 'foo' : 'x', 'building' : 'yes' },
                 'members' : [{'id' : 2, 'type' : 'W', 'role' : ''},
                             {'id' : 3, 'type' : 'W', 'role' : ''},
                             {'id' : 5, 'type' : 'W', 'role' : ''}]}
    r11_expect = {'id' : 11, 'tags' : { 'foo' : 'foo', 'source' : 'gogo' },
                   'members' : [{ 'id' : 11, 'type' : 'R', 'role' : '' }]}

    def create_tables(self, db):
        return [ FilteredTable(db.metadata, "test", db.osmdata.relation,
                                 sqla.text("tags ? 'foo'")) ]

    def test_create(self):
        self.import_data(self.basedata1)
        self.table_equals("test", [self.r1_expect])

    def test_update_add(self):
        self.import_data(self.basedata1)
        self.update_data(self.basedata2)
        self.table_equals("test", [self.r1_expect, self.r11_expect])

    def test_update_delete(self):
        self.import_data(self.basedata1)
        self.update_data("r1 dD")
        self.table_equals("test", [])

    def test_update_delete_unrelated(self):
        self.import_data(self.basedata1)
        self.update_data("r2 dD")
        self.table_equals("test", [self.r1_expect])

    def test_update_add_filter_tags(self):
        self.import_data(self.basedata1)
        self.update_data(self.basedata3)
        self.table_equals("test", [self.r1_expect, self.r2_expect])

    def test_update_remove_filter_tags(self):
        self.import_data(self.basedata1)
        self.update_data("r1 Tfooo=bar,name=house Mn23@,w4@forward")
        self.table_equals("test", [])


class TestFilteredTableView(TestFilteredTable):

    def create_tables(self, db):
        t = FilteredTable(db.metadata, "test", db.osmdata.relation,
                          sqla.text("tags ? 'foo'"))
        t.view_only = True
        return (t,)
