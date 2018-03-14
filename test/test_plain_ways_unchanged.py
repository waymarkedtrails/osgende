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
Tests for PlainWayTable.
"""

from osgende.lines import PlainWayTable
from db_compare import Line

from table_test_fixture import TableTestFixture

class TestPlainWayTableUnchanged(TableTestFixture):

    nodes = { 1: (1.0, 2.1), 2: (1.0, 2.2), 3: (1.0, 2.3),
              34: (0.9, 2.1), 36: (1.2, 2.5),
              101: (4.5, -34.1), 103: (4.51, -34.111)}

    baseimport = """\
                   w101 Tname=first Nn1,n2
                   w103 Tname=second Nn34,n1,n36
                 """

    expect_w101 = { 'id' : 101, 'tags' : { 'name' : 'first' },
                    'nodes' : [1, 2], 'geom' : Line(1, 2) }
    expect_w103 = { 'id' : 103, 'tags' : { 'name' : 'second' },
                    'nodes' : [34, 1, 36], 'geom' : Line(34, 1, 36) }

    def create_tables(self, db):
        return [PlainWayTable(db.metadata, "test", db.osmdata.way, db.osmdata)]

    def test_create(self):
        self.import_data(self.baseimport, self.nodes)
        self.table_equals("test", [self.expect_w101, self.expect_w103])

    def test_update_add_way(self):
        self.import_data(self.baseimport, self.nodes)
        self.update_data("""w3 Tx=y Nn101,n103""")
        self.has_changes("test_changeset", ['A3'])
        self.table_equals("test", [self.expect_w101, self.expect_w103,
            {'id': 3, 'tags': { 'x': 'y'}, 'nodes': [101, 103], 'geom': Line(101, 103)}
            ])

    def test_update_delete_way(self):
        self.import_data(self.baseimport, self.nodes)
        self.update_data("""w101 v2 dD""")
        self.has_changes("test_changeset", ['D101'])
        self.table_equals("test", [self.expect_w103])

    def test_update_change_tags(self):
        self.import_data(self.baseimport, self.nodes)
        self.update_data("""w101 v2 Tname=new Nn1,n2""")
        self.has_changes("test_changeset", ['M101'])
        new_w101 = dict(self.expect_w101)
        new_w101.update({'tags' : {"name" : "new"}})
        self.table_equals("test", [new_w101, self.expect_w103])

    def test_update_add_node(self):
        self.import_data(self.baseimport, self.nodes)
        self.update_data("""w101 v2 Tname=first Nn1,n2,n3""")
        self.has_changes("test_changeset", ['M101'])
        self.table_equals("test", [self.expect_w103,
            { 'id' : 101, 'tags' : { 'name' : 'first' },
                    'nodes' : [1, 2, 3], 'geom' : Line(1, 2, 3) }])

    def test_delete_node_invalidate_way(self):
        self.import_data(self.baseimport, self.nodes)
        self.update_data("""w101 v2 Tname=first Nn1""")
        self.has_changes("test_changeset", ['D101'])
        self.table_equals("test", [self.expect_w103])

    def test_move_node(self):
        self.import_data(self.baseimport, self.nodes)
        self.update_data("""n2 v2 x0.9 y2.1""")
        self.has_changes("test_changeset", ['M101'])
        self.table_equals("test", [self.expect_w103,
            { 'id' : 101, 'tags' : { 'name' : 'first' },
                    'nodes' : [1, 2], 'geom' : Line(1, (0.9, 2.1)) }])
