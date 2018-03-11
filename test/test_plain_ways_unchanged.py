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

    nodes = { 1: (1.0, 2.1), 2: (1.0, 2.2) }

    def create_tables(self, db):
        return [PlainWayTable(db.metadata, "test", db.osmdata.way, db.osmdata)]

    def test_create(self):
        self.import_data("""\
                w101 Tname=first Nn1,n2
                """, self.nodes)
        self.table_equals("test", [
            { 'id' : 101, 'tags' : { 'name' : 'first' },
              'nodes' : [1, 2], 'geom' : Line(1, 2) }])
