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
Test for relation hierarchy table
"""

import unittest
import sqlalchemy as sa

from osgende.relations import RelationHierarchy

from table_test_fixture import TableTestFixture

class TestHierarchyTale(TableTestFixture):

    def create_tables(self, db):
        return [RelationHierarchy(db.metadata, "test", db.osmdata.relation)]

    def test_flat_relations(self):
        self.import_data("""
            r1 Mn1@,n3@
            r2 Mw23@,w3@
            """)
        self.table_equals("test",
                [ ])

    def test_simple_hierarchy(self):
        self.import_data("""
            r1 Mr2@
            r2 Mr3@
            r3 Mw3@
            """)
        self.table_equals("test",
                [ { 'parent' : 1, 'child' : 2, 'depth' : 2 },
                  { 'parent' : 1, 'child' : 3, 'depth' : 3 },
                  { 'parent' : 2, 'child' : 3, 'depth' : 2 },
                ])

    def test_ciruclar_dependancy(self):
        self.import_data("""
            r1 Mr2@
            r2 Mr1@
            """)
        self.table_equals("test",
                [ { 'parent' : 1, 'child' : 2, 'depth' : 2 },
                  { 'parent' : 2, 'child' : 1, 'depth' : 2 },
                ])

    def test_self_contained(self):
        self.import_data("""
            r1 Mr2@,r1@
            r2 Mw1@
            """)
        self.table_equals("test",
                [ { 'parent' : 1, 'child' : 2, 'depth' : 2 },
                ])

