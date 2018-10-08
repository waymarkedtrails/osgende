import unittest
import sqlalchemy as sa

from osgende.lines import GroupedWayTable

from table_test_fixture import TableTestFixture

class TestFilteredTable(TableTestFixture):

    nodegrid = """\
        1    4    b
         2   5  a c d
           3 6    e
    """

    def create_tables(self, db):
        return [GroupedWayTable(db.metadata, 'test', db.osmdata.way, ('tags', ))]

    def test_create(self):
        self.import_data("""
         w5 Ttype=foo Nn1,n2,n3
         w6 Ttype=foo Nn4,n5
         w7 Ttype=foo Nn5,n6
        """)
        self.table_equals('test', [{'id': 6, 'child': 6},
                                   {'id': 6, 'child': 7}])

    def test_touching_with_different_tags(self):
        self.import_data("""
         w5 Ttype=foo,name=x Nn1,n2,n3,n4
         w6 Ttype=foo Nn4,n5
         w7 Ttype=foo Nn5,n6
        """)
        self.table_equals('test', [{'id': 6, 'child': 6},
                                   {'id': 6, 'child': 7}])

    def test_crossing_ways(self):
        self.import_data("""
         w1 Ttype=red Nn10,n12,n13
         w2 Ttype=red Nn11,n12,n14
        """)
        self.table_equals('test', [{'id': 1, 'child': 1},
                                   {'id': 1, 'child': 2}])

    def test_crossing_ways_with_different_tags(self):
        self.import_data("""
         w1 Ttype=red Nn10,n12,n13
         w2 Ttype=blue Nn11,n12,n14
        """)
        self.table_equals('test', [])

    def test_crossing_ways(self):
        self.import_data("""
         w1 Ttype=red Nn10,n12,n13
         w2 Ttype=red Nn11,n12,n14
        """)
        self.table_equals('test', [{'id': 1, 'child': 1},
                                   {'id': 1, 'child': 2}])

    def test_crossing_ways_without_touching(self):
        self.import_data("""
         w1 Ttype=red Nn10,n13
         w2 Ttype=red Nn11,n14
        """)
        self.table_equals('test', [])


