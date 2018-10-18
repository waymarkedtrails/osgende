import unittest
import sqlalchemy as sa

from osgende.lines import GroupedWayTable

from table_test_fixture import TableTestFixture

def H(d):
    res = []
    for k, v in d.items():
        res.extend([{'id': k, 'child': x} for x in v])

    return res


class TestFilteredTableImport(TableTestFixture):

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
        self.table_equals('test', H({6 : [6, 7]}))

    def test_circular(self):
        self.import_data("""
         w5 Ttype=foo Nn1,n2,n3
         w6 Ttype=foo Nn3,n4,n5
         w7 Ttype=foo Nn5,n6,n1
        """)
        self.table_equals('test', H({5 : [5, 6, 7]}))

    def test_touching_with_different_tags(self):
        self.import_data("""
         w5 Ttype=foo,name=x Nn1,n2,n3,n4
         w6 Ttype=foo Nn4,n5
         w7 Ttype=foo Nn5,n6
        """)
        self.table_equals('test', H({6 : [6, 7]}))

    def test_crossing_ways(self):
        self.import_data("""
         w1 Ttype=red Nn10,n12,n13
         w2 Ttype=red Nn11,n12,n14
        """)
        self.table_equals('test', H({1 : [1, 2]}))

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
        self.table_equals('test', H({1 : [1, 2]}))

    def test_crossing_ways_without_touching(self):
        self.import_data("""
         w1 Ttype=red Nn10,n13
         w2 Ttype=red Nn11,n14
        """)
        self.table_equals('test', [])


class TestFilteredTableUpdate(TableTestFixture):

    nodegrid = """\
        1    4    b
         2   5  a c d
           3 6    e
    """

    def create_tables(self, db):
        return [GroupedWayTable(db.metadata, 'test', db.osmdata.way, ('tags', ))]

    def test_add_single_independent_way(self):
        self.import_data("""
         w6 Ttype=foo Nn4,n5
         w7 Ttype=foo Nn5,n6
        """)
        self.update_data("w5 Ttype=foo Nn1,n2,n3")
        self.has_changes("test_changeset", [])
        self.table_equals('test', H({6 : [6, 7]}))

    def test_add_grouped_independent_way(self):
        self.import_data("""
         w5 Ttype=foo Nn1,n2,n3
        """)
        self.update_data("""
         w6 Ttype=foo Nn4,n5
         w7 Ttype=foo Nn5,n6
        """)
        self.has_changes("test_changeset", ['A6'])
        self.table_equals('test', H({6 : [6, 7]}))

    def test_add_to_single_way_matching(self):
        self.import_data("""
         w5 Ttype=foo Nn1,n2,n3
        """)
        self.update_data("w6 Ttype=foo Nn3,n6,n5")
        self.has_changes("test_changeset", ['A6'])
        self.table_equals('test', H({6 : [5, 6]}))

    def test_add_to_single_way_not_matching(self):
        self.import_data("""
         w5 Ttype=foo Nn1,n2,n3
        """)
        self.update_data("w6 Ttype=bar Nn3,n6,n5")
        self.has_changes("test_changeset", [])
        self.table_equals('test', H({}))

    def test_add_to_grouped_way_end_matching(self):
        self.import_data("""
         w6 Ttype=foo Nn4,n5
         w7 Ttype=foo Nn5,n6
        """)
        self.update_data("w5 Ttype=foo Nn6,n3,n2")
        self.has_changes("test_changeset", ['M6'])
        self.table_equals('test', H({6 : [6, 7, 5]}))

    def test_add_to_grouped_way_middle_matching(self):
        self.import_data("""
         w6 Ttype=foo Nn4,n5,n6
         w7 Ttype=foo Nn3,n6
        """)
        self.update_data("w5 Ttype=foo Nn5,n2")
        self.has_changes("test_changeset", ['M6'])
        self.table_equals('test', H({6 : [6, 7, 5]}))

    def test_add_to_grouped_way_connecting(self):
        self.import_data("""
         w6 Ttype=foo Nn4,n5
         w7 Ttype=foo Nn3,n6
        """)
        self.update_data("w5 Ttype=foo Nn5,n6")
        self.has_changes("test_changeset", ['A5'])
        self.table_equals('test', H({5 : [6, 7, 5]}))

    def test_add_to_grouped_way_connecting_grouped(self):
        self.import_data("""
         w6 Ttype=foo Nn4,n5,n6
         w7 Ttype=foo Nn3,n6
         w10 Ttype=foo Nn10,n12
         w11 Ttype=foo Nn11,n12
        """)
        self.update_data("w5 Ttype=foo Nn5,n10")
        self.has_changes("test_changeset", ['M6', 'D10'])
        self.table_equals('test', H({6 : [6, 7, 5, 10, 11]}))

    def test_delete_from_grouped_end_still_grouped(self):
        self.import_data("""
         w6 Ttype=foo Nn1,n2
         w7 Ttype=foo Nn2,n3
         w9 Ttype=foo Nn3,n6
        """)
        self.update_data("w9 v2 dD")
        self.has_changes("test_changeset", ['M6'])
        self.table_equals('test', H({6 : [6, 7]}))

    def test_delete_from_grouped_end_still_grouped_name_change(self):
        self.import_data("""
         w6 Ttype=foo Nn1,n2
         w7 Ttype=foo Nn2,n3
         w9 Ttype=foo Nn3,n6
        """)
        self.update_data("w6 v2 dD")
        self.has_changes("test_changeset", ['D6', 'A9'])
        self.table_equals('test', H({9 : [7, 9]}))


    def test_delete_from_grouped_end_singled(self):
        self.import_data("""
         w6 Ttype=foo Nn1,n2
         w7 Ttype=foo Nn2,n3
        """)
        self.update_data("w7 v2 dD")
        self.has_changes("test_changeset", ['D6'])
        self.table_equals('test', H({}))

    def test_delete_from_grouped_middle_singled(self):
        self.import_data("""
         w6 Ttype=foo Nn1,n2
         w7 Ttype=foo Nn2,n3
         w9 Ttype=foo Nn3,n6
        """)
        self.update_data("w7 v2 dD")
        self.has_changes("test_changeset", ['D6'])
        self.table_equals('test', H({}))

    def test_delete_from_grouped_middle_split(self):
        self.import_data("""
         w6 Ttype=foo Nn1,n2
         w7 Ttype=foo Nn2,n3
         w9 Ttype=foo Nn3,n6
         w10 Ttype=foo Nn5,n6
         w11 Ttype=foo Nn5,n4
        """)
        self.update_data("w9 v2 dD")
        self.has_changes("test_changeset", ['M6', 'A10'])
        self.table_equals('test', H({6 : [6, 7], 10 : [10, 11]}))
