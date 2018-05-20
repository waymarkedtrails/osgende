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
Tests for SegmentsTable
"""

from osgende.lines import PlainWayTable, SegmentsTable

from table_test_fixture import TableTestFixture
from db_compare import Line, Any, Set
from db_compare import make_db_line

def R(nodes, ways, **kargs):
    return make_db_line(Any(), nodes=nodes, ways=ways, geom=Line(*nodes), **kargs)

class TestSimpleSegmentsImport(TableTestFixture):

    nodegrid = """\
      1   2    3  4 5 6
            a b      c
    """

    def create_tables(self, db):
        # need base table from which to derive the segments
        plain = PlainWayTable(db.metadata, "base", db.osmdata.way, db.osmdata)
        # actual test target
        segments = SegmentsTable(db.metadata, "test", plain, [plain.data.c.tags])

        return [ plain, segments ]

    def _test(self, data, *args):
        self.import_data(data, grid=self.nodegrid)
        self.table_equals("test", args)

    def xtest_create_independent_ways_same_type(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=1 Nn100,n101,n102
            """,
            # result
            R([1, 2, 3], Set(23), tags={'ref': '1'}),
            R([100, 101, 102], Set(32), tags={'ref': '1'})
        )

    def xtest_create_independent_ways_differeny_type(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=2 Nn100,n101,n102
            """,
            # result
            R([1, 2, 3], Set(23), tags={'ref': '1'}),
            R([100, 101, 102], Set(32), tags={'ref': '2'})
        )

    def xtest_create_joined_ways_same_type_match(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=1 Nn3,n4,n5,n6
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def xtest_create_joined_ways_same_type_left_turn(self):
        self._test("""\
            w23 Tref=1 Nn3,n2,n1
            w32 Tref=1 Nn3,n4,n5,n6
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def xtest_create_joined_ways_same_type_right_turn(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=1 Nn6,n5,n4,n3
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def xtest_create_joined_ways_same_type_both_turn(self):
        self._test("""\
            w23 Tref=1 Nn3,n2,n1
            w32 Tref=1 Nn6,n5,n4,n3
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def xtest_create_joined_ways_differeny_type(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=2 Nn3,n4,n5
            """,
            # result
            R([1, 2, 3], Set(23), tags={'ref': '1'}),
            R([3, 4, 5], Set(32), tags={'ref': '2'})
        )

    def xtest_create_multiple_joined_same_type(self):
        self._test("""\
            w1 Tfoo=bar Nn3,n4,n5
            w2 Tfoo=bar Nn1,n2,n3
            w3 Tfoo=bar Nn6,n5
            """,
            #result
            R([1, 2, 3, 4, 5, 6], Set(1, 2, 3), tags={'foo': 'bar'}),
        )

    def xtest_circular_ways(self):
        self.nodegrid = """\
             1    2
                  3
                6   7
                  4
                  5
        """
        self._test("""\
            w1 Tref=14 Nn1,n2,n3
            w2 Tref=14 Nn4,n5
            w3 Tref=14 Nn3,n6,n4,n7,n3
            """,
            # result
            R([1, 2, 3], Set(1), tags={'ref': '14'}),
            R([4, 5], Set(2), tags={'ref': '14'}),
            R([3, 6, 4], Set(3), tags={'ref': '14'}),
            R([4, 7, 3], Set(3), tags={'ref': '14'}),
        )

    def xtest_circular_ways_unattached(self):
        self.nodegrid = """\
                 6      4
         2   1      8  5
         3       7
        """

        self._test("""\
            w1 Tref=A Nn1,n2,n3
            w2 Tref=A Nn4,n5
            w3 Tref=A Nn6,n8,n7,n1,n6
            """,
            # results
            R([1, 2, 3], Set(1), tags={'ref': 'A'}),
            R([4, 5], Set(2), tags={'ref': 'A'}),
            R([1, 7, 8, 6, 1], Set(3), tags={'ref': 'A'})
        )

    def xtest_circular_way_unattached_split(self):
        self.nodegrid = """
              3   2
                  1
                6    7
                  5
                   4
        """

        self._test("""\
            w1 Tref=c Nn1,n2,n3
            w2 Tref=c Nn4,n5
            w3 Tref=c Nn6,n5,n7,n1,n6
            """,
            # results
            R([1, 2 ,3], Set(1), tags={'ref': 'c'}),
            R([4, 5], Set(2), tags={'ref': 'c'}),
            R([5, 6, 1], Set(3), tags={'ref': 'c'}),
            R([5, 7, 1], Set(3), tags={'ref': 'c'}),
        )

    def xtest_circle_with_multiple_segments(self):
        self.nodegrid = """
           1  2  3
           7     4
              6  5
        """

        self._test("""\
            w1 Tref=m Nn1,n2,n3
            w2 Tref=m Nn3,n4,n5
            w3 Tref=m Nn5,n6,n7,n1
            """,
            # results
            R([5, 6, 7, 1, 2, 3, 4, 5], Set(1, 2, 3), tags={'ref': 'm'})
        )

    def xtest_y_intersection_same_type(self):
        self.nodegrid = """
           3 2 1 4 5
               6
        """
        self._test("""\
            w1 Tref=0 Nn1,n2,n3
            w2 Tref=0 Nn1,n4,n5
            w3 Tref=0 Nn1,n6
            """,
            R([1, 2, 3], Set(1), tags={'ref': '0'}),
            R([1, 4, 5], Set(2), tags={'ref': '0'}),
            R([1, 6], Set(3), tags={'ref': '0'}),
        )

    def xtest_crossing_y_intersection_same_type(self):
        self.nodegrid = """
           3 2 1 4 5
               6
        """
        self._test("""\
            w2 Tref=0 Nn3,n2,n1,n4,n5
            w3 Tref=0 Nn1,n6
            """,
            R([3, 2, 1], Set(2), tags={'ref': '0'}),
            R([1, 4, 5], Set(2), tags={'ref': '0'}),
            R([1, 6], Set(3), tags={'ref': '0'}),
        )

    def xtest_crossing_y_intersection_different_type(self):
        self.nodegrid = """
           3 2 1 4 5
               6
        """
        self._test("""\
            w2 Tref=0 Nn3,n2,n1,n4,n5
            w3 Tref=x Nn1,n6
            """,
            R([3, 2, 1], Set(2), tags={'ref': '0'}),
            R([1, 4, 5], Set(2), tags={'ref': '0'}),
            R([1, 6], Set(3), tags={'ref': 'x'}),
        )

    def xtest_crossing_intersection(self):
        self.nodegrid = """
                  1
               4  2  5
                  3
        """

        self._test("""\
            w1 Tref=1 Nn1,n2,n3
            w2 Tref=2 Nn4,n2,n5
            """,
            R([1, 2], Set(1), tags={'ref': '1'}),
            R([2, 3], Set(1), tags={'ref': '1'}),
            R([4, 2], Set(2), tags={'ref': '2'}),
            R([2, 5], Set(2), tags={'ref': '2'}),
        )

class TestSimpleSegmentsUpdate(TableTestFixture):

    def create_tables(self, db):
        # need base table from which to derive the segments
        plain = PlainWayTable(db.metadata, "base", db.osmdata.way, db.osmdata)
        # actual test target
        segments = SegmentsTable(db.metadata, "test", plain, [plain.data.c.tags])

        return [ plain, segments ]

    def _test(self, data, *args):
        self.import_data(data, grid=self.nodegrid)
        self.table_equals("test", args)

    def test_move_node(self):
        self.import_data("""\
            n1 x23.0 y-3.0
            n2 x23.001 y-3.43
            w1 Tref=1 Nn1,n2
            """)
        self.update_data("n2 x23.002 y-3.43")
        self.table_equals("test",
            {'tags': {'ref': '1'}, 'nodes': [1, 2], 'ways': [1],
             'geom': Line((23.0, -3.0), (23.002, -3.43))}
        )
