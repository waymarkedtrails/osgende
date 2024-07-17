# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Tests for SegmentsTable
"""
import pytest

from osgende.lines import PlainWayTable, SegmentsTable

from db_compare import Line, Any, Set
from db_compare import make_db_line


def R(nodes, ways, **kargs):
    return make_db_line(Any(), nodes=nodes, ways=ways, geom=Line(*nodes), **kargs)

class TestSimpleSegmentsImport:

    @pytest.fixture(autouse=True)
    def setup_test(self, db):
        # need base table from which to derive the segments
        plain = db.add_table(PlainWayTable(db.db.metadata, "base",
                                           db.db.osmdata.way, db.db.osmdata))
        # actual test target
        self.segments = db.add_table(SegmentsTable(db.db.metadata, "test",
                                                   plain.table,
                                                   [plain.table.data.c.tags]))

        self.db = db

    nodegrid = """\
      1   2    3  4 5 6
            a b      c
    """

    def _test(self, data, *args):
        self.db.import_data(data, grid=self.nodegrid)
        self.segments.has_data(*args)

    def test_create_independent_ways_same_type(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=1 Nn100,n101,n102
            """,
            # result
            R([1, 2, 3], Set(23), tags={'ref': '1'}),
            R([100, 101, 102], Set(32), tags={'ref': '1'})
        )

    def test_create_independent_ways_differeny_type(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=2 Nn100,n101,n102
            """,
            # result
            R([1, 2, 3], Set(23), tags={'ref': '1'}),
            R([100, 101, 102], Set(32), tags={'ref': '2'})
        )

    def test_create_joined_ways_same_type_match(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=1 Nn3,n4,n5,n6
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def test_create_joined_ways_same_type_left_turn(self):
        self._test("""\
            w23 Tref=1 Nn3,n2,n1
            w32 Tref=1 Nn3,n4,n5,n6
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def test_create_joined_ways_same_type_right_turn(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=1 Nn6,n5,n4,n3
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def test_create_joined_ways_same_type_both_turn(self):
        self._test("""\
            w23 Tref=1 Nn3,n2,n1
            w32 Tref=1 Nn6,n5,n4,n3
            """,
            # result
            R([1, 2, 3, 4, 5, 6], Set(23, 32), tags={'ref': '1'}),
        )

    def test_create_joined_ways_differeny_type(self):
        self._test("""\
            w23 Tref=1 Nn1,n2,n3
            w32 Tref=2 Nn3,n4,n5
            """,
            # result
            R([1, 2, 3], Set(23), tags={'ref': '1'}),
            R([3, 4, 5], Set(32), tags={'ref': '2'})
        )

    def test_create_multiple_joined_same_type(self):
        self._test("""\
            w1 Tfoo=bar Nn3,n4,n5
            w2 Tfoo=bar Nn1,n2,n3
            w3 Tfoo=bar Nn6,n5
            """,
            #result
            R([1, 2, 3, 4, 5, 6], Set(1, 2, 3), tags={'foo': 'bar'}),
        )

    def test_circular_ways(self):
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

    def test_circular_ways_unattached(self):
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

    def test_circular_way_unattached_split(self):
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

    def test_circle_with_multiple_segments(self):
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

    def test_y_intersection_same_type(self):
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

    def test_crossing_y_intersection_same_type(self):
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

    def test_crossing_y_intersection_different_type(self):
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

    def test_crossing_intersection(self):
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

    def test_one_node_way(self):
        self.nodegrid = "1 2 3"

        self._test("""\
            w1 Tref=1 Nn1
            w2 Tref=1 Nn2,n3
            """,
            R([2, 3], Set(2), tags={'ref': '1'}),
        )

class TestSimpleSegmentsUpdate:

    nodegrid = """\
            a
        1 2 3 4 5
            b
    """

    @pytest.fixture(autouse=True)
    def create_tables(self, db):
        # need base table from which to derive the segments
        plain = db.add_table(PlainWayTable(db.db.metadata, "base",
                                           db.db.osmdata.way, db.db.osmdata))
        # actual test target
        self.segments = db.add_table(SegmentsTable(db.db.metadata, "test",
                                                   plain.table,
                                                   [plain.table.data.c.tags]))

        self.db = db


    def _test(self, import_data, update_data, *args):
        self.db.import_data(import_data, grid=self.nodegrid)
        self.db.update_data(update_data)
        self.segments.has_data(*args)

    def test_move_node(self):
        self.db.import_data("""\
            n1 x23.0 y-3.0
            n2 x23.001 y-3.43
            w1 Tref=1 Nn1,n2
            """)
        self.db.update_data("n2 x23.002 y-3.43")
        self.segments.has_changes('A2', 'D1')
        self.segments.has_data(
            {'tags': {'ref': '1'}, 'nodes': [1, 2], 'ways': [1],
             'geom': Line((23.0, -3.0), (23.002, -3.43))}
        )

    def test_add_node_to_way(self):
        self._test("""\
            w1 Ta=a Nn1,n3
            """,
            # update
            "w1 Ta=a Nn1,n2,n3",
            # result
            R([1, 2, 3], Set(1), tags={'a': 'a'})
        )
        self.segments.has_changes('A2', 'D1')

    def test_remove_node_from_way(self):
        self._test("""\
            w1 Ta=a Nn1,n2,n3
            """,
            # update
            "w1 Ta=a Nn1,n3",
            # result
            R([1, 3], Set(1), tags={'a': 'a'})
        )
        self.segments.has_changes('A2', 'D1')

    def test_change_way_type(self):
        self._test("""\
            w1 Ta=a Nn1,n2,n3
            """,
            # update
            "w1 Tfoo=bar Nn1,n2,n3",
            # result
            R([1, 2, 3], Set(1), tags={'foo': 'bar'})
        )
        self.segments.has_changes('A2', 'D1')

    def test_add_unrelated_way(self):
        self._test("""\
            w1 Tref=1 Nn1,n2,n3
            """,
            # update
            "w2 Tref=1 Nn4,n5",
            # result
            R([1, 2, 3], Set(1), tags={'ref': '1'}),
            R([4, 5], Set(2), tags={'ref': '1'}),
        )
        self.segments.has_changes('A2')

    def test_add_adjoining_way_same_type(self):
        self._test("""\
            w1 Tref=1 Nn1,n2,n3
            """,
            # update
            "w2 Tref=1 Nn3,n4,n5",
            # result
            R([5, 4, 3, 2, 1], Set(1, 2), tags={'ref': '1'}),
        )

    def test_add_adjoining_way_different_type(self):
        self._test("""\
            w1 Tref=1 Nn1,n2,n3
            """,
            # update
            "w2 Tref=2 Nn3,n4,n5",
            # result
            R([1, 2, 3], Set(1), tags={'ref': '1'}),
            R([3, 4, 5], Set(2), tags={'ref': '2'}),
        )

    def test_add_touching_way_same_type(self):
        self._test("""\
            w1 Tref=w Nn2,n3,n4
            """,
            # update
            "w2 Tref=w Nn100,n3",
            # result
            R([2, 3], Set(1), tags={'ref': 'w'}),
            R([3, 4], Set(1), tags={'ref': 'w'}),
            R([100, 3], Set(2), tags={'ref': 'w'}),
        )


    def test_add_touching_way_different_type(self):
        self._test("""\
            w1 Tref=w Nn2,n3,n4
            """,
            # update
            "w2 Tref=z Nn100,n3",
            # result
            R([2, 3], Set(1), tags={'ref': 'w'}),
            R([3, 4], Set(1), tags={'ref': 'w'}),
            R([100, 3], Set(2), tags={'ref': 'z'}),
        )

    def test_add_touching_way_at_joint_same_type(self):
        self._test("""\
            w1 Tref=w Nn1,n2,n3
            w2 Tref=w Nn3,n4,n5
            """,
            # update
            "w10 Tref=w Nn100,n3",
            # result
            R([1, 2, 3], Set(1), tags={'ref': 'w'}),
            R([3, 4, 5], Set(2), tags={'ref': 'w'}),
            R([100, 3], Set(10), tags={'ref': 'w'}),
        )

    def test_add_touching_way_at_joint_different_type(self):
        self._test("""\
            w1 Tref=w Nn1,n2,n3
            w2 Tref=w Nn3,n4,n5
            """,
            # update
            "w10 Tfoo=w Nn100,n3",
            # result
            R([1, 2, 3], Set(1), tags={'ref': 'w'}),
            R([3, 4, 5], Set(2), tags={'ref': 'w'}),
            R([100, 3], Set(10), tags={'foo': 'w'}),
        )

    def test_add_crossing_way_same_type(self):
        self._test("""\
            w1 Tref=w Nn2,n3,n4
            """,
            # update
            "w2 Tref=w Nn100,n3,n101",
            # result
            R([2, 3], Set(1), tags={'ref': 'w'}),
            R([3, 4], Set(1), tags={'ref': 'w'}),
            R([100, 3], Set(2), tags={'ref': 'w'}),
            R([3, 101], Set(2), tags={'ref': 'w'}),
        )


    def test_add_crossing_way_different_type(self):
        self._test("""\
            w1 Tref=w Nn2,n3,n4
            """,
            # update
            "w2 Tref=z Nn100,n3,n101",
            # result
            R([2, 3], Set(1), tags={'ref': 'w'}),
            R([3, 4], Set(1), tags={'ref': 'w'}),
            R([100, 3], Set(2), tags={'ref': 'z'}),
            R([3, 101], Set(2), tags={'ref': 'z'}),
        )

    def test_add_way_to_y_intersection(self):
        self.nodegrid = """
           1 2 3 4 5
             6   7
        """

        self._test("""\
            w1 Trel=1 Nn1,n2,n3,n4,n5
            w2 Trel=2 Nn2,n6
            """,
            # update
            "w3 Trel=3 Nn4,n7",
            # result
            R([1, 2], Set(1), tags={'rel': '1'}),
            R([2, 3, 4], Set(1), tags={'rel': '1'}),
            R([4, 5], Set(1), tags={'rel': '1'}),
            R([2, 6], Set(2), tags={'rel': '2'}),
            R([4, 7], Set(3), tags={'rel': '3'})
        )

    def test_remove_touching_way_same_type(self):
        self._test("""\
            w1 Tref=4 Nn1,n2,n3
            w2 Tref=4 Nn3,n4,n5
            """,
            # update
            "w2 dD",
            # result
            R([1, 2, 3], Set(1), tags={'ref': '4'})
        )

    def test_change_touching_way_type_to_same(self):
        self._test("""\
            w1 Tref=4 Nn1,n2,n3
            w2 Tref=3 Nn3,n4,n5
            """,
            # update
            "w2 Tref=4 Nn3,n4,n5",
            # result
            R([5, 4, 3, 2, 1], Set(1, 2), tags={'ref': '4'})
        )
    def test_change_touching_way_type_to_different(self):
        self._test("""\
            w1 Tref=4 Nn1,n2,n3
            w2 Tref=4 Nn3,n4,n5
            """,
            # update
            "w2 Tref=2 Nn3,n4,n5",
            # result
            R([1, 2, 3], Set(1), tags={'ref': '4'}),
            R([3, 4, 5], Set(2), tags={'ref': '2'})
        )

    def test_move_touching_point_along_way(self):
        self._test("""\
            w1 Tref=? Nn1,n2,n3,n4,n5
            w2 Tref=other Nn100,n3
            """,
            # update
            "w2 Tref=other Nn100,n4",
            # result
            R([1, 2, 3, 4], Set(1), tags={'ref': '?'}),
            R([4, 5], Set(1), tags={'ref': '?'}),
            R([100, 4], Set(2), tags={'ref': 'other'}),
        )

    def test_change_touching_way_type_to_same(self):
        self._test("""\
            w1 Tref=4 Nn1,n2,n3
            w2 Tref=3 Nn3,n4,n5
            """,
            # update
            "w2 Tref=4 Nn3,n4,n5",
            # result
            R([5, 4, 3, 2, 1], Set(1, 2), tags={'ref': '4'})
        )


    def test_disconnect_touching_way_same_type(self):
        self._test("""\
            w1 Tref=4 Nn1,n2,n3
            w2 Tref=4 Nn3,n4,n5
            """,
            # update
            "w2 Tref=4 Nn100,n4,n5",
            # result
            R([1, 2, 3], Set(1), tags={"ref": "4"}),
            R([100, 4, 5], Set(2), tags={"ref": "4"})
        )

    def test_remove_crossing_wat_different_type(self):
        self._test("""\
            w1 Tref=A Nn2,n3,n4
            w2 Tref=B Nn100,n3,n101
            """,
            # update
            "w2 dD",
            # result
            R([2, 3, 4], Set(1), tags={"ref": "A"}),
        )

    def test_add_way_to_crossing_way_on_intersected_way(self):
        self.nodegrid = """
                    6
              1 2 3 4 5
                8   7
        """

        self._test("""\
            w1 Trel=1 Nn1,n2,n3,n4,n5
            w3 Trel=3 Nn2,n8
            """,
            # update
            "w2 Trel=2 Nn6,n4,n7",
            # result
            R([1, 2], Set(1), tags={'rel': '1'}),
            R([2, 3, 4], Set(1), tags={'rel': '1'}),
            R([4, 5], Set(1), tags={'rel': '1'}),
            R([6, 4], Set(2), tags={'rel': '2'}),
            R([4, 7], Set(2), tags={'rel': '2'}),
            R([2, 8], Set(3), tags={'rel': '3'}),
        )


    def test_add_way_at_end_of_intersected_way(self):
        self.nodegrid = """
          1 2 3 4 5
          6     7
        """

        self._test("""\
            w1 Trel=1 Nn1,n2,n3,n4,n5
            w3 Trel=3 Nn4,n7
            """,
            # update
            "w2 Trel=2 Nn1,n6",
            # result
            R([1, 2, 3, 4], Set(1), tags={'rel': '1'}),
            R([4, 5], Set(1), tags={'rel': '1'}),
            R([1, 6], Set(2), tags={'rel': '2'}),
            R([4, 7], Set(3), tags={'rel': '3'}),
        )

    def test_shorten_all_ways_around_intersection(self):
        self.nodegrid = """\
            3 2 1 5 6
                7
                8
        """

        self._test("""\
            w1 Trel=1 Nn1,n2,n3
            w2 Trel=1 Nn1,n5,n6
            w3 Trel=1 Nn1,n7,n8
            """,
            # update
            """\
            w1 Trel=1 Nn1,n2
            w2 Trel=1 Nn1,n5
            w3 Trel=1 Nn1,n7
            """,
            #result
            R([1, 2], Set(1), tags={'rel' : '1'}),
            R([1, 5], Set(2), tags={'rel' : '1'}),
            R([1, 7], Set(3), tags={'rel' : '1'}),
        )

    def test_add_single_node_way(self):
        self.nodegrid = "1 2 3"

        self._test(
            "w1 Trel=1 Nn1,n2",
            # update
            "w2 Trel=1 Nn3",
            # result
            R([1, 2], Set(1), tags={'rel' : '1'}),
        )
