# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
"""
Provide special compare classes for complex database values.
"""

from geoalchemy2.elements import _SpatialElement as GeoElement
from geoalchemy2.shape import to_shape
import shapely.geometry as sgeom

def make_db_line(oid, **kargs):
    line = dict(kargs)
    line['id'] = oid
    return line

class DBCompareValue:
    """ Generic DB value comparator. Inherit from this class for more
        specific comparators and implement the compare() function.
    """

    # Cache locations for nodes here.
    nodestore = {}

    @classmethod
    def compare(cls, a, b):
        if isinstance(b, DBCompareValue):
            return b.compare(a)

        if type(a) != type(b):
            return False

        if isinstance(a, str):
            return a == b

        if isinstance(a, dict) and isinstance(b, dict):
            if len(a) != len(b):
                return False

            for k,v in a.items():
                if k not in b or not cls.compare(v, b[k]):
                    return False

            return True

        try:
            for suba, subb in zip(iter(a), iter(b)):
                if not cls.compare(suba, subb):
                    return False
            return True
        except TypeError:
            pass

        return a == b


class Line(DBCompareValue):
    """ Compare with a GeoAlchemy or Shapely LineString. """

    def __init__(self, *args):
        self.points = args

    def get_points(self):
        pt = []
        for a in self.points:
            if isinstance(a, int):
                assert a in DBCompareValue.nodestore
                a = DBCompareValue.nodestore[a]

            assert len(a) == 2, f"not a point: {a}"
            pt.append(a)

        return pt

    def compare(self, o):
        pts = self.get_points()
        if isinstance(o, GeoElement):
            o = to_shape(o)

        if not isinstance(o, sgeom.LineString):
            return False

        if len(pts) != len(o.coords):
            return False

        for a, e in zip(pts, o.coords):
            if abs(a[0] - e[0]) > 0.00000001:
                return False
            if abs(a[1] - e[1]) > 0.00000001:
                return False

        assert o.is_valid

        return True

class Set(DBCompareValue):
    def __init__(self, *args):
        self.elems = set(args)

    def compare(self, o):
        if isinstance(o, list):
            return len(o) == len(self.elems) and set(o) == self.elems
        if isinstance(o, set):
            return o == self.elems

        return False

class Any(DBCompareValue):

    def compare(self, o):
        return True
