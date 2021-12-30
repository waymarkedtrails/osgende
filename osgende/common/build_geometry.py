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
Helper functions for building geometries for various OSM types.
"""

import sqlalchemy as sa
from shapely.geometry import LineString, MultiLineString
from geoalchemy2.shape import to_shape

def _sqr_dist(pt1, pt2):
    """ Returns the squared simple distance of two points.
        As we only compare close distances, we neither care about curvature
        nor about square roots.
    """
    return (pt1[0] - pt2[0])**2 + (pt1[1] - pt2[1])**2

class _MultiLine:
    """ A simple representation of a sequence of line strings.
    """

    def __init__(self, geom=None):
        self.turn_point = 0
        if geom is None:
            self.turn_point = -1
            self.geom = []
        elif isinstance(geom, LineString):
            self.geom = [list(geom.coords)]
        elif isinstance(geom, MultiLineString):
            self.geom = [list(g.coords) for g in list(geom.geoms)]
        else:
            raise ValueError(f"Bad geometry type '{type(geom)}'.")

    def to_shape(self):
        """ Return a Shapely line geometry for the multiline. Returns
            a LineStirng or MultiLineString depending on the number of segments.
        """
        if not self.geom:
            return None

        if len(self.geom) == 1:
            return LineString(self.geom[0])

        return MultiLineString(self.geom)

    def empty(self):
        """ Return `true` if the geometry is empty.
        """
        return not self.geom

    def first(self):
        """ Return the first point of the first segment.
        """
        assert self.geom
        return self.geom[0][0]

    def last(self):
        """ Return the last point of the last segment.
        """
        assert self.geom
        return self.geom[-1][-1]

    def reverse(self, from_seg=0):
        """ Reverse the entire geometry. """
        self.geom[from_seg:] = [list(reversed(g)) for g in reversed(self.geom[from_seg:])]

    def add(self, geom):
        """ Add the given geometry as new segments to the end of the multiline.
        """
        self.turn_point = len(self.geom)
        self.geom.extend(list(g) for g in geom.geom)

    def weld(self, geom):
        """ Adds the geometry by joining the last point of this geometry
            and the first point of the given one.
        """
        self.geom[-1].extend(geom.geom[0][1:])
        self.geom.extend(list(g) for g in geom.geom[1:])

    def weld_reverse(self, geom):
        """ Reverses the geometry and adds it by joining the last point of this
            geometry and the first point of the given one.
        """
        self.geom[-1].extend(geom.geom[-1][-2::-1])
        self.geom.extend((list(reversed(g)) for g in geom.geom[-2::-1]))

    def join_at_end(self, geom):
        """ Try to connect the given geometry at the end, possibly turning
            it around. Returns `true` if the join was successful.
        """
        if geom.first() == self.last():
            self.weld(geom)
            self.turn_point = -1
            return True

        if geom.last() == self.last():
            self.weld_reverse(geom)
            self.turn_point = -1
            return True

        return False

    def reverse_and_join_at_end(self, geom):
        """ Try to join the geometries by reversing the last segments starting
            from the current turn point of our own geometry and joining the
            given geometry at the new end.
            Returns `true` if the join was successful.
        """
        if self.turn_point < 0:
            return False

        connpt = self.geom[self.turn_point][0]

        if geom.first() == connpt:
            self.reverse(self.turn_point)
            self.weld(geom)
            self.turn_point = -1
            return True

        if geom.last() == connpt:
            self.reverse(self.turn_point)
            self.weld_reverse(geom)
            self.turn_point = -1
            return True

        return False

    def add_at_distance(self, geom):
        """ Add the geometry as new segments such that the distance to the
            existing end point is minimal.
        """
        mdist = _sqr_dist(self.last(), geom.first())
        dist_to_last = _sqr_dist(self.last(), geom.last())
        if dist_to_last < mdist:
            geom.reverse()
            mdist = dist_to_last

        # For the second way in the relation, we also allow the first
        # to be turned, if the two ways aren't connected.
        if self.turn_point == 0:
            d_first = _sqr_dist(self.first(), geom.first())
            d_last = _sqr_dist(self.first(), geom.last())
            if d_first < mdist or d_last < mdist:
                self.reverse()
                if d_last < d_first:
                    geom.reverse()

        self.add(geom)


def _get_member_geometries(conn, members, way_table, rel_table):
    """ Collect geometries for way and relation members from the respective
        tables.
    """
    geoms = {'W' : {}, 'R' : {}}

    for member in filter(lambda m: m['type'] != 'N', members):
        geoms[member['type']][member['id']] = None

    for kind, t in (('W', way_table), ('R', rel_table)):
        if geoms[kind]:
            sql = sa.select([t.c.id, t.c.geom])\
                    .where(t.c.id.in_(geoms[kind].keys()))\
                    .where(t.c.geom is not None)
            for r in conn.execute(sql):
                geoms[kind][r.id] = _MultiLine(to_shape(r.geom))

    return geoms


def build_route_geometry(conn, members, way_table, rel_table):
    """ Create a route geometry from a relation and way table given
        a member list.
    """
    geoms = _get_member_geometries(conn, members, way_table, rel_table)

    outgeom = _MultiLine()

    for member in filter(lambda m: m['type'] != 'N', members):
        geom = geoms[member['type']].get(member['id'])

        if geom is None:
            continue

        if outgeom.empty():
            outgeom.add(geom)
            continue

        # try connect with previous geometry at end point
        if outgeom.join_at_end(geom):
            continue

        # try to connect with previous geometry at start point
        if outgeom.reverse_and_join_at_end(geom):
            continue

        # nothing found, then turn the geometry such that the
        # end points are as close together as possible
        outgeom.add_at_distance(geom)


    return outgeom.to_shape()
