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

def _sqr_dist(p1, p2):
    """ Returns the squared simple distance of two points.
        As we only compare close distances, we neither care about curvature
        nor about square roots.
    """
    xd = p1[0] - p2[0]
    yd = p1[1] - p2[1]
    return xd * xd + yd * yd

def build_route_geometry(conn, members, way_table, rel_table):
    """ Create a route geometry from a relation and way table given
        a member list.
    """
    geoms = { 'W' : {}, 'R' : {} }
    for m in members:
        if m['type'] != 'N':
            geoms[m['type']][m['id']] = None
    # first get all involved geoemetries
    for kind, t in (('W', way_table), ('R', rel_table)):
        if geoms[kind]:
            sql = sa.select([t.c.id, t.c.geom]).where(t.c.id.in_(geoms[kind].keys()))
            for r in conn.execute(sql):
                geoms[kind][r['id']] = to_shape(r['geom'])

    # now put them together
    is_turnable = False
    outgeom = []
    for m in members:
        t = m['type']
        # ignore nodes and missing ways and relations
        if t == 'N' or m['id'] not in geoms[t]:
            continue

        # convert this to a tuple of coordinates
        geom = geoms[t][m['id']]
        if geom is None:
            continue 
        if geom.geom_type == 'MultiLineString':
            geom = [list(g.coords) for g in list(geom.geoms)]
        elif geom.geom_type == 'LineString':
            geom = [list(geom.coords)]
        else:
            raise RuntimeError("Bad geometry type '%s' (member type: %s member id: %d" % (geom.geom_type, t, m['id']))

        if outgeom:
            # try connect with previous geometry at end point
            if geom[0][0] == outgeom[-1][-1]:
                outgeom[-1].extend(geom[0][1:])
                outgeom.extend(geom[1:])
                is_turnable = False
                continue
            if geom[-1][-1] == outgeom[-1][-1]:
                outgeom[-1].extend(geom[-1][-2::-1])
                outgeom.extend(geom[-2::-1])
                is_turnable = False
                continue
            if is_turnable:
                # try to connect with previous geometry at start point
                if geom[0][0] == outgeom[-1][0]:
                    outgeom[-1].reverse()
                    outgeom[-1].extend(geom[0][1:])
                    outgeom.extend(geom[1:])
                    is_turnable = False
                    continue
                if geom[-1] == outgeom[-1][0]:
                    outgeom[-1].reverse()
                    outgeom[-1].extend(geom[-1][-2::-1])
                    outgeom.extend(geom[-2::-1])
                    is_turnable = False
                    continue
            # nothing found, then turn the geometry such that the
            # end points are as close together as possible
            mdist = _sqr_dist(outgeom[-1][-1], geom[0][0])
            d = _sqr_dist(outgeom[-1][-1], geom[-1][-1])
            if d < mdist:
                geom = [list(reversed(g)) for g in reversed(geom)]
                mdist = d
            # For the second way in the relation, we also allow the first
            # to be turned, if the two ways aren't connected.
            if is_turnable and len(outgeom) == 1:
                d1 = _sqr_dist(outgeom[-1][0], geom[0][0])
                d2 = _sqr_dist(outgeom[-1][0], geom[-1][-1])
                if d1 < mdist or d2 < mdist:
                    outgeom[-1].reverse()
                if d2 < d1:
                    geom = [list(reversed(g)) for g in reversed(geom)]

        outgeom.extend(geom)
        is_turnable = True

    if not outgeom:
        return None

    ret = LineString(outgeom[0]) if len(outgeom) == 1 else MultiLineString(outgeom)

    return ret

