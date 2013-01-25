# This file is part of Osgende
# Copyright (C) 2010-11 Sarah Hoffmann
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
Tables for ways
"""

from osgende.subtable import OsmosisSubTable
from osgende.tags import TagStore
import shapely.geometry as sgeom

class Ways(OsmosisSubTable):
    """Most basic table type to construct a simple derived table from
       the ways table. The extension to OsmosisSubTable is that
       it constructs the geometry of the way.
       Use 'geom' to state the name of the column that should contain
       the geometry.
    """

    # Name of predefined columns
    column_geom = 'geom'
    srid = '3857'


    def __init__(self, db, name, subset):
        OsmosisSubTable.__init__(self, db, 'way', name, subset)

    def layout(self, columns):
        """ Layout the table as specified in PGTable.layout() but
            it will add a column for the OSM id. The name of the column
            is specified in 'column_id'.
        """
        OsmosisSubTable.layout(self, columns)
        self.add_geometry_column(self.column_geom, self.srid,
                                 'GEOMETRY', with_index=True)

    def _process_next(self, obj):
        tags = self.transform_tags(obj['id'], TagStore(obj['tags']))

        if tags is not None:
            points = []
            prevpoints = (0,0)
            cur = self.thread.cursor

            way=self.db.select_one("SELECT nodes FROM ways WHERE id = %d" % (obj['id']))

            for n in way:
                res = self.db.get_nodegeom(n, cur)
                if res is not None:
                    pnt = (res.x, res.y)
                    if pnt == prevpoints:
                        points.append((res.x+0.00000001, res.y))
                    else:
                        points.append(pnt)
                    prevpoints = pnt

            # ignore ways where the node geometries are missing
            if len(points) > 1:
                geom = sgeom.LineString(points)
                geom._crs = 4326
                query = ("INSERT INTO %s (%s, %s, %s) VALUES (%s, %s, %s)" % 
                            (self.table, self.column_id, self.column_geom,
                             ','.join(tags.keys()), obj['id'], "ST_Transform(%%s, %s)" % (self.srid),
                             ','.join(['%s' for i in range(len(tags))])))
                params = [geom] + tags.values()
                self.thread.cursor.execute(query, params)
