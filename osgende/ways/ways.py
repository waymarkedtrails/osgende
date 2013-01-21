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
            # FIXME: does not work for nodestore
            geom = """ST_Transform(ST_MakeLine(ARRAY(
                      SELECT geom FROM (SELECT generate_subscripts(nodes, 1) AS i,
                          nodes FROM public.ways WHERE id = %d) w,
                      public.nodes n WHERE n.id = w.nodes[i] ORDER BY i)), %s)""" % (
                                  obj['id'], self.srid)
            query = ("INSERT INTO %s (%s, %s, %s) VALUES (%s, %s, %s)" % 
                        (self.table, self.column_id, self.column_geom,
                         ','.join(tags.keys()), obj['id'], geom,
                         ','.join(['%s' for i in range(len(tags))])))
            params = tags.values()
            self.thread.cursor.execute(query, params)
