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

from osgende.subtable import OsmosisSubTable
from osgende.tags import TagStore

class Ways(OsmosisSubTable):
    """A relation collection class that gets updated according to
       the changes in a RelationSegment table. If an optional hierarchy
       table is provided, super relations will be updated as well.
    """

    # Name of predefined columns
    column_geom = 'geom'
    srid = '3857'


    def __init__(self, db, name, subset):
        OsmosisSubTable.__init__(self, db, 'way', name, subset)

    def _process_next(self, obj):
        tags = self.transform_tags(obj['id'], TagStore(obj['tags']))

        if tags is not None:
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
