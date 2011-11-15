# This file is part of Osgende
# Copyright (C) 2011 Sarah Hoffmann
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
Tables for Pois
"""


from osgende.common.postgisconn import PGTable

class PoiSubTable(PGTable):
    """POI (or point of interest) objects are things like hotels,
       museums, shops etc. In OSM they may be tagged as either nodes
       or appear on ways when the entire area or building appears
       in OSM.

       The POI subtable will create a table of points for these objects.
       The points will either be put where the node is or in the center
       of the way.

       The point geometry will be saved in the 'geom' column of the
       table.

       'subset' is the query used to restricted the objects. Note that
       the query will be used on both, the node and the way table. You
       need to construct it accordingly.

       The table expects another column of boolean type that marks if
       the object was derived from a point or way. The name of the column
       is stated in 'waycol'.

       Note: some objects may appear as node and way in the OSM database
       at the moment you need to filter out these objects yourself.
    """

    # Names of predefined columns.
    column_geom = 'geom'
    column_id = 'id'
    column_kind = 'osmtype'
    srid = '900913'


    def __init__(self, db, name, subset):
        PGTable.__init__(self, db, name)
        updateset = "WHERE id = ANY(ARRAY(SELECT id FROM %s_changeset WHERE action <> 'D'))"
        if subset is None:
            wherequery = "WHERE tags is not null"
        else:
            wherequery = "WHERE %s" % subset
            updateset = "%s AND %s" % (updateset, subset)
        self.node_construct = 'SELECT id, tags, geom FROM nodes %s' % wherequery
        self.way_construct = 'SELECT id, tags, ST_Centroid(osgende_way_geom(nodes)) as geom FROM ways %s' % wherequery
        self.node_update = 'SELECT id, tags, geom FROM nodes %s' % updateset
        self.way_update = 'SELECT id, tags, ST_Centroid(osgende_way_geom(nodes)) as geom FROM ways %s' % updateset

        if self.srid == '4326':
            transform = '%s'
        else:
            transform = 'ST_Transform(%%s, %s)' % self.srid
        self.transform = transform

    def layout(self, columns):
        fullcol = [(self.column_id, 'bigint'),
                   (self.column_kind, 'char(1)')]
        fullcol.extend(columns)
        PGTable.layout(self, fullcol)
        self.add_geometry_column(self.column_geom, self.srid, 'POINT', with_index=True)

    def construct(self):
        """Fill the table"""

        self.truncate()
        print "Constructing point objects in table..."
        self._insert_objects('N', self.node_construct)
        print "Constructing way objects in table..."
        self._insert_objects('W', self.way_construct)

    def update(self):
        """Update table
        """

        # delete any objects that might have been changed
        self.query("""DELETE FROM %s 
                       WHERE %s = 'N' AND %s IN (SELECT id FROM node_changeset)
                   """ % (self.table, self.column_kind, self.column_id))
        self.query("""DELETE FROM %s 
                       WHERE %s = 'W' AND id IN (SELECT id FROM way_changeset)
                   """ % (self.table, self.column_type, self.column_id))
        # reinsert those that are not deleted
        self._insert_objects('N', self.node_update)
        self._insert_objects('W', self.way_update)

 
    def _insert_objects(self, typeid, wherequery):
        cur = self.db.select(wherequery)
        for obj in cur:
            tags = self.transform_tags(False, obj['id'], obj['tags'])

            query = ("INSERT INTO %s (%s, %s, %s, %s) VALUES (%%s, %%s, %s, %s)" % 
                        (self.table, self.column_id, self.column_kind, self.column_geom,
                         ','.join(tags.keys()), self.transform,
                         ','.join(['%s' for i in range(len(tags))])))
            params = [obj['id'], typeid, obj['geom']]
            params.extend(tags.values())
            self.db.query(query, params)


    def transform_tags(self, isway, osmid, tags):
        """ Transform OSM tags into database table columns.
            'osmid' contains the ID of the OSM object to be transformed,
            tags is a hash of OSM tag values. The function should return
            a hash of valies, where the key is the table column.

            This is just a dummy function that should be overwritten by
            derived classes to do something meaningful.

            Note that the OSM id should not be explictly saved, it will be
            always put in a column called 'id'.
        """
        return {}

