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
Tables for nodes
"""

from osgende.common.postgisconn import PGTable
from osgende.tags import TagStore

class NodeSubTable(PGTable):
    """Most basic table type to construct a simple derived table from
       the nodes table. The difference to OsmosisSubTable is that 
       it also copies the geometry of the node to the table.
       Use 'geom' to state the name of the column that should contain
       the geometry and 'transform' to give a transformation function.

       For update to work properly, the table needs to have the action
       module installed and expects the *_changeset tables to be existent.
       (TODO: link to action_function script.)
    """

    # Name of predefined columns
    column_id = 'id'
    column_geom = 'geom'
    srid = '900913'

    def __init__(self, db, name, subset, transform='%s'):
        PGTable.__init__(self, db, name)
        if subset is None:
            self.wherequery = "SELECT id, tags, geom FROM nodes"
            self.updatequery = "SELECT id, tags, geom FROM node_changeset WHERE action <> 'D'"
        else:
            self.wherequery = "SELECT id, tags, geom FROM nodes WHERE %s" % subset
            self.updatequery = "SELECT id, tags, geom FROM node_changeset WHERE action <> 'D' AND %s" % (subset)
        if self.srid != '4326':
            transform = 'ST_Transform(%s, %s)' % (transform, self.srid)
        self.transform = transform

    def layout(self, columns):
        """ Layout the table as specified in PGTable.layout() but
            it will add a column for the OSM id. The name of the column
            is specified in 'column_id'.
        """
        fullcol = [(self.column_id, 'bigint PRIMARY KEY')]
        fullcol.extend(columns)
        PGTable.layout(self, fullcol)
        self.add_geometry_column(self.column_geom, self.srid, 
                                 'POINT', with_index=True)
        

    def construct(self):
        """Fill the table"""

        print "Constructing objects in table..."
        self.truncate()
        self.insert_objects(self.wherequery)

    def update(self):
        """Update table
        """

        # delete any objects that might have been changed
        self.delete("id IN (SELECT id FROM node_changeset)")
        # reinsert those that are not deleted
        self.insert_objects(self.updatequery)

 
    def insert_objects(self, wherequery):
        workers = self.create_worker_queue(self._process_next)
        cur = self.db.select(wherequery)
        # XXX this really should make use of the COPY function
        for obj in cur:
            workers.add_task(obj)

        workers.finish()


    def _process_next(self, obj):
        tags = self.transform_tags(obj['id'], TagStore(obj['tags']))

        if tags is not None:
            query = ("INSERT INTO %s (%s, %s, %s) VALUES (%s, %s, %s)" % 
                        (self.table, self.column_id, self.column_geom,
                         ','.join(tags.keys()), obj['id'], self.transform,
                         ','.join(['%s' for i in range(len(tags))])))
            params = [obj['geom']]
            params.extend(tags.values())
            self.thread.cursor.execute(query, params)

    def transform_tags(self, osmid, tags):
        """ Transform OSM tags into database table columns.
            'osmid' contains the ID of the OSM object to be transformed,
            tags is a hash of OSM tag values. The function should return
            a hash of valies, where the key is the table column.

            This is just a dummy function that should be overwritten by
            derived classes to do something meaningful.

            Note that the OSM id and geometry will be automatically added.

            If worker threads are used, then this function needs to be 
            thread-safe. You can make use of the thread-local cursor in
            self.thread.cursor when accessing the DB.
        """
        return {}

