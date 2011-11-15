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

    def __init__(self, db, name, subset, geom='geom', transform='%s'):
        PGTable.__init__(self, db, name)
        updateset = "id IN (SELECT id FROM node_changeset WHERE action <> 'D')"
        if subset is None:
            self.wherequery = ""
            self.updatequery = "WHERE %s"% updateset
        else:
            self.wherequery = "WHERE %s" % subset
            self.updatequery = "WHERE %s AND %s" % (subset, updateset)
        self.geom = geom
        self.transform = transform

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
        cur = self.db.select("SELECT id, tags, geom FROM nodes %s" 
                         % (wherequery))
        # XXX this really should make use of the COPY function
        for obj in cur:
            tags = self.transform_tags(obj['id'], obj['tags'])
    
            if tags is not None:
                query = ("INSERT INTO %s (id, %s, %s) VALUES (%s, %s, %s)" % 
                            (self.table, self.geom,
                             ','.join(tags.keys()), obj['id'], self.transform,
                             ','.join(['%s' for i in range(len(tags))])))
                params = [obj['geom']]
                params.extend(tags.values())
                self.db.query(query, params)

    def transform_tags(self, osmid, tags):
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

