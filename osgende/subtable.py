# This file is part of Lonvia's Hiking Map
# Copyright (C) 2010 Sarah Hoffmann
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
Definitions shared between the different table type.
"""

from osgende.common.postgisconn import PGTable
from osgende.tags import TagStore


class OsmosisSubTable(PGTable):
    """Most basic table type to construct simple derived table from
       the nodes, ways or relations table.

       'basetable' specifies the Osmosis table to use as basis.


       For update to work properly, the table needs to have the action
       module installed and expects the *_changes tables to be existent.
       (TODO: link to action_function script.)
    """

    # Name of predefined columns
    column_id = 'id'

    def __init__(self, db, basetable, name, subset):
        PGTable.__init__(self, db, name)
        updateset = "%s IN (SELECT id FROM %s_changeset WHERE action <> 'D')" % \
                      (self.column_id, basetable)
        if subset is None:
            self.wherequery = ""
            self.updatequery = "WHERE %s"% updateset
        else:
            self.wherequery = "WHERE %s" % subset
            self.updatequery = "WHERE %s AND %s" % (subset, updateset)
        self.basetable = basetable

    def layout(self, columns):
        """ Layout the table as specified in PGTable.layout() but
            it will add a column for the OSM id. The name of the column
            is specified in 'column_id'.
        """
        fullcol = [(self.column_id, 'bigint PRIMARY KEY')]
        fullcol.extend(columns)
        PGTable.layout(self, fullcol)
        

    def construct(self):
        """Fill the table"""

        self.init_update()
        self.truncate()
        self.insert_objects(self.wherequery)
        self.finish_update()

    def update(self):
        """Update table
        """

        self.init_update()
        # delete any objects that might have been changed
        self.delete("""id IN (SELECT id FROM %s_changeset WHERE ACTION <> 'A')
                   """ % (self.basetable))
        # reinsert those that are not deleted
        self.insert_objects(self.updatequery)
        # finish up
        self.finish_update()

 
    def insert_objects(self, wherequery):
        # the worker threads
        workers = self.create_worker_queue(self._process_next)

        cur = self.db.select("SELECT id, tags FROM %ss %s" 
                         % (self.basetable, wherequery))
        for obj in cur:
            workers.add_task(obj)

        workers.finish()

    def _process_next(self, obj):
        tags = self.transform_tags(obj['id'], TagStore(obj['tags']))

        if tags is not None:
            tags['id'] = obj['id']
            self.insert_values(tags, self.thread.cursor)


    def transform_tags(self, osmid, tags):
        """ Transform OSM tags into database table columns.
            'osmid' contains the ID of the OSM object to be transformed,
            tags is a hash of OSM tag values. The function should return
            a hash of values, where the key is the table column.

            This is just a dummy function that should be overwritten by
            derived classes to do something meaningful.

            Note that the OSM id should not be explictly saved, it will be
            always put in a column called 'id'.

            If worker threads are enabled for the table, then this function
            will be executed within a thread, so make sure all commands
            are thread-safe.
        """
        return {}

    def init_update(self):
        """ This funtion is called before the construction of update of the
            table is started. By default, it doesn't do anything but it can
            be overwritten to do initialisation of datastructures or the
            database (e.g. prepare queries).
        """
        pass

    def finish_update(self):
        """ The counterpart of init_update() is called after the data has been
            written to the table (but before any commit()). Overwrite it to do
            something useful.
        """
        pass
