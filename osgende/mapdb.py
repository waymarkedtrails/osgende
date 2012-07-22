# This file is part of Lonvia's Hiking Map
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

from datetime import datetime

import osgende.common.osmdatabase as osmdb

class MapDB:
    """Basic class for creation and modification of a
       complete database.

       Subclass this for each route map and supply the
       create_table_objects() function.
    """

    def __init__(self, dba, options=None):
        nodestore = options.nodestore if hasattr(options, 'nodestore') else None
        self.db = osmdb.OSMDatabase(dba, nodestore)
        self.options = options
        self.data_tables = []
        self.style_tables = []
        self.segment_table = None
        self.update_table = None
        self.create_table_objects()

    def create_tables(self):
        for tab in self.data_tables:
            tab.create()
        for tab in self.style_tables:
            tab.create()
        if self.update_table is not None:
            self.update_table.create()


    def import_data(self):
        for tab in self.data_tables:
            print datetime.now(), "Importing", tab.table, "..."
            tab.construct()
            self.db.commit()
        for tab in self.style_tables:
            print datetime.now(), "Importing", tab.table, "..."
            tab.synchronize(0, None)
            self.db.commit()

    def update_data(self):
        if self.update_table is not None:
            self.update_table.truncate()
        # Commit here so that in case someyhing goes wrong
        # later, the map tiles are simply not touched
        self.db.commit()

        for tab in self.data_tables:
            print datetime.now(), "Updating", tab.table, "..."
            tab.update()
        if self.update_table is not None:
            for tab in self.style_tables:
                print datetime.now(), "Updating", tab.table, "..."
                tab.synchronize(self.segment_table.first_new_id, self.update_table)

    def finalize(self, dovacuum):
        self.db.commit()
        if dovacuum:
            print datetime.now(), "Vacuuming and analysing tables..."
            cmd = "VACUUM ANALYSE %s"
            self.db.conn.set_isolation_level(0)
        else:
            print datetime.now(), "Analysing tables..."
            cmd = "ANALYSE %s"
        for tab in self.data_tables:
            self.db.query(cmd % tab.table);
        for tab in self.style_tables:
            self.db.query(cmd % tab.table);

    # function mapping
    functions = { 'create' : create_tables,
              'import' : import_data,
              'update' : update_data,
            }

    def execute_action(self, action):
        self.functions[action](self)


def mapdb_main(dbclass):
    from optparse import OptionParser
    # fun with command line options
    parser = OptionParser(description=__doc__,
                          usage='%prog [options] <action>')
    parser.add_option('-d', action='store', dest='database', default='planet',
                       help='name of database')
    parser.add_option('-u', action='store', dest='username', default='osm',
                       help='database user')
    parser.add_option('-p', action='store', dest='password', default='',
                       help='password for database')

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
    else:
        hm = dbclass('dbname=%s user=%s password=%s' % 
                (options.database, options.username, options.password))
        hm.execute_action(args[0])
        hm.finalize(args[0] == 'update')
