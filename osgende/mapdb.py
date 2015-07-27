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
    """Basic class for creation and modification of a complete database.

       Subclass this for each route map and supply the create_table_objects()
       function.

       `options` may be extended with arbitrary attributes by subclasses. MapDB
       currently makes use of the following:

           * '''nodestore''' - filename of the location for the the node store.
           * '''schema''' - schema associated with this DB. The only effect this
             currently has is that the create action will attempt to create the
             schema.
           * '''ro_user''' - read-only user to grant rights to for all tables. Only
             used for create action.
    """

    def __init__(self, dba, options=None):
        self.options = options
        self.db = osmdb.OSMDatabase(dba, self.get_option('nodestore'))
        self.data_tables = []
        self.style_tables = []
        self.segment_table = None
        self.update_table = None
        self.create_table_objects()

    def get_option(self, option):
        """Return the value of the given option or None if not set.
        """
        return getattr(self.options, option, None)

    def create_tables(self):
        schema = self.get_option('schema')
        rouser = self.get_option('ro_user')
        if schema is not None:
            self.db.create_schema(schema)
            if rouser is not None:
                self.db.query('GRANT USAGE ON SCHEMA %s TO "%s"' % (schema, rouser))
        for tab in self.data_tables:
            tab.create()
            if rouser is not None:
                tab.grant('SELECT', rouser)
        for tab in self.style_tables:
            tab.create()
            if rouser is not None:
                tab.grant('SELECT', rouser)
        if self.update_table is not None:
            self.update_table.create()
            if rouser is not None:
                self.update_table.grant('SELECT', rouser)


    def import_data(self):
        for tab in self.data_tables:
            print(datetime.now(), "Importing", tab.table, "...")
            tab.construct()
            self.db.commit()
        for tab in self.style_tables:
            print(datetime.now(), "Importing", tab.table, "...")
            tab.synchronize(0, None)
            self.db.commit()

    def update_data(self):
        if self.update_table is not None:
            self.update_table.truncate()
        # Commit here so that in case someyhing goes wrong
        # later, the map tiles are simply not touched
        self.db.commit()

        for tab in self.data_tables:
            print(datetime.now(), "Updating", tab.table, "...")
            tab.update()
        if self.update_table is not None:
            for tab in self.style_tables:
                print(datetime.now(), "Updating", tab.table, "...")
                tab.synchronize(self.segment_table.first_new_id, self.update_table)

    def finalize(self, dovacuum):
        self.db.commit()
        if dovacuum:
            print(datetime.now(), "Vacuuming and analysing tables...")
            cmd = "VACUUM ANALYSE %s"
            self.db.conn.set_isolation_level(0)
        else:
            print(datetime.now(), "Analysing tables...")
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
    parser.add_option('-u', action='store', dest='username', default='',
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
