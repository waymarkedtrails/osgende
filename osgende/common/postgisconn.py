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
This module provides appropriate setup functions for a PostGIS-
and hstore-enabled psycopg2 and provides classes wrapping the most
frequently used SQL functions.

For geometric object support the GeoTypes module is required.
"""

import threading
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycpg2shapely import initialisePsycopgTypes

import osgende.common.threads as othread

# make sure that all strings are in unicode
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

class PGTableName(object):
    """Represents the name of a table. This includes next to the name of the
       table also an optional schema name.
    """

    def __init__(self, name, schema=None):
        self.table = name
        self.schema = schema
        if schema is None:
            self.fullname = name
        else:
            self.fullname = '%s.%s' % (schema, name)


class PGDatabase(object):
    """This base class for all database-related objects provides convenience
       functions for common SQL tasks."""

    def __init__(self, dba):
        # register the Shapely types
        initialisePsycopgTypes(psycopg_module=psycopg2,
                        psycopg_extensions_module=psycopg2.extensions,
                        connect_string=dba)

        self.conn = psycopg2.connect(dba)

        psycopg2.extras.register_hstore(self.conn, globally=False, unicode=True)
        

    def cursor(self):
        """Return the cursor of the instance.

           If a separate cursor is required, use select().

        """
        try:
            self._cursor
        except AttributeError:
            self._cursor = psycopg2.extensions.connection.cursor(self.conn)
        
        return self._cursor

    def create_cursor(self, name=None):
        """Return a standard cursor.

           If a name is given, a server-side cursor is created.
           (See psycopg2 documentation.)

        """
        if name is None:
            cur = psycopg2.extensions.connection.cursor(self.conn)
        else:
            cur = psycopg2.extensions.connection.cursor(self.conn, name)
        return cur


    def query(self, query, data=None, cur=None):
        """Execute a simple query without caring for the result."""
        if cur is None:
            cur = self.cursor()
        cur.execute(query, data)

    def prepare(self, funcname, query, cur=None):
        """Prepare an SQL query. """
        self.query("PREPARE %s AS %s" % (funcname, query), cur=cur)

    def deallocate(self, funcname, cur=None):
        """Free a previously prepared statement.
        """
        self.query("DEALLOCATE %s" % funcname, cur=cur)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def select_column(self, query, data=None, cur=None):
        """Execute the given query and return the first column as a list.

           The query is expected to return exactly one column of data. Any
           other columns are discarded.

        """
        if cur is None:
            cur = self.cursor()
        cur.execute(query, data)
        if cur.rowcount == 0:
            return None

        return [r[0] for r in cur]

    def select_one(self, query, data=None, default=None, cur=None):
        """Execute the given query and return the first result."""
        if cur is None:
            cur = self.cursor()
        cur.execute(query, data)
        if cur.rowcount > 0:
            res = cur.fetchone()
            if res is not None:
                return res[0]
        
        return default

    def select_row(self, query, data=None, cur=None):
        """Execute the query and return the first row of results as a tuple."""
        if cur is None:
            cur = self.cursor()
        cur.execute(query, data)
        res = cur.fetchone()
        return res

    def select(self, query, data=None, name=None):
        """General query, returning a new real dictionary cursor.

           If a name is given, a server-side cursor is created.
           (See psycopg2 documentation.)

        """
        if name is None:
            cur = psycopg2.extensions.connection.cursor(
                     self.conn, cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = psycopg2.extensions.connection.cursor(
                     self.conn, name, cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, data)
        return cur

    def select_cursor(self, query, data=None, name=None):
        """General query, returning a standard cursor.

           If a name is given, a server-side cursor is created.
           (See psycopg2 documentation.)

        """
        if name is None:
            cur = psycopg2.extensions.connection.cursor(self.conn)
        else:
            cur = psycopg2.extensions.connection.cursor(self.conn, name)
        cur.execute(query, data)
        return cur


class PGTable(object):
    """The base class for all derived tables.

       Each table is related to one specific database table. `name` must
       be a `PGTableName` object containing the name of the table.
    """

    def __init__(self, db, name):
        self.db = db
        self._table = name
        self.table = name.fullname
        self.numthreads = None
        self.dbconn_per_thread = False

    def set_num_threads(self, num):
        """Set the number of worker threads to use when processing the
           table. Note that this is the number of additional threads
           created when processing, so the total number of threads in
           the system is num+1. Setting num to None (the default) disables
           parallel processing.

           This option is not used by PGTable itself but by many of the
           specialised subtables. Refer to the documentation there on notes
           how to make the tables thread-safe. In particular, note, that
           you cannot use the default cursor object but need to create
           an extra one in the context of threads. At the moment, that
           means that you cannot use any of the convenience functions.

           See also
        """
        self.numthreads = num

    def set_dbconn_per_thread(self, connperthread):
        """If set to true, then a seperate database connection is created
           for each worker thread. It can be accessed via 
           self.thread.db from within the thread.

           In any case, there is a thread-local 
           cursor to the table-global table available under 
           self.thread.cursor 

            See also set_num_threads()
        """
        self.dbconn_per_thread = connperthread

    def create_worker_queue(self, processfunc):
        self.thread = threading.local()
        return othread.WorkerQueue(processfunc, self.numthreads,
                             self._init_worker_thread,
                             self._shutdown_worker_thread)



    def _init_worker_thread(self):
        print "Initialising worker..."
        if self.dbconn_per_thread:
            self.thread.db = PGDatabase(self.db.conn.dsn)
        self.thread.cursor = self.db.create_cursor()

    def _shutdown_worker_thread(self):
        print "Shutting down worker..."
        if self.dbconn_per_thread:
            self.thread.db.commit()
            self.thread.db.close()
        self.thread.cursor.close()


    def copy_create(self, query):
        """Create a new table using an SQL query.
        """
        self.drop()
        self.db.query("CREATE TABLE %s AS (%s)" % (self.table, query))

    def layout(self, columns):
        """Create a new table from the specified columns.
           Columns must be a list of 2-tuples, each containing the
           name of the column and its type including possible modifiers. """
        self.drop()
        self.db.query("CREATE TABLE %s (%s)" %
                       (self.table, 
                        ', '.join(['%s %s' % x for x in columns]))) 

    def drop(self):
        """Drop the table or do nothing if it doesn't exist yet."""
        self.db.query("DROP TABLE IF EXISTS %s CASCADE" % (self.table))

    def truncate(self):
        """Truncate the entire table."""
        self.db.query("TRUNCATE TABLE %s CASCADE" % (self.table))

    def add_geometry_column(self, column='geom', proj='4326', geom="GEOMETRY", with_index=False):
        """Add a geometry column to the given table."""
        schema = self._table.schema if self._table.schema is not None else ''
        self.db.query("SELECT AddGeometryColumn(%s, %s, %s, %s, %s, 2)",
                        (schema, self._table.table, column, proj, geom))
        if with_index:
            self.create_geometry_index(column)

    def create_index(self, col):
        """Create an index over the given column(s)."""
        self.db.query("CREATE INDEX %s_%s on %s (%s)" 
                    % (self._table.table, col, self.table, col))

    def create_geometry_index(self, col='geom'):
        """Create an index over a geomtry column using a gist index."""
        self.db.query("""CREATE INDEX %s_%s on %s 
                        using gist (%s GIST_GEOMETRY_OPS)"""
                      % (self._table.table, col, self.table, col))

    def insert_values(self, values, cur=None):
        """Insert a row into the table. 'values' must be a dict type where the 
           keys identify the column.
        """
        self.db.query("INSERT INTO %s (%s) VALUES (%s)" % 
                        (self.table, 
                         ','.join(values.keys()),
                         ('%s,' * len(values))[:-1]),
                     values.values(), cur=cur)

    def update_values(self, tags, where, data=None):
        """Update rows in the table. 'tags' must be a dict type where the keys
           identify the column.
        """
        if data is None:
            params = tags.values()
        else:
            params = tags.values() + list(data)
        self.db.query("UPDATE %s SET (%s) = (%s) WHERE %s" % 
                        (self.table, 
                         ','.join(tags.keys()),
                         ('%s,' * len(tags))[:-1], where),
                    params)

    def delete(self, wherequery):
        """Delete the colums matching the given query."""
        self.db.query("DELETE FROM %s WHERE %s"% (self.table, wherequery))

    def get_column_type(self, column):
        """Return the type of the column as a string or None if the
           column does not exist.
        """
        schema = 'public' if self._table.schema is None else self._table.schema
        return self.db.select_one("""
            SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) 
              FROM pg_catalog.pg_attribute a
             WHERE a.attname = %s 
               AND a.attrelid = (
                     SELECT c.oid
                     FROM pg_catalog.pg_class c
                          LEFT JOIN pg_catalog.pg_namespace n 
                          ON n.oid = c.relnamespace
                     WHERE c.relname = %s
                       AND n.nspname = %s )
         """, (column, self._table.table, schema))


