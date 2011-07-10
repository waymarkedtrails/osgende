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
This module provides appropriate setup functions for a PostGIS-
and hstore-enabled psycopg2 and provides classes wrapping the most
frequently used SQL functions.

For geometric object support the GeoTypes module is required.
"""

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycpg2shapely import initialisePsycopgTypes

# make sure that all strings are in unicode
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
#psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

def connect(dba='dbname=osmosis user=osm'):
        """Create conntection to the database using the creadentials given in the `dba` string."""
        # register the Shapely types
        initialisePsycopgTypes(psycopg_module=psycopg2,
                        psycopg_extensions_module=psycopg2.extensions,
                        connect_string=dba)

        ret = psycopg2.connect(dba)

        psycopg2.extras.register_hstore(ret, globally=False, unicode=True)

        return ret


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


class PGObject(object):
    """This base class for all database-related objects provides convenience
       functions for common SQL tasks."""

    def __init__(self, db):
        self.db = db

    def cursor(self):
        """Return the cursor of the instance.

           If a separate cursor is required, use select().

        """
        try:
            self._cursor
        except AttributeError:
            self._cursor = psycopg2.extensions.connection.cursor(self.db)
        
        return self._cursor

    def query(self, query, data=None):
        """Execute a simple query without caring for the result."""
        cur = self.cursor()
        cur.execute(query + ";", data)

    def prepare(self, funcname, query):
        """Prepare an SQL query. """
        self.query("PREPARE %s AS %s;" % (funcname, query))

    def deallocate(self, funcname):
        """Free a previously prepared statement.
        """
        self.query("DEALLOCATE %s;" % funcname)

    def select_column(self, query, data=None):
        """Execute the given query and return the first column as a list.

           The query is expected to return exactly one column of data. Any
           other columns are discarded.

        """
        cur = self.cursor()
        cur.execute(query, data)
        if cur.rowcount == 0:
            return None

        res = []
        for r in cur:
            res.append(r[0])
        return res

    def select_one(self, query, data=None, default=None):
        """Execute the given query and return the first result."""
        cur = self.cursor()
        cur.execute(query, data)
        res = cur.fetchone()
        if res is not None:
            return res[0]
        else:
            return default

    def select_row(self, query, data=None):
        """Execute the query and return the first row of results as a tuple."""
        cur = self.cursor()
        cur.execute(query, data)
        res = cur.fetchone()
        return res

    def select(self, query, data=None, name=None):
        """General query, returning a real dictionary cursor.

           If a name is given, a server-side cursor is created.
           (See psycopg2 documentation.)

        """
        if name is None:
            cur = psycopg2.extensions.connection.cursor(
                     self.db, cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = psycopg2.extensions.connection.cursor(
                     self.db, name, cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, data)
        return cur

    def select_cursor(self, query, data=None, name=None):
        """General query, returning a standard cursor.

           If a name is given, a server-side cursor is created.
           (See psycopg2 documentation.)

        """
        if name is None:
            cur = psycopg2.extensions.connection.cursor(self.db)
        else:
            cur = psycopg2.extensions.connection.cursor(self.db, name)
        cur.execute(query, data)
        return cur


class PGTable(PGObject):
    """The base class for all derived tables.

       Each table is related to one specific database table. `name` must
       be a `PGTableName` object containing the name of the table.
    """

    def __init__(self, db, name):
        PGObject.__init__(self, db)
        self._table = name
        self.table = name.fullname

    def create(self, layout):
        """Create a new table with the liven layout.
           The layout can be either a description of the rows or an
           SQL query. The first must be given with enclosing brackets, 
           the latter must be preceeded by AS.
        """
        self.drop()
        self.query("CREATE TABLE %s %s" % (self.table, layout))

    def drop(self):
        """Drop the table or do nothing if it doesn't exist yet."""
        self.query("DROP TABLE IF EXISTS %s CASCADE" % (self.table))

    def truncate(self):
        """Truncate the entire table."""
        self.query("TRUNCATE TABLE %s CASCADE" % (self.table))

    def add_geometry_column(self, column='geom', proj='4326', geom="GEOMETRY", with_index=False):
        """Add a geometry column to the given table."""
        schema = self._table.schema if self._table.schema is not None else ''
        self.query("SELECT AddGeometryColumn(%s, %s, %s, %s, %s, 2)",
                        (schema, self._table.table, column, proj, geom))
        if with_index:
            self.create_geometry_index(column)

    def create_index(self, col):
        """Create an index over the given column(s)."""
        self.query("CREATE INDEX %s_%s on %s (%s)" 
                    % (self._table.table, col, self.table, col))

    def create_geometry_index(self, col='geom'):
        """Create an index over a geomtry column using a gist index."""
        self.query("""CREATE INDEX %s_%s on %s 
                        using gist (%s GIST_GEOMETRY_OPS)"""
                      % (self._table.table, col, self.table, col))

    def insert_values(self, values):
        """Insert a row into the table. 'values' must be a dict type where the 
           keys identify the column.
        """
        self.query("INSERT INTO %s (%s) VALUES (%s)" % 
                        (self.table, 
                         ','.join(values.keys()),
                         ('%s,' * len(values))[:-1]),
                     values.values())

    def update_values(self, tags, where, data=None):
        """Update rows in the table. 'tags' must be a dict type where the keys
           identify the column.
        """
        if data is None:
            params = tags.values()
        else:
            params = tags.values() + list(data)
        self.query("UPDATE %s SET (%s) = (%s) WHERE %s" % 
                        (self.table, 
                         ','.join(tags.keys()),
                         ('%s,' * len(tags))[:-1], where),
                    params)

    def get_column_type(self, column):
        """Return the type of the column as a string or None if the
           column does not exist.
        """
        schema = 'public' if self._table.schema is None else self._table.schema
        return self.select_one("""
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


