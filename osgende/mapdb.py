# This file is part of Osgende
# Copyright (C) 2011-2015 Sarah Hoffmann
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

import logging
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import CreateSchema
from sqlalchemy_utils.functions import analyze

from osgende.osmdata import OsmSourceTables
from osgende.common.sqlalchemy import Analyse

log = logging.getLogger(__name__)

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

    def __init__(self, options):
        self.options = options
        self.osmdata = OsmSourceTables(MetaData(),
                                       nodestore=self.get_option('nodestore'),
                                       status_table=self.get_option('status', True))

        if not self.get_option('no_engine'):
            dba = URL('postgresql', username=options.username,
                      password=options.password, database=options.database)
            self.engine = create_engine(dba, echo=self.get_option('echo_sql', False))

        self.metadata = MetaData(schema=self.get_option('schema'))

        self.tables = self.create_tables()

    def get_option(self, option, default=None):
        """Return the value of the given option or None if not set.
        """
        return getattr(self.options, option, default)

    def create(self):
        schema = self.get_option('schema')
        rouser = self.get_option('ro_user')

        if schema is not None:
            with self.engine.begin() as conn:
                conn.execute(CreateSchema(schema))
                if rouser is not None:
                    conn.execute('GRANT USAGE ON SCHEMA %s TO "%s"' % (schema, rouser))

        self.metadata.create_all(bind=self.engine)

        if rouser is not None:
            with self.engine.begin() as conn:
                for t in self.tables:
                    if schema:
                        tname = '%s.%s' % (schema, str(t.data.name))
                    else:
                        tname = str(t.data.name)
                    conn.execute('GRANT SELECT ON TABLE %s TO "%s"' % (tname, rouser))

    def construct(self):
        for tab in self.tables:
            log.info("Importing %s..." % str(tab.data.name))
            tab.construct(self.engine)

    def update(self):
        for tab in self.tables:
            log.info("Updating %s..." % str(tab.data.name))
            tab.update(self.engine)

    def finalize(self, dovacuum):
        conn = self.engine.connect()\
                 .execution_options(isolation_level="AUTOCOMMIT")
        with conn.begin() as trans:
            for tab in self.tables:
                conn.execute(Analyse(tab.data, dovacuum));


