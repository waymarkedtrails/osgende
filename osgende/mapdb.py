# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2011-2020 Sarah Hoffmann

import logging
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import CreateSchema

from osgende.osmdata import OsmSourceTables
from osgende.common.sqlalchemy import Analyse
from osgende.common.status import StatusManager

log = logging.getLogger(__name__)

class MapDB:
    """Basic class for creation and modification of a complete database.

       Subclass this for each database and supply the create_tables()
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
                                       nodestore=self.get_option('nodestore'))

        if self.get_option('status', True):
            self.status = StatusManager(MetaData())
        else:
            self.status = None

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

    def has_option(self, option):
        return hasattr(self.options, option)

    def create(self):
        schema = self.get_option('schema')
        rouser = self.get_option('ro_user')

        with self.engine.begin() as conn:
            if schema is not None:
                conn.execute(CreateSchema(schema))
                if rouser is not None:
                    conn.execute('GRANT USAGE ON SCHEMA %s TO "%s"' % (schema, rouser))

            for t in self.tables:
                t.create(conn)

            if rouser is not None:
                for t in self.tables:
                    if schema:
                        tname = '%s.%s' % (schema, str(t.data.name))
                    else:
                        tname = str(t.data.name)
                    conn.execute('GRANT SELECT ON TABLE %s TO "%s"' % (tname, rouser))

    def construct(self):
        if self.has_option('schema'):
            tname = '%s.%%s' % self.options.schema
        else:
            tname = '%s'

        for tab in self.tables:
            log.info("Importing %s..." % str(tab.data.name))
            tab.construct(self.engine)
            if self.status:
                self.status.set_status_from(self.engine,
                                            tname % str(tab.data.name), 'base')

    def update(self):
        base_state = self.status.get_sequence(self.engine)
        schema = self.get_option('schema')

        if self.has_option('schema'):
            tname = '%s.%%s' % self.options.schema
        else:
            tname = '%s'

        for tab in self.tables:
            status_name = tname % str(tab.data.name)
            if base_state is not None:
                table_state = self.status.get_sequence(self.engine, status_name)
                if table_state is not None and table_state >= base_state:
                    log.info("Table %s already up-to-date." % tab)
                    continue

            if hasattr(tab, 'before_update'):
                tab.before_update(self.engine)
            log.info("Updating %s..." % str(tab.data.name))
            tab.update(self.engine)
            if hasattr(tab, 'after_update'):
                tab.after_update(self.engine)

            self.status.set_status_from(self.engine, status_name, 'base')

    def finalize(self, dovacuum):
        conn = self.engine.connect()\
                 .execution_options(isolation_level="AUTOCOMMIT")
        with conn.begin() as trans:
            for tab in self.tables:
                conn.execute(Analyse(tab.data, dovacuum));


