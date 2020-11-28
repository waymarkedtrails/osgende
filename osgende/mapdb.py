# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2011-2020 Sarah Hoffmann

import logging
import collections
import types
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import CreateSchema

from osgende.osmdata import OsmSourceTables
from osgende.common.sqlalchemy import Analyse
from osgende.common.status import StatusManager, DummyStatusManager

LOG = logging.getLogger(__name__)

class _Tables:

    def __init__(self):
        self._data = collections.OrderedDict()

    def __getitem__(self, key):
        return self._data.__getitem__(key)

    def __getattr__(self, key):
        try:
            return self._data.__getitem__(key)
        except KeyError:
            raise AttributeError()

    def __contains__(self, item):
        return item in self._data

    def __iter__(self):
        return self._data.values().__iter__()

    def __len__(self):
        return self._data.__len__()

    def add(self, name, table):
        self._data[name] = table

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
            self.status = DummyStatusManager()

        if not self.get_option('no_engine'):
            dba = URL('postgresql', username=options.username,
                      password=options.password, database=options.database)
            self.engine = create_engine(dba, echo=self.get_option('echo_sql', False))

        self.metadata = MetaData(schema=self.get_option('schema'))

        self.tables = _Tables()

    def add_table(self, name, table):
        """ Add a new table handler to the database. The table is available
            as self.table.<name> afterwards and will also be returned.
            Tables must be added in the order they need to be processed to
            ensure that dependencies are available.
        """
        self.tables.add(name, table)
        return table

    def set_metadata(self, key, value):
        """ Set a field in the SQLAlchemy metadata information field.
        """
        self.metadata.info[key] = value

    def add_function(self, name, func):
        """ Add a custom function for additional special handling.
        """
        setattr(self, name, types.MethodType(func, self))

    def get_option(self, option, default=None):
        """ Return the value of the given option or `default` if not set.
        """
        return getattr(self.options, option, default)

    def has_option(self, option):
        """ Check if the given option is available.
        """
        return hasattr(self.options, option)

    def create(self):
        schema = self.get_option('schema')
        rouser = self.get_option('ro_user')

        with self.engine.begin() as conn:
            if schema is not None:
                conn.execute(CreateSchema(schema))
                if rouser is not None:
                    conn.execute(f'GRANT USAGE ON SCHEMA {schema} TO "{rouser}"')

            for table in self.tables:
                table.create(conn)

            if rouser is not None:
                for table in self.tables:
                    conn.execute(f'GRANT SELECT ON TABLE {table.data.key} TO "{rouser}"')

    def construct(self):
        for tab in self.tables:
            LOG.info("Importing %s...", str(tab.data.name))
            tab.construct(self.engine)
            self.status.set_status_from(self.engine, tab.data.key, 'base')

    def update(self):
        base_state = self.status.get_sequence(self.engine)

        for tab in self.tables:
            if base_state is not None:
                table_state = self.status.get_sequence(self.engine, tab.data.key)
                if table_state is not None and table_state >= base_state:
                    LOG.info("Table %s already up-to-date.", tab)
                    continue

            if hasattr(tab, 'before_update'):
                tab.before_update(self.engine)
            LOG.info("Updating %s...", str(tab.data.name))
            tab.update(self.engine)
            if hasattr(tab, 'after_update'):
                tab.after_update(self.engine)

            self.status.set_status_from(self.engine, tab.data.key, 'base')

    def finalize(self, dovacuum):
        """ Analyse the tables to update the statistics.
        """
        conn = self.engine.connect()\
                 .execution_options(isolation_level="AUTOCOMMIT")
        with conn.begin():
            for tab in self.tables:
                conn.execute(Analyse(tab.data, dovacuum))
