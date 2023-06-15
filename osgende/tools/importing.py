# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2015-2022 Sarah Hoffmann
"""
Create/update OSM source tables from an OSM file.
"""

import sqlalchemy as sa

from osgende.common.sqlalchemy.database import database_create
from osgende.osmdata import OsmSourceTables
from osgende.common.status import StatusManager

class BaseImportManager:

    def __init__(self, dbname, verbose=False):
        self.dbname = dbname
        dburl = sa.engine.url.URL.create('postgresql', database=dbname)
        self.engine = sa.create_engine(dburl, echo=verbose)

        self.metadata = sa.MetaData()
        self.tables = OsmSourceTables(self.metadata)
        self.replication = None
        self.status = None

    def set_replication_source(self, url):
        self.replication = url
        self.status = StatusManager(self.metadata)

    def create_database(self):
        database_create(self.dbname)

        with self.engine.begin() as conn:
            conn.execute(sa.text("CREATE EXTENSION postgis"))
        self.metadata.create_all(self.engine)
