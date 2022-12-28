# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2022 Sarah Hoffmann
"""
Helper functions for mamaging databases.
"""
import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.sql import quoted_name

class CreateDatabase(Executable, ClauseElement):
    """ Create a new database.
    """
    inherit_cache = False

    def __init__(self, name):
        self.name = quoted_name(name, True)

@compiles(CreateDatabase, 'postgresql')
def _create_database(element, compiler, **kw):
    return f'CREATE DATABASE "{element.name}"'


def _find_database(conn, dbname):
    db_table = sa.Table('pg_database', sa.MetaData(),
                        sa.Column('datname'))
    return bool(conn.scalar(sa.select(sa.func.count())
                    .where(db_table.c.datname == dbname)))


def database_exists(dbname, verbose=False):
    """ Check if a database with the given name exists.
    """
    mgmt_engine = sa.create_engine('postgresql:///postgres', echo=verbose,
                                   isolation_level='AUTOCOMMIT', future=True)

    with mgmt_engine.begin() as conn:
        return _find_database(conn, dbname)


def database_create(dbname, verbose=False):
    """ Create a new database with the given name. Raise a RuntimeError
        if the databse exists already.
    """
    mgmt_engine = sa.create_engine('postgresql:///postgres', echo=verbose,
                                   isolation_level='AUTOCOMMIT', future=True)

    with mgmt_engine.begin() as conn:
        if _find_database(conn, dbname):
            raise RuntimeError(f"Database '{dbname}' already exists.")

        conn.execute(CreateDatabase(dbname))
