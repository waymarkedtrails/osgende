# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2022 Sarah Hoffmann
""" Additional Clauses for table manipulation.
"""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable

# Code courtesy of http://stackoverflow.com/questions/30575111/how-to-create-a-new-table-from-select-statement-in-sqlalchemy
class CreateTableAs(Executable, ClauseElement):
    """ Create a table from a select query.
    """
    inherit_cache = False

    def __init__(self, name, query, temporary=True):
        self.name = name
        self.query = query
        if temporary:
            self.prefix = 'TEMPORARY'
        else:
            self.prefix = ''

@compiles(CreateTableAs, "postgresql")
def _create_table_as(element, compiler, **kw):
    return "CREATE {} TABLE {} AS {}".format(element.prefix, element.name,
                                             compiler.process(element.query))


class Analyse(Executable, ClauseElement):
    """ Analyse the given table with or without vacuuming.
    """
    inherit_cache = False

    def __init__(self, table, dovacuum=False):
        self.table = table
        self.dovacuum = dovacuum

@compiles(Analyse, "postgresql")
def _analyse(element, compiler, **kw):
    return "{}ANALYSE {}".format("VACUUM " if element.dovacuum else '',
                                 compiler.process(element.table, asfrom=True))



class CreateView(Executable, ClauseElement):
    """ Create or replace a view from a select statement.
    """
    inherit_cache = False

    def __init__(self, name, select, replace=True):
        self.name = name
        self.select = select
        self.do_replace = replace

@compiles(CreateView)
def visit_create_view(element, compiler, **kw):
    return "CREATE {} VIEW {} AS {}".format(
        "OR REPLACE" if element.do_replace else "",
        element.name,
        compiler.process(element.select, literal_binds=True))


class DropIndexIfExists(Executable, ClauseElement):
    """ Drop given index only if it exists.
    """
    inherit_cache = False

    def __init__(self, index):
        self.index = index

@compiles(DropIndexIfExists, "postgresql")
def _analyse(element, compiler, **kw):
    if element.index.table is not None and element.index.table.schema:
        schema = "%s." % (element.index.table.schema)
    else:
        schema = ''
    return "DROP INDEX IF EXISTS %s%s" % (schema, element.index.name)


class Truncate(Executable, ClauseElement):
    """ Truncate the given table.
    """
    inherit_cache = False

    def __init__(self, table, reset=False):
        self.table = table
        self.attr = 'RESTART IDENTITY' if reset else ''

@compiles(Truncate, "postgresql")
def __truncate(element, compiler, **kw):
    return f"TRUNCATE {element.table.key} {element.attr}"
