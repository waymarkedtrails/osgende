# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2017 Sarah Hoffmann

# With minor modifications borrowed from
# https://bitbucket.org/zzzeek/sqlalchemy/issues/3566/figure-out-how-to-support-all-of-pgs

from sqlalchemy.sql import functions
from sqlalchemy.sql.selectable import FromClause
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.ext.compiler import compiles


class FunctionColumn(ColumnClause):
    def __init__(self, function, name, type_=None):
        self.function = self.table = function
        self.name = self.key = name
        self.type_ = type_
        self.is_literal = False

    @property
    def _from_objects(self):
        return []

    def _make_proxy(self, selectable, name=None, attach=True,
                    name_is_truncatable=False, **kw):
        co = ColumnClause(self.name, self.type_)
        co.table = selectable
        co._proxies = [self]
        if selectable._is_clone_of is not None:
            co._is_clone_of = \
                selectable._is_clone_of.columns.get(co.key)

        if attach:
            selectable._columns[co.key] = co
        return co


@compiles(FunctionColumn)
def _compile_function_column(element, compiler, **kw):
    return "(%s).%s" % (
        compiler.process(element.function, **kw),
        compiler.preparer.quote(element.name)
    )


class ColumnFunction(functions.FunctionElement):
    __visit_name__ = 'function'

    @property
    def columns(self):
        return FromClause.columns.fget(self)

    def _populate_column_collection(self):
        for name, t in self.column_names:
            self._columns[name] = FunctionColumn(self, name, t)
