# SPDX-License-Identifier: GPL-3.0-only
# This file is part of Osgende
# Copyright (C) 2015-2020 Sarah Hoffmann
"""
Various classes that provide connections between processed tables.
"""

from sqlalchemy import String, Table, Column, select, text
from sqlalchemy.dialects.postgresql import insert
from osgende.common.sqlalchemy import Truncate

class TableSource:
    """ Describes a source for another table.

        A table source always consists of two tables: the data table and
        a table for tracking changes.
    """

    view_only = False

    def __init__(self, data_table, change_table=None):
        """ Create a new table source. `data_table` must be an SQLAlchemy table
            with the data. It must contain a column named 'id' that serves as
            primary key. The optional `change_table` may either be an
            SQLAlchemy table with id and action column or it may be a string
            and a table with that name will be created using the same MetaObject
            as the data_table. If no `change table is given, then it is
            assumed that a complete wipe-out/recreation is expected on update.
        """
        if 'id' not in data_table.c:
            raise RuntimeError("Table has no 'id' comlumn.")

        self.data = data_table

        if change_table is None:
            self.change = None
        elif isinstance(change_table, str):
            self.change = Table(change_table, data_table.metadata,
                                Column('id', data_table.c.id.type),
                                Column('action', String(1))
                               )
        else:
            self.change = change_table
            if 'id' not in self.change.c \
                or str(self.change.c.id.type) != str(data_table.c.id.type):
                raise RuntimeError("'id' columns in change table incompatible.")

    @property
    def c(self):
        """ Return the columns of the data table.
        """
        return self.data.c

    @property
    def cc(self):
        """ Return the columns of the change table.
        """
        return self.change.c

    def create(self, engine):
        """ Create a new table.
        """
        if self.view_only and hasattr(self, 'create_view'):
            self.create_view(engine)
        elif hasattr(self, 'create_table'):
            self.create_table(engine)
        else:
            self.data.create(bind=engine, checkfirst=True)
            if self.change is not None:
                self.change.create(bind=engine, checkfirst=True)

    def construct(self, engine):
        """ Fill the table with data. This function is called for the initial
            import. Derived classes need to provide an implementation.
        """
        pass

    def update(self, engine):
        """ Update the content of the table from its sources. This function
            is called every time, the OSM data tables receive new data.
            Derived classes need to provide an implementation.
        """

    def truncate(self, conn):
        """ Truncate the table. Has no effect on views.
        """
        if not self.view_only:
            conn.execute(Truncate(self.data))


    def delete(self, ids):
        return self.data.delete().where(self.c.id.in_(ids))


    def write_change_table(self, conn, changeset):
        """ Truncates the attached change table and fills it with the
            content from `changeset`. `changeset` must be a dict with ids
            as keys and the appropriate action as value.
        """
        if self.change is None:
            return

        conn.execute(Truncate(self.change))
        if not changeset:
            return

        conn.execute(self.change.insert()
                     .values([{'id': k, 'action': v} for k, v in changeset.items()]))


    def upsert_data(self):
        """ Return an Upsert statement.

            Corresponds to Postgresql SQL
              INSERT  ... ON CONFLICT DO UPDATE <all columns except id>

            Add the actual inserted values or query to complete the query.
        """
        upsertdict = {c.name: text('EXCLUDED.' + c.name)
                      for c in self.c if c.name != 'id'}
        return insert(self.data)\
                .on_conflict_do_update(index_elements=[self.c.id],
                                       set_=upsertdict)


    def select_modify_delete(self):
        """ Return am SQLAlchemy where clause describing all objects which
            have either been modified or deleted. If no change table exists
            all objects are returned.
        """
        if self.change is None:
            return select([self.c.id])

        return select([self.cc.id]).where(self.cc.action != text("'A'"))

    def select_add_modify(self):
        """ Return am SQLAlchemy where clause describing all objects which
            have either been added or modified. If no change table exists
            all objects are returned.
        """
        if self.change is None:
            return select([self.c.id])

        return select([self.cc.id]).where(self.cc.action != text("'D'"))


    def select_modify(self):
        """ Return am SQLAlchemy select clause describing all ids which
            have been deleted. If no change table exists
            all objects are returned.
        """
        if self.change is None:
            return select([self.c.id])

        return select([self.cc.id]).where(self.cc.action == text("'M'"))


    def select_delete(self):
        """ Return am SQLAlchemy where clause describing all objects which
            have been deleted. If no change table exists
            all objects are returned.
        """
        if self.change is None:
            return select([self.c.id])

        return select([self.cc.id]).where(self.cc.action == text("'D'"))
