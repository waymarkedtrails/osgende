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
Definitions shared between the different table type.
"""

from sqlalchemy import Table, Column, BigInteger, select, and_
from osgende.tags import TagStore
from osgende.common.connectors import TableSource
from osgende.common.threads import ThreadableDBObject

class TagSubTable(ThreadableDBObject, TableSource):
    """Most basic table type to construct simple derived table from
       a table source with an id and a tag hstore.

       'datatable' specifies the Osmosis table to use as basis.
    """

    def __init__(self, meta, name, source, subset=None, change=None):
        # lay out the table
        id_col = Column(column_id, BigInteger, primary_key=True)
        table = Table(name, meta, self.id_col)
        for c in self.columns():
            table.append_column(c)
        TableSource.__init__(self, table, change, id_column=id_col)

        self.subset = subset
        self.src = source

        self.stm_insert = self.data.insert()

    def truncate(self, conn):
        conn.execute(self.data.delete())

    def construct(self, engine):
        """Fill the table"""

        self.truncate(engine)
        self.insert_objects(engine, self.src.select_all(self.subset))

    def update(self, engine):
        """Update table"""

        with engine.begin() as conn:
            # delete any objects that might have been changed
            delsql = self.data.delete().where(self.id_column.in_
                                            (self.src.select_modify_delete()))

            if self.change is None:
                conn.execute(delsql)
            else:
                conn.execute(self.change.delete())
                conn.execute(
                  self.insert_changes(
                      select([column('id'), text("'D'")],
                       from_obj(text(str(delsql.returning(self.id_column)))))
                  )
                )

            # reinsert those that are not deleted
            self.insert_objects(engine, self.src.select_updated(self.subset))

            # mark newly added objects
            if self.change is not None:
                conn.execute(self.insert_changes(
                              select([self.id_column, text("'A'")]).where(
                                  self.id_column.in_(self.src.select_add_modify())))
                            )

    def insert_objects(self, conn, selection):
        self.compiled_insert = self.stm_insert.compile(conn)
        # the worker threads
        workers = self.create_worker_queue(conn, self._process_next)

        res = conn.execution_options(stream_results=True).execute(selection)
        for obj in res:
            workers.add_task(res)

        workers.finish()

    def _process_next(self, obj):
        tags = self.transform_tags(obj['id'], TagStore(obj['tags']))

        if tags is not None:
            tags['id'] = obj['id']
            self.thread.conn.execute(self.compiled_insert, tags)

    def transform_tags(self, osmid, tags):
        """ Transform OSM tags into database table columns.
            'osmid' contains the ID of the OSM object to be transformed,
            tags is a hash of OSM tag values. The function should return
            a hash of values, where the key is the table column.

            This is just a dummy function that should be overwritten by
            derived classes to do something meaningful.

            Note that the OSM id should not be explictly saved, it will be
            always put in a column called 'id'.

            If worker threads are enabled for the table, then this function
            will be executed within a thread, so make sure all commands
            are thread-safe.
        """
        return {}
