# This file is part of Osgende
# Copyright (C) 2018 Sarah Hoffmann
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

from osgende.common.table import TableSource
from osgende.common.threads import ThreadableDBObject
import sqlalchemy as sa
from osgende.common.sqlalchemy import DropIndexIfExists

class TransformedTable(ThreadableDBObject, TableSource):
    """ Table that transforms column data from a single source.

        The table has one predefined column: id which takes the same
        ids as its source. It also takes the change table directly
        from source.

        This is an incomplete table that needs to be subclassed. Define
        two functions: add_columns() and transform()
    """

    def __init__(self, meta, name, source):
        table = sa.Table(name, meta,
                         sa.Column('id', source.c['id'].type,
                                   primary_key=True, autoincrement=False)
                        )

        self.add_columns(table, source)

        super().__init__(table, source.change)

        self.src = source

    def construct(self, engine):
        sql = self.src.data.select()
        res = engine.execution_options(stream_results=True).execute(sql)
        workers = self.create_worker_queue(engine, self._process_construct_next)

        for obj in res:
            workers.add_task(obj)

        workers.finish()

    def _process_construct_next(self, obj):
        cols = self.transform(obj)

        if cols is not None:
            cols['id'] = obj['id']
            self.thread.conn.execute(self.data.insert().values(cols))

