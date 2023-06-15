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
        ids as its source.

        This is an incomplete table that needs to be subclassed. Define
        two functions: add_columns() and transform()
    """

    def __init__(self, meta, name, source):
        table = sa.Table(name, meta,
                         sa.Column('id', source.c['id'].type,
                                   primary_key=True, autoincrement=False)
                        )

        self.add_columns(table, source)

        super().__init__(table, name + "_changeset")

        self.src = source

    def construct(self, engine):
        sql = self.src.data.select()
        workers = self.create_worker_queue(engine, self._process_construct_next)

        with engine.execution_options(stream_results=True).begin() as conn:
            for obj in conn.execute(sql):
                workers.add_task(obj)

        workers.finish()

    def update(self, engine):
        changeset = {}
        with engine.begin() as conn:
            # delete any objects that are gone
            delsql = self.data.delete()\
                       .where(self.c.id.in_(self.src.select_delete()))
            for row in conn.execute(delsql.returning(self.c.id)):
                changeset[row[0]] = 'D'

            # add/modify all other changed ways
            self._update_handle_modified(conn, changeset)

            # finally fill the changeset table
            self.write_change_table(conn, changeset)

    def _update_handle_modified(self, conn, changeset):
        d = self.data
        s = self.src.data

        cols = [s]
        for c in d.columns:
            cols.append(c.label('old_' + c.name))

        j = s.join(d, d.c.id == s.c.id, full = True)
        sql = sa.select(*cols).select_from(j)\
                .where(self.src.c.id.in_(self.src.select_add_modify()))

        deleted = []
        inserts = []
        for obj in conn.execute(sql):
            oid = obj.id
            is_added = obj.old_id is None

            if oid is None:
                deleted.append({'oid' : obj.old_id})
                changeset[obj.old_id] = 'D'
                continue

            cols = self.transform(obj)
            if cols is None:
                if not is_added:
                    deleted.append({'oid' : oid})
                    changeset[oid] = 'D'
                continue

            changed = False
            for k, v in cols.items():
                if str(obj._mapping['old_' + k]) != str(v):
                    changed = True
                    break

            if changed:
                cols['id'] = oid
                inserts.append(cols)
                changeset[oid] = 'A' if is_added else 'M'

        if len(inserts):
            conn.execute(self.upsert_data().values(inserts))
        if len(deleted):
            conn.execute(self.data.delete().where(d.c.id == sa.bindparam('oid')),
                         deleted)


    def _process_construct_next(self, obj):
        cols = self.transform(obj)

        if cols is not None:
            cols['id'] = obj.id
            self.thread.conn.execute(self.data.insert().values(cols))

