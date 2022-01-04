# This file is part of Osgende
# Copyright (C) 2017 Sarah Hoffmann
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
import sqlalchemy as sa
from osgende.common.sqlalchemy import DropIndexIfExists, CreateView

class FilteredTable(TableSource):
    """ Table that provides a filtered view of another table according
        to a given subquery.

        The table may be used in view-only mode. In this case, the change
        table will be filled a bit differently: it has the full content of
        the source change table but taking addition/deletion into account
        due to appearance and disappearance of filter criteria.

        The change table for a full table currently does not distiguish
        between 'add' and 'modified'. All new and changed rows appear as
        'modified'.
    """

    def __init__(self, meta, name, source, subset, view_only=False):
        self.view_only = view_only
        table = source.data.to_metadata(meta, name=name)
        TableSource.__init__(self, table, name + "_changeset")

        self.subset = subset
        self.src = source

        if view_only:
            self.update = self.update_view
            self.construct = lambda engine: None
        else:
            self.update = self.update_full

    def create_view(self, engine):
        sql = self.src.data.select().where(self.subset)
        engine.execute(CreateView(self.data, sql))
        self.change.create(bind=engine, checkfirst=True)

    def construct(self, engine):
        with engine.begin() as conn:
            idx = sa.Index("idx_%s_id" % self.data.name,
                             self.c.id, unique=True)

            conn.execute(DropIndexIfExists(idx))

            self.truncate(conn)

            src = self.src.data.select().where(self.subset)
            sql = self.data.insert().from_select(self.src.data.c, src)
            conn.execute(sql)

            idx.create(conn)


    def update_view(self, engine):
        if self.src.change is None:
            return

        with engine.begin() as conn:
            changeset = {}

            # Added and changed rows are taken from the change table.
            # Deleted rows are all changed objects which are not there anymore.
            # We end up with more objects than were initially in but that's
            # the best we can do.
            sql = sa.select([self.src.cc.id, self.src.cc.action,
                            self.src.cc.id.in_(sa.select([self.c.id]))])
            for row in conn.execute(sql):
                changeset[row[0]] = row[1] if row[2] else 'D'

            self.write_change_table(conn, changeset)

    def update_full(self, engine):
        if self.src.change is None:
            self.construct(engine)
            return

        with engine.begin() as conn:
            changeset = {}

            # delete deleted rows
            delsql = self.data.delete()\
                        .where(self.c.id.in_(self.src.select_delete()))
            for row in conn.execute(delsql.returning(self.c.id)):
                changeset[row[0]] = 'D'
            # delete rows that have lost the filter properties
            delsql = self.delete(
                        sa.select([self.src.c.id])\
                           .where(self._src_id_changed())\
                           .where(sa.not_(self.subset)))
            for row in conn.execute(delsql.returning(self.c.id)):
                changeset[row[0]] = 'D'
            # now upsert data
            inssql = self.upsert_data()\
                        .from_select(self.src.c,
                                     self.src.data.select()
                                       .where(self._src_id_changed())
                                       .where(self.subset))
            for row in conn.execute(inssql.returning(self.c.id)):
                changeset[row[0]] = 'M' # XXX 'A'?

            # finally fill the changeset table
            self.write_change_table(conn, changeset)


    def _src_id_changed(self):
        return self.src.c.id.in_(self.src.select_add_modify())

