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

        The table may be used in view-only mode. In this case, there is no
        separate change table created but the change is inherited from the
        source. Note that the change may not be completely correct then
        with respect to rows that disappear because the filter criteria are
        no longer met.

        The change table for a full table currently does not distiguish
        between 'add' and 'modified'. All new and changed rows appear as
        'modified'.
    """

    def __init__(self, meta, name, source, subset, view_only=False):
        self.view_only = view_only
        table = source.data.tometadata(meta, name=name)
        if self.view_only:
            TableSource.__init__(self, table, source.change)
        else:
            TableSource.__init__(self, table, name + "_changeset")

        self.subset = subset
        self.src = source

    def create_view(self, engine):
        sql = self.src.data.select().where(self.subset)
        engine.execute(CreateView(self.data, sql))


    def construct(self, engine):
        if self.view_only:
            return

        with engine.begin() as conn:
            idx = sa.Index("idx_%s_id" % self.data.name,
                             self.c.id, unique=True)

            conn.execute(DropIndexIfExists(idx))

            self.truncate(conn)

            src = self.src.data.select().where(self.subset)
            sql = self.data.insert().from_select(self.src.data.c, src)
            conn.execute(sql)

            idx.create(conn)


    def update(self, engine):
        if self.view_only:
            return

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

