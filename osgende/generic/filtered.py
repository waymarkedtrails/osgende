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
from osgende.common.sqlalchemy import DropIndexIfExists

class FilteredTable(TableSource):
    """ Table that provides a filtered view of another table according
        to a given subquery.

        FilteredTable does not provide its own change table but exports
        the one from the source.
    """

    def __init__(self, meta, name, source, subset):
        table = source.data.tometadata(meta, name=name)
        TableSource.__init__(self, table, source.change)

        self.subset = subset
        self.src = source


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
            # delete deleted rows
            delsql = self.data.delete()\
                        .where(self.c.id.in_(self.select_delete()))
            conn.execute(delsql)
            # delete rows that have lost the filter properties
            conn.execute(self.delete(
                        sa.select([self.src.c.id])\
                           .where(self._src_id_changed())\
                           .where(sa.not_(self.subset))))
            # now upsert data
            inssql = self.upsert_data()\
                        .from_select(self.src.c,
                                     self.src.data.select()
                                       .where(self._src_id_changed())
                                       .where(self.subset))
            conn.execute(inssql)

    def _src_id_changed(self):
        return self.src.c.id.in_(self.select_add_modify())

