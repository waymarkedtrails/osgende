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

from osgende.common.connectors import TableSource
import sqlalchemy as sqla
from osgende.common.sqlalchemy import DropIndexIfExists

class FilteredTable(TableSource):
    """ Table that provides a filtered view of another table according
        to a given subquery.
    """

    def __init__(self, meta, name, source, subset):
        table = source.data.tometadata(meta, name=name)
        TableSource.__init__(self, table, source.change,
                             id_column=table.c[source.id_column.name])

        self.subset = subset
        self.src = source


    def construct(self, engine):
        if self.view_only:
            return

        idx = sqla.Index("idx_%s_%s" % (self.data.name, self.id_column.name),
                          self.id_column, unique=True)

        with engine.begin() as conn:
            conn.execute(DropIndexIfExists(idx))
            self.truncate(conn)
            sql = self.data.insert().from_select(
                    self.src.data.c, self.src.select_all(self.subset))
            conn.execute(sql)
            idx.create(conn)


    def update(self, engine):
        if self.view_only:
            return

        if self.src.change is None:
            self.construct(engine)
            return

        with engine.begin() as conn:
            # delete deleted relations
            delsql = self.data.delete()\
                        .where(self.id_column.in_(self.select_delete()))
            conn.execute(delsql)
            # delete relations that have lost the filter properties
            todelete = sqla.select([self.src.data.c.id])\
                           .where(self.src.data.c.id.in_(self.select_add_modify()))\
                           .where(sqla.not_(self.subset))
            conn.execute(self.data.delete().where(self.id_column.in_(todelete)))
            # now upsert data
            inssql = self.upsert_data()\
                        .from_select(self.src.data.c,
                                     self.src.data.select()
                                       .where(self.src.id_column.in_(
                                         self.select_add_modify()))
                                       .where(self.subset))
            conn.execute(inssql)



