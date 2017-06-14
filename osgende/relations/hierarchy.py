# This file is part of Osgende
# Copyright (C) 2010-11 Sarah Hoffmann
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

from sqlalchemy import Table, Column, BigInteger, Integer, select,\
                       not_, column, literal_column, union_all, func
from sqlalchemy.dialects.postgresql import array

class RelationHierarchy(object):
    """Table describing the relation hierarchies of the OSM relations table.

       'subset' can be used to limit the relations taken into account.
       It must be a SELECT query that yields one row containing relation
       IDs that should be included. If no subset is given only relations
       that actually have sub- or super-relations are included. If subset
       is given all relations given in the subset will appear.
    """

    def __init__(self, meta, name, osmdata, subset=None):

        self.data = Table(name, meta,
                          Column('parent', BigInteger, index=True),
                          Column('child', BigInteger, index=True),
                          Column('depth', Integer)
                         )

        if subset is None:
            m = osmdata.member.data.alias()
            self.subset = select([func.unnest(array([m.c.relation_id,
                                                     m.c.member_id])).label('id')],
                                 distinct=True)\
                            .where(m.c.member_type == 'R')
        else:
            self.subset = subset
        self.osmdata = osmdata

    def truncate(self, conn):
        conn.execute(self.data.delete())

    def construct(self, engine):
        """Fill the table from the current relation table.
        """
        with engine.begin() as conn:
            self.truncate(conn)

            # Initially add all relations themselves.
            s = self.subset.alias()
            conn.execute(self.data.insert()
                           .from_select(self.data.c,
                                        select([s.c.id.label('parent'), s.c.id.label('child'), 1])))

            # Insert the direct children
            subset = select([self.data.c.parent.label('id')]).where(self.data.c.depth == 1)
            sm = self.osmdata.member.data.alias()
            children = select([sm.c.relation_id, sm.c.member_id, 2])\
                        .where(sm.c.relation_id.in_(subset.alias()))\
                        .where(sm.c.member_id.in_(subset.alias()))\
                        .where(sm.c.member_type == 'R')
            res = conn.execute(self.data.insert().from_select(self.data.c, children))

            level = 3
            while res.rowcount > 0 and level < 6:
                pd = self.data.alias()
                prev = select([pd.c.parent, pd.c.child]).where(pd.c.depth == (level - 1)).alias()
                nd = self.data.alias()
                newly = select([nd.c.parent, nd.c.child]).where(nd.c.depth == 2).alias()
                old = self.data.alias()
                subs = select([prev.c.parent, newly.c.child, level])\
                        .where(prev.c.child == newly.c.parent)\
                        .except_(select([old.c.parent, old.c.child, level]))

                res = conn.execute(self.data.insert().from_select(self.data.c, subs))
                level = level + 1

    def update(self, engine):
        """Update the table.

           The table is actually simply reconstructed because that is faster.
        """
        self.construct(engine)
