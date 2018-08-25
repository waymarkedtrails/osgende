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

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array
from osgende.common.sqlalchemy import Truncate, jsonb_array_elements

class RelationHierarchy(object):
    """Table describing the relation hierarchies of the OSM relations table.

       'subset' can be used to limit the relations taken into account.
       It must be a SELECT query that yields one row containing relation
       IDs that should be included. If no subset is given only relations
       that actually have sub- or super-relations are included. If subset
       is given all relations given in the subset will appear.
    """

    def __init__(self, meta, name, source):
        self.data = sa.Table(name, meta,
                          sa.Column('parent', sa.BigInteger, index=True),
                          sa.Column('child', sa.BigInteger, index=True),
                          sa.Column('depth', sa.Integer)
                         )

        self.src = source

    def truncate(self, conn):
        conn.execute(Truncate(self.data))

    def create(self, engine):
        self.data.create(bind=engine, checkfirst=True)

    def construct(self, engine):
        """Fill the table from the current relation table.
        """
        with engine.begin() as conn:
            self.truncate(conn)
            # Insert all direct children
            rels = self.src.data.alias('r')
            members = jsonb_array_elements(rels.c.members).lateral()

            sql = sa.select([rels.c.id.label('parent'),
                             members.c.value['id'].astext.cast(sa.BigInteger).label('child'), 2]
                       ).select_from(rels.join(members, onclause=sa.text("True")))\
                    .where(members.c.value['type'].astext == 'R')\
                    .where(members.c.value['id'].astext.cast(sa.BigInteger) != rels.c.id.label('parent'))
            res = conn.execute(self.data.insert().from_select(self.data.c, sql))

            level = 3
            while res.rowcount > 0 and level < 6:
                pd = self.data.alias()
                prev = sa.select([pd.c.parent, pd.c.child]).where(pd.c.depth == (level - 1)).alias()
                nd = self.data.alias()
                newly = sa.select([nd.c.parent, nd.c.child]).where(nd.c.depth == 2).alias()
                old = self.data.alias()
                subs = sa.select([prev.c.parent, newly.c.child, level])\
                        .where(prev.c.child == newly.c.parent)\
                        .where(prev.c.parent != newly.c.child)\
                        .except_(sa.select([old.c.parent, old.c.child, level]))

                res = conn.execute(self.data.insert().from_select(self.data.c, subs))
                level = level + 1

            # Finally add all relations themselves.
            s = self.src.data
            conn.execute(self.data.insert().from_select(self.data.c,
                sa.select([s.c.id.label('parent'), s.c.id.label('child'), 1])))

    def update(self, engine):
        """Update the table.

           The table is actually simply reconstructed because that is faster.
        """
        self.construct(engine)
