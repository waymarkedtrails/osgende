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

from sqlalchemy import Table, Column, BigInteger, Integer, select, bindparam,\
                       not_, exists, column, text, union
from osgende.common.postgisconn import PGTable

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
                          Column('parent', BigInteger),
                          Column('child', BigInteger),
                          Column('depth', Integer)
                         )

        m = osmdata.member.data
        prev = self.data.alias()
        self._stm_step = select([self.data.c.parent, m.c.member_id, bindparam('depth')])\
                          .where(self.data.c.depth == bindparam('depth') - 1)\
                          .where(self.data.c.child == m.c.relation_id)\
                          .where(m.c.member_type == 'R')\
                          .where(not_(exists().where(prev.c.parent == self.data.c.parent)
                                              .where(prev.c.child == m.c.member_id)))

        if subset is None:
            allrels = select([m.c.relation_id.label('id')]).where(m.c.member_type == 'R').union(
                       select([m.c.member_id.label('id')]).where(m.c.member_type == 'R'))
            self._stm_base = select([column('id'), column('id'), text('1')],
                                    from_obj=allrels.alias())
        else:
            # work around SQLAlchemy being stupid and removing a duplicated column
            a = subset.alias()
            self._stm_base = select([a.c.values()[0], text(a.c.keys()[0]), text('1')])
            self._stm_step = self._stm_step.where(m.c.member_id.in_(subset))

    def truncate(self, conn):
        conn.execute(self.data.delete())

    def construct(self, engine):
        """Fill the table from the current relation table.
        """
        with engine.begin() as conn:
            self.truncate(conn)
            # compute top-level relations
            conn.execute(self.data.insert().from_select(self.data.c,
                                                        self._stm_base))

            # recurse till there are no more children
            stm = self.data.insert().from_select(self.data.c,
                                                 self._stm_step).compile(engine)
            depth = 1
            res = 1
            while res > 0 and depth < 10:
                # then go through the recursive parts
                print("Recursion",depth)
                depth += 1
                res = conn.execute(stm, { 'depth' : depth }).rowcount
                break

    def update(self):
        """Update the table.

           The table is actually simply reconstructed because that is faster.
        """
        self.construct()
