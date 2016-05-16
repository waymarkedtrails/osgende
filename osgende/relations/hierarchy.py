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
                          Column('parent', BigInteger),
                          Column('child', BigInteger),
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

            # Initial step of the recursive query: all relations themselves.
            # path is a temporary array with all relations between parent and
            # child and is used to detect cycles.
            s = self.subset.alias()
            recurse = select([s.c.id.label('parent'), s.c.id.label('child'),
                              literal_column('1').label('depth'),
                              array([s.c.id]).label('path')])


            # temporary select with direct parent-child relations between relations
            sm = self.osmdata.member.data.alias()
            subs = select([sm.c.relation_id.label('up'),
                           sm.c.member_id.label('down')])\
                     .where(sm.c.member_type == 'R')\
                     .where(sm.c.relation_id.in_(self.subset.alias()))\
                     .alias('subs')

            # iterative step of recursion query: add next level of depth
            recurse = recurse.cte(recursive=True)
            iterstep = select([recurse.c.parent, subs.c.down,
                               recurse.c.depth + 1,
                               recurse.c.path.op('||')(subs.c.down)])\
                         .where(subs.c.up == recurse.c.child)\
                         .where(not_(recurse.c.path.any(subs.c.down)))\
                         .where(subs.c.down.in_(self.subset.alias()))

            # and union them all together
            recurse = recurse.union_all(iterstep)

            # insert the endresult in out hierarchy table
            conn.execute(self.data.insert()
                          .from_select(self.data.c,
                                       select([recurse.c.parent, recurse.c.child,
                                               recurse.c.depth])))

    def update(self, engine):
        """Update the table.

           The table is actually simply reconstructed because that is faster.
        """
        self.construct(engine)
