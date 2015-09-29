
# Copyright (C) 2010-11 Sarah Hoffmann
#               2012-13 Michael Spreng
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

from sqlalchemy import Table, Column, BigInteger, and_, select
import sqlalchemy.sql.functions as sqlf
from osgende.common.threads import ThreadableDBObject

class JoinedWays(ThreadableDBObject):
    """
    Table for ways that belong together:
     - they have the same attributes in mastertable
     - they share at least one node
    """

    def __init__(self, meta, mastertable, rows, osmtables, name="joined_ways"):
        self.master = mastertable
        self.way_table = osmtables.way
        self.rows = rows

        self.data = Table(name, meta,
                          Column('virtual_id', BigInteger),
                          Column('child', BigInteger, index=True))


    def truncate(self, conn):
        conn.execute(self.data.delete())

    def construct(self, engine):
        """Fill the table from the current master table.
        """
        self.truncate(engine)

        # the worker threads
        workers = self.create_worker_queue(engine, self._process_next)

        cur = engine.execution_options(stream_results=True).execute(
                                       select([self.master.data.c.id]))
        for obj in cur:
            workers.add_task(obj[0])

        workers.finish()

    def update(self, engine):

        with engine.begin() as conn:
            # XXX do we need this first delete?
            t = self.data
            conn.execute(t.delete()
                       .where(t.c.child.in_(self.way_table.select_modify_delete()))
            )
            tin = self.data.alias()
            lonely = select([tin.c.virtual_id])\
                      .group_by(tin.c.virtual_id)\
                      .having(sqlf.count(text(1)) < 2)
            conn.execute(t.delete().where(t.c.virtual_id.in_(lonely)))

        # the worker threads
        workers = self.create_worker_queue(engine, self._process_next)

        idcol = self.sater.data.c.id
        cur = engine.execute(select([idcol])
                              .where(idcol.in_(self.way_table.select_add_modify())))
        for obj in cur:
            workers.add_task(obj[0])

        workers.finish()

    def _get_all_adjacent_way_ids(self, baseid, properties, conn):
        """
            finds all ways adjacent to the one given in wid iteratively
            (sharing at least one node and all properties)
            It checks for directly adjacent ways and repeats this procedure
            for all found ways until all indirectly adjacent ways are found
        """
        unchecked_ways = [baseid]
        all_adjacent_ways = []
        done_ways = set()

        while unchecked_ways:
            wid = unchecked_ways.pop()
            """
                finds all directly adjacent ways to wid
                (sharing at least one node and all properties)
                It first requests all geometrically overlapping ways (assuming
                there is a geometry index which makes this operation fast) and
                then checks those candidates for common nodes and properties
            """
            malias = self.master.data.alias()
            intersecting_ways = self.master.data.select()\
                                 .where(and_(
                                          malias.c.id == wid,
                                          malias.c.geom.ST_Intersects(self.master.data.c.geom)))

            for candidate in conn.execute(intersecting_ways):
                cid = candidate['id']
                if cid in done_ways:
                    continue

                done_ways.add(cid)

                for k, v in zip(self.rows, properties):
                    if candidate[k] != v:
                        break # not the same
                else:
                    w = self.way_table.data.alias()
                    w2 = self.way_table.data.alias()
                    res = conn.execute(select([w.c.nodes.op('&&')(w2.c.nodes)])
                                        .where(and_(w.c.id == wid, w2.c.id == cid)))
                    if res.fetchone():
                        all_adjacent_ways.append(cid)
                        unchecked_ways.append(cid)


        return all_adjacent_ways

    def _process_next(self, wid):
        conn = self.thread.conn
        with conn.begin() as trans:
            # check if way already has a virtual_id
            vid = conn.scalar(select([sqlf.count()]).where(self.data.c.child == wid))

            # if it has an id, it is already done. Otherwise do search
            if vid > 0:
                return

            row = conn.execute(self.master.data.select()
                                     .where(self.master.data.c.id == wid)).first()

            properties = [row[name] for name in self.rows]

            merge_list = self._get_all_adjacent_way_ids(wid, properties, conn)

            # only insert ways that have neighbours
            if len(merge_list) > 1:
                conn.execute(self.data.insert(),
                             [ { 'virtual_id' : wid, 'child' : x } for x in merge_list ])

