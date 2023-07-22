# Copyright (C) 2010-15 Sarah Hoffmann
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

from osgende.common.table import TableSource
from osgende.common.sqlalchemy import DropIndexIfExists
from sqlalchemy.dialects.postgresql import ARRAY, insert

import sqlalchemy as sa

class GroupedWayTable(TableSource):
    """ Table that groups ways of the source table and assigns them
        a new id.

        Ways are grouped when they share a given list of attribures and
        at least one node. The source must contain the nodes list of the
        way.
    """

    def __init__(self, meta, name, source, rows):
        table = sa.Table(name, meta,
                         sa.Column('id', sa.BigInteger),
                         sa.Column('child', source.c.id.type, unique=True, index=True))

        super().__init__(table, name + "_changeset")

        self.rows = rows
        self.src = source

    def _select_src(self):
        rows = [self.src.c[r] for r in self.rows]
        rows.extend((self.src.c.id, self.src.c.nodes))

        return sa.select(*rows)

    def construct(self, engine):
        """ Create full table content from the source table.
        """
        with engine.begin() as conn:
            self.truncate(conn)

        done = set()
        with engine.begin() as conn, engine.begin() as iconn:
            cur = conn.execution_options(stream_results=True).execute(self._select_src())

            for obj in cur:
                oid = obj.id
                if oid in done:
                    # already has an id
                    continue

                done.update(self._insert_adjacent_ways((oid, obj.nodes),
                                                       obj, iconn))


    def update(self, engine):
        with engine.begin() as conn:
            changes = {}
            todo = set()
            done = set()
            # Remove all virtual IDs that are directly affected by the change.
            modsql = sa.select(self.c.id).distinct()\
                       .where(self.c.child.in_(self.src.select_modify_delete()))

            delsql = self.data.delete().where(self.c.id.in_(modsql))

            for row in conn.execute(delsql.returning(self.c.id, self.c.child)):
                changes[row[0]] = 'D'
                todo.add(row[1])

            # Insert added ways
            addsql = self._select_src()\
                       .where(self.src.c.id.in_(self.src.select_add_modify()))

            with engine.execution_options(stream_results=True).begin() as stream_conn:
                for obj in stream_conn.execute(addsql):
                    oid = obj.id
                    if oid in done:
                        # already has an id
                        continue

                    done.update(self._insert_adjacent_ways((oid, obj.nodes),
                                                           obj, conn, changes))

            # Insert modified ways
            for oid in todo:
                if oid in done:
                    continue

                cur = conn.execute(self._select_src().where(self.src.c.id == oid))

                for obj in cur:
                    done.update(self._insert_adjacent_ways((oid, obj.nodes),
                                                           obj, conn, changes))

            # finally fill the changeset table
            self.write_change_table(conn, changes)


    def _insert_adjacent_ways(self, base, obj, conn, changes=None):
        """ Find all ways adjacent to the one given in baseid iteratively
            (sharing at least one node and all properties)
            It checks for directly adjacent ways and repeats this procedure
            for all found ways until all indirectly adjacent ways are found
        """
        properties = [obj._mapping[name] for name in self.rows]
        unchecked_ways = [base]
        all_adjacent_ways = [base[0]]
        done_ways = set((base[0],))

        while unchecked_ways:
            wid, wnodes = unchecked_ways.pop()

            s = self.src.data
            intersecting_ways = self._select_src()\
                                 .where(s.c.nodes.overlap(sa.cast(wnodes, ARRAY(sa.BigInteger))))\
                                 .where(s.c.id.notin_(done_ways))

            for candidate in conn.execute(intersecting_ways):
                done_ways.add(candidate.id)
                for k, v in zip(self.rows, properties):
                    if candidate._mapping[k] != v:
                        break
                else:
                    all_adjacent_ways.append(candidate.id)
                    unchecked_ways.append((candidate.id, candidate.nodes))

        # only insert ways that have neighbours
        if len(all_adjacent_ways) <= 1:
            return []

        if changes is not None:
            # Figure out if there is an existing group or we have even
            # combined two groups.
            res = conn.execute(sa.select(self.c.id).distinct()
                                 .where(self.c.child.in_(all_adjacent_ways)))
            if res.rowcount > 0:
                base_id = res.fetchone()[0]
                changes[base_id] = 'M'
                for i in res:
                    changes[i[0]] = 'D'

                sql = insert(self.data)\
                        .on_conflict_do_update(index_elements=[self.c.child],
                                               set_={'id' : sa.text('EXCLUDED.id')})\
                        .values([{'id': base_id, 'child': x} for x in all_adjacent_ways])

                conn.execute(sql)
                return [base_id] + all_adjacent_ways

            changes[base[0]] = 'M' if base[0] in changes else 'A'


        conn.execute(self.data.insert(),
                     [{'id': base[0], 'child': x} for x in all_adjacent_ways])
        return all_adjacent_ways
