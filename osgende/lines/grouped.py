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
from sqlalchemy.dialects.postgresql import ARRAY

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
                         sa.Column('child', source.c.id.type, index=True))

        super().__init__(table)

        self.rows = rows
        self.src = source

    def construct(self, engine):
        """ Create full table content from the source table.
        """
        self.truncate(engine)

        done = set()
        to_select = [self.src.c[r] for r in self.rows]
        to_select.extend((self.src.c.id, self.src.c.nodes))
        cur = engine.execution_options(stream_results=True).execute(sa.select(to_select))

        for obj in cur:
            oid = obj['id']
            if oid in done:
                # already has an id
                continue

            properties = [obj[name] for name in self.rows]

            merge_list = self._get_all_adjacent_way_ids(
                           (oid, obj['nodes']), properties, engine)

            # only insert ways that have neighbours
            if len(merge_list) > 1:
                engine.execute(self.data.insert(),
                               [ { 'id' : oid, 'child' : x } for x in merge_list ])
                done.update(merge_list)

    def _get_all_adjacent_way_ids(self, base, properties, conn):
        """ Find all ways adjacent to the one given in baseid iteratively
            (sharing at least one node and all properties)
            It checks for directly adjacent ways and repeats this procedure
            for all found ways until all indirectly adjacent ways are found
        """
        unchecked_ways = [base]
        all_adjacent_ways = [base[0]]
        done_ways = set()

        while unchecked_ways:
            wid, wnodes = unchecked_ways.pop()
            done_ways.add(wid)

            s = self.src.data
            to_select = [s.c[r] for r in self.rows]
            to_select.extend((s.c.id, s.c.nodes))
            intersecting_ways = sa.select(to_select)\
                                 .where(s.c.nodes.overlap(sa.cast(wnodes, ARRAY(sa.BigInteger))))\
                                 .where(s.c.id.notin_(done_ways))

            for candidate in conn.execute(intersecting_ways):
                for k, v in zip(self.rows, properties):
                    if candidate[k] != v:
                        break
                else:
                    all_adjacent_ways.append(candidate['id'])
                    unchecked_ways.append((candidate['id'], candidate['nodes']))

        return all_adjacent_ways
