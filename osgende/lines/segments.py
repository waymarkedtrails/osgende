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

from collections import Counter, defaultdict

import sqlalchemy as sa
import sqlalchemy.sql.functions as saf
import osgende.common.sqlalchemy as osa
from sqlalchemy.dialects.postgresql import ARRAY
from geoalchemy2.shape import from_shape, to_shape

from shapely.geometry import LineString

from osgende.common.threads import ThreadableDBObject
from osgende.common.connectors import TableSource
from osgende.common.sqlalchemy import DropIndexIfExists

class SegmentsTable(ThreadableDBObject, TableSource):
    """A table that groups ways with identical properties into linear
       segments that are as long as possible. Two rows are considered
       to have identical properties when all columns except id, nodes
       and geom are identical. Segments are guaranteed not to cross
       any other segments. To achieve that a source way may have to
       be split at times. So segments may contain one or more source
       ways and a source way may appear in one or more of the segments.

       The source table `src` needs to be a way-like table which at least has
       the following columns:

         id    - Unique identifier
         nodes - Array of OSM nodes ids that make up the way
         geom  - Geometry of the way (must be a LineString)

       The resulting table will have identical columns as the source
       table except that it adds a column 'ways' which contains a
       list of ids from the source table.

       This table creates its own new space of unique identifiers in the
       `id` column and a separate changeset table called <name>_changeset
       which refers to these ids.
    """

    def __init__(self, meta, name, source, prop_cols):
        # Create a table with its own id column.
        id_col = sa.Column('id', sa.BigInteger,
                            primary_key=True, autoincrement=True)

        table = sa.Table(name, meta, id_col)

        super().__init__(table, name + "_changeset", id_column=id_col)

        # Copy all columns that are used to identify the way type.
        self.prop_columns = []
        for c in prop_cols:
            table.append_column(sa.Column(c.key, c.type))
            self.prop_columns.append(c.key)

        # Add our book-keeping columns.
        table.append_column(sa.Column('nodes', ARRAY(sa.BigInteger)))
        table.append_column(sa.Column('ways', ARRAY(source.id_column.type)))
        table.append_column(sa.Column('geom', source.data.c.geom.type))

        self.src = source

    def set_num_threads(self, num):
        self.numthreads = num

    def source_property_columns(self):
        return (self.src.data.c[x] for x in self.prop_columns)

    def dest_property_columns(self):
        return (self.data.c[x] for x in self.prop_columns)

    def construct(self, engine):
        """ Compute table content from scratch.
        """
        with engine.begin() as conn:
            # manual indexes
            wayidx = sa.Index("%s_ways_idx" % (self.data.name),
                              self.data.c.ways, postgresql_using='gin')
            ndsidx = sa.Index("%s_nodes_idx" % (self.data.name),
                              self.data.c.nodes, postgresql_using='gin')
            # drop indexes if any
            conn.execute(DropIndexIfExists(wayidx))
            conn.execute(DropIndexIfExists(ndsidx))

            self.truncate(conn)

        wayproc = _WayCollector(self, engine, creation_mode=True,
                                numthreads=self.numthreads)

        with engine.begin() as conn:
            # get all ways sorted by property columns
            sql = self.src.data.select().order_by(*self.source_property_columns())

            res = conn.execution_options(stream_results=True).execute(sql)
            prev_prop = None
            for w in res:
                prop = dict(((x, w[x]) for x in self.prop_columns))
                # process the next bit when we get to the next property column
                if prev_prop is not None and prop != prev_prop:
                    wayproc.process_segment(prev_prop)

                wayproc.add_way(w['id'], w['nodes'], w['geom'])

                prev_prop = prop

            if prev_prop is not None:
                wayproc.process_segment(prev_prop)

            wayproc.finish()

            # Finally (re)create indices needed for updates.
            wayidx.create(conn)
            ndsidx.create(conn)


class _WayCollector(ThreadableDBObject):
    """Collects a bunch of fusable ways and orders them by the given
       identity property. If the collector is in creation
       mode, it will do some optimations with respect to remembering
       which ways have already been processed. To be more detailed:
       it will only remember to not process again ways that are member
       of multiple relations.
    """

    def __init__(self, parent, engine, creation_mode=False,
                 numthreads=None):
        self.src = parent
        self.update_mode = not creation_mode

        # precompute intersectons if necesary
        self._get_intersections(engine)

        # actual collection of fuasble ways
        self.current_set = []
        # the worker threads
        self.set_num_threads(numthreads)
        self.workers = self.create_worker_queue(engine, self._process_next)

    def _srid(self):
        return self.src.data.c.geom.type.srid

    def _get_intersections(self, engine):
        """ Find all potetial mid-way intersections.
        """
        self.intersections = set()

        # When in update mode, the intersection points are collected on the
        # fly from the ways to be updated
        if self.update_mode:
            self.collected_nodes = Counter()
            return

        # In creation mode, the potential intersections are precomputed
        # from the source table.
        s = self.src.src.data

        # create a list of nodes with their position in the way
        nlist = sa.select([s.c.nodes,
                        sa.func.generate_subscripts(s.c.nodes, 1).label('i')])\
                    .alias("nodelist")
        # weigh each node by position (front, middle, end)
        wei = sa.select([nlist.c.nodes[nlist.c.i].label('nid'),
                     sa.case([(sa.or_(nlist.c.i == 1,
                                nlist.c.i == sa.func.array_length(nlist.c.nodes, 1)),
                            1)],
                          else_ = 2).label('w')
                     ]).alias('weighted')
        # sum up the weights for each node
        total = sa.select([wei.c.nid.label('nid'), saf.sum(wei.c.w).label('sum')])\
                  .group_by(wei.c.nid).alias('total')

        # anything with weight larger 2 must be a real intersection
        c = engine.execute(sa.select([total.c.nid]).where(total.c.sum > 2))

        for ele in c:
            self.intersections.add(ele['nid'])

    def process_segment(self, properties):
        self.workers.add_task((properties, self.current_set))
        self.current_set = []

    def add_way(self, osmid, nodes, geom):
        """ Add another way to the current set of ways with similar properties.
        """
        self.current_set.append((osmid, nodes, list(to_shape(geom).coords)))

        # update intersections
        if self.update_mode:
            self.collected_nodes[nodes[0]] += 1
            if len(nodes) > 1:
                for n in nodes[1:-1]:
                    self.collected_nodes[n] += 2
                self.collected_nodes[nodes[-1]] += 1

    def finish(self):
        self.workers.finish()
        del self.intersections

    def _process_next(self, item):
        properties, inways = item

        segments = set()
        fuse_pts = defaultdict(list)

        # add all ways to a temporary list and find potential fuse points
        for osmid, nodes, geom in inways:
            print("For ways")
            nnodes = len(nodes)
            # find all nodes that are forced intersections inside the line
            splitidx = [x for x in range(1, nnodes - 1)
                         if nodes[x] in self.intersections]
            splitidx = [0] + splitidx + [nnodes - 1]

            for i in range(len(splitidx) - 1):
                f = splitidx[i]
                t = splitidx[i+1] + 1
                w = _Segment(osmid, nodes[f:t], geom[f:t])
                if w.first() not in self.intersections:
                    fuse_pts[w.first()].append(w)
                if w.last() not in self.intersections:
                    fuse_pts[w.last()].append(w)
                segments.add(w)
                print("AddFor ways")

        # Rejoin the ways on the fuse points
        for nid, ends in fuse_pts.items():
            if len(ends) != 2:
                continue
            print("Fuseing")
            w1, w2 = ends
            if w1 is w2:
                continue # circular way found
            othernode = w1.fuse(w2, nid)
            if othernode is not None and othernode in fuse_pts:
                otherways = fuse_pts[othernode]
                if len(otherways) == 2:
                    if otherways[0] == w2:
                        otherways[0] = w1
                    else:
                        otherways[1] = w1
            segments.remove(w2)

        # and write everything out
        for w in segments:
            print("Print way")
            self._write_segment(properties, w)

    def _write_segment(self, props, segment):
        fields = { 'nodes' : segment.nodes,
                   'ways' : segment.osmids,
                   'geom' : from_shape(LineString(segment.geom), srid=self._srid()) }
        fields.update(props)
        self.thread.conn.execute(self.src.data.insert(fields))



class _Segment(object):

    def __init__(self, osmid, nodes, geom):
        self.osmids = set((osmid,))
        self.nodes = nodes
        self.geom = geom

    def first(self):
        return self.nodes[0]

    def last(self):
        return self.nodes[-1]

    def fuse(self, other, node):
        """ Fuse this way with the given way at the given node returning
            the now open end of the `other` way.

            The direction of the fused way is arbitrary. The other way may
            be destroyed.
        """
        if self.nodes[-1] != node:
            assert(self.nodes[0] == node)
            self.reverse()

        if other.nodes[0] != node:
            assert(other.nodes[-1] == node)
            other.reverse()

        if self.nodes == other.nodes:
            # The way is reversing back on itself, throw away the other part.
            return None

        self.osmids.update(other.osmids)
        self.nodes.extend(other.nodes[1:])
        self.geom.extend(other.geom[1:])

        return other.nodes[-1]

    def reverse(self):
        self.nodes.reverse()
        self.geom.reverse()
