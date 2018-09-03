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

import logging
from collections import Counter, defaultdict

import sqlalchemy as sa
import sqlalchemy.sql.functions as saf
import osgende.common.sqlalchemy as osa
from sqlalchemy.dialects.postgresql import ARRAY
from geoalchemy2.shape import from_shape, to_shape
from sqlalchemy.dialects import postgresql

from shapely.geometry import LineString

from osgende.common.threads import ThreadableDBObject
from osgende.common.table import TableSource
from osgende.common.sqlalchemy import DropIndexIfExists

log = logging.getLogger(__name__)

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
        table = sa.Table(name, meta,
                         sa.Column('id', sa.BigInteger,
                                   primary_key=True, autoincrement=True)
                        )

        # Copy all columns that are used to identify the way type.
        self.prop_columns = []
        for c in prop_cols:
            table.append_column(sa.Column(c.name, c.type))
            self.prop_columns.append(c.name)

        # Add our book-keeping columns.
        table.append_column(sa.Column('nodes', ARRAY(sa.BigInteger)))
        table.append_column(sa.Column('ways', ARRAY(source.c.id.type)))
        table.append_column(sa.Column('geom', source.data.c.geom.type))

        super().__init__(table, name + "_changeset")

        self.src = source
        self.set_num_threads(meta.info.get('num_threads', 1))

    def set_num_threads(self, num):
        self.numthreads = num

    @property
    def srid(self):
        return self.src.c.geom.type.srid

    def source_property_columns(self):
        return (self.src.c[x] for x in self.prop_columns)

    def dest_property_columns(self):
        return (self.c[x] for x in self.prop_columns)

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
            wayset = list()
            for w in res:
                prop = tuple((w[x] for x in self.prop_columns))
                # process the next bit when we get to the next property column
                if prev_prop is not None and prop != prev_prop:
                    wayproc.process_ways(prev_prop, wayset)
                    wayset = list()

                wayset.append((w['id'], w['nodes'], list(to_shape(w['geom']).coords)))
                prev_prop = prop

            if prev_prop is not None:
                wayproc.process_ways(prev_prop, wayset)

            wayproc.finish()

            # Finally (re)create indices needed for updates.
            wayidx.create(conn)
            ndsidx.create(conn)

    def update(self, engine):
        """ Update changed segments.
        """
        wayproc = _WayCollector(self, engine, creation_mode=False,
                                numthreads=self.numthreads)
        waysdone = set()

        with engine.begin() as conn:
            log.info("Collecting changed and new ways")
            sql = self.src.data.select()\
                    .where(self.src.c.id.in_(self.src.select_add_modify()))

            for w in conn.execute(sql):
                prop = tuple((w[x] for x in self.prop_columns))
                wayproc.add_way(prop, w['id'], w['nodes'], w['geom'])
                waysdone.add(w['id'])

            log.info("Collecting points effected by update")
            # 1. nodes in added or changed ways
            waychg = sa.select([saf.func.unnest(self.src.c.nodes)])\
                       .where(self.src.c.id.in_(self.src.select_add_modify()))
            # 2. nodes in segments where ways are changed
            waysel = sa.select([self.src.cc.id.label('tid')])
            segchg = sa.select([saf.func.unnest(self.c.nodes).label('tid')])\
                       .where(self.c.ways.op('&& ARRAY')(waysel))
            conn.execute(osa.CreateTableAs('temp_updated_nodes',
                                           sa.union(segchg, waychg).alias('sub'),
                                           temporary=False))
            temp_nodes = sa.Table('temp_updated_nodes', sa.MetaData(),
                                  autoload_with=conn)

            if log.isEnabledFor(logging.DEBUG):
                log.debug("Nodes needing updating: ",
                        [x for x in conn.execute(temp_nodes.select())])

            deleted_ids = {}
            # throw out all segments that have one of these points
            log.info("Segments with bad intersections...")
            # SQLAlchemy cannot produce DELETE FROM ... USING syntax
            # Falling back to handwritten SQL instead.
            q = """%s USING temp_updated_nodes
                    WHERE nodes && ARRAY[temp_updated_nodes.tid]
                    RETURNING id, ways""" \
                 % (str(self.data.delete()))

            while True:
                additional_ways = set()
                for c in conn.execute(q):
                    for w in c['ways']:
                        if w not in waysdone:
                            additional_ways.add(w)
                    deleted_ids[c['id']] = 'D'

                if not additional_ways:
                    break

                todo_nodes = set()
                sql = self.src.data.select()\
                        .where(self.src.c.id.in_(list(additional_ways)))
                for w in conn.execute(sql):
                    prop = tuple((w[x] for x in self.prop_columns))
                    wayproc.add_way(prop, w['id'], w['nodes'], w['geom'])
                    waysdone.add(w['id'])
                    todo_nodes.update(w['nodes'][1:-1])

                if not todo_nodes:
                    break

                q = self.data.delete()\
                      .where(self.c.nodes.overlap(
                         sa.cast(todo_nodes, type_=self.c.nodes.type)))\
                      .returning(self.c.id, self.c.ways)

            # done, add the result back to the table
            log.info("Processing segments")
            cur_id = conn.scalar(sa.select([saf.max(self.c.id)]))
            first_new_id = 0 if cur_id is None else cur_id + 1

            wayproc.process_cached_ways()
            wayproc.finish()

            # add all newly created segments to the update table
            if self.change is not None:
                self.write_change_table(conn, deleted_ids)
                conn.execute(self.change.insert().from_select(self.cc,
                  sa.select([self.c.id, sa.text("'A'")])
                    .where(self.c.id >= first_new_id)))


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

        if self.update_mode:
            # cache of ways to process
            self.way_cache = []
            # When in update mode, the intersection points are collected on the
            # fly from the ways to be updated
            self.collected_nodes = Counter()
        else:
            # precompute intersections
            self._get_intersections_from_db(engine)

        # the worker threads
        self.set_num_threads(numthreads)
        self.workers = self.create_worker_queue(engine, self._process_next)

    @property
    def srid(self):
        return self.src.c.geom.type.srid

    def _get_intersections_from_db(self, engine):
        """ Find all potetial mid-way intersections.
        """
        self.intersections = set()

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

    def process_ways(self, properties, ways):
        self.workers.add_task((properties, ways))

    def process_cached_ways(self):
        # compute intersections from node counts
        self.intersections = set((x for x, cnt in self.collected_nodes.items() if cnt > 2))

        # sort ways by property in place
        self.way_cache.sort(key=lambda w: w[0])

        # now process ways in groups
        prev_prop = None
        wayset = list()
        for pstr, prop, wayinfo in self.way_cache:
            # process the next bit when we get to the next property column
            if prev_prop is not None and prop != prev_prop:
                self.process_ways(prev_prop, wayset)
                wayset = list()

            wayset.append(wayinfo)
            prev_prop = prop

        if prev_prop is not None:
            self.process_ways(prev_prop, wayset)

        self.way_cache = []


    def add_way(self, props, osmid, nodes, geom):
        """ Add another way to the current set of ways with similar properties.
        """
        assert(self.update_mode)
        self.way_cache.append((str(props), props, (osmid, nodes, list(to_shape(geom).coords))))

        # update intersections
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
            nnodes = len(nodes)
            # find all nodes that are forced intersections inside the line
            splitidx = [x for x in range(1, nnodes - 1)
                         if nodes[x] in self.intersections]
            splitidx = [0] + splitidx + [nnodes - 1]

            for i in range(len(splitidx) - 1):
                f = splitidx[i]
                t = splitidx[i+1] + 1
                w = _Segment(osmid, nodes[f:t], geom[f:t])
                if w.first not in self.intersections:
                    fuse_pts[w.first].append(w)
                if w.last not in self.intersections:
                    fuse_pts[w.last].append(w)
                segments.add(w)

        # Rejoin the ways on the fuse points
        for nid, ends in fuse_pts.items():
            if len(ends) != 2:
                continue
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
            self._write_segment(properties, w)

    def _write_segment(self, props, segment):
        fields = {'nodes' : segment.nodes,
                  'ways' : segment.osmids,
                  'geom' : from_shape(LineString(segment.geom), srid=self.srid)}
        fields.update(dict(zip(self.src.prop_columns, props)))
        self.thread.conn.execute(self.src.data.insert(fields))



class _Segment(object):

    def __init__(self, osmid, nodes, geom):
        self.osmids = set((osmid,))
        self.nodes = nodes
        self.geom = geom

    @property
    def first(self):
        return self.nodes[0]

    @property
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
