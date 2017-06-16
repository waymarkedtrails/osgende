# This file is part of Osgende
# Copyright (C) 2015 Sarah Hoffmann
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

from sqlalchemy import Table, Column, BigInteger, select, Index, or_, bindparam,\
                       case, union, text, column, MetaData, cast, not_, String,\
                       literal_column
from sqlalchemy.sql import functions as sqlf
from sqlalchemy.dialects.postgresql import ARRAY, array
from geoalchemy2 import Geometry
from geoalchemy2.functions import ST_Transform
from geoalchemy2.shape import from_shape, to_shape
from osgende.common.sqlalchemy import CreateTableAs
from osgende.common.threads import ThreadableDBObject
from sys import version_info as python_version
import threading
from osgende.common.geom import FusableWay
from osgende.subtable import TagSubTable
from osgende.common.sqlalchemy import DropIndexIfExists
import osgende.common.threads as othreads
import shapely.geometry as sgeom
from shapely.ops import linemerge

log = logging.getLogger(__name__)

class RouteSegments(object):
    """ Collects the geometries of route relations in a network.

        Segments are the basic way system of the network. They are the longest
        linear pieces in the network that do not overlap and have the same
        set of routes going over them.

        This table only collects the information about the network geometry.
        If additional information needs to be stored, you should create an
        additional table and connect the two via the id primary key.
    """

    def __init__(self, meta, name, osmtables,
                 subset=None, srid=None, geom_change=None):
        if isinstance(subset, str):
            self.subset = text(subset)
        else:
            self.subset = subset
        self.geom_change = geom_change
        self.osmtables = osmtables
        self.meta = meta
        self.numthreads = None

        if srid is None:
            srid = meta.info.get('srid', osmtables.node.data.c.geom.type.srid)

        self.data = Table(name, meta,
                          Column('id', BigInteger, primary_key=True),
                          Column('nodes', ARRAY(BigInteger)),
                          Column('ways', ARRAY(BigInteger)),
                          Column('rels', ARRAY(BigInteger)),
                          Column('geom', Geometry('LINESTRING', srid=srid))
                         )

    def set_num_threads(self, num):
        self.numthreads = num

    def truncate(self, engine):
        engine.execute(self.data.delete())

    def _compute_first(self, conn):
        cur_id = conn.scalar(select([sqlf.max(self.data.c.id)]))
        if cur_id is None:
            self.first_new_id = 0
        else:
            self.first_new_id = cur_id + 1

    def construct(self, engine):
        """Collect all segments.
        """
        t = self.osmtables.member.data
        stm_get_ways = select([t.c.member_id])\
                         .where(t.c.member_type == 'W')\
                         .where(t.c.relation_id == bindparam('id'))\
                         .compile(engine)
        with engine.begin() as conn:
            self._compute_first(conn)

            # manual indexes
            relidx = Index("%s_rels_idx" % (self.data.name),
                           self.data.c.rels, postgresql_using='gin')
            wayidx = Index("%s_ways_idx" % (self.data.name),
                           self.data.c.ways, postgresql_using='gin')
            ndsidx = Index("%s_nodes_idx" % (self.data.name),
                           self.data.c.nodes, postgresql_using='gin')
            # drop indexes if any
            conn.execute(DropIndexIfExists(relidx))
            conn.execute(DropIndexIfExists(wayidx))
            conn.execute(DropIndexIfExists(ndsidx))

            self.truncate(conn)

            wayproc = _WayCollector(self, engine, creation_mode=True,
                                    numthreads=self.numthreads)

        with engine.begin() as conn:
            sortedrels = list(wayproc.relations)
            sortedrels.sort()
            todo = len(sortedrels)
            done = 0
            for rel in sortedrels:
                log.log(logging.INFO if done % 100 == 0 else logging.DEBUG,
                        "Processing relation %d (%d %%)", rel, done * 100 / todo)
                ways = conn.execute(stm_get_ways, { 'id' : rel })
                for w in ways:
                    wayproc.add_way(conn, w[0])

                # Put the ways collected so far into segments
                wayproc.process_segments()
                done += 1

            wayproc.finish()

            # finally prepare indices to speed up update
            relidx.create(conn)
            wayidx.create(conn)
            ndsidx.create(conn)


    def update(self, engine):
        """Update changed segments.
        """
        self._compute_first(engine)
        wayproc = _WayCollector(self, engine, precompute_intersections=False)
        log.debug("Valid relations:", wayproc.relations)

        with engine.begin() as conn:
            log.info("Collecting changed and new ways")
            wt = self.osmtables.way.data
            mt = self.osmtables.member.data
            rt = self.osmtables.relation.data
            sel = select([wt.c.id, wt.c.nodes]).where(wt.c.id.in_(
                        select([mt.c.member_id])
                          .where(rt.c.id == mt.c.relation_id)
                          .where(mt.c.member_type == 'W')
                          .where(self.subset)
                          .where(or_(mt.c.relation_id.in_(
                                     self.osmtables.relation.select_add_modify()),
                                     mt.c.member_id.in_(
                                     self.osmtables.way.select_modify())))
                  ))
            conn.execute(CreateTableAs('temp_updated_ways', sel))
            temp_ways = Table('temp_updated_ways', MetaData(), autoload_with=conn)

            log.info("Adding those ways to changeset")
            res = conn.execute(temp_ways.select())
            for c in res:
                wayproc.add_way(conn, c['id'], c['nodes'])

            log.info("Collecting points effected by update")
            # collect all nodes that are affected by the update:
            #  1. nodes in segments whose relation or ways have changed
            waysel = select([self.osmtables.way.change.c.id])
            relsel = select([self.osmtables.relation.change.c.id])
            segchg = select([sqlf.func.unnest(text('ARRAY[nodes[1],nodes[array_length(nodes,1)]]')).label('id')])\
                      .where(or_(
                          self.data.c.ways.op('&& ARRAY')(waysel),
                          self.data.c.rels.op('&& ARRAY')(relsel)
                            ))
            #  2. nodes in added or changed ways
            waychg = select([sqlf.func.unnest(temp_ways.c.nodes)])
            #  3. nodes that have been moved
            ndchg = self.osmtables.node.select_modify()

            conn.execute(CreateTableAs('temp_updated_nodes',
                                       union(segchg, waychg, ndchg).alias('sub')))
            temp_nodes = Table('temp_updated_nodes', MetaData(), autoload_with=conn)

            if log.isEnabledFor(logging.DEBUG):
                log.debug("Nodes needing updating: ",
                        [x for x in conn.execute(temp_nodes.select())])

            while True:
                # throw out all segments that have one of these points
                log.info("Segments with bad intersections...")
                # SQLAlchemy cannot produce DELTE FROM ... USING syntax
                # Falling back to handwritten SQL instead.
                ret = "ways"
                if self.geom_change is not None:
                    ret += ",geom"
                q = """%s USING temp_updated_nodes
                        WHERE nodes && ARRAY[temp_updated_nodes.id]
                        RETURNING %s""" \
                     % (str(self.data.delete()), ret)
                res = conn.execute(q)

                additional_ways = []
                for c in res:
                    for w in c['ways']:
                        if wayproc.add_way(conn, w):
                            additional_ways.append((w,))
                    if self.geom_change is not None:
                        self.geom_change.add(conn, c['geom'], 'D')

                if additional_ways:
                    conn.execute(temp_nodes.delete())
                    conn.execute(temp_nodes.insert().from_select(temp_nodes.c,
                        select([sqlf.func.unnest(wt.c.nodes[2:sqlf.func.array_upper(wt.c.nodes, 1)-1])])
                                   .where(wt.c.id.in_(additional_ways))))
                else:
                    break

            # done, add the result back to the table
            log.info("Processing segments")
            wayproc.process_segments()
            wayproc.finish()

            # add all newly created segments to the update table
            if self.geom_change is not None:
                self.geom_change.add_from_select(conn,
                      select([ text("'M'"), self.data.c.geom])
                        .where(self.data.c.id >= self.first_new_id))


class _WayCollector(ThreadableDBObject):
    """Collects a bunch of fusable ways and orders them by
       relation they belong to. If the collector is in creation
       mode, it will do some optimations with respect to remembering
       which ways have already been processed. To be more detailed:
       it will only remember to not process again ways that are member
       of multiple relations.
    """

    def __init__(self, parent, engine,
                  creation_mode=False, precompute_intersections=True,
                  numthreads=None):
        self.src = parent
        self.not_creation_mode = not creation_mode
        self.intersections_from_ways = not precompute_intersections
        self._get_intersections(engine)
        self.src_srid = parent.osmtables.node.data.c.geom.type.srid
        self.needs_transform = parent.data.c.geom.type.srid != self.src_srid

        # Next get the set of relevant relations
        r = self.src.osmtables.relation.data
        self.relations = set()
        res = engine.execute(select([r.c.id]).where(self.src.subset))
        for r in res:
            self.relations.add(r['id'])

        if self.relations is None:
            log.warning("WARNING: no relevant relations found")
        else:
            self.relations = set(self.relations)

        # ways already processed
        self.waysdone = set()

        # actual collection of fusable ways
        self.relgroups = {}

        # the worker threads
        self.set_num_threads(numthreads)
        self.workers = self.create_worker_queue(engine, self._process_next)

        # prepare the SQL we are going to need
        m = self.src.osmtables.member.data
        self._stm_way_rels = select([m.c.relation_id, m.c.member_role])\
                               .where(m.c.member_type == 'W')\
                               .where(m.c.member_id == bindparam('id'))\
                               .order_by(m.c.relation_id).compile(engine)

        w = self.src.osmtables.way.data
        self._stm_way_nodes = select([w.c.nodes])\
                                .where(w.c.id == bindparam('id'))\
                                .compile(engine)

    def add_way(self, conn, way, nodes=None):
        """Add another OSM way according to its relations.
        """
        if way in self.waysdone:
           return False

        # Determine the set of relation/role pairs for each way
        wcur = conn.execute(self._stm_way_rels, { 'id' : way })
        membership = []
        for c in wcur:
            if c[0] in self.relations:
                membership.append((c[0], c[1]))
        if len(membership) == 0:
            return False
        # We actually only need to remember ways with more than one
        # relation on them. All others are not expected to come up again.
        #if self.not_creation_mode or len(membership) > 1:
        self.waysdone.add(way)

        membership = tuple(membership)
        # get the nodes
        if nodes is None:
            nodes = conn.scalar(self._stm_way_nodes, { 'id' : way })

        if nodes:
            # remove duplicated nodes if they immediately follow each other
            # This needs to be done to resolve an as of yet unresolved Potlach
            # bug, see: http://trac.openstreetmap.org/ticket/2501
            for i in range(len(nodes)-1,0,-1):
                if nodes[i] == nodes[i-1]: del nodes[i]
            if not membership in self.relgroups:
                self.relgroups[membership] = _SegmentCollector(self.intersections)
            self.relgroups[membership].add(way,nodes)
            # update intersections
            if self.intersections_from_ways:
                self._add_intersection(nodes[0], 1)
                if len(nodes) > 1:
                    for n in nodes[1:-1]:
                        self._add_intersection(n, 2)
                    self._add_intersection(nodes[-1], 1)

            return True

        log.debug("No nodes. Dropped")
        return False

    def _add_intersection(self, nid, weight):
        if nid in self.collected_nodes:
            self.collected_nodes[nid] += weight
        else:
            self.collected_nodes[nid] = weight


    def process_segments(self):
        """Fuse and write out the ways collected so far.
        """
        if self.intersections_from_ways:
            if python_version[0] < 3:
                self.processing_intersections = set([k for (k,v) in self.collected_nodes.iteritems() if v > 2])
            else:
                self.processing_intersections = set([k for (k,v) in self.collected_nodes.items() if v > 2])

        else:
            self.processing_intersections = None

        while self.relgroups:
            (rels, collector) = self.relgroups.popitem()
            relids = [x for (x,y) in rels]
            self.workers.add_task((relids, collector))

    def _process_next(self, item):
        (relids, collector) = item
        collector.make_segments(intersections=self.processing_intersections)
        for w in collector.ways:
            self._write_segment(w, relids)

    def finish(self):
        """Finish up any pending operations.
           Needs only to be called once after all segments have been processed.
        """
        self.workers.finish()
        del self.intersections

    def _get_intersections(self, engine):
        """ Find all potential mid-way intersections.
        """
        self.intersections = set()

        if self.intersections_from_ways:
            self.collected_nodes = {}
            return

        # Explanation of that nasty bit of SQL:
        # Count for each node in how many ways that are part of
        # relevant relations it appears. Start and end nodes count only
        # half because we don't care about two ways meeting if they have
        # the same attributes.

        w = self.src.osmtables.way.data
        r = self.src.osmtables.relation.data
        m = self.src.osmtables.member.data
        # first find the ways that are part in any of the interesting relations
        way_ids = select([m.c.member_id]).distinct()\
                    .where(m.c.member_type == 'W')\
                    .where(m.c.relation_id.in_(
                                select([r.c.id]).where(self.src.subset)))
        # create a list of nodes with their position in the way
        nodelist = select([w.c.nodes,
                           sqlf.func.generate_subscripts(w.c.nodes, 1).label('i')])\
                     .where(w.c.id.in_(way_ids)).alias("nodelist")

        # weigh each node by position (front, middle, end)
        wei = select([nodelist.c.nodes[nodelist.c.i].label('nid'),
                     case([(or_(nodelist.c.i == 1,
                                nodelist.c.i == sqlf.func.array_length(nodelist.c.nodes, 1)),
                            1)],
                          else_ = 2).label('w')
                     ]).alias('weighted')
        # sum up the weights for each node
        total = select([wei.c.nid.label('nid'), sqlf.sum(wei.c.w).label('sum')])\
                  .group_by(wei.c.nid).alias('total')

        # anything with weight larger than 1 must be a real intersection
        c = engine.execute(select([total.c.nid]).where(total.c.sum > 2))

        for ele in c:
            self.intersections.add(ele['nid'])

        log.debug("Final intersections:", self.intersections)


    def _write_segment(self, way, relations):
        # get the node geometries and the countries
        countries = {}
        prevpoints = (0,0)

        points = self.src.osmtables.get_points(way.nodes)

        # ignore ways where the node geometries are missing
        if len(points) > 1:
            line = from_shape(sgeom.LineString(points), self.src_srid)

            if self.needs_transform:
                line = ST_Transform(line, self.src.data.c.geom.type.srid)

            self.thread.conn.execute(self.src.data.insert(
                 { 'nodes' : way.nodes, 
                   'ways' : way.ways,
                   'rels' : relations,
                   'geom' : line }))

            #self.db.commit()
        else:
            log.warning("empty way: %s", way)




class _SegmentCollector:
    """Helper class that collects ways of the same kind and
       creates subequent segments from it.

       'intersections' are nodes where a way must forcably broken up.
    """

    def __init__(self, intersections):
        self.intersections = intersections
        self.ways = set()
        self.pointlist = {}

    def add(self, way, nodes):
        """Add a new way to the list.

           Should any of the middle nodes be an intersection then
           the way is split at this point before adding it to the list.
        """
        # find all nodes that are forced intersecions
        splitidx = [x for x in range(len(nodes))
                     if nodes[x] in self.intersections]
        if len(splitidx) == 0 or splitidx[0] != 0:
            splitidx[:0] = [0]
        if splitidx[-1] != len(nodes)-1:
            splitidx.append(len(nodes)-1)
        for i in range(len(splitidx)-1):
            w = FusableWay(way, nodes[splitidx[i]:splitidx[i+1]+1])
            self.ways.add(w)
            for n in (w.first(),w.last()):
                if n in self.pointlist:
                    self.pointlist[n].append(w)
                else:
                    self.pointlist[n] = [w]


    def make_segments(self, intersections=None):
        """Fuse ways as much as possible.
        """
        if intersections is not None:
            self.intersections=intersections
            self.split_ways()
        for (node,ways) in self.pointlist.items():
            if len(ways) == 2 and not node in self.intersections:
                (w1,w2) = ways
                if w1 != w2:
                    othernode = w1.fuse(w2, node)
                    otherways = self.pointlist[othernode]
                    for i in range(len(otherways)):
                        if otherways[i] == w2:
                            otherways[i] = w1
                    self.ways.remove(w2)

    def split_ways(self):
        """Rebuild the waylist, splitting all ways on
           forced intersections. This will only work if
           all segments in the collector are part of exactly
           one way.
        """
        oldways = self.ways
        self.ways = set()
        self.pointlist = {}
        for w in oldways:
            assert(len(w.ways)==1)
            self.add(w.ways[0], w.nodes)


class Routes(TagSubTable):
    """A relation collection class that gets updated according to
       the changes in a RouteSegments table. If an optional hierarchy
       table is provided, super relations will be updated as well.
    """

    def __init__(self, name, segments, hiertable=None):
        self.segment_table = segments
        self.hierarchy_table = hiertable
        TagSubTable.__init__(self, segments.meta, name,
                             segments.osmtables.relation,
                             subset=segments.subset)

    def construct(self, engine):
        """ Fill the table in order of hierarchy in which they
            appear in the hierarchy table, if applicable.

            This means that we can rely on all subrelations already
            being computed.
        """
        self.truncate(engine)
        if self.hierarchy_table is None:
            self.insert_objects(engine, self.src.select_all(self.subset))
        else:
            h = self.hierarchy_table.data
            subtab = select([h.c.child, sqlf.max(h.c.depth).label("lvl")])\
                       .group_by(h.c.child).alias()
            for level in range(6, 0, -1):
                subset = self.src.data.select().where(subtab.c.lvl == level).where(self.src.data.c.id == subtab.c.child)
                self.insert_objects(engine, subset)


    def update(self, engine):
        firstid = self.segment_table.first_new_id

        with engine.begin() as conn:
            # delete any objects that might have been deleted
            # Note: a relation also might get deleted from this table
            # because it lost its relevant tags.
            conn.execute(self.data.delete().where(self.id_column.in_
                                            (self.src.select_modify_delete())))
            # Collect all changed relations in a temporary table
            sel = select([sqlf.func.unnest(self.segment_table.data.c.rels).label("id")],
                         distinct=True)\
                       .where(self.segment_table.data.c.id >= firstid)

            if self.hierarchy_table is not None:
                sel = select([self.hierarchy_table.data.c.parent], distinct=True)\
                       .where(self.hierarchy_table.data.c.child.in_(
                                sel.union(self.src.select_add_modify()))).alias()

                hmax = self.hierarchy_table.data.alias()
                crosstab = select([hmax.c.child, sqlf.max(hmax.c.depth).label("lvl")])\
                             .group_by(hmax.c.child).alias()

                sel = select([sel.c.parent.label("id"), crosstab.c.lvl])\
                        .where(sel.c.parent == crosstab.c.child)

            conn.execute(CreateTableAs('__tmp_osgende_routes_updaterels', sel,
                         temporary=False))
            tmp_rels = Table('__tmp_osgende_routes_updaterels',
                             MetaData(), autoload_with=conn)

            conn.execute(self.data.delete()\
                           .where(self.id_column.in_(select([tmp_rels.c.id]))))

        # reinsert those that are not deleted
        if self.hierarchy_table is None:
            inssel = self.src.select_all(self.src.data.c.id.in_(tmp_rels.select()))
            self.insert_objects(engine, inssel)
        else:
            for level in range(6, 0, -1):
                where = self.src.data.c.id.in_(select([tmp_rels.c.id])
                                                 .where(tmp_rels.c.lvl == level))
                self.insert_objects(engine, self.src.select_all(where))
        # drop the temporary table
        tmp_rels.drop(engine)

    def build_geometry(self, osmid):
        """ Build the geometry for the given relation. Geometries are
            computed by concatenating the geometries from the segments
            in the order of the relation members.
        """
        # Retrive the segment geometries in the order in which they appear
        # in the relation. Needs to be done iteratively for nested relations.

        # initially put the source relation
        recurse = select([cast('R', String(length=1)).label('tp'),
                          cast(osmid, BigInteger).label('id'),
                          cast(array([osmid]), ARRAY(BigInteger)).label('path'),
                          array([1]).label('pospath')])

        # then iteratively establish the path for all contained elements
        recurse = recurse.cte(recursive=True)
        m = self.segment_table.osmtables.member.data
        recurse = recurse.union_all(
                    select([m.c.member_type, m.c.member_id,
                            recurse.c.path.op('||')(m.c.member_id),
                            recurse.c.pospath.op('||')(m.c.sequence_id)])
                      .where(recurse.c.id == m.c.relation_id)
                      .where(recurse.c.tp == 'R')
                      .where(or_(m.c.member_type != 'R',
                             not_(recurse.c.path.any(m.c.member_id))))
                  )

        # now get the geometries for each way from the segment table
        # We cut exactly the part covered by the way in question.
        s = self.segment_table.data.alias('s')
        w = self.segment_table.osmtables.way.data.alias('w')

        # get the segments involved in each way
        waypaths = select([recurse.c.id, recurse.c.pospath, w.c.nodes.label('nds')])\
                    .where(recurse.c.tp == 'W')\
                    .where(w.c.id == recurse.c.id).alias('x')

        # black magic that cuts the geometry
        # Basic idea: create a table with node id/geometry point for segment
        # another for node id/index for way, match up nodes, sort by way index,
        # so that we get the original order of the way (for forward, backward
        # handling later), reassemble geometry points as LineString.
        # The additional where clause filters duplicate points for way loops
        # on segments, the ST_RemoveRepeatedPoints on ways.
        geom = literal_column("""
               (SELECT ST_RemoveRepeatedPoints(ST_MakeLine(pt))
                 FROM (SELECT (d).geom AS pt
                       FROM (SELECT unnest(s.nodes) AS nd,
                                    ST_DumpPoints(s.geom) AS d) nn
                             JOIN unnest(nds) WITH ORDINALITY AS ll(n,id)
                             ON nn.nd = ll.n
                             WHERE nds[id-1] = any(s.nodes) OR nds[id+1] = any(s.nodes)
                             ORDER BY ll.id) zz)""",
               type_=Geometry)

        sel = select([waypaths.c.id.label('wayid'),
                      s.c.id.label('segid'),
                      geom.label('geom')])\
                .where(array([waypaths.c.id]).op('&&')(s.c.ways))\
                .order_by(waypaths.c.pospath)

        # as geometries may be returned multiple times, this can become
        # a large result, stream it in
        cur = self.thread.conn.execution_options(stream_results=True).execute(sel)

        curway = None
        geom = RouteGeometry()
        for res in cur:
            if curway != res['wayid']:
                if curway is not None:
                    geom.process_segment()

                # set up for the next way
                curway = res['wayid']

            geom.append(to_shape(res['geom']))

        geom.process_segment()

        return geom.geometry()


class RouteGeometry(object):

    def __init__(self):
        self.geom = None
        self.num_segs = 0
        self.todo = []

    def append(self, geom):
        self.todo.append(geom)

    def process_segment(self):
        if not self.todo:
            return

        segment = linemerge(self.todo)

        if segment.geom_type == 'MultiLineString':
            # oh dear, there was a loop, resolve it somehow
            lines = [ l.coords[:] for l in segment ]
            while len(lines) > 1:
                src = lines.pop()
                for i in range(len(lines)):
                    l = lines[i]
                    if src[0] == l[0]:
                        lines[i] = src[:0:-1] + l
                    elif src[-1] == l[0]:
                        lines[i] = src + l[1:]
                    elif src[0] == l[-1]:
                        lines[i] = src[:0:-1] + l[::-1]
                    elif src[-1] == l[-1]:
                        lines[i] = src + l[-2::-1]
                    else:
                        continue
                    break
                else:
                    assert("No match found when merging lines")
            segment = sgeom.LineString(lines[0])

        if self.num_segs == 0:
            self.geom = [segment]
        elif self.num_segs == 1:
            newsegment = linemerge((self.geom[-1], segment))
            if newsegment.geom_type == 'LineString':
                # we still might have to invert the initial geometry
                if newsegment.coords[0] in (segment.coords[0], segment.coords[-1]):
                    self.geom = [sgeom.LineString(newsegment.coords[::-1])]
                else:
                    self.geom = [newsegment]
            else:
                # Lines don't overlap, find the endpoints with the shortest distance
                dist, x, y = min([(sgeom.Point(self.geom[-1].coords[-x]).distance(sgeom.Point(segment.coords[-y])), x, y)
                                          for x in (0, 1) for y in (0, 1)])
                if x == 0:
                    self.geom[-1] = sgeom.LineString(self.geom[-1].coords[::-1])
                if y == 1:
                    segment = sgeom.LineString(segment.coords[::-1])
                if self.geom[-1].coords[-1] == segment.coords[0]:
                    self.geom[-1] = sgeom.LineString(self.geom[-1].coords[:] + segment.coords[1:])
                else:
                    self.geom.append(segment)
        else:
            # just append the segment
            lastpt = sgeom.Point(self.geom[-1].coords[-1])
            dist, x = min([(lastpt.distance(sgeom.Point(segment.coords[-x])), x) for x in (0, 1)])
            if x == 1:
                segment = sgeom.LineString(segment.coords[::-1])
            if dist < 0.000001:
                coords = self.geom[-1].coords[:]
                coords.extend(segment.coords[1:])
                self.geom[-1] = sgeom.LineString(coords)
            else:
                self.geom.append(segment)

        self.todo = []
        self.num_segs += 1

    def geometry(self):
        if self.geom is None:
            return None

        if len(self.geom) == 1:
            return self.geom[0]

        return sgeom.MultiLineString(self.geom)
