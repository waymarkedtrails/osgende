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

from osgende.common.postgisconn import PGTable
from osgende.common.geom import FusableWay
import shapely.geometry as sgeom
from datetime import datetime as dt

class RelationSegments(PGTable):
    """ Builds a routable network out of OSM route relations.
    
        Segments are the basic way system of the network.

        This table only collects the information about the network geometry.
        If additional information needs to be stored, you should create an
        additional table and connect the two via the id primary key.

        Segments require an up-to-date country table. Note, however, that the
        country of the Segment is only calculated when it is updated. If a segment
        changes a country due to movement of a boundary, this will go undetected.
    """
    def __init__(self, db, name, subset, country_table=None, country_column='code'):
        PGTable.__init__(self, db, name)
        self.subset = subset
        self.country_table = country_table
        self.country_column = country_column

    def create(self, with_geom_index=True):
        if self.country_table is None:
            country_col = ''
        else:
            coltype = self.country_table.get_column_type(self.country_column)
            if coltype is None:
                raise Exception("column in country table not found")
            country_col = 'country %s,' % coltype
        PGTable.create(self,
                """(id         bigserial PRIMARY KEY,
                    nodes      bigint[],
                    %s
                    ways       bigint[],
                    rels       bigint[]
                )""" % country_col)

        self.add_geometry_column("geom", "900913", 'LINESTRING', with_index=with_geom_index)

    def construct(self):
        """Collect all segments.
        """
        self.first_new_id = self.select_one("""SELECT last_value FROM %s_id_seq""" % (self.table)) + 1
        self.truncate()
        wayproc = _WayCollector(self, self.country_table, self.country_column, 
                                self.subset, creation_mode=True)

        sortedrels = list(wayproc.relations)
        sortedrels.sort()
        for rel in sortedrels:
            print "Processing relation",rel
            ways = self.select_column("""SELECT member_id FROM relation_members 
                                  WHERE member_type = 'W' AND
                                        relation_id = %s""", (rel,))
            if ways:
                for w in ways:
                    wayproc.add_way(w)

                # Put the ways collected so far into segments
                wayproc.process_segments()



    def update(self, uptable):
        """Update changed segments.
        """
        self.first_new_id = self.select_one("""SELECT last_value FROM %s_id_seq""" % (self.table)) + 1
        wayproc = _WayCollector(self, self.country_table, 
                                self.country_column, self.subset,
                                precompute_intersections=False)
        # print "Valid relations:", wayproc.relations

        print dt.now(), "Collecting changed and new ways"
        self.query("""CREATE TEMP TABLE temp_updated_ways AS
                      (SELECT id, nodes FROM ways
                         WHERE id IN
                         (SELECT DISTINCT member_id FROM
                          (SELECT relation_id, member_id
                             FROM relation_members rm, relations r
                            WHERE r.id = rm.relation_id
                              AND rm.member_type = 'W'
                              AND %s) as relmem
                          WHERE relation_id IN (SELECT id FROM relation_changeset 
                                                WHERE action != 'D')
                             OR member_id IN (SELECT id FROM way_changeset 
                                              WHERE action = 'M')
                          )
                      )""" % (self.subset))
        print dt.now(), "Adding those ways to changeset"
        cur = self.select_cursor("SELECT id, nodes FROM temp_updated_ways")
        for c in cur:
            wayproc.add_way(c[0], c[1])


        print dt.now(), "Collecting points effected by update"
        # collect all nodes that are affected by the update:
        #  1. nodes in segments whose relation or ways have changed
        #  2. nodes in added or changed ways
        #  3. nodes that have been moved
        self.query("""CREATE TEMP TABLE temp_updated_nodes AS
            ((SELECT unnest(ARRAY[nodes[1],nodes[array_length(nodes,1)]]) as id FROM %s
              WHERE ways && ARRAY(SELECT id FROM way_changeset)
                 OR rels && ARRAY(SELECT id FROM relation_changeset)
             )
            UNION
            (SELECT unnest(nodes) as id FROM temp_updated_ways)
            UNION
             (SELECT id FROM node_changeset WHERE action = 'M')
            )
            """ % (self.table))

        #print "Nodes needing updating:", self.select_column("SELECT * FROM temp_updated_nodes")

        # throw out all segments that have one of these points
        print dt.now(), "Segments with bad intersections..."
        cur = self.select_cursor("""DELETE FROM %s
                                    WHERE nodes && 
                                           ARRAY(SELECT id FROM temp_updated_nodes)
                                    RETURNING ways, geom""" % (self.table))
        for c in cur:
            for w in c[0]:
                #print w
                wayproc.add_way(w)
            uptable.add(c[1], 'D')

        # done, add the result back to the table
        print dt.now(), "Processing segments"
        wayproc.process_segments()

        # add all newly created segments to the update table
        uptable.query("""INSERT INTO %s (action,geom) 
                      SELECT 'C', geom FROM %s WHERE id >= %%s"""
                     % (uptable.table, self.table), (self.first_new_id, )) 


class _WayCollector:
    """Collects a bunch of fusable ways and orders them by
       relation they belong to. If the collector is in creation
       mode, it will do some optimations with respect to remembering
       which ways have already been processed. To be more detailed:
       it will only remember to not process again ways that are member
       of multiple relations.
    """

    def __init__(self, table, cntrytab, cntrycol, subset,
                  creation_mode=False, precompute_intersections=True):
        self.table = table
        self.subset = subset
        if cntrytab is None:
            self._write_segment = self._write_segment_without_country
        else:
            self._write_segment = self._write_segment_with_country
            self.country_table = cntrytab.table
            self.country_column = cntrycol
        self.not_creation_mode = not creation_mode
        self.intersections_from_ways = not precompute_intersections
        self._get_intersections()

        # Next get the set of relevant relations
        self.relations = set(self.table.select_column(
            "SELECT id FROM relations WHERE %s" % (subset)))

        if not self.relations:
            print "WARNING: no relevant relations found"
            self.relations = {}

        # ways already processed
        self.waysdone = set()

        # actual collection of fusable ways
        self.relgroups = {}

    def add_way(self, way, nodes=None):
        """Add another OSM way accroding to its relations.
        """
        if way in self.waysdone:
           return


        # Determine the set of relation/role pairs for each way
        wcur = self.table.select_cursor("""SELECT relation_id, member_role
                              FROM relation_members
                              WHERE member_type = 'W'
                                AND member_id = %s
                              ORDER BY relation_id""", (way,))
        membership = []
        for c in wcur:
            # print "Potential member",c
            if c[0] in self.relations:
                membership.append((c[0], c[1]))
                # print "approved"
        if len(membership) == 0:
            return
        # We actually only need to remember ways with more than one
        # relation on them. All others are not expected to come up again.
        #if self.not_creation_mode or len(membership) > 1:
        self.waysdone.add(way)

        membership = tuple(membership)
        # get the nodes
        if nodes is None:
            nodes = self.table.select_one("""SELECT nodes FROM ways
                                       WHERE id = %s""", (way,))

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
                self._add_intersection(nodes[0], 0.5)
                if len(nodes) > 1:
                    for n in nodes[1:-1]:
                        self._add_intersection(n, 1)
                    self._add_intersection(nodes[-1], 0.5)

        else:
            print "No nodes. Dropped"

    def _add_intersection(self, nid, weight):
        if nid in self.collected_nodes:
            self.collected_nodes[nid] += weight
        else:
            self.collected_nodes[nid] = weight


    def process_segments(self):
        """Fuse and write out the ways collected so far.
        """
        if self.intersections_from_ways:
            intersections = set([k for (k,v) in self.collected_nodes.iteritems() if v > 1])
        else:
            intersections = None
            
        for (rels, collector) in self.relgroups.iteritems():
            # print "Processing collector", collector
            relids = [x for (x,y) in rels]
            collector.make_segments(intersections=intersections)
            for w in collector.ways:
                self._write_segment(w, relids)
        self.relgroups = {}

    def _get_intersections(self):
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
        c = self.table.select("""
          SELECT nid
          FROM (
            SELECT nid, sum(w), count(*)
            FROM (
              SELECT nodes[i] as nid,
                     (case when i = 1 or i = array_length(nodes,1) then 0.5 
                      else 1 end) as w
              FROM (
                  SELECT nodes, generate_subscripts(nodes, 1) as i
                  FROM ways
                  WHERE id IN 
                    (SELECT DISTINCT member_id 
                     FROM relation_members
                     WHERE member_type = 'W'
                       AND relation_id IN
                             (SELECT id FROM relations WHERE %s)
                    )
              ) as nodelist 
            ) as weighted
            GROUP BY nid
          ) as total
          WHERE sum > 1; """ % (self.subset))

        for ele in c:
            self.intersections.add(ele['nid'])



    def _write_segment_with_country(self, way, relations):
        points = []
        # get the node geometries and the countries
        countries = {}
        prevpoints = (0,0)
        for n in way.nodes:
            res = self.table.select_row("""SELECT n.geom, c.%s 
                                         FROM (SELECT ST_Transform(geom,900913) as geom
                                               FROM nodes WHERE id=%%s) as n 
                                           LEFT JOIN %s c 
                                           ON ST_Within(n.geom,c.geom)
                                         LIMIT 1
                                      """ % (self.country_column, self.country_table)
                                   ,(n,))
            if res is not None:
                pnts = (res[0].x, res[0].y)
                if pnts == prevpoints:
                    points.append((res[0].x+0.00000001, res[0].y))
                else:
                    points.append(pnts)
                prevpoints = pnts
                countries[res[1]] = countries.get(res[1], 0) + 1

        # ignore ways where the node geometries are missing
        if len(points) > 1:
            bestcountry = max(countries.iteritems(), key=lambda x: x[1])[0]
            line = sgeom.LineString(points)
            line._crs = 900913
            
            self.table.query("""INSERT INTO %s 
                                   (nodes, country, rels, ways, geom)
                                   VALUES(%%s, %%s, %%s, %%s, %%s
                                   )""" % (self.table.table),
                                 (way.nodes, bestcountry,
                                  relations, 
                                  way.ways, line))
            self.table.db.commit()
        else:
            print "Warning: empty way", way



    def _write_segment_without_country(self, way, relations):    
        points = []
        # get the node geometries and the countries
        countries = {}
        prevpoints = (0,0)
        for n in way.nodes:
            res = self.table.select_row("""SELECT ST_Transform(geom,900913) as geom
                                               FROM nodes WHERE id=%s""", (n,))
            if res is not None:
                pnts = (res[0].x, res[0].y)
                if pnts == prevpoints:
                    points.append((res[0].x+0.00000001, res[0].y))
                else:
                    points.append(pnts)
                prevpoints = pnts

        # ignore ways where the node geometries are missing
        if len(points) > 1:
            line = sgeom.LineString(points)
            line._crs = 900913
            
            self.table.query("""INSERT INTO %s 
                                   (nodes, rels, ways, geom)
                                   VALUES(%%s, %%s, %%s, %%s
                                   )""" % (self.table.table),
                                 (way.nodes,
                                  relations, 
                                  way.ways, line))
            self.table.db.commit()
        else:
            print "Warning: empty way", way




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
        # print "Split points:", splitidx
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
        for (node,ways) in self.pointlist.iteritems():
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



