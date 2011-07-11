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

from osgende.common.geom import FusableWay,Bbox
import osgende
import shapely.geometry as sgeom
import shapely.ops as sops
import shapely.geos as geos


class RelationPolygons(osgende.OsmosisSubTable):
    """Table of polygons created from relations (aka multipolygons).

       Tries to reconstruct polygons in a fault tolerant way. It should be able to
       handle:

       * simple polygons
       * multipolygons
       * nested relations
       * unclosed ways
       * self-intersecting ways
       * touching holes/polygons
       * ways appearing multiple times (todo)

       'subset' allows to constraint the entries to a subset of relations. Note
       that the table does not know anything about OSM tags and will try to make
       a polygon out of any relation encountered. Creating a table without a
       subquery will work but does not make much sense.

       'child_tags' restricts which subrelations are considered part of the polygon
       description. It contains a list of tags that need to have an equal value
       as the root relations. To include all subrelations, give the empty list.
       If 'child_tags' is set to None, subrelations will be ignored completely.

       As with most tables, this expects the database table to be created 
       externally. The only constraint is that there is an id column and 
       a geometry column that can store
       MULTIPOLYGON types. The name of the column can be given with the 'geom'
       parameter.

       It is possible to transform the final geometries before writing to the database
       by changing the transform string. This can be used, for example, to project
       the geometry or simplify it. TODO describe syntax.
    """

    def __init__(self, db, name, subset = None, child_tags=[], 
                      geom='geom', transform='%s'):
        osgende.OsmosisSubTable.__init__(self, db, 'relation', name, subset)
        self._child_tags = child_tags
        self._geomcol = geom
        self._transform = transform


    def insert_objects(self, wherequery):
        cur = self.select("SELECT id, tags FROM relations %s" 
                         % (wherequery))
        for obj in cur:
            poly = self.compute_polygon(obj['id'], obj['tags'])
            if poly is not None:
                tags = self.transform_tags(obj['id'], obj['tags'])
                values = [obj['id'], poly]
                values.extend(tags.values())

                self.query("INSERT INTO %s (id, %s, %s) VALUES (%%s, %s, %s)" % 
                            (self.table, 
                             self._geomcol,
                             ','.join(tags.keys()),
                             self._transform,
                             ','.join(['%s' for i in range(0,len(tags))])),
                            tuple(values))

    def compute_polygon(self, rid, tags):
        print "Computing polygon out of relation", rid, "Tags:", tags
        nodelist = {}
        waylist = set()
        self.collect_ways(rid, tags, waylist, nodelist, set())
        badnodes = {}
        badways = []
        # find all nodes with two outgoing ways and
        # fuse the ways concerned
        #print "Initial waylist", waylist
        while len(nodelist) > 0:
            #print "Nodelist", nodelist
            (node, ways) = nodelist.popitem()
            if not len(ways) == 2:
                # bad node, just push back for now
                # print "Bad node:",ways
                badnodes[node] = ways
            elif ways[0] != ways[1]:
                # fuse the two ways concerned
                #print "Before fusing",ways[0].nodes,"/",ways[1].nodes
                nextnode = ways[0].fuse(ways[1], node)
                #print "After fusing",ways[0].nodes
                # if we are full circle, the node will already have
                # been treated

                if nextnode in nodelist:
                    nextways = nodelist[nextnode]
                    nextways[nextways.index(ways[1])] = ways[0]
                    waylist.remove(ways[1])
                # XXX nextnode may be in badnodes if the way is not
                # closed or if multiple ways hit the same point.
                if nextnode in badnodes:
                    bn = badnodes[nextnode]
                    for i in range(len(bn)):
                        if bn[i] == ways[1]:
                            bn[i] = ways[0]
                    if len([x for x in bn if x == ways[0]]) == 2:
                        # the way got closed, find the remaining
                        # ways for this badnode and if it is two,
                        # push the node back for processing
                        newways = [x for x in bn if x != ways[0]]
                        if len(newways) == 2:
                            del badnodes[nextnode]
                            nodelist[nextnode] = newways
                        else:
                            badnodes[nextnode] = newways
                    waylist.remove(ways[1])
    
                    
        # Repair any open ways. Simply continue at the
        # closest point. Note that this is not necessarily
        # the correct heuristic.
        
        # but first we need the coordinates of the open nodes
        badcoords = []
        distances = []
        for node in badnodes.iterkeys():
            pt = self.select_one("SELECT geom FROM nodes WHERE id=%s", (node,))
            for (n2,pt2) in badcoords:
                distances.append((node, n2, pt.distance(pt2)))
            badcoords.append((node, pt))
            if len(badnodes[node]) != 1:
                print node,"::",[x.ways for x in badnodes[node]]
            assert len(badnodes[node]) == 1

        # sort by distance
        distances.sort(key=lambda x : x[2])
        
        for (n1,n2,dist) in distances:
            if n1 in badnodes and n2 in badnodes:
                w1 = badnodes.pop(n1)[0]
                w2 = badnodes.pop(n2)[0]
                # if both ways are the same, we are simply done
                if w1 != w2:
                    othernode = w1.append_at(n1, w2, n2)
                    waylist.remove(w2)
                    if othernode in badnodes:
                        badnodes[othernode] = [w1]

        # print "Final waylist", waylist
        # now save away all polygons that are done
        poly = None
        for way in waylist:
            pout = self.make_polygon(way)
            if pout is not None:
                if poly is None:
                    poly = pout
                else:
                    try:
                        poly = poly.symmetric_difference(pout)
                    except geos.TopologicalError:
                        return None

        if poly is not None:
            poly._crs = 4326      

        return poly

    def collect_ways(self, rid, tags, ways, nodelist, waysdone):
        # find all the ways and their start and end node
        cur = self.select("""SELECT member_type as type, member_id as id
                               FROM relation_members
                              WHERE relation_id = %s""", (rid,))
        for obj in cur:
            if obj['type'] == 'R':
                if self._child_tags is not None:
                    # test eligibility
                    issubrel = True
                    if len(self._child_tags) > 0:
                        subtags = self.select_one("""SELECT tags 
                                                     FROM relations 
                                                     WHERE id = %s""",
                                                  (obj['id'],))
                        for t in self._child_tags:
                            if not tags.get(t, None) == subtags.get(t, None):
                                issubrel = False
                    if issubrel:
                        self.collect_ways(obj['id'], tags, ways, nodelist, waysdone)
            elif obj['type'] == 'W':
                if not obj['id'] in waysdone:
                    waysdone.add(obj['id'])
                    nodes = self.select_one("SELECT nodes FROM ways WHERE id=%s"
                                              % obj['id'])
                    if nodes is not None and len(nodes) > 1:
                        # drop ways that are made of only one node.
                        # Happens with osmosis cuts.
                        w = FusableWay(obj['id'], nodes)
                        #print w.nodes
                        ways.add(w)
                        for pt in (w.first(), w.last()):
                            if pt in nodelist:
                                nodelist[pt].append(w)
                            else:
                                nodelist[pt] = [w]

    def make_polygon(self, way):
        """Create a valid (multi)-polygon from a list of points.

           This function can also work with self-intersecting and open polygons.
           Open polygons are simply closed at the open ends. If the line is 
           self-intersecting, then the functions creates the polygon that constitutes
           the outer hull of the line.

           The algorithm works as follows:
           For each line segment (the line between two consequtive points)
            check if it intersects with any previous line segments.
            If there are intersections, take the closest to the line segments
            and cut out the line until the closest intersecting line and
            make a polygon out of that. Remove the line section that was cut out.
           Continue until there are no more self-intersections.
           The union of all polygons is the polygon to be returned.

           TODO: handle intersecting lines.
        """
        # First: get all the geometries of the nodes
        points = []
        for n in way.nodes:
            p = self.select_one("SELECT geom FROM nodes WHERE id = %s", (n,))
            points.append(p.coords[0])

        # if the list is not a closed way, close it
        if not way.is_closed():
            points.append(points[0])

        #print "Initial Geometry:", points

        # Second: build a valid polygon from the points

        # this is a list of bboxes around the points up to the index in the list
        # This way, the intersection computation can be optimized slightly: if the
        # line to test is completely outside the box, we can be sure that there are
        # no intersections at all and skip further tests.
        bboxes = [Bbox(points[0])]
        curpoint = 1
        polygons = []
        while curpoint < len(points):
            lb = bboxes[-1]
            cp = points[curpoint]
            bboxes.append(Bbox(bboxes[-1],cp))
            curbox = Bbox(points[curpoint-1],cp)
            chkpoint = curpoint - 2
            while chkpoint > 0:
                lb = bboxes[chkpoint]
                if lb.intersects(curbox):
                    # potential intersection
                    A = points[chkpoint-1]
                    B = points[chkpoint]
                    C = points[curpoint-1]
                    D = cp
                    det = (B[0]-A[0])*(D[1]-C[1]) - (B[1]-A[1])*(D[0]-C[0])
                    if det == 0:
                        # lines are parallel
                        # XXX check if they overlap
                        # print "Parallel lines in", way.ways
                        assert ((A[1]-C[1])*(D[0]-C[0]) - (A[0]-C[0])*(D[1]-C[1])) != 0
                    else:
                        r = ((A[1]-C[1])*(D[0]-C[0]) - (A[0]-C[0])*(D[1]-C[1]))/det
                        s = ((A[1]-C[1])*(B[0]-A[0]) - (A[0]-C[0])*(B[1]-A[1]))/det
                        if r>=0 and r<=1 and s>=0 and s<=1:
                            # it did indeed intersect, get the point
                            intersection = (A[0]+r*(B[0]-A[0]),A[1]+r*(B[1]-A[1]))
                            # get the offending polygon for our collection
                            subpoints = points[chkpoint:curpoint]
                            if r < 1:
                                subpoints[:0] = [intersection]
                            if s > 0:
                                subpoints.append(intersection)
                            if len(subpoints) > 2:
                                polygons.append(sgeom.Polygon(subpoints))
                            else:
                                print "Warning: strangly overlapping polygon:",points
                            # and remove it from the point list
                            frm = chkpoint if r > 0 else chkpoint - 1
                            tow = curpoint if s < 1 else curpoint + 1
                            points[frm:tow] = [intersection]
                            # redo the bbox list
                            bboxes[frm:] = []
                            if len(bboxes) == 0:
                                bboxes.append(Bbox(intersection))
                            else:
                                bboxes.append(Bbox(bboxes[-1],intersection))
                            if s < 1:
                                bboxes.append(Bbox(bboxes[-1],points[frm+1]))
                            curpoint = frm + 1
                    chkpoint -= 1
                            
                else:
                    # no intersections at all, we are done
                    chkpoint = 0
            curpoint += 1

        #print points
        #assert len(points) == 1
                    
        return sops.cascaded_union(polygons) if len(polygons) > 0 else None
