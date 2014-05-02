# This file is part of Osgende
# Copyright (C) 2011-2014 Sarah Hoffmann
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
"""
A graph of a collection of line segments, representing a connected
route. Holes in the graph are closed with artifical way sections.
The graph has the notion of a main route and forks and secondary routes.
"""

import logging

from collections import defaultdict, namedtuple
from shapely.geometry import Point, LineString
from Queue import PriorityQueue

logger = logging.getLogger(__name__)

# a segment and a starting point
GraphVector = namedtuple('GraphVector', ['segment', 'point'])

# GraphVector with additional distance
DikstraEdge = namedtuple('DikstraEdge', [ 'dist', 'vec' ])

# Todo list item consisting of: distance, index of start point, todo point
TodoItem = namedtuple('TodoItem', ['dist', 'start', 'point'])


class RouteGraphSegment(object):
    """ An edge within a route graph.
    """

    def __init__(self, segid, geom, firstpnt, lastpnt):
        # Directional way: 0 - both, 1, forward only, -1 - backward only
        self.direction = 0
        # data from segment
        self.segid = segid
        # geometry
        self.geom = geom
        # OSM id of starting point
        self.firstpnt = firstpnt
        # OSM id of end point
        self.lastpnt = lastpnt
        # main paths: next vector
        self.forward = None
        self.backward = None # TODO unimplemented

    def reverse(self):
        """ Reverse direction of way and also switch
            forward and backward connections.
        """
        self.direction = -self.direction
        self.geom.reverse()
        tmp = self.lastpnt
        self.lastpnt = self.firstpnt
        self.firstpnt = tmp

    def __repr__(self):
        return "RouteGraphSegment(id=%d,firstpnt=%d,lastpnt=%d)" % (
                self.segid, self.firstpnt, self.lastpnt)


class RouteGraphPoint(object):
    """ A node within the route graph.
        The OSM node representing this graph node is `nodeid`.
    """
    def __init__(self, nodeid, coords):
        # id of independent subnet within the route (use by _mark_subgraphs)
        self.subnet = -1
        # OSM id of the corresponding node
        self.nodeid = nodeid
        # geometry
        self.coords = Point(coords)
        # outgoing edges as GraphVector(edge, next point)
        self.edges = []

    def distance_to(self, point):
        """ Returns the direct sitance to another RouteGraphPoint.
        """
        return self.coords.distance(point.coords)


    def __repr__(self):
        return "RouteGraphPoint(nodeid=%r,subnet=%r,edges=%r)" % (
                   self.nodeid, self.subnet, self.edges)



class RouteGraph(object):
    """ A directed graph of a route.

        The graph needs to be built up calling add_segment()
        for each segment and then sorted with build_directed_graph().
    """

    def __init__(self):
        # the starting point (a GraphVector(start segment, start point id))
        self.start = None
        # collection of RouteGraphPoints, hashed by their OSM id 
        self.nodes = {}
        # list of RouteGraphSegments
        self.segments = []
        
    def add_segment(self, segment):
        """Add a new segment (of type RouteGraphSegment). 
           This can only be done during built-up of the profile.
        """
        self.segments.append(segment)
        snode = self._get_or_create_node(segment.firstpnt, segment.geom.coords[0])
        snode.edges.append(GraphVector(segment, segment.lastpnt))
        snode = self._get_or_create_node(segment.lastpnt, segment.geom.coords[-1])
        snode.edges.append(GraphVector(segment, segment.firstpnt))

    def _get_or_create_node(self, nid, geom):
        if nid in self.nodes:
            node = self.nodes[nid]
        else:
            node = RouteGraphPoint(nid, geom)
            self.nodes[nid] = node
        return node


    def build_directed_graph(self):
        """ Make a directed graph out of the collection of segments.
        """
        if len(self.segments) == 1:
            # Simple case with only one segment
            self.start = GraphVector(self.segments[0], self.segments[0].firstpnt)
            return

        # step 1(a): mark out unconnected parts of the route
        endpoints = self._mark_subgraphs()
        # step 1(b): split circular ways
        for i in range(len(endpoints)):
            logger.debug("Endpoints subnet %d: %r" % (i, endpoints[i]))
            if len(endpoints[i]) <= 1:
                endpoints[i] = self._decycle_subgraph(i, endpoints[i])
                logger.debug("Endpoints subnet %d after decycling: %r" % (i, endpoints[i]))


        # step 2: make one large network
        if len(endpoints) > 1:
            # Multiple subnets, create artificial connections.
            danglings = self._connect_subgraphs(endpoints)
        else:
            # Simple case. We only have a single subnet
            danglings = endpoints[0]

        if len(danglings) < 2:
            raise Exception("Route must have at least two endpoints")
        
        # step 3: compute the routes
        self._compute_main_route(danglings)


    def get_main_geometry(self):
        """Return an unbroken line string for the complete geometry.
        """
        current, lastpoint = self.start
        logger.debug("get_main_geometry start: %r" % (self.start,))
        coords = [self.nodes[lastpoint].coords.coords[0]]
        while current:
            logger.debug("get_main_geometry current: %r" % current)
            logger.debug("get_main_geometry coords: %s" % (current.geom.coords[:],))
            logger.debug("get_main_geometry lastpoint: %r" % lastpoint)
            if lastpoint == current.firstpnt:
                coords.extend(current.geom.coords[1:])
                lastpoint = current.lastpnt
            else:
                coords.extend(current.geom.coords[-2::-1])
                lastpoint = current.firstpnt
            current = current.forward

        logger.debug("get_main_geometry final coords: %s", (coords,))
        return LineString(coords)

    def _compute_main_route(self, startpoints):
        """ Makes the undirected graph directed and computes the main route.
            startpoints is a list of all node ids to be used as starting
            points.

            Spagetti-Algorithm:
            do a Dikstra-like forward search (for the shortest path)
            up to the point where all the starting points meet. Then
            take the two longest of the paths and use that as the
            main path.
        """
        #
        # forward: find the point where all starting points meet
        # using a Dikstra-like shortest path algorithm
        #
        # XXX the forward should accutally only stop when all segments
        #     have been worked, then the longest path should be found
        #     using all centerpoints that have been encountered.

        # initialise the point store on the nodes:
        # for each start point it contains a DikstraEdge tuple)
        for n in self.nodes.itervalues():
            n.dikstra = [None for x in range(len(startpoints))]

        # todolist. Entries are TodoItems
        todo = PriorityQueue()
        # initial fill
        for s in range(len(startpoints)):
            todo.put(TodoItem(0.0, s, startpoints[s]))
            # startpoints are shortest to themselves
            startpoints[s].dikstra[s] = DikstraEdge(0, GraphVector(None, startpoints[s]))
        logger.debug("Node list: %r" % (self.nodes, ))
        logger.debug("Start nodes: %r" % (startpoints, ))

        centerpoint = None
        while not todo.empty() and centerpoint is None:
            dist, startidx, currentpt = todo.get()
            logger.debug("//dikstra// next %f, %d, %r" % (dist, startidx, currentpt))

            # found a shorter route in the meantime
            if currentpt.dikstra[startidx].dist < dist:
                continue

            for nxtedge, nxtptid in currentpt.edges:
                logger.debug("//dikstra// checking %d ==> %r" % (nxtptid, nxtedge))
                nxtpt = self.nodes[nxtptid]
                newdst = dist + nxtedge.geom.length
                if nxtpt.dikstra[startidx] is None or nxtpt.dikstra[startidx].dist > newdst:
                    # found a better solution, queue
                    nxtpt.dikstra[startidx] = DikstraEdge(newdst, GraphVector(nxtedge, currentpt))
                    todo.put(TodoItem(newdst, startidx, nxtpt))
                if not None in nxtpt.dikstra:
                    # we have finally met in a point, stop here
                    centerpoint = nxtpt
                    break

        assert centerpoint is not None

        #
        # backward: find the two farthest endpoints and span a route
        # btween those two
        #
        ids = sorted(range(len(startpoints)), key=lambda x: centerpoint.dikstra[x].dist, reverse=True)
        # Longest is first point, so enter backwards
        firstpt = ids[0]
        # Second longest is final point, so enter forwards
        lastpt = ids[1]

        logger.debug('%d -- %d %d %r' % (centerpoint.nodeid, firstpt, lastpt, startpoints))

        prevedge = centerpoint.dikstra[lastpt].vec.segment
        current = centerpoint.dikstra[firstpt].vec
        logger.debug('Forward prevedge: %r' % (prevedge))
        while True:
            logger.debug('Forward: %r' % (current,))
            current.segment.forward = prevedge
            prevedge = current.segment
            if current.point == startpoints[firstpt]:
                break
            else: 
                logger.debug('Forward dikstra: %r' % (current.point.dikstra,))
                current = current.point.dikstra[firstpt].vec
        self.start = GraphVector(current.segment, startpoints[firstpt].nodeid)

        prevedge = centerpoint.dikstra[firstpt].vec.segment
        current = centerpoint.dikstra[lastpt].vec
        if current.segment is not None:
            logger.debug('Backward prevedge: %r' % (prevedge))
            while True:
                logger.debug('Backward: %r' % (current,))
                prevedge.forward = current.segment
                prevedge = current.segment
                if current.point == startpoints[lastpt]:
                    break
                else:
                    current = current.point.dikstra[lastpt].vec
            current.segment.forward = None
        

    def _connect_subgraphs(self, endpoints):
        """Find the shortest connections between unconnected subgraphs.

           Returns the OSM IDs for all remaining endpoints.
        """
        # Get rid of circular subnets without an end
        zeropnts = filter(lambda x : x, endpoints)
        netids = range(len(zeropnts))
        finalendpoints = set()
        for pts in zeropnts:
            finalendpoints.update(pts)
            
        # compute all possible connections between subnets
        connections = []
        for frmnet in netids:
            for tonet in netids:
                if frmnet != tonet:
                    conn = [frmnet, None, tonet, None, float("inf")]
                    # find the shortest connection
                    for frmpnt in zeropnts[frmnet]:
                        for topnt in zeropnts[tonet]:
                            pdist = frmpnt.distance_to(topnt)
                            if pdist < conn[4]:
                                conn[1] = frmpnt
                                conn[3] = topnt
                                conn[4] = pdist
                    connections.append(conn)
        # sort by distance
        connections.sort(key=lambda x: x[4])

        # now keep connecting until we have a single graph
        for (frmnet, frmpt, tonet, topt, dist) in connections:
            if netids[frmnet] != netids[tonet]:
                # add the virtual connection
                geom = LineString((frmpt.coords.coords[0], topt.coords.coords[0]))
                segment = RouteGraphSegment(-1, geom, frmpt.nodeid, topt.nodeid)
                frmpt.edges.append(GraphVector(segment, topt.nodeid))
                topt.edges.append(GraphVector(segment, frmpt.nodeid))
                # remove final points
                if topt in finalendpoints:
                    finalendpoints.remove(topt)
                if frmpt in finalendpoints:
                    finalendpoints.remove(frmpt)
                # and join the nets
                oldsubid = netids[tonet]
                newsubid = netids[frmnet]
                netids = [newsubid if x == oldsubid else x for x in netids]

                # are we done yet? (i.e. is there still more than one subnet)
                for x in netids[1:]:
                    if x != netids[0]:
                        break
                else:
                    break
                            
        return list(finalendpoints)


    def _mark_subgraphs(self):
        """Go through the net and mark for each point to which subgraph
           it belongs. Returns for each subnets the endpoints (a list of
           lists). Endpoints are defined as points that have only one
           outgoing edge.
        """
        subnet = 0
        endpoints = []
        for pntid, pnt in self.nodes.iteritems():
            if pnt.subnet < 0:
                # new subnet, follow the net and mark all points
                # with the subnet id
                subnetendpoints = []
                todo = set([pntid,])
                while todo:
                    nxtid = todo.pop()
                    nxt = self.nodes[nxtid]
                    if nxt.subnet < 0:
                        nxt.subnet = subnet
                        todo.update([x.point for x in nxt.edges])
                        if len(nxt.edges) == 1:
                            subnetendpoints.append(nxt)
                subnet += 1
                # add the endpoints for this subnet to the return list
                endpoints.append(subnetendpoints)
                
        return endpoints


    def _decycle_subgraph(self, index, endpoints):
        """ Creates appropriate subpoints for subgraphs that are circles
            (zero endpoints) or dangling circles (one endpoints).

            TODO: it would be a good idea to do the splitting of circles
                  close to another dangling point.
        """
        if len(endpoints) == 0:
            # circles, just split them up on the first best point we can
            # find. XXX not the most efficient way to do that.
            for n in self.nodes.itervalues():
                if n.subnet == index:
                    newid = self._split_node(n.nodeid, newid=-100)
                    if len(n.edges) == 1:
                        return [n, newid]
                    else:
                        # point with multiple connections,
                        # that is dangling only.
                        endpoints = [newid]
                        break
            else:
                raise Exception("Could not find suitable point to split circle")
        
        if len(endpoints) == 1:
            # follow the dangling endpoint to the first fork and split
            # there
            logger.debug("_decycle_subgraph starting point = %r" % endpoints[0])
            curvec = endpoints[0].edges[0]
            while True:
                nextpt = self.nodes[curvec.point]
                logger.debug("_decycle_subgraph before split: %r (not: %r)" % (nextpt, curvec.segment))
                if len(nextpt.edges) > 2:
                    # found the fork
                    newid = self._split_node(nextpt.nodeid, curvec.segment.segid, -101)
                    logger.debug("_decycle_subgraph split at %r/%r" % (nextpt, newid))
                    return [endpoints[0], newid]
                else:
                    for e in nextpt.edges:
                        if e.segment.segid != curvec.segment.segid:
                            curvec = e
                            break
        else:
            # nothing cyclic
            return endpoints


    def _split_node(self, nodeid, origedgeid = None, newid=None):
        """ Duplicates the node and assign the duplicate to one
            of the edges
        """
        newnodeid = -nodeid if newid is None else newid
        oldnode = self.nodes[nodeid]
        newnode = RouteGraphPoint(newnodeid, oldnode.coords)
        newnode.subnet = oldnode.subnet
        self.nodes[newnodeid] = newnode # XXX are node ids always postive?

        assert(len(oldnode.edges) > 1)

        for edge in oldnode.edges:
            if edge.segment.segid != origedgeid:
                oldnode.edges.remove(edge)
                newnode.edges.append(edge)
        
                if edge.segment.firstpnt == nodeid:
                    edge.segment.firstpnt = newnodeid
                elif edge.segment.lastpnt == nodeid:
                    edge.segment.lastpnt = newnodeid
                else:
                    raise Exception("Edge with unexpected endpoints are found.")
                # fix the counter edge
                cpoint = self.nodes[edge.point]
                for i in range(len(cpoint.edges)):
                    if cpoint.edges[i].point == nodeid:
                        segm = cpoint.edges[i].segment
                        cpoint.edges[i] = GraphVector(segm, newnodeid)
                        break
                else:
                    raise Exception("Cannot find counter edge.")

                return newnode

        raise Exception("Found no matching edge, that should not happen.")

               
