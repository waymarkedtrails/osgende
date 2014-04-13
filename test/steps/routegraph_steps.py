"""
Steps for testing route graphs.
"""

import os
import logging
from time import time

from lettuce import *
from nose.tools import *
from shapely.geometry import LineString
from shapely.wkt import loads as wkt_loads
from collections import defaultdict

from osgende.common.routegraph import RouteGraph, RouteGraphSegment

logger = logging.getLogger(__name__)

@step(u"the following route segments")
def route_graph_segments(step):
    world.graph = RouteGraph()
    lastpt = 1
    nodehash = {}
    for line in step.hashes:
        geom = world.as_linegeom(line['geom'])
        if geom.coords[0] in nodehash:
            first = nodehash[geom.coords[0]]
        else:
            first = lastpt
            nodehash[geom.coords[0]] = first
            lastpt += 1
        if geom.coords[-1] in nodehash:
            last = nodehash[geom.coords[-1]]
        else:
            last = lastpt
            nodehash[geom.coords[-1]] = last
            lastpt += 1
        world.graph.add_segment(RouteGraphSegment(
                    int(line['id']), geom, first, last))
    world.graph.build_directed_graph()

@step(u"the main route is ([0-9,. -]+)")
def route_graph_main_route(step, route):
    geom = world.as_linegeom(route)
    maingeom = list(world.graph.get_main_geometry().coords)
    assert geom.coords[:] == maingeom or geom.coords[::-1] == maingeom, "Different geometries, got %s" % (str(maingeom))

@step(u"the main route is in \(\((.*)\)\)")
def route_graph_main_routes(step, route):
    maingeom = list(world.graph.get_main_geometry().coords)
    for subroute in route.split('), ('):
        logging.debug("route_graph_main_route: subroute = %r" % str(subroute))
        geom = world.as_linegeom(subroute.strip())
        if geom.coords[:] == maingeom or geom.coords[::-1] == maingeom:
            return

    assert False, "Different geometries, got %s" % (str(maingeom))


@step (u"the segments in scenario (.*)")
def route_graph_set_database(step, scenefile):
    world.segs_by_rel = defaultdict(list)
    sfd = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', scenefile)
    with open(sfd, 'r') as infile:
        for line in infile:
            if line.strip():
                fst, lst, rels, geom = line.split('|')
                segment = { 'first' : int(fst.strip()),
                            'last' : int(lst.strip()),
                            'geom' : wkt_loads(geom.strip())
                          }
                for rel in rels.strip()[1:-1].split(','):
                    world.segs_by_rel[int(rel)].append(segment)
            

@step ("all routes have a main route")
def route_graph_check_routes_from_segment(step):
    for relid, segs in world.segs_by_rel.iteritems():
        if relid in (906585,49178):
            continue
        graph = RouteGraph()
        segid = 0
        logger.debug("Processing %d" % relid)
        for seg in segs:
            s = RouteGraphSegment(segid, seg['geom'], seg['first'], seg['last'])
            logger.debug("Segment %r" % (s,))
            graph.add_segment(s)
            segid += 1
        t1 = time()
        graph.build_directed_graph()
        t2 = time()
        logger.debug('Relation %d took %0.3f ms' % (relid, (t2-t1)*1000.0))
