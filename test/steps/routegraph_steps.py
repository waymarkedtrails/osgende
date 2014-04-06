"""
Steps for testing route graphs.
"""

import os

from lettuce import *
from nose.tools import *
from shapely.geometry import LineString
from shapely.wkt import loads as wkt_loads
from collections import defaultdict

from osgende.common.routegraph import RouteGraph, RouteGraphSegment

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

@step(u"the main route is (.*)")
def route_graph_main_route(step, route):
    geom = world.as_linegeom(route)
    maingeom = list(world.graph.get_main_geometry().coords)
    assert geom.coords[:] == maingeom or geom.coords[::-1] == maingeom


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
                for rel in rels.strip()[1:-2].split(','):
                    world.segs_by_rel[int(rel)].append(segment)
            

@step ("all routes have a main route")
def route_graph_check_routes_from_segment(step):
    for segs in world.segs_by_rel.itervalues():
        graph = RouteGraph()
        segid = 0
        for seg in segs:
            graph.add_segment(RouteGraphSegment(
                   segid, seg['geom'], seg['first'], seg['last']))
            segid += 1
        graph.build_directed_graph()
