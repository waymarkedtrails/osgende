"""
Steps for testing route graphs.
"""

from lettuce import *
from nose.tools import *
from shapely.geometry import LineString

from osgende.common.routegraph import RouteGraph, RouteGraphSegment

@step(u"given the following route segments")
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
    print geom.coords[:]
    print maingeom
    print 'buh'
    assert geom.coords[:] == maingeom or geom.coords[::-1] == maingeom
