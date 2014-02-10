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
    for line in step.hashes:
        world.graph.add_segment(RouteGraphSegment(
                    int(line['id']), world.as_linegeom(line['geom']),
                    int(line['first']), int(line['last'])))
    world.graph.build_directed_graph()

@step(u"the main route is (.*)")
def route_graph_main_route(step, route):
    geom = world.as_linegeom(route)
    maingeom = list(world.graph.get_main_geometry().coords)
    print geom.coords[:]
    print maingeom
    print 'buh'
    assert geom.coords[:] == maingeom or geom.coords[::-1] == maingeom
