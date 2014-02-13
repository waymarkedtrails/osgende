from lettuce import *
from shapely.geometry import LineString

#import logging
#logging.basicConfig(filename='run.log', level=logging.DEBUG)

@world.absorb
def as_linegeom(route):
    points = [tuple([float(x) for x in p.split(',')]) for p in route.split()]
    return LineString(points)

