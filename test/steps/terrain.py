from lettuce import *
from shapely.geometry import LineString

import logging
import os

if 'LOGFILE' in os.environ:
    logging.basicConfig(
        filename=os.environ.get('LOGFILE','run.log'), 
        level=getattr(logging, os.environ.get('LOGLEVEL','info').upper())
    )
else:
    logging.basicConfig(
        level=getattr(logging, os.environ.get('LOGLEVEL','info').upper())
    )

@world.absorb
def as_linegeom(route):
    points = [tuple([float(x) for x in p.split(',')]) for p in route.split()]
    return LineString(points)

