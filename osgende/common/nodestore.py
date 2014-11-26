# This file is part of Osgende
# Copyright (C) 2012 Sarah Hoffmann
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
File-backed storage for node geometries.
"""

from shapely.geometry import Point
from osmium import index, osm

class NodeStore(object):
    """Provides a map like persistent storage for node geometries.

       This implementation relies on a osmium location index.
    """

    def __init__(self, filename):
        self.idxfile = open(filename, 'a+')
        self.mapfile = index.DenseLocationMapFile(self.idxfile.fileno())

    def __del__(self):
        self.close()


    def __getitem__(self, nodeid):
        loc = self.mapfile.get(nodeid)
        return Point(loc.lon, loc.lat) 

    def __setitem__(self, nodeid, value):
        self.mapfile.set(nodeid, osm.Location(value.x, value.y))

    def __delitem__(self, nodeid):
        self.mapfile.set(nodeid, osm.Location())

    def close(self):
        if hasattr(self, 'mapfile'):
            print("Used memory by index: %d" % self.mapfile.used_memory())
            del self.mapfile
            self.idxfile.close()


if __name__ == '__main__':
    print("Creating store...")
    store = NodeStore('test.store')

    print("Filling store...")
    for i in range(25500,26000):
        store[i] = Point(1,i/1000.0)

    store.close()
    del store

    print("Reloading store...")
    store = NodeStore('test.store')

    print("Checking store...")
    for i in range(25500,26000):
        assert store[i].y == i/1000.0

    try:
        x = store[1000]
    except KeyError:
        print("Yeah!")

    print("Filling store...")
    for i in range(100055500,100056000):
        store[i] = Point(i/10000000.0,1)

    store.close()
    del store

    print("Reloading store...")
    store = NodeStore('test.store')

    print("Checking store...")
    for i in range(100055500,100056000):
        assert store[i].x == i/10000000.0

    print("Checking store...")
    for i in range(25500,26000):
        assert store[i].y == i/1000.0


    store.close()

