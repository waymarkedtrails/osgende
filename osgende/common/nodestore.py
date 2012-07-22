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

from collections import deque
import numpy as np
from shapely.geometry import Point
import threading

class NodeStore(object):
    """Provides a map like persistent storage for node geometries.

       Node geometries are saved in a huge continous array which is
       partly memmapped as needed.
    """

    dt = np.dtype("f8, f8")


    def __init__(self, filename, numbuckets=32, bucketsize=12):
        self.filename = filename
        self.numbuckets = numbuckets
        self.bucketsize = bucketsize
        self.bucketlist = deque()
        self.buckethash = {}
        self.lock = threading.Lock()
        self.stat_buckethits = 0
        self.stat_bucketmisses = 0

    def __del__(self):
        print "Hits:",self.stat_buckethits,"Misses:",self.stat_bucketmisses


    def _get_bucket(self, bucketno):
        bucket = self.buckethash.get(bucketno)
        if bucket is None:
            self.stat_bucketmisses += 1
            if len(self.bucketlist) >= self.bucketsize:
                lastno = self.bucketlist.pop()
                last = self.buckethash[lastno]
                del self.buckethash[lastno]
                del last
            try:
                bucket = np.memmap(self.filename, dtype=self.dt, mode='r+', 
                                offset=bucketno<<(self.bucketsize+4), shape = (1<<self.bucketsize,))
            except IOError:
                bucket = np.memmap(self.filename, dtype=self.dt, mode='w+', 
                                offset=bucketno<<(self.bucketsize+4), shape = (1<<self.bucketsize,))
            self.buckethash[bucketno] = bucket
        else:
            self.bucketlist.remove(bucketno)
            self.stat_buckethits += 1

        self.bucketlist.append(bucketno)

        return bucket

    def __getitem__(self, nodeid):
        bucketid = nodeid >> self.bucketsize

        with self.lock:
            bucket = self._get_bucket(bucketid)

            if bucket is None:
                raise KeyError()

            x,y = bucket[nodeid - (bucketid << self.bucketsize)]

        if x == 0 and y == 0:
            return None

        return Point(x,y) 

    def __setitem__(self, nodeid, value): 
        bucketid = nodeid >> self.bucketsize

        with self.lock:
            bucket = self._get_bucket(bucketid)

            if bucket is None:
                raise KeyError()

            bucket[nodeid - (bucketid << self.bucketsize)] = (value.x, value.y)

    def __delitem__(self, nodeid):
        bucketid = nodeid >> self.bucketsize

        with self.lock:
            bucket = self._get_bucket(bucketid)

            if bucket is not None:
                bucket[nodeid - (bucketid << self.bucketsize)] = (0,0)

    def setByCoords(self, nodeid, x, y):
        bucketid = nodeid >> self.bucketsize

        with self.lock:
            bucket = self._get_bucket(bucketid)

            if bucket is None:
                raise KeyError()

            bucket[nodeid - (bucketid << self.bucketsize)] = (x, y)

if __name__ == '__main__':
    print "Creating store..."
    store = NodeStore('test.store', bucketsize=5)

    print "Filling store..."
    for i in range(25500,26000):
        store[i] = Point(1,i)

    del store

    print "Reloading store..."
    store = NodeStore('test.store', bucketsize=10, numbuckets=2)

    print "Checking store..."
    for i in range(25500,26000):
        assert store[i].y == i

    try:
        x = store[1000]
    except KeyError:
        print "Yeah!"

    print "Filling store..."
    for i in range(100055500,100056000):
        store[i] = Point(i,1)

    del store

    print "Reloading store..."
    store = NodeStore('test.store', bucketsize=10, numbuckets=2)

    print "Checking store..."
    for i in range(100055500,100056000):
        assert store[i].x == i

    print "Checking store..."
    for i in range(25500,26000):
        assert store[i].y == i
