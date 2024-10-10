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

import logging
from binascii import hexlify
from struct import pack
from collections import namedtuple

from osmium import index, osm
from osmium.geom import lonlat_to_mercator, Coordinates

LOG = logging.getLogger(__name__)

class NodeStorePoint(namedtuple('NodeStorePoint', ['x', 'y'])):
    """ A single entry in a permanent node storage.
    """

    def wkb(self, srid=4326):
        """ Return the coordinates as PostGIS-compatible extended WKB with
            SRID. Default SRID is WSG84. Use the `srid` parameter to encode
            a different one.
        """
        # PostGIS extension that includes a SRID, see postgis/doc/ZMSGeoms.txt
        return hexlify(pack("=biidd", 1, 0x20000001, srid,
                            self.x, self.y)).decode()

    def to_mercator(self):
        """ Project the point to Mercator.
        """
        coord = lonlat_to_mercator(Coordinates(self.x, self.y))
        return NodeStorePoint(coord.x, coord.y)

class NodeStore:
    """ Provides a map like persistent storage for node geometries.

        This implementation relies on a osmium location index.
    """

    def __init__(self, filename):
        self.mapfile = index.create_map("dense_file_array," + filename)

    def __del__(self):
        self.close()

    def create_handler(self, apply_nodes_to_ways=False):
        handler = osmium.NodeLocationForWays(self.mapfile)
        handler.apply_nodes_to_ways = apply_nodes_to_ways
        handler.ignore_errors()
        return handler

    def __getitem__(self, nodeid):
        loc = self.mapfile.get(nodeid)
        return NodeStorePoint(loc.lon, loc.lat)

    def __setitem__(self, nodeid, value):
        self.mapfile.set(nodeid, osm.Location(value.x, value.y))

    def __delitem__(self, nodeid):
        self.mapfile.set(nodeid, osm.Location())

    def set_from_node(self, node):
        """ Set an entry from the given node. Its id functions as array index
            and its location as value.
        """
        self.mapfile.set(node.id, node.location)

    def close(self):
        """ Close the underlying storage file.
        """
        if hasattr(self, 'mapfile'):
            LOG.info("Used memory by index: %d", self.mapfile.used_memory())
            del self.mapfile
