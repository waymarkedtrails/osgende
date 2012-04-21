# This file is part of Lonvia's Hiking Map
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

from osgende.common.postgisconn import PGDatabase
from osgende.common.nodestore import NodeStore

class OSMDatabase(PGDatabase):
    """ A specialised database access class that provides
        convenience access functions to the OSM DB backstorage
        and hides schema differences.
    """

    def __init__(self, dba, nodestore=None):
        PGDatabase.__init__(self, dba)

        if nodestore is None:
            self.get_nodegeom = self._get_nodegeom_db
        else:
            self._nodestore = NodeStore(nodestore, 64, 18)
            self.get_nodegeom = self._get_nodegeom_ns


    def _get_nodegeom_db(self, nodeid, cur=None):
        return self.select_one("SELECT geom FROM nodes WHERE id=%s", (nodeid,), cur=cur)


    def _get_nodegeom_ns(self, nodeid, cur=None):
        return self._nodestore[nodeid]
