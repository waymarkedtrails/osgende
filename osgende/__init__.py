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

""" A collection of classes that provide general implementation of tables
    derived from the Osmosis pgsnapshot schema.
"""

from . import mapdb
from .subtable import OsmosisSubTable
from .relations import RelationHierarchy, RelationPolygons, RelationSegments, RelationSegmentRoutes
from .ways import Ways, JoinedWays
from . import nodes
from .update import UpdatedGeometriesTable
from . import common
from . import tags
