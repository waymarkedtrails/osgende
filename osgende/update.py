# This file is part of Osgende
# Copyright (C) 2011 Sarah Hoffmann
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
Tables to trace updates
"""

from osgende.common.postgisconn import PGTable

class UpdatedGeometriesTable(PGTable):
    """Table that stores just a list of geometries that have been changed
       in the course of an update.

       This table contains created and modified geometries as well as
       deleted ones. The state of the geometry is identified by the action
       column. ('A' - added, 'M' - modified, 'D' - deleted)
    """

    def __init__(self, db, name):
        PGTable.__init__(self, db, name)

    def create(self):
        PGTable.create(self, "(action  char(1))")
        self.add_geometry_column("geom", "900913", 'GEOMETRY', with_index=True)

    def add(self, geom, action='M'):
        self.query("INSERT INTO %s (action, geom) VALUES (%%s, %%s)"
                     % (self.table), (action, geom))

