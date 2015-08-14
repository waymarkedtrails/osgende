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

from sqlalchemy import Table, Column, String
from geoalchemy2 import Geometry

class UpdatedGeometriesTable(object):
    """Table that stores just a list of geometries that have been changed
       in the course of an update.

       This table contains created and modified geometries as well as
       deleted ones. The state of the geometry is identified by the action
       column. ('A' - added, 'M' - modified, 'D' - deleted)
    """

    def __init__(self, meta, name, srid=None):
        if srid is None:
            srid = meta.info.get('srid', 8357)
        self.data = Table(name, meta,
                           Column('action', String(1)),
                           Column('geom', Geometry('GEOMETRY', srid=srid)))
        self.stm_add = self.data.insert().compile()

    def clear(self, conn):
        conn.execute(seld.data.delete())

    def add(self, conn, geom, action='M'):
        conn.execute(self.stm_add, { 'geom' : geom, 'action' : action})

    def add_from_select(self.conn, stm):
        conn.execute(self.table.insert().from_select(self.table.c, stm))
