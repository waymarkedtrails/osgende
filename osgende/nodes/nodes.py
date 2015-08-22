# This file is part of Osgende
# Copyright (C) 2015 Sarah Hoffmann
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
Tables for nodes
"""

from osgende.subtable import TagSubTable
from osgende.tags import TagStore
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
from geoalchemy2.functions import ST_Transform
from sqlalchemy import Column, bindparam, func, select, text
from shapely.geometry import Point


class NodeSubTable(TagSubTable):
    """Most basic table type to construct a simple derived table from
       the nodes table. The difference to TagSubTable is that
       it also copies the geometry of the node to the table.
    """

    def __init__(self, meta, name, osmtables, subset=None, change=None,
                 column_geom='geom', geomchange=None):
        TagSubTable.__init__(self, meta, name, osmtables.node, subset=subset,
                             change=change)
        # need a geometry column
        if isinstance(column_geom, Column):
            self.column_geom = column_geom
        else:
            self.column_geom = Column(column_geom,
                                      Geometry('POINT', srid=meta.info.get('srid', 4326)))
        self.data.append_column(self.column_geom)

        # add an additional transform to the insert statement if the srid changes
        params = {}
        for c in self.data.c:
            if c == self.column_geom:
                # XXX This ugly from_shape hack is here to be able to inject
                # the geometry into the compiled expression later. This can't 
                # be the right way to go about this. Better ideas welcome.
                if self.src.data.c.geom.type.srid != self.column_geom.type.srid:
                    params[c.name] = ST_Transform(from_shape(Point(0, 0), srid=0),
                                              self.column_geom.type.srid)
                else:
                    params[c.name] = from_shape(Point(0, 0), srid=0)
            else:
                params[c.name] = bindparam(c.name)
        self.stm_insert = self.stm_insert.values(params)

        # the table to remember geometry changes
        self.geom_change = geomchange

    def update(self, engine):
        if self.geom_change:
            self.geom_change.add_from_select(engine,
               select([text("'D'"), self.column_geom])
                .where(self.id_column.in_(self.src.select_modify_delete()))
            )

        TagSubTable.update(self, engine)

        if self.geom_change:
            self.geom_change.add_from_select(engine,
               select([text("'A'"), self.column_geom])
                .where(self.id_column.in_(self.src.select_add_modify())))


    def _process_next(self, obj):
        tags = self.transform_tags(obj['id'], TagStore(obj['tags']))

        if tags is not None:
            tags[self.id_column.name] = obj['id']
            tags.update(obj['geom'].compile().params)
            self.thread.compiled_insert.execute(tags)

