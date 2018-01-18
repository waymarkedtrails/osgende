# This file is part of Osgende
# Copyright (C) 2010-15 Sarah Hoffmann
#               2012-13 Michael Spreng
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
Tables for ways
"""

from sqlalchemy import Column, bindparam, select, text
from geoalchemy2 import Geometry
from geoalchemy2.functions import ST_Transform
from osgende.subtable import TagSubTable
from osgende.tags import TagStore
from shapely.geometry import Point, LineString
from geoalchemy2.shape import from_shape

class Ways(TagSubTable):
    """Most basic table type to construct a simple derived table from
       the ways table. The extension to OsmosisSubTable is that
       it constructs the geometry of the way.
    """

    def __init__(self, meta, name, osmtables, subset=None, change=None,
                 column_geom='geom', geom_change=None):
        TagSubTable.__init__(self, meta, name, osmtables.way, subset=subset,
                             change=change)
        src_srid = osmtables.node.data.c.geom.type.srid
        # need a geometry column
        if isinstance(column_geom, Column):
            self.column_geom = column_geom
            srid = column_geom.type.srid
        else:
            srid = meta.info.get('srid', osmtables.node.data.c.geom.type.srid)
            self.column_geom = Column(column_geom,
                                      Geometry('GEOMETRY', srid=srid))
        self.data.append_column(self.column_geom)
        self.osmtables = osmtables
        self.geom_change = geom_change

        # add an additional transform to the insert statement if the srid changes
        params = {}
        for c in self.data.c:
            if c == self.column_geom:
                # XXX This ugly from_shape hack is here to be able to inject
                # the geometry into the compiled expression later. This can't 
                # be the right way to go about this. Better ideas welcome.
                if src_srid != srid:
                    params[c.name] = ST_Transform(from_shape(Point(0, 0), srid=0),
                                                  srid)
                else:
                    params[c.name] = from_shape(Point(0, 0), srid=0)
            else:
                params[c.name] = bindparam(c.name)
        self.stm_insert = self.stm_insert.values(params)

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
            points = self.osmtables.get_points(obj['nodes'], self.thread.conn)

            # ignore ways where the node geometries are missing
            if len(points) > 1:
                tags[self.id_column.name] = obj['id']
                tags.update(from_shape(LineString(points), srid=4326).compile().params)
                self.thread.conn.execute(self.compiled_insert, tags)
