# This file is part of Osgende
# Copyright (C) 2018 Sarah Hoffmann
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

from osgende.common.connectors import TableSource
from sqlalchemy.dialects.postgresql import ARRAY
import sqlalchemy as sa
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape, to_shape
from shapely.geometry import LineString

from osgende.common.sqlalchemy import DropIndexIfExists
from osgende.common.threads import ThreadableDBObject

class PlainWayTable(ThreadableDBObject, TableSource):
    """Table that transforms columns and adds a LineString geometry column
       from a OSM node list.

       The source table must contain a 'nodes' column with the list of
       nodes. The output table then receives a copy of the 'nodes'
       column and a 'geom' column with the computed geometry and an
       id column that is the same as in the source table.

       Derived classes may overwrite add_columns() and tag_transform()
       to additionally transform the source table column. The default
       implementation just copies all data verbatim.

       This table creates its own changeset table which also takes into
       account changes to the geometry.
    """

    def __init__(self, meta, name, source, osmdata):
        id_col = sa.Column(source.id_column.name, source.id_column.type,
                           primary_key=True, autoincrement=False)
        srid = meta.info.get('srid', 4326)
        table = sa.Table(name, meta,
                           id_col,
                           sa.Column('nodes', ARRAY(sa.BigInteger)),
                           sa.Column('geom', Geometry('LINESTRING', srid=srid))
                          )

        self.add_columns(table, source)

        super().__init__(table, name + "_changeset", id_column=id_col)

        self.osmdata = osmdata
        self.src = source


    def add_columns(self, dest, src):
        """ Add additional data columns.
            This default implementation adds all columns from src except
            the id and nodes column.
        """
        to_ignore = ('nodes', src.id_column.name)

        for c in src.data.columns:
            if c.name not in to_ignore:
                dest.append_column(sa.Column(c.name, c.type))


    def construct(self, engine):
        ndsidx = sa.Index(self.data.name + "_nodes_idx",
                          self.data.c.nodes, postgresql_using='gin')

        with engine.begin() as conn:
            conn.execute(DropIndexIfExists(ndsidx))
            self.truncate(conn)

        # insert
        sql = self.src.data.select()
        res = engine.execution_options(stream_results=True).execute(sql)
        workers = self.create_worker_queue(engine, self._process_construct_next)
        for obj in res:
            workers.add_task(obj)

        workers.finish()

        with engine.begin() as conn:
            ndsidx.create(conn)


    def update(self, engine):
        pass


    def _process_construct_next(self, obj):
        cols = self._construct_row(obj, self.thread.conn)

        if cols is not None:
            self.thread.conn.execute(self.data.insert().values(cols))


    def _construct_row(self, obj, conn):
        cols = self.transform_tags(obj)
        if cols is None:
            return None

        points = self.osmdata.get_points(obj['nodes'], conn)
        if len(points) <= 1:
            return  None

        cols['geom'] = from_shape(LineString(points),
                                  srid=self.data.c.geom.type.srid)

        cols[self.id_column.name] = obj[self.id_column.name]
        cols['nodes'] = obj['nodes']

        return cols


    def transform_tags(self, obj):
        to_ignore = ('nodes', self.src.id_column.name)

        cols = {}
        for c in self.src.data.columns:
            if c.name not in to_ignore:
                cols[c.name] = obj[c.name]

        return cols
