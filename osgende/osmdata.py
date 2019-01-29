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

from sqlalchemy import Table, Column, Integer, BigInteger, String, DateTime, select
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from geoalchemy2 import Geometry
from osgende.common.table import TableSource
from osgende.common.nodestore import NodeStore, NodeStorePoint

class OsmSourceTables(object):
    """Collection of table sources that point to raw OSM data.
    """

    def __init__(self, meta, nodestore=None, status_table=False):
        # node table is special as we have a larger change table
        data = Table('nodes', meta,
                     Column('id', BigInteger),
                     Column('tags', JSONB),
                     Column('geom', Geometry('POINT', srid=4326, spatial_index=False))
                    )
        change = Table('node_changeset', meta,
                       Column('id', BigInteger),
                       Column('action', String(1)),
                       Column('tags', JSONB),
                       Column('geom', Geometry('POINT', srid=4326))
                      )
        self.node = TableSource(data, change)

        # way and relation get a standard change table
        self.way = TableSource(Table('ways', meta,
                                     Column('id', BigInteger),
                                     Column('tags', JSONB),
                                     Column('nodes', ARRAY(BigInteger))
                               ), change_table='way_changeset')
        self.relation = TableSource(Table('relations', meta,
                                          Column('id', BigInteger),
                                          Column('tags', JSONB),
                                          Column('members', JSONB),
                                    ), change_table='relation_changeset')

        if nodestore is None:
            self.get_points = self.__table_get_points
            self.nodestore = None
        else:
            self.get_points = self.__nodestore_get_points
            if isinstance(nodestore, str):
                self.nodestore = NodeStore(nodestore)
            else:
                self.nodestore = nodestore

        if status_table:
            self.status = Table('status', meta,
                                Column('part', String),
                                Column('date', DateTime(timezone=True)),
                                Column('sequence', Integer)
                               )

    def get_status(self, conn, part='base'):
        if not hasattr(self, 'status'):
            return None

        cur = conn.execute(self.status.select().where(status.c.part == part))

        if cur is None:
            return None

        return cur['sequence']

    def set_status_from(self, conn, part, src):
        if not hasattr(self, 'status'):
            return

        data = self.status.select().where(self.status.c.part = src)

        conn.execute(self.status.upsert().values([{part : part,
                                                   date : data['date'],
                                                   sequence : data['sequence']}])

    def __getitem__(self, key):
        return getattr(self, key)

    def __nodestore_get_points(self, nodes, engine=None):
        return self.__mkpointlist_points(nodes, self.nodestore)

    def __table_get_points(self, nodes, conn):
        t = self.node.data
        sql = select([t.c.id, t.c.geom.ST_X().label('x'),
                      t.c.geom.ST_Y().label('y')]).where(t.c.id.in_(nodes))

        geoms = {}
        for res in conn.execute(sql):
            geoms[res['id']] = NodeStorePoint(res['x'], res['y'])

        return self.__mkpointlist_points(nodes, geoms)

    def __mkpointlist_points(self, nodes, store):
        ret = []
        prev = None
        for n in filter(None.__ne__, nodes):
            try:
                coord = store[n]
                if coord == prev:
                    coord = NodeStorePoint(coord.x + 0.00000001, coord.y)
                prev = coord
                ret.append(coord)
            except KeyError:
                pass

        return ret
