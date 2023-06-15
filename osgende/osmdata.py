# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2015-2022 Sarah Hoffmann

from sqlalchemy import Table, Column, BigInteger, String, select
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from geoalchemy2 import Geometry
from osgende.common.table import TableSource
from osgende.common.nodestore import NodeStore, NodeStorePoint

def _mkpointlist_points(nodes, store):
    ret = []
    prev = None
    for n in filter(lambda x: x is not None, nodes):
        try:
            coord = store[n]
            if coord == prev:
                coord = NodeStorePoint(coord.x + 0.00000001, coord.y)
            prev = coord
            ret.append(coord)
        except KeyError:
            pass

    return ret

class OsmSourceTables:
    """Collection of table sources that point to raw OSM data.
    """

    def __init__(self, meta, nodestore=None):
        self.node = self.create_node_table(meta)
        self.way = self.create_way_table(meta)
        self.relation = self.create_relation_table(meta)

        if nodestore is None:
            self.get_points = self.__table_get_points
            self.nodestore = None
        else:
            self.get_points = self.__nodestore_get_points
            if isinstance(nodestore, str):
                self.nodestore = NodeStore(nodestore)
            else:
                self.nodestore = nodestore

    @staticmethod
    def create_node_table(meta):
        """ Create the table source for backing OSM node data.
            Node tables are special because they have a change table that
            also stores tags and geometry of changed nodes to avoid lookup.
        """
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
        return TableSource(data, change)

    @staticmethod
    def create_way_table(meta):
        """ Create the table source for backing OSM way data.
        """
        return TableSource(Table('ways', meta,
                                 Column('id', BigInteger),
                                 Column('tags', JSONB),
                                 Column('nodes', ARRAY(BigInteger))
                                ), change_table='way_changeset')

    @staticmethod
    def create_relation_table(meta):
        """ Create the table source for backing OSM relation data.
        """
        return TableSource(Table('relations', meta,
                                 Column('id', BigInteger),
                                 Column('tags', JSONB),
                                 Column('members', JSONB),
                                ), change_table='relation_changeset')

    def __getitem__(self, key):
        return getattr(self, key)

    def __nodestore_get_points(self, nodes, engine=None):
        return _mkpointlist_points(nodes, self.nodestore)

    def __table_get_points(self, nodes, conn):
        t = self.node.data
        sql = select(t.c.id, t.c.geom.ST_X().label('x'),
                     t.c.geom.ST_Y().label('y')).where(t.c.id.in_(nodes))

        geoms = {}
        for res in conn.execute(sql):
            geoms[res.id] = NodeStorePoint(res.x, res.y)

        return _mkpointlist_points(nodes, geoms)
