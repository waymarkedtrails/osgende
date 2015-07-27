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

from sqlalchemy import Table, Column, Integer, BigInteger, String
from sqlalchemy.dialects.postgresql import HSTORE, ARRAY
from geoalchemy2 import Geometry
from osgende.common.connectors import TableSource

class OsmSourceTables(object):
    """Collection of table sources that point to raw OSM data.
    """

    def __init__(self, meta):
        # node table is special as we have a larger change table
        data = Table('nodes', meta,
                     Column('id', BigInteger),
                     Column('tags', HSTORE),
                     Column('geom', Geometry('POINT', srid=4326, spatial_index=False))
                    )
        change = Table('node_changeset', meta,
                       Column('id', BigInteger),
                       Column('action', String(1)),
                       Column('tags', HSTORE),
                       Column('geom', Geometry('POINT', srid=4326))
                      )
        self.node = TableSource(data, change)

        # way and relation get a standard change table
        self.way = TableSource(Table('ways', meta,
                                     Column('id', BigInteger),
                                     Column('tags', HSTORE),
                                     Column('nodes', ARRAY(BigInteger))
                               ), change_table='way_changeset')
        self.relation = TableSource(Table('relations', meta,
                                          Column('id', BigInteger),
                                          Column('tags', HSTORE)
                                    ), change_table='relation_changeset')

        # the secondary member table has no changes at all
        data = Table('relation_members', meta,
                     Column('relation_id', BigInteger),
                     Column('member_id', BigInteger),
                     Column('member_type', String(1)),
                     Column('member_role', String),
                     Column('sequence_id', Integer)
                    )
        self.member = TableSource(data, id_column=data.c.relation_id)

    def __getitem__(self, key):
        return getattr(self, key)
