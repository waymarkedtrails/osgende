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

from sqlalchemy import Table, Column, Integer, BigInteger, String, MetaData
from sqlalchemy.dialects.postgresql import HSTORE, ARRAY, VARCHAR
from geoalchemy2 import Geometry


class OsmSchema(object):
    """ Collection of tables for the OSM backing store.
    """

    def __init__(self, meta):
        self.item = { 'node' :
                         Table('nodes', meta,
                               Column('id', BigInteger),
                               Column('tags', HSTORE),
                               Column('geom', Geometry('POINT', srid=4326))
                              ),
                       'way' :
                         Table('ways', meta,
                               Column('id', BigInteger),
                               Column('tags', HSTORE),
                               Column('nodes', ARRAY(BigInteger))
                              ),
                       'relation' :
                         Table('relations', meta,
                               Column('id', BigInteger),
                               Column('tags', HSTORE)
                              ),
                       'member' :
                         Table('relation_members', meta,
                               Column('relation_id', BigInteger),
                               Column('member_id', BigInteger),
                               Column('member_type', VARCHAR(1)),
                               Column('member_role', String),
                               Column('sequence_id', Integer)
                              )
                      }

        self.change = { 'node' :
                          Table('node_changeset', meta,
                                Column('id', BigInteger),
                                Column('action', VARCHAR(1)),
                                Column('tags', HSTORE),
                                Column('geom', Geometry('POINT', srid=4326))
                             ),
                        'way' :
                          Table('way_changeset', meta,
                                Column('id', BigInteger),
                                Column('action', VARCHAR(1)),
                               ),
                        'relation' :
                          Table('relation_changeset', meta,
                                Column('id', BigInteger),
                                Column('action', VARCHAR(1)),
                               )
                      }


