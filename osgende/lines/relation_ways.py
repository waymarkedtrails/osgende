# This file is part of Osgende
# Copyright (C) 2017 Sarah Hoffmann
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

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, array_agg
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
import shapely.geometry as sgeom

from osgende.common.connectors import TableSource
from osgende.common.sqlalchemy import CreateView, jsonb_array_elements
from osgende.common.threads import ThreadableDBObject


class RelationWayTable(ThreadableDBObject, TableSource):
    """ Derived way table that contains nodes, geometries and
        a list of relations that the way is part of.

        Only ways that are in a relation from the source table
        are listed. The table may contain additional columns
        with derived tagging information. If you want that
        create a subclass and implement the add_columns() and
        transform_tags() functions. By default no tagging
        is retained.

        The table has the following predefined columns:
          id    - way id
          rels  - array of containing relation ids
          nodes - array of nodes that make up the way
          geom  - geometry

        This table creates its own change table which lists all
        ways that have been directly or indirectly modified.

        The table creates an additional view on the relation table
        of the relation-way relation_ship."
    """

    def __init__(self, meta, name, way_src, relation_src, nodestore=None):
        id_col = sa.Column('id', sa.BigInteger,
                             primary_key=True, autoincrement=False)
        srid = meta.info.get('srid', 4326)
        table = sa.Table(name, meta,
                           id_col,
                           sa.Column('nodes', ARRAY(sa.BigInteger)),
                           sa.Column('rels', ARRAY(sa.BigInteger)),
                          )

        if nodestore is not None:
            sa.Column('geom', Geometry('LINESTRING', srid=srid))

        if hasattr(self, 'add_columns'):
            self.add_columns(table)

        super().__init__(table, name + "_changeset", id_column=id_col)

        self.nodes = nodestore
        self.way_src = way_src
        self.relation_src = relation_src

        self.relway_view = sa.Table(name + '_relation_way_view', meta,
                                      sa.Column('relation_id', sa.BigInteger),
                                      sa.Column('way_id', sa.BigInteger))

    def create_table(self, engine):
        self.data.create(bind=engine, checkfirst=True)

        rels = self.relation_src.data.alias('r')
        members = jsonb_array_elements(rels.c.members).lateral()

        sql = sa.select([rels.c.id.label('relation_id'),
                         members.c.value['id'].astext.cast(sa.BigInteger).label('way_id')]
                       ).select_from(rels.join(members, onclause=sa.text("True")))\
                    .where(members.c.value['type'].astext == 'W')

        engine.execute(CreateView(self.relway_view.key, sql))

    def construct(self, engine):
        self.truncate(engine)

        w = self.way_src.data
        r = self.relway_view

        sub = sa.select([r.c.way_id, array_agg(r.c.relation_id).label('rels')])\
                .group_by(r.c.way_id).alias('aggway')

        cols = [sub.c.way_id, sub.c.rels, w.c.nodes]
        if hasattr(self, 'transform_tags'):
            cols.append(w.c.tags)

        sql = sa.select(cols).where(w.c.id == sub.c.way_id)

        res = engine.execution_options(stream_results=True).execute(sql)
        workers = self.create_worker_queue(engine, self._process_construct_next)
        for obj in res:
            workers.add_task(obj)

        workers.finish()

    def _process_construct_next(self, obj):
        if hasattr(self, 'transform_tags'):
            cols = self.transform_tags(obj['way_id'], TagStore(obj['tags']))
            if cols is None:
                return
        else:
            cols = {}

        if self.nodes is not None:
            points = self.nodes.get_points(obj['nodes'])
            if len(points) <= 1:
                return
            cols['geom'] = from_shape(sgeom.LineString(points))

        cols[self.id_column.name] = obj['way_id']
        cols['rels'] = obj['rels']
        cols['nodes'] = obj['nodes']

        self.thread.conn.execute(self.data.insert().values(cols))
