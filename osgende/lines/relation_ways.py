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
from osgende.common.sqlalchemy import CreateView, jsonb_array_elements, DropIndexIfExists, Truncate
from osgende.common.tags import TagStore
from osgende.common.threads import ThreadableDBObject


class RelationWayTable(ThreadableDBObject, TableSource):
    """ Derived way table that contains nodes, geometries and a list of
        relations that the way is part of.

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
          geom  - geometry (only if `osmdata` is given)

        This table creates its own change table which lists all
        ways that have been directly or indirectly modified.

        The table creates an additional view on the relation table
        of the relation-way relation_ship."
    """

    def __init__(self, meta, name, way_src, relation_src, osmdata=None):
        id_col = sa.Column('id', sa.BigInteger,
                             primary_key=True, autoincrement=False)
        srid = meta.info.get('srid', 4326)
        table = sa.Table(name, meta,
                           id_col,
                           sa.Column('nodes', ARRAY(sa.BigInteger)),
                           sa.Column('rels', ARRAY(sa.BigInteger)),
                          )

        if osmdata is not None:
            table.append_column(
                    sa.Column('geom', Geometry('LINESTRING', srid=srid)))

        if hasattr(self, 'add_columns'):
            self.add_columns(table)

        super().__init__(table, name + "_changeset", id_column=id_col)

        self.osmdata = osmdata
        self.way_src = way_src
        self.relation_src = relation_src

        self.relway_view = sa.Table(name + '_relation_way_view', meta,
                                      sa.Column('relation_id', sa.BigInteger),
                                      sa.Column('way_id', sa.BigInteger))

    def create_table(self, engine):
        self.data.create(bind=engine, checkfirst=True)
        self.change.create(bind=engine, checkfirst=True)

        rels = self.relation_src.data.alias('r')
        members = jsonb_array_elements(rels.c.members).lateral()

        sql = sa.select([rels.c.id.label('relation_id'),
                         members.c.value['id'].astext.cast(sa.BigInteger).label('way_id')]
                       ).select_from(rels.join(members, onclause=sa.text("True")))\
                    .where(members.c.value['type'].astext == 'W')

        engine.execute(CreateView(self.relway_view.key, sql))

    def construct(self, engine):
        self.truncate(engine)

        # manual indexes
        relidx = sa.Index(self.data.name + "_rels_idx",
                          self.data.c.rels, postgresql_using='gin')
        ndsidx = sa.Index(self.data.name + "_nodes_idx",
                          self.data.c.nodes, postgresql_using='gin')
        # drop indexes if any
        engine.execute(DropIndexIfExists(relidx))
        engine.execute(DropIndexIfExists(ndsidx))

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

        # need reverse lookup indexes for rels and nodes
        relidx.create(engine)
        ndsidx.create(engine)

    def update(self, engine):

        # first pass: handle changed ways and nodes
        changeset = self._update_handle_changed_ways(engine)
        # second pass: handle changed relations
        changeset.update(self._update_handle_changed_rels(engine))
        # third pass: new ways added to set
        changeset.update(self._update_handle_new_ways(engine))


        # finally fill the changeset table
        engine.execute(Truncate(self.change))
        if len(changeset):
            engine.execute(self.change.insert().values([{'id': k, 'action' : v}
                                                         for k, v in changeset.items()]))

    def _update_handle_changed_ways(self, engine):
        """ Handle changes to way tags, added and removed nodes and moved nodes.
        """
        with_tags = hasattr(self, 'transform_tags')
        with_geom = self.osmdata is not None

        # Get old rows where nodes and tags have changed and
        # new node set.
        d = self.data
        w = self.way_src.data
        wheresql = [d.c.id.in_(self.way_src.select_add_modify())]
        if with_geom:
            wheresql.append(d.c.nodes.op('&& ARRAY')(self.osmdata.node.select_add_modify()))

        cols = [d, w.c.nodes.label('new_nodes')]
        if with_tags:
            cols.append(w.c.tags.label('new_tags'))
        sql = sa.select(cols).where(d.c.id == w.c.id).where(sa.or_(*wheresql))

        inserts = []
        deletes = []
        changeset = {}
        for obj in engine.execute(sql):
            oid = obj['id']
            changed = False
            if with_tags:
                cols = self.transform_tags(oid, TagStore(obj['new_tags']))
                if cols is None:
                    deletes.append({'oid' : oid})
                    changeset[oid] = 'D'
                    continue
                # check if there are actual tag changes
                for k, v in cols.items():
                    if str(obj[k]) != str(v):
                        changed = True
                        break
            else:
                cols = {}

            # Always rebuild the geometry when with_geom as nodes might have
            # moved.
            if with_geom:
                # TODO only look up new/changed nodes
                points = self.osmdata.get_points(obj['nodes'], engine)
                if len(points) <= 1:
                    deletes.append(oid)
                    changeset[oid] = 'D'
                    continue
                cols['geom'] = from_shape(sgeom.LineString(points))
                changed = changed or (cols['geom'] != obj['geom'])
            elif obj['nodes'] != obj['new_nodes']:
                changed = True
            elif changed and with_geom:
                cols['geom'] = obj['geom']

            if changed:
                cols['nodes'] = obj['new_nodes']
                cols[self.id_column.name] = oid
                cols['rels'] = obj['rels']
                inserts.append(cols)
                changeset[oid] = 'M'

        if len(inserts):
            engine.execute(self.upsert_data().values(inserts))
        if len(deletes):
            engine.execute(self.data.delete()
                            .where(self.data.c.id == sa.bindparam('oid')),
                           deletes)

        return changeset


    def _update_handle_changed_rels(self, engine):
        w = self.data
        r = self.relway_view
        rs = self.relway_view.alias('relsrc')

        # Recreate the relation set for all ways in changed relations.
        sub = sa.select([array_agg(r.c.relation_id)]).where(w.c.id == r.c.way_id)
        sql = sa.select([w.c.id, w.c.rels, sub.label('new_rels')])\
                .where(sa.or_(
                    w.c.id.in_(
                         sa.select([rs.c.way_id]).
                           where(rs.c.relation_id.in_(
                               self.relation_src.select_add_modify()))),
                    w.c.rels.op('&& ARRAY')(sa.select([self.relation_src.change_id_column()]))
                           ))

        inserts = []
        deletes = []
        changeset = {}
        for obj in engine.execute(sql):
            oid = obj['id']
            rels = sorted(obj['new_rels'])
            # If the new set is empty, the way has been removed from the set.
            if len(rels) == 0:
                deletes.append({'oid' : oid})
                changeset[oid] = 'D'
            # If the relation set differs, there was a relevant change.
            # (Only update the way set here. geometry and tag changes have
            #  already been done during the first pass.)
            elif rels != obj['rels']:
                inserts.append({'oid' : oid, 'rels' : rels})
                changeset[oid] = 'M'

        if len(inserts):
            engine.execute(self.data.update()
                             .where(self.data.c.id == sa.bindparam('oid'))
                             .values(rels=sa.bindparam('rels')), inserts)

        if len(deletes):
            engine.execute(self.data.delete()
                            .where(self.data.c.id == sa.bindparam('oid')),
                           deletes)

        return changeset

    def _update_handle_new_ways(self, engine):
        w = self.way_src.data
        r = self.relway_view
        wold = self.data

        sub = sa.select([r.c.way_id, array_agg(r.c.relation_id).label('rels')])\
                .where(r.c.way_id.notin_(sa.select([wold.c.id])))\
                .group_by(r.c.way_id).alias('aggway')

        cols = [sub.c.way_id, sub.c.rels, w.c.nodes]
        if hasattr(self, 'transform_tags'):
            cols.append(w.c.tags)

        sql = sa.select(cols).where(w.c.id == sub.c.way_id)

        changeset = {}
        inserts = []
        for obj in engine.execute(sql):
            cols = self._construct_row(obj, engine)
            if cols is not None:
                changeset[obj['way_id']] = 'A'
                inserts.append(cols)

        if len(inserts):
            engine.execute(self.data.insert().values(inserts))

        return changeset

    def _process_construct_next(self, obj):
        cols = self._construct_row(obj, self.thread.conn)

        if cols is not None:
            self.thread.conn.execute(self.data.insert().values(cols))


    def _construct_row(self, obj, conn):
        if hasattr(self, 'transform_tags'):
            cols = self.transform_tags(obj['way_id'], TagStore(obj['tags']))
            if cols is None:
                return None
        else:
            cols = {}

        if self.osmdata is not None:
            points = self.osmdata.get_points(obj['nodes'], conn)
            if len(points) <= 1:
                return
            cols['geom'] = from_shape(sgeom.LineString(points),
                                      srid=self.data.c.geom.type.srid)

        cols[self.id_column.name] = obj['way_id']
        cols['rels'] = sorted(obj['rels'])
        cols['nodes'] = obj['nodes']

        return cols
