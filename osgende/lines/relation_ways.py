# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2022 Sarah Hoffmann

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, array_agg, array
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape, to_shape
import shapely.geometry as sgeom

from osgende.common.table import TableSource
from osgende.common.sqlalchemy import CreateView, DropIndexIfExists, Truncate
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
        of the relation-way relationship."
    """

    def __init__(self, meta, name, way_src, relation_src, osmdata=None):
        table = sa.Table(name, meta,
                           sa.Column('id', sa.BigInteger,
                                     primary_key=True, autoincrement=False),
                           sa.Column('nodes', ARRAY(sa.BigInteger)),
                           sa.Column('rels', ARRAY(sa.BigInteger)),
                          )

        if osmdata is not None:
            if meta.info.get('srid', 4326) not in (4326, 3857):
                raise RuntimeError("Can only handle SRID 4326 or 3857.")
            table.append_column(
                    sa.Column('geom', Geometry('LINESTRING',
                              srid=meta.info.get('srid', 4326))))

        if hasattr(self, 'add_columns'):
            self.add_columns(table)

        super().__init__(table, name + "_changeset")

        self.osmdata = osmdata
        self.way_src = way_src
        self.relation_src = relation_src

        self.relway_view = sa.Table(name + '_relation_way_view', meta,
                                      sa.Column('relation_id', sa.BigInteger),
                                      sa.Column('way_id', sa.BigInteger))

        self.set_num_threads(meta.info.get('num_threads', 1))

    @property
    def srid(self):
        return self.c.geom.type.srid

    def create_table(self, engine):
        self.data.create(bind=engine, checkfirst=True)
        self.change.create(bind=engine, checkfirst=True)

        rels = self.relation_src.data.alias('r')
        members = sa.func.jsonb_array_elements(rels.c.members)\
                         .table_valued(sa.column('value', JSONB)).lateral()

        sql = sa.select(rels.c.id.label('relation_id'),
                        members.c.value['id'].astext.cast(sa.BigInteger).label('way_id')
                       ).select_from(rels.join(members, onclause=sa.text("True")))\
                    .where(members.c.value['type'].astext == 'W')

        engine.execute(CreateView(self.relway_view.key, sql))

    def construct(self, engine):
        with engine.begin() as conn:
            self.truncate(conn)

            # manual indexes
            relidx = sa.Index(self.data.name + "_rels_idx",
                              self.data.c.rels, postgresql_using='gin')
            ndsidx = sa.Index(self.data.name + "_nodes_idx",
                              self.data.c.nodes, postgresql_using='gin')
            # drop indexes if any
            conn.execute(DropIndexIfExists(relidx))
            conn.execute(DropIndexIfExists(ndsidx))

        w = self.way_src.data
        r = self.relway_view

        sub = sa.select(r.c.way_id, array_agg(r.c.relation_id).label('rels'))\
                .group_by(r.c.way_id).alias('aggway')

        cols = [sub.c.way_id, sub.c.rels, w.c.nodes]
        if hasattr(self, 'transform_tags'):
            cols.append(w.c.tags)

        sql = sa.select(*cols).where(w.c.id == sub.c.way_id)
        workers = self.create_worker_queue(engine, self._process_construct_next)

        with engine.execution_options(stream_results=True).begin() as conn:
            for obj in conn.execute(sql):
                workers.add_task(obj)

        workers.finish()

        # need reverse lookup indexes for rels and nodes
        with engine.begin() as conn:
            relidx.create(conn)
            ndsidx.create(conn)

    def update(self, engine):
        # first pass: handle changed ways and nodes
        changeset = self._update_handle_changed_ways(engine)
        # second pass: handle changed relations
        changeset.update(self._update_handle_changed_rels(engine))
        # third pass: new ways added to set
        changeset.update(self._update_handle_new_ways(engine))
        # finally fill the changeset table
        with engine.begin() as conn:
            self.write_change_table(conn, changeset)

    def _update_handle_changed_ways(self, engine):
        """ Handle changes to way tags, added and removed nodes and moved nodes.
        """
        with_tags = hasattr(self, 'transform_tags')
        with_geom = self.osmdata is not None

        # Get old rows where nodes and tags have changed and new node set.
        d = self.data
        w = self.way_src.data
        wc = self.way_src.change

        # ids of ways that have changed directly
        sql_idchg = sa.select(d.c.id)\
                      .where(d.c.id == wc.c.id).where(wc.c.action != 'D')

        # ids of ways where nodes have changed (only geometry mode)
        if with_geom:
            nc = self.osmdata.node.cc.id
            sql_ndchg = sa.select(d.c.id)\
                          .where(d.c.nodes.overlap(array([nc])))
            sql_idchg = sa.union(sql_idchg, sql_ndchg)

        sql_idchg = sql_idchg.alias('ids')

        waynode_sql = sa.select(w.c.nodes).where(w.c.id == d.c.id)

        cols = [d, waynode_sql.scalar_subquery().label('new_nodes')]
        if with_tags:
            waytag_sql = sa.select(w.c.tags).where(w.c.id == d.c.id)
            cols.append(waytag_sql.scalar_subquery().label('new_tags'))
        sql = sa.select(*cols).where(sql_idchg.c.id == d.c.id)

        inserts = []
        deletes = []
        changeset = {}

        with engine.begin() as conn, engine.begin() as inner:
            for obj in conn.execute(sql):
                oid = obj.id
                if obj.new_nodes is None:
                    deletes.append({'oid' : oid})
                    changeset[oid] = 'D'
                    continue
                changed = False
                if with_tags:
                    cols = self.transform_tags(oid, TagStore(obj.new_tags))
                    if cols is None:
                        deletes.append({'oid' : oid})
                        changeset[oid] = 'D'
                        continue
                    # check if there are actual tag changes
                    for k, v in cols.items():
                        if str(obj._mapping[k]) != str(v):
                            changed = True
                            break
                else:
                    cols = {}

                # Always rebuild the geometry when with_geom as nodes might have
                # moved.
                if with_geom:
                    points = self.osmdata.get_points(obj.new_nodes, inner)
                    if self.srid == 3857:
                        points = [p.to_mercator() for p in points]
                    new_geom = self.make_geometry(points)
                    if new_geom is None:
                        deletes.append({'oid' : oid})
                        changeset[oid] = 'D'
                        continue
                    cols['geom'] = from_shape(new_geom, srid=self.srid)
                    changed = changed or (new_geom != to_shape(obj.geom))
                elif obj.nodes != obj.new_nodes:
                    changed = True

                if changed:
                    cols['nodes'] = obj.new_nodes
                    cols['id'] = oid
                    cols['rels'] = obj.rels
                    inserts.append(cols)
                    changeset[oid] = 'M'

            if len(inserts):
                conn.execute(self.upsert_data().values(inserts))
            if len(deletes):
                conn.execute(self.data.delete()
                                .where(self.c.id == sa.bindparam('oid')),
                               deletes)

        return changeset


    def make_geometry(self, points):
        if len(points) <= 1:
            return None

        return sgeom.LineString(points)


    def _update_handle_changed_rels(self, engine):
        w = self.data
        r = self.relation_src.data
        rs = self.relway_view.alias('relsrc')

        # Recreate the relation set for all ways in changed relations.
        sub = sa.select(array_agg(r.c.id))\
                .where(r.c.members.contains(
                         sa.func.jsonb_build_array(
                           sa.func.jsonb_build_object('type', 'W', 'id', w.c.id))))\
                .scalar_subquery()
        sql = sa.select(w.c.id, w.c.rels, sub.label('new_rels'))\
                .where(sa.or_(
                    w.c.id.in_(
                         sa.select(rs.c.way_id)
                           .where(rs.c.relation_id.in_(
                               self.relation_src.select_add_modify()))),
                    w.c.rels.op('&& ARRAY')(sa.select(self.relation_src.cc.id).scalar_subquery())
                           ))

        inserts = []
        deletes = []
        changeset = {}
        with engine.begin() as conn:
            for obj in conn.execute(sql):
                oid = obj.id
                # If the new set is empty, the way has been removed from the set.
                if obj.new_rels is None:
                    deletes.append({'oid' : oid})
                    changeset[oid] = 'D'
                # If the relation set differs, there was a relevant change.
                # (Only update the way set here. geometry and tag changes have
                #  already been done during the first pass.)
                else:
                    rels = sorted(obj.new_rels)
                    if rels != obj.rels:
                        inserts.append({'oid' : oid, 'rels' : rels})
                        changeset[oid] = 'M'

            if len(inserts):
                conn.execute(self.data.update()
                                 .where(self.c.id == sa.bindparam('oid'))
                                 .values(rels=sa.bindparam('rels')), inserts)

            if len(deletes):
                conn.execute(self.data.delete()
                                 .where(self.c.id == sa.bindparam('oid')),
                             deletes)

        return changeset

    def _update_handle_new_ways(self, engine):
        w = self.way_src.data
        r = self.relway_view
        wold = self.data

        sub = sa.select(r.c.way_id, array_agg(r.c.relation_id).label('rels'))\
                .where(r.c.way_id.notin_(sa.select(wold.c.id)))\
                .group_by(r.c.way_id).alias('aggway')

        cols = [sub.c.way_id, sub.c.rels, w.c.nodes]
        if hasattr(self, 'transform_tags'):
            cols.append(w.c.tags)

        sql = sa.select(*cols).where(w.c.id == sub.c.way_id)

        changeset = {}
        inserts = []
        with engine.begin() as conn, engine.begin() as inner:
            for obj in conn.execute(sql):
                cols = self._construct_row(obj, inner)
                if cols is not None:
                    changeset[obj.way_id] = 'A'
                    inserts.append(cols)

        if len(inserts):
            with engine.begin() as conn:
                conn.execute(self.data.insert().values(inserts))

        return changeset

    def _process_construct_next(self, obj):
        cols = self._construct_row(obj, self.thread.conn)

        if cols is not None:
            self.thread.conn.execute(self.data.insert().values(cols))


    def _construct_row(self, obj, conn):
        if hasattr(self, 'transform_tags'):
            cols = self.transform_tags(obj.way_id, TagStore(obj.tags))
            if cols is None:
                return None
        else:
            cols = {}

        if self.osmdata is not None:
            points = self.osmdata.get_points(obj.nodes, conn)
            if self.srid == 3857:
                points = [p.to_mercator() for p in points]
            new_geom = self.make_geometry(points)
            if new_geom is None:
                return
            cols['geom'] = from_shape(new_geom, srid=self.srid)

        cols['id'] = obj.way_id
        cols['rels'] = sorted(obj.rels)
        cols['nodes'] = obj.nodes

        return cols
