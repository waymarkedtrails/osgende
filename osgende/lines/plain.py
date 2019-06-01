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

from osgende.common.table import TableSource
from sqlalchemy.dialects.postgresql import ARRAY, array
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
        table = sa.Table(name, meta,
                           sa.Column("id", source.c.id.type,
                                     primary_key=True, autoincrement=False),
                           sa.Column('nodes', ARRAY(sa.BigInteger)),
                           sa.Column('geom', Geometry('LINESTRING',
                                     srid=meta.info.get('srid', 4326)))
                          )

        self.add_columns(table, source)

        super().__init__(table, name + "_changeset")

        self.osmdata = osmdata
        self.src = source

        self.set_num_threads(meta.info.get('num_threads', 1))

    @property
    def srid(self):
        return self.c.geom.type.srid

    def add_columns(self, dest, src):
        """ Add additional data columns.
            This default implementation adds all columns from src except
            the id and nodes column.
        """
        for c in filter(lambda x : x.name not in ('nodes', 'id'), src.c):
            dest.append_column(sa.Column(c.name, c.type))


    def construct(self, engine):
        ndsidx = sa.Index(self.data.name + "_nodes_idx",
                          self.c.nodes, postgresql_using='gin')

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
            return None

        if self.srid == 3857:
            points = [p.to_mercator() for p in points]

        cols['geom'] = from_shape(LineString(points), srid=self.srid)

        cols['id'] = obj['id']
        cols['nodes'] = obj['nodes']

        return cols


    def transform_tags(self, obj):
        return {c.name: obj[c.name]
                  for c in self.src.c if c.name not in ('nodes', 'id')}


    def update(self, engine):
        with engine.begin() as conn:
            # remove all ways that have been deleted
            changeset = self._update_handle_deleted_ways(conn)
            # add new ways and update modified ones
            changeset.update(self._update_handle_modified_ways(conn))
            # finally fill the changeset table
            self.write_change_table(conn, changeset)


    def _update_handle_deleted_ways(self, conn):
        delsql = self.data.delete()\
                   .where(self.c.id.in_(self.src.select_delete()))

        changes = {}
        for row in conn.execute(delsql.returning(self.c.id)):
            changes[row[0]] = 'D'

        return changes


    def _update_handle_modified_ways(self, conn):
        d = self.data
        s = self.src.data

        cols = [s]
        for c in d.columns:
            if c.name not in ('id', 'nodes'):
                cols.append(c.label('old_' + c.name))

        # modified ways
        waysql = self.src.select_add_modify()
        # ways with modified nodes
        od = self.data.alias("old")
        ndsql = sa.select([od.c.id])\
                  .where(od.c.nodes.overlap(array([self.osmdata.node.cc.id])))\
                  .where(self.osmdata.node.cc.action != 'D')
        # combine both to get the id of modified ways
        idsql = sa.union(waysql, ndsql).alias('ids')

        # now get the info
        j = s.join(d, d.c.id == s.c.id, isouter=True)
        sql = sa.select(cols).select_from(j)\
                 .where(s.c.id == idsql.c.id)

        deleted = []
        inserts = []
        changeset = {}
        for obj in conn.execute(sql):
            oid = obj['id']
            is_added = obj['old_geom'] is None
            changed = False

            cols = self.transform_tags(obj)
            if cols is None:
                # if there is no old obejct info, then the object wasn't
                # there before and is not now
                if not is_added:
                    deleted.append(oid)
                    changeset[oid] = 'D'
                continue

            changed = False
            for k, v in cols.items():
                if str(obj['old_' + k]) != str(v):
                    changed = True
                    break

            points = self.osmdata.get_points(obj['nodes'], conn)
            if len(points) <= 1:
                if not is_added:
                    deleted.append({'oid': oid})
                    changeset[oid] = 'D'
                continue

            if self.srid == 3857:
                points = [p.to_mercator() for p in points]

            new_geom = LineString(points)
            cols['geom'] = from_shape(new_geom, srid=self.srid)
            changed = changed or is_added or (new_geom != to_shape(obj['old_geom']))

            if changed:
                cols['nodes'] = obj['nodes']
                cols['id'] = oid
                inserts.append(cols)
                changeset[oid] = 'A' if is_added else 'M'

        if len(inserts):
            conn.execute(self.upsert_data().values(inserts))
        if len(deleted):
            conn.execute(self.data.delete().where(d.c.id == sa.bindparam('oid')),
                         deleted)

        return changeset





