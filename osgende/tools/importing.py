# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2015-2024 Sarah Hoffmann
"""
Create/update OSM source tables from an OSM file.
"""
from binascii import hexlify
import struct
from contextlib import closing

from psycopg.adapt import Dumper
from psycopg.types.json import Jsonb

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
import osmium

from osgende.common.nodestore import NodeStore
from osgende.common.sqlalchemy.database import database_create
from osgende.osmdata import OsmSourceTables
from osgende.common.status import StatusManager
from osmium.replication.server import ReplicationServer

def obj2action(obj):
    if obj.visible:
        return 'C' if obj.version == 1 else 'M'
    else:
        return 'D' # delete

def loc2wkb(loc):
    # PostGIS extension that includes a SRID, see postgis/doc/ZMSGeoms.txt
    return hexlify(struct.pack("=biidd", 1, 0x20000001, 4326,
                               loc.lon, loc.lat)).decode()

def copy_to(conn, sql):
    with conn.cursor() as cur:
        with cur.copy(sql) as copy:
            while True:
                try:
                    row = (yield None)
                    copy.write_row(row)
                except GeneratorExit as e:
                    break


COPY_SQL = {
    'N': 'COPY nodes(id, tags, geom) FROM stdin',
    'W': 'COPY ways(id, tags, nodes) FROM stdin',
    'R': 'COPY relations(id, tags, members) FROM stdin'
}

COPY_CHANGE_SQL = {
    'N': 'COPY node_changeset(id, action, tags, geom) FROM stdin',
    'W': 'COPY way_changeset(id, action) FROM stdin',
    'R': 'COPY relation_changeset(id, action) FROM stdin'
}

class CopyWriter:

    def __init__(self, engine, sql):
        self.sql = sql
        self.conn = engine.raw_connection()
        self.current_writer = None
        self.copy = None

    def close(self):
        if self.copy is not None:
            self.copy.close()
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()

    def write(self, what, *attrs):
        if what != self.current_writer:
            if self.copy is not None:
                self.copy.close()
            self.conn.commit()
            self.copy = copy_to(self.conn, self.sql[what])
            next(self.copy)
            self.current_writer = what

        self.copy.send(attrs)


class ImportHandler:

    def __init__(self, engine):
        self.copy = CopyWriter(engine, COPY_SQL)

    def close(self):
        self.copy.close()

    def node(self, node):
        self.copy.write('N', node.id, Jsonb(dict(node.tags)),
                        loc2wkb(node.location))

    def way(self, way):
        self.copy.write('W', way.id, Jsonb(dict(way.tags)),
                        [n.ref for n in way.nodes])

    def relation(self, rel):
        self.copy.write('R', rel.id, Jsonb(dict(rel.tags)),
                    Jsonb([{'id': m.ref, 'role': m.role, 'type': m.type.upper()} for m in rel.members]))


class UpdateHandler:

    def __init__(self, engine, tables, keep_empty_nodes):
        self.keep_empty_nodes = keep_empty_nodes
        self.copy_change = CopyWriter(engine, COPY_CHANGE_SQL)
        self.conn = engine.connect()
        self.ntab = tables.node.data
        self.wtab = tables.way.data
        self.rtab = tables.relation.data

    def close(self):
        self.copy_change.close()
        self.conn.commit()
        self.conn.close()

    def node(self, node):
        tags = dict(node.tags)
        geom = loc2wkb(node.location)
        self.copy_change.write('N', node.id, obj2action(node), Jsonb(tags), geom)

        if node.deleted or (not self.keep_empty_nodes and not node.tags):
            sql = self.ntab.delete().where(self.ntab.c.id == node.id)
        else:
            sql = insert(self.ntab)\
                      .values(id=node.id, tags=tags, geom=geom)\
                      .on_conflict_do_update(index_elements=[self.ntab.c.id],
                                          set_={'tags': sa.text('EXCLUDED.tags'),
                                                'geom': sa.text('EXCLUDED.geom')})
        self.conn.execute(sql)

    def way(self, way):
        self.copy_change.write('W', way.id, obj2action(way))

        if way.deleted:
            sql = self.wtab.delete().where(self.wtab.c.id == way.id)
        else:
            sql = insert(self.wtab)\
                      .values(id=way.id, tags=dict(way.tags),
                              nodes=[n.ref for n in way.nodes])\
                      .on_conflict_do_update(index_elements=[self.wtab.c.id],
                                          set_={'tags': sa.text('EXCLUDED.tags'),
                                                'nodes': sa.text('EXCLUDED.nodes')})
        self.conn.execute(sql)


    def relation(self, rel):
        self.copy_change.write('R', rel.id, obj2action(rel))

        if rel.deleted:
            sql = self.rtab.delete().where(self.rtab.c.id == rel.id)
        else:
            sql = insert(self.rtab)\
                      .values(id=rel.id, tags=dict(rel.tags),
                              members=[{'id': m.ref, 'role': m.role, 'type': m.type.upper()}
                                              for m in rel.members])\
                      .on_conflict_do_update(index_elements=[self.rtab.c.id],
                                          set_={'tags': sa.text('EXCLUDED.tags'),
                                                'members': sa.text('EXCLUDED.members')})
        self.conn.execute(sql)


class BaseImportManager:

    def __init__(self, dbname, verbose=False):
        self.dbname = dbname
        dburl = sa.engine.url.URL.create('postgresql+psycopg', database=dbname)
        self.engine = sa.create_engine(dburl, echo=verbose)

        self.metadata = sa.MetaData()
        self.tables = OsmSourceTables(self.metadata)
        self.replication = None
        self.status = None
        self.nodestore = None

    def close(self):
        if self.nodestore is not None:
            self.nodestore.close()
            self.nodestore = None
        self.engine.dispose()


    def __enter__(self):
        return self


    def __exit__(self, *_):
        self.close()


    def set_replication_source(self, url):
        self.replication = ReplicationServer(url)
        self.status = StatusManager(self.metadata)

    def set_nodestore(self, filename):
        assert filename
        self.nodestore = NodeStore(filename)

    def create_database(self):
        database_create(self.dbname)

        with self.engine.begin() as conn:
            conn.execute(sa.text("CREATE EXTENSION postgis"))
        self.metadata.create_all(self.engine)


    def process_file(self, filename, is_change_file=None):
        """ Apply data from a file. This may be a change file.
        """
        reader = osmium.io.Reader(filename)
        if is_change_file is None:
            is_change_file = reader.header().has_multiple_object_versions

        if is_change_file:
            self._prepare_changeset()
            handler = UpdateHandler(self.engine, self.tables, self.nodestore is None)
        else:
            handler = ImportHandler(self.engine)

        with closing(handler) as h:
            osmium.apply(reader, *self._make_extra_handlers(is_change_file), h)

        if self.replication is not None:
            ts = osmium.replication.newest_change_from_file(filename)
            diffinfo = self.replication.get_state_info(
                           self.replication.timestamp_to_sequence(ts))
            with self.engine.begin() as conn:
                self.status.set_status(conn, 'base', diffinfo.timestamp,
                                       sequence=diffinfo.sequence)


    def process_replication(self, max_size=1024):
        """ Apply updates from the configured replication source.
        """
        if self.replication is None:
            raise RuntimeError("Need replication source to apply updates")

        with self.engine.begin() as conn:
            seq = self.status.get_sequence(conn)
            if seq is None:
                raise RuntimeError("Replication sequence missing.")

        diffs = self.replication.collect_diffs(start_id=seq, max_size=max_size)
        if diffs is None:
            return

        self._prepare_changeset()

        with closing(UpdateHandler(self.engine, self.tables, self.nodestore is None)) as h:
            diffs.reader.apply(*self._make_extra_handlers(True), h)

        diffinfo = self.replication.get_state_info(diffs.id)
        if diffinfo is not None:
            with self.engine.begin() as conn:
                self.status.set_status(conn, 'base', diffinfo.timestamp,
                                       sequence=diffinfo.sequence)

    def _make_extra_handlers(self, is_change):
        handlers = []
        if self.nodestore is not None:
            handlers.append(self.nodestore.create_handler())
            if not is_change:
                handlers.append(osmium.filter.EmptyTagFilter())

        return handlers


    def _prepare_changeset(self):
        with self.engine.begin() as conn:
            for n in ('node', 'way', 'relation'):
                conn.execute(sa.text(f"DROP INDEX IF EXISTS pk_{n}_change"))
                conn.execute(sa.text(f"TRUNCATE {n}_changeset"))


    def create_indices(self, for_change=False):
        with self.engine.begin() as conn:
            if for_change:
                for n in ('node', 'way', 'relation'):
                    i = sa.Index('pk_%s_change' % n, self.tables[n].change.c.id)
                    i.create(conn)
            else:
                for n in ('node', 'way', 'relation'):
                    i = sa.Index('pk_%ss' % n, self.tables[n].data.c.id, unique=True)
                    i.create(conn)


    def grant_read_access(self, user):
        with self.engine.begin() as conn:
            for table in (self.tables.node, self.tables.way, self.tables.relation):
                conn.execute(sa.text(f'GRANT SELECT ON TABLE {table.data.key} TO "{user}"'))
                self.status.grant_read_access(conn, user)

