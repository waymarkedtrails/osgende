# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
import sys
from pathlib import Path
from textwrap import dedent
import tempfile

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

# always test against the source
SRC_DIR = (Path(__file__) / '..' / '..').resolve()
sys.path.insert(0, str(SRC_DIR))

from osgende import MapDB
from osgende.tools.importing import BaseImportManager
from osgende.common.sqlalchemy.database import database_drop
from db_compare import DBCompareValue


class DBOptions:
    database = 'osgende_test'
    username = None
    password = None
    status = False
    no_engine = True

class TestableDB:

    def __init__(self, tempdir):
        self.tempdir = tempdir
        self.db = MapDB(DBOptions())

    def import_data(self, data, grid=None):
        database_drop(DBOptions.database, True)
        osm_data = ""
        DBCompareValue.nodestore = {}

        if isinstance(grid, dict):
            for nid, pt in grid.items():
                osm_data += f"n{nid} x{pt[0]} y{pt[1]}\n"
                DBCompareValue.nodestore[nid] = pt
        elif isinstance(grid, str):
            x = 1
            for l in grid.splitlines():
                y = 1
                for c in l:
                    nid = None
                    if c.isdigit():
                        nid = int(c)
                    elif c.islower():
                        nid = 100 + (ord(c) - ord('a'))
                    elif c.isupper():
                        nid = 200 + (ord(c) - ord('A'))
                    elif not c.isspace():
                        raise RuntimeError("Unparsable character '%c' in node grid" % c)

                    if nid is not None:
                        osm_data += "n%d x%f y%f\n" % (nid, x, y)
                        DBCompareValue.nodestore[nid] = (x, y)
                    y += 0.0001
                x += 0.0001

        osm_data += dedent(data)

        with tempfile.NamedTemporaryFile(dir=self.tempdir, suffix='.opl') as fd:
            fd.write(osm_data.encode('utf-8'))
            fd.write(b'\n')
            fd.flush()
            with BaseImportManager(DBOptions.database) as mgr:
                mgr.create_database()
                mgr.process_file(fd.name, False)
                mgr.create_indices()

        self.db.engine = create_engine(URL.create('postgresql',
                                                  database=DBOptions.database),
                                       echo=False)

        self.db.create()
        self.db.construct()


    def update_data(self, data):
        with tempfile.NamedTemporaryFile(dir=self.tempdir, suffix='.osh.opl') as fd:
            fd.write(dedent(data).encode('utf-8'))
            fd.write(b'\n')
            fd.flush()
            with BaseImportManager(DBOptions.database) as mgr:
                mgr.process_file(fd.name, True)

        self.db.update()

    def add_table(self, table):
        return TestableTable(self.db, self.db.add_table(table.data.name, table))


class TestableTable:

    def __init__(self, db, table):
        self.db = db
        self.table = table

    def _contains(self, table, content):
        with self.db.engine.begin() as conn:
            res = conn.execute(table.select())

            assert len(content) == res.rowcount,\
                   f"Expected {len(content)} rows, got {res.rowcount}."

            todo = list(content)
            for c in res:
                for exp in content:
                    assert isinstance(exp, dict)
                    for k, v in exp.items():
                        assert k in c._fields
                        if not DBCompareValue.compare(c._mapping[k], v):
                            break
                    else:
                        todo.remove(exp)
                        break
                else:
                    assert False, f"Row {c} not expected. Stil expected: {todo}"

        assert not todo, f"Missing rows in database: {todo}"

    def has_data(self, *content):
        self._contains(self.table.data, content)

    def has_changes(self, *content):
        as_array = [{ 'action' : l[0], 'id' : int(l[1:]) } for l in content]
        self._contains(self.table.change, as_array)


@pytest.fixture
def db(tmp_path):
    db = TestableDB(tmp_path)

    yield db

    if hasattr(db.db, 'engine'):
        db.db.engine.dispose()
