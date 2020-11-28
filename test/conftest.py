# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2020 Sarah Hoffmann

import os
from pathlib import Path
from textwrap import dedent
import tempfile
import subprocess

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


from osgende import MapDB
from db_compare import DBCompareValue

IMPORT_CMD = Path(__file__, '..', '..', 'tools', 'osgende-import').resolve()

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
        assert os.system(f'dropdb --if-exists {DBOptions.database}') == 0
        osm_data = ""
        DBCompareValue.nodestore = {}

        if grid is not None:
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
            cmd = [IMPORT_CMD, '-c', '-d', DBOptions.database, fd.name]
            subprocess.run(cmd, check=True)

        self.db.engine = create_engine(URL('postgresql', database=DBOptions.database),
                                       echo=False)

        self.db.create()
        self.db.construct()


    def update_data(self, data):
        with tempfile.NamedTemporaryFile(dir=self.tempdir, suffix='.osh.opl') as fd:
            fd.write(dedent(data).encode('utf-8'))
            fd.write(b'\n')
            fd.flush()
            cmd = [IMPORT_CMD, '-C', '-d', DBOptions.database, fd.name]
            subprocess.run(cmd)

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
                        assert k in c
                        if not DBCompareValue.compare(c[k], v):
                            break
                    else:
                        todo.remove(exp)
                        break
                else:
                    assert False, f"Row {c} not expected. Stil expected: {todo}"

        assert not todo, f"Missing rows in database: {todo}"

    def has_data(self, *content):
        return self._contains(self.table.data, content)

    def has_changes(self, *content):
        as_array = [{ 'action' : l[0], 'id' : int(l[1:]) } for l in content]
        return self._contains(self.table.change, as_array)


@pytest.fixture
def db(tmp_path):
    db = TestableDB(tmp_path)

    yield db

    db.db.engine.dispose()
