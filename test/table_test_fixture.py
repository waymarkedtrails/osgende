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

from collections import namedtuple, OrderedDict
import os
import subprocess
import tempfile
import unittest
from nose.tools import *
from textwrap import dedent


from osgende import MapDB

def _deep_compare(a, b):
    if type(a) != type(b):
        return False

    if isinstance(a, str):
        return a == b

    if isinstance(a, dict) and isinstance(b, dict):
        if len(a) != len(b):
            return False

        for k,v in a.items():
            if k not in b or not _deep_compare(v, b[k]):
                return False

    try:
        for suba, subb in zip(iter(a), iter(b)):
            if not _deep_compare(suba, subb):
                return False
    except TypeError:
        pass

    return a == b

class TableTestFixture(unittest.TestCase):

    class Options(object):
        database = 'osgende_test'
        username = None
        password = None

    class TestDB(MapDB):

        def __init__(self, tables):
            self.test_tables = tables
            MapDB.__init__(self, TableTestFixture.Options())

        def create_tables(self):
            tables = OrderedDict()
            for t in self.test_tables(self):
                tables[t.data.name] = t

            _RouteTables = namedtuple('_RouteTables', tables.keys())

            return _RouteTables(**tables)


    def import_data(self, data):
        assert_equal(0, os.system('dropdb --if-exists ' + self.Options.database))
        with tempfile.NamedTemporaryFile(dir=tempfile.gettempdir(), suffix='.opl') as fd:
            fd.write(dedent(data).encode('utf-8'))
            fd.write(b'\n')
            fd.flush()
            cmd = ['../tools/osgende-import', '-c', '-d', self.Options.database, fd.name]
            subprocess.run(cmd, check=True)


        self.db = self.TestDB(self.create_tables)
        self.db.create()
        self.db.construct()

    def update_data(self, data):
        with tempfile.NamedTemporaryFile(dir=tempfile.gettempdir(), suffix='.osh.opl') as fd:
            fd.write(dedent(data).encode('utf-8'))
            fd.write(b'\n')
            fd.flush()
            cmd = ['../tools/osgende-import', '-C', '-d', self.Options.database, fd.name]
            subprocess.run(cmd)

        self.db.update()

    def table_equals(self, tablename, content):
        table = self.db.metadata.tables[tablename]
        res = self.db.engine.execute(table.select())

        assert_equal(len(content), res.rowcount)

        badrow = None
        for c in res:
            for exp in list(content):
                for k,v in exp.items():
                    if k not in c:
                        badrow = str(c)
                        break
                    if not _deep_compare(c[k], v):
                        break
                else:
                    content.remove(exp)
                    break
                if badrow is not None:
                    break
            else:
                badrow = str(c)

        assert_is_none(badrow, "unexpected row in database")

        assert_false(content, "missing rows in database")

    def tearDown(self):
        self.db.engine.dispose()
