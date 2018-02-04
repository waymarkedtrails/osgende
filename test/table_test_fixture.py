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

from geoalchemy2.elements import _SpatialElement as GeoElement
from geoalchemy2.shape import to_shape
from collections import namedtuple, OrderedDict
import os
import subprocess
import tempfile
import unittest
from nose.tools import *
from textwrap import dedent


from osgende import MapDB
from db_compare import DBCompareValue

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


    def import_data(self, data, nodes={}):
        assert_equal(0, os.system('dropdb --if-exists ' + self.Options.database))
        osm_data = ""
        for k, v in nodes.items():
            osm_data += "n%d x%f y%f\n" % (k, v[0], v[1])
            DBCompareValue.nodestore[k] = v
        osm_data += dedent(data)

        with tempfile.NamedTemporaryFile(dir=tempfile.gettempdir(), suffix='.opl') as fd:
            fd.write(osm_data.encode('utf-8'))
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

        badrow = None
        with self.db.engine.begin() as conn:
            res = conn.execute(table.select())

            assert_equal(len(content), res.rowcount)

            for c in res:
                for exp in list(content):
                    for k,v in exp.items():
                        if k not in c:
                            badrow = str(c)
                            break
                        if not DBCompareValue.compare(c[k], v):
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

    def has_changes(self, tablename, content):
        as_array = [ { 'action' : l[0], 'id' : int(l[1:]) } for l in content ]
        return self.table_equals(tablename, as_array)


    def tearDown(self):
        self.db.engine.dispose()
