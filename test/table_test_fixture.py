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
from pathlib import Path
import os
import subprocess
import tempfile
import unittest
from textwrap import dedent


from osgende import MapDB
from db_compare import DBCompareValue

IMPORT_CMD = Path(__file__, '..', '..', 'tools', 'osgende-import').resolve()

class TableTestFixture(unittest.TestCase):

    class Options(object):
        database = 'osgende_test'
        username = None
        password = None
        status = False

    def import_data(self, data, nodes={}, grid=None):
        assert os.system('dropdb --if-exists ' + self.Options.database) == 0
        osm_data = ""
        for k, v in nodes.items():
            osm_data += "n%d x%f y%f\n" % (k, v[0], v[1])
            DBCompareValue.nodestore[k] = v

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

        with tempfile.NamedTemporaryFile(dir=tempfile.gettempdir(), suffix='.opl') as fd:
            fd.write(osm_data.encode('utf-8'))
            fd.write(b'\n')
            fd.flush()
            cmd = [IMPORT_CMD, '-c', '-d', self.Options.database, fd.name]
            subprocess.run(cmd, check=True)

        self.db = MapDB(self.Options())
        for table in self.create_tables(self.db):
            self.db.add_table(table.data.name, table)

        self.db.create()
        self.db.construct()

    def update_data(self, data):
        with tempfile.NamedTemporaryFile(dir=tempfile.gettempdir(), suffix='.osh.opl') as fd:
            fd.write(dedent(data).encode('utf-8'))
            fd.write(b'\n')
            fd.flush()
            cmd = [IMPORT_CMD, '-C', '-d', self.Options.database, fd.name]
            subprocess.run(cmd)

        self.db.update()

    def table_equals(self, tablename, content):
        table = self.db.metadata.tables[tablename]

        with self.db.engine.begin() as conn:
            res = conn.execute(table.select())

            assert len(content) == res.rowcount

            todo = list(content)
            for c in res:
                for exp in content:
                    assert isinstance(exp, dict)
                    for k,v in exp.items():
                        assert k in c, f"Column '{k}' missing in row: {c!s}"
                        if not DBCompareValue.compare(c[k], v):
                            break
                    else:
                        todo.remove(exp)
                        break
                else:
                    assert not c, "Row not expected. Still expected: {todo!s}"

        assert not todo, "Missing rows in database"

    def has_changes(self, tablename, content):
        as_array = [ { 'action' : l[0], 'id' : int(l[1:]) } for l in content ]
        return self.table_equals(tablename, as_array)


    def tearDown(self):
        self.db.engine.dispose()
