# This file is part of Lonvia's Hiking Map
# Copyright (C) 2011 Sarah Hoffmann
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
"""Test frame work for testing the segment table
"""

import osmosis_db_test as tests
from osgende import RelationSegments
import osgende.common.postgisconn as pgconn

def rhike(rid,members, tags = {}):
    tags['type'] = 'route'
    tags['route'] = 'hiking'
    return { 'id' : rid,
             'tags' : tags,
             'members' : [ 'W%s' % x for x in members] }

def rhikem(rid,members, tags = {}):
    tags['type'] = 'route'
    tags['route'] = 'hiking'
    return { 'id' : rid,
             'tags' : tags,
             'members' : members }

class GeneralSegmentTableTestCase(tests.OsmosisDBTest):


    def check_content(self, table):
        for o in self.out:
            o['rels'] = set(o['rels'])
        cur = table.select("""SELECT nodes,ways,rels,
                                     ST_Transform(geom,4326) as geom
                                FROM %s""" % (table.table))
        for c in cur:
            relset = set(c['rels'])
            nodes = [ int(round(x)) for (x,y) in c['geom'].coords]
            nodesrev = [ x for x in nodes ]
            nodesrev.reverse()
            found = None
            print str(c),str(nodes)
            for o in self.out:
                if relset != o['rels']:
                    continue
                if nodes == o['nodes']:
                    self.assertEqual(c['nodes'], [abs(x) for x in o['nodes']])
                elif nodesrev == o['nodes']:
                    self.assertEqual(c['nodes'], 
                            [abs(o['nodes'][x]) for x in range(len(o['nodes'])-1,-1,-1)])
                    c['ways'].reverse()
                else:
                    continue
                self.assertEqual(c['ways'], o['ways'])
                found = o
                break
            self.failIfEqual(found, None, "Spurious entry in DB: %s/%s" % (str(c), str(nodes)))
            self.out.remove(o)

        self.assertEqual(len(self.out), 0, "Missing entry in DB: %s"% str(self.out))

    def setup_segment_table(self):
        segtab = RelationSegments(self.db, pgconn.PGTableName('test_segments'),
                                  "tags->'route' = 'hiking'")
        segtab.create()

        return segtab
                


class CreateSegmentTableTestCase(GeneralSegmentTableTestCase):
    def runTest(self):
        segtab = self.setup_segment_table()
        segtab.construct()
        self.check_content(segtab)

class UpdateTableDummy:
    table = ''

    def add(self, foo, bar):
        pass

    def query(self, q, a):
        pass

class UpdateSegmentTableTestCase(GeneralSegmentTableTestCase):
    def runTest(self):
        segtab = self.setup_segment_table()
        segtab.construct()
        self.updateDB()
        segtab.update(UpdateTableDummy())
        self.check_content(segtab)
