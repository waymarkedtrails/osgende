# This file is part of Osgende
# Copyright (C) 2011-2014 Sarah Hoffmann
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
""" General test framework.
"""

import unittest
import osgende.common.postgisconn as pgconn
import shapely.geometry as sgeom
import psycopg2
from datetime import datetime


class OsmosisDBTest(unittest.TestCase):
    ways=[]
    rels=[]
    upnodes=[]
    upways=[]
    uprels=[]

    def setUp(self):
        """Set up a test data base.
        """
        try:
            db = pgconn.connect("dbname=planet user=osm")
            db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = psycopg2.extensions.connection.cursor(db)
            cur.execute("""CREATE DATABASE testdb 
                           WITH TEMPLATE osmosis_test_template
                           ENCODING 'UTF-8';
                        """)

            self.db = pgconn.connect("dbname=testdb user=osm")

            self.fillDB()
        except:
            self.tearDown()
            raise
        

    def tearDown(self):
        """Delete the test database.
        """
        if not hasattr(self, 'db'):
            return

        if not self.db.closed:
            self.db.close()
        db = pgconn.connect("dbname=planet user=osm")
        db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = psycopg2.extensions.connection.cursor(db)
        cur.execute("DROP DATABASE testdb;")
    
    def fillDB(self):
        """Set up the osmosis tables in the database
        """
        nodeset = set()

        # first create the ways
        tab = pgconn.PGTable(self.db, pgconn.PGTableName("ways"))
        for w in self.ways:
            self._add_way(tab, w)
            nodeset.update(w['nodes'])

        # now add nodes
        tab = pgconn.PGTable(self.db,pgconn.PGTableName("nodes"))
        for n in nodeset:
            self._add_node(tab, n)

        # finally the relations
        tab = pgconn.PGTable(self.db,pgconn.PGTableName("relations"))
        tabmem = pgconn.PGTable(self.db,pgconn.PGTableName("relation_members"))
        for r in self.rels:
            self._add_relation(tab, tabmem, r)


    def updateDB(self):
        """Add the updates.
        """
        print "Updating database..."
        nodeset = set()

        # first create the ways
        tab = pgconn.PGTable(self.db, pgconn.PGTableName("ways"))
        action = pgconn.PGTable(self.db, pgconn.PGTableName("way_changeset"))
        for w in self.upways:
            if w['id'] < 0:
                tab.query("DELETE FROM ways WHERE id = %s", (-w['id'],))
                ac = 'D'
                wid = -w['id']
            else:
                cur = tab.select("DELETE FROM ways WHERE id = %s RETURNING id", (w['id'],))
                wid = w['id']
                if cur.fetchone():
                    ac = 'M'
                else:
                    ac = 'A'
                self._add_way(tab, w)
                nodeset.update(w['nodes'])
            action.insert_values({ 'id' : wid, 'action' : ac })

        print "Updated nodes",nodeset

        # now add nodes
        for w in self.ways:
            nodeset.difference_update(w['nodes'])

        print "Adding nodes",nodeset
        tab = pgconn.PGTable(self.db,pgconn.PGTableName("nodes"))
        action = pgconn.PGTable(self.db, pgconn.PGTableName("node_changeset"))
        for n in nodeset:
            self._add_node(tab, n)
            action.insert_values({ 'id' : n, 'action' : 'A' })

        # update nodes that should be moved
        for n in self.upnodes:
            if n < 0:
                action.insert_values({ 'id' : -n, 'action' : 'D' })
            else:
                print "Moving node",n
                action.insert_values({ 'id' : n, 'action' : 'M' })
                geom = sgeom.Point(-n,-n)
                geom._crs = 4326
                tab.query("UPDATE nodes SET geom=%s WHERE id = %s", (geom,n))

        # finally the relations
        tab = pgconn.PGTable(self.db,pgconn.PGTableName("relations"))
        tabmem = pgconn.PGTable(self.db, pgconn.PGTableName("relation_members"))
        action = pgconn.PGTable(self.db, pgconn.PGTableName("relation_changeset"))
        for r in self.uprels:
            if r['id'] < 0:
                tab.query("DELETE FROM relations WHERE id = %s", (-r['id'],))
                tab.query("DELETE FROM relation_members WHERE relation_id = %s", 
                           (-r['id'],))
                ac = 'D'
                rid = -r['id']
            else:
                num = tab.select("DELETE FROM relations WHERE id = %s RETURNING id",
                        (r['id'],))
                rid = r['id']
                if num.fetchone() is None:
                    ac = 'A'
                else:
                    print "Deleting members for relation",r['id']
                    tab.query("DELETE FROM relation_members WHERE relation_id = %s", (r['id'],))
                    ac = 'M'
                print r
                self._add_relation(tab, tabmem, r)
            action.insert_values({ 'id' : rid, 'action' : ac })

        print "Nodes",tab.select_column("SELECT id from nodes")


            
    def _add_way(self, tab, w):
        w['version'] = 1
        w['user_id'] = 1
        w['tstamp'] = datetime.now()
        w['changeset_id'] = 1
        tab.insert_values(w)


    def _add_node(self, tab, n):
        geom = sgeom.Point(n,n)
        geom._crs = 4326
        tab.insert_values({ 'id' : n,
                            'version' : 1,
                            'user_id' : 1,
                            'tstamp' : datetime.now(),
                            'changeset_id' : 1,
                            'tags' : {},
                            'geom' : geom
                          })


    def _add_relation(self, tab, tabmem, r):
        seq = 1
        for mem in r['members']:
            dp = mem.find(':')
            if dp >= 0:
                role = mem[:dp]
                mem = mem[dp+1:]
            else:
                role = ''
            tabmem.insert_values({ 'relation_id' : r['id'],
                                   'member_type' : mem[0],
                                   'member_id' : mem[1:],
                                   'member_role' : role,
                                   'sequence_id' : seq })
            seq += 1
        del r['members']
        r['version'] = 1
        r['user_id'] = 1
        r['tstamp'] = datetime.now()
        r['changeset_id'] = 1
        tab.insert_values(r)

    
