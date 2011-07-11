#!/usr/bin/python
#
# Test polygon creation.
# 
# Requires the test database to be installed

import osmtables
import postgisconn
import unittest

class PolygonTable(osmtables.RelationPolygons):

    def __init__(self, db):
        osmtables.RelationPolygons.__init__(self, db, 'poly', 
                  subset = "tags->'type' = 'test'", child_tags=['subtype'])

    def create(self):
        osmtables.RelationPolygons.create(self, "(id bigint, name text)")
        self.add_geometry_column(with_index=True)
        self.db.commit()

    def transform_tags(self, id, tags):
        return ({'name' : tags['name']})


class TestPolygonTable(unittest.TestCase):

    def setUp(self):
        self.db = postgisconn.connect('dbname=testdb user=osm')

    def test_polygons(self):
        table = PolygonTable(self.db)
        table.drop()
        table.create()
        table.construct()
        self.db.commit()

        cur = table.select("""SELECT (n.tags->'pos' = 'in') as pos,
                                     p.geom as pgeom, n.geom as ngeom,
                                     ST_Within(n.geom, p.geom) as within,
                                     n.tags->'name' as name, 
                                     n.id as id
                              FROM nodes n LEFT JOIN poly p
                              ON n.tags->'name' = p.name
                           """)
        for c in cur:
            #self.assertNotEqual(c['pgeom'], None, "Geometry for polygon %s missing" % (c['name']))
            if c['ngeom'] is not None and c['pgeom'] is not None:
                # There seems to be a bug in PostGIS. It does not evaluate the 
                # within correctly when the name condition appears.
                within = c['ngeom'].within(c['pgeom'])
                self.assertEqual(c['pos'], within, 
                                  'Node %s %s inside polygon %s' % (str(c['ngeom']), 'not' if c['pos'] else '', c['name']) )


if __name__ == "__main__":
    unittest.main()

