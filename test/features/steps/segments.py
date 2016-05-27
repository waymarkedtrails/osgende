import logging
from behave import *
from nose.tools import *

from sqlalchemy import MetaData, text, String, Column
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape

from osgende.relations import Routes, RouteSegments
from osgende.tags import TagStore

class HikingRoutes(Routes):

    def __init__(self, segments, hier=None):
        Routes.__init__(self, 'HikingRoutes', segments, hier)

    def columns(self):
        return (Column('name', String),
                Column('geom', Geometry('GEOMETRY',
                                        srid=self.segment_table.data.c.geom.type.srid)))

    def transform_tags(self, oid, tags):
        g = self.build_geometry(oid)
        return { 'name' : tags.get('name'),
                 'geom' : None if g is None else from_shape(g, srid=self.data.c.geom.type.srid) }

    def _process_next(self, obj):
        tags = self.transform_tags(obj['id'], TagStore(obj['tags']))

        if tags is not None:
            tags['id'] = obj['id']
            self.thread.conn.execute(self.data.insert().values(**tags))



@when(u"constructing a RouteSegments table '{name}'")
def step_impl(context, name):
    meta = MetaData()

    if name == 'Hiking':
        subset = "tags->'type' = 'route' AND tags->'route' = 'hiking'"
    else:
        assert_false("Unknown way table type", name)

    context.tables[name] = RouteSegments(meta, 'hiking_routes',
                                         context.osmdata, subset)
    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)


def construct_routes_table(context, name, segments, hier=None):
    if name == 'HikingRoutes':
        context.tables[name] = HikingRoutes(segments, hier)

    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)

@when(u"constructing a Routes table '{name}' from '{segments}' and '{hier}'")
def step_impl(context, name, segments, hier):
    construct_routes_table(context, name, context.tables[segments],
                           context.tables[hier])

@when(u"constructing a Routes table '{name}' from '{segments}'")
def step_impl(context, name, segments):
    construct_routes_table(context, name, context.tables[segments])
