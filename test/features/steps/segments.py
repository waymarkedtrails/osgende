import logging
from behave import *
from nose.tools import *

from sqlalchemy import MetaData, text, String, Column

from osgende.relations import Routes, RouteSegments

class HikingRoutes(Routes):

    def __init__(self, segments, hier=None):
        Routes.__init__(self, 'HikingRoutes', segments, hier)

    def columns(self):
        return (Column('name', String),)

    def transform_tags(self, oid, tags):
        return { 'name' : tags.get('name') }


@when(u"constructing a RouteSegments table '{name}'")
def step_impl(context, name):
    meta = MetaData()

    if name == 'Hiking':
        subset = text("tags->'type' = 'route' AND tags->'route' = 'hiking'")
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
