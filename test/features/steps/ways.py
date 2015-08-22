import logging
from behave import *
from nose.tools import *

from sqlalchemy import MetaData, Column, String
from geoalchemy2 import Geometry

from osgende.ways import Ways

class Highway(Ways):

    def __init__(self, meta, source, subset, geom):
        Ways.__init__(self, meta, 'highways', source, subset=subset,
                      geom_change=geom)

    def columns(self):
        return (Column('type', String),)

    def transform_tags(self, oid, tags):
        if 'highway' in tags:
            return { 'type' : tags['highway'] }
        else:
            return None

class HighwayTransform(Highway):

    def __init__(self, meta, source, subset, geom):
        Ways.__init__(self, meta, 'highways', source,
                      column_geom=Column('geom',
                                    Geometry('GEOMETRY', srid=900913)),
                      subset=subset, geom_change=geom)

def construct_table(context, name, subset=None, geom=None):
    meta = MetaData()
    if geom is not None:
        geom = context.tables[geom]

    if name == 'Highway':
        context.tables[name] = Highway(meta, context.osmdata, subset, geom)
    elif name == 'HighwayTransform':
        context.tables[name] = HighwayTransform(meta, context.osmdata, subset, geom)
    else:
        assert_false("Unknown way table type", name)

    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)


@when("constructing a WaySubTable '{name}' with subset: {subset}")
def step_impl(context, name, subset):
    construct_table(context, name, subset=subset)

@when("constructing a WaySubTable '{name}' using geometry change '{geom}'")
def step_impl(context, name, geom):
    construct_table(context, name, geom=geom)

@when("constructing a WaySubTable '{name}'")
def step_impl(context, name):
    construct_table(context, name)

