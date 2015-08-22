import logging
from behave import *
from nose.tools import *

from sqlalchemy import MetaData, Column, String
from geoalchemy2 import Geometry

from osgende.nodes import NodeSubTable

class FooBar(NodeSubTable):

    def __init__(self, meta, source, subset, geom):
        NodeSubTable.__init__(self, meta, 'foobar', source, subset=subset, geomchange=geom)

    def columns(self):
        return (Column('foo', String), Column('bar', String))

    def transform_tags(self, osmid, tags):
        if 'foo' in tags or 'bar' in tags:
            return { 'foo' : tags.get('foo'), 'bar' : tags.get('bar') }

        return None

class FooBarTransform(FooBar):

    def __init__(self, meta, source, subset, geom):
        NodeSubTable.__init__(self, meta, 'foobar', source, subset=subset,
                              column_geom=Column('geom',
                                    Geometry('POINT', srid=900913)),
                              geomchange=geom)

def construct_table(context, name, subset=None, geom=None):
    meta = MetaData()
    if geom is not None:
        geom = context.tables[geom]
    if name == 'FooBar':
        context.tables[name] = FooBar(meta, context.osmdata, subset, geom)
    elif name == 'FooBarTransform':
        context.tables[name] = FooBarTransform(meta, context.osmdata,
                                               subset, geom)
    else:
        assert_false("Unknown table type", name)

    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)

@when("constructing a NodeSubTable '{name}' with subset: {subset}")
def step_impl(context, name, subset):
    construct_table(context, name, subset=subset)

@when("constructing a NodeSubTable '{name}' using geometry change '{geom}'")
def step_impl(context, name, geom):
    construct_table(context, name, geom=geom)

@when("constructing a NodeSubTable '{name}'")
def step_impl(context, name):
    construct_table(context, name)

