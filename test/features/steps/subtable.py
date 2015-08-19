import logging
from behave import *
from nose.tools import *

from sqlalchemy import MetaData, Column, String

from osgende.subtable import TagSubTable

class FooBar(TagSubTable):

    def __init__(self, meta, source, subset):
        TagSubTable.__init__(self, meta, 'foobar', source, subset=subset)

    def columns(self):
        return (Column('foo', String), Column('bar', String))

    def transform_tags(self, osmid, tags):
        if 'foo' in tags or 'bar' in tags:
            return { 'foo' : tags.get('foo'), 'bar' : tags.get('bar') }

        return None

def construct_table(context, name, osmtype, subset=None):
    meta = MetaData()
    src = getattr(context.osmdata, osmtype)
    if name == 'FooBar':
        context.tables[name] = FooBar(meta, src, subset=subset)
    else:
        assert_false("Unknown table type", name)

    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)

@when("constructing a TagSubTable {name} on '{osmtype}' with subset: {subset}")
def step_impl(context, name, osmtype, subset):
    construct_table(context, name, osmtype, subset)

@when("constructing a TagSubTable {name} on '{osmtype}'")
def step_impl(context, name, osmtype):
    construct_table(context, name, osmtype)

