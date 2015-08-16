import logging
from behave import *
from nose.tools import *

from sqlalchemy import MetaData, Column, String

from osgende.subtable import TagSubTable

class FooBar(TagSubTable):

    def __init__(self, meta, source):
        TagSubTable.__init__(self, meta, 'foobar', source)

    def columns(self):
        return (Column('foo', String), Column('bar', String))

    def transform_tags(self, osmid, tags):
        if 'foo' in tags or 'bar' in tags:
            return { 'foo' : tags.get('foo'), 'bar' : tags.get('bar') }

        return None

@when("constructing a TagSubTable {name} on {osmtype}")
def step_impl(context, name, osmtype):
    meta = MetaData()
    src = getattr(context.osmdata, osmtype)
    if name == 'FooBar':
        context.tables[name] = FooBar(meta, src)
    else:
        assert_false("Unknown table type", name)

    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)
