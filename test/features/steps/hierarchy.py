import logging
from behave import *
from nose.tools import *

from sqlalchemy import MetaData, select, text

from osgende.relations import RelationHierarchy

@when("constructing a RelationHierarchy '{name}' with subset: {subset}")
def step_impl(context, name, subset):
    r = context.osmdata.relation.data
    context.tables[name] = RelationHierarchy(MetaData(), name, context.osmdata,
                                             subset=select([r.c.id]).where(text(subset)))
    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)

@when("constructing a RelationHierarchy '{name}'")
def step_impl(context, name):
    context.tables[name] = RelationHierarchy(MetaData(), name, context.osmdata)
    context.tables[name].data.create(context.engine)
    context.tables[name].construct(context.engine)
