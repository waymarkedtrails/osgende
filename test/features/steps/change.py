import logging
from behave import *
from nose.tools import *

from osgende.update import UpdatedGeometriesTable
from sqlalchemy import MetaData

@given("a geometry change table '{name}'")
def step_impl(context, name):
    context.tables[name] = UpdatedGeometriesTable(MetaData(), name)
    context.tables[name].data.create(context.engine)
